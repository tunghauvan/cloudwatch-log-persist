"""
Full integration tests for the 3-tier write pipeline: WAL → cold Iceberg → S3 Iceberg.

Tier mapping (unit tests use local FS everywhere):
  Hot   (WAL)     : JSONL files at  staging_dir/{table}/wal/wal_*.jsonl
  Cold  (Iceberg) : local Iceberg   staging_dir/cold/  (SQLite catalog)
  Archive (S3)    : main Iceberg    warehouse_dir/     (SQLite catalog in tests,
                                                        PostgreSQL+S3 in production)

What is tested:
  1. insert_logs writes to WAL only (cold and archive tables stay empty)
  2. Staged data is readable via query() before any flush (WAL tier merged inline)
  3. compact() flushes WAL \u2192 cold \u2192 archive and clears both hot and cold tiers
  4. Post-compaction query reads from archive (WAL + cold are empty after compact)
  5. Multiple tables (cloudwatch_logs + loki_logs) are isolated
  6. Compaction is idempotent when WAL and cold are empty (no archive writes)
  7. Timestamps are preserved exactly through the WAL \u2192 cold \u2192 archive round-trip
  8. Filter expressions are applied against WAL data before compaction
  9. Sort order is preserved on archive after compaction (ascending timestamp)
  10. Large batch (1\u202f000 rows) round-trip: WAL \u2192 compact \u2192 archive
"""
import shutil
import tempfile
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import pytest

# ---------------------------------------------------------------------------
# Helpers to build WarehouseManager pointing at a fully local tmp dir
# (no running MinIO / PostgreSQL needed)
# ---------------------------------------------------------------------------

def _make_warehouse(tmp_path: Path):
    """Return a WarehouseManager backed by local FS + SQLite only."""
    from unittest.mock import patch, mock_open
    import yaml

    warehouse_dir = tmp_path / "warehouse"
    staging_dir = tmp_path / "staging"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    cfg = {
        "warehouse": f"file://{warehouse_dir}",
        "catalog": "sqlite",           # triggers the SQLite branch in WarehouseManager.catalog
        "namespace": "default",
        "table_name": "cloudwatch_logs",
        "loki": {"table_name": "loki_logs"},
        "compaction": {
            "enabled": True,
            "interval_seconds": 3600,
            "max_data_files": 1000,
            "local_staging_dir": str(staging_dir),
        },
        "retention": {"enabled": False, "days": 7},
        "ingest": {"labels": {"columns": ["service", "env"]}},
    }

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(cfg))

    # patch warehouse_metrics to avoid import side-effects
    with patch("src.warehouse.warehouse.warehouse_metrics") as _m:
        _m.record_insert = lambda **_: None
        _m.record_query = lambda **_: None
        _m.record_compaction = lambda: None

    from src.warehouse.warehouse import WarehouseManager  # noqa: PLC0415
    wm = WarehouseManager(str(cfg_path))
    wm.ensure_warehouse()
    return wm


def _make_logs(n: int = 5, base_ts_ms: int = None) -> List[Dict[str, Any]]:
    if base_ts_ms is None:
        base_ts_ms = int(time.time() * 1000)
    return [
        {
            "logGroupName": "/test/group",
            "logStreamName": "stream-01",
            "timestamp": base_ts_ms + i * 1000,
            "message": f"log message {i}",
            "ingestionTime": base_ts_ms + i * 1000,
            "sequenceToken": i,
            "label_service": "svc",
            "label_env": "test",
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_warehouse(tmp_path):
    """Provides a fresh WarehouseManager per test."""
    from unittest.mock import patch

    with patch("src.warehouse.warehouse.warehouse_metrics") as mock_metrics:
        mock_metrics.record_insert = lambda **_: None
        mock_metrics.record_query = lambda **_: None
        mock_metrics.record_compaction = lambda: None
        mock_metrics.update_stats_cache = lambda **_: None
        wm = _make_warehouse(tmp_path)
        yield wm


# ==========================================================================
# Test 1 — insert_logs writes to staging only; S3 table stays empty
# ==========================================================================

def test_insert_goes_to_staging_only(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(3)
    wm.insert_logs(logs)

    # Archive (S3-equivalent) table should have 0 rows
    s3_table = wm.catalog.load_table(f"{wm.namespace}.{wm.table_name}")
    s3_rows = s3_table.scan().to_arrow().num_rows
    assert s3_rows == 0, f"Archive table should be empty before compaction, got {s3_rows}"

    # WAL (hot tier) should hold 3 rows
    staging_rows = wm._staging_table_row_count(wm.table_name)
    assert staging_rows == 3, f"Hot WAL should have 3 rows, got {staging_rows}"


# ==========================================================================
# Test 2 — query() sees staged data before compaction
# ==========================================================================

def test_query_returns_staged_data_before_compact(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(5)
    wm.insert_logs(logs)

    result = wm.query(limit=100)
    assert result.num_rows == 5, f"query() should return 5 staged rows, got {result.num_rows}"


# ==========================================================================
# Test 3 — compact() pushes staging → S3 and clears staging
# ==========================================================================

def test_compact_pushes_staging_to_s3_and_clears_staging(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(7)
    wm.insert_logs(logs)

    res = wm.compact()
    assert res["status"] == "success"
    assert res["rows_pushed_from_staging"] == 7

    # Archive table should now have 7 rows
    s3_table = wm.catalog.load_table(f"{wm.namespace}.{wm.table_name}")
    s3_rows = s3_table.scan().to_arrow().num_rows
    assert s3_rows == 7, f"Archive should have 7 rows after compact, got {s3_rows}"

    # WAL + cold should both be empty
    staging_rows = wm._staging_table_row_count(wm.table_name)
    assert staging_rows == 0, f"Hot+cold tiers should be empty after compact, got {staging_rows}"


# ==========================================================================
# Test 4 — post-compaction query reads from S3
# ==========================================================================

def test_query_after_compact_reads_from_s3(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(4)
    wm.insert_logs(logs)
    wm.compact()

    result = wm.query(limit=100)
    assert result.num_rows == 4, f"Should return 4 rows from archive after compact, got {result.num_rows}"


# ==========================================================================
# Test 5 — multiple inserts across two compactions accumulate correctly
# ==========================================================================

def test_multiple_insert_compact_cycles_accumulate(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)

    wm.insert_logs(_make_logs(3, base_ts_ms=base))
    wm.compact()  # pushes 3 → S3

    wm.insert_logs(_make_logs(4, base_ts_ms=base + 10_000))
    wm.compact()  # pushes 4 more → S3

    result = wm.query(limit=200)
    assert result.num_rows == 7, f"Should accumulate 7 rows across 2 cycles, got {result.num_rows}"


# ==========================================================================
# Test 6 — compact is idempotent when staging is empty
# ==========================================================================

def test_compact_idempotent_when_staging_empty(tmp_warehouse):
    wm = tmp_warehouse
    # No inserts — compact should be a no-op
    res = wm.compact()
    assert res["status"] in ("success", "skipped")
    assert res.get("rows_pushed_from_staging", 0) == 0


# ==========================================================================
# Test 7 — timestamps are preserved exactly through stage → compact → query
# ==========================================================================

def test_timestamps_preserved_through_round_trip(tmp_warehouse):
    wm = tmp_warehouse
    base_ms = 1_700_000_000_000  # fixed epoch ms
    logs = _make_logs(3, base_ts_ms=base_ms)
    wm.insert_logs(logs)
    wm.compact()

    result = wm.get_logs(log_group_name="/test/group")
    ts_list = sorted(e["timestamp"] for e in result)
    expected = sorted(base_ms + i * 1000 for i in range(3))
    assert ts_list == expected, f"Timestamps mismatch: {ts_list} != {expected}"


# ==========================================================================
# Test 8 — filter expression applied to staged data (before compaction)
# ==========================================================================

def test_filter_applied_to_staging(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)
    logs = [
        {
            "logGroupName": "/test/group",
            "logStreamName": "stream-01",
            "timestamp": base + i * 1000,
            "message": f"msg {i}",
            "ingestionTime": base + i * 1000,
            "sequenceToken": i,
            "label_service": "svc",
            "label_env": "test",
        }
        for i in range(10)
    ]
    wm.insert_logs(logs)

    # Query only first 5 seconds
    result = wm.get_logs(
        log_group_name="/test/group",
        start_time=base,
        end_time=base + 5_000,
    )
    ts_list = [e["timestamp"] for e in result]
    assert all(base <= ts <= base + 5_000 for ts in ts_list), (
        f"Filter not applied to staging: {ts_list}"
    )
    assert len(ts_list) <= 6, f"Expected at most 6 events, got {len(ts_list)}"


# ==========================================================================
# Test 9 — sort order on S3 after compaction is ascending timestamp
# ==========================================================================

def test_sort_order_after_compaction(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)
    # Insert in reverse order
    logs = _make_logs(5, base_ts_ms=base)
    logs_reversed = list(reversed(logs))
    wm.insert_logs(logs_reversed)
    wm.compact()

    s3_table = wm.catalog.load_table(f"{wm.namespace}.{wm.table_name}")
    arrow_data = s3_table.scan().to_arrow()
    ts_col = [t.as_py() for t in arrow_data.column("timestamp")]
    assert ts_col == sorted(ts_col), f"S3 data not sorted after compaction: {ts_col}"


# ==========================================================================
# Test 10 — large batch (1 000 rows) round-trip
# ==========================================================================

def test_large_batch_round_trip(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)
    logs = _make_logs(1000, base_ts_ms=base)
    wm.insert_logs(logs)
    res = wm.compact()

    assert res["rows_pushed_from_staging"] == 1000

    s3_table = wm.catalog.load_table(f"{wm.namespace}.{wm.table_name}")
    s3_rows = s3_table.scan().to_arrow().num_rows
    assert s3_rows == 1000, f"Expected 1000 rows on archive, got {s3_rows}"

    staging_rows = wm._staging_table_row_count(wm.table_name)
    assert staging_rows == 0, f"WAL+cold not cleared after large batch compact: {staging_rows}"


# ==========================================================================
# Test 11 — table isolation: cloudwatch_logs staging ≠ loki_logs staging
# ==========================================================================

def test_table_isolation(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(3)
    wm.insert_logs(logs, table_name="cloudwatch_logs")

    cw_staging = wm._staging_table_row_count("cloudwatch_logs")
    loki_staging = wm._staging_table_row_count("loki_logs")

    assert cw_staging == 3, f"cloudwatch_logs WAL should have 3 rows, got {cw_staging}"
    assert loki_staging == 0, f"loki_logs WAL should be empty, got {loki_staging}"


# ==========================================================================
# Test 12 — concurrent inserts don't corrupt staging (thread-safety)
# ==========================================================================

def test_concurrent_inserts_thread_safe(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)
    errors: List[Exception] = []

    def insert_batch(thread_id: int):
        try:
            logs = [
                {
                    "logGroupName": "/test/group",
                    "logStreamName": f"stream-{thread_id:02d}",
                    "timestamp": base + thread_id * 100 + i,
                    "message": f"t{thread_id} msg {i}",
                    "ingestionTime": base + thread_id * 100 + i,
                    "sequenceToken": thread_id * 10 + i,
                    "label_service": "svc",
                    "label_env": "test",
                }
                for i in range(20)
            ]
            wm.insert_logs(logs)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=insert_batch, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent insert raised errors: {errors}"

    staging_rows = wm._staging_table_row_count(wm.table_name)
    assert staging_rows == 100, f"Expected 100 WAL rows (5 threads × 20), got {staging_rows}"


# ==========================================================================
# Test 13 — get_log_groups and get_log_streams work after compact
# ==========================================================================

def test_get_log_groups_streams_after_compact(tmp_warehouse):
    wm = tmp_warehouse
    logs = _make_logs(5)
    wm.insert_logs(logs)
    wm.compact()

    groups = wm.get_log_groups()
    assert "/test/group" in groups, f"Expected log group '/test/group', got {list(groups.keys())}"

    streams = wm.get_log_streams("/test/group")
    stream_names = [s["logStreamName"] for s in streams]
    assert "stream-01" in stream_names, f"Expected stream 'stream-01', got {stream_names}"


# ==========================================================================
# Test 14 — get_logs with time range returns correct subset after compact
# ==========================================================================

def test_get_logs_time_range_after_compact(tmp_warehouse):
    wm = tmp_warehouse
    base = int(time.time() * 1000)
    logs = _make_logs(10, base_ts_ms=base)  # 10 events, 1s apart
    wm.insert_logs(logs)
    wm.compact()

    # Ask for only the first 3 events
    result = wm.get_logs(
        log_group_name="/test/group",
        start_time=base,
        end_time=base + 3_000,
        limit=100,
    )
    assert len(result) <= 4, f"Expected ≤4 events in 3 s window, got {len(result)}"
    for event in result:
        assert base <= event["timestamp"] <= base + 3_000, (
            f"Event {event['timestamp']} outside requested range"
        )


# ==========================================================================
# Test 15 — staging survives warehouse restart (catalog persisted on disk)
# ==========================================================================

def test_staging_survives_restart(tmp_path):
    """
    Stage some rows, create a new WarehouseManager instance pointing at
    the same directories, and verify the staged rows are still there.
    """
    from unittest.mock import patch
    import yaml

    warehouse_dir = tmp_path / "warehouse"
    staging_dir = tmp_path / "staging"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    cfg = {
        "warehouse": f"file://{warehouse_dir}",
        "catalog": "sqlite",
        "namespace": "default",
        "table_name": "cloudwatch_logs",
        "loki": {"table_name": "loki_logs"},
        "compaction": {
            "enabled": True,
            "interval_seconds": 3600,
            "local_staging_dir": str(staging_dir),
        },
        "retention": {"enabled": False, "days": 7},
        "ingest": {"labels": {"columns": ["service", "env"]}},
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(cfg))

    with patch("src.warehouse.warehouse.warehouse_metrics") as mock_metrics:
        mock_metrics.record_insert = lambda **_: None
        mock_metrics.record_query = lambda **_: None
        mock_metrics.record_compaction = lambda: None
        mock_metrics.update_stats_cache = lambda **_: None

        from src.warehouse.warehouse import WarehouseManager

        # First instance: insert 5 logs
        wm1 = WarehouseManager(str(cfg_path))
        wm1.ensure_warehouse()
        wm1.insert_logs(_make_logs(5))

        # Second instance: same paths — staged rows should still be present
        wm2 = WarehouseManager(str(cfg_path))
        wm2.ensure_warehouse()
        staging_rows = wm2._staging_table_row_count("cloudwatch_logs")
        assert staging_rows == 5, (
            f"WAL should persist across restarts (plain files on disk), expected 5 rows got {staging_rows}"
        )
