import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger("service.warehouse")


class WALMixin:
    """Write-Ahead Log (hot tier) and direct flush to Delta S3."""

    # ------------------------------------------------------------------
    # Hot tier — WAL directories
    # ------------------------------------------------------------------

    def _wal_dir(self, table_name: str) -> Path:
        d = self.local_staging_dir / table_name / "wal"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _wal_row_count(self, table_name: str) -> int:
        with self._wal_write_lock:
            if table_name in self._wal_row_counters:
                return self._wal_row_counters[table_name]
        # First call: scan WAL files once to seed the counter.
        d = self.local_staging_dir / table_name / "wal"
        if not d.exists():
            with self._wal_write_lock:
                self._wal_row_counters[table_name] = 0
            return 0
        count = 0
        for f in d.glob("wal_*.jsonl"):
            try:
                with open(f) as fp:
                    count += sum(1 for line in fp if line.strip())
            except Exception:
                pass
        with self._wal_write_lock:
            self._wal_row_counters[table_name] = max(
                count, self._wal_row_counters.get(table_name, 0)
            )
        return count

    def _staging_table_row_count(self, table_name: str) -> int:
        """Total not-yet-archived rows (WAL only — no more cold tier)."""
        return self._wal_row_count(table_name)

    # ------------------------------------------------------------------
    # Arrow helpers
    # ------------------------------------------------------------------

    def _logs_to_arrow(self, logs: list) -> pa.Table:
        """Convert raw log dicts (camelCase keys, int timestamps) to a sorted PyArrow Table."""
        label_columns = self._get_label_columns()

        def _convert_ts(ts):
            if ts is None:
                return None
            ts = int(ts)
            if ts > 1_000_000_000_000_000_000:
                ts_s = ts / 1_000_000_000.0
            elif ts > 1_000_000_000_000_000:
                ts_s = ts / 1_000_000.0
            elif ts > 1_000_000_000_000:
                ts_s = ts / 1_000.0
            else:
                ts_s = float(ts)
            return datetime.fromtimestamp(ts_s, tz=timezone.utc).replace(tzinfo=None)

        arrays = [
            pa.array([log.get("logGroupName", "") for log in logs]),
            pa.array([log.get("logStreamName", "") for log in logs]),
            pa.array([_convert_ts(log.get("timestamp")) for log in logs], type=pa.timestamp("us")),
            pa.array([log.get("message", "") for log in logs]),
            pa.array([_convert_ts(log.get("ingestionTime")) for log in logs], type=pa.timestamp("us")),
            pa.array([log.get("sequenceToken") for log in logs], type=pa.int64()),
        ]
        names = ["log_group_name", "log_stream_name", "timestamp", "message", "ingestion_time", "sequence_token"]
        for col in label_columns:
            arrays.append(pa.array([log.get(f"label_{col}", "") for log in logs]))
            names.append(f"label_{col}")

        tbl = pa.table(arrays, names=names)
        idx = pc.sort_indices(tbl, sort_keys=[("timestamp", "ascending")])
        tbl = tbl.take(idx)

        # Add date + hour partition columns derived from (sorted) timestamp.
        # Delta Lake stores them as date=YYYY-MM-DD/hour=HH/ (Hive-style).
        # optimize.compact() operates per-partition, so files are only merged
        # within the same hour — not across all time.
        ts_col = tbl.column("timestamp")
        dates = []
        hours = []
        for v in ts_col:
            dt = v.as_py()
            if dt is not None:
                dates.append(dt.date().isoformat())          # "2026-04-08"
                hours.append(f"{dt.hour:02d}")               # "14"
            else:
                dates.append(None)
                hours.append(None)
        tbl = tbl.append_column("date", pa.array(dates, type=pa.string()))
        tbl = tbl.append_column("hour", pa.array(hours, type=pa.string()))
        return tbl

    # ------------------------------------------------------------------
    # flush_wal: Hot → Delta S3 (direct, no cold tier)
    # ------------------------------------------------------------------

    def flush_wal(self, table_name: Optional[str] = None) -> int:
        """Drain all WAL JSONL files into a single Delta S3 write per flush cycle.

        Strategy: collect ALL rows from ALL pending WAL files into one in-memory
        list, then call write_deltalake() ONCE.  This guarantees exactly 1 Parquet
        file per partition (date/hour) per flush cycle instead of N files where
        N = ceil(total_rows / WAL_FLUSH_BATCH).

        Memory budget: bounded by wal_max_rows (default 500k) × row size (~200 B)
        ≈ 100 MB peak — acceptable for this workload.

        Safety: WAL files are only deleted after the single write commits to S3.
        """
        from deltalake import write_deltalake

        target = table_name or self.table_name
        wal_dir = self.local_staging_dir / target / "wal"
        if not wal_dir.exists():
            return 0

        delta_uri = self._get_delta_uri(target)
        storage_opts = self._get_delta_storage_options()

        with self._wal_write_lock:
            wal_files = sorted(wal_dir.glob("wal_*.jsonl"))
            if not wal_files:
                return 0

        # Resolve effective partition scheme once (avoid per-file overhead).
        _desired_partitions = ["date", "hour"]
        try:
            from deltalake import DeltaTable
            _existing_partitions = DeltaTable(delta_uri, storage_options=storage_opts).metadata().partition_columns
        except Exception:
            _existing_partitions = _desired_partitions  # table doesn't exist yet

        _effective_partitions = _existing_partitions if _existing_partitions != _desired_partitions else _desired_partitions

        # Phase 1: atomically claim WAL files by renaming under lock.
        # Any concurrent flush_wal() call will see no wal_*.jsonl files and return 0,
        # eliminating the race between _wal_flush_loop and compact() Phase 1.
        with self._wal_write_lock:
            wal_files = sorted(wal_dir.glob("wal_*.jsonl"))
            if not wal_files:
                return 0
            claimed: list = []
            for f in wal_files:
                processing = f.with_suffix(".processing")
                try:
                    f.rename(processing)
                    claimed.append(processing)
                except Exception:
                    pass  # Already claimed by another concurrent call — skip
            if not claimed:
                return 0

        # Phase 2: read all claimed .processing files into memory.
        all_rows: list = []
        readable_files: list = []
        for f in claimed:
            file_rows: list = []
            try:
                with open(f) as fp:
                    for line in fp:
                        line = line.strip()
                        if line:
                            file_rows.append(json.loads(line))
            except Exception as e:
                import traceback
                logger.warning(f"[WAL] Could not read {f}: {e}\n{traceback.format_exc()}")
                # Restore the file so it can be retried next cycle
                try:
                    f.rename(f.with_suffix(".jsonl"))
                except Exception:
                    pass
                continue
            all_rows.extend(file_rows)
            readable_files.append(f)

        if not all_rows:
            return 0

        # Phase 3: single write_deltalake call → 1 Parquet file per partition.
        try:
            arrow_batch = self._logs_to_arrow(all_rows)
            tbl = arrow_batch
            for col in ("date", "hour"):
                if col not in _effective_partitions and col in tbl.schema.names:
                    tbl = tbl.remove_column(tbl.schema.get_field_index(col))
            write_kwargs: dict = dict(
                mode="append",
                storage_options=storage_opts,
                schema_mode="merge",
            )
            if _effective_partitions:
                write_kwargs["partition_by"] = _effective_partitions
            write_deltalake(delta_uri, tbl, **write_kwargs)
        except Exception as e:
            import traceback
            logger.warning(f"[WAL] Flush failed: {e}\n{traceback.format_exc()}")
            # Restore .processing files back to .jsonl so they can be retried
            for f in readable_files:
                try:
                    f.rename(f.with_suffix(".jsonl"))
                except Exception:
                    pass
            return 0

        # Phase 4: delete successfully written .processing files.
        flushed = len(all_rows)
        with self._wal_write_lock:
            for f in readable_files:
                try:
                    f.unlink()
                except Exception:
                    pass
            self._wal_row_counters[target] = max(
                0, self._wal_row_counters.get(target, 0) - flushed
            )

        logger.info(f"[WAL] Flushed {flushed} rows → Delta S3 '{target}' ({len(readable_files)} WAL file(s))")
        return flushed

    # ------------------------------------------------------------------
    # insert_logs: write to WAL (hot tier)
    # ------------------------------------------------------------------

    def insert_logs(self, logs: List[Dict[str, Any]], table_name: Optional[str] = None):
        from service.services.warehouse_metrics import warehouse_metrics

        start_time = time.time()
        logs_count = len(logs)
        target_table_name = table_name or self.table_name

        # Back-pressure: reject if WAL has grown too large (S3 unavailable etc.)
        current_rows = self._staging_table_row_count(target_table_name)
        if current_rows >= self.wal_max_rows:
            warehouse_metrics.record_insert(
                logs_count=logs_count,
                duration_seconds=time.time() - start_time,
                error=True,
            )
            raise RuntimeError(
                f"WAL back-pressure: {current_rows} rows >= limit {self.wal_max_rows}. "
                "Delta flush may be stuck."
            )

        with self._wal_write_lock:
            wal_dir = self._wal_dir(target_table_name)
            ts_ms = int(time.time() * 1000)
            wal_file = wal_dir / f"wal_{ts_ms}_{uuid.uuid4().hex[:8]}.jsonl"
            with wal_file.open("w") as fh:
                for log in logs:
                    fh.write(json.dumps(log, default=str) + "\n")
            new_wal_count = self._wal_row_counters.get(target_table_name, 0) + len(logs)
            self._wal_row_counters[target_table_name] = new_wal_count

        logger.info(
            f" Staged {len(logs)} logs for '{target_table_name}' "
            f"(hot WAL; total unstaged: {new_wal_count} rows)"
        )

        warehouse_metrics.record_insert(
            logs_count=logs_count,
            duration_seconds=time.time() - start_time,
            error=False,
        )
