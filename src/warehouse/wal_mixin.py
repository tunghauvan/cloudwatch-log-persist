import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.catalog import load_catalog

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class WALMixin:
    """Write-Ahead Log (hot tier), cold tier helpers, and insert_logs."""

    # ------------------------------------------------------------------
    # Hot tier — WAL (Write-Ahead Log)
    # ------------------------------------------------------------------

    def _wal_dir(self, table_name: str) -> Path:
        """Directory for hot-tier JSONL WAL files."""
        d = self.local_staging_dir / table_name / "wal"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _wal_row_count(self, table_name: str) -> int:
        # Fast path: counter already initialised for this table.
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
            # Another thread may have already seeded it; take the max to avoid
            # losing rows counted between the scan and this moment.
            self._wal_row_counters[table_name] = max(
                count, self._wal_row_counters.get(table_name, 0)
            )
        return count

    # ------------------------------------------------------------------
    # Cold tier row counts
    # ------------------------------------------------------------------

    def _cold_row_count(self, table_name: str) -> int:
        try:
            t = self.cold_catalog.load_table(f"{self.COLD_NAMESPACE}.{table_name}")
            snapshot = t.current_snapshot()
            if snapshot is None:
                return 0
            # Read row count from snapshot summary metadata — zero RAM cost
            summary = snapshot.summary
            if summary and "total-records" in summary.additional_properties:
                return int(summary.additional_properties["total-records"])
            # Fallback: count via batch reader (still avoids a single giant allocation)
            total = 0
            for batch in t.scan().to_arrow_batch_reader():
                total += batch.num_rows
            return total
        except Exception:
            return 0

    def _staging_table_row_count(self, table_name: str) -> int:
        """Total not-yet-archived rows: WAL (hot) + cold Iceberg."""
        return self._wal_row_count(table_name) + self._cold_row_count(table_name)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _logs_to_arrow(self, logs):
        """Convert raw log dicts (camelCase keys, int timestamps) to a PyArrow Table."""
        import pyarrow as pa

        label_columns = self._get_label_columns()

        def _convert_ts(ts):
            if ts is None:
                return None
            if isinstance(ts, (int, float)):
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
            return ts

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

        import pyarrow.compute as pc
        tbl = pa.table(arrays, names=names)
        idx = pc.sort_indices(tbl, sort_keys=[("timestamp", "ascending")])
        return tbl.take(idx)

    def _build_arrow_mask(self, table, expr):
        """Translate a PyIceberg BooleanExpression to a PyArrow boolean array."""
        import pyarrow as pa
        import pyarrow.compute as pc
        try:
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual, AlwaysTrue, AlwaysFalse
        except ImportError:
            return None

        if isinstance(expr, AlwaysTrue):
            return None  # no mask needed
        if isinstance(expr, AlwaysFalse):
            return pc.cast(pa.array([False] * len(table), type=pa.bool_()), pa.bool_())
        if isinstance(expr, And):
            left = self._build_arrow_mask(table, expr.left)
            right = self._build_arrow_mask(table, expr.right)
            if left is None and right is None:
                return None
            if left is None:
                return right
            if right is None:
                return left
            return pc.and_(left, right)
        if isinstance(expr, (EqualTo, GreaterThanOrEqual, LessThanOrEqual)):
            col_name = expr.term.name
            if col_name not in table.schema.names:
                return None
            col = table.column(col_name)
            val = expr.literal.value
            try:
                scalar = pa.scalar(val, type=col.type)
            except Exception:
                try:
                    scalar = pa.scalar(str(val))
                except Exception:
                    return None
            if isinstance(expr, EqualTo):
                return pc.equal(col, scalar)
            if isinstance(expr, GreaterThanOrEqual):
                return pc.greater_equal(col, scalar)
            if isinstance(expr, LessThanOrEqual):
                return pc.less_equal(col, scalar)
        return None

    def _filter_arrow_table(self, table, filter_expression):
        """Apply a PyIceberg expression to a PyArrow Table (used for WAL tier)."""
        if filter_expression is None:
            return table
        try:
            from pyiceberg.expressions import AlwaysTrue
            if isinstance(filter_expression, AlwaysTrue):
                return table
        except ImportError:
            return table
        try:
            mask = self._build_arrow_mask(table, filter_expression)
            if mask is not None:
                return table.filter(mask)
        except Exception as e:
            logger.debug(f"[WAL filter] {e}")
        return table

    # ------------------------------------------------------------------
    # flush_wal: Hot → Cold
    # ------------------------------------------------------------------

    def flush_wal(self, table_name=None):
        """
        Hot → Cold: read WAL JSONL files in streaming batches and append to
        local Iceberg (cold), then delete each WAL file.  Idempotent.

        Memory cost: at most WAL_FLUSH_BATCH rows in RAM at a time.

        Lock discipline:
        - _wal_write_lock: held briefly to snapshot the file list, then again
          to delete each processed file and update the row counter.
        - _cold_lock: held only during cold-Iceberg appends (not during WAL
          file reads or deletions), so concurrent ingest is never blocked.
        """
        import json
        target = table_name or self.table_name
        wal_dir = self.local_staging_dir / target / "wal"
        if not wal_dir.exists():
            return 0

        # Rows to accumulate in memory before a single cold-Iceberg append.
        WAL_FLUSH_BATCH = 10_000

        # Snapshot the file list while holding the WAL lock (O(1) metadata op).
        with self._wal_write_lock:
            wal_files = sorted(wal_dir.glob("wal_*.jsonl"))
            if not wal_files:
                return 0

        self._ensure_cold_table(target)

        flushed = 0
        pending: list = []

        def _flush_pending():
            nonlocal flushed
            if not pending:
                return
            arrow_batch = self._logs_to_arrow(pending)
            with self._cold_lock:
                cold_tbl = self.cold_catalog.load_table(f"{self.COLD_NAMESPACE}.{target}")
                cold_tbl.append(arrow_batch)
            flushed += len(pending)
            pending.clear()

        for f in wal_files:
            try:
                with open(f) as fp:
                    for line in fp:
                        line = line.strip()
                        if line:
                            pending.append(json.loads(line))
                            if len(pending) >= WAL_FLUSH_BATCH:
                                _flush_pending()
            except Exception as e:
                logger.warning(f"[WAL] Could not read {f}: {e}")

            # flush after finishing each file so we can delete it immediately
            _flush_pending()

            with self._wal_write_lock:
                try:
                    f.unlink()
                except Exception:
                    pass

        # Decrement the WAL row counter by the number of rows successfully flushed.
        if flushed:
            with self._wal_write_lock:
                self._wal_row_counters[target] = max(
                    0, self._wal_row_counters.get(target, 0) - flushed
                )
            logger.info(f"[WAL] Flushed {flushed} rows → cold Iceberg for '{target}'")
        return flushed

    # ------------------------------------------------------------------
    # insert_logs: write to WAL (hot tier)
    # ------------------------------------------------------------------

    def insert_logs(self, logs: List[Dict[str, Any]], table_name: Optional[str] = None):
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from service.services.warehouse_metrics import warehouse_metrics

        start_time = time.time()
        logs_count = len(logs)

        if not PYICEBERG_AVAILABLE:
            warehouse_metrics.record_insert(
                logs_count=logs_count,
                duration_seconds=time.time() - start_time,
                error=True,
            )
            raise RuntimeError("PyIceberg is not installed")

        target_table_name = table_name or self.table_name

        # Back-pressure: if WAL+cold has grown too large (e.g. S3 unavailable),
        # reject the write so the in-memory buffer doesn't silently absorb data
        # that will never be persisted.
        current_rows = self._staging_table_row_count(target_table_name)
        if current_rows >= self.wal_max_rows:
            warehouse_metrics.record_insert(
                logs_count=logs_count,
                duration_seconds=time.time() - start_time,
                error=True,
            )
            raise RuntimeError(
                f"WAL back-pressure: {current_rows} rows in hot+cold tier "
                f">= limit {self.wal_max_rows}. Compaction may be stuck."
            )

        # Write to WAL (hot tier) — fast append-only JSONL, survives restart.
        # flush_wal() moves WAL → cold Iceberg; compact() moves cold → S3 Iceberg.
        import json
        import uuid
        with self._wal_write_lock:
            wal_dir = self._wal_dir(target_table_name)
            ts_ms = int(time.time() * 1000)
            wal_file = wal_dir / f"wal_{ts_ms}_{uuid.uuid4().hex[:8]}.jsonl"
            with wal_file.open("w") as fh:
                for log in logs:
                    fh.write(json.dumps(log, default=str) + "\n")
            # Maintain in-memory counter (O(1) — avoids per-write file scan).
            # Read the updated value while still holding the lock so we can use
            # it for logging below without a second lock acquisition.
            new_wal_count = self._wal_row_counters.get(target_table_name, 0) + len(logs)
            self._wal_row_counters[target_table_name] = new_wal_count

        # Compute staging_rows for logging: use the counter we just read + cold metadata.
        # _cold_row_count() does NOT need _wal_write_lock so no deadlock risk.
        cold_count = self._cold_row_count(target_table_name)
        staging_rows = new_wal_count + cold_count

        logger.info(
            f" Staged {len(logs)} logs for '{target_table_name}' "
            f"(hot WAL; total unstaged: {staging_rows} rows)"
        )

        duration = time.time() - start_time
        warehouse_metrics.record_insert(
            logs_count=logs_count, duration_seconds=duration, error=False
        )
