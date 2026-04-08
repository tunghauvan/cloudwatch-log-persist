import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger("service.warehouse")

# Thread-local DuckDB connections (one per gunicorn worker thread)
_duckdb_local = threading.local()


class QueryMixin:
    """Query operations: DuckDB → Delta S3 + PyArrow WAL hot tier."""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ms_to_datetime(ms: int) -> datetime:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)

    def _get_duckdb_conn(self):
        """Return a thread-local DuckDB connection configured for S3/MinIO."""
        if getattr(_duckdb_local, "conn", None) is not None:
            return _duckdb_local.conn

        import duckdb

        conn = duckdb.connect()

        # Install extensions (idempotent — fast if already cached locally)
        for ext in ("httpfs", "delta"):
            try:
                conn.execute(f"INSTALL {ext}; LOAD {ext};")
            except Exception as e:
                logger.warning(f"[DuckDB] Could not install extension '{ext}': {e}")

        s3_cfg = self.config.get("s3", {})
        use_ec2_role = s3_cfg.get("use_ec2_role", False)
        region = s3_cfg.get("region", "us-east-1")

        if not use_ec2_role:
            access_key = s3_cfg.get("access_key", "admin")
            secret_key = s3_cfg.get("secret_key", "admin123")
            endpoint = s3_cfg.get("endpoint", "").strip()

            # Use DuckDB Secret API — the delta extension (delta-kernel-rs) reads
            # credentials from Secrets, not from the legacy SET s3_* variables.
            secret_sql = f"""
                CREATE OR REPLACE SECRET _s3_secret (
                    TYPE S3,
                    KEY_ID '{access_key}',
                    SECRET '{secret_key}',
                    REGION '{region}'
            """
            if endpoint:
                ep = endpoint.replace("https://", "").replace("http://", "")
                secret_sql += f",\n                    ENDPOINT '{ep}'"
                secret_sql += ",\n                    URL_STYLE 'path'"
                secret_sql += ",\n                    USE_SSL false"
            secret_sql += "\n                );"
            conn.execute(secret_sql)

        _duckdb_local.conn = conn
        return conn

    def _reset_duckdb_conn(self):
        """Close and discard the thread-local connection (e.g. after a fatal error)."""
        conn = getattr(_duckdb_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        _duckdb_local.conn = None

    # ------------------------------------------------------------------
    # Delta S3 scan via DuckDB
    # ------------------------------------------------------------------

    def _scan_delta_duckdb(
        self,
        table_name: str,
        log_group_name: Optional[str] = None,
        log_stream_name: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        labels_filter: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        limit: int = 500_000,
    ) -> pa.Table:
        """Query a Delta Lake table on S3/MinIO using DuckDB delta_scan().

        DuckDB handles:
        - Partition pruning (date/hour columns pushed to file-selection)
        - Parquet predicate pushdown (timestamp, log_group_name, etc.)
        - Streaming result → Arrow without loading all files into RAM
        """
        delta_uri = self._get_delta_uri(table_name)

        conditions: List[str] = []
        params: list = []

        if log_group_name:
            conditions.append("log_group_name = ?")
            params.append(log_group_name)
        if log_stream_name:
            conditions.append("log_stream_name = ?")
            params.append(log_stream_name)

        start_dt = self._ms_to_datetime(start_time_ms) if start_time_ms else None
        end_dt = self._ms_to_datetime(end_time_ms) if end_time_ms else None

        if start_dt:
            conditions.append("timestamp >= ?")
            params.append(start_dt)
        if end_dt:
            conditions.append("timestamp <= ?")
            params.append(end_dt)
        # NOTE: We intentionally do NOT add WHERE filters on the partition columns
        # `date` and `hour`. DuckDB 1.5.x delta extension has a bug where filtering
        # on partition-derived columns causes incorrect over-pruning (e.g. adding
        # `WHERE date = '2026-04-08'` silently drops non-current-hour partitions).
        # The `timestamp` column filter above is sufficient for correctness; DuckDB
        # will apply it as a post-scan row filter on all relevant parquet files.

        if labels_filter:
            label_mapping = {"service_name": "service"}
            for k, v in labels_filter.items():
                mapped = label_mapping.get(k, k)
                conditions.append(f"label_{mapped} = ?")
                params.append(v)

        # Use SELECT * so delta_scan can push partition columns into file pruning properly.
        # date/hour are partition-derived columns (not in Parquet files) — EXCLUDE syntax
        # can behave unexpectedly in some DuckDB versions, so we drop them in Python instead.
        if columns:
            # Pull date+hour alongside requested cols so WHERE filters stay consistent.
            proj_cols = list(dict.fromkeys(["date", "hour"] + list(columns)))
            col_select = ", ".join(f'"{c}"' for c in proj_cols)
        else:
            col_select = "*"

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT {col_select}
            FROM delta_scan(?)
            {where}
            ORDER BY timestamp ASC
            LIMIT {int(limit)}
        """

        try:
            conn = self._get_duckdb_conn()
            result = conn.execute(sql, [delta_uri] + params).fetch_arrow_table()
            # Strip partition helper columns unless explicitly requested
            drop_cols = {"date", "hour"} - (set(columns) if columns else set())
            keep = [c for c in result.schema.names if c not in drop_cols]
            if keep and len(keep) < result.num_columns:
                result = result.select(keep)
            return result
        except Exception as e:
            msg = str(e)
            # Table not yet initialised — silently return empty
            if any(x in msg for x in (
                "not a Delta table", "No log files", "doesn't exist",
                "Invariant violation", "TableNotFound",
            )):
                return pa.table({})
            # For other errors (stale file refs, S3 issues) log a warning and return empty
            # so individual query failures don't bring down the service.
            logger.warning(
                f"[DuckDB] delta_scan failed for '{table_name}': {type(e).__name__}: {e}"
            )
            self._reset_duckdb_conn()
            return pa.table({})

    # ------------------------------------------------------------------
    # WAL hot tier scan (local JSONL → PyArrow)
    # ------------------------------------------------------------------

    def _scan_wal(
        self,
        table_name: str,
        log_group_name: Optional[str] = None,
        log_stream_name: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        labels_filter: Optional[Dict[str, str]] = None,
        limit: int = 100_000,
    ) -> Optional[pa.Table]:
        """Read WAL JSONL files and apply filters in-memory via PyArrow compute."""
        wal_dir = self.local_staging_dir / table_name / "wal"
        if not wal_dir.exists():
            return None

        logs = []
        for f in sorted(wal_dir.glob("wal_*.jsonl"), reverse=True):
            if len(logs) >= limit:
                break
            try:
                with open(f) as fp:
                    for line in fp:
                        line = line.strip()
                        if line:
                            logs.append(json.loads(line))
                            if len(logs) >= limit:
                                break
            except Exception:
                pass

        if not logs:
            return None

        tbl = self._logs_to_arrow(logs)

        # Build filter mask using PyArrow compute (vectorised, no row-by-row Python)
        masks = []
        if log_group_name:
            masks.append(pc.equal(tbl["log_group_name"], log_group_name))
        if log_stream_name:
            masks.append(pc.equal(tbl["log_stream_name"], log_stream_name))
        if start_time_ms:
            start_dt = self._ms_to_datetime(start_time_ms)
            masks.append(pc.greater_equal(
                tbl["timestamp"], pa.scalar(start_dt, type=pa.timestamp("us"))
            ))
        if end_time_ms:
            end_dt = self._ms_to_datetime(end_time_ms)
            masks.append(pc.less_equal(
                tbl["timestamp"], pa.scalar(end_dt, type=pa.timestamp("us"))
            ))
        if labels_filter:
            label_mapping = {"service_name": "service"}
            for k, v in labels_filter.items():
                mapped = label_mapping.get(k, k)
                col = f"label_{mapped}"
                if col in tbl.schema.names:
                    masks.append(pc.equal(tbl[col], v))

        if masks:
            mask = masks[0]
            for m in masks[1:]:
                mask = pc.and_(mask, m)
            tbl = tbl.filter(mask)

        return tbl if tbl.num_rows > 0 else None

    # ------------------------------------------------------------------
    # Public query API
    # ------------------------------------------------------------------

    def query(
        self,
        log_group_name: Optional[str] = None,
        log_stream_name: Optional[str] = None,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 100,
        table_name: Optional[str] = None,
        labels_filter: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        # Legacy kwargs accepted but ignored (kept for call-site compat)
        filter_expr: Optional[str] = None,
        filter_expression=None,
        selected_fields=None,
    ) -> pa.Table:
        from service.services.warehouse_metrics import warehouse_metrics

        start = time.time()
        target = table_name or self.table_name

        if selected_fields and not columns:
            columns = list(selected_fields)

        MAX_ROWS = 500_000
        scan_limit = limit if (limit and limit > 0) else MAX_ROWS

        scan_kwargs = dict(
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            labels_filter=labels_filter,
        )

        try:
            s3_result = self._scan_delta_duckdb(
                target, **scan_kwargs, columns=columns, limit=scan_limit
            )
            wal_result = self._scan_wal(
                target, **scan_kwargs, limit=min(scan_limit, 100_000)
            )

            parts = [p for p in (s3_result, wal_result) if p is not None and p.num_rows > 0]

            if len(parts) == 0:
                result = pa.table({})
            elif len(parts) == 1:
                result = parts[0]
            else:
                try:
                    combined = pa.concat_tables(parts, promote_options="default")
                except Exception:
                    combined = s3_result
                sort_idx = pc.sort_indices(combined, sort_keys=[("timestamp", "ascending")])
                result = combined.take(sort_idx)
                if limit and limit > 0 and result.num_rows > limit:
                    result = result.slice(0, limit)

            logger.debug(
                f"[DuckDB] Query '{target}': {result.num_rows} rows "
                f"(delta={s3_result.num_rows if s3_result is not None else 0}, "
                f"wal={wal_result.num_rows if wal_result is not None else 0})"
            )
            warehouse_metrics.record_query(
                logs_returned=result.num_rows,
                duration_seconds=time.time() - start,
                error=False,
            )
            return result

        except Exception as e:
            warehouse_metrics.record_query(
                logs_returned=0,
                duration_seconds=time.time() - start,
                error=True,
            )
            raise

    # ------------------------------------------------------------------
    # High-level helpers (log groups, streams, events)
    # ------------------------------------------------------------------

    def get_log_groups(self):
        now = time.time()
        if self._log_groups_cache is not None and now - self._log_groups_cache_time < 30.0:
            return self._log_groups_cache

        result = self.query(columns=["log_group_name", "ingestion_time"], limit=10000)
        groups = {}
        for lg, ts in zip(
            result.column("log_group_name").to_pylist(),
            result.column("ingestion_time").to_pylist(),
        ):
            if lg and lg not in groups:
                groups[lg] = {
                    "logGroupName": lg,
                    "creationTime": int(ts.timestamp() * 1000) if ts else 0,
                    "metricFilterCount": 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{lg}",
                    "storedBytes": 0,
                }

        self._log_groups_cache = groups
        self._log_groups_cache_time = now
        return groups

    def get_log_streams(self, log_group_name: str):
        now = time.time()
        cached = self._log_streams_cache.get(log_group_name)
        if cached and now - cached[0] < 30.0:
            return cached[1]

        result = self.query(
            log_group_name=log_group_name,
            columns=["log_stream_name", "ingestion_time"],
            limit=10000,
        )
        streams = {}
        for ls, ts in zip(
            result.column("log_stream_name").to_pylist(),
            result.column("ingestion_time").to_pylist(),
        ):
            if ls and ls not in streams:
                streams[ls] = {
                    "logStreamName": ls,
                    "creationTime": int(ts.timestamp() * 1000) if ts else 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-stream:{log_group_name}/{ls}",
                    "storedBytes": 0,
                }

        result_list = list(streams.values())
        self._log_streams_cache[log_group_name] = (now, result_list)
        return result_list

    def get_logs(
        self,
        log_group_name: str,
        log_stream_name: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
        table_name: Optional[str] = None,
        labels_filter: Optional[Dict[str, str]] = None,
    ):
        import calendar

        result = self.query(
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            start_time_ms=start_time,
            end_time_ms=end_time,
            limit=limit,
            table_name=table_name,
            labels_filter=labels_filter,
        )

        def _naive_utc_to_ms(dt) -> int:
            if dt is None:
                return 0
            return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000

        label_columns = self._get_label_columns()
        label_data = {}
        for col in label_columns:
            try:
                label_data[col] = result.column(f"label_{col}").to_pylist()
            except Exception:
                label_data[col] = [None] * result.num_rows

        events = []
        ts_col = result.column("timestamp").to_pylist() if result.num_rows else []
        msg_col = result.column("message").to_pylist() if result.num_rows else []
        ingest_col = result.column("ingestion_time").to_pylist() if result.num_rows else []
        lg_col = result.column("log_group_name").to_pylist() if result.num_rows else []
        ls_col = result.column("log_stream_name").to_pylist() if result.num_rows else []

        for i in range(result.num_rows):
            event = {
                "timestamp": _naive_utc_to_ms(ts_col[i]),
                "message": msg_col[i] or "",
                "ingestionTime": _naive_utc_to_ms(ingest_col[i]),
                "logGroupName": lg_col[i] or "",
                "logStreamName": ls_col[i] or "",
            }
            for col, values in label_data.items():
                event[f"label_{col}"] = values[i] or ""
            events.append(event)

        events.sort(key=lambda x: x["timestamp"])
        return events[:limit] if limit else events



