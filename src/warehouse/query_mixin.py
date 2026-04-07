import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.expressions import (
        And,
        EqualTo,
        GreaterThanOrEqual,
        LessThanOrEqual,
        AlwaysTrue,
    )
    from pyiceberg.expressions.literals import TimestampLiteral

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class QueryMixin:
    """Query and read operations for the warehouse."""

    def _scan_to_arrow_with_limit(self, scan, limit: int):
        import pyarrow as pa

        batch_reader = scan.to_arrow_batch_reader()
        batches = []
        rows_collected = 0

        for batch in batch_reader:
            if batch.num_rows <= 0:
                continue

            remaining = limit - rows_collected
            if batch.num_rows > remaining:
                batches.append(batch.slice(0, remaining))
                rows_collected += remaining
                break

            batches.append(batch)
            rows_collected += batch.num_rows

            if rows_collected >= limit:
                break

        if batches:
            return pa.Table.from_batches(batches)

        schema = getattr(batch_reader, "schema", None)
        if schema is not None:
            return pa.Table.from_batches([], schema=schema)

        return scan.to_arrow()

    def query(
        self,
        filter_expr: Optional[str] = None,
        limit: int = 100,
        table_name: Optional[str] = None,
        filter_expression=None,
        selected_fields: Optional[tuple] = None,
    ):
        from service.services.warehouse_metrics import warehouse_metrics

        start_time = time.time()

        if not PYICEBERG_AVAILABLE:
            warehouse_metrics.record_query(
                logs_returned=0, duration_seconds=time.time() - start_time, error=True
            )
            raise RuntimeError("PyIceberg is not installed")

        # Always reload table to get fresh data files
        target_table_name = table_name or self.table_name
        table_id = f"{self.namespace}.{target_table_name}"

        try:
            table_obj = self.catalog.load_table(table_id)
        except Exception as e:
            if "not found" in str(e).lower() or "nosuch" in str(e).lower():
                logger.info(f" Table {table_id} not found, returning empty result")
                warehouse_metrics.record_query(
                    logs_returned=0,
                    duration_seconds=time.time() - start_time,
                    error=False,
                )
                import pyarrow as pa

                return pa.table(
                    {
                        "log_group_name": [],
                        "log_stream_name": [],
                        "timestamp": [],
                        "message": [],
                        "ingestion_time": [],
                        "sequence_token": [],
                    }
                )
            raise

        try:
            print(
                f"Running query on {target_table_name} filter_expression={filter_expression is not None} filter_expr={filter_expr}"
            )
            # Build scan with optional column projection
            scan_kwargs = {}
            if selected_fields:
                scan_kwargs["selected_fields"] = selected_fields
            scan = table_obj.scan(**scan_kwargs)

            # Prefer typed Expression objects (push-down into Parquet row-group stats)
            if filter_expression is not None:
                scan = scan.filter(filter_expression)
            elif filter_expr:
                scan = scan.filter(filter_expr)

            # Hard cap per tier — prevents OOM on long time-range queries (e.g. 24 h).
            # Even without an explicit limit, never materialise more than MAX_SCAN_ROWS
            # rows per tier; after merge+sort the result is still sliced to `limit`.
            MAX_SCAN_ROWS = 500_000
            effective_limit = limit if (limit and limit > 0) else MAX_SCAN_ROWS

            # Use batch reader for early stop — avoids downloading all S3 files into RAM
            s3_result = self._scan_to_arrow_with_limit(scan, effective_limit)

            # Also read local staging Parquet files so data is visible before compaction.
            import pyarrow as pa
            import pyarrow.compute as pc

            # ---- Cold tier: local Parquet data, PostgreSQL catalog metadata ----
            cold_result: Optional[pa.Table] = None
            try:
                # Acquire the lock only for the catalog metadata lookup so that
                # Phase-2 compaction (which holds _cold_lock during drop_table +
                # catalog reset) cannot race with us.  The actual Parquet scan
                # happens outside the lock to avoid blocking compaction I/O.
                with self._cold_lock:
                    self._ensure_cold_table(target_table_name)
                    cold_tbl = self.cold_catalog.load_table(f"{self.COLD_NAMESPACE}.{target_table_name}")
                cold_scan = cold_tbl.scan(**scan_kwargs)
                if filter_expression is not None:
                    cold_scan = cold_scan.filter(filter_expression)
                elif filter_expr:
                    cold_scan = cold_scan.filter(filter_expr)
                cold_data = self._scan_to_arrow_with_limit(cold_scan, effective_limit)
                if cold_data.num_rows > 0:
                    cold_result = cold_data
            except Exception as cold_err:
                logger.debug(f"[Cold] Read skipped: {cold_err}")

            # ---- Hot tier: WAL JSONL files ----
            import json
            wal_result: Optional[pa.Table] = None
            try:
                wal_dir = self.local_staging_dir / target_table_name / "wal"
                wal_logs = []
                # Cap WAL rows read per query to avoid OOM when WAL is large.
                # We read the newest files first (reverse sort) so recent data is
                # always visible even when older files push past the cap.
                # Never exceed 100k from WAL — hot tier shouldn't hold huge datasets.
                wal_cap = min(max(effective_limit, 10_000), 100_000)
                if wal_dir.exists():
                    for f in sorted(wal_dir.glob("wal_*.jsonl"), reverse=True):
                        if len(wal_logs) >= wal_cap:
                            break
                        try:
                            with open(f) as fp:
                                for line in fp:
                                    line = line.strip()
                                    if line:
                                        wal_logs.append(json.loads(line))
                                        if len(wal_logs) >= wal_cap:
                                            break
                        except Exception:
                            pass
                if wal_logs:
                    wal_tbl = self._logs_to_arrow(wal_logs)
                    wal_tbl = self._filter_arrow_table(wal_tbl, filter_expression)
                    if wal_tbl.num_rows > 0:
                        wal_result = wal_tbl
            except Exception as wal_err:
                logger.debug(f"[WAL] Read skipped: {wal_err}")

            # ---- Merge all 3 tiers ----
            # Normalise string types: S3/PostgreSQL catalog returns large_utf8,
            # cold (SQLite) and WAL (Arrow) return utf8. Cast to S3 schema.
            target_schema = s3_result.schema if s3_result.num_columns > 0 else None

            def _norm(tbl):
                if tbl is None or target_schema is None or tbl.schema == target_schema:
                    return tbl
                try:
                    return tbl.cast(target_schema)
                except Exception:
                    return tbl

            parts = [s3_result]
            if cold_result is not None and cold_result.num_rows > 0:
                parts.append(_norm(cold_result))
            if wal_result is not None and wal_result.num_rows > 0:
                parts.append(_norm(wal_result))

            if len(parts) > 1:
                try:
                    combined = pa.concat_tables(parts, promote_options="default")
                except Exception:
                    combined = s3_result
                sort_idx = pc.sort_indices(combined, sort_keys=[("timestamp", "ascending")])
                result = combined.take(sort_idx)
                if limit and limit > 0 and len(result) > limit:
                    result = result.slice(0, limit)
            else:
                result = s3_result

            logs_returned = len(result)
            print(
                f"Query returned {logs_returned} rows from {target_table_name} "
                f"(archive={s3_result.num_rows}, cold={cold_result.num_rows if cold_result else 0}, wal={wal_result.num_rows if wal_result else 0})"
            )

            # Record successful query
            duration = time.time() - start_time
            warehouse_metrics.record_query(
                logs_returned=logs_returned, duration_seconds=duration, error=False
            )
            return result
        except FileNotFoundError as e:
            logger.info(f" Data file not found, rebuilding table: {e}")
            warehouse_metrics.record_query(
                logs_returned=0, duration_seconds=time.time() - start_time, error=True
            )
            # self._repair_missing_data_files() # Needs update for multi-table
            import pyarrow as pa

            return pa.table(
                {
                    "log_group_name": [],
                    "log_stream_name": [],
                    "timestamp": [],
                    "message": [],
                    "ingestion_time": [],
                    "sequence_token": [],
                }
            )
        except Exception as e:
            warehouse_metrics.record_query(
                logs_returned=0, duration_seconds=time.time() - start_time, error=True
            )
            raise

        return result

    def get_log_groups(self):
        now = time.time()
        if self._log_groups_cache is not None and now - self._log_groups_cache_time < 30.0:
            return self._log_groups_cache

        # Column projection: only fetch the 2 columns we need — avoids reading message payloads
        result = self.query(
            limit=10000,
            selected_fields=("log_group_name", "ingestion_time"),
        )
        col_group = result.column("log_group_name").to_pylist()
        col_ingestion = result.column("ingestion_time").to_pylist()

        groups = {}
        for i, group_name in enumerate(col_group):
            if group_name and group_name not in groups:
                ts = col_ingestion[i]
                groups[group_name] = {
                    "logGroupName": group_name,
                    "creationTime": int(ts.timestamp() * 1000) if ts else 0,
                    "metricFilterCount": 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}",
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

        # Expression API + column projection: avoids full-table scan and message reads
        result = self.query(
            filter_expression=EqualTo("log_group_name", log_group_name),
            limit=10000,
            selected_fields=("log_stream_name", "ingestion_time"),
        )
        col_stream = result.column("log_stream_name").to_pylist()
        col_ingestion = result.column("ingestion_time").to_pylist()

        streams = {}
        for i, stream_name in enumerate(col_stream):
            if stream_name and stream_name not in streams:
                ts = col_ingestion[i]
                streams[stream_name] = {
                    "logStreamName": stream_name,
                    "creationTime": int(ts.timestamp() * 1000) if ts else 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-stream:{log_group_name}/{stream_name}",
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
        # Build typed Expression objects — pushed down into Parquet row-group statistics
        # for true predicate push-down, unlike string-based filters.
        filter_expression = AlwaysTrue()

        if log_group_name:
            filter_expression = And(filter_expression, EqualTo("log_group_name", log_group_name))
        if log_stream_name:
            filter_expression = And(filter_expression, EqualTo("log_stream_name", log_stream_name))
        if start_time:
            # PyIceberg 0.7 requires TimestampLiteral (microseconds since epoch)
            filter_expression = And(filter_expression, GreaterThanOrEqual("timestamp", TimestampLiteral(int(start_time) * 1000)))
        if end_time:
            filter_expression = And(filter_expression, LessThanOrEqual("timestamp", TimestampLiteral(int(end_time) * 1000)))
        if labels_filter:
            for k, v in labels_filter.items():
                label_mapping = {"service_name": "service"}
                mapped_key = label_mapping.get(k, k)
                filter_expression = And(filter_expression, EqualTo(f"label_{mapped_key}", v))

        result = self.query(filter_expression=filter_expression, limit=limit, table_name=table_name)

        ts_col = result.column("timestamp").to_pylist()
        msg_col = result.column("message").to_pylist()
        ingest_col = result.column("ingestion_time").to_pylist()
        try:
            lg_col = result.column("log_group_name").to_pylist()
        except Exception:
            lg_col = [None] * result.num_rows
        try:
            ls_col = result.column("log_stream_name").to_pylist()
        except Exception:
            ls_col = [None] * result.num_rows

        label_columns = self._get_label_columns()
        label_data = {}
        for label_col in label_columns:
            try:
                label_data[label_col] = result.column(f"label_{label_col}").to_pylist()
            except Exception:
                label_data[label_col] = [None] * result.num_rows

        import calendar

        def _naive_utc_to_ms(dt) -> int:
            """Convert a tz-naive datetime stored as UTC to epoch milliseconds."""
            if dt is None:
                return 0
            return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000

        events = []
        for i in range(result.num_rows):
            ts = ts_col[i]
            ts_ms = _naive_utc_to_ms(ts)
            event = {
                "timestamp": ts_ms,
                "message": msg_col[i] or "",
                "ingestionTime": _naive_utc_to_ms(ingest_col[i]),
                "logGroupName": lg_col[i] or "",
                "logStreamName": ls_col[i] or "",
            }
            for label_col, values in label_data.items():
                event[f"label_{label_col}"] = values[i] or ""
            events.append(event)

        events.sort(key=lambda x: x["timestamp"])
        return events[:limit] if limit else events
