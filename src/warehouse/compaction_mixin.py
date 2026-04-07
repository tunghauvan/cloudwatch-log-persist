import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.catalog import load_catalog

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class CompactionMixin:
    """Compaction operations for CloudWatch and ALB tables."""

    def compact(self, table_name=None):
        """
        3-tier compaction — low-memory streaming variant.

        Phase 1 (hot -> cold): flush WAL in 10k-row batches (see flush_wal).
        Phase 2 (cold -> S3) : stream cold Iceberg via batch_reader -> sort in
                               SORT_BATCH_SIZE chunks -> S3 append; drop cold after.
        Phase 3 (S3 merge)  : only when data_files > S3_MERGE_THRESHOLD (4).
                               Streams per-day-partition; never loads more than
                               one partition into RAM at a time.
        """
        from service.services.warehouse_metrics import warehouse_metrics

        if not self.compaction_enabled:
            return {"status": "skipped", "reason": "Compaction is disabled in config"}

        if not PYICEBERG_AVAILABLE:
            return {"status": "skipped", "reason": "PyIceberg not available"}

        target_table  = table_name or self.table_name
        s3_table_id   = f"{self.namespace}.{target_table}"
        cold_table_id = f"{self.COLD_NAMESPACE}.{target_table}"

        try:
            warehouse_metrics.record_compaction()
            import pyarrow as pa
            import pyarrow.compute as pc

            # ------------------------------------------------------------------
            # Phase 1: WAL -> cold Iceberg (streamed, 10k rows / batch)
            # ------------------------------------------------------------------
            wal_flushed = self.flush_wal(target_table)
            logger.info(
                f"[Compaction] Phase 1 done: flushed {wal_flushed} WAL rows -> cold for {target_table}"
            )

            # ------------------------------------------------------------------
            # Phase 2: cold Iceberg -> S3 (streaming batch reader, no full load)
            # ------------------------------------------------------------------
            cold_rows = self._cold_row_count(target_table)
            logger.info(
                f"[Compaction] Phase 2: {cold_rows} cold rows to push to S3 {s3_table_id}"
            )

            rows_pushed = 0
            SORT_BATCH = 50_000   # rows per sort-buffer chunk

            if cold_rows > 0:
                with self._cold_lock:
                    try:
                        cold_tbl   = self.cold_catalog.load_table(cold_table_id)
                        s3_tbl_obj = self.catalog.load_table(s3_table_id)

                        pending: list = []
                        pending_rows  = 0

                        def _push_pending():
                            nonlocal rows_pushed
                            if not pending:
                                return
                            chunk = pa.concat_tables(pending, promote_options="default")
                            pending.clear()
                            idx = pc.sort_indices(chunk, sort_keys=[("timestamp", "ascending")])
                            s3_tbl_obj.append(chunk.take(idx))
                            rows_pushed += chunk.num_rows

                        for rb in cold_tbl.scan().to_arrow_batch_reader():
                            if rb.num_rows == 0:
                                continue
                            pending.append(pa.Table.from_batches([rb]))
                            pending_rows += rb.num_rows
                            if pending_rows >= SORT_BATCH:
                                _push_pending()
                                pending_rows = 0

                        _push_pending()  # flush remainder
                        logger.info(f"[Compaction] Pushed {rows_pushed} rows to S3 {s3_table_id}")

                        # Drop cold table to free local disk + catalog rows
                        try:
                            self.cold_catalog.drop_table(cold_table_id)
                            self._cold_catalog = None
                            logger.info(f"[Compaction] Cleared cold Iceberg for {target_table}")
                        except Exception as drop_err:
                            logger.warning(f"[Compaction] Could not drop cold table: {drop_err}")

                    except Exception as p2_err:
                        logger.error(
                            f"[Compaction] Phase 2 error: {type(p2_err).__name__}: {p2_err}"
                        )

            # ------------------------------------------------------------------
            # Phase 3: S3 in-place merge — only when heavily fragmented
            # Skip entirely when <= S3_MERGE_THRESHOLD data files (common case).
            # Process one day-partition at a time; explicit del after overwrite.
            # ------------------------------------------------------------------
            S3_MERGE_THRESHOLD = 4   # data files before we bother merging
            s3_compacted = 0
            try:
                s3_tbl_obj = self.catalog.load_table(s3_table_id)
                snapshot   = s3_tbl_obj.current_snapshot()
                if snapshot is None:
                    return {
                        "status": "success",
                        "rows_pushed_from_staging": rows_pushed,
                        "rows_merged_on_s3": 0,
                    }

                manifests = snapshot.manifests(s3_tbl_obj.io)
                data_file_count = sum(
                    m.existing_files_count + m.added_files_count for m in manifests
                )
                logger.info(
                    f"[Compaction] S3 table has ~{data_file_count} data files"
                    f" across {len(manifests)} manifests"
                )

                if self.s3_merge_enabled and data_file_count > S3_MERGE_THRESHOLD:
                    # Collect unique partition day values (metadata only, no data)
                    partition_days: set = set()
                    for manifest in manifests:
                        try:
                            for entry in manifest.fetch_manifest_entry(s3_tbl_obj.io):
                                try:
                                    day_val = entry.data_file.partition[0]
                                    if day_val is not None:
                                        partition_days.add(int(day_val))
                                except (IndexError, TypeError, AttributeError):
                                    pass
                        except Exception as m_err:
                            logger.warning(f"[Compaction] Manifest read error: {m_err}")

                    if partition_days:
                        for day_val in sorted(partition_days):
                            try:
                                day_start = datetime.fromtimestamp(
                                    day_val * 86400, tz=timezone.utc
                                ).replace(tzinfo=None)
                                day_end = datetime.fromtimestamp(
                                    (day_val + 1) * 86400, tz=timezone.utc
                                ).replace(tzinfo=None)
                                day_filter = (
                                    "ingestion_time >= '" + day_start.isoformat() + "'"
                                    " AND ingestion_time < '" + day_end.isoformat() + "'"
                                )
                                # Stream partition into memory (one day at a time)
                                part_batches: list = []
                                for rb in s3_tbl_obj.scan().filter(day_filter).to_arrow_batch_reader():
                                    if rb.num_rows > 0:
                                        part_batches.append(pa.Table.from_batches([rb]))
                                if not part_batches:
                                    continue
                                part_data = pa.concat_tables(part_batches, promote_options="default")
                                del part_batches
                                si = pc.sort_indices(part_data, sort_keys=[("timestamp", "ascending")])
                                s3_tbl_obj.overwrite(part_data.take(si), overwrite_filter=day_filter)
                                s3_compacted += part_data.num_rows
                                logger.info(
                                    f"[Compaction] Merged partition day={day_val}: {part_data.num_rows} rows"
                                )
                                del part_data  # free ASAP before next partition
                            except Exception as part_err:
                                logger.warning(f"[Compaction] Partition day={day_val}: {part_err}")
                    else:
                        # No partitions — stream full table, sort, overwrite
                        all_batches: list = []
                        for rb in s3_tbl_obj.scan().to_arrow_batch_reader():
                            if rb.num_rows > 0:
                                all_batches.append(pa.Table.from_batches([rb]))
                        if all_batches:
                            all_data = pa.concat_tables(all_batches, promote_options="default")
                            del all_batches
                            si = pc.sort_indices(all_data, sort_keys=[("timestamp", "ascending")])
                            s3_tbl_obj.overwrite(all_data.take(si))
                            s3_compacted = all_data.num_rows
                            del all_data

            except Exception as s3_err:
                logger.warning(f"[Compaction] S3 in-place compaction error: {s3_err}")
                s3_compacted = 0

            logger.info(
                f"[Compaction] Done for {s3_table_id}:"
                f" pushed={rows_pushed} s3_merged={s3_compacted}"
            )

            # ---- Snapshot expiration: remove old Avro manifests from S3 ----
            # This is separate from `write.metadata.delete-after-commit` which only
            # removes old .metadata.json files.  Manifest files (.avro) are only
            # reclaimed when the snapshot that references them is expired.
            # Keep the current snapshot + the one immediately before it (for
            # rollback safety); expire everything older.
            try:
                s3_tbl_refresh = self.catalog.load_table(s3_table_id)
                snapshots = s3_tbl_refresh.snapshots()
                if len(snapshots) > 2:
                    # Sort ascending by timestamp, keep the 2 most recent
                    sorted_snaps = sorted(snapshots, key=lambda s: s.timestamp_ms)
                    cutoff_ms = sorted_snaps[-2].timestamp_ms  # keep last 2
                    cutoff_dt = datetime.fromtimestamp(
                        cutoff_ms / 1000.0, tz=timezone.utc
                    )
                    s3_tbl_refresh.maintenance.expire_snapshots().older_than(
                        cutoff_dt
                    ).commit()
                    expired_count = len(sorted_snaps) - 2
                    logger.info(
                        f"[Compaction] Expired {expired_count} old snapshot(s) for {s3_table_id}"
                    )
                    # GC orphaned Avro files in metadata/ that are no longer
                    # referenced by any live snapshot.
                    s3_tbl_refresh = self.catalog.load_table(s3_table_id)
                    gc_deleted = self._gc_metadata_orphans(s3_tbl_refresh, target_table)
                    if gc_deleted:
                        logger.info(
                            f"[Compaction] GC: deleted {gc_deleted} orphan metadata file(s)"
                        )
            except Exception as exp_err:
                logger.warning(f"[Compaction] expire_snapshots skipped: {exp_err}")

            return {
                "status": "success",
                "rows_pushed_from_staging": rows_pushed,
                "rows_merged_on_s3": s3_compacted,
            }

        except Exception as e:
            logger.error(f"[Compaction] Error: {type(e).__name__}: {str(e)[:200]}")
            return {"status": "error", "message": f"{type(e).__name__}: {str(e)[:200]}"}

    def compact_alb(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Daily compaction for the ALB Iceberg table.

        For each ``event_day`` partition that contains more than
        ``alb_compaction_min_files`` data files:
          1. Read the entire partition into memory (one day at a time).
          2. Sort by ``time`` ascending.
          3. Atomically replace the partition via
             ``table.dynamic_partition_overwrite()`` — this produces a single
             compact Parquet file for that day and removes the old small files.

        Partitions for today (UTC) are skipped because they may still be
        receiving new data from the SQS consumer.
        """
        if not PYICEBERG_AVAILABLE:
            return {"status": "skipped", "reason": "PyIceberg not available"}

        target = table_name or self.alb_table_name
        table_id = f"{self.namespace}.{target}"

        try:
            tbl = self.catalog.load_table(table_id)
        except Exception as e:
            logger.debug(f"[ALB Compaction] Table {table_id} not found: {e}")
            return {"status": "skipped", "reason": f"Table not found: {e}"}

        snapshot = tbl.current_snapshot()
        if snapshot is None:
            return {"status": "skipped", "reason": "No snapshot yet"}

        import pyarrow as pa
        from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

        # ---- collect file counts per day partition (metadata only) ----
        partition_file_counts: Dict[int, int] = {}
        try:
            for manifest in snapshot.manifests(tbl.io):
                for entry in manifest.fetch_manifest_entry(tbl.io):
                    try:
                        day_val = entry.data_file.partition[0]
                        if day_val is not None:
                            partition_file_counts[int(day_val)] = (
                                partition_file_counts.get(int(day_val), 0) + 1
                            )
                    except (IndexError, TypeError, AttributeError):
                        pass
        except Exception as e:
            logger.warning(f"[ALB Compaction] Manifest scan error: {e}")
            return {"status": "error", "message": str(e)}

        if not partition_file_counts:
            return {"status": "ok", "partitions_compacted": 0, "rows_rewritten": 0}

        today_day = int(datetime.now(timezone.utc).timestamp() // 86400)
        compacted_partitions = 0
        rows_rewritten = 0

        for day_val in sorted(partition_file_counts):
            file_count = partition_file_counts[day_val]

            # Skip today's partition — may still be receiving new data
            if day_val >= today_day:
                logger.debug(
                    f"[ALB Compaction] Skipping day={day_val} (today or future)"
                )
                continue

            if file_count < self.alb_compaction_min_files:
                logger.debug(
                    f"[ALB Compaction] day={day_val}: {file_count} file(s) — nothing to do"
                )
                continue

            day_start = datetime(
                *time.gmtime(day_val * 86400)[:6], tzinfo=None
            )  # naive UTC
            day_end = datetime(
                *time.gmtime((day_val + 1) * 86400)[:6], tzinfo=None
            )
            day_str = day_start.strftime("%Y-%m-%d")

            logger.info(
                f"[ALB Compaction] Compacting day={day_str}: {file_count} files"
            )

            try:
                row_filter = And(
                    GreaterThanOrEqual("time", day_start.isoformat()),
                    LessThan("time", day_end.isoformat()),
                )
                batches = [
                    pa.Table.from_batches([rb])
                    for rb in tbl.scan(row_filter=row_filter).to_arrow_batch_reader()
                    if rb.num_rows > 0
                ]
                if not batches:
                    logger.debug(
                        f"[ALB Compaction] day={day_str}: scan returned no rows — skipping"
                    )
                    continue

                merged = pa.concat_tables(batches, promote_options="default")
                del batches
                # Sort by time for optimal query performance
                merged = merged.sort_by([("time", "ascending")])

                # Reload table to use latest snapshot before overwriting
                tbl = self.catalog.load_table(table_id)
                n_rows = merged.num_rows
                tbl.overwrite(merged, overwrite_filter=row_filter)
                del merged
                rows_rewritten += n_rows
                compacted_partitions += 1

                logger.info(
                    f"[ALB Compaction] day={day_str}: merged {file_count} files "
                    f"-> 1 file ({n_rows} rows)"
                )
            except Exception as part_err:
                logger.warning(
                    f"[ALB Compaction] day={day_str} error: {part_err}"
                )

        logger.info(
            f"[ALB Compaction] Done: {compacted_partitions} partitions compacted, "
            f"{rows_rewritten} rows rewritten"
        )

        # ---- expire old snapshots + delete orphan files ----
        if compacted_partitions > 0:
            tbl = self.catalog.load_table(table_id)
            orphans_deleted = self._expire_and_gc_alb(tbl, table_id)
            logger.info(
                f"[ALB Compaction] GC: deleted {orphans_deleted} orphan file(s)"
            )
        else:
            orphans_deleted = 0

        return {
            "status": "ok",
            "partitions_compacted": compacted_partitions,
            "rows_rewritten": rows_rewritten,
            "orphan_files_deleted": orphans_deleted,
        }

    def _expire_and_gc_alb(self, tbl, table_id: str) -> int:
        """
        1. Expire all snapshots older than now (retain the current one).
        2. Collect the set of data files referenced by the surviving snapshot.
        3. Delete every data file in the table's data/ prefix that is NOT in
           that live set (orphan files left over from pre-compaction appends).

        Returns the number of orphan files deleted.
        """
        # Step 1 — expire snapshots
        try:
            tbl.maintenance.expire_snapshots().older_than(
                datetime.now(timezone.utc)
            ).commit()
            tbl = self.catalog.load_table(table_id)
            logger.debug(
                f"[ALB GC] Snapshots after expire: {len(tbl.snapshots())}"
            )
        except Exception as e:
            logger.warning(f"[ALB GC] expire_snapshots failed: {e}")

        # Step 2 — collect live data file paths
        live_files: set[str] = set()
        snap = tbl.current_snapshot()
        if snap is None:
            return 0
        try:
            for mf in snap.manifests(tbl.io):
                for entry in mf.fetch_manifest_entry(tbl.io):
                    if entry.status.value != 2:  # 2 = DELETED
                        # Strip s3a:// scheme and bucket name → S3 object key
                        raw = entry.data_file.file_path
                        # e.g. s3a://stag-log-warehouse/default/alb_logs/...
                        without_scheme = raw.split("://", 1)[-1]  # stag-log-warehouse/default/...
                        key = without_scheme.split("/", 1)[-1]      # default/alb_logs/...
                        live_files.add(key)
        except Exception as e:
            logger.warning(f"[ALB GC] manifest scan failed: {e}")
            return 0

        # Step 3 — list & delete orphan data files via S3
        try:
            import boto3
            import yaml as _yaml
            with open(self._config_path) as fh:
                _cfg = _yaml.safe_load(fh)
            s3_cfg = _cfg.get("s3", {})
            s3 = boto3.client(
                "s3",
                endpoint_url=s3_cfg.get("endpoint"),
                region_name=s3_cfg.get("region", "us-east-1"),
                aws_access_key_id=s3_cfg.get("access_key"),
                aws_secret_access_key=s3_cfg.get("secret_key"),
            )
            # Derive bucket from warehouse URI, e.g. s3a://stag-log-warehouse/
            warehouse_uri = _cfg.get("warehouse", "")
            bucket = warehouse_uri.split("://", 1)[-1].rstrip("/").split("/")[0]
            namespace = self.namespace
            table_data_prefix = f"{namespace}/{self.alb_table_name}/data/"

            paginator = s3.get_paginator("list_objects_v2")
            orphans: list[str] = []
            for page in paginator.paginate(Bucket=bucket, Prefix=table_data_prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"] not in live_files:
                        orphans.append(obj["Key"])

            if not orphans:
                return 0

            deleted = 0
            for i in range(0, len(orphans), 1000):
                batch = [{"Key": k} for k in orphans[i : i + 1000]]
                resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted += len(resp.get("Deleted", []))
                for err in resp.get("Errors", []):
                    logger.warning(f"[ALB GC] delete error: {err}")

            logger.info(
                f"[ALB GC] Deleted {deleted} orphan file(s) from s3://{bucket}/{table_data_prefix}"
            )
            return deleted
        except Exception as e:
            logger.warning(f"[ALB GC] S3 orphan cleanup failed: {e}")
            return 0
