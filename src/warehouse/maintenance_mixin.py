import ctypes
import ctypes.util
import gc
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.catalog import load_catalog

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class MaintenanceMixin:
    """Background maintenance threads, retention, stats, repair, and cleanup."""

    def enforce_retention(self):
        if not self.retention_enabled:
            return {"status": "skipped", "reason": "disabled"}

        if not PYICEBERG_AVAILABLE:
            return {"status": "skipped", "reason": "PyIceberg not available"}

        try:
            from service.services.warehouse_metrics import warehouse_metrics
            import pyarrow as pa

            table = self.get_table()
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
            cutoff_naive = cutoff.replace(tzinfo=None)
            cutoff_day = int(cutoff.timestamp() // 86400)

            logger.info(
                f"[Retention] Removing data older than {self.retention_days} days "
                f"(cutoff: {cutoff_naive})"
            )

            snapshot = table.current_snapshot()
            if snapshot is None:
                return {"status": "success", "removed_partitions": 0}

            # Collect old partition day values from Iceberg manifest metadata —
            # no data files are read, only tiny manifest JSON.
            old_days: set = set()
            try:
                for manifest in snapshot.manifests(table.io):
                    for entry in manifest.fetch_manifest_entry(table.io):
                        try:
                            day_val = entry.data_file.partition[0]
                            if day_val is not None and int(day_val) < cutoff_day:
                                old_days.add(int(day_val))
                        except (IndexError, TypeError, AttributeError):
                            pass
            except Exception as e:
                logger.warning(f"[Retention] Manifest scan error: {e}")
                return {"status": "error", "error": str(e)}

            if not old_days:
                warehouse_metrics.record_retention(removed_partitions=0)
                return {"status": "success", "removed_partitions": 0}

            # Build an empty PyArrow table with the correct schema by consulting
            # the Iceberg batch reader's schema attribute (zero data I/O).
            try:
                pa_schema = table.scan().to_arrow_batch_reader().schema
            except Exception:
                from pyiceberg.io.pyarrow import schema_to_pyarrow
                pa_schema = schema_to_pyarrow(table.schema())
            empty_tbl = pa.Table.from_batches([], schema=pa_schema)

            removed = 0
            for day_val in sorted(old_days):
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
                try:
                    # Reload table to always operate on latest snapshot
                    tbl = self.catalog.load_table(f"{self.namespace}.{self.table_name}")
                    tbl.overwrite(empty_tbl, overwrite_filter=day_filter)
                    removed += 1
                    logger.info(
                        f"[Retention] Removed partition ingestion_day={day_val} "
                        f"({day_start.date()})"
                    )
                except Exception as e:
                    logger.warning(
                        f"[Retention] Could not remove partition day={day_val}: {e}"
                    )

            # Expire old snapshots so physical storage is reclaimed
            if removed > 0:
                try:
                    tbl = self.catalog.load_table(f"{self.namespace}.{self.table_name}")
                    tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
                except Exception as e:
                    logger.warning(f"[Retention] expire_snapshots skipped: {e}")

            warehouse_metrics.record_retention(removed_partitions=removed)
            return {"status": "success", "removed_partitions": removed}
        except Exception as e:
            logger.error(f"[Retention] Error: {e}")
            return {"status": "error", "error": str(e)}

    def get_stats(self) -> Dict[str, Any]:
        if not PYICEBERG_AVAILABLE:
            return {"status": "unavailable"}

        try:
            table = self.get_table()
            snapshot = table.current_snapshot()

            # --- Archive (S3/Iceberg) stats ---
            archive_files = 0
            archive_rows = 0
            try:
                if snapshot is not None:
                    manifests = snapshot.manifests(table.io)
                    archive_files = sum(
                        m.existing_files_count + m.added_files_count for m in manifests
                    )
                    # Use snapshot summary metadata — zero RAM, zero S3 I/O.
                    summary = snapshot.summary
                    if summary and "total-records" in summary.additional_properties:
                        archive_rows = int(summary.additional_properties["total-records"])
                    else:
                        # Fallback: aggregate from manifest entry counts (still no data read)
                        archive_rows = archive_files  # rough lower-bound; better than full scan
            except Exception:
                pass

            # --- Cold (local Iceberg) stats ---
            cold_rows: Dict[str, int] = {}
            for tname in (self.table_name, self.loki_table_name):
                cold_rows[tname] = self._cold_row_count(tname)

            # --- Hot (WAL) stats ---
            wal_rows: Dict[str, int] = {}
            wal_files: Dict[str, int] = {}
            for tname in (self.table_name, self.loki_table_name):
                wal_dir = self.local_staging_dir / tname / "wal"
                files = list(wal_dir.glob("wal_*.jsonl")) if wal_dir.exists() else []
                wal_files[tname] = len(files)
                wal_rows[tname] = self._wal_row_count(tname)

            total_unstaged = sum(wal_rows.values()) + sum(cold_rows.values())

            return {
                "archive": {
                    "table": f"{self.namespace}.{self.table_name}",
                    "location": table.location(),
                    "data_files": archive_files,
                    "rows": archive_rows,
                    "compaction_enabled": self.compaction_enabled,
                    "compaction_interval_seconds": self.compaction_interval,
                    "retention_enabled": self.retention_enabled,
                    "retention_days": self.retention_days,
                },
                "cold": {tname: {"rows": cold_rows[tname]} for tname in cold_rows},
                "hot_wal": {
                    tname: {"rows": wal_rows[tname], "files": wal_files[tname]}
                    for tname in wal_rows
                },
                "backpressure": {
                    "total_unstaged_rows": total_unstaged,
                    "limit": self.wal_max_rows,
                    "pct_full": round(total_unstaged / max(self.wal_max_rows, 1) * 100, 1),
                },
            }
        except Exception as e:
            return {"error": str(e)}

    def cleanup_metadata(self):
        """
        Clean up old Iceberg metadata snapshots.
        Keep only the most recent snapshots and delete old ones.
        """
        try:
            import boto3

            logger.info(f"[Metadata Cleanup] Starting for {self.namespace}.{self.table_name}")

            # Derive bucket from warehouse URI (e.g. s3://bucket/path or s3a://bucket/path)
            bucket = (
                self.warehouse_path.split("://", 1)[-1].rstrip("/").split("/")[0]
            )

            s3 = boto3.client("s3", **self._build_s3_client_config())

            # List all metadata files
            prefix = f"{self.namespace}/{self.table_name}/metadata/"
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if "Contents" not in response:
                logger.info(f"[Metadata Cleanup] No metadata files found")
                return {"status": "skipped", "reason": "no_metadata"}

            metadata_files = [obj["Key"] for obj in response["Contents"]]

            # Keep only latest 50 snapshots, delete the rest
            keep_count = 50
            if len(metadata_files) > keep_count:
                # Sort files - newer ones should be at the end (by number)
                to_delete = metadata_files[:-keep_count]

                delete_count = 0
                for key_to_delete in to_delete:
                    try:
                        s3.delete_object(Bucket=bucket, Key=key_to_delete)
                        logger.debug(f"[Metadata Cleanup] Deleted: {key_to_delete}")
                        delete_count += 1
                    except Exception as e:
                        logger.warning(f"[Metadata Cleanup] Could not delete {key_to_delete}: {e}")

                logger.info(f"[Metadata Cleanup] Deleted {delete_count} old metadata files, kept {keep_count}")
                return {
                    "status": "success",
                    "message": f"Cleaned up old metadata snapshots",
                    "deleted": delete_count,
                    "kept": keep_count
                }
            else:
                logger.info(f"[Metadata Cleanup] Only {len(metadata_files)} snapshots, no cleanup needed")
                return {"status": "skipped", "reason": "below_threshold"}

        except Exception as e:
            logger.error(f"[Metadata Cleanup] Error: {e}")
            return {"status": "error", "message": str(e)}

    def _gc_metadata_orphans(self, tbl, table_name: str) -> int:
        """Delete Avro manifest/manifest-list files in the table's metadata/ prefix
        that are no longer referenced by any live snapshot.

        PyIceberg's expire_snapshots().commit() updates the metadata JSON to remove
        old snapshot references but does not always delete the physical Avro files
        (manifest lists + manifest files) in S3.  This method does that explicitly.

        Safe to call any time; it only deletes files whose paths do not appear in
        any manifest list of any surviving snapshot.

        Returns the number of files deleted.
        """
        if not self.warehouse_path.startswith("s3"):
            return 0  # local FS — nothing to GC via boto3

        try:
            import boto3

            # Collect all Avro file paths still referenced by live snapshots.
            live_manifest_paths: set = set()
            for snap in tbl.snapshots():
                # snapshot.manifest_list is the manifest-list (.avro) file path
                if snap.manifest_list:
                    live_manifest_paths.add(snap.manifest_list)
                # Each manifest list references a set of manifest files
                try:
                    for mf in snap.manifests(tbl.io):
                        live_manifest_paths.add(mf.manifest_path)
                except Exception:
                    pass

            # Build the set of S3 object keys (strip scheme + bucket prefix)
            def _to_key(uri: str) -> str:
                without_scheme = uri.split("://", 1)[-1]   # bucket/path
                return without_scheme.split("/", 1)[-1]     # path only

            live_keys: set = {_to_key(p) for p in live_manifest_paths if p}

            bucket = (
                self.warehouse_path.split("://", 1)[-1].rstrip("/").split("/")[0]
            )
            s3 = boto3.client("s3", **self._build_s3_client_config())
            metadata_prefix = f"{self.namespace}/{table_name}/metadata/"

            orphans: list = []
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=metadata_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Only target Avro files (.avro), leave .metadata.json to
                    # write.metadata.delete-after-commit.
                    if key.endswith(".avro") and key not in live_keys:
                        orphans.append(key)

            if not orphans:
                return 0

            deleted = 0
            for key in orphans:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                    deleted += 1
                    logger.debug(f"[GC] Deleted orphan: {key}")
                except Exception as e:
                    logger.warning(f"[GC] Could not delete {key}: {e}")

            return deleted
        except Exception as e:
            logger.warning(f"[GC] _gc_metadata_orphans error: {e}")
            return 0

    @staticmethod
    def _trim_memory():
        """Return free pages to the OS after a compaction cycle.

        PyArrow uses jemalloc/mimalloc as its C++ memory pool. After bulk
        Arrow buffers are deleted Python releases the objects but the C++
        allocator keeps the pages in its own free-list — RSS stays inflated.
        Calling malloc_trim(0) on Linux forces glibc (and the jemalloc shim)
        to punch-hole those empty pages back to the OS, visibly dropping RSS.
        gc.collect() is called first to ensure all Python-level references are
        dropped before we trim.
        """
        gc.collect()
        try:
            libc_name = ctypes.util.find_library("c")
            if libc_name:
                libc = ctypes.CDLL(libc_name)
                if hasattr(libc, "malloc_trim"):
                    libc.malloc_trim(0)
        except Exception:
            pass  # not available on macOS/Windows — silently skip

    def _compaction_loop(self):
        """Flush cold → S3 every compaction_interval seconds.
        Waits 60 s after startup before the first run so the JVM / PyArrow
        allocators settle and the memory baseline is accurate.
        """
        # 60-second grace period: let the service finish initialising before
        # the first heavy compaction (avoids a double RAM spike at boot).
        if self._stop_event.wait(timeout=60):
            return
        try:
            self.compact(table_name=self.table_name)
            self.compact(table_name=self.loki_table_name)
        except Exception as e:
            logger.warning(f"[Compaction] Initial run error: {e}")
        finally:
            self._trim_memory()
        while not self._stop_event.wait(timeout=self.compaction_interval):
            try:
                self.compact(table_name=self.table_name)
                self.compact(table_name=self.loki_table_name)
            except Exception as e:
                logger.warning(f"[Compaction] Loop error: {e}")
            finally:
                self._trim_memory()

    def _wal_flush_loop(self):
        """Flush WAL → cold Iceberg on a short interval (default 30s).
        Keeps the WAL small and makes data available to Iceberg filter push-down sooner.
        """
        while not self._stop_event.wait(timeout=self._wal_flush_interval):
            for tname in (self.table_name, self.loki_table_name):
                try:
                    flushed = self.flush_wal(tname)
                    if flushed:
                        logger.debug(f"[WAL flush] {flushed} rows → cold for {tname}")
                except Exception as e:
                    logger.warning(f"[WAL flush] Error for {tname}: {e}")

    def _retention_loop(self):
        while not self._stop_event.is_set():
            time.sleep(3600)
            if not self._stop_event.is_set():
                self.enforce_retention()

    def _alb_compaction_loop(self):
        """Compact ALB day-partitions every alb_compaction_interval seconds."""
        # Initial delay of 60 s so startup ingest settles first
        if self._stop_event.wait(timeout=60):
            return
        try:
            self.compact_alb()
        except Exception as e:
            logger.warning(f"[ALB Compaction] Initial run error: {e}")
        finally:
            self._trim_memory()
        while not self._stop_event.wait(timeout=self.alb_compaction_interval):
            try:
                self.compact_alb()
            except Exception as e:
                logger.warning(f"[ALB Compaction] Loop error: {e}")
            finally:
                self._trim_memory()

    def _check_and_repair_catalog(self) -> bool:
        import psycopg2

        try:
            table_path = self.get_table_path()
            metadata_dir = table_path / "metadata"

            if not metadata_dir.exists():
                logger.info(f" No metadata directory found at {metadata_dir}")
                return False

            metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
            if not metadata_files:
                logger.info(f" No metadata files found in {metadata_dir}")
                return False

            latest_metadata = metadata_files[-1]
            new_location = str(latest_metadata.absolute())

            db_config = self.config.get("database", {})
            conn = psycopg2.connect(
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", 5432),
                dbname=db_config.get("name", "iceberg_db"),
                user=db_config.get("user", "admin"),
                password=db_config.get("password", "admin123"),
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT metadata_location FROM iceberg_tables WHERE table_namespace = %s AND table_name = %s",
                (self.namespace, self.table_name),
            )
            row = cursor.fetchone()

            if row:
                current_location = row[0]
                if current_location != new_location:
                    print(
                        f"Catalog mismatch: catalog={current_location}, latest={latest_metadata.name}"
                    )
                    cursor.execute(
                        "UPDATE iceberg_tables SET metadata_location = %s WHERE table_namespace = %s AND table_name = %s",
                        (new_location, self.namespace, self.table_name),
                    )
                    conn.commit()
                    logger.info(f" Updated catalog to {latest_metadata.name}")

            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.info(f" Catalog check/repair failed: {e}")
            return False

    def _repair_missing_data_files(self):
        try:
            table_path = self.get_table_path()
            data_path = table_path / "data"

            existing_files = set()
            if data_path.exists():
                for f in data_path.rglob("*.parquet"):
                    existing_files.add(f.name)

            if not existing_files:
                logger.info(f" No data files exist, will reset table")
                self._reset_table_for_missing_data()
                return

            logger.info(f" Found {len(existing_files)} data files on disk")
            self._table = None

        except Exception as e:
            logger.info(f" Error repairing missing data files: {e}")
            self._table = None

    def _reset_table_for_missing_data(self):
        try:
            import psycopg2
            import shutil

            db_config = self.config.get("database", {})
            conn = psycopg2.connect(
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", 5432),
                dbname=db_config.get("name", "iceberg_db"),
                user=db_config.get("user", "admin"),
                password=db_config.get("password", "admin123"),
            )
            cursor = conn.cursor()

            cursor.execute(
                "DELETE FROM iceberg_tables WHERE table_namespace = %s AND table_name = %s",
                (self.namespace, self.table_name),
            )
            conn.commit()
            cursor.close()
            conn.close()

            table_path = self.get_table_path()
            if table_path.exists():
                shutil.rmtree(table_path)

            print(
                f"Reset table for {self.namespace}.{self.table_name}, table will be recreated on next write"
            )
            self._table = None
            self._catalog = None

        except Exception as e:
            logger.info(f" Error resetting table: {e}")
            self._table = None

    def start_maintenance(self):
        if self.compaction_enabled and not self._compaction_thread:
            self._compaction_thread = threading.Thread(
                target=self._compaction_loop, daemon=True
            )
            self._compaction_thread.start()
            logger.info(f"Started compaction thread (interval: {self.compaction_interval}s)")

        if self.compaction_enabled and not self._wal_flush_thread:
            self._wal_flush_thread = threading.Thread(
                target=self._wal_flush_loop, daemon=True
            )
            self._wal_flush_thread.start()
            logger.info(f"Started WAL-flush thread (interval: {self._wal_flush_interval}s)")

        if self.retention_enabled and not self._retention_thread:
            self._retention_thread = threading.Thread(
                target=self._retention_loop, daemon=True
            )
            self._retention_thread.start()
            logger.info(f"Started retention enforcement thread")

        if self.alb_compaction_enabled and not self._alb_compaction_thread:
            self._alb_compaction_thread = threading.Thread(
                target=self._alb_compaction_loop, daemon=True
            )
            self._alb_compaction_thread.start()
            logger.info(
                f"Started ALB compaction thread "
                f"(interval: {self.alb_compaction_interval}s, "
                f"min_files: {self.alb_compaction_min_files})"
            )

    def stop_maintenance(self):
        # Signal threads to stop
        self._stop_event.set()

        # Graceful drain: flush WAL → cold → S3 before exit so no data is lost
        logger.info("[Shutdown] Flushing WAL and cold tiers before shutdown...")
        for tname in (self.table_name, self.loki_table_name):
            try:
                flushed = self.flush_wal(tname)
                if flushed:
                    logger.info(f"[Shutdown] Flushed {flushed} WAL rows for {tname}")
            except Exception as e:
                logger.warning(f"[Shutdown] WAL flush error for {tname}: {e}")
        try:
            self.compact(table_name=self.table_name)
            self.compact(table_name=self.loki_table_name)
            logger.info("[Shutdown] Compaction complete")
        except Exception as e:
            logger.warning(f"[Shutdown] Compaction error: {e}")

        if self._wal_flush_thread:
            self._wal_flush_thread.join(timeout=5)
        if self._compaction_thread:
            self._compaction_thread.join(timeout=10)
        if self._retention_thread:
            self._retention_thread.join(timeout=5)
        if self._alb_compaction_thread:
            self._alb_compaction_thread.join(timeout=5)
        logger.info("Stopped all maintenance threads")
