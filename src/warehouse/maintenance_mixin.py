import ctypes
import ctypes.util
import gc
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

logger = logging.getLogger("service.warehouse")


class MaintenanceMixin:
    """Background maintenance threads, retention (vacuum), and stats."""

    # ------------------------------------------------------------------
    # Retention
    # ------------------------------------------------------------------

    def enforce_retention(self):
        if not self.retention_enabled:
            return {"status": "skipped", "reason": "disabled"}

        from deltalake import DeltaTable
        from service.services.warehouse_metrics import warehouse_metrics

        removed = 0
        for tname in (self.table_name, self.loki_table_name):
            delta_uri = self._get_delta_uri(tname)
            storage_opts = self._get_delta_storage_options()
            try:
                dt = DeltaTable(delta_uri, storage_options=storage_opts)
                # vacuum() deletes files older than retention_hours that are no
                # longer referenced by any live snapshot — safe, ACID-correct.
                result = dt.vacuum(
                    retention_hours=self.retention_days * 24,
                    enforce_retention_duration=False,
                    dry_run=False,
                )
                deleted = len(result) if isinstance(result, list) else 0
                logger.info(
                    f"[Retention] Vacuumed '{tname}': {deleted} file(s) removed "
                    f"(retention={self.retention_days}d)"
                )
                removed += deleted
            except Exception as e:
                msg = str(e)
                if any(x in msg for x in ("not a Delta table", "No log files", "doesn't exist")):
                    logger.debug(f"[Retention] '{tname}': table not yet created, skipping")
                else:
                    logger.warning(f"[Retention] '{tname}': {e}")

        warehouse_metrics.record_retention(removed_partitions=removed)
        return {"status": "success", "files_removed": removed}

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        from deltalake import DeltaTable

        archive: Dict[str, Any] = {}
        for tname in (self.table_name, self.loki_table_name):
            delta_uri = self._get_delta_uri(tname)
            storage_opts = self._get_delta_storage_options()
            try:
                dt = DeltaTable(delta_uri, storage_options=storage_opts)
                history = dt.history(limit=1)
                latest = history[0] if history else {}
                # Delta stores file stats in the transaction log
                files = dt.files()
                archive[tname] = {
                    "uri": delta_uri,
                    "data_files": len(files),
                    "version": dt.version(),
                    "last_commit": latest.get("timestamp", ""),
                }
            except Exception as e:
                archive[tname] = {"uri": delta_uri, "error": str(e)}

        wal_rows: Dict[str, int] = {}
        wal_files: Dict[str, int] = {}
        for tname in (self.table_name, self.loki_table_name):
            wal_dir = self.local_staging_dir / tname / "wal"
            files = list(wal_dir.glob("wal_*.jsonl")) if wal_dir.exists() else []
            wal_files[tname] = len(files)
            wal_rows[tname] = self._wal_row_count(tname)

        total_unstaged = sum(wal_rows.values())

        return {
            "archive": archive,
            "hot_wal": {
                tname: {"rows": wal_rows[tname], "files": wal_files[tname]}
                for tname in wal_rows
            },
            "backpressure": {
                "total_unstaged_rows": total_unstaged,
                "limit": self.wal_max_rows,
                "pct_full": round(total_unstaged / max(self.wal_max_rows, 1) * 100, 1),
            },
            "compaction_enabled": self.compaction_enabled,
            "compaction_interval_seconds": self.compaction_interval,
            "retention_enabled": self.retention_enabled,
            "retention_days": self.retention_days,
        }

    # ------------------------------------------------------------------
    # Memory trim (post-compaction)
    # ------------------------------------------------------------------

    @staticmethod
    def _trim_memory():
        gc.collect()
        try:
            libc_name = ctypes.util.find_library("c")
            if libc_name:
                libc = ctypes.CDLL(libc_name)
                if hasattr(libc, "malloc_trim"):
                    libc.malloc_trim(0)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Background thread loops
    # ------------------------------------------------------------------

    def _compaction_loop(self):
        # 60 s grace period at startup
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
        """Flush WAL → Delta S3 every wal_flush_interval seconds."""
        while not self._stop_event.wait(timeout=self._wal_flush_interval):
            for tname in (self.table_name, self.loki_table_name):
                try:
                    flushed = self.flush_wal(tname)
                    if flushed:
                        logger.debug(f"[WAL flush] {flushed} rows → Delta S3 for '{tname}'")
                except RuntimeError as e:
                    if "interpreter shutdown" in str(e):
                        return
                    logger.warning(f"[WAL flush] Error for '{tname}': {e}")
                except Exception as e:
                    logger.warning(f"[WAL flush] Error for '{tname}': {e}")

    def _retention_loop(self):
        while not self._stop_event.is_set():
            time.sleep(3600)
            if not self._stop_event.is_set():
                self.enforce_retention()

    def _alb_compaction_loop(self):
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

    # ------------------------------------------------------------------
    # Start / stop
    # ------------------------------------------------------------------

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
            logger.info("Started retention enforcement thread")

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
        self._stop_event.set()

        logger.info("[Shutdown] Flushing WAL to Delta S3 before shutdown...")
        for tname in (self.table_name, self.loki_table_name):
            try:
                flushed = self.flush_wal(tname)
                if flushed:
                    logger.info(f"[Shutdown] Flushed {flushed} WAL rows for '{tname}'")
            except Exception as e:
                import traceback
                logger.warning(
                    f"[Shutdown] WAL flush skipped for '{tname}' "
                    f"(WAL is safe on disk): {e}\n{traceback.format_exc()}"
                )

        if self._wal_flush_thread:
            self._wal_flush_thread.join(timeout=5)
        if self._compaction_thread:
            self._compaction_thread.join(timeout=10)
        if self._retention_thread:
            self._retention_thread.join(timeout=5)
        if self._alb_compaction_thread:
            self._alb_compaction_thread.join(timeout=5)
        logger.info("Stopped all maintenance threads")
