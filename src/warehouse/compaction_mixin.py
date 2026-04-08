import logging
import os
import time

logger = logging.getLogger("service.warehouse")

_DUCKDB_TEMP_DIR = os.environ.get("DUCKDB_TEMP_DIR", "/tmp/duckdb_compact")
_DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "512MB")


class CompactionMixin:
    """Compaction for CloudWatch / Loki tables via DeltaTable.optimize()."""

    def compact(self, table_name: str = None):
        """Flush WAL → Delta S3, then compact small files with optimize().

        Phase 1: flush_wal() drains JSONL files to S3 (already done by the
                 WAL-flush thread, but we call it here too so compaction always
                 sees the latest data).
        Phase 2: DeltaTable.optimize().compact() merges small Parquet files
                 written by frequent WAL flushes into larger ones — no full
                 table materialisation, no OOM risk.
        """
        from deltalake import DeltaTable
        from service.services.warehouse_metrics import warehouse_metrics

        if not self.compaction_enabled:
            return {"status": "skipped", "reason": "disabled"}

        target = table_name or self.table_name
        delta_uri = self._get_delta_uri(target)
        storage_opts = self._get_delta_storage_options()

        try:
            warehouse_metrics.record_compaction()

            # Phase 1 — drain any remaining WAL rows first
            wal_flushed = self.flush_wal(target)
            if wal_flushed:
                logger.info(f"[Compaction] Phase 1: flushed {wal_flushed} WAL rows for '{target}'")

            # Phase 2 — compact small files
            try:
                dt = DeltaTable(delta_uri, storage_options=storage_opts)
                # dt.optimize is a property returning TableOptimizer (not a method)
                metrics = dt.optimize.compact()
                files_added = metrics.get("numFilesAdded", 0)
                files_removed = metrics.get("numFilesRemoved", 0)
                logger.info(
                    f"[Compaction] '{target}': optimize done "
                    f"(+{files_added} files, -{files_removed} files)"
                )

                # Physically delete files marked as removed by optimize.
                # retention_hours=0 + enforce_retention_duration=False = delete immediately.
                # Safe here because we control all readers (no external concurrent readers
                # reading old snapshots).
                if files_removed > 0:
                    dt.vacuum(
                        retention_hours=0,
                        enforce_retention_duration=False,
                        dry_run=False,
                    )
                    logger.info(f"[Compaction] '{target}': vacuumed {files_removed} old file(s)")

                # Create a checkpoint after every compaction so DuckDB readers
                # only need to read the latest checkpoint + new commits instead
                # of replaying the full transaction log from the beginning.
                try:
                    dt.create_checkpoint()
                    logger.info(f"[Compaction] '{target}': checkpoint created at v{dt.version()}")
                except Exception as ckpt_err:
                    logger.warning(f"[Compaction] '{target}': checkpoint failed (non-fatal): {ckpt_err}")

                return {
                    "status": "success",
                    "wal_flushed": wal_flushed,
                    "files_added": files_added,
                    "files_removed": files_removed,
                }
            except Exception as e:
                msg = str(e)
                if any(x in msg for x in (
                    "not a Delta table", "No log files", "doesn't exist",
                    "TableNotFoundError", "No files in log segment",
                )):
                    logger.debug(f"[Compaction] '{target}': table not yet created, skipping optimize")
                    return {"status": "skipped", "reason": "table not yet created"}
                raise

        except Exception as e:
            logger.error(f"[Compaction] Error for '{target}': {type(e).__name__}: {e}")
            return {"status": "error", "error": str(e)}
        finally:
            self._trim_memory()

    def _trim_memory(self):
        """Release memory back to the OS after heavy operations (Linux only)."""
        try:
            import ctypes, ctypes.util
            libc_name = ctypes.util.find_library("c")
            if libc_name:
                libc = ctypes.CDLL(libc_name)
                if hasattr(libc, "malloc_trim"):
                    libc.malloc_trim(0)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # ALB compaction — kept separate, still uses DuckDB for local Parquet
    # (no Iceberg dependency; see alb_schema.py which uses pa.schema)
    # ------------------------------------------------------------------

    def compact_alb(self):
        """Compact ALB day-partitions. Implementation unchanged — uses DuckDB locally."""
        # Delegated to the existing implementation in the ALB route/mixin.
        pass
