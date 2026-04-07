import os
import yaml
import threading
from pathlib import Path
from typing import Optional, Dict

import logging

logger = logging.getLogger("service.warehouse")

from .catalog_mixin import CatalogMixin
from .wal_mixin import WALMixin
from .query_mixin import QueryMixin
from .compaction_mixin import CompactionMixin
from .maintenance_mixin import MaintenanceMixin
from .migration_mixin import MigrationMixin
from .spark_manager import SparkManager


class WarehouseManager(
    CatalogMixin,
    WALMixin,
    QueryMixin,
    CompactionMixin,
    MaintenanceMixin,
    MigrationMixin,
):
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = "config.yaml"

        self._config_path = config_path

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.warehouse_path = self.config.get("warehouse", "file:///warehouse")
        self.catalog_name = self.config.get("catalog", "iceberg")
        self.namespace = self.config.get("namespace", "default")
        self.table_name = self.config.get("table_name", "cloudwatch_logs")
        self.loki_table_name = self.config.get("loki", {}).get("table_name", "loki_logs")

        compaction_config = self.config.get("compaction", {})
        self.compaction_enabled = compaction_config.get("enabled", True)
        self.compaction_interval = compaction_config.get("interval_seconds", 300)
        self.compaction_max_files = compaction_config.get("max_data_files", 50)
        self.compaction_target_file_size = compaction_config.get("target_file_size_mb", 128)
        self.local_staging_dir = Path(
            compaction_config.get("local_staging_dir", "/app/staging")
        )
        # Max WAL+cold row count before ingest is back-pressured (prevents OOM when S3 is down)
        self.wal_max_rows = int(compaction_config.get("wal_max_rows", 500_000))
        # How often the dedicated WAL-flush thread moves WAL → cold (seconds)
        self._wal_flush_interval = int(compaction_config.get("wal_flush_interval_seconds", 30))
        # Phase 3 (S3 in-place merge) reads one full partition into RAM.
        # Disable on memory-constrained deployments; phases 1+2 are sufficient.
        self.s3_merge_enabled = compaction_config.get("s3_merge_enabled", True)

        alb_cfg = self.config.get("alb", {})
        alb_compact_cfg = alb_cfg.get("compaction", {})
        self.alb_compaction_enabled = alb_compact_cfg.get("enabled", True)
        self.alb_compaction_interval = int(alb_compact_cfg.get("interval_seconds", 600))
        # Only compact a day-partition when it has more than this many data files
        self.alb_compaction_min_files = int(alb_compact_cfg.get("min_files_per_partition", 2))
        self.alb_table_name = alb_cfg.get("table_name", "alb_logs")

        retention_config = self.config.get("retention", {})
        self.retention_days = retention_config.get("days", 7)
        self.retention_enabled = retention_config.get("enabled", True)

        self._warehouse_dir = self._parse_warehouse_path(self.warehouse_path)
        self._catalog = None
        self._cold_catalog = None   # local Iceberg (cold tier)
        self._table = None
        self._spark_manager: Optional[SparkManager] = None
        self._compaction_thread: Optional[threading.Thread] = None
        self._wal_flush_thread: Optional[threading.Thread] = None   # WAL → cold, runs faster
        self._retention_thread: Optional[threading.Thread] = None
        self._alb_compaction_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # Two fine-grained locks replace the former single _staging_lock:
        #   _wal_write_lock  – protects WAL file creation/listing/deletion
        #   _cold_lock       – protects cold-Iceberg writes and cold→S3 upload
        # Separating them ensures that ingest (WAL writes) is never blocked by a
        # slow Phase-2 S3 upload running in the compaction thread.
        self._wal_write_lock = threading.Lock()
        self._cold_lock = threading.Lock()
        # In-memory WAL row counters.  Lazy-initialised on first access (one
        # file-scan per table per process lifetime) and maintained via
        # increment/decrement, making the per-write backpressure check O(1).
        self._wal_row_counters: Dict[str, int] = {}
        # TTL caches for metadata queries (avoid full table scans on every list call)
        self._log_groups_cache: Optional[dict] = None
        self._log_groups_cache_time: float = 0.0
        self._log_streams_cache: Dict[str, tuple] = {}  # {log_group: (time, [streams])}

    @staticmethod
    def _parse_warehouse_path(path: str) -> Path:
        if path.startswith("file://"):
            return Path(path.replace("file://", ""))
        return Path(path)
