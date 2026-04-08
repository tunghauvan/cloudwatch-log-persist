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


class WarehouseManager(
    CatalogMixin,
    WALMixin,
    QueryMixin,
    CompactionMixin,
    MaintenanceMixin,
):
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = "config.yaml"

        self._config_path = config_path
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.warehouse_path = self.config.get("warehouse", "s3://log-warehouse")
        self.catalog_name = "delta"
        self.namespace = self.config.get("namespace", "default")
        self.table_name = self.config.get("table_name", "cloudwatch_logs")
        self.loki_table_name = self.config.get("loki", {}).get("table_name", "loki_logs")

        compaction_cfg = self.config.get("compaction", {})
        self.compaction_enabled = compaction_cfg.get("enabled", True)
        self.compaction_interval = compaction_cfg.get("interval_seconds", 3600)
        self.local_staging_dir = Path(compaction_cfg.get("local_staging_dir", "/app/staging"))
        self.wal_max_rows = int(compaction_cfg.get("wal_max_rows", 500_000))
        self._wal_flush_interval = int(compaction_cfg.get("wal_flush_interval_seconds", 30))

        alb_cfg = self.config.get("alb", {})
        alb_compact_cfg = alb_cfg.get("compaction", {})
        self.alb_compaction_enabled = alb_compact_cfg.get("enabled", True)
        self.alb_compaction_interval = int(alb_compact_cfg.get("interval_seconds", 600))
        self.alb_compaction_min_files = int(alb_compact_cfg.get("min_files_per_partition", 2))
        self.alb_table_name = alb_cfg.get("table_name", "alb_logs")

        retention_cfg = self.config.get("retention", {})
        self.retention_days = retention_cfg.get("days", 7)
        self.retention_enabled = retention_cfg.get("enabled", True)

        self._warehouse_dir = self._parse_warehouse_path(self.warehouse_path)

        # Threading
        self._compaction_thread: Optional[threading.Thread] = None
        self._wal_flush_thread: Optional[threading.Thread] = None
        self._retention_thread: Optional[threading.Thread] = None
        self._alb_compaction_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # _wal_write_lock: serialises WAL file creation/listing/deletion
        self._wal_write_lock = threading.Lock()
        # In-memory WAL row counters (O(1) back-pressure check)
        self._wal_row_counters: Dict[str, int] = {}
        # TTL caches
        self._log_groups_cache: Optional[dict] = None
        self._log_groups_cache_time: float = 0.0
        self._log_streams_cache: Dict[str, tuple] = {}

    @staticmethod
    def _parse_warehouse_path(path: str) -> Path:
        for prefix in ("s3a://", "s3n://", "s3://", "file://"):
            if path.startswith(prefix):
                # For S3 paths return a dummy local path (not used for Delta ops)
                return Path("/tmp/warehouse")
        return Path(path)
