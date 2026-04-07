import ctypes
import ctypes.util
import gc
import os
import yaml
import time
import threading
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

# Import metrics
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from service.services.warehouse_metrics import warehouse_metrics

import logging

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        StringType,
        TimestampType,
        LongType,
        NestedField,
    )
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import DayTransform
    from pyiceberg.expressions import (
        And,
        EqualTo,
        GreaterThanOrEqual,
        LessThanOrEqual,
        AlwaysTrue,
    )
    from pyiceberg.expressions.literals import TimestampLiteral

    PYICEBERG_AVAILABLE = True
except ImportError as e:
    logger.debug(f"PyIceberg import error: {e}")
    PYICEBERG_AVAILABLE = False

try:
    import pyspark  # lightweight check — actual SparkSession is lazy-loaded in SparkManager.get_spark()
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class SparkManager:
    def __init__(self, config: Dict[str, Any], warehouse_path: str):
        self.config = config
        self.warehouse_path = warehouse_path
        self._spark = None  # SparkSession, lazily initialised

    def _get_spark_config(self) -> Dict[str, str]:
        db_config = self.config.get("database", {})
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        name = db_config.get("name", "iceberg_db")
        user = db_config.get("user", "admin")
        password = db_config.get("password", "admin123")

        s3_config = self.config.get("s3", {})
        use_ec2_role = s3_config.get("use_ec2_role", False)
        endpoint = s3_config.get("endpoint", "http://localhost:9000")
        
        config_dict = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.type": "sql",
            f"spark.sql.catalog.spark_catalog.uri": f"postgresql://{user}:{password}@{host}:{port}/{name}",
            "spark.sql.warehouse.dir": self.warehouse_path,
            "spark.local.dir": "/tmp/spark",
            # S3A Configuration
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            # S3A timeout settings (in milliseconds to avoid parsing issues)
            "spark.hadoop.fs.s3a.connection.timeout": "60000",  # 60 seconds in ms
            "spark.hadoop.fs.s3a.socket.timeout": "60000",      # 60 seconds in ms
        }
        
        # Configure credentials based on whether EC2 role is enabled
        if use_ec2_role:
            # Use EC2 instance profile credentials
            config_dict["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.auth.IAMInstanceProfileCredentialsProvider"
            )
        else:
            # Use explicit static credentials
            config_dict["spark.hadoop.fs.s3a.access.key"] = s3_config.get("access_key", "admin")
            config_dict["spark.hadoop.fs.s3a.secret.key"] = s3_config.get("secret_key", "admin123")
            config_dict["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        
        return config_dict


    def get_spark(self):
        if self._spark is None:
            # Heavy import deferred until first actual Spark use (saves ~300 MB at startup)
            from pyspark.sql import SparkSession

            builder = SparkSession.builder
            builder = builder.appName(
                self.config.get("spark", {}).get("app_name", "CloudWatchLogPersist")
            )
            builder = builder.master(
                self.config.get("spark", {}).get("master", "local[*]")
            )

            for key, value in self._get_spark_config().items():
                builder = builder.config(key, value)

            self._spark = builder.getOrCreate()
        return self._spark


class WarehouseManager:
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = "config.yaml"

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

    def _parse_warehouse_path(self, path: str) -> Path:
        if path.startswith("file://"):
            return Path(path.replace("file://", ""))
        return Path(path)

    def _build_s3_client_config(self) -> dict:
        """
        Build boto3 S3 client configuration supporting both explicit credentials and EC2 role.
        
        If s3.use_ec2_role is True, credentials are obtained from the EC2 instance's IAM role.
        Otherwise, explicit access_key and secret_key are used.
        
        Returns: dict of kwargs to pass to boto3.client("s3", **config)
        """
        s3_config = self.config.get("s3", {})
        use_ec2_role = s3_config.get("use_ec2_role", False)
        endpoint_url = s3_config.get("endpoint", "http://localhost:9000")
        region = s3_config.get("region", "us-east-1")
        
        client_kwargs = {
            "region_name": region,
        }
        
        # Only add endpoint if specified (standard AWS doesn't use custom endpoints)

        
        # Add credentials only if not using EC2 role
        if not use_ec2_role:
            access_key = s3_config.get("access_key", "admin")
            secret_key = s3_config.get("secret_key", "admin123")
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url
                
        return client_kwargs

    def _build_s3_catalog_props(self) -> dict:
        """
        Build PyIceberg catalog properties for S3 supporting both explicit credentials and EC2 role.
        
        If s3.use_ec2_role is True, credentials are obtained from the EC2 instance's IAM role.
        Otherwise, explicit s3.access-key-id and s3.secret-access-key are included.
        
        Returns: dict of S3-specific properties for PyIceberg catalog
        """
        s3_config = self.config.get("s3", {})
        use_ec2_role = s3_config.get("use_ec2_role", False)
        endpoint = s3_config.get("endpoint", "http://localhost:9000")
        region = s3_config.get("region", "us-east-1")
        
        props = {
            "s3.endpoint": endpoint,
            "s3.region": region,
        }
        
        # Add credentials only if not using EC2 role
        if not use_ec2_role:
            access_key = s3_config.get("access_key", "admin")
            secret_key = s3_config.get("secret_key", "admin123")
            props["s3.access-key-id"] = access_key
            props["s3.secret-access-key"] = secret_key
        
        return props

    @property
    def warehouse_dir(self) -> Path:
        return self._warehouse_dir

    @property
    def spark(self) -> Optional[SparkManager]:
        if self._spark_manager is None and PYSPARK_AVAILABLE:
            self._spark_manager = SparkManager(
                self.config, self.warehouse_path
            )
        return self._spark_manager

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
    # ------------------------------------------------------------------
    # Cold tier — local Parquet data + PostgreSQL catalog metadata
    # Namespace "cold" keeps cold tables isolated from archive "default".
    # Using PostgreSQL (already running) instead of SQLite:
    #   - no single-writer file-lock contention for concurrent flush_wal()
    #   - catalog metadata survives staging volume loss
    #   - consistent with archive catalog (no second DB engine)
    # ------------------------------------------------------------------

    COLD_NAMESPACE = "cold"

    @property
    def cold_catalog(self):
        """PostgreSQL-backed Iceberg catalog for cold tier.
        Data (Parquet) is written to the local staging volume;
        only the tiny catalog metadata rows go to PostgreSQL.
        Falls back to SQLite when the catalog is not postgresql (unit tests).
        """
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")
        if self._cold_catalog is None:
            cold_data_dir = self.local_staging_dir / "cold"
            cold_data_dir.mkdir(parents=True, exist_ok=True)

            if self.catalog_name == "postgresql":
                db_config = self.config.get("database", {})
                host = db_config.get("host", "localhost")
                port = db_config.get("port", 5432)
                name = db_config.get("name", "iceberg_db")
                user = db_config.get("user", "admin")
                password = db_config.get("password", "admin123")
                # Data warehouse path must be a local file:// URI so Parquet
                # files land on the staging volume, not S3.
                self._cold_catalog = load_catalog(
                    "cold",
                    **{
                        "uri": f"postgresql://{user}:{password}@{host}:{port}/{name}",
                        "warehouse": cold_data_dir.as_uri(),  # file:///app/staging/cold
                    },
                )
            else:
                # Unit-test path: catalog=sqlite, fall back to local SQLite
                db_path = cold_data_dir / "catalog.db"
                self._cold_catalog = load_catalog(
                    "cold",
                    **{
                        "uri": f"sqlite:///{db_path.absolute()}",
                        "warehouse": str(cold_data_dir.absolute()),
                    },
                )
        return self._cold_catalog

    def _ensure_cold_table(self, table_name: str):
        """Create cold Iceberg table under the 'cold' namespace if it doesn't exist."""
        table_id = f"{self.COLD_NAMESPACE}.{table_name}"
        try:
            self.cold_catalog.create_namespace(self.COLD_NAMESPACE)
        except Exception:
            pass
        try:
            self.cold_catalog.load_table(table_id)
        except Exception:
            cold_data_dir = self.local_staging_dir / "cold"
            try:
                self.cold_catalog.create_table(
                    table_id,
                    schema=self._get_schema(),
                    partition_spec=self._get_partition_spec(),
                    location=str(cold_data_dir / table_name),
                )
            except Exception:
                # Concurrent callers may have already created the table.
                pass

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

    @property
    def catalog(self):
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")
        if self._catalog is None:
            if self.catalog_name == "postgresql":
                db_config = self.config.get("database", {})
                host = db_config.get("host", "localhost")
                port = db_config.get("port", 5432)
                name = db_config.get("name", "iceberg_db")
                user = db_config.get("user", "admin")
                password = db_config.get("password", "admin123")

                catalog_props = {
                    "uri": f"postgresql://{user}:{password}@{host}:{port}/{name}",
                    "warehouse": self.warehouse_path,
                }

                if self.warehouse_path.startswith("s3"):
                    catalog_props.update(self._build_s3_catalog_props())

                self._catalog = load_catalog("sql", **catalog_props)
            else:
                db_path = self._warehouse_dir / "catalog.db"
                self._catalog = load_catalog(
                    "sql",
                    **{
                        "uri": f"sqlite:///{db_path.absolute()}",
                        "warehouse": str(self._warehouse_dir.absolute()),
                    },
                )
        return self._catalog

    def _get_schema(self) -> Schema:
        ingest_config = self.config.get("ingest", {})
        labels_config = ingest_config.get("labels", {})
        label_columns = labels_config.get("columns", [])

        fields = [
            NestedField(1, "log_group_name", StringType(), required=False),
            NestedField(2, "log_stream_name", StringType(), required=False),
            NestedField(3, "timestamp", TimestampType(), required=False),
            NestedField(4, "message", StringType(), required=False),
            NestedField(5, "ingestion_time", TimestampType(), required=False),
            NestedField(6, "sequence_token", LongType(), required=False),
        ]

        field_id = 7
        for label in label_columns:
            safe_name = label.replace("-", "_").replace(" ", "_")
            fields.append(
                NestedField(
                    field_id, f"label_{safe_name}", StringType(), required=False
                )
            )
            field_id += 1

        return Schema(*fields)

    def _get_partition_spec(self) -> PartitionSpec:
        """Partition spec on ingestion_time (field 5) — legacy for backward compat."""
        return PartitionSpec(
            PartitionField(
                source_id=5,
                field_id=100,
                name="ingestion_day",
                transform=DayTransform(),
            )
        )

    def _get_partition_spec_v2(self) -> PartitionSpec:
        """Optimized partition spec on timestamp (field 3) for better partition pruning."""
        return PartitionSpec(
            PartitionField(
                source_id=3,
                field_id=100,
                name="event_day",
                transform=DayTransform(),
            )
        )

    def migrate_to_timestamp_partitioning(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Migrate table from ingestion_time partitioning to timestamp partitioning.
        This improves query performance because PyIceberg can prune partitions based on timestamp filters.
        
        Process:
        1. Create temp table with new partition spec (source_id=3 for timestamp)
        2. Scan all data from original table → insert into temp table
        3. Drop original table
        4. Rename temp table to original name
        
        Returns migration status dict.
        """
        if not PYICEBERG_AVAILABLE:
            return {"status": "skipped", "reason": "PyIceberg not available"}

        target_table = table_name or self.table_name
        table_id = f"{self.namespace}.{target_table}"
        temp_table_id = f"{self.namespace}.{target_table}_v2_migration"

        logger.info(f"[Migration] Starting timestamp partitioning migration for {table_id}")

        try:
            # 1. Check if original table exists and is using old partition spec
            orig_table = self.catalog.load_table(table_id)
            orig_spec = orig_table.spec()
            
            # Check if already migrated (partition field source_id = 3 = timestamp)
            for pf in orig_spec.fields:
                if pf.source_id == 3:  # timestamp
                    logger.info(f"[Migration] Table {table_id} already using timestamp partitioning")
                    return {"status": "skipped", "reason": "already_migrated"}

            logger.info(f"[Migration] Original table has {orig_table.scan().to_arrow().num_rows} rows")

            # 2. Create temp table with new partition spec
            try:
                self.catalog.load_table(temp_table_id)
                logger.warning(f"[Migration] Temp table {temp_table_id} exists, dropping it first")
                self.catalog.drop_table(temp_table_id)
            except Exception:
                pass  # Doesn't exist yet
            
            temp_table = self.catalog.create_table(
                temp_table_id,
                schema=self._get_schema(),
                partition_spec=self._get_partition_spec_v2(),
            )
            logger.info(f"[Migration] Created temp table {temp_table_id} with timestamp partitioning")

            # 3. Copy data: scan original → insert into temp
            orig_data = orig_table.scan().to_arrow()
            if orig_data.num_rows > 0:
                temp_table.append(orig_data)
                logger.info(f"[Migration] Copied {orig_data.num_rows} rows to temp table")
            else:
                logger.info(f"[Migration] Original table is empty")

            # 4. Drop original and rename temp
            self.catalog.drop_table(table_id)
            logger.info(f"[Migration] Dropped original table {table_id}")
            
            # Rename via SQL if available, otherwise drop+recreate with data
            try:
                # PyIceberg doesn't have rename_table, so we manually do drop+recreate
                # Get temp table location
                temp_location = temp_table.location()
                
                # Create new table with original name at temp location
                # Actually, we can't just move files. Let's do it properly:
                # Drop temp, recreate original with new spec, copy data back
                self.catalog.drop_table(temp_table_id)
                
                # Recreate original table with new partition spec
                new_table = self.catalog.create_table(
                    table_id,
                    schema=self._get_schema(),
                    partition_spec=self._get_partition_spec_v2(),
                )
                logger.info(f"[Migration] Recreated {table_id} with timestamp partitioning")
                
                # Re-insert data into new table
                if orig_data.num_rows > 0:
                    new_table.append(orig_data)
                    logger.info(f"[Migration] Re-inserted {orig_data.num_rows} rows into migrated table")
                
            except Exception as rename_err:
                logger.error(f"[Migration] Rename/recreate failed: {rename_err}")
                # Cleanup: drop temp table if it still exists
                try:
                    self.catalog.drop_table(temp_table_id)
                except Exception:
                    pass
                raise

            # 5. Verify migration
            migrated_table = self.catalog.load_table(table_id)
            migrated_spec = migrated_table.spec()
            logger.info(f"[Migration] Migration complete. New spec: {migrated_spec}")

            return {
                "status": "success",
                "message": f"Migrated {table_id} to timestamp partitioning",
                "rows_migrated": orig_data.num_rows if orig_data else 0,
            }

        except Exception as e:
            logger.error(f"[Migration] Failed: {type(e).__name__}: {str(e)[:200]}")
            return {
                "status": "error",
                "message": str(e),
            }

    def ensure_warehouse(self):
        if not PYICEBERG_AVAILABLE:
            self._warehouse_dir.mkdir(parents=True, exist_ok=True)
            self.get_table_path().mkdir(parents=True, exist_ok=True)
            return

        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass

        # Create CloudWatch logs table
        table_id = f"{self.namespace}.{self.table_name}"
        try:
            # Check if table already exists in catalog
            self.catalog.load_table(table_id)
            logger.info(f" Loaded existing CloudWatch logs table: {table_id}")
        except Exception:
            try:
                self.catalog.create_table(
                    table_id,
                    schema=self._get_schema(),
                    partition_spec=self._get_partition_spec(),
                )
                logger.info(f" Created new CloudWatch logs table: {table_id}")
            except Exception as e:
                logger.info(f" CloudWatch table exception: {type(e).__name__}: {e}")

        # Create Loki logs table if configured
        loki_table = self.config.get("loki", {}).get("table_name", "loki_logs")
        loki_table_id = f"{self.namespace}.{loki_table}"
        try:
            self.catalog.load_table(loki_table_id)
            logger.info(f" Loaded existing Loki logs table: {loki_table_id}")
        except Exception:
            try:
                self.catalog.create_table(
                    loki_table_id,
                    schema=self._get_schema(),
                    partition_spec=self._get_partition_spec(),
                )
                logger.info(f" Created new Loki logs table: {loki_table_id}")
            except Exception as e:
                logger.info(f" Loki table exception: {type(e).__name__}: {e}")

        # Apply write-optimization properties to both tables
        for t_id in [table_id, loki_table_id]:
            try:
                self._apply_write_properties(self.catalog.load_table(t_id))
            except Exception:
                pass

    def _apply_write_properties(self, table_obj) -> None:
        """Set Parquet write-optimization properties on a table if not already present."""
        desired = {
            # Target one file per partition flush ≈ 128 MB
            "write.target-file-size-bytes": "134217728",
            # Row-group size 64 MB → richer min/max statistics per group
            "write.parquet.row-group-size-bytes": "67108864",
            "write.parquet.compression-codec": "snappy",
        }
        existing = table_obj.properties()
        missing = {k: v for k, v in desired.items() if k not in existing}
        if missing:
            try:
                with table_obj.transaction() as tx:
                    tx.set_properties(**missing)
                logger.info(f"[Warehouse] Applied write properties to {table_obj.name()}: {list(missing.keys())}")
            except Exception as e:
                logger.warning(f"[Warehouse] Could not set write properties: {e}")

    def _get_label_columns(self) -> List[str]:
        ingest_config = self.config.get("ingest", {})
        labels_config = ingest_config.get("labels", {})
        label_columns = labels_config.get("columns", [])
        return [col.replace("-", "_").replace(" ", "_") for col in label_columns]

    def get_table_path(self, table_name: Optional[str] = None) -> Path:
        name = table_name or self.table_name
        return self._warehouse_dir / self.namespace / name

    def list_tables(self) -> list[str]:
        if not PYICEBERG_AVAILABLE:
            namespace_dir = self._warehouse_dir / self.namespace
            if not namespace_dir.exists():
                return []
            return [d.name for d in namespace_dir.iterdir() if d.is_dir()]

        return [t.name for t in self.catalog.list_tables(self.namespace)]

    def insert_logs(self, logs: List[Dict[str, Any]], table_name: Optional[str] = None):
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

    def query(
        self,
        filter_expr: Optional[str] = None,
        limit: int = 100,
        table_name: Optional[str] = None,
        filter_expression=None,
        selected_fields: Optional[tuple] = None,
    ):
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

    def get_table(self):
        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.load_table(table_id)
        return self._table

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
        return {
            "status": "ok",
            "partitions_compacted": compacted_partitions,
            "rows_rewritten": rows_rewritten,
        }

    def enforce_retention(self):
        if not self.retention_enabled:
            return {"status": "skipped", "reason": "disabled"}

        try:
            table = self.get_table()
            data_path = Path(table.location()) / "data"
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)

            print(
                f"[Retention] Removing data older than {self.retention_days} days (cutoff: {cutoff})"
            )

            if not data_path.exists():
                return {"status": "success", "removed_partitions": 0}

            removed = 0
            for partition_dir in data_path.iterdir():
                if partition_dir.is_dir() and partition_dir.name.startswith(
                    "ingestion_day="
                ):
                    date_str = partition_dir.name.replace("ingestion_day=", "")
                    try:
                        partition_date = datetime.strptime(
                            date_str, "%Y-%m-%d"
                        ).replace(tzinfo=timezone.utc)
                        if partition_date < cutoff:
                            import shutil

                            shutil.rmtree(partition_dir)
                            removed += 1
                            print(
                                f"[Retention] Removed partition: {partition_dir.name}"
                            )
                    except ValueError:
                        pass

            warehouse_metrics.record_retention(removed_partitions=removed)
            return {"status": "success", "removed_partitions": removed}
        except Exception as e:
            print(f"[Retention] Error: {e}")
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
            
            bucket = "warehouse"
            
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
        """Flush WAL \u2192 cold Iceberg on a short interval (default 30s).\n        Keeps the WAL small and makes data available to Iceberg filter push-down sooner.\n        """
        while not self._stop_event.wait(timeout=self._wal_flush_interval):
            for tname in (self.table_name, self.loki_table_name):
                try:
                    flushed = self.flush_wal(tname)
                    if flushed:
                        logger.debug(f"[WAL flush] {flushed} rows \u2192 cold for {tname}")
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
