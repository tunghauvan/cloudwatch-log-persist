import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

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

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class CatalogMixin:
    """Catalog setup, schema definitions, and warehouse initialization."""

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
    def spark(self):
        from .spark_manager import SparkManager, PYSPARK_AVAILABLE
        if self._spark_manager is None and PYSPARK_AVAILABLE:
            self._spark_manager = SparkManager(
                self.config, self.warehouse_path
            )
        return self._spark_manager

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

    COLD_NAMESPACE = "cold"

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
        """Set Parquet write-optimization and metadata-cleanup properties on a table."""
        desired = {
            # Target one file per partition flush ≈ 128 MB
            "write.target-file-size-bytes": "134217728",
            "write.parquet.compression-codec": "snappy",
            # Auto-delete old .metadata.json files after every commit.
            # Without this, each append/overwrite leaves one extra metadata file in S3
            # that accumulates indefinitely. Iceberg only needs the current one.
            "write.metadata.delete-after-commit.enabled": "true",
            # Keep at most 3 previous metadata.json versions (current + 2 prior).
            # Enough for a single rollback; beyond that they're waste in object store.
            "write.metadata.previous-versions-max": "3",
        }
        # table_obj.properties is a dict attribute (not a method)
        existing = table_obj.properties if isinstance(table_obj.properties, dict) else {}
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

    def get_table(self):
        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.load_table(table_id)
        return self._table

    def list_tables(self) -> list[str]:
        if not PYICEBERG_AVAILABLE:
            namespace_dir = self._warehouse_dir / self.namespace
            if not namespace_dir.exists():
                return []
            return [d.name for d in namespace_dir.iterdir() if d.is_dir()]

        return [t.name for t in self.catalog.list_tables(self.namespace)]
