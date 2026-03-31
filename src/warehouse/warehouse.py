import os
import yaml
from pathlib import Path
from typing import Optional, List, Dict, Any

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

    PYICEBERG_AVAILABLE = True
except ImportError as e:
    print(f"Import error: {e}")
    PYICEBERG_AVAILABLE = False


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

        self._warehouse_dir = self._parse_warehouse_path(self.warehouse_path)
        self._catalog = None
        self._table = None

    def _parse_warehouse_path(self, path: str) -> Path:
        if path.startswith("file://"):
            return Path(path.replace("file://", ""))
        return Path(path)

    @property
    def warehouse_dir(self) -> Path:
        return self._warehouse_dir

    @property
    def catalog(self):
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")

        if self._catalog is None:
            self._warehouse_dir.mkdir(parents=True, exist_ok=True)

            if self.catalog_name == "postgresql":
                db_config = self.config.get("database", {})
                host = db_config.get("host", "localhost")
                port = db_config.get("port", 5432)
                name = db_config.get("name", "iceberg_db")
                user = db_config.get("user", "admin")
                password = db_config.get("password", "admin123")
                self._catalog = load_catalog(
                    "sql",
                    **{
                        "uri": f"postgresql://{user}:{password}@{host}:{port}/{name}",
                        "warehouse": str(self._warehouse_dir.absolute()),
                    },
                )
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
        return Schema(
            NestedField(1, "log_group_name", StringType(), required=False),
            NestedField(2, "log_stream_name", StringType(), required=False),
            NestedField(3, "timestamp", TimestampType(), required=False),
            NestedField(4, "message", StringType(), required=False),
            NestedField(5, "ingestion_time", TimestampType(), required=False),
            NestedField(6, "sequence_token", LongType(), required=False),
        )

    def _get_partition_spec(self) -> PartitionSpec:
        return PartitionSpec()

    def ensure_warehouse(self):
        if not PYICEBERG_AVAILABLE:
            self._warehouse_dir.mkdir(parents=True, exist_ok=True)
            self.get_table_path().mkdir(parents=True, exist_ok=True)
            return

        try:
            self.catalog.create_namespace(self.namespace)
        except Exception:
            pass

        try:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.create_table(
                table_id,
                schema=self._get_schema(),
                partition_spec=self._get_partition_spec(),
            )
        except Exception:
            self._table = self.catalog.load_table(f"{self.namespace}.{self.table_name}")

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

    def insert_logs(self, logs: List[Dict[str, Any]]):
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")

        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.load_table(table_id)

        import pyarrow as pa

        arrays = [
            pa.array([log.get("logGroupName", "") for log in logs]),
            pa.array([log.get("logStreamName", "") for log in logs]),
            pa.array([log.get("timestamp") for log in logs], type=pa.timestamp("us")),
            pa.array([log.get("message", "") for log in logs]),
            pa.array(
                [log.get("ingestionTime") for log in logs], type=pa.timestamp("us")
            ),
            pa.array([log.get("sequenceToken") for log in logs], type=pa.int64()),
        ]

        table = pa.table(
            arrays,
            names=[
                "log_group_name",
                "log_stream_name",
                "timestamp",
                "message",
                "ingestion_time",
                "sequence_token",
            ],
        )

        self._table.append(table)

    def query(self, filter_expr: Optional[str] = None, limit: int = 100):
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")

        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.load_table(table_id)

        scan = self._table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)

        result = scan.to_arrow()
        if limit and len(result) > limit:
            result = result.slice(0, limit)
        return result
