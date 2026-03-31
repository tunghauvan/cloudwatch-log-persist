import os
import yaml
import time
import threading
from datetime import datetime, timezone, timedelta
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
    from pyiceberg.transforms import DayTransform

    PYICEBERG_AVAILABLE = True
except ImportError as e:
    print(f"PyIceberg import error: {e}")
    PYICEBERG_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, dayofmonth, to_timestamp, lit

    PYSPARK_AVAILABLE = True
except ImportError as e:
    print(f"PySpark import error: {e}")
    PYSPARK_AVAILABLE = False


class SparkManager:
    def __init__(self, config: Dict[str, Any], warehouse_path: str):
        self.config = config
        self.warehouse_path = warehouse_path
        self._spark: Optional[SparkSession] = None

    def _get_spark_config(self) -> Dict[str, str]:
        db_config = self.config.get("database", {})
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        name = db_config.get("name", "iceberg_db")
        user = db_config.get("user", "admin")
        password = db_config.get("password", "admin123")

        return {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.spark_catalog.type": "sql",
            f"spark.sql.catalog.spark_catalog.uri": f"postgresql://{user}:{password}@{host}:{port}/{name}",
            "spark.sql.warehouse.dir": self.warehouse_path,
            "spark.local.dir": "/tmp/spark",
        }

    def get_spark(self) -> SparkSession:
        if self._spark is None:
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

        compaction_config = self.config.get("compaction", {})
        self.compaction_enabled = compaction_config.get("enabled", True)
        self.compaction_interval = compaction_config.get("interval_seconds", 300)
        self.compaction_max_files = compaction_config.get("max_data_files", 50)
        self.compaction_target_file_size = compaction_config.get(
            "target_file_size_mb", 128
        )

        retention_config = self.config.get("retention", {})
        self.retention_days = retention_config.get("days", 7)
        self.retention_enabled = retention_config.get("enabled", True)

        self._warehouse_dir = self._parse_warehouse_path(self.warehouse_path)
        self._catalog = None
        self._table = None
        self._spark_manager: Optional[SparkManager] = None
        self._compaction_thread: Optional[threading.Thread] = None
        self._retention_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _parse_warehouse_path(self, path: str) -> Path:
        if path.startswith("file://"):
            return Path(path.replace("file://", ""))
        return Path(path)

    @property
    def warehouse_dir(self) -> Path:
        return self._warehouse_dir

    @property
    def spark(self) -> Optional[SparkManager]:
        if self._spark_manager is None and PYSPARK_AVAILABLE:
            self._spark_manager = SparkManager(
                self.config, str(self._warehouse_dir.absolute())
            )
        return self._spark_manager

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
        return PartitionSpec(
            PartitionField(
                source_id=5,
                field_id=100,
                name="ingestion_day",
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

        try:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.create_table(
                table_id,
                schema=self._get_schema(),
                partition_spec=self._get_partition_spec(),
            )
            print(f"[Warehouse] Created table with partition spec")
        except Exception as e:
            self._table = self.catalog.load_table(f"{self.namespace}.{self.table_name}")
            print(f"[Warehouse] Loaded existing table: {e}")

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

    def get_table(self):
        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            self._table = self.catalog.load_table(table_id)
        return self._table

    def get_log_groups(self):
        result = self.query(limit=10000)

        groups = {}
        for i in range(result.num_rows):
            group_name = result.column("log_group_name")[i].as_py()
            if group_name and group_name not in groups:
                groups[group_name] = {
                    "logGroupName": group_name,
                    "creationTime": int(
                        result.column("ingestion_time")[i].as_py().timestamp() * 1000
                    )
                    if result.column("ingestion_time")[i].as_py()
                    else 0,
                    "metricFilterCount": 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}",
                    "storedBytes": 0,
                }
        return groups

    def get_log_streams(self, log_group_name: str):
        filter_expr = f"log_group_name == '{log_group_name}'"
        result = self.query(filter_expr=filter_expr, limit=10000)

        streams = {}
        for i in range(result.num_rows):
            stream_name = result.column("log_stream_name")[i].as_py()
            if stream_name and stream_name not in streams:
                streams[stream_name] = {
                    "logStreamName": stream_name,
                    "creationTime": int(
                        result.column("ingestion_time")[i].as_py().timestamp() * 1000
                    )
                    if result.column("ingestion_time")[i].as_py()
                    else 0,
                    "arn": f"arn:aws:logs:us-east-1:123456789012:log-stream:{log_group_name}/{stream_name}",
                    "storedBytes": 0,
                }
        return list(streams.values())

    def get_logs(
        self,
        log_group_name: str,
        log_stream_name: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ):
        filter_expr = f"log_group_name == '{log_group_name}'"

        if log_stream_name:
            filter_expr += f" AND log_stream_name == '{log_stream_name}'"

        if start_time:
            filter_expr += f" AND timestamp >= {start_time * 1000}"
        if end_time:
            filter_expr += f" AND timestamp <= {end_time * 1000}"

        result = self.query(filter_expr=filter_expr, limit=limit)

        events = []
        for i in range(result.num_rows):
            ts = result.column("timestamp")[i].as_py()
            events.append(
                {
                    "timestamp": int(ts.timestamp() * 1000) if ts else 0,
                    "message": result.column("message")[i].as_py() or "",
                    "ingestionTime": int(
                        result.column("ingestion_time")[i].as_py().timestamp() * 1000
                    )
                    if result.column("ingestion_time")[i].as_py()
                    else 0,
                }
            )

        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]

    def compact(self):
        if not self.compaction_enabled:
            return {"status": "skipped", "reason": "disabled"}

        try:
            import pyarrow.parquet as pq
            import pyarrow
            import tempfile
            import shutil

            table = self.get_table()
            data_path = Path(table.location()) / "data"

            if not data_path.exists():
                return {"status": "skipped", "reason": "no data path"}

            files = list(data_path.glob("**/*.parquet"))

            if len(files) <= self.compaction_max_files:
                return {
                    "status": "skipped",
                    "reason": f"only {len(files)} files, max is {self.compaction_max_files}",
                }

            print(f"[Compaction] Starting file compaction for {len(files)} files...")

            partition_files = {}
            for f in files:
                parent = f.parent
                if parent not in partition_files:
                    partition_files[parent] = []
                partition_files[parent].append(f)

            total_rewritten = 0
            for partition_path, parquet_files in partition_files.items():
                if len(parquet_files) < 5:
                    continue

                tables = [pq.read_table(fp) for fp in parquet_files]
                combined = pyarrow.concat_tables(tables)

                partition_name = partition_path.name
                output_name = f"merged_{int(time.time())}.parquet"
                output_path = partition_path / output_name

                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=".parquet"
                ) as tmp:
                    tmp_path = tmp.name

                pq.write_table(combined, tmp_path, compression="zstd")
                shutil.move(tmp_path, output_path)

                for fp in parquet_files:
                    fp.unlink()

                total_rewritten += 1
                print(
                    f"[Compaction] Compacted {len(parquet_files)} files into {output_name}"
                )

            return {
                "status": "success",
                "partitions_rewritten": total_rewritten,
                "message": f"Compacted {total_rewritten} partitions using PyArrow merge",
            }
        except Exception as e:
            print(f"[Compaction] Error: {e}")
            import traceback

            traceback.print_exc()
            return {"status": "error", "error": str(e)}

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

            return {"status": "success", "removed_partitions": removed}
        except Exception as e:
            print(f"[Retention] Error: {e}")
            return {"status": "error", "error": str(e)}

    def get_stats(self) -> Dict[str, Any]:
        if not PYICEBERG_AVAILABLE:
            return {"status": "unavailable"}

        try:
            table = self.get_table()

            warehouse_path = Path(table.location())
            data_path = warehouse_path / "data"

            data_files = []
            total_size = 0
            if data_path.exists():
                for f in data_path.rglob("*.parquet"):
                    data_files.append(str(f.relative_to(warehouse_path)))
                    total_size += f.stat().st_size

            partitions = {}
            if data_path.exists():
                for partition_dir in data_path.iterdir():
                    if partition_dir.is_dir() and partition_dir.name.startswith(
                        "ingestion_day="
                    ):
                        file_count = len(list(partition_dir.glob("*.parquet")))
                        partitions[partition_dir.name] = file_count

            return {
                "table": f"{self.namespace}.{self.table_name}",
                "location": table.location(),
                "data_files": len(data_files),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / 1024 / 1024, 2),
                "partition_spec": "ingestion_day (day)",
                "partitions": partitions,
                "compaction_enabled": self.compaction_enabled,
                "retention_enabled": self.retention_enabled,
                "retention_days": self.retention_days,
                "spark_available": PYSPARK_AVAILABLE,
            }
        except Exception as e:
            return {"error": str(e)}

    def _compaction_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.compaction_interval)
            if not self._stop_event.is_set():
                self.compact()

    def _retention_loop(self):
        while not self._stop_event.is_set():
            time.sleep(3600)
            if not self._stop_event.is_set():
                self.enforce_retention()

    def start_maintenance(self):
        if self.compaction_enabled and not self._compaction_thread:
            self._compaction_thread = threading.Thread(
                target=self._compaction_loop, daemon=True
            )
            self._compaction_thread.start()
            print(
                f"[Warehouse] Started compaction thread (interval: {self.compaction_interval}s)"
            )

        if self.retention_enabled and not self._retention_thread:
            self._retention_thread = threading.Thread(
                target=self._retention_loop, daemon=True
            )
            self._retention_thread.start()
            print(f"[Warehouse] Started retention enforcement thread")

    def stop_maintenance(self):
        self._stop_event.set()
        if self._compaction_thread:
            self._compaction_thread.join(timeout=5)
        if self._retention_thread:
            self._retention_thread.join(timeout=5)
        print("[Warehouse] Stopped maintenance threads")
