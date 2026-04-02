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
from cloudwatch_local_service.services.warehouse_metrics import warehouse_metrics

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

        # Create CloudWatch logs table
        table_id = f"{self.namespace}.{self.table_name}"
        try:
            self._table = self.catalog.create_table(
                table_id,
                schema=self._get_schema(),
                partition_spec=self._get_partition_spec(),
            )
            print(f"[Warehouse] Created CloudWatch logs table with partition spec")
        except Exception as e:
            print(f"[Warehouse] CloudWatch table exception: {type(e).__name__}: {e}")
            if self._check_and_repair_catalog():
                try:
                    self._table = self.catalog.load_table(table_id)
                    print(f"[Warehouse] Loaded existing CloudWatch logs table")
                except Exception as load_err:
                    print(f"[Warehouse] Load CloudWatch table failed: {load_err}")
                    raise
            else:
                print(f"[Warehouse] Repair failed")
                raise e

    def _check_and_repair_catalog(self) -> bool:
        import psycopg2

        try:
            table_path = self.get_table_path()
            metadata_dir = table_path / "metadata"

            if not metadata_dir.exists():
                print(f"[Warehouse] No metadata directory found at {metadata_dir}")
                return False

            metadata_files = sorted(metadata_dir.glob("*.metadata.json"))
            if not metadata_files:
                print(f"[Warehouse] No metadata files found in {metadata_dir}")
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
                        f"[Warehouse] Catalog mismatch: catalog={current_location}, latest={latest_metadata.name}"
                    )
                    cursor.execute(
                        "UPDATE iceberg_tables SET metadata_location = %s WHERE table_namespace = %s AND table_name = %s",
                        (new_location, self.namespace, self.table_name),
                    )
                    conn.commit()
                    print(f"[Warehouse] Updated catalog to {latest_metadata.name}")

            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"[Warehouse] Catalog check/repair failed: {e}")
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
                print(f"[Warehouse] No data files exist, will reset table")
                self._reset_table_for_missing_data()
                return

            print(f"[Warehouse] Found {len(existing_files)} data files on disk")
            self._table = None

        except Exception as e:
            print(f"[Warehouse] Error repairing missing data files: {e}")
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
                f"[Warehouse] Reset table for {self.namespace}.{self.table_name}, table will be recreated on next write"
            )
            self._table = None
            self._catalog = None

        except Exception as e:
            print(f"[Warehouse] Error resetting table: {e}")
            self._table = None

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

    def insert_logs(self, logs: List[Dict[str, Any]]):
        start_time = time.time()
        logs_count = len(logs)

        if not PYICEBERG_AVAILABLE:
            warehouse_metrics.record_insert(
                logs_count=logs_count,
                duration_seconds=time.time() - start_time,
                error=True,
            )
            raise RuntimeError("PyIceberg is not installed")

        if self._table is None:
            table_id = f"{self.namespace}.{self.table_name}"
            try:
                self._table = self.catalog.load_table(table_id)
            except Exception as e:
                err_str = str(e).lower()
                if (
                    "not found" in err_str
                    or "nosuch" in err_str
                    or "does not exist" in err_str
                ):
                    print(
                        f"[Warehouse] Table {table_id} not found, returning empty result"
                    )
                    warehouse_metrics.record_insert(
                        logs_count=logs_count,
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
                warehouse_metrics.record_insert(
                    logs_count=logs_count,
                    duration_seconds=time.time() - start_time,
                    error=True,
                )
                raise

        import pyarrow as pa

        label_columns = self._get_label_columns()

        def convert_timestamp(ts):
            """Convert various timestamp formats to datetime64[us]"""
            if ts is None:
                return None
            if isinstance(ts, int) or isinstance(ts, float):
                ts = int(ts)
                # Detect timestamp unit based on magnitude
                if ts > 1_000_000_000_000_000_000:  # nanoseconds
                    ts_seconds = ts / 1_000_000_000.0
                elif ts > 1_000_000_000_000_000:  # microseconds
                    ts_seconds = ts / 1_000_000.0
                elif ts > 1_000_000_000_000:  # milliseconds
                    ts_seconds = ts / 1_000.0
                elif ts > 1_000_000_000:  # seconds
                    ts_seconds = float(ts)
                else:
                    ts_seconds = float(ts)

                return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).replace(
                    tzinfo=None
                )
            return ts

        arrays = [
            pa.array([log.get("logGroupName", "") for log in logs]),
            pa.array([log.get("logStreamName", "") for log in logs]),
            pa.array(
                [convert_timestamp(log.get("timestamp")) for log in logs],
                type=pa.timestamp("us"),
            ),
            pa.array([log.get("message", "") for log in logs]),
            pa.array(
                [convert_timestamp(log.get("ingestionTime")) for log in logs],
                type=pa.timestamp("us"),
            ),
            pa.array([log.get("sequenceToken") for log in logs], type=pa.int64()),
        ]

        names = [
            "log_group_name",
            "log_stream_name",
            "timestamp",
            "message",
            "ingestion_time",
            "sequence_token",
        ]

        for label_col in label_columns:
            arrays.append(pa.array([log.get(f"label_{label_col}", "") for log in logs]))
            names.append(f"label_{label_col}")

        table = pa.table(arrays, names=names)
        print(f"[Warehouse] Inserting {len(logs)} logs to table {self.table_name}")
        self._table.append(table)

        # Record successful insert
        duration = time.time() - start_time
        warehouse_metrics.record_insert(
            logs_count=logs_count, duration_seconds=duration, error=False
        )

    def query(self, filter_expr: Optional[str] = None, limit: int = 100):
        start_time = time.time()

        if not PYICEBERG_AVAILABLE:
            warehouse_metrics.record_query(
                logs_returned=0, duration_seconds=time.time() - start_time, error=True
            )
            raise RuntimeError("PyIceberg is not installed")

        # Always reload table to get fresh data files
        table_id = f"{self.namespace}.{self.table_name}"

        try:
            self._table = self.catalog.load_table(table_id)
        except Exception as e:
            if "not found" in str(e).lower() or "nosuch" in str(e).lower():
                print(f"[Warehouse] Table {table_id} not found, returning empty result")
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
                f"[Warehouse] Running query on {self.table_name} with filter: {filter_expr}"
            )
            scan = self._table.scan()
            if filter_expr:
                scan = scan.filter(filter_expr)

            result = scan.to_arrow()
            logs_returned = len(result)
            print(
                f"[Warehouse] Query returned {logs_returned} rows from {self.table_name}"
            )

            # Record successful query
            duration = time.time() - start_time
            warehouse_metrics.record_query(
                logs_returned=logs_returned, duration_seconds=duration, error=False
            )
        except FileNotFoundError as e:
            print(f"[Warehouse] Data file not found, rebuilding table: {e}")
            warehouse_metrics.record_query(
                logs_returned=0, duration_seconds=time.time() - start_time, error=True
            )
            self._repair_missing_data_files()
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
        filter_expr_parts = []
        if log_group_name:
            filter_expr_parts.append(f"log_group_name == '{log_group_name}'")

        if log_stream_name:
            filter_expr_parts.append(f"log_stream_name == '{log_stream_name}'")

        if start_time:
            # logs are in microseconds, start_time is in milliseconds
            ts_start = datetime.fromtimestamp(
                start_time / 1000.0, tz=timezone.utc
            ).replace(tzinfo=None)
            print(f"[Warehouse] Filter start: {ts_start}")
            filter_expr_parts.append(f"timestamp >= {int(start_time * 1000)}")
        if end_time:
            ts_end = datetime.fromtimestamp(end_time / 1000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
            print(f"[Warehouse] Filter end: {ts_end}")
            filter_expr_parts.append(f"timestamp <= {int(end_time * 1000)}")

        if start_time:
            # Get everything and filter in memory if Iceberg filter is being tricky
            print(f"[Warehouse] Requesting all data due to filtering issues")
            filter_expr = None
        else:
            filter_expr = " AND ".join(filter_expr_parts) if filter_expr_parts else None

        result = self.query(filter_expr=filter_expr, limit=limit)

        events = []
        for i in range(result.num_rows):
            ts = result.column("timestamp")[i].as_py()
            ts_ms = int(ts.timestamp() * 1000) if ts else 0

            # Manual in-memory timestamp filtering
            if start_time and ts_ms < start_time:
                continue
            if end_time and ts_ms > end_time:
                continue

            event = {
                "timestamp": ts_ms,
                "message": result.column("message")[i].as_py() or "",
                "ingestionTime": int(
                    result.column("ingestion_time")[i].as_py().timestamp() * 1000
                )
                if result.column("ingestion_time")[i].as_py()
                else 0,
            }

            try:
                event["logGroupName"] = result.column("log_group_name")[i].as_py() or ""
            except:
                pass
            try:
                event["logStreamName"] = (
                    result.column("log_stream_name")[i].as_py() or ""
                )
            except:
                pass

            label_columns = self._get_label_columns()
            for label_col in label_columns:
                try:
                    event[f"label_{label_col}"] = (
                        result.column(f"label_{label_col}")[i].as_py() or ""
                    )
                except:
                    event[f"label_{label_col}"] = ""

            events.append(event)

        events.sort(key=lambda x: x.get("timestamp", 0))
        return events[:limit]

    def compact(self):
        """
        Compaction is currently disabled via direct file manipulation because it breaks Iceberg metadata.
        Iceberg requires manifest updates when files are deleted or added.
        """
        warehouse_metrics.record_compaction()
        return {"status": "skipped", "reason": "unsafe file-level compaction disabled"}

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

            # Get log groups and streams count for metrics
            try:
                log_groups = self.get_log_groups()
                log_groups_count = len(log_groups)
                # Count streams across all groups
                log_streams_count = 0
                for group_name in log_groups:
                    try:
                        streams = self.get_log_streams(group_name)
                        log_streams_count += len(streams)
                    except:
                        pass
                # Update metrics cache
                warehouse_metrics.update_stats_cache(
                    log_groups=log_groups_count, log_streams=log_streams_count
                )
            except:
                log_groups_count = 0
                log_streams_count = 0

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
                "log_groups_count": log_groups_count,
                "log_streams_count": log_streams_count,
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
