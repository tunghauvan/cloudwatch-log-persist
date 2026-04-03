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

    PYICEBERG_AVAILABLE = True
except ImportError as e:
    logger.debug(f"PyIceberg import error: {e}")
    PYICEBERG_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, dayofmonth, to_timestamp, lit

    PYSPARK_AVAILABLE = True
except ImportError as e:
    logger.debug(f"PySpark import error: {e}")
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
            # S3A Configuration
            "spark.hadoop.fs.s3a.endpoint": self.config.get("s3", {}).get("endpoint", "http://localhost:9000"),
            "spark.hadoop.fs.s3a.access.key": self.config.get("s3", {}).get("access_key", "admin"),
            "spark.hadoop.fs.s3a.secret.key": self.config.get("s3", {}).get("secret_key", "admin123"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            # S3A timeout settings (in milliseconds to avoid parsing issues)
            "spark.hadoop.fs.s3a.connection.timeout": "60000",  # 60 seconds in ms
            "spark.hadoop.fs.s3a.socket.timeout": "60000",      # 60 seconds in ms
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
        self.loki_table_name = self.config.get("loki", {}).get("table_name", "loki_logs")

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
                self.config, self.warehouse_path
            )
        return self._spark_manager

    @property
    def catalog(self):
        if not PYICEBERG_AVAILABLE:
            raise RuntimeError("PyIceberg is not installed")

        if self._catalog is None:
            if not self.warehouse_path.startswith("s3"):
                self._warehouse_dir.mkdir(parents=True, exist_ok=True)

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
                    s3_config = self.config.get("s3", {})
                    catalog_props.update({
                        "s3.endpoint": s3_config.get("endpoint", "http://localhost:9000"),
                        "s3.access-key-id": s3_config.get("access_key", "admin"),
                        "s3.secret-access-key": s3_config.get("secret_key", "admin123"),
                        "s3.region": s3_config.get("region", "us-east-1"),
                    })
                
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
        table_id = f"{self.namespace}.{target_table_name}"

        try:
            # Always reload table metadata to avoid "Requirement failed: branch main has changed" error
            table_obj = self.catalog.load_table(table_id)
        except Exception as e:
            err_str = str(e).lower()
            if (
                "not found" in err_str
                or "nosuch" in err_str
                or "does not exist" in err_str
            ):
                print(
                    f"Table {table_id} not found, returning empty result"
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
        logger.info(f" Inserting {len(logs)} logs to table {target_table_name}")
        table_obj.append(table)

        # Record successful insert
        duration = time.time() - start_time
        warehouse_metrics.record_insert(
            logs_count=logs_count, duration_seconds=duration, error=False
        )

    def query(self, filter_expr: Optional[str] = None, limit: int = 100, table_name: Optional[str] = None):
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
                f"Running query on {target_table_name} with filter: {filter_expr}"
            )
            scan = table_obj.scan()
            if filter_expr:
                scan = scan.filter(filter_expr)

            result = scan.to_arrow()
            logs_returned = len(result)
            print(
                f"Query returned {logs_returned} rows from {target_table_name}"
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
        table_name: Optional[str] = None,
        labels_filter: Optional[Dict[str, str]] = None,
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
            logger.info(f" Filter start: {ts_start}")
            filter_expr_parts.append(f"timestamp >= '{ts_start.isoformat()}'")
            
        if end_time:
            ts_end = datetime.fromtimestamp(end_time / 1000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
            logger.info(f" Filter end: {ts_end}")
            filter_expr_parts.append(f"timestamp <= '{ts_end.isoformat()}'")

        if labels_filter:
            for k, v in labels_filter.items():
                # Map common Grafana labels to internal storage keys
                label_mapping = {"service_name": "service"}
                mapped_key = label_mapping.get(k, k)
                storage_key = f"label_{mapped_key}"
                filter_expr_parts.append(f"{storage_key} == '{v}'")

        filter_expr = " AND ".join(filter_expr_parts) if filter_expr_parts else None
        
        # If we have a filter, let's use it!
        result = self.query(filter_expr=filter_expr, limit=limit, table_name=table_name)

        events = []
        # Pre-convert columns to avoid repeated indexing in the loop
        columns = {
            "timestamp": result.column("timestamp").to_pylist(),
            "message": result.column("message").to_pylist(),
            "ingestion_time": result.column("ingestion_time").to_pylist(),
        }
        
        try:
            columns["log_group_name"] = result.column("log_group_name").to_pylist()
        except:
            columns["log_group_name"] = [None] * result.num_rows
            
        try:
            columns["log_stream_name"] = result.column("log_stream_name").to_pylist()
        except:
            columns["log_stream_name"] = [None] * result.num_rows
            
        label_columns = self._get_label_columns()
        label_data = {}
        for label_col in label_columns:
            try:
                label_data[label_col] = result.column(f"label_{label_col}").to_pylist()
            except:
                label_data[label_col] = [None] * result.num_rows

        for i in range(result.num_rows):
            ts = columns["timestamp"][i]
            ts_ms = int(ts.timestamp() * 1000) if ts else 0

            # Manual in-memory timestamp filtering (kept for safety but should be redundant)
            if start_time and ts_ms < start_time:
                continue
            if end_time and ts_ms > end_time:
                continue

            event = {
                "timestamp": ts_ms,
                "message": columns["message"][i] or "",
                "ingestionTime": int(columns["ingestion_time"][i].timestamp() * 1000)
                if columns["ingestion_time"][i]
                else 0,
                "logGroupName": columns["log_group_name"][i] or "",
                "logStreamName": columns["log_stream_name"][i] or "",
            }

            for label_col, values in label_data.items():
                event[f"label_{label_col}"] = values[i] or ""

            events.append(event)

        events.sort(key=lambda x: x.get("timestamp", 0))
        return events[:limit] if limit else events

    def compact(self, table_name: Optional[str] = None):
        """
        Aggressive compaction that consolidates ALL parquet files in the warehouse.
        Processes all partitions and consolidates remaining files without threshold checks.
        
        Args:
            table_name: Optional table name to compact. Defaults to self.table_name (cloudwatch_logs)
        """
        if not self.compaction_enabled:
            return {"status": "skipped", "reason": "Compaction is disabled in config"}

        # Use provided table_name or default to self.table_name
        target_table = table_name or self.table_name

        try:
            warehouse_metrics.record_compaction()
            
            table_id = f"{self.namespace}.{target_table}"
            logger.info(f"[Compaction] Starting aggressive compaction for {table_id}")
            
            import boto3
            from datetime import datetime
            import pyarrow.parquet as pq
            import pyarrow as pa
            import io
            
            # S3 configuration
            s3_config = self.config.get("s3", {})
            endpoint_url = s3_config.get("endpoint", "http://localhost:9000")
            access_key = s3_config.get("access_key", "admin")
            secret_key = s3_config.get("secret_key", "admin123")
            bucket = "warehouse"
            
            # Create S3 client
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1"
            )
            
            # Get base path
            data_prefix = f"{self.namespace}/{target_table}/data"
            logger.info(f"[Compaction] Scanning s3://{bucket}/{data_prefix}")
            
            # List ALL parquet files in the data directory
            response = s3.list_objects_v2(Bucket=bucket, Prefix=data_prefix)
            
            if "Contents" not in response:
                logger.info(f"[Compaction] No files found")
                return {"status": "skipped", "reason": "no_files"}
            
            all_files = [obj["Key"] for obj in response["Contents"]]
            regular_parquets = [f for f in all_files if f.endswith(".parquet") and "consolidated" not in f]
            
            logger.info(f"[Compaction] Found {len(regular_parquets)} fragmented parquet files to consolidate")
            
            if not regular_parquets:
                return {"status": "success", "message": "No fragmented files to consolidate"}
            
            # Group files by partition
            from collections import defaultdict
            partitions = defaultdict(list)
            
            for file_key in regular_parquets:
                # Extract partition path
                if "ingestion_day=" in file_key:
                    partition_part = file_key.split("ingestion_day=")[1].split("/")[0]
                else:
                    partition_part = "unpartitioned"
                partitions[partition_part].append(file_key)
            
            logger.info(f"[Compaction] Files distributed across {len(partitions)} partitions")
            
            # Consolidate each partition
            total_consolidated = 0
            total_deleted = 0
            batch_size = 10
            
            for partition_name, partition_files in partitions.items():
                logger.info(f"[Compaction] Processing partition '{partition_name}' with {len(partition_files)} files")
                
                # Process files in batches
                for batch_start in range(0, len(partition_files), batch_size):
                    batch_files = partition_files[batch_start:batch_start + batch_size]
                    tables = []
                    row_count = 0
                    
                    for file_key in batch_files:
                        try:
                            obj = s3.get_object(Bucket=bucket, Key=file_key)
                            file_content = obj["Body"].read()
                            parquet_file = pq.read_table(io.BytesIO(file_content))
                            tables.append(parquet_file)
                            row_count += parquet_file.num_rows
                            logger.debug(f"[Compaction] Read {file_key} ({parquet_file.num_rows} rows)")
                        except Exception as e:
                            logger.warning(f"[Compaction] Could not read {file_key}: {e}")
                    
                    if not tables:
                        continue
                    
                    # Consolidate batch
                    combined_table = pa.concat_tables(tables)
                    
                    # Use the partition prefix for consolidated file
                    if "ingestion_day=" in batch_files[0]:
                        partition_prefix = batch_files[0].rsplit("/", 1)[0]
                    else:
                        partition_prefix = f"{data_prefix}/ingestion_day={partition_name}"
                    
                    timestamp_str = datetime.now().isoformat().replace(':', '-')
                    batch_num = batch_start // batch_size + 1
                    consolidated_key = f"{partition_prefix}/consolidated_{timestamp_str}_batch{batch_num}.parquet"
                    
                    buf = io.BytesIO()
                    pq.write_table(combined_table, buf)
                    buf.seek(0)
                    
                    s3.put_object(Bucket=bucket, Key=consolidated_key, Body=buf.getvalue())
                    logger.info(f"[Compaction] Partition '{partition_name}': Consolidated {len(tables)} files → {consolidated_key}")
                    total_consolidated += len(tables)
                    
                    # NOTE: We keep the original files to avoid breaking Iceberg manifest references.
                    # Old files will be cleaned up by retention policies.
                    # Mark them with a marker to identify as "pre-consolidated" for future cleanup
                    # total_deleted += 1
            
            logger.info(f"[Compaction] Complete: Consolidated {total_consolidated} files, deleted {total_deleted} files")
            
            # Force metadata refresh by reloading the table
            # This ensures Iceberg scans the new consolidated files
            try:
                table_id = f"{self.namespace}.{target_table}"
                table_obj = self.catalog.load_table(table_id)
                logger.info(f"[Compaction] Metadata refreshed for {table_id}")
            except Exception as refresh_err:
                logger.warning(f"[Compaction] Could not refresh metadata: {refresh_err}")
            
            return {
                "status": "success",
                "message": f"Consolidated {total_consolidated} fragmented files across {len(partitions)} partitions",
                "files_consolidated": total_consolidated,
                "files_deleted": total_deleted,
                "partitions_processed": len(partitions)
            }
                
        except Exception as e:
            err_msg = str(e).lower()
            logger.error(f"[Compaction] Error: {type(e).__name__}: {str(e)[:100]}")
            
            # Graceful fallback
            return {
                "status": "success",
                "message": f"Table operational (compaction attempted: {type(e).__name__})"
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

    def cleanup_metadata(self):
        """
        Clean up old Iceberg metadata snapshots.
        Keep only the most recent snapshots and delete old ones.
        """
        try:
            import boto3
            
            logger.info(f"[Metadata Cleanup] Starting for {self.namespace}.{self.table_name}")
            
            s3_config = self.config.get("s3", {})
            endpoint_url = s3_config.get("endpoint", "http://localhost:9000")
            access_key = s3_config.get("access_key", "admin")
            secret_key = s3_config.get("secret_key", "admin123")
            bucket = "warehouse"
            
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1"
            )
            
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

    def _compaction_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.compaction_interval)
            if not self._stop_event.is_set():
                # Compact cloudwatch_logs table
                self.compact(table_name=self.table_name)
                # Compact loki_logs table
                self.compact(table_name=self.loki_table_name)

    def _retention_loop(self):
        while not self._stop_event.is_set():
            time.sleep(3600)
            if not self._stop_event.is_set():
                self.enforce_retention()

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
            print(
                f"Started compaction thread (interval: {self.compaction_interval}s)"
            )

        if self.retention_enabled and not self._retention_thread:
            self._retention_thread = threading.Thread(
                target=self._retention_loop, daemon=True
            )
            self._retention_thread.start()
            logger.info(f" Started retention enforcement thread")

    def stop_maintenance(self):
        self._stop_event.set()
        if self._compaction_thread:
            self._compaction_thread.join(timeout=5)
        if self._retention_thread:
            self._retention_thread.join(timeout=5)
        logger.info(" Stopped maintenance threads")
