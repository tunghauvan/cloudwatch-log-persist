import logging
from typing import Any, Dict

logger = logging.getLogger("service.warehouse")

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
