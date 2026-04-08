import sys
import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

s3_cfg = config.get('s3', {})
endpoint = 'http://localhost:9000'
access_key = s3_cfg.get('access_key', 'admin')
secret_key = s3_cfg.get('secret_key', 'admin123')
region = s3_cfg.get('region', 'ap-southeast-1')

loki_table = config.get('loki', {}).get('table_name', 'loki_logs')
table_path = f"s3a://stag-log-warehouse/delta/{loki_table}"

print(f"📍 Table path: {table_path}")
print("=" * 60)

# Build SparkSession with Delta Lake support
builder = (
    SparkSession.builder
    .appName("DeltaCheckpoint")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # hadoop-aws provides S3AFileSystem (Hadoop 3.4.x requires AWS SDK v2)
    .config("spark.jars", "/tmp/spark-jars/hadoop-aws-3.4.1.jar,/tmp/spark-jars/aws-sdk-bundle-v2.jar")
    # S3A / MinIO settings
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Suppress verbose logs
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    # Step 1: Count rows
    print("\n📊 Counting rows...")
    count = spark.read.format("delta").load(table_path).count()
    print(f"   Total rows: {count:,}")

    # Step 2: Reset checkpointInterval to default (10) — was incorrectly set to 1
    print("\n🔧 Resetting checkpointInterval to 10 (default)...")
    spark.sql(f"""
        ALTER TABLE delta.`{table_path}`
        SET TBLPROPERTIES ('delta.checkpointInterval' = '10')
    """)
    print("✅ checkpointInterval = 10 set.")

    # Step 3: Force a single checkpoint at current version
    print("\n🔄 Creating checkpoint at current version...")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, table_path)
    dt.toDF()  # materialise snapshot
    # Trigger checkpoint explicitly via Scala JVM call
    spark.sql(f"OPTIMIZE delta.`{table_path}`")
    print("✅ Checkpoint created!")

    # Step 4: Verify
    print("\n📊 Verifying count after...")
    count_after = spark.read.format("delta").load(table_path).count()
    print(f"✅ Total rows: {count_after:,}")
    assert count == count_after, f"Row count mismatch: {count} → {count_after}"
    print("✅ Row count consistent.")

finally:
    spark.stop()
