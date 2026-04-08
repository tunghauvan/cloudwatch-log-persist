import duckdb
import yaml
import time

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

s3_cfg = config.get('s3', {})
endpoint = 'http://localhost:9000'  # Override for local MinIO
access_key = s3_cfg.get('access_key', 'admin')
secret_key = s3_cfg.get('secret_key', 'admin123')
region = s3_cfg.get('region', 'us-east-1')

# Connect to DuckDB
conn = duckdb.connect()

# Install and load extensions
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("INSTALL delta; LOAD delta;")

# Create S3 secret
ep = endpoint.replace("https://", "").replace("http://", "")
secret_sql = f"""
CREATE OR REPLACE SECRET _s3_secret (
    TYPE S3,
    KEY_ID '{access_key}',
    SECRET '{secret_key}',
    REGION '{region}',
    ENDPOINT '{ep}',
    URL_STYLE 'path',
    USE_SSL false
);
"""
conn.execute(secret_sql)

# Query the Delta table - use s3:// protocol with explicit endpoint
# The table is stored at: s3://stag-log-warehouse/delta/loki_logs
warehouse_path = config.get('warehouse', 's3://stag-log-warehouse/').rstrip('/')
# Convert s3a:// to s3:// for DuckDB
if warehouse_path.startswith('s3a://'):
    warehouse_path = warehouse_path.replace('s3a://', 's3://', 1)

# Data is stored under the 'delta' subfolder in the warehouse
loki_table = config.get('loki', {}).get('table_name', 'loki_logs')
table_path = f"{warehouse_path}/delta/{loki_table}"
query = f"SELECT COUNT(*) as total_rows FROM delta_scan('{table_path}')"
try:
    # init time to measure query duration
    start_time = time.time()
    result = conn.execute(query).fetchone()
    end_time = time.time()
    duration = end_time - start_time
    print(f"✅ Total rows: {result[0]:,}")
    print(f"⏱️  Query duration: {duration:.2f} seconds")
    print(f"Total rows: {result[0]}")
except Exception as e:
    print(f"Error querying table: {e}")
    print("The table may not exist or be empty.")

# Close connection
conn.close()