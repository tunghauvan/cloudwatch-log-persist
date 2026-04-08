import duckdb
import yaml

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

s3_cfg = config.get('s3', {})
endpoint = 'http://localhost:9000'  # Override for local MinIO
access_key = s3_cfg.get('access_key', 'admin')
secret_key = s3_cfg.get('secret_key', 'admin123')
region = s3_cfg.get('region', 'ap-southeast-1')

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

# Table path
warehouse_path = config.get('warehouse', 's3://stag-log-warehouse/').rstrip('/')
if warehouse_path.startswith('s3a://'):
    warehouse_path = warehouse_path.replace('s3a://', 's3://', 1)

loki_table = config.get('loki', {}).get('table_name', 'loki_logs')
table_path = f"{warehouse_path}/delta/{loki_table}"

print(f"📍 Table path: {table_path}")
print("=" * 60)

# Step 1: Create checkpoint
print("\n🔄 Creating checkpoint...")
try:
    conn.execute(f"CALL delta_create_checkpoint('{table_path}');")
    print("✅ Checkpoint created successfully!")
except Exception as e:
    print(f"⚠️  Checkpoint creation failed: {e}")

# Step 2: Query total rows
print("\n📊 Querying total rows after checkpoint...")
query = f"SELECT COUNT(*) as total_rows FROM delta_scan('{table_path}')"
try:
    result = conn.execute(query).fetchone()
    print(f"✅ Total rows: {result[0]:,}")
except Exception as e:
    print(f"❌ Error querying table: {e}")

# Close connection
conn.close()
