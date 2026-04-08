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

# === FILTERS: Update these to query different data ===
log_group_filter = None  # e.g. "my-log-group"
log_stream_filter = None  # e.g. "lambda-logs"
start_time_filter = None  # e.g. "2026-04-08 00:00:00" or None for all
end_time_filter = None    # e.g. "2026-04-09 00:00:00" or None for all
message_pattern = None    # e.g. "ERROR" to search in message field
label_filters = {"service": "worker"}  # e.g. {"service": "worker", "env": "prod"}
limit_rows = None         # e.g. 1000 to limit result rows, or None for all

# Build WHERE clause
conditions = []
params = []

if log_group_filter:
    conditions.append("log_group_name = ?")
    params.append(log_group_filter)
if log_stream_filter:
    conditions.append("log_stream_name = ?")
    params.append(log_stream_filter)
if start_time_filter:
    conditions.append("timestamp >= ?")
    params.append(start_time_filter)
if end_time_filter:
    conditions.append("timestamp <= ?")
    params.append(end_time_filter)
if message_pattern:
    conditions.append("message ILIKE ?")
    params.append(f"%{message_pattern}%")
if label_filters:
    for label_key, label_value in label_filters.items():
        conditions.append(f"label_{label_key} = ?")
        params.append(label_value)

where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
limit_clause = f" LIMIT {limit_rows}" if limit_rows else ""

# Example queries
queries = [
    # Count total rows (fast, uses partition pruning)
    (f"SELECT COUNT(*) as total_rows FROM delta_scan('{table_path}'){where_clause}", params, "Total rows"),
    # Show sample logs with filters
    (f"SELECT timestamp, log_group_name, log_stream_name, message FROM delta_scan('{table_path}'){where_clause} ORDER BY timestamp DESC{limit_clause}", params, "Sample logs"),
    # Volume by log level
    (f"""
        WITH level_data AS (
            SELECT 
                CASE 
                    WHEN message ILIKE '%ERROR%' OR message ILIKE '%error%' THEN 'error'
                    WHEN message ILIKE '%WARN%' OR message ILIKE '%warn%' THEN 'warn'
                    WHEN message ILIKE '%DEBUG%' OR message ILIKE '%debug%' THEN 'debug'
                    WHEN message ILIKE '%INFO%' OR message ILIKE '%info%' THEN 'info'
                    ELSE 'other'
                END as level,
                COUNT(*) as count
            FROM delta_scan('{table_path}'){where_clause}
            GROUP BY level
        )
        SELECT level, count, ROUND(100.0 * count / SUM(count) OVER (), 1) as percent
        FROM level_data
        ORDER BY count DESC
    """, params, "Volume by log level"),
    # Volume by hour and level (for chart visualization)
    (f"""
        WITH hourly_level as (
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                CASE 
                    WHEN message ILIKE '%ERROR%' OR message ILIKE '%error%' THEN 'error'
                    WHEN message ILIKE '%WARN%' OR message ILIKE '%warn%' THEN 'warn'
                    WHEN message ILIKE '%DEBUG%' OR message ILIKE '%debug%' THEN 'debug'
                    WHEN message ILIKE '%INFO%' OR message ILIKE '%info%' THEN 'info'
                    ELSE 'other'
                END as level,
                COUNT(*) as count
            FROM delta_scan('{table_path}'){where_clause}
            GROUP BY DATE_TRUNC('hour', timestamp),
                CASE 
                    WHEN message ILIKE '%ERROR%' OR message ILIKE '%error%' THEN 'error'
                    WHEN message ILIKE '%WARN%' OR message ILIKE '%warn%' THEN 'warn'
                    WHEN message ILIKE '%DEBUG%' OR message ILIKE '%debug%' THEN 'debug'
                    WHEN message ILIKE '%INFO%' OR message ILIKE '%info%' THEN 'info'
                    ELSE 'other'
                END
        )
        SELECT hour, level, count
        FROM hourly_level
        ORDER BY hour DESC, count DESC
        LIMIT 96
    """, params, "Volume by hour and level"),
    # Volume by log stream
    (f"SELECT log_group_name, log_stream_name, COUNT(*) as row_count FROM delta_scan('{table_path}'){where_clause} GROUP BY log_group_name, log_stream_name ORDER BY row_count DESC LIMIT 20", params, "Volume by log_stream (top 20)"),
    # Volume by hour
    (f"SELECT DATE_TRUNC('hour', timestamp) as hour, COUNT(*) as row_count FROM delta_scan('{table_path}'){where_clause} GROUP BY DATE_TRUNC('hour', timestamp) ORDER BY hour DESC LIMIT 24", params, "Volume by hour (last 24h)"),
    # Volume by label
    (f"SELECT label_service, COUNT(*) as row_count FROM delta_scan('{table_path}'){where_clause} GROUP BY label_service ORDER BY row_count DESC", params, "Volume by service"),
]

try:
    for query, query_params, label in queries:
        start_time = time.time()
        result = conn.execute(query, query_params).fetchall()
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n{'='*100}")
        print(f"📊 {label}")
        print(f"{'='*100}")
        
        if label == "Total rows":
            print(f"✅ Total: {result[0][0]:,} logs")
        elif "sample" in label.lower():
            print(f"✅ {len(result)} rows returned (showing first 10):")
            for row in result[:10]:
                print(f"   {row}")
        elif "log level" in label.lower():
            # Display log level distribution like Loki
            if result:
                total = sum(row[1] for row in result)
                print(f"Total: {total:,}")
                print()
                # Create visual bar chart
                for level, count, percent in result:
                    bar_len = int(percent / 2)
                    bar = "█" * bar_len
                    color_map = {"error": "🔴", "warn": "🟡", "debug": "🟠", "info": "🟢", "other": "⚪"}
                    icon = color_map.get(level, "⚪")
                    print(f"  {icon} {level:8} {bar} {count:>8,} ({percent:>5.1f}%)")
        elif "hour and level" in label.lower():
            # Show table with hour and level breakdown
            if result:
                current_hour = None
                print(f"\n{'Hour':<25} {'info':<12} {'debug':<12} {'warn':<12} {'error':<12}")
                print("-" * 75)
                hour_data = {}
                for hour, level, count in result:
                    if hour not in hour_data:
                        hour_data[hour] = {"info": 0, "debug": 0, "warn": 0, "error": 0, "other": 0}
                    hour_data[hour][level] = count
                
                for hour in sorted(hour_data.keys(), reverse=True)[:12]:  # Show last 12 hours
                    data = hour_data[hour]
                    print(f"{str(hour):<25} {data.get('info', 0):<12,} {data.get('debug', 0):<12,} {data.get('warn', 0):<12,} {data.get('error', 0):<12,}")
        elif "volume" in label.lower() or "hour" in label.lower() or "stream" in label.lower() or "service" in label.lower():
            # Show as table
            if result:
                col_names = conn.execute(query, query_params).description
                header = " | ".join([f"{col[0]:<20}" for col in col_names])
                print(header)
                print("-" * len(header))
                for row in result:
                    print(" | ".join([f"{str(val):<20}" for val in row]))
            else:
                print("No data")
        else:
            print(f"✅ {len(result)} rows")
            for row in result:
                print(f"   {row}")
        
        print(f"⏱️  Duration: {duration:.2f}s")
except Exception as e:
    print(f"Error querying table: {e}")
    print("The table may not exist or be empty.")

# Close connection
conn.close()