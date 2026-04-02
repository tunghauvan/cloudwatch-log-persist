import requests
import time
from datetime import datetime

# Loki API Base URL
LOKI_URL = "http://localhost:4588/loki/api/v1/query_range"

def query_loki(query, start_time_ns, end_time_ns, limit=1000):
    params = {
        'query': query,
        'start': str(start_time_ns),
        'end': str(end_time_ns),
        'limit': limit,
        'direction': 'backward'
    }
    
    print(f"  [Debug] Request: start={format_timestamp(start_time_ns)}, end={format_timestamp(end_time_ns)}, limit={limit}")
    try:
        response = requests.get(LOKI_URL, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error querying Loki: {e}")
        return None

def format_timestamp(ts_ns):
    """Convert nanoseconds to human readable datetime."""
    return datetime.fromtimestamp(int(ts_ns) / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def run_test_queries(query="{log_group=~\".+\"}"):
    now_ns = int(time.time() * 1e9)
    
    ranges = [
        ("Last 1 minute", now_ns - (60 * 1e9)),
        ("Last 5 minutes", now_ns - (5 * 60 * 1e9)),
        ("Last 1 hour", now_ns - (3600 * 1e9))
    ]
    
    print(f"--- Querying Loki for: {query} ---\n")
    
    for label, start_ns in ranges:
        print(f"Range: {label}")
        data = query_loki(query, int(start_ns), now_ns)
        
        if data and data.get('data', {}).get('result'):
            results = data['data']['result']
            total_logs = sum(len(r.get('values', [])) for r in results)
            print(f"  Found {total_logs} log lines.")
            
            # Find the latest log line across all streams
            latest_ts = 0
            latest_msg = ""
            
            for stream in results:
                values = stream.get('values', [])
                if values:
                    # Loki returns [timestamp_ns, message]
                    ts = int(values[0][0])
                    msg = values[0][1]
                    if ts > latest_ts:
                        latest_ts = ts
                        latest_msg = msg
            
            if latest_ts > 0:
                print(f"  Last log datetime: {format_timestamp(latest_ts)}")
                print(f"  Last log message : {latest_msg[:100].strip()}...")
        else:
            print("  No logs found in this range.")
        print("-" * 30)

if __name__ == "__main__":
    # You can change the query to match your specific log groups or labels
    # Example: {log_group="my-log-group"} or {job="fluent-bit"}
    target_query = "{log_group=~\".+\"}" 
    run_test_queries(target_query)
