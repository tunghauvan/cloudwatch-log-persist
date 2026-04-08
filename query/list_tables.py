import boto3
import yaml
import json
from pathlib import Path

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

s3_cfg = config.get('s3', {})
# Use localhost for local development
endpoint = 'http://localhost:9000'
access_key = s3_cfg.get('access_key', 'admin')
secret_key = s3_cfg.get('secret_key', 'admin123')
region = s3_cfg.get('region', 'ap-southeast-1')

# Connect to S3/MinIO
s3 = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

# List buckets
print("=" * 60)
print("BUCKETS IN MINIO:")
print("=" * 60)
try:
    response = s3.list_buckets()
    for bucket in response.get('Buckets', []):
        print(f"  - {bucket['Name']}")
except Exception as e:
    print(f"Error listing buckets: {e}")

# List warehouse contents
warehouse = config.get('warehouse', 's3://stag-log-warehouse/').rstrip('/')
bucket_name = warehouse.split('://')[1].split('/')[0]
prefix = '/'.join(warehouse.split('://')[1].split('/')[1:])

print(f"\n{'=' * 60}")
print(f"CONTENTS OF BUCKET: {bucket_name}")
print(f"PREFIX: {prefix}")
print(f"{'=' * 60}")

try:
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    paths = set()
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Extract top-level paths
            if prefix:
                key = key[len(prefix):].lstrip('/')
            parts = key.split('/')
            if len(parts) > 1:
                paths.add(parts[0])
    
    if paths:
        print("\nTop-level directories:")
        for path in sorted(paths):
            print(f"  - {path}/")
    else:
        print("\nNo directories found under this prefix.")
        
except Exception as e:
    print(f"Error listing objects: {e}")

# Check for Delta tables specifically
print(f"\n{'=' * 60}")
print("CHECKING FOR DELTA TABLES:")
print(f"{'=' * 60}")

namespaces_to_check = [
    'default',
    'loki',
    '',
]

for namespace in namespaces_to_check:
    if namespace:
        check_path = f"{prefix}/{namespace}" if prefix else namespace
        table_names = ['loki_logs', 'cloudwatch_logs', 'alb_logs']
        for table_name in table_names:
            key = f"{check_path}/{table_name}/_delta_log"
            try:
                response = s3.head_object(Bucket=bucket_name, Key=f"{key}/")
                print(f"  ✓ Found: {namespace}/{table_name}")
            except:
                pass
