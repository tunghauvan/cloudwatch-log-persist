import boto3
import yaml

# Load config
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

s3_cfg = config.get('s3', {})
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

bucket_name = 'stag-log-warehouse'
prefix = 'delta/'

print(f"Listing all objects in {bucket_name}/{prefix}:")
print("=" * 60)

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

all_keys = []
for page in pages:
    for obj in page.get('Contents', []):
        all_keys.append(obj['Key'])

if not all_keys:
    print("No objects found.")
else:
    for key in sorted(all_keys)[:50]:  # Show first 50
        print(f"  {key}")
    if len(all_keys) > 50:
        print(f"  ... and {len(all_keys) - 50} more objects")

print(f"\n{'=' * 60}")
print(f"Total objects: {len(all_keys)}")
