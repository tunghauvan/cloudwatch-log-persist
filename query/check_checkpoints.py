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

s3 = boto3.client(
    's3',
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

bucket_name = 'stag-log-warehouse'
log_prefix = 'delta/loki_logs/_delta_log/'

# Check for checkpoint files
print("Checking for checkpoint files in _delta_log/:")
print("=" * 60)

try:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=log_prefix)
    
    checkpoints = []
    log_files = []
    
    for obj in response.get('Contents', []):
        key = obj['Key'].split('/')[-1]
        if '_checkpoint' in key:
            checkpoints.append(key)
        elif key.endswith('.json'):
            log_files.append(key)
    
    print(f"📋 Transaction log files: {len(log_files)}")
    if log_files:
        print(f"   Từ: {min(log_files)}")
        print(f"   Đến: {max(log_files)}")
    
    print(f"\n✓ Checkpoint files: {len(checkpoints)}")
    for ckpt in sorted(checkpoints):
        print(f"   - {ckpt}")
    
    if not checkpoints:
        print("   ⚠️  Không có checkpoint - DuckDB phải scan tất cả {len(log_files)} files!")
        
except Exception as e:
    print(f"Error: {e}")
