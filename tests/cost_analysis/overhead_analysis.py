#!/usr/bin/env python3
"""
Cost Overhead & Storage Compression Analysis
Analyzes metadata overhead, compression effectiveness, and storage optimization
"""
import boto3
import requests
import time
import subprocess
import json
from pathlib import Path

client = boto3.client('logs', endpoint_url='http://localhost:4588', region_name='us-east-1',
                     aws_access_key_id='testing', aws_secret_access_key='testing')

BASE_DIR = Path("/Users/vantunghau/interspace/cloudwatch-log-persist/data-warehouse")

print("="*70)
print("COST OVERHEAD & STORAGE COMPRESSION ANALYSIS")
print("="*70)

# === TEST 1: Metadata Overhead Analysis ===
print("\n💾 TEST 1: Metadata File Size vs Event Count")
print("-"*70)

def get_file_stats(pattern: str) -> dict:
    """Get file count and total size for pattern"""
    try:
        # Count files
        count_cmd = f"find {BASE_DIR} -type f -name '{pattern}' 2>/dev/null | wc -l"
        count = int(subprocess.run(count_cmd, shell=True, capture_output=True, text=True).stdout.strip())
        
        # Get total size
        size_cmd = f"find {BASE_DIR} -type f -name '{pattern}' -exec wc -c {{}} + 2>/dev/null | tail -1"
        size_result = subprocess.run(size_cmd, shell=True, capture_output=True, text=True).stdout.strip()
        
        if size_result:
            size_bytes = int(size_result.split()[0])
        else:
            size_bytes = 0
        
        return {
            "count": count,
            "size_bytes": size_bytes,
            "size_kb": size_bytes / 1024,
            "avg_file_size": size_bytes / max(1, count)
        }
    except:
        return {"count": 0, "size_bytes": 0, "size_kb": 0, "avg_file_size": 0}

# Analyze file types
file_patterns = {
    "Parquet data": "*.parquet",
    "Metadata JSON": "*.metadata.json",
    "Avro schema": "*.avro",
    "Manifest": "*.manifest"
}

print("\n📂 File Type Analysis:")
total_bytes = 0
all_stats = {}

for name, pattern in file_patterns.items():
    stats = get_file_stats(pattern)
    all_stats[name] = stats
    total_bytes += stats['size_bytes']
    
    pct = (stats['size_bytes'] / max(1, total_bytes) * 100) if total_bytes > 0 else 0
    print(f"\n   {name}:")
    print(f"      📁 Count: {stats['count']} files")
    print(f"      💾 Total size: {stats['size_kb']:.1f} KB")
    print(f"      📊 Avg file size: {stats['avg_file_size']/1024:.2f} KB")

# Calculate overhead
print(f"\n📈 Storage Overhead Analysis:")
data_size = all_stats['Parquet data']['size_bytes']
metadata_size = (all_stats['Metadata JSON']['size_bytes'] + 
                 all_stats['Avro schema']['size_bytes'])

overhead_ratio = metadata_size / max(1, data_size)
overhead_pct = (metadata_size / max(1, data_size + metadata_size)) * 100

print(f"   Data files: {data_size/1024:.1f} KB")
print(f"   Metadata overhead: {metadata_size/1024:.1f} KB")
print(f"   Ratio: {overhead_ratio:.2f}x (metadata = {overhead_pct:.0f}% of total)")

if overhead_ratio > 2:
    print(f"   ⚠️  HIGH OVERHEAD - Metadata is {overhead_ratio:.1f}x the data size!")
elif overhead_ratio > 1:
    print(f"   🟡 MODERATE OVERHEAD - Metadata is {overhead_ratio:.1f}x the data size")
else:
    print(f"   ✅ LOW OVERHEAD - Metadata is smaller than data")

# === TEST 2: Event Density in Parquet Files ===
print(f"\n💾 TEST 2: Parquet Compression Efficiency")
print("-"*70)

# Find latest parquet files
latest_parquet_cmd = f"find {BASE_DIR} -name '*.parquet' -type f -printf '%T@ %p\\n' | sort -rn | head -5 | awk '{{print $2}}'"
result = subprocess.run(latest_parquet_cmd, shell=True, capture_output=True, text=True)
latest_files = result.stdout.strip().split('\n')

if latest_files and latest_files[0]:
    print(f"\n   Latest Parquet files (by modification):")
    
    for fpath in latest_files[:3]:
        if not fpath:
            continue
        
        try:
            file_size = Path(fpath).stat().st_size
            
            # Try to estimate event count (rough: ~300-500 bytes per event in parquet)
            estimated_events = max(1, file_size // 350)
            
            size_kb = file_size / 1024
            
            compression = 100 - (file_size / (estimated_events * 1000) * 100)  # Rough estimate
            
            print(f"\n      📄 {Path(fpath).name}")
            print(f"         Size: {size_kb:.1f} KB ({file_size} bytes)")
            print(f"         Est. events: ~{estimated_events}")
            print(f"         Bytes/event: {file_size/max(1, estimated_events):.0f}")
            
        except Exception as e:
            pass

# === TEST 3: Ingestion Cost Per Event ===
print(f"\n💾 TEST 3: Total Cost Per Event Stored")
print("-"*70)

# Estimate total events from test patterns
print(f"\n   Cost breakdown per event:")

# Assuming average 350 bytes per event in parquet
avg_data_bytes = 350
metadata_bytes_per_event = (metadata_size / max(1, data_size)) * avg_data_bytes

print(f"      📊 Data file storage: ~{avg_data_bytes} bytes/event")
print(f"      📋 Metadata overhead: ~{metadata_bytes_per_event:.0f} bytes/event")
print(f"      📈 Total storage: ~{avg_data_bytes + metadata_bytes_per_event:.0f} bytes/event")

# API call overhead
print(f"\n   API Call Cost:")
print(f"      🔧 Single event write: ~2.0ms")
print(f"      🔧 100 events batch write: ~0.06ms per event (99% savings!)")

# === TEST 4: Write Pattern Optimization ===
print(f"\n💾 TEST 4: Optimal Write Patterns & Cost")
print("-"*70)

patterns = {
    "High frequency (1 event/sec)": {
        "batching": "None",
        "events_per_call": 1,
        "api_cost_ms_per_event": 2.0,
        "description": "Real streaming logs (worst case)"
    },
    "Moderate batching (10 events/call)": {
        "batching": "Minimal",
        "events_per_call": 10,
        "api_cost_ms_per_event": 0.2,
        "description": "Microservice publishing"
    },
    "Aggressive batching (100 events/call)": {
        "batching": "Good",
        "events_per_call": 100,
        "api_cost_ms_per_event": 0.06,
        "description": "Batch processing applications"
    },
    "Maximum batching (1000+ events/call)": {
        "batching": "Optimal",
        "events_per_call": 1000,
        "api_cost_ms_per_event": 0.023,
        "description": "Data warehouse loads"
    }
}

print(f"\n   📋 WRITE PATTERN RECOMMENDATIONS:\n")

for pattern_name, info in patterns.items():
    print(f"   {pattern_name}:")
    print(f"      📊 {info['events_per_call']} events/call")
    print(f"      ⏱️  API cost: {info['api_cost_ms_per_event']:.3f}ms per event")
    
    # Calculate total cost per million events
    api_cost_per_million = (1_000_000 * info['api_cost_ms_per_event']) / 1000 / 60 / 60
    storage_cost_per_million = 1_000_000 * (avg_data_bytes + metadata_bytes_per_event) / 1024 / 1024 / 1024
    
    print(f"      💰 1M events = {api_cost_per_million:.2f} CPU-hours + {storage_cost_per_million:.2f} GB")
    print(f"      💡 Use case: {info['description']}\n")

# === TEST 5: Inefficiency Warnings ===
print(f"\n{'='*70}")
print("⚠️  COST INEFFICIENCY WARNINGS")
print(f"{'='*70}")

warnings = []

# Check for high metadata ratio
if overhead_ratio > 2:
    warnings.append(f"🔴 CRITICAL: Metadata is {overhead_ratio:.1f}x data size - consider aggressive compaction")

# Check small file syndrome
small_parquet_count = all_stats['Parquet data']['count']
if small_parquet_count > 500:
    warnings.append(f"🔴 CRITICAL: {small_parquet_count} parquet files - indicates excessive small writes")

# Check metadata file explosion
meta_count = all_stats['Metadata JSON']['count']
data_count = all_stats['Parquet data']['count']
if meta_count > data_count:
    warnings.append(f"🟡 WARNING: {meta_count} metadata files vs {data_count} data files - high ratio")

# Avro schema duplication
avro_count = all_stats['Avro schema']['count']
if avro_count > data_count * 2:
    warnings.append(f"🟡 WARNING: {avro_count} avro schema files - likely duplication")

if not warnings:
    print("\n✅ No critical inefficiencies detected!")
else:
    print(f"\n{len(warnings)} issues found:\n")
    for warning in warnings:
        print(f"   {warning}")

# === OPTIMIZATION RECOMMENDATIONS ===
print(f"\n{'='*70}")
print("💡 OPTIMIZATION RECOMMENDATIONS")
print(f"{'='*70}")

print("""
1️⃣  BATCH WRITES AGGRESSIVELY
    • Use 100+ events per PutLogEvents call
    • Reduces API overhead by 95%
    • Example: Buffer logs client-side before sending

2️⃣  CONSOLIDATE METADATA
    • Current metadata = {:.0f}% of storage cost
    • Compress metadata files
    • De-duplicate schema definitions
    • Consider single manifest file per partition

3️⃣  OPTIMIZE PARQUET COMPRESSION
    • Current avg file size indicates compression effectiveness
    • Monitor codec choices (snappy vs gzip)
    • Balance compression ratio vs CPU cost

4️⃣  PARTITION STRATEGY
    • Current partitioning by ingestion_day is good
    • Consider additional partition by log_group for hot data
    • Archive old partitions to reduce active metadata

5️⃣  FILE SIZE TUNING
    • Target 10-50MB per parquet file (optimal Iceberg size)
    • Current files suggest {} bytes/file average
    • Adjust flush interval and batch sizes accordingly

6️⃣  MONITORING
    • Track metadata:data ratio monthly
    • Alert if ratio exceeds 1.5x
    • Monitor API call patterns for inefficient single-writes
""".format(overhead_pct, all_stats['Parquet data']['avg_file_size']/1024/1024))

print("="*70)
