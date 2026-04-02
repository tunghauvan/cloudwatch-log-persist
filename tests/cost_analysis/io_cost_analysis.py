#!/usr/bin/env python3
"""
System Cost Analysis: File I/O and Storage Efficiency
Tests to measure write overhead, file fragmentation, and I/O efficiency
"""
import boto3
import requests
import time
import os
import subprocess
import json
from pathlib import Path
from collections import defaultdict

client = boto3.client('logs', endpoint_url='http://localhost:4588', region_name='us-east-1',
                     aws_access_key_id='testing', aws_secret_access_key='testing')

BASE_DIR = Path("/Users/vantunghau/interspace/cloudwatch-log-persist/data-warehouse/default/cloudwatch_logs")

print("=" * 70)
print("SYSTEM COST ANALYSIS: FILE I/O & STORAGE EFFICIENCY")
print("=" * 70)

# === TEST 1: File Count During Incremental Writes ===
print("\n📊 TEST 1: File Creation Overhead During Writes")
print("-" * 70)

def count_files(path=BASE_DIR):
    """Count data and metadata files in warehouse"""
    try:
        result = subprocess.run(
            f"find {path} -type f | wc -l",
            shell=True, capture_output=True, text=True
        )
        return int(result.stdout.strip())
    except:
        return 0

def count_parquet_files(path=BASE_DIR):
    """Count only Parquet data files"""
    try:
        result = subprocess.run(
            f"find {path} -type f -name '*.parquet' | wc -l",
            shell=True, capture_output=True, text=True
        )
        return int(result.stdout.strip())
    except:
        return 0

def get_disk_usage(path=BASE_DIR):
    """Get human-readable disk usage"""
    try:
        result = subprocess.run(
            f"du -sh {path}",
            shell=True, capture_output=True, text=True
        )
        return result.stdout.strip().split('\t')[0]
    except:
        return "0B"

print(f"Baseline files: {count_files()} total, {count_parquet_files()} parquet")
print(f"Baseline disk usage: {get_disk_usage()}")

# Write incrementally and measure file creation
write_sizes = [100, 200, 500]
results = []

for size in write_sizes:
    now_ms = int(time.time() * 1000)
    group = f"io-test-{size}-{int(time.time() * 1000)}"
    stream = "data-stream"
    
    # Create events
    events = [
        {"timestamp": now_ms + (i*5), "message": f"Log {i}: " + "x"*500}
        for i in range(size)
    ]
    
    # Measure before
    files_before = count_files()
    parquet_before = count_parquet_files()
    disk_before = get_disk_usage()
    
    # Write
    start = time.time()
    client.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)
    put_time = time.time() - start
    
    # Flush
    requests.post("http://localhost:4588/flush")
    time.sleep(0.5)
    
    # Measure after
    files_after = count_files()
    parquet_after = count_parquet_files()
    disk_after = get_disk_usage()
    
    files_created = files_after - files_before
    parquet_created = parquet_after - parquet_before
    
    result = {
        "event_count": size,
        "put_time_ms": put_time * 1000,
        "files_created": files_created,
        "parquet_files": parquet_created,
        "avg_events_per_parquet": size / max(1, parquet_created),
        "disk_usage": disk_after
    }
    results.append(result)
    
    print(f"\n  📝 Writing {size:3d} events:")
    print(f"     ⏱️  PutLogEvents: {result['put_time_ms']:.1f}ms ({result['put_time_ms']/size:.2f}ms/event)")
    print(f"     📁 Files created: {files_created} total ({parquet_created} parquet)")
    print(f"     📊 Events per parquet: {result['avg_events_per_parquet']:.1f}")
    print(f"     💾 Disk usage: {disk_after}")

# === TEST 2: Batch Write Efficiency ===
print(f"\n📊 TEST 2: Batch Write Efficiency Analysis")
print("-" * 70)

batch_results = []
batch_sizes = [50, 100, 500, 1000]

for batch_size in batch_sizes:
    now_ms = int(time.time() * 1000)
    group = f"batch-io-{batch_size}-{int(time.time() * 1000)}"
    stream = "batch-stream"
    
    # Pre-flush to have clean baseline
    requests.post("http://localhost:4588/flush")
    time.sleep(0.2)
    
    files_before = count_parquet_files()
    
    # Single batch write
    events = [
        {"timestamp": now_ms + (i*2), "message": f"Batch {i}: msg"}
        for i in range(batch_size)
    ]
    
    start = time.time()
    client.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)
    write_time = time.time() - start
    
    requests.post("http://localhost:4588/flush")
    time.sleep(0.3)
    
    files_after = count_parquet_files()
    files_written_for_batch = files_after - files_before
    
    throughput = batch_size / write_time if write_time > 0 else 0
    
    batch_result = {
        "batch_size": batch_size,
        "write_time_ms": write_time * 1000,
        "throughput_events_per_sec": throughput,
        "files_per_batch": files_written_for_batch,
        "events_per_file": batch_size / max(1, files_written_for_batch)
    }
    batch_results.append(batch_result)
    
    print(f"\n  📦 Batch size: {batch_size:4d} events")
    print(f"     ⏱️  Write time: {batch_result['write_time_ms']:.1f}ms")
    print(f"     ⚡ Throughput: {batch_result['throughput_events_per_sec']:.0f} events/sec")
    print(f"     📁 Files created: {files_written_for_batch}")
    print(f"     📊 Events per file: {batch_result['events_per_file']:.1f}")

# === TEST 3: Multiple Streams File Overhead ===
print(f"\n📊 TEST 3: Multiple Stream Write Fragmentation")
print("-" * 70)

now_ms = int(time.time() * 1000)
base_group = f"multi-stream-{int(time.time() * 1000)}"
events_per_stream = 100
stream_count = 5

files_before = count_parquet_files()

# Write to multiple streams
for i in range(stream_count):
    events = [
        {"timestamp": now_ms + (j*5), "message": f"Stream{i}-Log{j}"}
        for j in range(events_per_stream)
    ]
    client.put_log_events(logGroupName=base_group, logStreamName=f"stream-{i}", logEvents=events)

requests.post("http://localhost:4588/flush")
time.sleep(0.5)

files_after = count_parquet_files()
total_files = files_after - files_before
total_events = stream_count * events_per_stream

print(f"\n  Writing {stream_count} streams × {events_per_stream} events:")
print(f"     📁 Total files created: {total_files}")
print(f"     📊 Total events: {total_events}")
print(f"     📈 Events per file: {total_events / max(1, total_files):.1f}")
print(f"     🔀 Fragmentation ratio: {total_files / stream_count:.2f} files/stream")

# === TEST 4: Metadata File Ratio ===
print(f"\n📊 TEST 4: Metadata vs Data File Ratio")
print("-" * 70)

try:
    # Count metadata files
    meta_result = subprocess.run(
        f"find {BASE_DIR} -type f -name '*.metadata.json' | wc -l",
        shell=True, capture_output=True, text=True
    )
    meta_count = int(meta_result.stdout.strip())
    
    # Count manifest files
    manifest_result = subprocess.run(
        f"find {BASE_DIR} -type f -name '*.manifest' | wc -l",
        shell=True, capture_output=True, text=True
    )
    manifest_count = int(manifest_result.stdout.strip())
    
    # Count avro files
    avro_result = subprocess.run(
        f"find {BASE_DIR} -type f -name '*.avro' | wc -l",
        shell=True, capture_output=True, text=True
    )
    avro_count = int(avro_result.stdout.strip())
    
    parquet_count = count_parquet_files()
    
    total_files = meta_count + manifest_count + avro_count + parquet_count
    
    print(f"\n  📂 File Type Breakdown:")
    print(f"     📄 Parquet (data): {parquet_count} ({parquet_count*100//max(1,total_files)}%)")
    print(f"     📑 Metadata JSON: {meta_count} ({meta_count*100//max(1,total_files)}%)")
    print(f"     📋 Manifest: {manifest_count} ({manifest_count*100//max(1,total_files)}%)")
    print(f"     🔤 Avro schema: {avro_count} ({avro_count*100//max(1,total_files)}%)")
    print(f"     ➕ Total files: {total_files}")
    
except Exception as e:
    print(f"  ❌ Error counting metadata files: {e}")

# === TEST 5: Write Performance Degradation ===
print(f"\n📊 TEST 5: Write Performance Over Multiple Flushes")
print("-" * 70)

degrade_results = []
for flush_num in range(5):
    now_ms = int(time.time() * 1000)
    group = f"degrade-test-{int(time.time() * 1000)}"
    stream = f"stream-{flush_num}"
    
    events = [
        {"timestamp": now_ms + (i*5), "message": f"Degradation test log {i}"}
        for i in range(200)
    ]
    
    start = time.time()
    client.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)
    put_time = time.time() - start
    
    requests.post("http://localhost:4588/flush")
    time.sleep(0.2)
    
    degrade_results.append({
        "flush_number": flush_num + 1,
        "write_time_ms": put_time * 1000
    })
    
    print(f"  Flush {flush_num + 1}: {put_time*1000:.1f}ms")

# Calculate trend
if len(degrade_results) > 1:
    first = degrade_results[0]['write_time_ms']
    last = degrade_results[-1]['write_time_ms']
    degradation = ((last - first) / first * 100) if first > 0 else 0
    trend = "📈 INCREASING" if degradation > 5 else "✅ STABLE" if degradation > -5 else "📉 IMPROVING"
    print(f"  {trend}: {degradation:+.1f}% change from first to last")

# === Summary ===
print(f"\n{'='*70}")
print("SUMMARY: File I/O Cost Analysis")
print(f"{'='*70}")

if batch_results:
    best_throughput = max(batch_results, key=lambda x: x['throughput_events_per_sec'])
    print(f"\n🏆 Best throughput: {best_throughput['batch_size']} events → {best_throughput['throughput_events_per_sec']:.0f} evt/sec")

    avg_events_per_file = sum(r['events_per_file'] for r in batch_results) / len(batch_results)
    print(f"📊 Average events per file: {avg_events_per_file:.1f}")
    print(f"   → {1/avg_events_per_file*1000:.2f} files per 1000 events")

total_disk = get_disk_usage()
print(f"💾 Total warehouse size: {total_disk}")

print(f"\n✅ I/O Analysis Complete")
print("="*70)
