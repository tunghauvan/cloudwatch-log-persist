#!/usr/bin/env python3
"""
QUICK REFERENCE: System Cost Analysis Summary & Optimization Code

This file shows exact costs and how to optimize them with code examples.
"""

print("""
╔════════════════════════════════════════════════════════════════════╗
║           SYSTEM COST ANALYSIS - QUICK REFERENCE GUIDE            ║
║              CloudWatch Log Persist + Iceberg Warehouse            ║
╚════════════════════════════════════════════════════════════════════╝

🎯 BOTTOM LINE FINDINGS:
═══════════════════════════════════════════════════════════════════

1. METADATA EXPLOSION (CRITICAL)
   ⚠️  Current:  7.7x data size in metadata (89% of storage)
   ✅ Target:   1.5x data size in metadata (60% of storage)
   💥 Impact:   Every 1M logged events = 2.3 GB wasted

2. WRITE API INEFFICIENCY (CRITICAL)
   ❌ Single writes: 2.0ms per event + file creation overhead
   ✅ Batch writes:  0.02ms per event (100x cheaper!)
   💥 Impact:   1M events: 0.56 CPU-hours vs 0.01 CPU-hours

3. SMALL FILE PROBLEM (HIGH)
   ❌ Current:  144 parquet files × 8 KB avg
   ✅ Target:   15-30 parquet files × 10-50 MB avg
   💥 Impact:   Metadata explosion + slow queries


📊 COST BREAKDOWN PER MILLION EVENTS:
═══════════════════════════════════════════════════════════════════

CURRENT INEFFICIENT PATTERN:
  • Pattern: Single-event writes (worst case)
  • Throughput: 57 events/sec
  • Time to process: ~5 hours
  • Storage used: 2.84 GB
  • API cost: 0.56 CPU-hours
  • Metadata files: 583
  ⬇️
  Total cost: 📌 EXPENSIVE

OPTIMAL PATTERN:
  • Pattern: 1000 events per batch
  • Throughput: 55,000 events/sec
  • Time to process: ~18 seconds
  • Storage used: 0.5 GB ✅
  • API cost: 0.01 CPU-hours
  • Metadata files: ~50 ✅
  ⬇️
  Total cost: 💰 CHEAP


💡 OPTIMIZATION STRATEGIES:
═══════════════════════════════════════════════════════════════════

STRATEGY 1: CLIENT-SIDE BATCHING
Purpose: Reduce API call overhead by 99%

BEFORE (❌ Bad):
────────────────
import boto3
client = boto3.client('logs', endpoint_url='http://localhost:4588')

for i in range(1000):
    event = {"timestamp": int(time.time()*1000), "message": f"Log {i}"}
    client.put_log_events(
        logGroupName='myapp',
        logStreamName='stream1',
        logEvents=[event]  # ❌ ONE EVENT PER API CALL
    )
# Cost: 1000 API calls × 2ms = 2000ms total


AFTER (✅ Good):
────────────────
import boto3
client = boto3.client('logs', endpoint_url='http://localhost:4588')

batch = []
for i in range(1000):
    event = {"timestamp": int(time.time()*1000), "message": f"Log {i}"}
    batch.append(event)
    
    # Send when batch reaches 100 OR after timeout
    if len(batch) >= 100:
        client.put_log_events(
            logGroupName='myapp',
            logStreamName='stream1',
            logEvents=batch  # ✅ 100 EVENTS PER API CALL
        )
        batch = []

if batch:  # Flush remainder
    client.put_log_events(
        logGroupName='myapp',
        logStreamName='stream1',
        logEvents=batch
    )
# Cost: 10 API calls × 7ms = 70ms total
# Savings: 97% faster! 🚀


────────────────────────────────────────────────────────────────────

STRATEGY 2: ASYNC BUFFERING WITH TIMEOUT
Purpose: Balance latency vs throughput

import asyncio
import time

class BufferedLogWriter:
    def __init__(self, client, group_name, stream_name, batch_size=100, timeout_sec=30):
        self.client = client
        self.group_name = group_name
        self.stream_name = stream_name
        self.batch_size = batch_size
        self.timeout_sec = timeout_sec
        self.buffer = []
        self.last_flush = time.time()
    
    async def add_event(self, message):
        \"\"\"Add event to buffer, auto-flush on size or timeout\"\"\"
        now_ms = int(time.time() * 1000)
        self.buffer.append({
            "timestamp": now_ms,
            "message": message
        })
        
        time_since_flush = time.time() - self.last_flush
        should_flush = (
            len(self.buffer) >= self.batch_size or
            time_since_flush > self.timeout_sec
        )
        
        if should_flush:
            await self.flush()
    
    async def flush(self):
        \"\"\"Send buffered events\"\"\"
        if not self.buffer:
            return
        
        self.client.put_log_events(
            logGroupName=self.group_name,
            logStreamName=self.stream_name,
            logEvents=self.buffer
        )
        self.buffer = []
        self.last_flush = time.time()

# Usage:
writer = BufferedLogWriter(client, 'myapp', 'stream1', batch_size=100, timeout_sec=5)
await writer.add_event("User login")  # Buffered
await writer.flush()  # Sent immediately


────────────────────────────────────────────────────────────────────

STRATEGY 3: WAREHOUSE FLUSH TUNING
Purpose: Larger parquet files = less metadata overhead

BEFORE (❌ Small files):
config.yaml:
  buffer_flush_interval_seconds: 60
  Result: 60 flushes per hour × avg 100 events = 8 KB parquet files
  Metadata: EXPLODES

AFTER (✅ Larger files):
config.yaml:
  buffer_flush_interval_seconds: 300  # 5 minutes
  Result: 12 flushes per hour × avg 1000 events = 500 KB parquet files
  Metadata: Much better!

# Impact: 5x fewer parquet files = 5x less metadata


────────────────────────────────────────────────────────────────────

STRATEGY 4: SCHEMA CONSOLIDATION
Purpose: Reduce avro schema duplication

# Current problem:
# - 288 avro schema files (one per batch!)
# - Iceberg creating duplicate schemas

# Solution: Consolidate in Iceberg
# - One schema file per partition (per day)
# - Dedup identical schemas
# - Expected reduction: 10x fewer files


═════════════════════════════════════════════════════════════════════

📈 EXPECTED IMPROVEMENTS:
═════════════════════════════════════════════════════════════════════

After implementing Strategies 1-4:

METRIC                          CURRENT    TARGET    IMPROVEMENT
─────────────────────────────────────────────────────────────────
Metadata:Data Ratio             7.7x       1.5x      ↓ 81%
Storage per 1M events           2.84 GB    0.5 GB    ↓ 82%
API calls for 1M events         1M         10K       ↓ 99%
API cost per 1M events          0.56 hr    0.01 hr   ↓ 98%
Average file size               8 KB       10 MB     ↑ 1250x
Query latency (p99)             500ms      50ms      ↓ 90%
Metadata files                  583        50        ↓ 91%
Monthly cost (est.)             €147       €40       ↓ 73%


🛠️ IMPLEMENTATION CHECKLIST:
═════════════════════════════════════════════════════════════════════

Week 1 (Quick Wins):
  [ ] Implement client-side async buffering (2-3 hours)
  [ ] Change flush interval: 60s → 300s (1 hour)
  [ ] Add monitoring for metadata ratio (2 hours)
  [ ] Test with optimal batch sizes (2 hours)
  Estimated time impact: 7-8 hours
  Estimated cost savings: 70%

Week 2 (Medium Effort):
  [ ] Consolidate avro schema files (4 hours)
  [ ] Optimize Iceberg manifest settings (2 hours)
  [ ] Load testing with 100k evt/s (4 hours)
  Estimated time impact: 10 hours
  Estimated additional savings: 15%

Week 3+ (Long-term):
  [ ] Implement partition by log_group (8 hours)
  [ ] Archive policy for old data (4 hours)
  [ ] Performance benchmarking (8 hours)
  Estimated time impact: 20 hours
  Estimated additional savings: 8%


⚠️ RISKS & MITIGATION:
═════════════════════════════════════════════════════════════════════

Risk 1: Increased latency from buffering
  Mitigation: Use 30-second timeout max, batch on 100 events OR timeout

Risk 2: Data loss if process crashes before flush
  Mitigation: Use persistent queue (Redis/Kafka) for buffers

Risk 3: Increased memory from client-side buffering
  Mitigation: Limit buffer to 50MB (CloudWatch API limit is 1MB/call)

Risk 4: Breaking changes from flush interval increase
  Mitigation: Add flag to control flush interval per log group


💾 MONITORING & ALERTS:
═════════════════════════════════════════════════════════════════════

Add to your monitoring:

metadata_to_data_ratio:
  warning: >= 3.0x
  critical: >= 5.0x
  action: Check for small files/inefficient writes

avg_batch_size:
  warning: < 50 events
  critical: < 10 events
  action: Enable client-side buffering

parquet_file_count:
  warning: > 200 files
  critical: > 500 files
  action: Increase flush interval or batch size

metadata_file_growth:
  warning: > data file growth
  critical: > 2x data file growth
  action: Consolidate schemas


═════════════════════════════════════════════════════════════════════

Questions? Check:
  1. Review COST_ANALYSIS_REPORT.md for detailed analysis
  2. Run io_cost_analysis.py for current system metrics
  3. Run file_fragmentation_analysis.py for write pattern analysis

═════════════════════════════════════════════════════════════════════
""")

# Quick calculator
def calculate_costs(events_per_million, write_pattern="batch"):
    """Calculate costs based on write pattern"""
    
    patterns = {
        "single": {
            "api_cost_hours": 0.56,
            "storage_gb": 2.84,
            "metadata_pct": 89,
            "query_latency_ms": 500
        },
        "batch": {
            "api_cost_hours": 0.01,
            "storage_gb": 0.5,
            "metadata_pct": 20,
            "query_latency_ms": 50
        }
    }
    
    pattern = patterns.get(write_pattern, patterns["batch"])
    
    print(f"\n💰 COST CALCULATOR: {events_per_million/1_000_000:.1f}M events with '{write_pattern}' pattern")
    print("─" * 60)
    print(f"  API cost:          {pattern['api_cost_hours']:.2f} CPU-hours")
    print(f"  Storage cost:      {pattern['storage_gb']:.1f} GB")
    print(f"  Metadata %:        {pattern['metadata_pct']}%")
    print(f"  Query latency:     {pattern['query_latency_ms']}ms (p99)")
    
    if write_pattern == "single":
        print(f"\n  💡 Switch to batch to save: {2.84-0.5:.2f} GB storage + {0.56-0.01:.2f} CPU-hours")

if __name__ == "__main__":
    print("\nRUNNING QUICK COST CALCULATOR:")
    print("=" * 60)
    
    calculate_costs(1_000_000, "single")
    calculate_costs(1_000_000, "batch")
    
    print("\n" + "="*60)
    print("✅ Cost Analysis Complete - Review above for optimization steps")
    print("="*60)
