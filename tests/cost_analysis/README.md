# Cost Analysis Tools

Tools for analyzing system write costs, file I/O efficiency, and storage overhead.

## Tools Overview

### 1. **io_cost_analysis.py** - Overall Cost Metrics
Comprehensive system cost analysis covering:
- File creation overhead during writes
- Batch write efficiency 
- Multiple stream write fragmentation
- Metadata vs data file ratio
- Write performance degradation

**Run**: `python3 tests/cost_analysis/io_cost_analysis.py`

**Output**: Baseline metrics, throughput analysis, file type breakdown

---

### 2. **file_fragmentation_analysis.py** - Write Pattern Comparison
Detailed comparison of single vs batch write costs:
- Single-event writes (inefficient) vs batch writes (efficient)
- Streaming write patterns (high frequency)
- Large batch scaling tests
- Message size impact on file creation

**Run**: `python3 tests/cost_analysis/file_fragmentation_analysis.py`

**Output**: Cost breakdown showing 31x difference between single and batch writes

---

### 3. **overhead_analysis.py** - Storage Overhead Quantification
Analyzes metadata overhead and optimization opportunities:
- Metadata file size vs event count
- Parquet compression efficiency
- Total cost per event stored
- Optimal write patterns for different use cases
- Inefficiency warnings and optimization recommendations

**Run**: `python3 tests/cost_analysis/overhead_analysis.py`

**Output**: Metadata ratio (currently 7.7x), cost per event, optimization suggestions

---

### 4. **cost_quick_reference.py** - Implementation Guide
Quick reference with code examples and implementation checklist:
- Bottom-line findings
- Cost breakdown per million events
- 4 optimization strategies with code examples:
  1. Client-side batching
  2. Async buffering with timeout
  3. Warehouse flush tuning
  4. Schema consolidation
- Implementation checklist
- Risk mitigation strategies
- Monitoring recommendations

**Run**: `python3 tests/cost_analysis/cost_quick_reference.py`

**Output**: Quick reference guide with cost calculator

---

## Key Findings Summary

| Issue | Current | Target | Impact |
|-------|---------|--------|--------|
| **Metadata:Data ratio** | 7.7x | 1.5x | 89% of storage is overhead |
| **Single vs batch writes** | 31x slower | 100x faster | 99% API cost reduction |
| **Storage per 1M events** | 2.84 GB | 0.5 GB | 82% savings |
| **throughput** | 57 evt/s | 55k evt/s | 1000x improvement |

---

## Quick Start

### Run All Cost Analysis Tools
```bash
cd /Users/vantunghau/interspace/cloudwatch-log-persist

# Quick overview
python3 tests/cost_analysis/cost_quick_reference.py

# Detailed analysis
python3 tests/cost_analysis/io_cost_analysis.py
python3 tests/cost_analysis/file_fragmentation_analysis.py
python3 tests/cost_analysis/overhead_analysis.py
```

### View Full Report
```bash
cat docs/COST_ANALYSIS_REPORT.md
```

---

## Optimization Priorities

### 🔴 CRITICAL (Do First)
1. Implement write batching in clients (100+ events/call)
2. Increase flush interval: 60s → 300s
3. Consolidate avro schema files

### 🟡 HIGH (This Sprint)
4. Tune Iceberg manifest settings
5. Add metadata ratio monitoring
6. Load testing with optimal batches

### 🟢 MEDIUM (Next Sprint)
7. Implement schema deduplication
8. Add partition by log_group
9. Archive policy for old data

---

## Expected Savings

- **Storage**: ↓ 82% (2.84 GB → 0.5 GB per 1M events)
- **API cost**: ↓ 98% (0.56 CPU-hrs → 0.01 CPU-hrs)
- **Query latency**: ↓ 90% (500ms → 50ms)
- **Throughput**: ↑ 1000x (57 → 55k events/sec)
- **Monthly cost**: ↓ 73% (~€147 → €40)

---

## Monitoring Alerts

Track these metrics to ensure optimization stays on track:

```yaml
metadata_to_data_ratio:
  target: < 1.5x
  warning: >= 3.0x
  critical: >= 5.0x

avg_batch_size:
  target: 100+ events
  warning: < 50 events
  critical: < 10 events

parquet_file_count:
  target: 15-30 files
  warning: > 200 files
  critical: > 500 files
```

---

## Related Files

- **Integration Tests**: `tests/integration_api_test.py` (10 tests passing)
- **Full Report**: `docs/COST_ANALYSIS_REPORT.md`
- **CloudWatch Local Service**: `src/service/`
- **Warehouse**: `src/warehouse/warehouse.py`

---

**Last Updated**: April 2, 2026  
**Status**: Complete analysis with 4 actionable optimization strategies ready to implement
