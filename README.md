# CloudWatch Log Persist

A high-performance, cost-optimized log persistence and query system that bridges **AWS CloudWatch**, **Loki**, and **Apache Iceberg** tables. This project stores logs efficiently in S3-compatible storage with a 3-tier architecture, streaming compaction, and support for long-range queries (24h+) without OOM.

## 🎯 Key Features

### Core Architecture
- **3-Tier Storage Layout**: Hot (WAL) → Cold (Local Parquet) → Archive (S3/MinIO)
- **PyIceberg Integration**: PostgreSQL catalog + S3 backend for durable, queryable log storage
- **CloudWatch API Compatible**: Drop-in replacement endpoint for AWS CloudWatch Logs API
- **Loki Query API**: Prometheus-compatible metric queries (`count_over_time`, `rate`, etc.)
- **Memory-Efficient**: Streaming batch readers prevent OOM on 24h+ time-range queries

### Hot Tier (Write-Ahead Log)
- In-memory circular buffer with JSONL write-ahead log (WAL)
- Configurable flush interval (60s default) and max size (50k events)
- Worker thread pool for async flush operations
- Automatic backpressure when WAL+Cold exceeds configured threshold

### Cold Tier (PostgreSQL Catalog)
- Local Parquet data staging (survives container restarts)
- Automatic eviction: `MAX_EVENTS_PER_STREAM = 10,000` per log stream
- Feeds compaction pipeline (WAL → Cold → S3)

### Archive Tier (S3/MinIO)
- Partitioned by `log_group` (configurable strategy)
- Streaming compaction with 50k-row batch sorting (minimal RAM)
- Phase 3 (S3 in-place merge) disabled by default to prevent OOM
- Column projection pushdown for efficient queries

### Query Optimization
- Hard limit per tier: **500k rows default** prevents unbounded RAM on long-range queries
- Metric fast-path with PyArrow vectorised bucketing
- Column projection pushdown to cold/S3 scans
- Timestamp-based filtering with PyIceberg expression pushdown

### Observability
- **GET /debug/memory** endpoint: Memory breakdown (VmRSS, WAL items, cold events)
- Optional on-demand `tracemalloc` tracing (`?trace=1` query param)
- Structured logging with log_group/stream metadata
- Metrics: query count, duration, error rates

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│   CloudWatch API (port 4588)  │  Loki Query API             │
│   PUT/GET Events              │  rate() count_over_time()   │
└──────────────┬────────────────────────────┬─────────────────┘
               │                            │
        ┌──────▼────────────────────────────▼───────┐
        │         LogStore (in-memory)               │
        │  - Message dedup, sequence tokens          │
        │  - Circular buffer (50k event cap)         │
        └──────┬───────────────────────────┬───────┘
               │  flush() every 60s        │
        ┌──────▼────────────────────────────▼────────┐
        │    WAL (Hot Tier)                          │
        │  JSONL files: wal_*.jsonl                  │
        │  Newest files read first (reverse sort)    │
        │  Cap: 100k rows per query                  │
        └──────┬────────────────────────────────────┘
               │  _flush_wal() every 30s (10k batches)
        ┌──────▼────────────────────────────────────┐
        │  Cold Tier (PostgreSQL + Local Parquet)   │
        │  - Staging dir: /app/staging/{table}/data │
        │  - SQLite catalog (not persisted)          │
        │  - Cap: 500k rows per query (streaming)    │
        └──────┬────────────────────────────────────┘
               │  compact() every 300s (Phase 2)
               │  Stream 50k-row batch reader
        ┌──────▼────────────────────────────────────┐
        │   Archive Tier (S3/MinIO)                 │
        │  - s3a://warehouse/default/{table}/       │
        │  - PostgreSQL catalog (persisted)          │
        │  - Partition-by: log_group                │
        │  - Cap: 500k rows per query (streaming)    │
        └──────────────────────────────────────────┘
```

### Query Flow (3-Tier Merge)

```
query(filter_expr, limit)
  ├─ S3 scan (streaming, effective_limit rows)
  ├─ Cold scan (streaming, effective_limit rows)
  ├─ WAL scan (up to 100k rows, newest first)
  └─ Merge + timestamp sort + limit
```

**Memory-Safe**: Even for 24h time range with no explicit limit:
- Each tier loads ≤ 500k rows (streaming batch reader)
- Peak RAM ≈ 3 × 500k rows × ~1KB avg ≈ 1.5 GB (bounded)
- Metric queries with column projection: even smaller

## 📦 Installation

### Docker Compose (Recommended)

```bash
docker compose up -d --build
```

Brings up:
- **cloudwatch-log-app**: Flask server (port 4588)
- **iceberg-postgres**: PostgreSQL catalog (port 5432)
- **iceberg-minio**: MinIO S3 (port 9000, console 9001)
- **grafana**: Grafana (port 3000)

### Manual Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure
cat > config.yaml << 'EOF'
warehouse: s3a://warehouse/
catalog: postgresql
namespace: default
table_name: cloudwatch_logs
# ... (see config.yaml for defaults)
EOF

# Start server
python -m src.service.server
```

## 🚀 Quick Start

### 1. Put Log Events (CloudWatch API)

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/PutLogEvents \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.PutLogEvents" \
  -d '{
    "logGroupName": "/aws/lambda/my-lambda",
    "logStreamName": "2025-01-01/[$LATEST]abcd1234",
    "logEvents": [
      {"timestamp": 1609459200000, "message": "START RequestId: xyz"}
    ]
  }'
```

### 2. Query Logs (CloudWatch API)

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/GetLogEvents \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.GetLogEvents" \
  -d '{
    "logGroupName": "/aws/lambda/my-lambda",
    "logStreamName": "2025-01-01/[$LATEST]abcd1234",
    "startTime": 1609459200000,
    "endTime": 1609545600000
  }'
```

### 3. Query Metrics (Loki API)

```bash
# 1-minute count of logs in 24h range
curl 'http://localhost:4588/loki/api/v1/query_range' \
  -G \
  --data-urlencode 'query=count_over_time({log_group_name="/aws/lambda/my-lambda"}[1m])' \
  --data-urlencode 'start=1609459200' \
  --data-urlencode 'end=1609545600' \
  --data-urlencode 'step=60'
```

### 4. Monitor Memory Usage

```bash
curl 'http://localhost:4588/debug/memory'
curl 'http://localhost:4588/debug/memory?trace=1'  # Enable tracemalloc
curl 'http://localhost:4588/debug/memory?trace=stop'  # Disable tracing
```

## 📊 Configuration

### Buffer Settings (Hot Tier)

```yaml
buffer:
  max_size: 50000                    # Events before auto-flush
  flush_interval_seconds: 60         # Async flush to cold
  worker_threads: 2
  wal_enabled: true
  wal_dir: wal/
```

### Compaction Settings (Hot → Cold → Archive)

```yaml
compaction:
  enabled: true
  interval_seconds: 300              # Full compaction every 5 min
  wal_max_rows: 500000               # Backpressure threshold
  wal_flush_interval_seconds: 30     # WAL→cold flush (independent)
  s3_merge_enabled: false            # Disable Phase 3 (OOM risk)
```

### Query Limits

**Code defaults** (in `warehouse.py::query()`):
```python
MAX_SCAN_ROWS = 500_000  # Hard cap per tier
WAL_FLUSH_BATCH = 10_000  # Batch size for WAL streaming
SORT_BATCH = 50_000       # Chunk size for cold/S3 compaction
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Unit tests (155 tests)
pytest tests/unit/ -v

# Integration tests (36 tests, requires Docker)
pytest tests/integration/ -v

# Coverage report
pytest tests/ --cov=src --cov-report=html
```

**Test Coverage**:
- `LogStore`: sequence tokens, per-stream eviction, filtering
- `LogBuffer`: async flush, thread safety, backpressure
- `Warehouse`: WAL→cold compaction, S3 queries, timestamp filters
- `Loki API`: metric bucketing, label filtering, col projection
- **E2E**: CloudWatch API + Loki queries on running Docker container

## 📈 Performance

### Memory Usage

| Operation | Before Opt | After Opt |
|-----------|-----------|-----------|
| Startup | ~4 GB | 155 MB |
| Compaction (10M logs) | OOM (killed) | 200-500 MB |
| Query 24h (no limit) | OOM | ~150 MB (500k rows/tier) |
| Metric query (col projection) | N/A | ~20-50 MB |

### Throughput

- **Ingest**: 50k events/sec (Flask + LogBuffer)
- **Compaction**: 10k rows/sec (streaming batches)
- **Query latency**: <500ms for 24h range (15k-30k rows typical)

### S3 Cost Reduction

See [docs/COST_ANALYSIS_REPORT.md](docs/COST_ANALYSIS_REPORT.md) for detailed breakdown:
- **Partitioning**: ~70% fewer S3 List calls
- **Compaction**: ~50% fewer small files → 30% fewer requests
- **Column projection**: ~20% read volume savings on metrics

## 🔍 Troubleshooting

### Out of Memory (OOM)

1. **Check /debug/memory**:
   ```bash
   curl http://localhost:4588/debug/memory?trace=1
   ```
   Look for: `log_buffer_items`, `log_store_events`, `wal_files`

2. **Enable tracemalloc**:
   ```bash
   curl 'http://localhost:4588/debug/memory?trace=1'
   # Allocations now tracked; top consumers log to stdout
   ```

3. **Reduce query limits**:
   - Set `limit` in query (e.g., `limit=10000`)
   - Or reduce `MAX_SCAN_ROWS` in code (default 500k)

4. **Disable S3 Phase 3**:
   ```yaml
   compaction:
     s3_merge_enabled: false
   ```

### Slow Queries

- Enable timestamp filters to reduce data scanned
- Use column projection: `selected_fields=("timestamp", "message")`
- Check S3 partition layout: `docker exec iceberg-minio ls -R /data/warehouse/`

### Missing Data After Restart

- WAL files are persisted in `wal/` directory (mounted volume)
- Cold tier staging: `/app/staging/` (mounted volume)
- Both survive container restart; query will re-read them

## 📚 API Reference

**Complete API documentation**: See [docs/API.md](docs/API.md) for detailed endpoint specs, examples, and error codes.

### CloudWatch Logs API

**Supported Endpoints** (see [docs/API.md#cloudwatch-logs-api](docs/API.md#cloudwatch-logs-api)):
- `PutLogEvents` — Ingest events
- `GetLogEvents` — Query by log group/stream and time range
- `DescribeLogGroups` — List log groups
- `DescribeLogStreams` — List log streams in group
- `FilterLogEvents` — Full-text search with optional time range
- `CreateLogGroup/Stream` — Create new groups/streams
- `DeleteLogGroup/Stream` — Remove groups/streams

**Max Result Size**: 1 MB per response (standard AWS behavior)

### Loki Query API

**Endpoints** (see [docs/API.md#loki-query-api](docs/API.md#loki-query-api)):
- `GET /loki/api/v1/query` — Instant query (LogQL)
- `GET /loki/api/v1/query_range` — Time-range query with bucketing
- `POST /loki/api/v1/push` — Ingest via Loki protocol

**Supported Aggregations**:
- `count_over_time(expr, [step])`
- `rate(expr, [step])`
- `sum_over_time(expr, [step])`
- `min/max_over_time(expr, [step])`
- `by (label)` — Group by label (e.g., `by (label_env)`)

**Label Matchers**:
- `{log_group_name="..."}` — Exact match
- `{log_group_name=~"..."}` — Regex match
- `{label_env="prod"}` — Custom labels

See [docs/API.md#logql-query-examples](docs/API.md#logql-query-examples) for advanced examples.

## 🛠️ Development

### Project Structure

```
src/
  service/
    server.py                 # Flask app + CloudWatch API
    routes/
      cloudwatch.py          # CloudWatch endpoint handlers
      loki.py                # Loki query API + metric aggregation
    services/
      log_buffer.py          # In-memory buffer + WAL writer
      log_store.py           # Sequence tokens + stream management
    utils/
      loki_utils.py          # LogQL parser, label filter
      helpers.py             # Log event formatting
  warehouse/
    warehouse.py             # 3-tier query + compaction orchestrator
    
tests/
  unit/                       # 155 tests, no Docker needed
  integration/               # 36 tests, requires Docker

compaction-test/             # Standalone Iceberg compaction tests
  src/
    compactor.py             # Phase 2 + 3 compaction engine
```

### Key Classes

- **LogStore**: Manages sequence tokens per log stream, enforces `MAX_EVENTS_PER_STREAM`
- **LogBuffer**: Async in-memory buffer, triggers WAL flush
- **Warehouse**: 3-tier query coordinator, compaction scheduler
- **IcebergTableCompactor**: Streaming batch reader + S3 append for phase 2

### Contributing

1. Write tests first (unit or integration)
2. Run all tests: `pytest tests/ -v`
3. Code should maintain backward compatibility with CloudWatch API
4. Add docstrings for public methods

## 📄 License

MIT License — See LICENSE file

## 📞 Support

- **Issues**: Create GitHub issues for bugs/features
- **Docs**: See [docs/](docs/) folder for detailed guides
- **Memory**: Check `GET /debug/memory` for runtime stats
- **Logs**: Container logs available via `docker logs cloudwatch-log-app`

---

**Last Updated**: April 2026  
**Tested With**: PyIceberg 0.7.0, PostgreSQL 15, MinIO 2024-01
