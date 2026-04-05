# API Documentation

Complete API reference for CloudWatch Logs, Loki Query, and custom debug endpoints.

## Table of Contents

1. [CloudWatch Logs API](#cloudwatch-logs-api)
2. [Loki Query API](#loki-query-api)
3. [Custom Debug Endpoints](#custom-debug-endpoints)
4. [Error Responses](#error-responses)

---

## CloudWatch Logs API

AWS CloudWatch Logs compatible API. Base URL: `http://localhost:4588`

All requests use Content-Type: `application/x-amz-json-1.1` with target header.

### Common Request Headers

```
Content-Type: application/x-amz-json-1.1
X-Amz-Target: Logs_20140328.<OperationName>
```

### PutLogEvents

Ingest log events into a log stream.

**Endpoint**: `POST /2014-03-28/logData/PutLogEvents`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/PutLogEvents \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.PutLogEvents" \
  -d '{
    "logGroupName": "/aws/lambda/my-function",
    "logStreamName": "2025-01-01/[$LATEST]abcd1234",
    "logEvents": [
      {
        "timestamp": 1704067200000,
        "message": "START RequestId: 12345"
      },
      {
        "timestamp": 1704067201000,
        "message": "Processing event..."
      }
    ]
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name (e.g., `/aws/lambda/my-function`)
- `logStreamName` (string, required) — Log stream name (e.g., `2025-01-01/[$LATEST]xyz`)
- `logEvents` (array, required) — Array of log events
  - `timestamp` (number, required) — Epoch milliseconds
  - `message` (string, required) — Log message

**Response** (201 Created):

```json
{
  "nextSequenceToken": "f334567890abcdef0"
}
```

**Errors**:
- `ResourceNotFoundException` — Log group or stream doesn't exist
- `InvalidParameterException` — Invalid parameters
- `DataAlreadyAcceptedException` — Duplicate log events (same timestamp + message)

---

### GetLogEvents

Retrieve log events from a log stream.

**Endpoint**: `POST /2014-03-28/logData/GetLogEvents`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/GetLogEvents \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.GetLogEvents" \
  -d '{
    "logGroupName": "/aws/lambda/my-function",
    "logStreamName": "2025-01-01/[$LATEST]abcd1234",
    "startTime": 1704067200000,
    "endTime": 1704153600000,
    "limit": 100,
    "startFromHead": true
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name
- `logStreamName` (string, required) — Log stream name
- `startTime` (number, optional) — Start time (epoch ms)
- `endTime` (number, optional) — End time (epoch ms)
- `limit` (number, optional) — Max events to return (1-10000, default 100)
- `startFromHead` (boolean, optional) — Return oldest events first (default: false = newest first)

**Response** (200 OK):

```json
{
  "events": [
    {
      "timestamp": 1704067200000,
      "message": "START RequestId: 12345",
      "ingestionTime": 1704067200500
    },
    {
      "timestamp": 1704067201000,
      "message": "Processing event...",
      "ingestionTime": 1704067201500
    }
  ],
  "nextForwardToken": "f334567890abcdef1",
  "nextBackwardToken": "b123456789abcdef0"
}
```

---

### FilterLogEvents

Search log events by pattern across log streams.

**Endpoint**: `POST /2014-03-28/logData/FilterLogEvents`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/FilterLogEvents \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.FilterLogEvents" \
  -d '{
    "logGroupName": "/aws/lambda/my-function",
    "logStreamNames": ["2025-01-01/[$LATEST]abcd1234"],
    "filterPattern": "ERROR",
    "startTime": 1704067200000,
    "endTime": 1704153600000,
    "limit": 50,
    "interleaved": true
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name
- `logStreamNames` (array, optional) — Filter by stream names; if empty, search all
- `filterPattern` (string, optional) — Case-insensitive substring match (empty = all logs)
- `startTime` (number, optional) — Start time (epoch ms)
- `endTime` (number, optional) — End time (epoch ms)
- `limit` (number, optional) — Max events (1-10000, default 100)
- `interleaved` (boolean, optional) — Sort by timestamp across streams (default: false)

**Response** (200 OK):

```json
{
  "events": [
    {
      "logStreamName": "2025-01-01/[$LATEST]abcd1234",
      "timestamp": 1704067300000,
      "message": "ERROR: Connection failed",
      "ingestionTime": 1704067300500
    }
  ],
  "searchedLogStreams": [
    {
      "logStreamName": "2025-01-01/[$LATEST]abcd1234",
      "searchedCompletely": true
    }
  ]
}
```

---

### DescribeLogGroups

List all log groups.

**Endpoint**: `POST /2014-03-28/logData/DescribeLogGroups`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/DescribeLogGroups \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.DescribeLogGroups" \
  -d '{
    "logGroupNamePrefix": "/aws/",
    "limit": 50
  }'
```

**Parameters**:
- `logGroupNamePrefix` (string, optional) — Filter by prefix
- `limit` (number, optional) — Max groups (1-50, default 50)

**Response** (200 OK):

```json
{
  "logGroups": [
    {
      "logGroupName": "/aws/lambda/my-function",
      "creationTime": 1704067200000,
      "retentionInDays": 7,
      "storedBytes": 1048576
    },
    {
      "logGroupName": "/aws/rds/instance/mydb",
      "creationTime": 1704067300000,
      "retentionInDays": 15,
      "storedBytes": 5242880
    }
  ]
}
```

---

### DescribeLogStreams

List log streams in a log group.

**Endpoint**: `POST /2014-03-28/logData/DescribeLogStreams`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/DescribeLogStreams \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.DescribeLogStreams" \
  -d '{
    "logGroupName": "/aws/lambda/my-function",
    "logStreamNamePrefix": "2025-01",
    "orderBy": "LastEventTime",
    "descending": true,
    "limit": 50
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name
- `logStreamNamePrefix` (string, optional) — Filter by prefix
- `orderBy` (string, optional) — Sort by `LogStreamName` or `LastEventTime` (default)
- `descending` (boolean, optional) — Reverse order (default: false)
- `limit` (number, optional) — Max streams (1-50, default 50)

**Response** (200 OK):

```json
{
  "logStreams": [
    {
      "logStreamName": "2025-01-01/[$LATEST]abcd1234",
      "creationTime": 1704067200000,
      "firstEventTimestamp": 1704067200000,
      "lastEventTimestamp": 1704153600000,
      "lastIngestionTime": 1704153600500,
      "uploadSequenceToken": "f334567890abcdef0",
      "arn": "arn:aws:logs:ap-southeast-1:123456789012:log-group:/aws/lambda/my-function:log-stream:2025-01-01/[$LATEST]abcd1234",
      "storedBytes": 524288
    }
  ]
}
```

---

### CreateLogGroup

Create a new log group.

**Endpoint**: `POST /2014-03-28/logData/CreateLogGroup`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/CreateLogGroup \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.CreateLogGroup" \
  -d '{
    "logGroupName": "/custom/my-app"
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name (must be unique)

**Response** (201 Created):

```json
{}
```

**Errors**:
- `ResourceAlreadyExistsException` — Log group already exists

---

### CreateLogStream

Create a new log stream in a log group.

**Endpoint**: `POST /2014-03-28/logData/CreateLogStream`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/CreateLogStream \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.CreateLogStream" \
  -d '{
    "logGroupName": "/custom/my-app",
    "logStreamName": "instance-01"
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name
- `logStreamName` (string, required) — Log stream name (must be unique within group)

**Response** (201 Created):

```json
{}
```

---

### DeleteLogGroup

Delete a log group and all its log streams.

**Endpoint**: `POST /2014-03-28/logData/DeleteLogGroup`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/DeleteLogGroup \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.DeleteLogGroup" \
  -d '{
    "logGroupName": "/custom/my-app"
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name

**Response** (204 No Content):

```json
{}
```

---

### DeleteLogStream

Delete a log stream.

**Endpoint**: `POST /2014-03-28/logData/DeleteLogStream`

**Request**:

```bash
curl -X POST http://localhost:4588/2014-03-28/logData/DeleteLogStream \
  -H "Content-Type: application/x-amz-json-1.1" \
  -H "X-Amz-Target: Logs_20140328.DeleteLogStream" \
  -d '{
    "logGroupName": "/custom/my-app",
    "logStreamName": "instance-01"
  }'
```

**Parameters**:
- `logGroupName` (string, required) — Log group name
- `logStreamName` (string, required) — Log stream name

**Response** (204 No Content):

```json
{}
```

---

## Loki Query API

Prometheus-compatible log query API. Base URL: `http://localhost:4588/loki/api/v1`

### Query Range

Query logs over a time range with bucketing.

**Endpoint**: `GET /query_range` or `POST /query_range`

**Request (GET)**:

```bash
curl 'http://localhost:4588/loki/api/v1/query_range' \
  -G \
  --data-urlencode 'query=count_over_time({log_group_name="/aws/lambda/my-function"}[1m])' \
  --data-urlencode 'start=1704067200' \
  --data-urlencode 'end=1704153600' \
  --data-urlencode 'step=60' \
  --data-urlencode 'limit=5000'
```

**Request (POST)**:

```bash
curl -X POST http://localhost:4588/loki/api/v1/query_range \
  -H "Content-Type: application/json" \
  -d '{
    "query": "count_over_time({log_group_name=\"/aws/lambda/my-function\"}[1m])",
    "start": 1704067200,
    "end": 1704153600,
    "step": 60,
    "limit": 5000
  }'
```

**Parameters**:
- `query` (string, required) — LogQL query (see examples below)
- `start` (number, required) — Start time (epoch seconds)
- `end` (number, required) — End time (epoch seconds)
- `step` (number, required) — Bucket width (seconds)
- `limit` (number, optional) — Max rows per response (default 10000)

**Response** (200 OK):

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {},
        "values": [
          [1704067260, "42"],
          [1704067320, "38"],
          [1704067380, "45"]
        ]
      }
    ]
  }
}
```

---

### Query (Instant)

Evaluate a query at a single point in time.

**Endpoint**: `GET /query` or `POST /query`

**Request**:

```bash
curl 'http://localhost:4588/loki/api/v1/query' \
  -G \
  --data-urlencode 'query={log_group_name="/aws/lambda/my-function"} | json | level="ERROR"' \
  --data-urlencode 'time=1704153600'
```

**Parameters**:
- `query` (string, required) — LogQL query
- `time` (number, optional) — Epoch seconds (default: now)
- `limit` (number, optional) — Max results

**Response** (200 OK):

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": {
          "log_group_name": "/aws/lambda/my-function",
          "log_stream_name": "2025-01-01/[$LATEST]abcd1234"
        },
        "values": [
          ["1704153600000000000", "ERROR: Connection timeout"]
        ]
      }
    ]
  }
}
```

---

### Push (Loki Protocol)

Ingest logs via Loki push protocol.

**Endpoint**: `POST /push`

**Request**:

```bash
curl -X POST http://localhost:4588/loki/api/v1/push \
  -H "Content-Type: application/json" \
  -d '{
    "streams": [
      {
        "stream": {
          "log_group_name": "/app/service",
          "log_stream_name": "pod-1",
          "env": "prod",
          "service": "api"
        },
        "values": [
          ["1704153600000000000", "Request processed in 45ms"],
          ["1704153601000000000", "Response sent"]
        ]
      }
    ]
  }'
```

**Parameters**:
- `streams` (array, required) — Array of log streams
  - `stream` (object, required) — Label dictionary
    - `log_group_name` (required) — Log group
    - `log_stream_name` (required) — Log stream
    - Other keys → stored as custom labels
  - `values` (array, required) — Log entries `[timestamp_ns, message]`

**Response** (204 No Content):

```json
{}
```

---

## LogQL Query Examples

### Basic Label Matching

```
{log_group_name="/aws/lambda/my-function"}
```

Exact match on log group.

### Regex Matching

```
{log_group_name=~"^/aws/lambda.*"}
{log_stream_name!="old-stream"}
```

- `=~` — Regex match
- `!~` — Regex non-match
- `!=` — Exact non-match

### Multiple Labels

```
{log_group_name="/aws/lambda/my-function", label_env="prod"}
```

Logical AND on all label filters.

### Line Filters

```
{log_group_name="/app/api"} | "ERROR"
```

Filter messages containing "ERROR" (case-insensitive).

```
{log_group_name="/app/api"} | json | status >= 500
```

Parse JSON, filter if `status >= 500`.

### Metric Aggregations

```
count_over_time({log_group_name="/aws/lambda/my-function"}[5m])
```

Count logs per 5-minute bucket. Step should be ≤ 5m.

```
rate({log_group_name="/app/api"} | code="500"[1m])
```

Rate of 500 errors per second (per 1m bucket).

```
sum_over_time({log_group_name="/app/service"}[10m])
```

Sum metric values per 10m bucket.

```
count_over_time({log_group_name="/app/service"}[1m]) by (label_env)
```

Count per 1m bucket, grouped by `label_env` (e.g., separate lines for prod/staging).

---

## Custom Debug Endpoints

### Memory Status

Get current memory usage and object counts.

**Endpoint**: `GET /debug/memory`

**Request**:

```bash
curl http://localhost:4588/debug/memory
```

**Response** (200 OK):

```json
{
  "process": {
    "vm_rss_mb": 155,
    "vm_hwm_mb": 512,
    "vm_size_mb": 1024,
    "resident_mb": 155
  },
  "app_state": {
    "log_buffer_items": 1234,
    "log_store_events": 5000,
    "log_store_groups": 15,
    "wal_files": 3,
    "wal_rows_pending": 50000
  },
  "warehouse": {
    "cold_table_rows": 250000,
    "s3_partitions": 8,
    "last_compaction": "2025-01-01T12:30:00Z"
  }
}
```

---

### Memory Trace (On-Demand)

Enable/disable `tracemalloc` for allocation tracking.

**Endpoint**: `GET /debug/memory?trace=1`

Enable tracemalloc:

```bash
curl 'http://localhost:4588/debug/memory?trace=1'
```

**Response**: Same as `/debug/memory`, plus `tracemalloc` starts capturing allocations. Top consuming frames logged to stdout.

---

**Endpoint**: `GET /debug/memory?trace=stop`

Disable tracemalloc:

```bash
curl 'http://localhost:4588/debug/memory?trace=stop'
```

**Response**: Memory snapshot with tracemalloc disabled.

---

## Error Responses

All APIs return errors in standard format.

### CloudWatch Error Format

```json
{
  "__type": "ResourceNotFoundException",
  "message": "The specified log group does not exist"
}
```

**HTTP Status**: 400-500 depending on error type

**Common Error Types**:
- `ResourceNotFoundException` — Log group/stream not found (404)
- `ResourceAlreadyExistsException` — Already exists (409)
- `InvalidParameterException` — Bad request parameters (400)
- `DataAlreadyAcceptedException` — Duplicate log event (400)
- `ServiceUnavailableException` — Internal error (503)

### Loki Error Format

```json
{
  "status": "error",
  "data": null,
  "error": "parse error at line 1"
}
```

**HTTP Status**: 200 (with `status: "error"` in body) or 400-500

---

## Limits & Quotas

| Resource | Limit | Notes |
|----------|-------|-------|
| Log event size | 256 KB | Per message |
| Log group size | ~1 GB | Partitioned by date + log_group |
| Batch size (PutLogEvents) | 1 MB | Compressed |
| Response size | 1 MB | Per GetLogEvents response |
| Query result rows | 500k | Per tier (soft cap, streaming) |
| Metric query cap | 2M | Hardcoded for `count_over_time` |
| Log retention | Configurable | Default 7 days |
| Ingest rate | ~50k events/sec | Flask + buffer limits |

---

## Authentication

Currently no authentication. For production, implement:
- API key via header
- AWS SigV4 signing
- OAuth2 bearer token

All requests are processed without credentials.

---

## Rate Limiting

No builtin rate limiting. Add via:
- Flask-Limiter extension
- Reverse proxy (nginx) `limit_req`
- Custom middleware with token bucket

---

## Versioning

API versions:
- **CloudWatch**: `2014-03-28` (fixed, AWS standard)
- **Loki**: `v1` (stable LogQL + push protocol)

Future versions (v2, v3) will maintain backward compatibility.

---

**Last Updated**: April 2026
