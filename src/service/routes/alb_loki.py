"""
Loki-compatible API for ALB access logs.

Configure in Grafana as a Loki datasource:
    URL: http://logs-service:4588/alb
    (no Auth, no TLS)

Stream selectors (low-cardinality, usable in {label="value"} filter):
    elb, request_method, type, domain_name

Each log entry is a logfmt message that Grafana parses into fields:
    method, path, status, latency, client, bytes_in, bytes_out,
    host, error_reason, trace, ua

Metric queries (count_over_time, rate, sum by) are pushed into DuckDB
SQL — no raw-row transfer to Python.
"""

import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False

from flask import Blueprint, request, jsonify, Response

logger = logging.getLogger("service.alb_loki")

alb_loki_bp = Blueprint("alb_loki", __name__, url_prefix="/alb")

# Low-cardinality fields exposed as Loki stream selector labels
_LABEL_FIELDS = ["elb", "request_method", "type", "domain_name"]

# Extra fields made available in Grafana label browser
_EXTRA_LABEL_FIELDS = ["elb_status_code", "client_ip", "ssl_protocol"]

# Virtual labels: Grafana Drilldown always looks for service_name first.
# We expose it as an alias for elb so the Drilldown renders properly.
# key = virtual label name,  value = real ALB column
_VIRTUAL_LABELS: Dict[str, str] = {
    "service_name": "elb",
}

# Columns fetched for log stream queries (order must match _STREAM_NAMES)
_STREAM_COLS = (
    "CAST(EXTRACT(EPOCH FROM time) * 1e9 AS BIGINT) AS ts_ns",
    "elb", "request_method", "type", "domain_name",
    "elb_status_code", "target_processing_time",
    "client_ip", "request_path",
    "received_bytes", "sent_bytes",
    "trace_id", "error_reason", "user_agent",
)
_STREAM_NAMES = [
    "ts_ns", "elb", "request_method", "type", "domain_name",
    "elb_status_code", "target_processing_time",
    "client_ip", "request_path",
    "received_bytes", "sent_bytes",
    "trace_id", "error_reason", "user_agent",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_warehouse():
    from service.server import warehouse
    return warehouse


def _parse_ns(val, default_ns: int) -> int:
    """Normalise Grafana time param to nanoseconds.

    Accepts: nanoseconds int, microseconds int, milliseconds int,
             seconds int/float, or RFC3339/ISO-8601 string.
    """
    if not val:
        return default_ns
    # Try numeric first (nanoseconds / ms / s)
    try:
        ts = int(float(val))
        if ts < 1e10:    return ts * 1_000_000_000   # seconds
        if ts < 1e13:    return ts * 1_000_000        # ms
        if ts < 1e16:    return ts * 1_000            # µs
        return ts                                      # already ns
    except (ValueError, TypeError):
        pass
    # Try ISO-8601 / RFC3339 (e.g. "2026-04-01T14:48:29.060Z")
    try:
        dt = datetime.fromisoformat(str(val).replace('Z', '+00:00'))
        return int(dt.timestamp() * 1e9)
    except (ValueError, TypeError):
        return default_ns


def _ns_to_dt(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).replace(tzinfo=None)


def _naive(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_step(step_str: str, start_ms: int, end_ms: int, max_dp: int = 500) -> int:
    if step_str:
        try:
            m = re.match(r'^(\d+)(s|m|h|d)?$', step_str.strip())
            if m:
                v = int(m.group(1))
                unit = m.group(2) or 's'
                return max(1, v * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[unit])
        except Exception:
            pass
    return max(1, int(max(1, (end_ms - start_ms) / 1000.0) / max(max_dp, 1)))


def _parse_logql(query: str) -> Tuple[dict, Optional[str], Optional[str]]:
    """Return (label_filters, message_filter, agg_label).

    Handles:
        {elb="my-elb", request_method="GET"} |= "404"
        count_over_time({elb="my-elb"}[1m]) by (elb_status_code)

    Only exact-match (`=`) selectors are pushed to SQL.
    Regex (`=~`), negative (`!=`, `!~`) and match-all patterns are ignored.
    """
    label_filters: dict = {}
    message_filter: Optional[str] = None
    agg_label: Optional[str] = None

    sel = re.search(r'\{([^}]*)\}', query)
    if sel:
        # Match only exact-equality matchers: label="value" or label='value'
        # Explicitly exclude =~ (regex), != (neq), !~ (neg-regex)
        for m in re.finditer(r'(\w+)\s*=\s*"([^"]*)"', sel.group(1)):
            # The char before = must not be ! or ~
            label_filters[m.group(1)] = m.group(2)
        for m in re.finditer(r"(\w+)\s*=\s*'([^']*)'", sel.group(1)):
            label_filters[m.group(1)] = m.group(2)

    # Drop empty-string values (Grafana sends {label=""} meaning "all values")
    # Also drop regex patterns that slipped through (value contains . or +)
    label_filters = {k: v for k, v in label_filters.items() if v and v not in ('.+', '.*', '.+')}

    after = query[sel.end():] if sel else query
    pipe = re.search(r'\|=\s*"([^"]+)"', after)
    if pipe:
        message_filter = pipe.group(1)

    by_m = re.search(r'by\s*\(\s*([^)]+)\s*\)', query, re.IGNORECASE)
    if by_m:
        agg_label = by_m.group(1).strip()

    return label_filters, message_filter, agg_label


def _is_metric(query: str) -> bool:
    lq = query.lower()
    return any(x in lq for x in [
        'count_over_time', 'rate(', 'bytes_over_time',
        'sum_over_time', 'avg_over_time', 'max_over_time', 'min_over_time',
    ])


def _build_where(
    from_dt: datetime,
    to_dt: datetime,
    label_filters: dict,
    message_filter: Optional[str] = None,
) -> Tuple[str, list]:
    """Build WHERE clause and params for ALB delta table queries.

    Note: NO date/hour string filters are added here. DuckDB delta_scan
    with hive partitioning (date=.../hour=...) prunes partition folders via
    the time >= / time <= pushdown. Adding explicit string conditions on
    partition columns causes DuckDB to use column min/max stats instead,
    which returns 0 rows after optimize().compact() rewrites the files.
    """
    conditions = ["time >= ?", "time <= ?"]
    params: list = [_naive(from_dt), _naive(to_dt)]

    # Expand virtual labels (e.g. service_name → elb)
    resolved = {}
    for k, v in label_filters.items():
        resolved[_VIRTUAL_LABELS.get(k, k)] = v

    for field in _LABEL_FIELDS:
        if field in resolved:
            conditions.append(f"{field} = ?")
            params.append(resolved[field])

    # Additional label filters (e.g. elb_status_code from Grafana explore)
    for field in _EXTRA_LABEL_FIELDS:
        if field in resolved:
            if field == 'elb_status_code':
                try:
                    conditions.append(f"{field} = ?")
                    params.append(int(resolved[field]))
                except ValueError:
                    pass
            else:
                conditions.append(f"{field} = ?")
                params.append(resolved[field])

    # Message filter: search key text fields (parameterised, no injection risk)
    if message_filter:
        conditions.append(
            "(request_path ILIKE ? "
            " OR CAST(elb_status_code AS VARCHAR) ILIKE ? "
            " OR client_ip ILIKE ?)"
        )
        mf = f'%{message_filter}%'
        params += [mf, mf, mf]

    return "WHERE " + " AND ".join(conditions), params


def _run_sql(warehouse, sql: str, params: list) -> list:
    conn = warehouse._get_duckdb_conn()
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as e:
        msg = str(e)
        if any(kw in msg for kw in (
            'not a Delta table', "doesn't exist", 'No log files',
            'Invariant violation', 'TableNotFound',
        )):
            return []
        if '404' in msg or 'Not Found' in msg:
            warehouse._reset_duckdb_conn()
            try:
                return warehouse._get_duckdb_conn().execute(sql, params).fetchall()
            except Exception:
                warehouse._reset_duckdb_conn()
                return []
        logger.warning(f"[ALBLoki] DuckDB error: {type(e).__name__}: {e}")
        warehouse._reset_duckdb_conn()
        return []


def _row_to_logfmt(row: dict) -> str:
    """Format an ALB row dict as a logfmt message (for Grafana Fields panel)."""
    parts = []

    method = row.get('request_method')
    if method:
        parts.append(f'method={method}')

    path = row.get('request_path') or ''
    if path:
        if any(c in path for c in ' "='):
            path = '"' + path.replace('"', '\\"') + '"'
        parts.append(f'path={path}')

    status = row.get('elb_status_code')
    if status is not None:
        parts.append(f'status={status}')

    lat = row.get('target_processing_time')
    if lat is not None and lat >= 0:
        parts.append(f'latency={lat:.3f}s')

    client = row.get('client_ip')
    if client:
        parts.append(f'client={client}')

    rx = row.get('received_bytes')
    if rx is not None:
        parts.append(f'bytes_in={rx}')

    tx = row.get('sent_bytes')
    if tx is not None:
        parts.append(f'bytes_out={tx}')

    host = row.get('domain_name') or row.get('elb') or ''
    if host:
        parts.append(f'host={host}')

    err = row.get('error_reason') or ''
    if err and err != '-':
        parts.append(f'error_reason={err}')

    trace = row.get('trace_id') or ''
    if trace and trace != '-':
        parts.append(f'trace={trace}')

    ua = row.get('user_agent') or ''
    if ua and ua != '-':
        parts.append(f'ua="{ua[:120]}"')

    return ' '.join(parts) or '-'


def _loki_json(data: dict) -> Response:
    if _HAS_ORJSON:
        return Response(orjson.dumps(data), mimetype='application/json')
    return jsonify(data)


def _loki_stats(total_lines: int, total_bytes: int, n_returned: int) -> dict:
    """Return a full Loki-compatible stats block matching the real Loki API format."""
    _zero_cache = {
        'entriesFound': 0, 'entriesRequested': 0, 'entriesStored': 0,
        'bytesReceived': 0, 'bytesSent': 0, 'requests': 0,
        'downloadTime': 0, 'queryLengthServed': 0,
    }
    _zero_store = {
        'totalChunksRef': 0, 'totalChunksDownloaded': 0, 'chunksDownloadTime': 0,
        'queryReferencedStructuredMetadata': False,
        'chunk': {
            'headChunkBytes': 0, 'headChunkLines': 0,
            'decompressedBytes': 0, 'decompressedLines': 0,
            'compressedBytes': 0, 'totalDuplicates': 0, 'postFilterLines': 0,
            'headChunkStructuredMetadataBytes': 0,
            'decompressedStructuredMetadataBytes': 0,
        },
        'chunkRefsFetchTime': 0, 'congestionControlLatency': 0,
        'pipelineWrapperFilteredLines': 0,
    }
    return {
        'summary': {
            'bytesProcessedPerSecond': 0,
            'linesProcessedPerSecond': 0,
            'totalBytesProcessed': total_bytes,
            'totalLinesProcessed': total_lines,
            'execTime': 0.001,
            'queueTime': 0,
            'subqueries': 0,
            'totalEntriesReturned': n_returned,
            'splits': 1,
            'shards': 0,
            'totalPostFilterLines': total_lines,
            'totalStructuredMetadataBytesProcessed': 0,
        },
        'querier': {'store': _zero_store},
        'ingester': {
            'totalReached': 0, 'totalChunksMatched': 0,
            'totalBatches': 0, 'totalLinesSent': 0,
            'store': _zero_store,
        },
        'cache': {
            'chunk': _zero_cache,
            'index': _zero_cache,
            'result': _zero_cache,
            'statsResult': _zero_cache,
            'volumeResult': {**_zero_cache, 'entriesRequested': 1, 'entriesStored': 1, 'requests': 1},
            'seriesResult': _zero_cache,
            'labelResult': _zero_cache,
            'instantMetricResult': _zero_cache,
        },
        'index': {
            'totalChunks': 0, 'postFilterChunks': 0,
            'shardsDuration': 0, 'usedBloomFilters': False,
        },
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@alb_loki_bp.route('/ready', methods=['GET'])
def alb_ready():
    return 'ready', 200


@alb_loki_bp.route('/loki/api/v1/labels', methods=['GET'])
def alb_labels():
    return _loki_json({
        'status': 'success',
        'data': list(_VIRTUAL_LABELS.keys()) + _LABEL_FIELDS + _EXTRA_LABEL_FIELDS,
    })


@alb_loki_bp.route('/loki/api/v1/label/<name>/values', methods=['GET'])
def alb_label_values(name: str):
    warehouse = _get_warehouse()
    if not warehouse:
        return _loki_json({'status': 'success', 'data': []})

    # Resolve virtual labels (service_name → elb)
    real_col = _VIRTUAL_LABELS.get(name, name)
    allowed = set(_LABEL_FIELDS + _EXTRA_LABEL_FIELDS)
    if real_col not in allowed:
        return _loki_json({'status': 'success', 'data': []})

    now = datetime.now(tz=timezone.utc)
    from datetime import timedelta
    from_dt = now - timedelta(days=30)
    to_dt = now

    delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
    safe_uri = delta_uri.replace("'", "''")
    where, params = _build_where(_naive(from_dt), _naive(to_dt), {})

    col = f'CAST({real_col} AS VARCHAR)' if real_col == 'elb_status_code' else real_col
    sql = f"""
        SELECT DISTINCT {col}
        FROM delta_scan('{safe_uri}')
        {where} AND {real_col} IS NOT NULL
        ORDER BY 1 LIMIT 500
    """
    rows = _run_sql(warehouse, sql, params)
    return _loki_json({'status': 'success', 'data': [str(r[0]) for r in rows if r[0]]})


@alb_loki_bp.route('/loki/api/v1/series', methods=['GET', 'POST'])
def alb_series():
    """Return distinct stream label combinations — Drilldown uses this for the 'N of X' count."""
    warehouse = _get_warehouse()

    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    args = request.args if request.method == 'GET' else (request.get_json(silent=True) or request.form or {})
    start_ns = _parse_ns(args.get('start'), now_ns - 30 * 24 * 3600 * int(1e9))
    end_ns   = _parse_ns(args.get('end'),   now_ns)
    query    = args.get('query', '')

    from_dt = _ns_to_dt(start_ns)
    to_dt   = _ns_to_dt(end_ns)
    label_filters, _, _ = _parse_logql(query)
    resolved_filters = {
        _VIRTUAL_LABELS.get(k, k): v
        for k, v in label_filters.items()
        if _VIRTUAL_LABELS.get(k, k) in set(_LABEL_FIELDS + _EXTRA_LABEL_FIELDS)
    }

    result = []
    if warehouse:
        delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
        safe_uri = delta_uri.replace("'", "''")
        where, params = _build_where(from_dt, to_dt, resolved_filters)

        sql = f"""
            SELECT DISTINCT elb, request_method, type, domain_name
            FROM delta_scan('{safe_uri}')
            {where}
            LIMIT 500
        """
        rows = _run_sql(warehouse, sql, params)
        for elb_v, method_v, type_v, domain_v in rows:
            elb_s = str(elb_v or '')
            result.append({
                'service_name': elb_s,
                'elb': elb_s,
                'request_method': str(method_v or ''),
                'type': str(type_v or ''),
                'domain_name': str(domain_v or ''),
            })

    if not result:
        result = [{'service_name': '', 'elb': '', 'request_method': '', 'type': '', 'domain_name': ''}]

    return _loki_json({'status': 'success', 'data': result})


@alb_loki_bp.route('/loki/api/v1/query', methods=['GET', 'POST'])
def alb_loki_query():
    """Instant query — returns a single count as a vector."""
    warehouse = _get_warehouse()
    if not warehouse:
        return _loki_json({'status': 'success', 'data': {'resultType': 'vector', 'result': []}})

    if request.method == 'POST':
        body = request.get_json(silent=True) or request.form or {}
        query = body.get('query', '')
        time_param = body.get('time', '')
    else:
        query = request.args.get('query', '')
        time_param = request.args.get('time', '')

    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    time_ns = _parse_ns(time_param, now_ns)
    to_dt = _ns_to_dt(time_ns)
    from datetime import timedelta
    from_dt = _ns_to_dt(time_ns - 3600 * int(1e9))

    label_filters, message_filter, _ = _parse_logql(query)

    delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
    safe_uri = delta_uri.replace("'", "''")
    where, params = _build_where(from_dt, to_dt, label_filters, message_filter)

    sql = f"SELECT COUNT(*) FROM delta_scan('{safe_uri}') {where}"
    rows = _run_sql(warehouse, sql, params)
    count = rows[0][0] if rows else 0

    return _loki_json({
        'status': 'success',
        'data': {
            'resultType': 'vector',
            'result': [{'metric': {}, 'value': [int(to_dt.timestamp()), str(count)]}],
        },
    })


@alb_loki_bp.route('/loki/api/v1/query_range', methods=['GET', 'POST'])
def alb_loki_query_range():
    warehouse = _get_warehouse()
    if not warehouse:
        return _loki_json({'status': 'success', 'data': {'resultType': 'streams', 'result': []}})

    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)

    if request.method == 'POST':
        body = request.get_json(silent=True) or request.form or {}
        query  = body.get('query', '')
        limit  = int(body.get('limit', 1000))
        start_ns = _parse_ns(body.get('start'), now_ns - 6 * 3600 * int(1e9))
        end_ns   = _parse_ns(body.get('end'),   now_ns)
        step_str = body.get('step', '')
        max_dp   = int(body.get('maxDataPoints', 500))
    else:
        query  = request.args.get('query', '')
        limit  = int(request.args.get('limit', 1000))
        start_ns = _parse_ns(request.args.get('start'), now_ns - 6 * 3600 * int(1e9))
        end_ns   = _parse_ns(request.args.get('end'),   now_ns)
        step_str = request.args.get('step', '')
        max_dp   = int(request.args.get('maxDataPoints', 500))

    start_ms = start_ns // 1_000_000
    end_ms   = end_ns   // 1_000_000
    from_dt  = _ns_to_dt(start_ns)
    to_dt    = _ns_to_dt(end_ns)

    label_filters, message_filter, agg_label = _parse_logql(query)

    if _is_metric(query):
        return _handle_alb_metric(
            warehouse, from_dt, to_dt, start_ms, end_ms,
            label_filters, agg_label, step_str, max_dp,
        )

    # ---- Log stream query ----
    delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
    safe_uri = delta_uri.replace("'", "''")
    where, params = _build_where(from_dt, to_dt, label_filters, message_filter)

    sel = ", ".join(_STREAM_COLS)
    sql = f"""
        SELECT {sel}
        FROM delta_scan('{safe_uri}')
        {where}
        ORDER BY time DESC
        LIMIT {int(limit)}
    """

    rows_raw = _run_sql(warehouse, sql, params)
    total = len(rows_raw)

    # Group rows by label combination → Loki streams
    streams: Dict[tuple, dict] = {}
    for raw in rows_raw:
        row = dict(zip(_STREAM_NAMES, raw))
        ts_ns = row['ts_ns']
        if ts_ns is None:
            continue
        label_dict = {f: str(row.get(f) or '') for f in _LABEL_FIELDS}
        # Virtual: service_name → elb (Grafana Drilldown requires this)
        label_dict['service_name'] = label_dict.get('elb', '')
        # Virtual: level derived from HTTP status (Drilldown uses for color-coding)
        status = row.get('elb_status_code')
        if status is None:
            level = 'unknown'
        elif status >= 500:
            level = 'error'
        elif status >= 400:
            level = 'warn'
        else:
            level = 'info'
        label_dict['level'] = level
        key = tuple(sorted(label_dict.items()))
        msg = _row_to_logfmt(row)
        if key not in streams:
            streams[key] = {'stream': label_dict, 'values': []}
        streams[key]['values'].append([str(ts_ns), msg])

    result = list(streams.values())
    for s in result:
        s['values'].sort(key=lambda x: x[0])

    return _loki_json({
        'status': 'success',
        'data': {
            'resultType': 'streams',
            'result': result,
            'stats': {
                'summary': {
                    'totalLinesProcessed': total,
                    'totalEntriesReturned': total,
                    'execTime': 0.001,
                }
            },
        },
    })


def _handle_alb_metric(
    warehouse,
    from_dt: datetime,
    to_dt: datetime,
    start_ms: int,
    end_ms: int,
    label_filters: dict,
    agg_label: Optional[str],
    step_str: str,
    max_dp: int,
) -> Response:
    """Push COUNT(*) GROUP BY into DuckDB — no raw-row transfer."""
    step_s = _parse_step(step_str, start_ms, end_ms, max_dp)

    delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
    safe_uri = delta_uri.replace("'", "''")
    where, params = _build_where(from_dt, to_dt, label_filters)

    bucket = f"(CAST(EXTRACT(EPOCH FROM time) AS BIGINT) / {step_s}) * {step_s}"

    if agg_label:
        # Map Grafana pseudo-label "detected_level" → HTTP status class
        col_map = {
            'detected_level': (
                "CASE"
                "  WHEN elb_status_code >= 500 THEN 'error'"
                "  WHEN elb_status_code >= 400 THEN 'warn'"
                "  WHEN elb_status_code IS NOT NULL THEN 'info'"
                "  ELSE 'unknown' END"
            ),
        }
        group_expr = col_map.get(agg_label, f"CAST({agg_label} AS VARCHAR)")
        select   = f"{bucket} AS bkt, ({group_expr}) AS grp, COUNT(*) AS cnt"
        group_by = f"1, ({group_expr})"
    else:
        select   = f"{bucket} AS bkt, NULL AS grp, COUNT(*) AS cnt"
        group_by = "1"

    sql = f"""
        SELECT {select}
        FROM delta_scan('{safe_uri}')
        {where}
        GROUP BY {group_by}
        ORDER BY 1
    """

    rows_raw = _run_sql(warehouse, sql, params)
    total = sum(r[2] for r in rows_raw) if rows_raw else 0

    series: Dict[str, list] = {}
    for bkt_s, grp, cnt in rows_raw:
        if bkt_s is None:
            continue
        lv = str(grp) if grp is not None else 'total'
        series.setdefault(lv, []).append([int(bkt_s), str(cnt)])

    result: List[dict] = []
    for lv, values in series.items():
        metric = {agg_label: lv} if agg_label else {}
        result.append({'metric': metric, 'values': sorted(values)})

    if not result:
        result = [{'metric': {}, 'values': []}]

    return _loki_json({
        'status': 'success',
        'data': {
            'resultType': 'matrix',
            'result': result,
            'stats': {
                'summary': {
                    'totalLinesProcessed': total,
                    'totalEntriesReturned': len(result),
                    'execTime': 0.001,
                }
            },
        },
    })


# ---------------------------------------------------------------------------
# Extra routes required by Grafana Loki plugin
# ---------------------------------------------------------------------------

@alb_loki_bp.route('/loki/api/v1/label', methods=['GET'])
def alb_label():
    """Alias for /labels (older Grafana versions call this path)."""
    return _loki_json({
        'status': 'success',
        'data': list(_VIRTUAL_LABELS.keys()) + _LABEL_FIELDS + _EXTRA_LABEL_FIELDS,
    })


@alb_loki_bp.route('/loki/api/v1/index/stats', methods=['GET', 'POST'])
def alb_index_stats():
    warehouse = _get_warehouse()
    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    start_ns = _parse_ns(request.args.get('start') or (request.form or {}).get('start'), now_ns - 3600 * int(1e9))
    end_ns   = _parse_ns(request.args.get('end')   or (request.form or {}).get('end'),   now_ns)

    from_dt = _ns_to_dt(start_ns)
    to_dt   = _ns_to_dt(end_ns)

    entries = 0
    if warehouse:
        delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
        safe_uri = delta_uri.replace("'", "''")
        where, params = _build_where(from_dt, to_dt, {})
        sql = f"SELECT COUNT(*) FROM delta_scan('{safe_uri}') {where}"
        rows = _run_sql(warehouse, sql, params)
        entries = rows[0][0] if rows else 0

    # Approximate bytes (avg ALB logfmt line ~250 bytes)
    approx_bytes = entries * 250

    return _loki_json({
        'streams': 1,
        'chunks': max(1, entries // 1000),
        'entries': entries,
        'bytes': approx_bytes,
    })


@alb_loki_bp.route('/loki/api/v1/index/volume', methods=['GET'])
def alb_index_volume():
    """Log volume vector — bytes per label group (used by Grafana Explore volume panel)."""
    warehouse = _get_warehouse()
    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    start_ns = _parse_ns(request.args.get('start'), now_ns - 3600 * int(1e9))
    end_ns   = _parse_ns(request.args.get('end'),   now_ns)
    query    = request.args.get('query', '')

    from_dt = _ns_to_dt(start_ns)
    to_dt   = _ns_to_dt(end_ns)
    label_filters, _, _ = _parse_logql(query)

    result = []
    total_entries = 0

    if warehouse:
        delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
        safe_uri = delta_uri.replace("'", "''")
        # Resolve virtual label filters (service_name → elb), drop unknown labels
        resolved_filters = {
            _VIRTUAL_LABELS.get(k, k): v
            for k, v in label_filters.items()
            if _VIRTUAL_LABELS.get(k, k) in set(_LABEL_FIELDS + _EXTRA_LABEL_FIELDS)
        }
        where, params = _build_where(_naive(from_dt), _naive(to_dt), resolved_filters)

        sql = f"""
            SELECT elb, COUNT(*) AS cnt
            FROM delta_scan('{safe_uri}')
            {where}
            GROUP BY elb
            ORDER BY cnt DESC
            LIMIT 500
        """
        rows = _run_sql(warehouse, sql, params)
        ts = int(to_dt.timestamp())
        for elb_val, cnt in rows:
            total_entries += cnt
            result.append({
                # Only service_name as metric key — matches real Loki format
                'metric': {'service_name': str(elb_val or 'unknown')},
                'value': [ts, str(cnt * 250)],   # approx bytes
            })

    _FULL_STATS = {
        'summary': {
            'bytesProcessedPerSecond': 0, 'linesProcessedPerSecond': 0,
            'totalBytesProcessed': total_entries * 250,
            'totalLinesProcessed': total_entries,
            'execTime': 0.001, 'queueTime': 0, 'subqueries': 0,
            'totalEntriesReturned': len(result),
            'splits': 1, 'shards': 0,
            'totalPostFilterLines': total_entries,
            'totalStructuredMetadataBytesProcessed': 0,
        }
    }

    return _loki_json({
        'status': 'success',
        'data': {'resultType': 'vector', 'result': result,
                 'stats': _loki_stats(total_entries, total_entries * 250, len(result))},
    })


@alb_loki_bp.route('/loki/api/v1/index/volume_range', methods=['GET'])
def alb_index_volume_range():
    """Log volume matrix — bytes per time bucket per label group."""
    warehouse = _get_warehouse()
    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    start_ns = _parse_ns(request.args.get('start'), now_ns - 3600 * int(1e9))
    end_ns   = _parse_ns(request.args.get('end'),   now_ns)
    query    = request.args.get('query', '')
    step_str = request.args.get('step', '60')

    start_ms = start_ns // 1_000_000
    end_ms   = end_ns   // 1_000_000
    from_dt  = _ns_to_dt(start_ns)
    to_dt    = _ns_to_dt(end_ns)
    step_s   = _parse_step(step_str, start_ms, end_ms, 500)

    label_filters, _, _ = _parse_logql(query)
    resolved_filters = {
        _VIRTUAL_LABELS.get(k, k): v
        for k, v in label_filters.items()
        if _VIRTUAL_LABELS.get(k, k) in set(_LABEL_FIELDS + _EXTRA_LABEL_FIELDS)
    }

    result = []
    total_entries = 0

    if warehouse:
        delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
        safe_uri = delta_uri.replace("'", "''")
        where, params = _build_where(_naive(from_dt), _naive(to_dt), resolved_filters)
        bucket = f"(CAST(EXTRACT(EPOCH FROM time) AS BIGINT) / {step_s}) * {step_s}"

        sql = f"""
            SELECT {bucket} AS bkt, elb, COUNT(*) AS cnt
            FROM delta_scan('{safe_uri}')
            {where}
            GROUP BY 1, elb
            ORDER BY 1
        """
        rows = _run_sql(warehouse, sql, params)

        series: Dict[str, list] = {}
        for bkt_s, elb_val, cnt in rows:
            if bkt_s is None:
                continue
            key = str(elb_val or 'unknown')
            total_entries += cnt
            series.setdefault(key, []).append([int(bkt_s), str(cnt * 250)])

        # Only service_name as metric key — matches real Loki format
        result = [{'metric': {'service_name': k}, 'values': sorted(v)} for k, v in series.items()]

    _FULL_STATS = {
        'summary': {
            'totalBytesProcessed': total_entries * 250,
            'totalLinesProcessed': total_entries,
            'execTime': 0.001, 'queueTime': 0, 'subqueries': 0,
            'totalEntriesReturned': len(result),
            'splits': 1, 'shards': 0,
            'totalPostFilterLines': total_entries,
            'totalStructuredMetadataBytesProcessed': 0,
        }
    }

    return _loki_json({
        'status': 'success',
        'data': {'resultType': 'matrix', 'result': result,
                 'stats': _loki_stats(total_entries, total_entries * 250, len(result))},
    })


@alb_loki_bp.route('/loki/api/v1/detected_labels', methods=['GET'])
def alb_detected_labels():
    # Include virtual labels first so Drilldown picks up service_name + level
    detected = [{'label': lbl, 'cardinality': 5} for lbl in _VIRTUAL_LABELS.keys()]
    detected += [{'label': 'level', 'cardinality': 3}]   # virtual: error/warn/info
    detected += [{'label': lbl, 'cardinality': 5} for lbl in _LABEL_FIELDS]
    detected += [{'label': lbl, 'cardinality': 10} for lbl in _EXTRA_LABEL_FIELDS]
    return _loki_json({'detectedLabels': detected})


# ALB logfmt fields exposed as detected fields in Grafana Explore
_ALB_DETECTED_FIELDS = [
    ('method', 'string', 8, ['logfmt']),
    ('path', 'string', 1000, ['logfmt']),
    ('status', 'string', 50, ['logfmt']),
    ('latency', 'string', 100, ['logfmt']),
    ('client', 'string', 500, ['logfmt']),
    ('bytes_in', 'string', 100, ['logfmt']),
    ('bytes_out', 'string', 100, ['logfmt']),
    ('host', 'string', 10, ['logfmt']),
    ('error_reason', 'string', 20, ['logfmt']),
    ('trace', 'string', 200, ['logfmt']),
    ('ua', 'string', 200, ['logfmt']),
]


@alb_loki_bp.route('/loki/api/v1/detected_fields', methods=['GET'])
def alb_detected_fields():
    fields = [
        {'label': name, 'type': typ, 'cardinality': card, 'parsers': parsers}
        for name, typ, card, parsers in _ALB_DETECTED_FIELDS
    ]
    return _loki_json({'fields': fields, 'limit': 1000})


@alb_loki_bp.route('/loki/api/v1/detected_field/<name>/values', methods=['GET', 'POST'])
def alb_detected_field_values(name: str):
    """Return distinct values for a logfmt field (backed by DuckDB for DB columns)."""
    # Map logfmt field name → ALB table column name
    _field_col = {
        'method': 'request_method',
        'status': 'elb_status_code',
        'client': 'client_ip',
        'host': 'domain_name',
        'error_reason': 'error_reason',
    }
    warehouse = _get_warehouse()
    limit = int(request.args.get('limit') or (request.form or {}).get('limit') or 500)

    col = _field_col.get(name)
    if not col or not warehouse:
        # path, latency, trace, ua, bytes — return empty (too high-cardinality)
        return _loki_json({'values': [], 'limit': limit})

    now_ns = int(datetime.now(tz=timezone.utc).timestamp() * 1e9)
    from datetime import timedelta
    from_dt = datetime.now(tz=timezone.utc) - timedelta(days=7)
    to_dt   = datetime.now(tz=timezone.utc)
    where, params = _build_where(_naive(from_dt), _naive(to_dt), {})

    delta_uri = warehouse._get_delta_uri(warehouse.alb_table_name)
    safe_uri = delta_uri.replace("'", "''")

    col_expr = f'CAST({col} AS VARCHAR)' if col == 'elb_status_code' else col
    sql = f"""
        SELECT DISTINCT {col_expr}
        FROM delta_scan('{safe_uri}')
        {where} AND {col} IS NOT NULL
        ORDER BY 1 LIMIT {int(limit)}
    """
    rows = _run_sql(warehouse, sql, params)
    return _loki_json({'values': [str(r[0]) for r in rows if r[0]], 'limit': limit})


@alb_loki_bp.route('/loki/api/v1/patterns', methods=['GET'])
def alb_patterns():
    return _loki_json({'status': 'success', 'data': []})


@alb_loki_bp.route('/loki/api/v1/drilldown-limits', methods=['GET'])
def alb_drilldown_limits():
    return _loki_json({'status': 'success', 'data': {'maxLines': 10000, 'maxIntervalSeconds': 3600}})


@alb_loki_bp.route('/loki/api/v1/status/buildinfo', methods=['GET'])
def alb_buildinfo():
    return _loki_json({
        'version': '2.9.0',
        'revision': '12345678',
        'branch': 'main',
        'buildUser': 'cloudwatch-local',
        'buildDate': '2024-01-01',
        'goVersion': 'go1.21.0',
        'edition': 'oss',
        'features': {
            'metric_aggregation': True,
            'log_push_api': False,
            'ruler_api': False,
            'alertmanager_api': False,
        },
        'limits': {'retention_period': '7d', 'max_query_lookback': '30d'},
    })


@alb_loki_bp.route('/loki', methods=['GET'])
def alb_loki_root():
    return _loki_json({'status': 'ok', 'message': 'ALB Loki-compatible endpoint'})

