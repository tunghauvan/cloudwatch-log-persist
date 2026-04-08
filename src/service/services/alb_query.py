"""
ALB analytics query service.

Scans the alb_logs Delta table via DuckDB delta_scan() — all aggregations are
pushed into SQL, so Python never processes raw rows.  All public functions
return plain Python dicts/lists — ready for JSON serialisation.

Grafana SimpleJSON metric catalogue
------------------------------------
Time-series  : request_rate, error_rate, error_rate_pct,
               2xx_rate, 3xx_rate, 4xx_rate, 5xx_rate,
               latency_p50, latency_p95, latency_p99,
               bandwidth_in, bandwidth_out
Table        : top_paths, top_ips, status_distribution, latency_stats
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("service.alb_query")

TIMESERIES_METRICS: List[str] = [
    "request_rate",
    "error_rate",
    "error_rate_pct",
    "2xx_rate",
    "3xx_rate",
    "4xx_rate",
    "5xx_rate",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "bandwidth_in",
    "bandwidth_out",
]

TABLE_METRICS: List[str] = [
    "top_paths",
    "top_ips",
    "status_distribution",
    "latency_stats",
]

ALL_METRICS: List[str] = TIMESERIES_METRICS + TABLE_METRICS


# ---------------------------------------------------------------------------
# Internal: DuckDB execution helper
# ---------------------------------------------------------------------------

def _duckdb_query(warehouse, sql: str, params: list = None):
    """Run SQL against the warehouse DuckDB connection; return fetchall rows."""
    conn = warehouse._get_duckdb_conn()
    try:
        return conn.execute(sql, params or []).fetchall()
    except Exception as e:
        msg = str(e)
        if any(x in msg for x in (
            "not a Delta table", "No log files", "doesn't exist",
            "Invariant violation", "TableNotFound",
        )):
            return []
        if "404" in msg or "Not Found" in msg:
            warehouse._reset_duckdb_conn()
            try:
                return warehouse._get_duckdb_conn().execute(sql, params or []).fetchall()
            except Exception:
                warehouse._reset_duckdb_conn()
                return []
        logger.warning(f"[ALBQuery] DuckDB error: {type(e).__name__}: {e}")
        warehouse._reset_duckdb_conn()
        return []


def _alb_scan_sql(warehouse, table_name: str, from_dt: datetime, to_dt: datetime) -> tuple:
    """Return (delta_uri, where_clause, params) for an alb_logs time-range query."""
    uri = warehouse._get_delta_uri(table_name)
    safe_uri = uri.replace("'", "''")

    # Ensure tz-aware → naive UTC for the Delta (us, no-tz) timestamp column
    def _naive(dt: datetime) -> datetime:
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    conditions = ["time >= ?", "time <= ?"]
    params = [_naive(from_dt), _naive(to_dt)]

    # Date-column pruning (avoids DuckDB equality-on-stats bug: only add when range > 1 day)
    from_date = from_dt.strftime('%Y-%m-%d')
    to_date = to_dt.strftime('%Y-%m-%d')
    if from_date != to_date:
        conditions += ["date >= ?", "date <= ?"]
        params += [from_date, to_date]

    where = "WHERE " + " AND ".join(conditions)
    return safe_uri, where, params


def _bucket_expr(interval_ms: int) -> str:
    step_s = max(60, interval_ms // 1000)
    return f"(CAST(EXTRACT(EPOCH FROM time) AS BIGINT) / {step_s}) * {step_s}"


# ---------------------------------------------------------------------------
# Public: time-series
# ---------------------------------------------------------------------------

def query_metric_timeseries(
    warehouse,
    table_name: str,
    metric: str,
    from_dt: datetime,
    to_dt: datetime,
    interval_ms: int,
) -> List[List]:
    """Return [[value, ts_ms], …] for *metric* over the given time range."""
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    bkt = _bucket_expr(interval_ms)

    if metric == "request_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "error_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} AND elb_status_code >= 400 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "error_rate_pct":
        sql = f"""
            SELECT {bkt} AS bkt,
                   ROUND(100.0 * SUM(CASE WHEN elb_status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*), 4)
            FROM delta_scan('{safe_uri}') {where}
            GROUP BY 1 ORDER BY 1
        """
        rows = _duckdb_query(warehouse, sql, params)
        return [[float(v) if v is not None else 0.0, int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "2xx_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} AND elb_status_code >= 200 AND elb_status_code < 300 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "3xx_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} AND elb_status_code >= 300 AND elb_status_code < 400 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "4xx_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} AND elb_status_code >= 400 AND elb_status_code < 500 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "5xx_rate":
        sql = f"SELECT {bkt} AS bkt, COUNT(*) FROM delta_scan('{safe_uri}') {where} AND elb_status_code >= 500 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v), int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "latency_p50":
        sql = f"SELECT {bkt} AS bkt, ROUND(QUANTILE_CONT(target_processing_time, 0.5), 6) FROM delta_scan('{safe_uri}') {where} AND target_processing_time >= 0 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[float(v) if v is not None else 0.0, int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "latency_p95":
        sql = f"SELECT {bkt} AS bkt, ROUND(QUANTILE_CONT(target_processing_time, 0.95), 6) FROM delta_scan('{safe_uri}') {where} AND target_processing_time >= 0 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[float(v) if v is not None else 0.0, int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "latency_p99":
        sql = f"SELECT {bkt} AS bkt, ROUND(QUANTILE_CONT(target_processing_time, 0.99), 6) FROM delta_scan('{safe_uri}') {where} AND target_processing_time >= 0 GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[float(v) if v is not None else 0.0, int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "bandwidth_in":
        sql = f"SELECT {bkt} AS bkt, SUM(received_bytes) FROM delta_scan('{safe_uri}') {where} GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v) if v is not None else 0, int(bkt_s) * 1000] for bkt_s, v in rows]

    if metric == "bandwidth_out":
        sql = f"SELECT {bkt} AS bkt, SUM(sent_bytes) FROM delta_scan('{safe_uri}') {where} GROUP BY 1 ORDER BY 1"
        rows = _duckdb_query(warehouse, sql, params)
        return [[int(v) if v is not None else 0, int(bkt_s) * 1000] for bkt_s, v in rows]

    return []


# ---------------------------------------------------------------------------
# Public: table queries
# ---------------------------------------------------------------------------

def query_top_paths(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 20,
) -> Dict[str, Any]:
    columns = [
        {"text": "Path", "type": "string"},
        {"text": "Requests", "type": "number"},
        {"text": "Errors", "type": "number"},
        {"text": "Error %", "type": "number"},
    ]
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    sql = f"""
        SELECT request_path,
               COUNT(*) AS cnt,
               SUM(CASE WHEN elb_status_code >= 400 THEN 1 ELSE 0 END) AS errs,
               ROUND(100.0 * SUM(CASE WHEN elb_status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*), 2) AS err_pct
        FROM delta_scan('{safe_uri}')
        {where} AND request_path IS NOT NULL
        GROUP BY request_path
        ORDER BY cnt DESC
        LIMIT {int(limit)}
    """
    rows_raw = _duckdb_query(warehouse, sql, params)
    rows = [[path, int(cnt), int(errs), float(err_pct) if err_pct is not None else 0.0]
            for path, cnt, errs, err_pct in rows_raw]
    return {"type": "table", "columns": columns, "rows": rows}


def query_top_ips(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 20,
) -> Dict[str, Any]:
    columns = [
        {"text": "Client IP", "type": "string"},
        {"text": "Requests", "type": "number"},
    ]
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    sql = f"""
        SELECT client_ip, COUNT(*) AS cnt
        FROM delta_scan('{safe_uri}')
        {where} AND client_ip IS NOT NULL
        GROUP BY client_ip
        ORDER BY cnt DESC
        LIMIT {int(limit)}
    """
    rows_raw = _duckdb_query(warehouse, sql, params)
    rows = [[ip, int(cnt)] for ip, cnt in rows_raw]
    return {"type": "table", "columns": columns, "rows": rows}


def query_status_distribution(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    columns = [
        {"text": "Status Code", "type": "number"},
        {"text": "Count", "type": "number"},
        {"text": "Percentage", "type": "number"},
    ]
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    sql = f"""
        WITH counts AS (
            SELECT elb_status_code, COUNT(*) AS cnt
            FROM delta_scan('{safe_uri}')
            {where} AND elb_status_code IS NOT NULL
            GROUP BY elb_status_code
        ),
        total AS (SELECT SUM(cnt) AS t FROM counts)
        SELECT elb_status_code, cnt, ROUND(100.0 * cnt / t, 2)
        FROM counts, total
        ORDER BY elb_status_code
    """
    rows_raw = _duckdb_query(warehouse, sql, params)
    rows = [[int(code), int(cnt), float(pct) if pct is not None else 0.0]
            for code, cnt, pct in rows_raw]
    return {"type": "table", "columns": columns, "rows": rows}


def query_latency_stats(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    columns = [
        {"text": "Metric", "type": "string"},
        {"text": "Value (s)", "type": "number"},
    ]
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    sql = f"""
        SELECT
            ROUND(AVG(target_processing_time), 6)            AS mean,
            ROUND(QUANTILE_CONT(target_processing_time, 0.50), 6) AS p50,
            ROUND(QUANTILE_CONT(target_processing_time, 0.75), 6) AS p75,
            ROUND(QUANTILE_CONT(target_processing_time, 0.90), 6) AS p90,
            ROUND(QUANTILE_CONT(target_processing_time, 0.95), 6) AS p95,
            ROUND(QUANTILE_CONT(target_processing_time, 0.99), 6) AS p99,
            ROUND(MAX(target_processing_time), 6)             AS max
        FROM delta_scan('{safe_uri}')
        {where} AND target_processing_time IS NOT NULL AND target_processing_time >= 0
    """
    rows_raw = _duckdb_query(warehouse, sql, params)
    if not rows_raw or rows_raw[0][0] is None:
        return {"type": "table", "columns": columns, "rows": []}
    r = rows_raw[0]
    rows = [
        ["mean", float(r[0]) if r[0] else 0.0],
        ["p50",  float(r[1]) if r[1] else 0.0],
        ["p75",  float(r[2]) if r[2] else 0.0],
        ["p90",  float(r[3]) if r[3] else 0.0],
        ["p95",  float(r[4]) if r[4] else 0.0],
        ["p99",  float(r[5]) if r[5] else 0.0],
        ["max",  float(r[6]) if r[6] else 0.0],
    ]
    return {"type": "table", "columns": columns, "rows": rows}


# ---------------------------------------------------------------------------
# Public: summary stats (REST /alb/stats)
# ---------------------------------------------------------------------------

def query_stats(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    safe_uri, where, params = _alb_scan_sql(warehouse, table_name, from_dt, to_dt)
    sql = f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN elb_status_code >= 400 THEN 1 ELSE 0 END) AS errors,
            SUM(received_bytes)  AS rx,
            SUM(sent_bytes)      AS tx,
            ROUND(QUANTILE_CONT(CASE WHEN target_processing_time >= 0 THEN target_processing_time END, 0.50), 6) AS p50,
            ROUND(QUANTILE_CONT(CASE WHEN target_processing_time >= 0 THEN target_processing_time END, 0.99), 6) AS p99
        FROM delta_scan('{safe_uri}')
        {where}
    """
    rows_raw = _duckdb_query(warehouse, sql, params)
    if not rows_raw or rows_raw[0][0] is None or rows_raw[0][0] == 0:
        return {
            "total_requests": 0,
            "error_count": 0,
            "error_rate_pct": 0.0,
            "total_received_bytes": 0,
            "total_sent_bytes": 0,
            "latency_p50_s": None,
            "latency_p99_s": None,
        }
    total, errors, rx, tx, p50, p99 = rows_raw[0]
    total = int(total) if total else 0
    errors = int(errors) if errors else 0
    return {
        "total_requests": total,
        "error_count": errors,
        "error_rate_pct": round(errors / total * 100, 2) if total > 0 else 0.0,
        "total_received_bytes": int(rx) if rx else 0,
        "total_sent_bytes": int(tx) if tx else 0,
        "latency_p50_s": float(p50) if p50 is not None else None,
        "latency_p99_s": float(p99) if p99 is not None else None,
    }


TIMESERIES_METRICS: List[str] = [
    "request_rate",
    "error_rate",
    "error_rate_pct",
    "2xx_rate",
    "3xx_rate",
    "4xx_rate",
    "5xx_rate",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "bandwidth_in",
    "bandwidth_out",
]

TABLE_METRICS: List[str] = [
    "top_paths",
    "top_ips",
    "status_distribution",
    "latency_stats",
]

ALL_METRICS: List[str] = TIMESERIES_METRICS + TABLE_METRICS


# ---------------------------------------------------------------------------
# Internal: data loading
# ---------------------------------------------------------------------------

def _load_df(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> pd.DataFrame:
    """
    Scan alb_logs for rows with time in [from_dt, to_dt].

    Uses PyIceberg partition-pruning so only relevant day-partitions are read.
    Returns an empty DataFrame on any error (table not found, empty result, …).
    """
    try:
        from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

        table_id = f"{warehouse.namespace}.{table_name}"
        table = warehouse.catalog.load_table(table_id)

        # PyIceberg TimestampType() → microseconds since epoch (no tz)
        def _to_us(dt: datetime) -> int:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)

        row_filter = And(
            GreaterThanOrEqual("time", _to_us(from_dt)),
            LessThanOrEqual("time", _to_us(to_dt)),
        )

        arrow_table = table.scan(row_filter=row_filter).to_arrow()
        if arrow_table.num_rows == 0:
            return pd.DataFrame()

        df = arrow_table.to_pandas()
        if "time" in df.columns and not df.empty:
            df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        return df

    except Exception as e:
        logger.warning(f"[ALBQuery] Scan error for '{table_name}': {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Internal: time-series helpers
# ---------------------------------------------------------------------------

def _bucket(df: pd.DataFrame, interval_ms: int) -> pd.Series:
    freq = max(pd.Timedelta(milliseconds=interval_ms), pd.Timedelta(seconds=60))
    return df["time"].dt.floor(freq)


def _status_mask(df: pd.DataFrame, low: int, high: Optional[int] = None) -> pd.Series:
    sc = df.get("elb_status_code")
    if sc is None or sc.empty:
        return pd.Series([False] * len(df), index=df.index)
    valid = sc.notna()
    if high is None:
        return valid & (sc >= low)
    return valid & (sc >= low) & (sc < high)


def _ts_count(
    df: pd.DataFrame, interval_ms: int, mask: Optional[pd.Series] = None
) -> List[List]:
    if df.empty:
        return []
    src = df if mask is None else df[mask]
    if src.empty:
        return []
    series = src.groupby(_bucket(src, interval_ms)).size()
    return [[int(v), int(k.timestamp() * 1000)] for k, v in series.items()]


def _ts_sum(df: pd.DataFrame, col: str, interval_ms: int) -> List[List]:
    if df.empty or col not in df.columns:
        return []
    valid = df[df[col].notna()]
    if valid.empty:
        return []
    series = valid.groupby(_bucket(valid, interval_ms))[col].sum()
    return [[float(v), int(k.timestamp() * 1000)] for k, v in series.items()]


def _ts_percentile(
    df: pd.DataFrame, col: str, q: float, interval_ms: int
) -> List[List]:
    if df.empty or col not in df.columns:
        return []
    valid = df[df[col].notna() & (df[col] >= 0)]
    if valid.empty:
        return []
    series = valid.groupby(_bucket(valid, interval_ms))[col].quantile(q)
    return [[round(float(v), 6), int(k.timestamp() * 1000)] for k, v in series.items()]


def _ts_error_rate_pct(df: pd.DataFrame, interval_ms: int) -> List[List]:
    if df.empty or "elb_status_code" not in df.columns:
        return []
    df2 = df.copy()
    df2["_is_err"] = _status_mask(df2, 400).astype(int)
    buckets = _bucket(df2, interval_ms)
    grouped = df2.groupby(buckets)
    pct = (grouped["_is_err"].sum() / grouped.size() * 100).fillna(0)
    return [[round(float(v), 4), int(k.timestamp() * 1000)] for k, v in pct.items()]


# ---------------------------------------------------------------------------
# Public: time-series query dispatcher
# ---------------------------------------------------------------------------

def query_metric_timeseries(
    warehouse,
    table_name: str,
    metric: str,
    from_dt: datetime,
    to_dt: datetime,
    interval_ms: int,
) -> List[List]:
    """
    Return [[value, ts_ms], …] for *metric* over the given time range.
    Returns [] if the table is empty or the metric is unknown.
    """
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    if df.empty:
        return []

    if metric == "request_rate":
        return _ts_count(df, interval_ms)
    if metric == "error_rate":
        return _ts_count(df, interval_ms, mask=_status_mask(df, 400))
    if metric == "error_rate_pct":
        return _ts_error_rate_pct(df, interval_ms)
    if metric == "2xx_rate":
        return _ts_count(df, interval_ms, mask=_status_mask(df, 200, 300))
    if metric == "3xx_rate":
        return _ts_count(df, interval_ms, mask=_status_mask(df, 300, 400))
    if metric == "4xx_rate":
        return _ts_count(df, interval_ms, mask=_status_mask(df, 400, 500))
    if metric == "5xx_rate":
        return _ts_count(df, interval_ms, mask=_status_mask(df, 500))
    if metric == "latency_p50":
        return _ts_percentile(df, "target_processing_time", 0.50, interval_ms)
    if metric == "latency_p95":
        return _ts_percentile(df, "target_processing_time", 0.95, interval_ms)
    if metric == "latency_p99":
        return _ts_percentile(df, "target_processing_time", 0.99, interval_ms)
    if metric == "bandwidth_in":
        return _ts_sum(df, "received_bytes", interval_ms)
    if metric == "bandwidth_out":
        return _ts_sum(df, "sent_bytes", interval_ms)
    return []


# ---------------------------------------------------------------------------
# Public: table queries
# ---------------------------------------------------------------------------

def query_top_paths(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 20,
) -> Dict[str, Any]:
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    columns = [
        {"text": "Path", "type": "string"},
        {"text": "Requests", "type": "number"},
        {"text": "Errors", "type": "number"},
        {"text": "Error %", "type": "number"},
    ]
    if df.empty or "request_path" not in df.columns:
        return {"type": "table", "columns": columns, "rows": []}

    df2 = df.copy()
    df2["_is_err"] = _status_mask(df2, 400).astype(int)
    agg = df2.groupby("request_path").agg(
        count=("request_path", "size"),
        errors=("_is_err", "sum"),
    ).reset_index()
    agg["error_pct"] = (agg["errors"] / agg["count"] * 100).round(2)
    agg = agg.sort_values("count", ascending=False).head(limit)

    rows = [
        [row["request_path"], int(row["count"]), int(row["errors"]), float(row["error_pct"])]
        for _, row in agg.iterrows()
    ]
    return {"type": "table", "columns": columns, "rows": rows}


def query_top_ips(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
    limit: int = 20,
) -> Dict[str, Any]:
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    columns = [
        {"text": "Client IP", "type": "string"},
        {"text": "Requests", "type": "number"},
    ]
    if df.empty or "client_ip" not in df.columns:
        return {"type": "table", "columns": columns, "rows": []}

    top = (
        df.groupby("client_ip")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(limit)
    )
    rows = [[row["client_ip"], int(row["count"])] for _, row in top.iterrows()]
    return {"type": "table", "columns": columns, "rows": rows}


def query_status_distribution(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    columns = [
        {"text": "Status Code", "type": "number"},
        {"text": "Count", "type": "number"},
        {"text": "Percentage", "type": "number"},
    ]
    if df.empty or "elb_status_code" not in df.columns:
        return {"type": "table", "columns": columns, "rows": []}

    dist = (
        df.dropna(subset=["elb_status_code"])
        .groupby("elb_status_code")
        .size()
        .reset_index(name="count")
        .sort_values("elb_status_code")
    )
    total = int(dist["count"].sum())
    rows = [
        [int(row["elb_status_code"]), int(row["count"]), round(row["count"] / total * 100, 2)]
        for _, row in dist.iterrows()
    ]
    return {"type": "table", "columns": columns, "rows": rows}


def query_latency_stats(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    columns = [
        {"text": "Metric", "type": "string"},
        {"text": "Value (s)", "type": "number"},
    ]
    if df.empty or "target_processing_time" not in df.columns:
        return {"type": "table", "columns": columns, "rows": []}

    col = df["target_processing_time"].dropna()
    col = col[col >= 0]
    if col.empty:
        return {"type": "table", "columns": columns, "rows": []}

    rows = [
        ["mean", round(float(col.mean()), 6)],
        ["p50",  round(float(col.quantile(0.50)), 6)],
        ["p75",  round(float(col.quantile(0.75)), 6)],
        ["p90",  round(float(col.quantile(0.90)), 6)],
        ["p95",  round(float(col.quantile(0.95)), 6)],
        ["p99",  round(float(col.quantile(0.99)), 6)],
        ["max",  round(float(col.max()), 6)],
    ]
    return {"type": "table", "columns": columns, "rows": rows}


# ---------------------------------------------------------------------------
# Public: summary stats (REST /alb/stats)
# ---------------------------------------------------------------------------

def query_stats(
    warehouse,
    table_name: str,
    from_dt: datetime,
    to_dt: datetime,
) -> Dict[str, Any]:
    df = _load_df(warehouse, table_name, from_dt, to_dt)
    if df.empty:
        return {
            "total_requests": 0,
            "error_count": 0,
            "error_rate_pct": 0.0,
            "total_received_bytes": 0,
            "total_sent_bytes": 0,
            "latency_p50_s": None,
            "latency_p99_s": None,
        }

    total = len(df)
    errors = int(_status_mask(df, 400).sum())

    lat = (
        df["target_processing_time"].dropna()
        if "target_processing_time" in df.columns
        else pd.Series(dtype=float)
    )
    lat = lat[lat >= 0]

    return {
        "total_requests": total,
        "error_count": errors,
        "error_rate_pct": round(errors / total * 100, 2) if total > 0 else 0.0,
        "total_received_bytes": int(df["received_bytes"].sum()) if "received_bytes" in df.columns else 0,
        "total_sent_bytes": int(df["sent_bytes"].sum()) if "sent_bytes" in df.columns else 0,
        "latency_p50_s": round(float(lat.quantile(0.50)), 6) if not lat.empty else None,
        "latency_p99_s": round(float(lat.quantile(0.99)), 6) if not lat.empty else None,
    }
