"""
ALB analytics query service.

Scans the alb_logs Iceberg table with partition-pruning time filters,
then aggregates results using pandas.  All public functions return plain
Python dicts/lists — ready for JSON serialisation.

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

import pandas as pd

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
