"""
ALB routes.

Ingestion
---------
POST /alb/s3-event
    Accepts an S3 Event Notification (same JSON structure SQS delivers).
    Downloads each ALB log file from S3, parses it, writes rows to the
    alb_logs Iceberg table.  Use this to test the pipeline without SQS.

Analytics (REST)
----------------
GET  /alb/stats            summary counts + error rate + latency
GET  /alb/timeseries       request_rate (or any metric) over time
GET  /alb/status-codes     HTTP status code distribution
GET  /alb/top-paths        top N paths by request count
GET  /alb/top-ips          top N client IPs by request count
GET  /alb/latency          latency percentiles

Grafana SimpleJSON datasource  (configure URL = .../alb/grafana)
----------------------------------------------------------------
GET  /alb/grafana/         health check → "OK"
POST /alb/grafana/search   → list of available metrics
POST /alb/grafana/query    → timeseries or table data
POST /alb/grafana/annotations → []
POST /alb/grafana/tag-keys   → available filter keys
POST /alb/grafana/tag-values → values for a filter key

Query params accepted by REST analytics endpoints
--------------------------------------------------
from    ISO-8601 or epoch-ms  (default: 24 h ago)
to      ISO-8601 or epoch-ms  (default: now)
limit   int                   (default: 20, top-paths / top-ips only)
metric  str                   (default: request_rate, timeseries only)
interval_ms int               (default: 3 600 000 = 1 h, timeseries only)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from flask import Blueprint, jsonify, request

logger = logging.getLogger("service.routes.alb")

alb_bp = Blueprint("alb", __name__, url_prefix="/alb")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_warehouse():
    from service.server import warehouse
    return warehouse


def _get_config() -> dict:
    try:
        from service.server import config
        return config
    except Exception:
        return {}


def _get_processor():
    config = _get_config()
    warehouse = _get_warehouse()
    if not warehouse:
        return None, "warehouse not initialized"
    if not config.get("alb", {}).get("enabled", True):
        return None, "alb pipeline is disabled (set alb.enabled: true in config.yaml)"
    from service.services.alb_processor import ALBProcessor
    return ALBProcessor(warehouse, config), None


def _parse_datetime(s: Optional[str], default: datetime) -> datetime:
    if not s:
        return default
    # epoch milliseconds
    try:
        ms = float(s)
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except ValueError:
        pass
    # ISO-8601
    try:
        s = s.rstrip("Z")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return default


def _time_range():
    """Parse ?from / ?to query params. Defaults: last 24 h."""
    now = datetime.now(tz=timezone.utc)
    from_dt = _parse_datetime(request.args.get("from"), now - timedelta(hours=24))
    to_dt   = _parse_datetime(request.args.get("to"),   now)
    return from_dt, to_dt


def _table_name() -> str:
    return _get_config().get("alb", {}).get("table_name", "alb_logs")


# ---------------------------------------------------------------------------
# Ingestion: S3 event
# ---------------------------------------------------------------------------

@alb_bp.route("/s3-event", methods=["POST"])
def receive_s3_event():
    """
    Accept an S3 Event Notification JSON and process each ALB log file.
    """
    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON body"}), 400

    records = body.get("Records", [])
    if not records:
        return jsonify({"error": "no Records found in payload"}), 400

    s3_records = []
    for rec in records:
        source = rec.get("eventSource", "")
        if source and source != "aws:s3":
            return jsonify({"error": f"unexpected eventSource: {source}"}), 400
        s3_info = rec.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name")
        key = s3_info.get("object", {}).get("key")
        if not bucket or not key:
            return jsonify(
                {"error": "each Record must have s3.bucket.name and s3.object.key"}
            ), 400
        s3_records.append({"bucket": bucket, "key": key})

    processor, err = _get_processor()
    if err:
        return jsonify({"error": err}), 503

    results = []
    for rec in s3_records:
        result = processor.process_s3_object(rec["bucket"], rec["key"])
        result["bucket"] = rec["bucket"]
        result["key"] = rec["key"]
        results.append(result)

    total_rows = sum(r.get("rows_written", 0) for r in results)
    errors = [r for r in results if r.get("status") == "error"]
    status_code = 207 if errors else 200
    return jsonify({"processed": len(results), "rows_written": total_rows, "results": results}), status_code


# ---------------------------------------------------------------------------
# REST analytics
# ---------------------------------------------------------------------------

@alb_bp.route("/stats", methods=["GET"])
def alb_stats():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    from service.services.alb_query import query_stats
    result = query_stats(warehouse, _table_name(), from_dt, to_dt)
    return jsonify(result)


@alb_bp.route("/timeseries", methods=["GET"])
def alb_timeseries():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    metric = request.args.get("metric", "request_rate")
    interval_ms = int(request.args.get("interval_ms", 3_600_000))
    from service.services.alb_query import query_metric_timeseries, TIMESERIES_METRICS
    if metric not in TIMESERIES_METRICS:
        return jsonify({"error": f"unknown metric '{metric}'", "valid": TIMESERIES_METRICS}), 400
    datapoints = query_metric_timeseries(warehouse, _table_name(), metric, from_dt, to_dt, interval_ms)
    return jsonify({"metric": metric, "datapoints": datapoints})


@alb_bp.route("/status-codes", methods=["GET"])
def alb_status_codes():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    from service.services.alb_query import query_status_distribution
    return jsonify(query_status_distribution(warehouse, _table_name(), from_dt, to_dt))


@alb_bp.route("/top-paths", methods=["GET"])
def alb_top_paths():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    limit = int(request.args.get("limit", 20))
    from service.services.alb_query import query_top_paths
    return jsonify(query_top_paths(warehouse, _table_name(), from_dt, to_dt, limit))


@alb_bp.route("/top-ips", methods=["GET"])
def alb_top_ips():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    limit = int(request.args.get("limit", 20))
    from service.services.alb_query import query_top_ips
    return jsonify(query_top_ips(warehouse, _table_name(), from_dt, to_dt, limit))


@alb_bp.route("/latency", methods=["GET"])
def alb_latency():
    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503
    from_dt, to_dt = _time_range()
    from service.services.alb_query import query_latency_stats
    return jsonify(query_latency_stats(warehouse, _table_name(), from_dt, to_dt))


# ---------------------------------------------------------------------------
# Grafana SimpleJSON datasource
# Configure datasource URL as: http://<host>:<port>/alb/grafana
# ---------------------------------------------------------------------------

@alb_bp.route("/grafana", methods=["GET"])
@alb_bp.route("/grafana/", methods=["GET"])
def grafana_health():
    """SimpleJSON health check — must return 200."""
    return "OK", 200


@alb_bp.route("/grafana/search", methods=["POST"])
def grafana_search():
    """Return all available metric names for the metric picker."""
    from service.services.alb_query import ALL_METRICS
    return jsonify(ALL_METRICS)


@alb_bp.route("/grafana/query", methods=["POST"])
def grafana_query():
    """
    Main Grafana SimpleJSON query endpoint.

    Each target is handled independently:
      - type "timeserie" (or metric in TIMESERIES_METRICS) → timeseries response
      - type "table"     (or metric in TABLE_METRICS)      → table response
    """
    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON body"}), 400

    warehouse = _get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not initialized"}), 503

    from service.services.alb_query import (
        ALL_METRICS, TABLE_METRICS, TIMESERIES_METRICS,
        query_metric_timeseries,
        query_top_paths, query_top_ips,
        query_status_distribution, query_latency_stats,
    )

    # --- time range ---
    now = datetime.now(tz=timezone.utc)
    range_ = body.get("range", {})
    from_dt = _parse_datetime(range_.get("from"), now - timedelta(hours=24))
    to_dt   = _parse_datetime(range_.get("to"),   now)

    range_ms = max(int((to_dt - from_dt).total_seconds() * 1000), 1)
    max_pts  = max(int(body.get("maxDataPoints", 100)), 1)
    interval_ms = max(int(body.get("intervalMs", 0)) or range_ms // max_pts, 60_000)

    tname = _table_name()
    results = []

    for target in body.get("targets", []):
        metric = target.get("target", "")
        ttype  = target.get("type", "timeserie")

        if not metric or metric not in ALL_METRICS:
            continue

        # Force table type for known table metrics
        is_table = (ttype == "table") or (metric in TABLE_METRICS)

        if is_table:
            if metric == "top_paths":
                data = query_top_paths(warehouse, tname, from_dt, to_dt)
            elif metric == "top_ips":
                data = query_top_ips(warehouse, tname, from_dt, to_dt)
            elif metric == "status_distribution":
                data = query_status_distribution(warehouse, tname, from_dt, to_dt)
            elif metric == "latency_stats":
                data = query_latency_stats(warehouse, tname, from_dt, to_dt)
            else:
                continue
            results.append(data)
        else:
            datapoints = query_metric_timeseries(
                warehouse, tname, metric, from_dt, to_dt, interval_ms
            )
            results.append({"target": metric, "datapoints": datapoints})

    return jsonify(results)


@alb_bp.route("/grafana/annotations", methods=["POST"])
def grafana_annotations():
    return jsonify([])


@alb_bp.route("/grafana/tag-keys", methods=["POST"])
def grafana_tag_keys():
    return jsonify([
        {"type": "string", "text": "elb"},
        {"type": "string", "text": "request_method"},
        {"type": "string", "text": "domain_name"},
    ])


@alb_bp.route("/grafana/tag-values", methods=["POST"])
def grafana_tag_values():
    # Stub — could be extended to query distinct values from the table
    return jsonify([])


# ---------------------------------------------------------------------------
# Simulate SQS — load a local file from samples/ without needing real SQS/S3
# ---------------------------------------------------------------------------

def _resolve_samples_dir() -> "Path":
    """Return the absolute path of the samples directory."""
    from pathlib import Path
    cfg = _get_config()
    samples_dir = cfg.get("alb", {}).get("samples_dir", "samples")
    base = Path(cfg.get("_project_root", "")) or Path(__file__).parent.parent.parent.parent
    resolved = (base / samples_dir).resolve()
    return resolved


@alb_bp.route("/simulate-sqs", methods=["POST"])
def simulate_sqs():
    """
    Simulate an SQS S3-Event notification using a local file from samples/.

    Accepted body formats
    ---------------------
    Option A — filename only:
        { "filename": "695414238084_...log.gz" }

    Option B — full SQS/S3-Event JSON with local flag:
        { "sqs_message": { "Records": [{...}] }, "local": true }
        The key's basename is matched against files in samples/.

    Response is the same JSON structure as POST /alb/s3-event.
    """
    import os
    from pathlib import Path

    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON body"}), 400

    samples_dir = _resolve_samples_dir()

    # --- resolve filename ---
    filename: Optional[str] = None

    if "filename" in body:
        filename = os.path.basename(str(body["filename"]))

    elif body.get("local") and "sqs_message" in body:
        records = body["sqs_message"].get("Records", [])
        if not records:
            return jsonify({"error": "sqs_message has no Records"}), 400
        key = records[0].get("s3", {}).get("object", {}).get("key", "")
        filename = os.path.basename(key)

    else:
        return jsonify({
            "error": "provide 'filename' or 'sqs_message' + 'local': true"
        }), 400

    if not filename:
        return jsonify({"error": "could not determine filename"}), 400

    # Security: reject any path traversal attempt
    if "/" in filename or "\\" in filename or filename.startswith("."):
        return jsonify({"error": "invalid filename"}), 400

    file_path = (samples_dir / filename).resolve()

    # Ensure the resolved path is still inside samples_dir
    try:
        file_path.relative_to(samples_dir)
    except ValueError:
        return jsonify({"error": "filename escapes samples directory"}), 400

    if not file_path.exists():
        available = [f.name for f in samples_dir.iterdir() if f.is_file()]
        return jsonify({
            "error": f"file not found: {filename}",
            "available": available,
        }), 404

    processor, err = _get_processor()
    if err:
        return jsonify({"error": err}), 503

    result = processor.process_local_file(str(file_path))
    result["file"] = filename
    status_code = 200 if result.get("status") == "ok" else 500
    return jsonify(result), status_code
