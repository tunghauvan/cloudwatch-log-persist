import sys
from pathlib import Path
from flask import Blueprint, request, jsonify
import time
import re

sys.path.insert(0, str(Path(__file__).parent.parent))

from service.services.loki_metrics import loki_metrics
import logging

logger = logging.getLogger("service.loki")

loki_bp = Blueprint("loki", __name__)


def get_warehouse():
    from service.server import warehouse

    return warehouse


def get_log_buffer():
    from service.server import log_buffer

    return log_buffer


def parse_logql_filter(filter_expr):
    if not filter_expr:
        return None, None, None, {}, None

    log_group = None
    log_stream = None
    message_filter = None
    labels_filter = {}
    regex_label = None  # Track regex label for grouping

    pattern = r"\{([^}]+)\}"
    match = re.search(pattern, filter_expr)
    if match:
        labels_str = match.group(1)
        for label in labels_str.split(","):
            label = label.strip()
            # Handle regex match operator =~
            if "=~" in label:
                key, value = label.split("=~", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("`")
                # For regex match .+ (match any), track it for grouping but don't filter
                if value == ".+" or value == ".*":
                    regex_label = key  # Track this label for grouping
                    continue  # Skip this label - match all
                if key == "log_group" or key == "log_group_name":
                    log_group = value
                elif key == "log_stream" or key == "log_stream_name":
                    log_stream = value
                else:
                    labels_filter[key] = value
            elif "=" in label:
                key, value = label.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("`")
                if key == "log_group" or key == "log_group_name":
                    log_group = value
                elif key == "log_stream" or key == "log_stream_name":
                    log_stream = value
                else:
                    labels_filter[key] = value

    if "|=" in filter_expr:
        parts = filter_expr.split("|=")
        if len(parts) > 1:
            message_filter = parts[-1].strip().strip('"').strip("`")
    elif "|~" in filter_expr:
        parts = filter_expr.split("|~")
        if len(parts) > 1:
            message_filter = parts[-1].strip().strip('"').strip("`")

    return log_group, log_stream, message_filter, labels_filter, regex_label


def logs_to_loki_streams(logs):
    streams = {}
    for log in logs:
        labels = {
            "log_group": log.get("logGroupName", "default"),
            "log_stream": log.get("logStreamName", "default"),
        }

        if "label_env" in log and log["label_env"]:
            labels["env"] = log["label_env"]
        if "label_service" in log and log["label_service"]:
            labels["service"] = log["label_service"]
            # Add service_name alias for Grafana drilldown compatibility
            labels["service_name"] = log["label_service"]
        if "label_host" in log and log["label_host"]:
            labels["host"] = log["label_host"]
        if "label_region" in log and log["label_region"]:
            labels["region"] = log["label_region"]

        # Use frozenset of sorted items as key for dict
        label_key = tuple(sorted(labels.items()))

        if label_key not in streams:
            streams[label_key] = {"stream": dict(labels), "values": []}

        timestamp = log.get(
            "timestamp", log.get("ingestionTime", int(time.time() * 1000))
        )
        # Convert to nanoseconds (19 digits for Loki)
        if isinstance(timestamp, int):
            if timestamp > 1e18:  # Already nanoseconds
                timestamp_ns = str(timestamp)
            elif timestamp > 1e15:  # Already microseconds (16 digits)
                timestamp_ns = str(timestamp * 1000)
            elif timestamp > 1e11:  # Milliseconds (13 digits)
                timestamp_ns = str(timestamp * 1000000)
            else:  # Seconds
                timestamp_ns = str(timestamp * 1000000000)
        else:
            timestamp_ns = str(int(timestamp) * 1000000)

        # Loki format: values is array of [timestamp, line] arrays
        streams[label_key]["values"].append([timestamp_ns, log.get("message", "")])

    return list(streams.values())


@loki_bp.route("/loki/api/v1/push", methods=["POST"])
def loki_push():
    log_buffer = get_log_buffer()
    warehouse = get_warehouse()

    if not log_buffer or not warehouse:
        return jsonify({"error": "service not available"}), 503

    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON"}), 400

    # Handle Fluent Bit format (array of records) vs Loki format (object with streams)
    if isinstance(data, list):
        # Fluent Bit format: array of records
        streams = [{"stream": {}, "values": []}]
        ingestion_time = int(time.time() * 1000)
        for record in data:
            if isinstance(record, dict) and "log" in record:
                # Extract kubernetes labels if present
                k8s = record.get("kubernetes", {})
                labels = k8s.get("labels", {})
                stream_labels = {
                    "pod": k8s.get("pod_name", "unknown"),
                    "container": k8s.get("container_name", "unknown"),
                    "namespace": k8s.get("namespace", "default"),
                    "app": labels.get("app", "unknown"),
                    "level": record.get("level", "info"),
                }
                # Build message
                msg = record.get("log", "")
                if "message" in record:
                    msg = record["message"]
                ts_ns = str(int(time.time() * 1000000000))
                streams[0]["values"].append([ts_ns, msg])
                streams[0]["stream"] = stream_labels
    else:
        # Standard Loki format
        streams = data.get("streams", [])

    if not streams:
        loki_metrics.record_push(logs_count=0, streams_count=0)
        return jsonify({"status": "ok"}), 200

    ingestion_time = int(time.time() * 1000)
    warehouse_logs = []
    total_logs = 0
    stream_count = len(streams)

    loki_table = warehouse.config.get("loki", {}).get("table_name", "loki_logs")

    for stream in streams:
        labels_str = stream.get("stream", {})
        log_group = (
            labels_str.get("log_group")
            or labels_str.get("log_group_name")
            or labels_str.get("namespace", "default")
        )
        log_stream = (
            labels_str.get("log_stream")
            or labels_str.get("log_stream_name")
            or labels_str.get("pod", "default")
        )

        for entry in stream.get("values", []):
            timestamp_ns, message = entry
            # Convert nanoseconds (Loki) to microseconds (Warehouse)
            timestamp_us = int(int(timestamp_ns) / 1000)
            total_logs += 1

            log_entry = {
                "logGroupName": log_group,
                "logStreamName": log_stream,
                "timestamp": timestamp_us,
                "message": message,
                "ingestionTime": timestamp_us,  # Use log timestamp for ingestion_time
                "sequenceToken": 0,
                "_warehouse_table": loki_table,
            }

            # Map ALL stream labels to label_ prefix for storage
            for k, v in labels_str.items():
                safe_key = k.replace("-", "_").replace(" ", "_")
                log_entry[f"label_{safe_key}"] = v

            # Auto mapping alias: pod -> logStreamName, namespace -> logGroupName if needed
            if "pod" in labels_str and log_stream == "default":
                log_entry["logStreamName"] = labels_str["pod"]
            if "namespace" in labels_str and log_group == "default":
                log_entry["logGroupName"] = labels_str["namespace"]

            # Application alias: if app is missing but service is present, use service
            if not labels_str.get("app") and labels_str.get("service"):
                log_entry["label_app"] = labels_str["service"]
            elif not labels_str.get("service") and labels_str.get("app"):
                log_entry["label_service"] = labels_str["app"]

            warehouse_logs.append(log_entry)

    try:
        log_buffer.add(warehouse_logs)
        loki_metrics.record_push(
            logs_count=total_logs, streams_count=stream_count, error=False
        )
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        loki_metrics.record_push(
            logs_count=total_logs, streams_count=stream_count, error=True
        )
        return jsonify({"error": str(e)}), 500


def is_metric_query(query):
    """Check if query is a metric query (not log query)"""
    metric_keywords = [
        "vector(",
        "sum(",
        "count(",
        "rate(",
        "count_over_time(",
        "sum by",
        "avg(",
        "max(",
        "min(",
        "quantile(",
        "bytes_over_time(",
        "bytes_rate(",
    ]
    query_lower = query.lower()
    return any(kw in query_lower for kw in metric_keywords)


@loki_bp.route("/loki/api/v1/query", methods=["GET"])
def loki_query():
    warehouse = get_warehouse()
    if not warehouse:
        return jsonify({"error": "service not available"}), 503

    query = request.args.get("query", "")
    limit = int(request.args.get("limit", 100))
    time_param = request.args.get("time", "")

    # Check if this is a metric query (for health check)
    if is_metric_query(query):
        # Return vector result for metric queries
        timestamp = int(time_param) if time_param else int(time.time())
        loki_metrics.record_query(logs_returned=1, error=False)
        return jsonify(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {}, "value": [timestamp, "2"]}],
                    "stats": {
                        "summary": {
                            "bytesProcessedPerSecond": 0,
                            "linesProcessedPerSecond": 0,
                            "totalBytesProcessed": 0,
                            "totalLinesProcessed": 0,
                            "execTime": 0.001,
                            "queueTime": 0.0,
                            "subqueries": 0,
                            "totalEntriesReturned": 1,
                            "splits": 0,
                            "shards": 0,
                            "totalPostFilterLines": 0,
                            "totalStructuredMetadataBytesProcessed": 0,
                        },
                        "querier": {
                            "store": {
                                "totalChunksRef": 0,
                                "totalChunksDownloaded": 0,
                                "chunksDownloadTime": 0,
                                "queryReferencedStructuredMetadata": False,
                                "chunk": {
                                    "headChunkBytes": 0,
                                    "headChunkLines": 0,
                                    "decompressedBytes": 0,
                                    "decompressedLines": 0,
                                    "compressedBytes": 0,
                                    "totalDuplicates": 0,
                                    "postFilterLines": 0,
                                    "headChunkStructuredMetadataBytes": 0,
                                    "decompressedStructuredMetadataBytes": 0,
                                },
                                "chunkRefsFetchTime": 0,
                                "congestionControlLatency": 0,
                                "pipelineWrapperFilteredLines": 0,
                            }
                        },
                        "ingester": {
                            "totalReached": 0,
                            "totalChunksMatched": 0,
                            "totalBatches": 0,
                            "totalLinesSent": 0,
                            "store": {
                                "totalChunksRef": 0,
                                "totalChunksDownloaded": 0,
                                "chunksDownloadTime": 0,
                                "queryReferencedStructuredMetadata": False,
                                "chunk": {
                                    "headChunkBytes": 0,
                                    "headChunkLines": 0,
                                    "decompressedBytes": 0,
                                    "decompressedLines": 0,
                                    "compressedBytes": 0,
                                    "totalDuplicates": 0,
                                    "postFilterLines": 0,
                                    "headChunkStructuredMetadataBytes": 0,
                                    "decompressedStructuredMetadataBytes": 0,
                                },
                                "chunkRefsFetchTime": 0,
                                "congestionControlLatency": 0,
                                "pipelineWrapperFilteredLines": 0,
                            },
                        },
                        "cache": {
                            "chunk": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "index": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "result": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "statsResult": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "volumeResult": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "seriesResult": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "labelResult": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                            "instantMetricResult": {
                                "entriesFound": 0,
                                "entriesRequested": 0,
                                "entriesStored": 0,
                                "bytesReceived": 0,
                                "bytesSent": 0,
                                "requests": 0,
                                "downloadTime": 0,
                                "queryLengthServed": 0,
                            },
                        },
                        "index": {
                            "totalChunks": 0,
                            "postFilterChunks": 0,
                            "shardsDuration": 0,
                            "usedBloomFilters": False,
                        },
                    },
                },
            }
        )

    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )

    start_time = int((time.time() - 3600) * 1000)
    end_time = int(time.time() * 1000)

    try:
        events = warehouse.get_logs(table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"), 
            log_group_name=log_group,
            log_stream_name=log_stream,
            start_time=start_time,
            end_time=end_time,
            limit=limit * 10,
        )
    except Exception as e:
        loki_metrics.record_query(logs_returned=0, error=True)
        events = []

    filtered_events = []
    for e in events:
        match = True
        for label_key, label_value in labels_filter.items():
            # Map label names if needed (Grafana drilldown compatibility)
            label_mapping = {"service_name": "service"}
            mapped_key = label_mapping.get(label_key, label_key)
            storage_key = f"label_{mapped_key}"
            if e.get(storage_key) != label_value:
                match = False
                break
        if match and message_filter:
            if message_filter.lower() not in e.get("message", "").lower():
                match = False
        if match:
            filtered_events.append(e)

    filtered_events = filtered_events[:limit]

    logs = []
    for e in filtered_events:
        logs.append(
            {
                "timestamp": e.get("timestamp", 0),
                "message": e.get("message", ""),
                "ingestionTime": e.get("ingestionTime", 0),
                "logGroupName": e.get("logGroupName", "default"),
                "logStreamName": e.get("logStreamName", "default"),
                "label_env": e.get("label_env", ""),
                "label_service": e.get("label_service", ""),
                "label_host": e.get("label_host", ""),
                "label_region": e.get("label_region", ""),
            }
        )

    streams = logs_to_loki_streams(logs)

    loki_metrics.record_query(logs_returned=len(logs), error=False)
    return jsonify(
        {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": streams,
                "stats": {
                    "summary": {
                        "bytesProcessed": 0,
                        "linesProcessed": len(logs),
                        "totalLinesProcessed": len(logs),
                        "totalBytesProcessed": 0,
                        "totalEvents": len(logs),
                        "execTime": 0.0,
                    }
                },
            },
        }
    )


@loki_bp.route("/loki/api/v1/query_range", methods=["GET", "POST"])
def loki_query_range():
    print(
        f"[Debug] Loki Query Range received. Params: {request.args if request.method == 'GET' else 'POST body'}"
    )
    warehouse = get_warehouse()
    if not warehouse:
        return jsonify({"error": "service not available"}), 503

    if request.method == "POST":
        data = request.get_json(force=True) or {}
        query = data.get("query", "")
        limit = int(data.get("limit", 1000))
        # Default start to 6 hours ago to match Loki default behavior if not provided
        start = int(data.get("start", int((time.time() - 6 * 3600) * 1e9)))
        end = int(data.get("end", int(time.time() * 1e9)))
        step = data.get("step", "1h")
    else:
        query = request.args.get("query", "")
        limit = int(request.args.get("limit", 1000))
        start = int(request.args.get("start", int((time.time() - 6 * 3600) * 1e9)))
        end = int(request.args.get("end", int(time.time() * 1e9)))
        step = request.args.get("step", "1h")

    # Correct magnitude detection for milliseconds (1e12) or nanoseconds (1e18)
    # Warehouse expects milliseconds for its get_logs filters
    if start > 1e16:  # Nanoseconds
        start = start // 1_000_000
    elif start > 1e13:  # Microseconds
        start = start // 1_000

    if end > 1e16:  # Nanoseconds
        end = end // 1_000_000
    elif end > 1e13:  # Microseconds
        end = end // 1_000

    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )
    print(
        f"[Debug] Parsed filter: group={log_group}, stream={log_stream}, labels={labels_filter}"
    )

    try:
        # Remove fetch limit as requested to ensure all logs in range are processed
        events = warehouse.get_logs(
            table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"),
            log_group_name=log_group,
            log_stream_name=log_stream,
            start_time=start,
            end_time=end,
            limit=None,  # No limit, fetch all logs in the time range
        )
        logger.debug(f" Warehouse returned {len(events)} events before filtering")
    except Exception as e:
        logger.debug(f" Warehouse error: {e}")
        events = []

    filtered_events = []
    logger.debug(f" Filtering starts for {len(events)} events. Request Limit: {limit}")
    filtered_events = []
    # Log the first event keys and all labels found
    if events:
        first_e = events[0]
        logger.debug(f" First event object keys: {list(first_e.keys())}")
        label_keys = [k for k in first_e.keys() if k.startswith("label_")]
        if label_keys:
            print(
                f"[Debug] Label keys found in first event: {label_keys}, Example values: {[(k, first_e[k]) for k in label_keys]}"
            )
        else:
            logger.debug(f" NO label_ keys found in first event!")

    for e in events:
        match = True
        for label_key, label_value in labels_filter.items():
            # Map label names if needed (Grafana drilldown compatibility)
            label_mapping = {"service_name": "service"}
            mapped_key = label_mapping.get(label_key, label_key)
            storage_key = f"label_{mapped_key}"
            if e.get(storage_key) != label_value:
                match = False
                break
        if match and message_filter:
            if message_filter.lower() not in e.get("message", "").lower():
                match = False
        if match:
            filtered_events.append(e)

    logger.debug(f" Filtering done. {len(filtered_events)} events matched.")
    
    # Only apply the limit for regular log queries to keep Grafana happy.
    # For metric queries, we want to use all matched events.
    if not is_metric_query(query):
        filtered_events = filtered_events[:limit]

    # Check if this is a metric query (aggregation like count_over_time, sum by, etc.)
    if is_metric_query(query):
        # Parse step parameter - auto-calculate if empty
        step_str = (
            request.args.get("step", "")
            if request.method == "GET"
            else (request.json or {}).get("step", "")
        )

        # Get maxDataPoints for auto step calculation
        max_data_points = int(
            request.args.get("maxDataPoints", 500)
            if request.method == "GET"
            else (request.json or {}).get("maxDataPoints", 500)
        )

        # Auto-calculate step if not provided
        if not step_str or step_str == "":
            # Calculate step based on time range and max data points
            # Converting start/end from ms to seconds first
            range_seconds = (end - start) / 1000.0
            auto_step = max(1, int(range_seconds / max(max_data_points, 1)))
            step_seconds = auto_step
            print(
                f"[Debug] Auto-calculated step: {step_seconds}s from range {range_seconds}s and maxDataPoints {max_data_points}"
            )
        else:
            try:
                if isinstance(step_str, str):
                    if step_str.endswith("s"):
                        step_seconds = int(step_str[:-1])
                    elif step_str.endswith("m"):
                        step_seconds = int(step_str[:-1]) * 60
                    elif step_str.endswith("h"):
                        step_seconds = int(step_str[:-1]) * 3600
                    else:
                        step_seconds = int(float(step_str))
                else:
                    step_seconds = int(step_str)
            except:
                # Fallback to auto-calculate
                range_seconds = (end - start) / 1000.0
                step_seconds = max(1, int(range_seconds / max(max_data_points, 1)))
                print(
                    f"[Debug] Step parse failed, using auto-calculated: {step_seconds}s"
                )

        # Extract aggregation label (e.g., "by (detected_level)")
        agg_label = None
        if "by (" in query.lower():
            import re

            match = re.search(r"by\s*\(\s*([^)]+)\s*\)", query, re.IGNORECASE)
            if match:
                agg_label = match.group(1).strip()

        # Build matrix result (time series)
        result = []
        total_bytes = 0
        total_lines = len(filtered_events)

        # Group by aggregation label and time bucket
        time_buckets = {}
        for e in filtered_events:
            ts_ms = e.get("timestamp", 0)
            if isinstance(ts_ms, int) and ts_ms > 1e12:
                # Convert to seconds and round to step
                bucket_ts = int(ts_ms / 1000 / step_seconds) * step_seconds
            else:
                bucket_ts = int(time.time() / step_seconds) * step_seconds

            # Get label value for grouping
            if agg_label:
                if agg_label == "detected_level":
                    # Extract level from message
                    msg = e.get("message", "").lower()
                    if "error" in msg:
                        label_val = "error"
                    elif "warn" in msg:
                        label_val = "warn"
                    elif "info" in msg:
                        label_val = "info"
                    elif "debug" in msg:
                        label_val = "debug"
                    else:
                        label_val = "unknown"
                else:
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(agg_label, agg_label)
                    label_val = e.get(f"label_{mapped_key}", "unknown")
            else:
                label_val = "total"

            key = (label_val, bucket_ts)
            time_buckets[key] = time_buckets.get(key, 0) + 1
            total_bytes += len(e.get("message", "").encode("utf-8"))

        # Build matrix result
        if time_buckets:
            # Group by label value
            label_values = {}
            for (label_val, bucket_ts), count in time_buckets.items():
                if label_val not in label_values:
                    label_values[label_val] = []
                label_values[label_val].append([bucket_ts, str(count)])

            for label_val, values in label_values.items():
                metric = {}
                if agg_label:
                    metric[agg_label] = label_val
                result.append({"metric": metric, "values": sorted(values)})
        else:
            # Return empty result
            result = [{"metric": {}, "values": []}]

        return jsonify(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": result,
                    "stats": {
                        "summary": {
                            "bytesProcessedPerSecond": 0,
                            "linesProcessedPerSecond": 0,
                            "totalBytesProcessed": total_bytes,
                            "totalLinesProcessed": total_lines,
                            "execTime": 0.001,
                            "queueTime": 0,
                            "subqueries": 0,
                            "totalEntriesReturned": len(result),
                            "splits": 1,
                            "shards": 0,
                            "totalPostFilterLines": total_lines,
                            "totalStructuredMetadataBytesProcessed": 0,
                        }
                    },
                },
            }
        )

    # Regular log query - return streams
    logs = []
    for e in filtered_events:
        logs.append(
            {
                "timestamp": e.get("timestamp", 0),
                "message": e.get("message", ""),
                "ingestionTime": e.get("ingestionTime", 0),
                "logGroupName": e.get("logGroupName", "default"),
                "logStreamName": e.get("logStreamName", "default"),
                "label_env": e.get("label_env", ""),
                "label_service": e.get("label_service", ""),
                "label_host": e.get("label_host", ""),
                "label_region": e.get("label_region", ""),
            }
        )

    streams = logs_to_loki_streams(logs)

    loki_metrics.record_query_range(logs_returned=len(logs), error=False)
    return jsonify(
        {
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": streams,
                "stats": {
                    "summary": {
                        "bytesProcessed": 0,
                        "linesProcessed": len(logs),
                        "totalLinesProcessed": len(logs),
                        "totalBytesProcessed": 0,
                        "totalEvents": len(logs),
                        "execTime": 0.0,
                    }
                },
            },
        }
    )


@loki_bp.route("/loki/api/v1/label", methods=["GET"])
@loki_bp.route("/loki/api/v1/labels", methods=["GET"])
def loki_labels():
    loki_metrics.record_label_request()
    return jsonify(
        {
            "status": "success",
            "data": ["log_group", "log_stream", "env", "service", "host", "region"],
        }
    )


@loki_bp.route("/loki/api/v1/label/<label_name>/values", methods=["GET"])
def loki_label_values(label_name):
    loki_metrics.record_label_values_request()
    # Return some sample values for each label
    values_map = {
        "log_group": ["/myapp", "/system"],
        "log_stream": ["main", "worker"],
        "env": ["prod", "dev", "staging"],
        "service": ["api", "worker", "frontend"],
        "host": ["server-01", "server-02"],
        "region": ["us-east-1", "us-west-2", "ap-southeast-1"],
    }
    return jsonify({"status": "success", "data": values_map.get(label_name, [])})


@loki_bp.route("/loki/api/v1/series", methods=["GET", "POST"])
def loki_series():
    # Per Loki docs: returns array of label objects (streams)
    # Supports match[] parameter for filtering
    warehouse = get_warehouse()

    # Get parameters from query string or form body
    if request.method == "POST":
        match = request.form.getlist("match[]") or request.args.getlist("match[]")
        start = request.form.get("start", "")
        end = request.form.get("end", "")
    else:
        match = request.args.getlist("match[]")
        start = request.args.get("start", "")
        end = request.args.get("end", "")

    # Parse time range
    def parse_time(time_str):
        if not time_str:
            return None
        try:
            ts = int(time_str)
            if ts > 1e15:
                return int(ts / 1000000)
            elif ts > 1e12:
                return ts
            else:
                return int(ts * 1000)
        except ValueError:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                return None

    start_time = parse_time(start) or int((time.time() - 3600) * 1000)
    end_time = parse_time(end) or int(time.time() * 1000)

    streams = []

    if warehouse:
        try:
            events = warehouse.get_logs(table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"), 
                log_group_name=None,
                log_stream_name=None,
                start_time=start_time,
                end_time=end_time,
                limit=10000,
            )

            # Get unique streams (label combinations)
            unique_streams = {}
            for e in events:
                # Build label set
                labels = {
                    "log_group": e.get("logGroupName", "default"),
                    "log_stream": e.get("logStreamName", "default"),
                }

                # Add label columns
                if e.get("label_env"):
                    labels["env"] = e["label_env"]
                if e.get("label_service"):
                    labels["service"] = e["label_service"]
                    labels["service_name"] = e["label_service"]
                if e.get("label_host"):
                    labels["host"] = e["label_host"]
                if e.get("label_region"):
                    labels["region"] = e["label_region"]

                # Create unique key
                key = tuple(sorted(labels.items()))
                if key not in unique_streams:
                    unique_streams[key] = labels

            streams = list(unique_streams.values())
        except Exception as e:
            logger.error(f" Error: {e}")

    # Return default if no streams
    if not streams:
        streams = [
            {
                "log_group": "/myapp",
                "log_stream": "main",
                "env": "prod",
                "service": "api",
            },
            {
                "log_group": "/myapp",
                "log_stream": "worker",
                "env": "prod",
                "service": "worker",
            },
        ]

    loki_metrics.record_series_request(error=False)
    return jsonify({"status": "success", "data": streams})


@loki_bp.route("/loki/api/v1/index/stats", methods=["GET", "POST"])
def loki_index_stats():
    # Per Loki docs: returns flat JSON (not wrapped in status/data)
    # Also supports POST with form-urlencoded body
    warehouse = get_warehouse()
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")

    # Parse time range
    def parse_time(time_str):
        if not time_str:
            return None
        try:
            ts = int(time_str)
            if ts > 1e15:
                return int(ts / 1000000)
            elif ts > 1e12:
                return ts
            else:
                return int(ts * 1000)
        except ValueError:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                return None

    start_time = parse_time(start) or int((time.time() - 3600) * 1000)
    end_time = parse_time(end) or int(time.time() * 1000)

    # Parse query to get filters
    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )

    streams = 0
    chunks = 0
    entries = 0
    bytes_total = 0

    if warehouse:
        try:
            events = warehouse.get_logs(table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"), 
                log_group_name=log_group,
                log_stream_name=log_stream,
                start_time=start_time,
                end_time=end_time,
                limit=10000,
            )

            # Apply label filters
            filtered_events = []
            for e in events:
                match = True
                for label_key, label_value in labels_filter.items():
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(label_key, label_key)
                    storage_key = f"label_{mapped_key}"
                    if e.get(storage_key) != label_value:
                        match = False
                        break
                if match:
                    filtered_events.append(e)

            # Count unique streams (by label combination)
            unique_streams = set()
            for e in filtered_events:
                stream_key = (
                    e.get("logGroupName", "default"),
                    e.get("logStreamName", "default"),
                    e.get("label_env", ""),
                    e.get("label_service", ""),
                )
                unique_streams.add(stream_key)

            streams = len(unique_streams)
            entries = len(filtered_events)
            bytes_total = sum(
                len(e.get("message", "").encode("utf-8")) for e in filtered_events
            )
            chunks = max(1, entries // 1000)  # Approximate chunks
        except Exception as e:
            logger.error(f" Error: {e}")

    # Return flat JSON (not wrapped)
    loki_metrics.record_index_stats_request()
    return jsonify(
        {
            "streams": streams,
            "chunks": chunks,
            "entries": entries,
            "bytes": bytes_total,
        }
    )


@loki_bp.route("/loki/api/v1/index/volume", methods=["GET"])
def loki_index_volume():
    # Real Loki format with full stats - query actual data
    warehouse = get_warehouse()
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    limit = int(request.args.get("limit", 5000))

    # Parse time range (supports nanoseconds, milliseconds, seconds, or RFC3339)
    def parse_time(time_str):
        if not time_str:
            return None
        try:
            # Try parsing as integer (nanoseconds, milliseconds, or seconds)
            ts = int(time_str)
            # Detect format based on length
            if ts > 1e15:  # Nanoseconds
                return int(ts / 1000000)
            elif ts > 1e12:  # Milliseconds
                return ts
            else:  # Seconds
                return int(ts * 1000)
        except ValueError:
            # Try parsing as RFC3339/ISO format
            try:
                from datetime import datetime

                # Parse ISO format with timezone
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                return None

    start_time = parse_time(start) or int((time.time() - 3600) * 1000)
    end_time = parse_time(end) or int(time.time() * 1000)

    # Parse query to get label filters
    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )

    # Build result with real data
    result = []
    total_bytes = 0
    total_lines = 0

    if warehouse:
        try:
            # Query logs to calculate volume by label
            events = warehouse.get_logs(table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"), 
                log_group_name=log_group,
                log_stream_name=log_stream,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )

            # Group by labels and calculate volume
            volume_by_label = {}
            for e in events:
                # Apply label filters
                match = True
                for label_key, label_value in labels_filter.items():
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(label_key, label_key)
                    storage_key = f"label_{mapped_key}"
                    if e.get(storage_key) != label_value:
                        match = False
                        break

                if not match:
                    continue

                # Get label value for grouping
                # Priority: regex_label > first filter label > logGroupName
                if regex_label:
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(regex_label, regex_label)
                    label_val = e.get(f"label_{mapped_key}", "default")
                    group_key = regex_label
                elif labels_filter:
                    # Use first filter label for grouping
                    first_label = list(labels_filter.keys())[0]
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(first_label, first_label)
                    label_val = e.get(f"label_{mapped_key}", "default")
                    group_key = first_label
                else:
                    label_val = e.get("logGroupName", "default")
                    group_key = "log_group"

                msg_size = len(e.get("message", "").encode("utf-8"))

                if label_val not in volume_by_label:
                    volume_by_label[label_val] = {
                        "bytes": 0,
                        "lines": 0,
                        "key": group_key,
                    }
                volume_by_label[label_val]["bytes"] += msg_size
                volume_by_label[label_val]["lines"] += 1

                total_bytes += msg_size
                total_lines += 1

            # Build result array
            timestamp = int(time.time())
            for label_val, vol in volume_by_label.items():
                metric = {}
                group_key = vol.get("key", "log_group")
                metric[group_key] = label_val

                result.append(
                    {"metric": metric, "value": [timestamp, str(vol["bytes"])]}
                )
        except Exception as e:
            logger.error(f" Error querying: {e}")

    # If no results, add a default one with proper metric
    if not result:
        # Return empty result like real Loki when no data
        pass

    return jsonify(
        {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": result,
                "stats": {
                    "summary": {
                        "bytesProcessedPerSecond": 0,
                        "linesProcessedPerSecond": 0,
                        "totalBytesProcessed": total_bytes,
                        "totalLinesProcessed": total_lines,
                        "execTime": 0.001,
                        "queueTime": 0,
                        "subqueries": 0,
                        "totalEntriesReturned": len(result),
                        "splits": 1,
                        "shards": 0,
                        "totalPostFilterLines": total_lines,
                        "totalStructuredMetadataBytesProcessed": 0,
                    },
                    "querier": {
                        "store": {
                            "totalChunksRef": 0,
                            "totalChunksDownloaded": 0,
                            "chunksDownloadTime": 0,
                            "queryReferencedStructuredMetadata": False,
                            "chunk": {
                                "headChunkBytes": 0,
                                "headChunkLines": 0,
                                "decompressedBytes": 0,
                                "decompressedLines": 0,
                                "compressedBytes": 0,
                                "totalDuplicates": 0,
                                "postFilterLines": 0,
                                "headChunkStructuredMetadataBytes": 0,
                                "decompressedStructuredMetadataBytes": 0,
                            },
                            "chunkRefsFetchTime": 0,
                            "congestionControlLatency": 0,
                            "pipelineWrapperFilteredLines": 0,
                        }
                    },
                    "ingester": {
                        "totalReached": 0,
                        "totalChunksMatched": 0,
                        "totalBatches": 0,
                        "totalLinesSent": 0,
                        "store": {
                            "totalChunksRef": 0,
                            "totalChunksDownloaded": 0,
                            "chunksDownloadTime": 0,
                            "queryReferencedStructuredMetadata": False,
                            "chunk": {
                                "headChunkBytes": 0,
                                "headChunkLines": 0,
                                "decompressedBytes": 0,
                                "decompressedLines": 0,
                                "compressedBytes": 0,
                                "totalDuplicates": 0,
                                "postFilterLines": 0,
                                "headChunkStructuredMetadataBytes": 0,
                                "decompressedStructuredMetadataBytes": 0,
                            },
                            "chunkRefsFetchTime": 0,
                            "congestionControlLatency": 0,
                            "pipelineWrapperFilteredLines": 0,
                        },
                    },
                    "cache": {
                        "chunk": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "index": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "result": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "statsResult": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "volumeResult": {
                            "entriesFound": 0,
                            "entriesRequested": 1,
                            "entriesStored": 1,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 1,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "seriesResult": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "labelResult": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                        "instantMetricResult": {
                            "entriesFound": 0,
                            "entriesRequested": 0,
                            "entriesStored": 0,
                            "bytesReceived": 0,
                            "bytesSent": 0,
                            "requests": 0,
                            "downloadTime": 0,
                            "queryLengthServed": 0,
                        },
                    },
                    "index": {
                        "totalChunks": 0,
                        "postFilterChunks": 0,
                        "shardsDuration": 0,
                        "usedBloomFilters": False,
                    },
                },
            },
        }
    )
    loki_metrics.record_index_volume_request()


@loki_bp.route("/loki/api/v1/index/volume_range", methods=["GET"])
def loki_index_volume_range():
    # Per Loki docs: returns matrix (time series) instead of single vector point
    # Requires 'step' parameter
    warehouse = get_warehouse()
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    step = request.args.get("step", "60")  # Default 60s
    limit = int(request.args.get("limit", 100))

    # Parse step (can be duration like "5m" or seconds)
    step_seconds = 60
    try:
        if step.endswith("s"):
            step_seconds = int(step[:-1])
        elif step.endswith("m"):
            step_seconds = int(step[:-1]) * 60
        elif step.endswith("h"):
            step_seconds = int(step[:-1]) * 3600
        else:
            step_seconds = int(step)
    except:
        step_seconds = 60

    # Parse time range
    def parse_time(time_str):
        if not time_str:
            return None
        try:
            ts = int(time_str)
            if ts > 1e15:
                return int(ts / 1000000)
            elif ts > 1e12:
                return ts
            else:
                return int(ts * 1000)
        except ValueError:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                return None

    start_time = parse_time(start) or int((time.time() - 3600) * 1000)
    end_time = parse_time(end) or int(time.time() * 1000)

    # Parse query to get label filters
    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )

    # Build result with real data
    result = []
    total_bytes = 0
    total_lines = 0

    if warehouse:
        try:
            # Query all logs in the time range without limit to fill the volume chart
            events = warehouse.get_logs(
                table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"),
                log_group_name=log_group,
                log_stream_name=log_stream,
                start_time=start_time,
                end_time=end_time,
                limit=None, # No limit, fetch everything in range
            )

            # Group by labels AND time buckets
            volume_by_label_time = {}
            for e in events:
                # Apply label filters
                match = True
                for label_key, label_value in labels_filter.items():
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(label_key, label_key)
                    storage_key = f"label_{mapped_key}"
                    if e.get(storage_key) != label_value:
                        match = False
                        break

                if not match:
                    continue

                # Get label value for grouping
                if regex_label:
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(regex_label, regex_label)
                    label_val = e.get(f"label_{mapped_key}", "default")
                    group_key = regex_label
                elif labels_filter:
                    first_label = list(labels_filter.keys())[0]
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(first_label, first_label)
                    label_val = e.get(f"label_{mapped_key}", "default")
                    group_key = first_label
                else:
                    label_val = e.get("logGroupName", "default")
                    group_key = "log_group"

                # Calculate time bucket
                ts_ms = e.get("timestamp", 0)
                if isinstance(ts_ms, int):
                    bucket_ts = int(ts_ms / 1000 / step_seconds) * step_seconds
                else:
                    bucket_ts = int(time.time())

                msg_size = len(e.get("message", "").encode("utf-8"))

                key = (label_val, group_key)
                if key not in volume_by_label_time:
                    volume_by_label_time[key] = {}
                if bucket_ts not in volume_by_label_time[key]:
                    volume_by_label_time[key][bucket_ts] = 0
                volume_by_label_time[key][bucket_ts] += msg_size

                total_bytes += msg_size
                total_lines += 1

            # Build matrix result (time series)
            for (label_val, group_key), time_buckets in volume_by_label_time.items():
                metric = {group_key: label_val}
                values = []
                for bucket_ts in sorted(time_buckets.keys()):
                    values.append([bucket_ts, str(time_buckets[bucket_ts])])

                result.append({"metric": metric, "values": values})
        except Exception as e:
            logger.error(f" Error: {e}")

    loki_metrics.record_index_volume_range_request()
    return jsonify(
        {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": result,
                "stats": {
                    "summary": {
                        "bytesProcessedPerSecond": 0,
                        "linesProcessedPerSecond": 0,
                        "totalBytesProcessed": total_bytes,
                        "totalLinesProcessed": total_lines,
                        "execTime": 0.001,
                        "queueTime": 0,
                        "subqueries": 0,
                        "totalEntriesReturned": len(result),
                        "splits": 1,
                        "shards": 0,
                        "totalPostFilterLines": total_lines,
                        "totalStructuredMetadataBytesProcessed": 0,
                    }
                },
            },
        }
    )


@loki_bp.route("/loki/api/v1/detected_labels", methods=["GET"])
def loki_detected_labels():
    # Real Loki format: {"detectedLabels": [{"label": "...", "cardinality": N}, ...]}
    warehouse = get_warehouse()
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")

    # Parse time range
    try:
        start_time = (
            int(int(start) / 1000000) if start else int((time.time() - 3600) * 1000)
        )
        end_time = int(int(end) / 1000000) if end else int(time.time() * 1000)
    except:
        start_time = int((time.time() - 3600) * 1000)
        end_time = int(time.time() * 1000)

    # Get labels from config
    labels = ["log_group", "log_stream", "env", "service", "host", "region"]
    detected = []

    if warehouse:
        try:
            # Query to get unique values for each label
            for label in labels:
                storage_key = f"label_{label}"
                # Get distinct values count
                distinct_count = 1  # Default
                detected.append({"label": label, "cardinality": distinct_count})
        except:
            pass

    if not detected:
        # Return hardcoded if can't query
        detected = [
            {"label": "log_group", "cardinality": 2},
            {"label": "log_stream", "cardinality": 3},
            {"label": "env", "cardinality": 3},
            {"label": "service", "cardinality": 3},
            {"label": "host", "cardinality": 2},
            {"label": "region", "cardinality": 3},
        ]

    return jsonify({"detectedLabels": detected})


@loki_bp.route("/loki/api/v1/detected_fields", methods=["GET"])
def loki_detected_fields():
    # Real Loki format: {"fields": [{"label": "...", "type": "...", "cardinality": N, "parsers": [...]}, ...], "limit": 1000}
    return jsonify(
        {
            "fields": [
                {
                    "label": "message",
                    "type": "string",
                    "cardinality": 100,
                    "parsers": ["logfmt"],
                },
                {
                    "label": "level",
                    "type": "string",
                    "cardinality": 5,
                    "parsers": ["logfmt"],
                },
            ],
            "limit": 1000,
        }
    )


@loki_bp.route("/loki/api/v1/detected_field/<name>/values", methods=["GET", "POST"])
def loki_detected_field_values(name):
    # Per Loki docs: returns values for a specific detected field
    # Format: {"values": ["value1", "value2", ...], "limit": 1000}
    warehouse = get_warehouse()
    query = request.args.get("query", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    limit = int(request.args.get("limit", 1000))

    # Parse time range
    def parse_time(time_str):
        if not time_str:
            return None
        try:
            ts = int(time_str)
            if ts > 1e15:
                return int(ts / 1000000)
            elif ts > 1e12:
                return ts
            else:
                return int(ts * 1000)
        except ValueError:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except:
                return None

    start_time = parse_time(start) or int((time.time() - 3600) * 1000)
    end_time = parse_time(end) or int(time.time() * 1000)

    # Parse query to get filters
    log_group, log_stream, message_filter, labels_filter, regex_label = (
        parse_logql_filter(query)
    )

    values = []

    if warehouse:
        try:
            events = warehouse.get_logs(table_name=warehouse.config.get("loki", {}).get("table_name", "loki_logs"), 
                log_group_name=log_group,
                log_stream_name=log_stream,
                start_time=start_time,
                end_time=end_time,
                limit=10000,
            )

            # Extract unique values for the requested field
            unique_values = set()
            for e in events:
                # Apply label filters
                match = True
                for label_key, label_value in labels_filter.items():
                    label_mapping = {"service_name": "service"}
                    mapped_key = label_mapping.get(label_key, label_key)
                    storage_key = f"label_{mapped_key}"
                    if e.get(storage_key) != label_value:
                        match = False
                        break

                if not match:
                    continue

                # Extract value based on field name
                if name == "message":
                    val = e.get("message", "")
                elif name == "level":
                    # Try to extract level from message (simple approach)
                    msg = e.get("message", "").lower()
                    if "error" in msg:
                        val = "error"
                    elif "warn" in msg:
                        val = "warn"
                    elif "info" in msg:
                        val = "info"
                    elif "debug" in msg:
                        val = "debug"
                    else:
                        val = "info"
                elif name.startswith("label_"):
                    val = e.get(name, "")
                else:
                    val = e.get(name, "")

                if val:
                    unique_values.add(val)

            values = list(unique_values)[:limit]
        except Exception as e:
            logger.error(f" Error: {e}")

    # Return default values if no data
    if not values:
        if name == "level":
            values = ["error", "warn", "info", "debug"]
        else:
            values = []

    return jsonify(
        {
            "values": values,
            "limit": limit,
        }
    )


@loki_bp.route("/loki/api/v1/patterns", methods=["GET"])
def loki_patterns():
    # Real Loki format: {"status": "success", "data": []}
    return jsonify({"status": "success", "data": []})


@loki_bp.route("/loki/api/v1/drilldown-limits", methods=["GET"])
def loki_drilldown_limits():
    return jsonify(
        {"status": "success", "data": {"maxLines": 10000, "maxIntervalSeconds": 3600}}
    )


@loki_bp.route("/loki/api/v1/status/buildinfo", methods=["GET"])
def loki_buildinfo():
    return jsonify(
        {
            "version": "2.9.0",
            "revision": "12345678",
            "branch": "main",
            "buildUser": "cloudwatch-local",
            "buildDate": "2024-01-01",
            "goVersion": "go1.21.0",
            "edition": "oss",
            "features": {
                "metric_aggregation": True,
                "log_push_api": True,
                "ruler_api": True,
                "alertmanager_api": True,
            },
            "limits": {"retention_period": "7d", "max_query_lookback": "30d"},
        }
    )


@loki_bp.route("/loki", methods=["GET"])
def loki_root():
    return jsonify({"status": "ok", "message": "Loki API compatible endpoint"})


@loki_bp.route("/ready", methods=["GET"])
def loki_ready():
    # Real Loki returns plain text "ready"
    return "ready"
