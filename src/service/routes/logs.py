import sys
from pathlib import Path

from flask import Blueprint, request, jsonify
import time

from service.services.log_store import log_store

logs_bp = Blueprint("logs", __name__)


def get_log_buffer():
    from service.server import log_buffer
    return log_buffer


def get_warehouse():
    from service.server import warehouse
    return warehouse


def get_request_params():
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        return {
            "log_group_name": data.get("logGroupName"),
            "log_stream_name": data.get("logStreamName"),
            "log_events": data.get("logEvents", []),
            "start_time": data.get("startTime"),
            "end_time": data.get("endTime"),
            "limit": data.get("limit", 100),
            "filter_pattern": data.get("filterPattern", ""),
            "log_stream_name_prefix": data.get("logStreamNamePrefix"),
        }
    else:
        return {
            "log_group_name": request.args.get("logGroupName"),
            "log_stream_name": request.args.get("logStreamName"),
            "log_events": [],
            "start_time": request.args.get("startTime"),
            "end_time": request.args.get("endTime"),
            "limit": int(request.args.get("limit", 100)),
            "filter_pattern": request.args.get("filterPattern", ""),
            "log_stream_name_prefix": request.args.get("logStreamNamePrefix"),
        }


@logs_bp.route("/PutLogEvents", methods=["POST"])
def put_log_events():
    params = get_request_params()
    log_group_name = params["log_group_name"]
    log_stream_name = params["log_stream_name"]
    log_events = params["log_events"]

    if not log_group_name or not log_stream_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    key = f"{log_group_name}/{log_stream_name}"
    ingestion_time = int(time.time() * 1000)

    existing_token = request.headers.get("X-Amz-Sequence-Token")
    expected_token = log_store.get_sequence_token(key)

    if existing_token and existing_token != expected_token:
        return jsonify(
            {
                "__type": "InvalidSequenceTokenException",
                "message": "Sequence token is invalid",
                "expectedSequenceToken": expected_token,
            }
        ), 400

    new_sequence = log_store.add_log_group(
        log_group_name, log_stream_name, log_events, ingestion_time
    )

    # Add logs to log_buffer (Hot Tier)
    import logging
    logger = logging.getLogger("service.logs")
    
    try:
        buffer = get_log_buffer()
        logger.info(f"[PutLogEvents] Buffer obj: {buffer}")
        if buffer:
            events_to_buffer = []
            for event in log_events:
                events_to_buffer.append({
                    "logGroupName": log_group_name,
                    "logStreamName": log_stream_name,
                    # timestamp from CloudWatch API is already in ms; _logs_to_arrow
                    # normalises any epoch scale so we pass it as-is.
                    "timestamp": event.get("timestamp"),
                    "message": event.get("message"),
                    "ingestionTime": ingestion_time,
                    "sequenceToken": new_sequence,
                })
            buffer.add(events_to_buffer)
            logger.info(f"[PutLogEvents] Added {len(events_to_buffer)} events to buffer, buffer size now: {buffer.size()}")
        else:
            logger.warning(f"[PutLogEvents] Buffer is None!")
    except Exception as e:
        logger.error(f"[PutLogEvents] Could not add to buffer: {e}", exc_info=True)

    next_token = str(new_sequence)

    return jsonify(
        {
            "nextSequenceToken": next_token,
            "rejectedLogEventsInfo": {
                "tooOldLogEventEndIndex": [],
                "tooNewLogEventStartIndex": [],
                "matchedLogEventsNotInserted": [],
            },
        }
    )


@logs_bp.route("/GetLogEvents", methods=["GET", "POST"])
def get_log_events():
    params = get_request_params()
    log_group_name = params["log_group_name"]
    log_stream_name = params["log_stream_name"]
    start_time = params["start_time"]
    end_time = params["end_time"]
    limit = params["limit"]

    if start_time:
        start_time = int(start_time)
    if end_time:
        end_time = int(end_time)

    if not log_group_name or not log_stream_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    buffer = get_log_buffer()
    warehouse = get_warehouse()

    # 1. Search in Hot Tier (memory buffer)
    hot_events = []
    if buffer:
        hot_events = buffer.get_logs(
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    # 2. Search in Cold Tier (S3/Iceberg)
    events = log_store.get_events(
        log_group_name=log_group_name,
        log_stream_name=log_stream_name,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )
    
    # Merge & Sort
    all_events = events + hot_events
    # Remove duplicates if any (based on message and timestamp)
    seen = set()
    unique_events = []
    for e in all_events:
        # Create a unique key for the event
        key = (e.get('timestamp'), e.get('message'), e.get('log_stream_name') or log_stream_name)
        if key not in seen:
            seen.add(key)
            unique_events.append(e)

    unique_events.sort(key=lambda x: x.get("timestamp", 0))
    events = unique_events[:limit]

    print(f"[Debug] Returning events from Hot & Cold tier. Total: {len(events)} (Hot: {len(hot_events)}, Cold: {len(all_events)-len(hot_events)})")
    return jsonify(
        {
            "events": [
                {
                    "timestamp": int(e["timestamp"]) if "timestamp" in e else 0,
                    "message": e["message"],
                    "ingestionTime": int(e.get("ingestionTime") or e.get("ingestion_time") or 0),
                }
                for e in events
            ],
            "nextForwardToken": "end",
            "nextBackwardToken": "end",
        }
    )


@logs_bp.route("/FilterLogEvents", methods=["GET", "POST"])
def filter_log_events():
    params = get_request_params()
    log_group_name = params["log_group_name"]
    log_stream_name_prefix = params["log_stream_name_prefix"]
    filter_pattern = params["filter_pattern"]
    start_time = params["start_time"]
    end_time = params["end_time"]
    limit = params["limit"]

    if start_time:
        start_time = int(start_time)
    if end_time:
        end_time = int(end_time)

    if not log_group_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    from service.utils.helpers import parse_filter_pattern
    
    buffer = get_log_buffer()

    hot_events = []
    if buffer:
        hot_events = buffer.get_logs(
            log_group_name=log_group_name,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    
    # Filter hot events by stream prefix and pattern
    filtered_hot = []
    for e in hot_events:
        if log_stream_name_prefix:
            if not e.get("log_stream_name", "").startswith(log_stream_name_prefix):
                continue
        if filter_pattern:
            if not parse_filter_pattern(filter_pattern, e.get("message", "")):
                continue
        filtered_hot.append(e)

    events = log_store.filter_events(
        log_group_name=log_group_name,
        log_stream_name_prefix=log_stream_name_prefix,
        filter_pattern=filter_pattern,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    # Merge & Sort
    all_events = events + filtered_hot
    seen = set()
    unique_events = []
    for e in all_events:
        key = (e.get('timestamp'), e.get('message'), e.get('log_stream_name') or e.get('_stream_name'))
        if key not in seen:
            seen.add(key)
            unique_events.append(e)

    unique_events.sort(key=lambda x: x.get("timestamp", 0))
    events = unique_events[:limit]

    return jsonify(
        {
            "events": [
                {
                    "timestamp": e["timestamp"],
                    "message": e["message"],
                    "ingestionTime": e.get("ingestionTime") or e.get("ingestion_time") or 0,
                }
                for e in events
            ]
        }
    )


@logs_bp.route("/MigrateToTimestampPartitioning", methods=["POST"])
def migrate_to_timestamp_partitioning():
    """
    Migrate CloudWatch logs table from ingestion_time partitioning to timestamp partitioning.
    This improves query performance by enabling better partition pruning.
    
    POST /MigrateToTimestampPartitioning
    """
    warehouse = get_warehouse()
    if not warehouse:
        return jsonify({
            "status": "error",
            "message": "Warehouse not available"
        }), 500
    
    result = warehouse.migrate_to_timestamp_partitioning()
    status_code = 200 if result["status"] in ("success", "skipped") else 500
    return jsonify(result), status_code
