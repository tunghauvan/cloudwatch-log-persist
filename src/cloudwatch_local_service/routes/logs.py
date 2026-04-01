import sys
from pathlib import Path

from flask import Blueprint, request, jsonify
import time

from cloudwatch_local_service.services.log_store import log_store

logs_bp = Blueprint("logs", __name__)


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

    events = log_store.get_events(
        log_group_name=log_group_name,
        log_stream_name=log_stream_name,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    print(f"[Debug] Returning events. First event timestamp: {events[0]['timestamp'] if events else 'None'}")
    return jsonify(
        {
            "events": [
                {
                    "timestamp": int(e["timestamp"] / 1000) if "timestamp" in e else 0,
                    "message": e["message"],
                    "ingestionTime": int(e["ingestionTime"] / 1000) if "ingestionTime" in e else 0,
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

    events = log_store.filter_events(
        log_group_name=log_group_name,
        log_stream_name_prefix=log_stream_name_prefix,
        filter_pattern=filter_pattern,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    return jsonify(
        {
            "events": [
                {
                    "timestamp": e["timestamp"],
                    "message": e["message"],
                    "ingestionTime": e["ingestionTime"],
                }
                for e in events
            ]
        }
    )
