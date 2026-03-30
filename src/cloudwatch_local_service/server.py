import os
import sys
from pathlib import Path
from flask import Flask, request, jsonify

sys.path.insert(0, str(Path(__file__).parent.parent))

from cloudwatch_local_service.services.file_storage import FileStorage
from cloudwatch_local_service.routes.logs import logs_bp
from cloudwatch_local_service.routes.groups import groups_bp
from cloudwatch_local_service.routes.streams import streams_bp
from cloudwatch_local_service.routes.query import query_bp
from cloudwatch_local_service.routes.store import store_bp

app = Flask(__name__)

LOGS_DIR = Path(os.getenv("LOGS_DIR", "./logs"))
LOGS_DIR.mkdir(parents=True, exist_ok=True)

file_storage = FileStorage(LOGS_DIR)

app.register_blueprint(logs_bp)
app.register_blueprint(groups_bp)
app.register_blueprint(streams_bp)
app.register_blueprint(query_bp)
app.register_blueprint(store_bp)


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "cloudwatch-logs-local"})


@app.route("/", methods=["GET", "POST", "PUT"])
def handle_request():
    amz_target = request.headers.get("X-Amz-Target", "")

    if amz_target:
        action = amz_target.split(".")[-1] if "." in amz_target else amz_target
    else:
        action = request.args.get("Action")
        if not action and request.method == "POST":
            data = request.get_json(force=True) or {}
            action = data.get("Action")

    if action == "DescribeLogGroups":
        return describe_log_groups()
    elif action == "DescribeLogStreams":
        return describe_log_streams()
    elif action == "GetLogEvents":
        return get_log_events()
    elif action == "PutLogEvents":
        return put_log_events()
    elif action == "FilterLogEvents":
        return filter_log_events()
    elif action == "StartQueryExecution":
        return start_query_execution()
    elif action == "StartQuery":
        return start_query_execution()
    elif action == "GetQueryResults":
        return get_query_execution_results()
    elif action == "GetQuery":
        return get_query_execution_results()

    if request.method == "POST":
        data = request.get_json(force=True) or {}
        if "logEvents" in data:
            return put_log_events()
        else:
            return get_log_events()

    if action == "GetLogEvents" or (not action and request.method == "GET"):
        return get_log_events()
    else:
        return put_log_events()


def describe_log_groups():
    from cloudwatch_local_service.services.log_store import log_store

    groups = log_store.get_all_log_groups()
    return jsonify({"logGroups": list(groups.values())})


def describe_log_streams():
    from cloudwatch_local_service.services.log_store import log_store

    if request.method == "POST":
        data = request.get_json(force=True) or {}
        log_group_name = data.get("logGroupName")
    else:
        log_group_name = request.args.get("logGroupName")

    if not log_group_name:
        return jsonify(
            {"__type": "InvalidParameterException", "message": "Missing logGroupName"}
        ), 400

    streams = log_store.get_log_streams(log_group_name)
    return jsonify({"logStreams": streams})


def put_log_events():
    from cloudwatch_local_service.services.log_store import log_store
    import time

    data = request.get_json(force=True)
    log_group_name = data.get("logGroupName")
    log_stream_name = data.get("logStreamName")
    log_events = data.get("logEvents", [])

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

    file_storage.save_logs(log_group_name, log_stream_name, log_events, ingestion_time)

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


def get_log_events():
    from cloudwatch_local_service.services.log_store import log_store

    if request.method == "POST":
        data = request.get_json(force=True)
        log_group_name = data.get("logGroupName")
        log_stream_name = data.get("logStreamName")
        start_time = data.get("startTime")
        end_time = data.get("endTime")
        limit = data.get("limit", 100)
    else:
        log_group_name = request.args.get("logGroupName")
        log_stream_name = request.args.get("logStreamName")
        start_time = request.args.get("startTime")
        end_time = request.args.get("endTime")
        limit = request.args.get("limit", 100)

    if start_time:
        start_time = int(start_time)
    if end_time:
        end_time = int(end_time)
    if limit:
        limit = int(limit)

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

    return jsonify(
        {
            "events": [
                {
                    "timestamp": e["timestamp"],
                    "message": e["message"],
                    "ingestionTime": e["ingestionTime"],
                }
                for e in events
            ],
            "nextForwardToken": "end",
            "nextBackwardToken": "end",
        }
    )


def filter_log_events():
    from cloudwatch_local_service.services.log_store import log_store

    data = request.get_json(force=True) or {}
    log_group_name = data.get("logGroupName")
    log_stream_name_prefix = data.get("logStreamNamePrefix")
    filter_pattern = data.get("filterPattern", "")
    start_time = data.get("startTime")
    end_time = data.get("endTime")
    limit = data.get("limit", 100)

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


def start_query_execution():
    from cloudwatch_local_service.routes.query import (
        start_query_execution as start_query,
    )

    return start_query()


def get_query_execution_results():
    from cloudwatch_local_service.routes.query import get_query_results as get_results

    return get_results()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4588, debug=True)
