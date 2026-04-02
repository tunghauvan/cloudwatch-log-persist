import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, request, jsonify

from service.services.log_store import log_store

streams_bp = Blueprint("streams", __name__)


@streams_bp.route("/DescribeLogStreams", methods=["GET", "POST"])
def describe_log_streams():
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


@streams_bp.route("/log-streams", methods=["GET", "POST"])
def describe_log_streams_alt():
    return describe_log_streams()
