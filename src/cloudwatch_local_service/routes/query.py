import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, request, jsonify
import time
import re
from cloudwatch_local_service.services.log_store import log_store

query_bp = Blueprint("query", __name__)


class QueryExecution:
    def __init__(
        self,
        query_id: str,
        log_group_name: str,
        query_string: str,
        start_time: int,
        end_time: int,
    ):
        self.query_id = query_id
        self.log_group_name = log_group_name
        self.query_string = query_string
        self.start_time = start_time
        self.end_time = end_time
        self.status = "Running"
        self.results = []
        self.created_at = int(time.time() * 1000)


query_executions = {}


@query_bp.route("/StartQuery", methods=["POST"])
@query_bp.route("/StartQueryExecution", methods=["POST"])
def start_query_execution():
    data = request.get_json(force=True) or {}
    log_group_name = data.get("logGroupName")
    query_string = data.get("queryString", "")
    start_time = data.get("startTime", int((time.time() - 3600) * 1000))
    end_time = data.get("endTime", int(time.time() * 1000))

    if not log_group_name or not query_string:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    query_id = f"query-{int(time.time() * 1000)}"

    execution = QueryExecution(
        query_id, log_group_name, query_string, start_time, end_time
    )
    query_executions[query_id] = execution

    results = _execute_query(execution)
    execution.results = results
    execution.status = "Complete"

    return jsonify({"queryId": query_id})


@query_bp.route("/GetQueryResults", methods=["POST"])
@query_bp.route("/GetQuery", methods=["POST"])
def get_query_results():
    data = request.get_json(force=True) or {}
    query_id = data.get("queryId")

    if not query_id:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing queryId",
            }
        ), 400

    if query_id not in query_executions:
        return jsonify(
            {
                "__type": "ResourceNotFoundException",
                "message": "Query not found",
            }
        ), 400

    execution = query_executions[query_id]

    return jsonify(
        {
            "queryId": query_id,
            "status": execution.status,
            "results": [
                [
                    {"field": "@timestamp", "value": str(r.get("timestamp", ""))},
                    {"field": "@message", "value": r.get("message", "")},
                ]
                for r in execution.results
            ],
            "statistics": {
                "recordsMatched": len(execution.results),
                "recordsScanned": len(execution.results),
                "bytesScanned": sum(
                    len(str(r.get("message", ""))) for r in execution.results
                ),
            },
        }
    )


@query_bp.route("/start-query", methods=["POST"])
def start_query_execution_alt():
    return start_query_execution()


@query_bp.route("/get-query-results", methods=["POST"])
def get_query_results_alt():
    return get_query_results()


def _execute_query(execution: QueryExecution):
    events = log_store.get_events(
        log_group_name=execution.log_group_name,
        start_time=execution.start_time,
        end_time=execution.end_time,
        limit=1000,
    )

    query_string = execution.query_string.lower()
    results = []

    if (
        "fields @timestamp, @message" in query_string
        or "fields @message" in query_string
    ):
        limit_match = re.search(r"limit\s+(\d+)", query_string)
        limit = int(limit_match.group(1)) if limit_match else 100

        if "filter" in query_string:
            filter_match = re.search(r"filter\s+(.+?)(?:\s+|$)", query_string)
            if filter_match:
                filter_expr = filter_match.group(1)
                if "like" in filter_expr:
                    pattern_match = re.search(r"/(.+?)/", filter_expr)
                    if pattern_match:
                        pattern = pattern_match.group(1)
                        events = [
                            e
                            for e in events
                            if pattern.lower() in e.get("message", "").lower()
                        ]
                elif "@message" in filter_expr:
                    if "=" in filter_expr:
                        parts = filter_expr.split("=")
                        if len(parts) == 2:
                            search_term = parts[1].strip().strip('"')
                            events = [
                                e
                                for e in events
                                if search_term.lower() in e.get("message", "").lower()
                            ]

        results = events[:limit]

    elif "stats" in query_string:
        if "count()" in query_string:
            if "by bin" in query_string:
                bin_match = re.search(r"bin\((\d+)([mhd])", query_string)
                if bin_match:
                    results = [
                        {"timestamp": e.get("timestamp"), "count": 1}
                        for e in events[:100]
                    ]
            else:
                results = [{"count": len(events)}]
        else:
            results = [{"count": len(events)}]

    return results
