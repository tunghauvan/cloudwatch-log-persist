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


def _get_warehouse():
    from cloudwatch_local_service.server import warehouse

    return warehouse


def _execute_query(execution: QueryExecution):
    warehouse = _get_warehouse()

    if warehouse:
        try:
            filter_expr = f"log_group_name == '{execution.log_group_name}'"

            if execution.start_time:
                filter_expr += f" AND timestamp >= {execution.start_time * 1000000}"
            if execution.end_time:
                filter_expr += f" AND timestamp <= {execution.end_time * 1000000}"

            print(f"[Query] filter_expr: {filter_expr}")

            result = warehouse.query(filter_expr=filter_expr, limit=1000)

            events = []
            for i in range(result.num_rows):
                row = {
                    col: result.column(col)[i].as_py() for col in result.column_names
                }
                events.append(row)
            print(f"[Query] query_string: {execution.query_string}")
            print(f"[Query] Warehouse returned {len(events)} events")
        except Exception as e:
            print(f"[Query] Warehouse query error: {e}")
            events = []
    else:
        events = log_store.get_events(
            log_group_name=execution.log_group_name,
            start_time=execution.start_time,
            end_time=execution.end_time,
            limit=1000,
        )
        print(f"[Query] Fallback to log_store: {len(events)} events")

    query_string = execution.query_string.lower()
    results = []

    if (
        "fields @timestamp, @message" in query_string
        or "fields @message" in query_string
    ):
        limit_match = re.search(r"limit\s+(\d+)", query_string)
        limit = int(limit_match.group(1)) if limit_match else 100

        if "filter" in query_string:
            filter_match = re.search(r"filter\s+(.+?)(?:\s*\|)", query_string)
            if filter_match:
                filter_expr = filter_match.group(1).strip()
                if "like" in filter_expr:
                    pattern_match = re.search(r"/(.+?)/", filter_expr)
                    if pattern_match:
                        pattern = pattern_match.group(1)
                        events = [
                            e
                            for e in events
                            if pattern.lower() in str(e.get("message", "")).lower()
                        ]
                elif "@message" in filter_expr:
                    if "!=" in filter_expr:
                        parts = filter_expr.split("!=")
                        if len(parts) == 2:
                            search_term = parts[1].strip().strip("\"'")
                            events = [
                                e
                                for e in events
                                if search_term.lower()
                                not in str(e.get("message", "")).lower()
                            ]
                    elif "=" in filter_expr:
                        parts = filter_expr.split("=")
                        if len(parts) == 2:
                            search_term = parts[1].strip().strip("\"'")
                            events = [
                                e
                                for e in events
                                if search_term.lower()
                                in str(e.get("message", "")).lower()
                            ]

        results = events[:limit]
        return results

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


def execute_query_internal(
    log_group_name: str, query_string: str, start_time: int, end_time: int
):
    query_id = f"query-{int(time.time() * 1000)}"

    execution = QueryExecution(
        query_id, log_group_name, query_string, start_time, end_time
    )
    query_executions[query_id] = execution

    results = _execute_query(execution)
    execution.results = results
    execution.status = "Complete"

    return {"queryId": query_id}


def get_query_results_internal(query_id: str):
    if not query_id or query_id not in query_executions:
        return {
            "__type": "ResourceNotFoundException",
            "message": "Query not found",
        }

    execution = query_executions[query_id]
    results = execution.results or []

    if results and "count" in results[0]:
        formatted_results = [
            [
                {"field": "@ptr", "value": ""},
                {"field": "count()", "value": str(r.get("count", 0))},
            ]
            for r in results
        ]
    else:
        formatted_results = []
        for r in results:
            ts = r.get("timestamp")
            if hasattr(ts, "timestamp"):
                # datetime object - convert to milliseconds
                ts_int = int(ts.timestamp() * 1000)
            elif isinstance(ts, (int, float)):
                ts_int = int(ts)
            else:
                ts_int = ts
            formatted_results.append(
                [
                    {"field": "@timestamp", "value": str(ts_int)},
                    {"field": "@message", "value": r.get("message", "")},
                ]
            )

    return {
        "queryId": query_id,
        "status": execution.status,
        "results": formatted_results,
        "statistics": {
            "recordsMatched": len(results),
            "recordsScanned": len(results),
            "bytesScanned": sum(len(str(r.get("message", ""))) for r in results),
        },
    }
