import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, request, jsonify
import time
import re
from cloudwatch_local_service.services.log_store import log_store
from cloudwatch_local_service.routes.query_parser import parse_query

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
        self.parsed_query = None


query_executions = {}


def _get_warehouse():
    from cloudwatch_local_service.server import warehouse

    return warehouse


def _apply_filter(event: dict, filters: list) -> bool:
    for f in filters:
        if not _evaluate_filter(event, f):
            return False
    return True


def _evaluate_filter(event: dict, f) -> bool:
    if not isinstance(f, (list, tuple)):
        return True

    op = f[0]

    if op == "and":
        return _evaluate_filter(event, f[1]) and _evaluate_filter(event, f[2])

    if op == "or":
        return _evaluate_filter(event, f[1]) or _evaluate_filter(event, f[2])

    if op == "not":
        return not _evaluate_filter(event, f[1])

    if op not in ("like", "regex", "not_like", "not_regex", "cmp", "in"):
        return True

    field = f[1]
    val = f[2] if len(f) > 2 else None

    lookup_field = field.lstrip("@") if isinstance(field, str) else field
    field_val = str(event.get(lookup_field, ""))

    if op == "like":
        return val.lower() in field_val.lower()

    elif op == "regex":
        import re

        return bool(re.search(val, field_val))

    elif op == "not_like":
        return val.lower() not in field_val.lower()

    elif op == "not_regex":
        import re

        return not bool(re.search(val, field_val))

    elif op == "cmp":
        cmp_field = f[1]
        cmp_op = f[2]
        cmp_val = f[3]
        cmp_lookup = cmp_field.lstrip("@") if isinstance(cmp_field, str) else cmp_field
        try:
            msg_val = str(event.get(cmp_lookup, ""))
            if cmp_op == "=":
                return cmp_val.lower() == msg_val.lower()
            elif cmp_op == "!=":
                return cmp_val.lower() != msg_val.lower()
            elif cmp_op == ">=":
                return msg_val >= cmp_val
            elif cmp_op == "<=":
                return msg_val <= cmp_val
            elif cmp_op == ">":
                return msg_val > cmp_val
            elif cmp_op == "<":
                return msg_val < cmp_val
        except (TypeError, ValueError):
            return False

    elif op == "in":
        in_field = f[1]
        in_vals = f[2]
        in_lookup = in_field.lstrip("@") if isinstance(in_field, str) else in_field
        field_val = str(event.get(in_lookup, ""))
        return field_val in in_vals

    return True


def _execute_query(execution: QueryExecution):
    warehouse = _get_warehouse()

    try:
        execution.parsed_query = parse_query(execution.query_string)
        print(f"[Query] Parsed CWL to SQL: {execution.parsed_query['sql']}")
    except Exception as e:
        print(f"[Query] Failed to parse CWL query: {e}")
        execution.parsed_query = None

    if warehouse:
        try:
            where_parts = [f"log_group_name == '{execution.log_group_name}'"]

            if execution.start_time:
                where_parts.append(f"timestamp >= {execution.start_time * 1000000}")
            if execution.end_time:
                where_parts.append(f"timestamp <= {execution.end_time * 1000000}")

            filter_expr = " AND ".join(where_parts)
            print(f"[Query] filter_expr: {filter_expr}")

            limit = (
                execution.parsed_query.get("limit") or 1000
                if execution.parsed_query
                else 1000
            )
            if limit < 1:
                limit = 1000
            result = warehouse.query(filter_expr=filter_expr, limit=limit * 10)

            events = []
            for i in range(result.num_rows):
                row = {
                    col: result.column(col)[i].as_py() for col in result.column_names
                }
                events.append(row)
            print(f"[Query] Warehouse returned {len(events)} events before CWL filter")

            if execution.parsed_query and execution.parsed_query["filters"]:
                cwl_filters = execution.parsed_query["filters"]
                print(f"[Query] Applying {len(cwl_filters)} CWL filters: {cwl_filters}")
                events = [e for e in events if _apply_filter(e, cwl_filters)]
                print(f"[Query] After CWL filter: {len(events)} events")

            if execution.parsed_query and execution.parsed_query["sort"]:
                sort_field, sort_dir = execution.parsed_query["sort"]
                print(f"[Query] Sorting by {sort_field} {sort_dir}")
                reverse = sort_dir == "desc"
                events.sort(key=lambda x: x.get(sort_field, ""), reverse=reverse)

            events = events[:limit]
            print(f"[Query] query_string: {execution.query_string}")
            print(f"[Query] Final result: {len(events)} events")
        except Exception as e:
            print(f"[Query] Warehouse query error: {e}")
            import traceback

            traceback.print_exc()
            events = []
    else:
        events = log_store.get_events(
            log_group_name=execution.log_group_name,
            start_time=execution.start_time,
            end_time=execution.end_time,
            limit=1000,
        )
        print(f"[Query] Fallback to log_store: {len(events)} events")

    return events


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
