import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import unittest
from unittest.mock import Mock, patch, MagicMock
import time

from src.cloudwatch_local_service.routes.query_parser import parse_query
from src.cloudwatch_local_service.routes.query import (
    _apply_filter,
    QueryExecution,
    execute_query_internal,
    get_query_results_internal,
    query_executions,
)
from src.cloudwatch_local_service.routes.query import _get_warehouse


class TestQueryParser(unittest.TestCase):
    def test_filter_like_string(self):
        q = 'fields @timestamp, @message | filter @message like "error"'
        result = parse_query(q)
        self.assertEqual(result["filters"], [("like", "@message", "error")])
        self.assertEqual(result["fields"], ["@timestamp", "@message"])

    def test_filter_like_regex(self):
        q = "fields @timestamp, @message | filter @message like /error/"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("regex", "@message", "error")])

    def test_filter_like_regex_with_flags(self):
        q = "fields @timestamp, @message | filter @message like /(?i)error/"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("regex", "@message", "(?i)error")])

    def test_filter_regex_syntax(self):
        q = "fields @timestamp, @message | filter @message =~ /90994/"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("regex", "@message", "90994")])

    def test_filter_not_like(self):
        q = 'fields @timestamp, @message | filter @message not like "debug"'
        result = parse_query(q)
        self.assertEqual(result["filters"], [("not_like", "@message", "debug")])

    def test_filter_comparison_eq(self):
        q = 'fields @timestamp, @message | filter @message = "test"'
        result = parse_query(q)
        self.assertEqual(result["filters"], [("cmp", "@message", "=", "test")])

    def test_filter_comparison_gte(self):
        q = "fields @timestamp, @message | filter status >= 400"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("cmp", "status", ">=", "400")])

    def test_filter_comparison_lt(self):
        q = "fields @timestamp, @message | filter status < 500"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("cmp", "status", "<", "500")])

    def test_filter_and(self):
        q = "fields @timestamp, @message | filter status >= 400 and status < 500"
        result = parse_query(q)
        expected = (
            "and",
            ("cmp", "status", ">=", "400"),
            ("cmp", "status", "<", "500"),
        )
        self.assertEqual(result["filters"], [expected])

    def test_filter_in(self):
        q = 'fields @timestamp, @message | filter @message in ["error", "warn"]'
        result = parse_query(q)
        self.assertEqual(result["filters"], [("in", "@message", ["error", "warn"])])

    def test_sort_asc(self):
        q = "fields @timestamp | sort @timestamp asc"
        result = parse_query(q)
        self.assertEqual(result["sort"], ("@timestamp", "asc"))

    def test_sort_desc(self):
        q = "fields @timestamp | sort @timestamp desc"
        result = parse_query(q)
        self.assertEqual(result["sort"], ("@timestamp", "desc"))

    def test_limit(self):
        q = "fields @timestamp | limit 20"
        result = parse_query(q)
        self.assertEqual(result["limit"], 20)

    def test_full_query(self):
        q = "fields @timestamp, @message | filter @message like /error/ | sort @timestamp desc | limit 100"
        result = parse_query(q)
        self.assertEqual(result["fields"], ["@timestamp", "@message"])
        self.assertEqual(result["filters"], [("regex", "@message", "error")])
        self.assertEqual(result["sort"], ("@timestamp", "desc"))
        self.assertEqual(result["limit"], 100)

    def test_multiline_query(self):
        q = """fields @timestamp, @message
| filter @message like /90994/
| sort @timestamp desc
| limit 10000"""
        result = parse_query(q)
        self.assertEqual(result["filters"], [("regex", "@message", "90994")])
        self.assertEqual(result["sort"], ("@timestamp", "desc"))
        self.assertEqual(result["limit"], 10000)

    def test_stats_query(self):
        q = "stats count() by bin(1h)"
        result = parse_query(q)
        self.assertEqual(result["stats"], (("count", "*"), ("bin", 1, "h")))

    def test_generated_sql(self):
        q = 'fields @timestamp, @message | filter @message like "error"'
        result = parse_query(q)
        self.assertIn("SELECT", result["sql"])
        self.assertIn("@timestamp", result["sql"])
        self.assertIn("@message", result["sql"])
        self.assertIn("FROM logs", result["sql"])
        self.assertIn("LIKE", result["sql"])

    def test_filter_or(self):
        q = 'fields @timestamp, @message | filter @message like "error" or @message like "warn"'
        result = parse_query(q)
        self.assertEqual(len(result["filters"]), 1)
        self.assertEqual(result["filters"][0][0], "or")

    def test_filter_not_regex(self):
        q = "fields @timestamp, @message | filter @message not like /(?i)error/"
        result = parse_query(q)
        self.assertEqual(result["filters"], [("not_regex", "@message", "(?i)error")])

    def test_stats_avg(self):
        q = "stats avg(duration) by bin(5m)"
        result = parse_query(q)
        self.assertEqual(result["stats"], (("avg", "duration"), ("bin", 5, "m")))


class TestApplyFilter(unittest.TestCase):
    def test_like_match(self):
        event = {
            "message": "Order 90994 created successfully",
            "@timestamp": "2024-01-01",
        }
        filters = [("like", "@message", "Order")]
        self.assertTrue(_apply_filter(event, filters))

    def test_like_no_match(self):
        event = {"message": "Order 12345 created", "@timestamp": "2024-01-01"}
        filters = [("like", "@message", "90994")]
        self.assertFalse(_apply_filter(event, filters))

    def test_regex_match(self):
        event = {
            "message": "Order 90994 created successfully",
            "@timestamp": "2024-01-01",
        }
        filters = [("regex", "@message", "90994")]
        self.assertTrue(_apply_filter(event, filters))

    def test_regex_no_match(self):
        event = {"message": "Order 12345 created", "@timestamp": "2024-01-01"}
        filters = [("regex", "@message", "90994")]
        self.assertFalse(_apply_filter(event, filters))

    def test_regex_with_flags(self):
        event = {"message": "ERROR: something failed", "@timestamp": "2024-01-01"}
        filters = [("regex", "@message", "(?i)error")]
        self.assertTrue(_apply_filter(event, filters))

    def test_regex_case_sensitive(self):
        event = {"message": "Error: something failed", "@timestamp": "2024-01-01"}
        filters = [("regex", "@message", "error")]
        self.assertFalse(_apply_filter(event, filters))

    def test_regex_case_sensitive_strict(self):
        event = {"message": "error: something failed", "@timestamp": "2024-01-01"}
        filters = [("regex", "@message", "error")]
        self.assertTrue(_apply_filter(event, filters))

    def test_not_like_match(self):
        event = {"message": "Order 12345 created", "@timestamp": "2024-01-01"}
        filters = [("not_like", "@message", "error")]
        self.assertTrue(_apply_filter(event, filters))

    def test_not_like_no_match(self):
        event = {"message": "ERROR: something failed", "@timestamp": "2024-01-01"}
        filters = [("not_like", "@message", "error")]
        self.assertFalse(_apply_filter(event, filters))

    def test_cmp_eq_match(self):
        event = {"message": "test", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "=", "test")]
        self.assertTrue(_apply_filter(event, filters))

    def test_cmp_eq_no_match(self):
        event = {"message": "test", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "=", "other")]
        self.assertFalse(_apply_filter(event, filters))

    def test_cmp_gte_match(self):
        event = {"message": "500", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", ">=", "400")]
        self.assertTrue(_apply_filter(event, filters))

    def test_cmp_lt_match(self):
        event = {"message": "300", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "<", "400")]
        self.assertTrue(_apply_filter(event, filters))

    def test_cmp_gte_no_match(self):
        event = {"message": "300", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", ">=", "400")]
        self.assertFalse(_apply_filter(event, filters))

    def test_cmp_lt_no_match(self):
        event = {"message": "500", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "<", "400")]
        self.assertFalse(_apply_filter(event, filters))

    def test_cmp_neq_match(self):
        event = {"message": "error", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "!=", "success")]
        self.assertTrue(_apply_filter(event, filters))

    def test_cmp_neq_no_match(self):
        event = {"message": "error", "@timestamp": "2024-01-01"}
        filters = [("cmp", "@message", "!=", "error")]
        self.assertFalse(_apply_filter(event, filters))

    def test_in_match(self):
        event = {"message": "error", "@timestamp": "2024-01-01"}
        filters = [("in", "@message", ["error", "warn", "info"])]
        self.assertTrue(_apply_filter(event, filters))

    def test_in_no_match(self):
        event = {"message": "debug", "@timestamp": "2024-01-01"}
        filters = [("in", "@message", ["error", "warn", "info"])]
        self.assertFalse(_apply_filter(event, filters))

    def test_in_case_sensitive(self):
        event = {"message": "ERROR", "@timestamp": "2024-01-01"}
        filters = [("in", "@message", ["error", "warn", "info"])]
        self.assertFalse(_apply_filter(event, filters))

    def test_multiple_filters_and(self):
        event = {"message": "Order 90994 error", "@timestamp": "2024-01-01"}
        filters = [
            ("like", "@message", "90994"),
            ("like", "@message", "error"),
        ]
        self.assertTrue(_apply_filter(event, filters))

    def test_multiple_filters_one_fails(self):
        event = {"message": "Order 90994 success", "@timestamp": "2024-01-01"}
        filters = [
            ("like", "@message", "90994"),
            ("like", "@message", "error"),
        ]
        self.assertFalse(_apply_filter(event, filters))

    def test_empty_filters(self):
        event = {"message": "anything", "@timestamp": "2024-01-01"}
        filters = []
        self.assertTrue(_apply_filter(event, filters))

    def test_field_with_at_prefix(self):
        event = {"message": "test message", "@timestamp": "2024-01-01"}
        filters = [("like", "@message", "test")]
        self.assertTrue(_apply_filter(event, filters))

    def test_field_without_at_prefix(self):
        event = {"message": "test message", "message": "test message"}
        filters = [("like", "message", "test")]
        self.assertTrue(_apply_filter(event, filters))

    def test_and_or_combined(self):
        event = {"message": "error warning", "@timestamp": "2024-01-01"}
        filters = [
            ("or", ("like", "@message", "error"), ("like", "@message", "warning"))
        ]
        self.assertTrue(_apply_filter(event, filters))

    def test_not_expr(self):
        event = {"message": "success", "@timestamp": "2024-01-01"}
        filters = [("not", ("like", "@message", "error"))]
        self.assertTrue(_apply_filter(event, filters))


class TestQueryExecution(unittest.TestCase):
    def setUp(self):
        query_executions.clear()

    def test_query_execution_init(self):
        execution = QueryExecution(
            query_id="test-123",
            log_group_name="/my/group",
            query_string="fields @timestamp, @message",
            start_time=1000,
            end_time=2000,
        )
        self.assertEqual(execution.query_id, "test-123")
        self.assertEqual(execution.log_group_name, "/my/group")
        self.assertEqual(execution.query_string, "fields @timestamp, @message")
        self.assertEqual(execution.start_time, 1000)
        self.assertEqual(execution.end_time, 2000)
        self.assertEqual(execution.status, "Running")
        self.assertEqual(execution.results, [])
        self.assertIsNone(execution.parsed_query)

    def test_query_execution_default_values(self):
        execution = QueryExecution(
            query_id="test-456",
            log_group_name="/my/group",
            query_string="stats count()",
            start_time=None,
            end_time=None,
        )
        self.assertIsNone(execution.start_time)
        self.assertIsNone(execution.end_time)
        self.assertEqual(execution.status, "Running")


class TestExecuteQueryInternal(unittest.TestCase):
    def setUp(self):
        query_executions.clear()

    @patch("cloudwatch_local_service.routes.query._get_warehouse")
    def test_execute_query_without_warehouse(self, mock_get_warehouse):
        mock_get_warehouse.return_value = None

        result = execute_query_internal(
            log_group_name="/test/group",
            query_string='fields @timestamp, @message | filter @message like "error"',
            start_time=int(time.time()) - 3600,
            end_time=int(time.time()),
        )

        self.assertIn("queryId", result)
        query_id = result["queryId"]
        self.assertIn(query_id, query_executions)

    @patch("cloudwatch_local_service.routes.query._get_warehouse")
    def test_execute_query_creates_execution(self, mock_get_warehouse):
        mock_get_warehouse.return_value = None

        result = execute_query_internal(
            log_group_name="/test/group",
            query_string="fields @timestamp",
            start_time=None,
            end_time=None,
        )

        query_id = result["queryId"]
        execution = query_executions[query_id]
        self.assertEqual(execution.log_group_name, "/test/group")
        self.assertEqual(execution.status, "Complete")


class TestGetQueryResultsInternal(unittest.TestCase):
    def setUp(self):
        query_executions.clear()

    def test_get_query_results_not_found(self):
        result = get_query_results_internal("non-existent-id")
        self.assertEqual(result["__type"], "ResourceNotFoundException")
        self.assertIn("not found", result["message"])

    def test_get_query_results_empty_results(self):
        query_id = "test-empty-123"
        execution = QueryExecution(
            query_id=query_id,
            log_group_name="/test/group",
            query_string="fields @timestamp",
            start_time=None,
            end_time=None,
        )
        execution.results = []
        execution.status = "Complete"
        query_executions[query_id] = execution

        result = get_query_results_internal(query_id)
        self.assertEqual(result["queryId"], query_id)
        self.assertEqual(result["status"], "Complete")
        self.assertEqual(result["results"], [])
        self.assertEqual(result["statistics"]["recordsMatched"], 0)

    def test_get_query_results_with_data(self):
        query_id = "test-data-123"
        execution = QueryExecution(
            query_id=query_id,
            log_group_name="/test/group",
            query_string="fields @timestamp, @message",
            start_time=None,
            end_time=None,
        )
        execution.results = [
            {"timestamp": 1704067200000, "message": "test message 1"},
            {"timestamp": 1704067201000, "message": "test message 2"},
        ]
        execution.status = "Complete"
        query_executions[query_id] = execution

        result = get_query_results_internal(query_id)
        self.assertEqual(result["queryId"], query_id)
        self.assertEqual(result["status"], "Complete")
        self.assertEqual(len(result["results"]), 2)
        self.assertEqual(result["statistics"]["recordsMatched"], 2)

    def test_get_query_results_stats_format(self):
        query_id = "test-stats-123"
        execution = QueryExecution(
            query_id=query_id,
            log_group_name="/test/group",
            query_string="stats count() by bin(1h)",
            start_time=None,
            end_time=None,
        )
        execution.results = [
            {"count": 100},
            {"count": 200},
        ]
        execution.status = "Complete"
        query_executions[query_id] = execution

        result = get_query_results_internal(query_id)
        self.assertEqual(len(result["results"]), 2)
        self.assertEqual(result["results"][0][1]["field"], "count()")
        self.assertEqual(result["results"][0][1]["value"], "100")

    def test_get_query_results_null_query_id(self):
        result = get_query_results_internal(None)
        self.assertEqual(result["__type"], "ResourceNotFoundException")

    def test_get_query_results_empty_query_id(self):
        result = get_query_results_internal("")
        self.assertEqual(result["__type"], "ResourceNotFoundException")


class TestQueryIntegration(unittest.TestCase):
    """Integration tests for query parsing and filtering together"""

    def setUp(self):
        query_executions.clear()

    def test_parse_and_filter_integration(self):
        q = "fields @timestamp, @message | filter @message like /error/ | sort @timestamp desc | limit 50"
        result = parse_query(q)

        events = [
            {"timestamp": "2024-01-01T00:00:00", "message": "This is an error message"},
            {"timestamp": "2024-01-01T00:00:01", "message": "This is a warning"},
            {"timestamp": "2024-01-01T00:00:02", "message": "Another error occurred"},
            {"timestamp": "2024-01-01T00:00:03", "message": "Success!"},
        ]

        filtered = [e for e in events if _apply_filter(e, result["filters"])]
        self.assertEqual(len(filtered), 2)

    def test_case_insensitive_like(self):
        q = 'fields @message | filter @message like "ERROR"'
        result = parse_query(q)

        events = [
            {"message": "Error: something failed"},
            {"message": "error: something failed"},
            {"message": "ERROR: something failed"},
            {"message": "Success!"},
        ]

        filtered = [e for e in events if _apply_filter(e, result["filters"])]
        self.assertEqual(len(filtered), 3)

    def test_numeric_comparison(self):
        q = 'fields @message | filter @message >= "400" and @message < "500"'
        result = parse_query(q)

        events = [
            {"message": "400"},
            {"message": "404"},
            {"message": "499"},
            {"message": "500"},
            {"message": "300"},
        ]

        filtered = [e for e in events if _apply_filter(e, result["filters"])]
        self.assertEqual(len(filtered), 3)


if __name__ == "__main__":
    unittest.main()
