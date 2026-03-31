import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import unittest
from src.cloudwatch_local_service.routes.query_parser import parse_query
from src.cloudwatch_local_service.routes.query import _apply_filter


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

    def test_in_match(self):
        event = {"message": "error", "@timestamp": "2024-01-01"}
        filters = [("in", "@message", ["error", "warn", "info"])]
        self.assertTrue(_apply_filter(event, filters))

    def test_in_no_match(self):
        event = {"message": "debug", "@timestamp": "2024-01-01"}
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


if __name__ == "__main__":
    unittest.main()
