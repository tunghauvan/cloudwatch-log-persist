"""Unit tests for Loki route utility functions.

Tests functions that do not require a running Flask app:
  - parse_logql_filter
  - convert_timestamp_to_ns
  - is_metric_query
  - _parse_step_seconds
  - _classified_level
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest

# Import functions without triggering Flask app creation
from service.routes.loki import (
    parse_logql_filter,
    convert_timestamp_to_ns,
    is_metric_query,
    _parse_step_seconds,
    _classified_level,
)


# ===========================================================================
# parse_logql_filter
# ===========================================================================

class TestParseLogqlFilter:
    def test_none_returns_empty(self):
        lg, ls, mf, lf, rl = parse_logql_filter(None)
        assert lg is None
        assert ls is None
        assert mf is None
        assert lf == {}
        assert rl is None

    def test_empty_string_returns_empty(self):
        lg, ls, mf, lf, rl = parse_logql_filter("")
        assert lg is None and ls is None and lf == {}

    def test_simple_app_label(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{app="myapp"}')
        assert lf == {"app": "myapp"}
        assert lg is None

    def test_log_group_extracted(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{log_group="/prod/api"}')
        assert lg == "/prod/api"
        assert "log_group" not in lf

    def test_log_stream_extracted(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{log_stream="stream-01"}')
        assert ls == "stream-01"

    def test_multiple_labels(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{app="api", env="prod"}')
        assert lf.get("app") == "api"
        assert lf.get("env") == "prod"

    def test_message_filter_pipe_equals(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{app="api"} |= "error"')
        assert mf == "error"

    def test_message_filter_pipe_regex(self):
        # |~ preserves the operator prefix in the extracted filter string
        lg, ls, mf, lf, rl = parse_logql_filter('{app="api"} |~ "err.*"')
        assert mf is not None
        assert "err.*" in mf

    def test_regex_match_any_sets_regex_label(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{app=~".+"}')
        assert rl == "app"
        assert "app" not in lf  # not added as filter

    def test_detected_level_pipe_filter(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{app="x"} | detected_level="error"')
        assert mf and "error" in mf

    def test_log_group_name_alias(self):
        lg, ls, mf, lf, rl = parse_logql_filter('{log_group_name="/my/group"}')
        assert lg == "/my/group"


# ===========================================================================
# convert_timestamp_to_ns
# ===========================================================================

class TestConvertTimestampToNs:
    def test_nanoseconds_passthrough(self):
        ts = 1_700_000_000_000_000_000  # already ns
        assert convert_timestamp_to_ns(ts) == str(ts)

    def test_microseconds_to_ns(self):
        ts = 1_700_000_000_000_000  # µs
        result = convert_timestamp_to_ns(ts)
        assert result == str(ts * 1000)

    def test_milliseconds_to_ns(self):
        ts = 1_700_000_000_000  # ms
        result = convert_timestamp_to_ns(ts)
        assert result == str(ts * 1_000_000)

    def test_seconds_to_ns(self):
        ts = 1_700_000_000  # s
        result = convert_timestamp_to_ns(ts)
        assert result == str(ts * 1_000_000_000)

    def test_string_input_treated_as_microseconds(self):
        # str input falls into the `else` branch: int(str) * 1_000_000 (µs → ns)
        result = convert_timestamp_to_ns("1000")
        assert result == str(1000 * 1_000_000)


# ===========================================================================
# is_metric_query
# ===========================================================================

class TestIsMetricQuery:
    def test_count_over_time(self):
        assert is_metric_query('count_over_time({app="x"}[5m])') is True

    def test_rate(self):
        assert is_metric_query('rate({app="x"}[1m])') is True

    def test_sum_by(self):
        assert is_metric_query('sum by (level) (count_over_time({app="x"}[5s]))') is True

    def test_plain_log_stream(self):
        assert is_metric_query('{app="x"}') is False

    def test_log_stream_with_filter(self):
        assert is_metric_query('{app="x"} |= "error"') is False

    def test_bytes_over_time(self):
        assert is_metric_query('bytes_over_time({app="x"}[5m])') is True

    def test_avg(self):
        assert is_metric_query('avg(rate({app="x"}[5m]))') is True

    def test_empty_string(self):
        assert is_metric_query("") is False


# ===========================================================================
# _parse_step_seconds
# ===========================================================================

class TestParseStepSeconds:
    START = 1_700_000_000_000
    END   = START + 3_600_000  # 1 hour range

    def test_seconds_suffix(self):
        assert _parse_step_seconds("30s", self.START, self.END) == 30

    def test_minutes_suffix(self):
        assert _parse_step_seconds("5m", self.START, self.END) == 300

    def test_hours_suffix(self):
        assert _parse_step_seconds("1h", self.START, self.END) == 3600

    def test_milliseconds_suffix(self):
        # 5000ms → 5s
        assert _parse_step_seconds("5000ms", self.START, self.END) == 5

    def test_plain_integer_string(self):
        assert _parse_step_seconds("60", self.START, self.END) == 60

    def test_empty_string_auto_calculates(self):
        # 3600s range / 500 max_data_points = 7s
        result = _parse_step_seconds("", self.START, self.END, max_data_points=500)
        assert result >= 1

    def test_none_auto_calculates(self):
        result = _parse_step_seconds(None, self.START, self.END)
        assert result >= 1

    def test_minimum_is_1(self):
        assert _parse_step_seconds("0s", self.START, self.END) == 1


# ===========================================================================
# _classified_level
# ===========================================================================

class TestClassifiedLevel:
    def test_error(self):
        assert _classified_level("error: connection refused") == "error"

    def test_exception(self):
        assert _classified_level("java.lang.nullpointerexception at line 42") == "error"

    def test_fatal(self):
        assert _classified_level("fatal: out of memory") == "error"

    def test_warn(self):
        assert _classified_level("warn: retrying request") == "warn"

    def test_debug(self):
        assert _classified_level("debug: opening socket") == "debug"

    def test_info(self):
        assert _classified_level("info: server started on port 8080") == "info"

    def test_unknown(self):
        assert _classified_level("just some random text") == "unknown"

    def test_caller_must_lowercase_before_calling(self):
        # _classified_level expects an already-lowercased string
        assert _classified_level("error happened") == "error"  # lowercase → detected
        assert _classified_level("ERROR happened") == "unknown"  # uppercase → not detected
