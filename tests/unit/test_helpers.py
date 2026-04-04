"""Unit tests for service/utils/helpers.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest
from service.utils.helpers import (
    get_log_group_key,
    parse_filter_pattern,
    format_log_event,
)


# ---------------------------------------------------------------------------
# get_log_group_key
# ---------------------------------------------------------------------------

def test_get_log_group_key_basic():
    assert get_log_group_key("/app/server", "stream-1") == "/app/server/stream-1"


def test_get_log_group_key_with_slashes():
    assert get_log_group_key("/a/b/c", "x") == "/a/b/c/x"


# ---------------------------------------------------------------------------
# parse_filter_pattern — pass-through cases
# ---------------------------------------------------------------------------

def test_empty_pattern_matches_everything():
    assert parse_filter_pattern("", "any message") is True
    assert parse_filter_pattern("   ", "any message") is True
    assert parse_filter_pattern(None, "any message") is True


def test_all_pattern_matches_everything():
    assert parse_filter_pattern("ALL", "any message") is True
    assert parse_filter_pattern("ALL", "") is True


# ---------------------------------------------------------------------------
# parse_filter_pattern — substring (default)
# ---------------------------------------------------------------------------

def test_substring_match_case_insensitive():
    assert parse_filter_pattern("error", "ERROR: disk full") is True
    assert parse_filter_pattern("ERROR", "error: disk full") is True


def test_substring_no_match():
    assert parse_filter_pattern("critical", "info: started") is False


# ---------------------------------------------------------------------------
# parse_filter_pattern — NOT pattern
# ---------------------------------------------------------------------------

def test_not_pattern_excludes_matching():
    assert parse_filter_pattern("!error", "info: all good") is True
    assert parse_filter_pattern("!error", "error: something bad") is False


# ---------------------------------------------------------------------------
# parse_filter_pattern — like regex
# ---------------------------------------------------------------------------

def test_like_pattern_with_regex():
    assert parse_filter_pattern("field like /err.r/", "fatal error occurred") is True
    assert parse_filter_pattern("field like /^info/", "info: started") is True
    assert parse_filter_pattern("field like /^info/", "error: failed") is False


# ---------------------------------------------------------------------------
# parse_filter_pattern — JSON pattern [...]
# ---------------------------------------------------------------------------

def test_json_pattern_empty_brackets_matches_all():
    assert parse_filter_pattern("[]", "anything") is True


def test_json_pattern_key_check():
    msg = '{"service": "api", "level": "error"}'
    assert parse_filter_pattern('[service="api"]', msg) is True
    # Key not present → no match
    assert parse_filter_pattern('[region="us-east"]', msg) is False


# ---------------------------------------------------------------------------
# format_log_event
# ---------------------------------------------------------------------------

def test_format_log_event_maps_fields():
    event = {"timestamp": 1000, "message": "hello", "ingestionTime": 2000}
    result = format_log_event(event, "/group", "stream-1")
    assert result["timestamp"] == 1000
    assert result["message"] == "hello"
    assert result["ingestionTime"] == 2000
    assert result["logGroupName"] == "/group"
    assert result["logStreamName"] == "stream-1"


def test_format_log_event_missing_fields():
    result = format_log_event({}, "/g", "s")
    assert result["timestamp"] is None
    assert result["message"] is None
    assert result["logGroupName"] == "/g"
    assert result["logStreamName"] == "s"
