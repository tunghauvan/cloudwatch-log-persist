"""Unit tests for LogStore — in-memory per-stream event store."""
import sys
from pathlib import Path
import time
import threading

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest
from service.services.log_store import LogStore


@pytest.fixture
def store():
    return LogStore()


# ---------------------------------------------------------------------------
# add_log_group
# ---------------------------------------------------------------------------

def test_add_creates_group_and_returns_sequence(store):
    seq = store.add_log_group(
        "/app/server", "stream-1",
        [{"timestamp": 1000, "message": "hello"}],
        ingestion_time=9000,
    )
    assert seq == 2  # initial=1, +1 event


def test_add_increments_sequence_on_second_call(store):
    store.add_log_group("/g", "s", [{"timestamp": 1, "message": "a"}], ingestion_time=0)
    seq2 = store.add_log_group("/g", "s", [{"timestamp": 2, "message": "b"}, {"timestamp": 3, "message": "c"}], ingestion_time=0)
    assert seq2 == 4  # 1 (init) + 1 (first batch) + 2 (second batch)


def test_add_multiple_streams_are_independent(store):
    store.add_log_group("/g", "stream-a", [{"timestamp": 1, "message": "a"}], ingestion_time=0)
    store.add_log_group("/g", "stream-b", [{"timestamp": 2, "message": "b"}], ingestion_time=0)
    assert store.get_sequence_token("/g/stream-a") == "2"
    assert store.get_sequence_token("/g/stream-b") == "2"


# ---------------------------------------------------------------------------
# get_sequence_token
# ---------------------------------------------------------------------------

def test_get_sequence_token_unknown_returns_1(store):
    assert store.get_sequence_token("/missing/stream") == "1"


def test_get_sequence_token_after_add(store):
    store.add_log_group("/g", "s", [{"timestamp": 1, "message": "x"}, {"timestamp": 2, "message": "y"}], ingestion_time=0)
    assert store.get_sequence_token("/g/s") == "3"


# ---------------------------------------------------------------------------
# get_events
# ---------------------------------------------------------------------------

def test_get_events_returns_all_for_group(store):
    store.add_log_group("/g", "s1", [{"timestamp": 100, "message": "a"}], ingestion_time=0)
    store.add_log_group("/g", "s2", [{"timestamp": 200, "message": "b"}], ingestion_time=0)
    events = store.get_events("/g")
    assert len(events) == 2
    assert events[0]["timestamp"] == 100  # sorted ascending


def test_get_events_filters_by_stream(store):
    store.add_log_group("/g", "s1", [{"timestamp": 1, "message": "a"}], ingestion_time=0)
    store.add_log_group("/g", "s2", [{"timestamp": 2, "message": "b"}], ingestion_time=0)
    events = store.get_events("/g", log_stream_name="s1")
    assert len(events) == 1
    assert events[0]["message"] == "a"


def test_get_events_start_time_filter(store):
    base = 1_700_000_000_000
    store.add_log_group(
        "/g", "s",
        [{"timestamp": base + i * 1000, "message": f"m{i}"} for i in range(5)],
        ingestion_time=0,
    )
    events = store.get_events("/g", start_time=base + 2000)
    assert all(e["timestamp"] >= base + 2000 for e in events)
    assert len(events) == 3


def test_get_events_end_time_filter(store):
    base = 1_700_000_000_000
    store.add_log_group(
        "/g", "s",
        [{"timestamp": base + i * 1000, "message": f"m{i}"} for i in range(5)],
        ingestion_time=0,
    )
    events = store.get_events("/g", end_time=base + 2000)
    assert all(e["timestamp"] <= base + 2000 for e in events)
    assert len(events) == 3


def test_get_events_limit(store):
    store.add_log_group(
        "/g", "s",
        [{"timestamp": i, "message": f"m{i}"} for i in range(20)],
        ingestion_time=0,
    )
    events = store.get_events("/g", limit=5)
    assert len(events) == 5


def test_get_events_returns_empty_for_unknown_group(store):
    assert store.get_events("/nonexistent") == []


# ---------------------------------------------------------------------------
# filter_events
# ---------------------------------------------------------------------------

def test_filter_events_no_pattern_returns_all(store):
    store.add_log_group("/g", "s", [{"timestamp": 1, "message": "hello world"}], ingestion_time=0)
    events = store.filter_events("/g")
    assert len(events) == 1


def test_filter_events_matches_substring(store):
    store.add_log_group(
        "/g", "s",
        [
            {"timestamp": 1, "message": "error: connection refused"},
            {"timestamp": 2, "message": "info: request ok"},
        ],
        ingestion_time=0,
    )
    events = store.filter_events("/g", filter_pattern="error")
    assert len(events) == 1
    assert "error" in events[0]["message"]


def test_filter_events_stream_prefix(store):
    store.add_log_group("/g", "api-1", [{"timestamp": 1, "message": "x"}], ingestion_time=0)
    store.add_log_group("/g", "worker-1", [{"timestamp": 2, "message": "y"}], ingestion_time=0)
    events = store.filter_events("/g", log_stream_name_prefix="api")
    assert len(events) == 1
    assert events[0]["message"] == "x"


# ---------------------------------------------------------------------------
# get_all_log_groups / get_log_streams
# ---------------------------------------------------------------------------

def test_get_all_log_groups(store):
    store.add_log_group("/app/api", "stream-1", [{"timestamp": 1, "message": "a"}], ingestion_time=99)
    store.add_log_group("/app/worker", "stream-1", [{"timestamp": 2, "message": "b"}], ingestion_time=99)
    groups = store.get_all_log_groups()
    assert "/app/api" in groups
    assert "/app/worker" in groups
    assert groups["/app/api"]["logGroupName"] == "/app/api"


def test_get_log_streams(store):
    store.add_log_group("/g", "s1", [{"timestamp": 1, "message": "a"}], ingestion_time=0)
    store.add_log_group("/g", "s2", [{"timestamp": 2, "message": "b"}], ingestion_time=0)
    streams = store.get_log_streams("/g")
    names = [s["logStreamName"] for s in streams]
    assert "s1" in names
    assert "s2" in names


def test_get_log_streams_empty_for_unknown_group(store):
    assert store.get_log_streams("/nonexistent") == []


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------

def test_clear_removes_all_data(store):
    store.add_log_group("/g", "s", [{"timestamp": 1, "message": "hi"}], ingestion_time=0)
    store.clear()
    assert store.get_events("/g") == []
    assert store.get_sequence_token("/g/s") == "1"


# ---------------------------------------------------------------------------
# thread-safety
# ---------------------------------------------------------------------------

def test_concurrent_adds_are_safe(store):
    errors = []

    def worker(i):
        try:
            store.add_log_group(
                "/g", f"stream-{i}",
                [{"timestamp": j, "message": f"t{i} m{j}"} for j in range(10)],
                ingestion_time=0,
            )
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # 10 streams × 10 events = 100 events total
    assert len(store.get_events("/g", limit=200)) == 100
