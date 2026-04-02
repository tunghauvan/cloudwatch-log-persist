"""Tests for metrics endpoint"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_format_prometheus_metric():
    """Test Prometheus metric formatting"""
    from cloudwatch_local_service.routes.metrics import format_prometheus_metric

    result = format_prometheus_metric("test_metric", 42, "Test description", "gauge")

    expected_lines = [
        "# HELP test_metric Test description",
        "# TYPE test_metric gauge",
        "test_metric 42",
    ]

    for line in expected_lines:
        assert line in result, f"Expected line '{line}' not found in result"

    print("✓ format_prometheus_metric test passed")


def test_format_without_help():
    """Test metric formatting without help text"""
    from cloudwatch_local_service.routes.metrics import format_prometheus_metric

    result = format_prometheus_metric("simple_metric", 100)

    assert "# TYPE simple_metric gauge" in result
    assert "simple_metric 100" in result
    assert "# HELP" not in result

    print("✓ format_without_help test passed")


def test_counter_metric():
    """Test counter type metric"""
    from cloudwatch_local_service.routes.metrics import format_prometheus_metric

    result = format_prometheus_metric(
        "requests_total", 999, "Total requests", "counter"
    )

    assert "# TYPE requests_total counter" in result
    assert "requests_total 999" in result

    print("✓ counter_metric test passed")


if __name__ == "__main__":
    test_format_prometheus_metric()
    test_format_without_help()
    test_counter_metric()
    print("\n✅ All metrics tests passed!")
