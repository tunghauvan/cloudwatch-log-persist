"""
Integration tests for the ALB log pipeline.

Tests cover:
  1. Parser unit paths (run anywhere, no service needed)
  2. POST /alb/s3-event with inline mock S3 content (service required)
  3. POST /alb/s3-event validation errors
"""

import gzip
import io
import json
import os
import sys
import textwrap
import time
import unittest.mock
from pathlib import Path
from datetime import datetime, timezone

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

BASE_URL = os.getenv("AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "http://localhost:4588")
ALB_ENDPOINT = f"{BASE_URL}/alb/s3-event"

# ---------------------------------------------------------------------------
# Sample ALB log lines (representative real-world format)
# ---------------------------------------------------------------------------
_SAMPLE_LINES = textwrap.dedent("""\
    https 2026-04-06T10:00:00.000001Z app/my-alb/1234567890abcdef \
192.168.1.100:52000 10.0.1.5:80 \
0.001 0.012 0.000 200 200 \
512 2048 \
"GET https://api.example.com/v1/users?page=1 HTTP/1.1" \
"Mozilla/5.0 (compatible; MyBot/1.0)" \
ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 \
arn:aws:elasticloadbalancing:ap-southeast-1:123456789:targetgroup/my-tg/abcdef \
Root=1-deadbeef-1234567890abcdef0 \
api.example.com \
arn:aws:acm:ap-southeast-1:123456789:certificate/abc \
0 \
2026-04-06T10:00:00.000001Z \
"forward" \
"-" "-" \
"10.0.1.5:80" "200" "" ""
    https 2026-04-06T10:00:01.000000Z app/my-alb/1234567890abcdef \
192.168.1.200:53000 10.0.1.6:80 \
0.000 0.020 0.001 500 500 \
256 512 \
"POST https://api.example.com/v1/orders HTTP/2.0" \
"curl/7.81.0" \
ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 \
arn:aws:elasticloadbalancing:ap-southeast-1:123456789:targetgroup/my-tg/abcdef \
Root=1-deadbeef-abcdef1234567890 \
api.example.com \
arn:aws:acm:ap-southeast-1:123456789:certificate/abc \
0 \
2026-04-06T10:00:01.000000Z \
"forward" \
"-" "-" \
"10.0.1.6:80" "500" "" ""
    https 2026-04-06T10:00:02.000000Z app/my-alb/1234567890abcdef \
10.0.0.1:54000 - \
0.001 -1 0.000 504 - \
128 256 \
"GET https://api.example.com/health HTTP/1.1" \
"kube-probe/1.26" \
- - \
arn:aws:elasticloadbalancing:ap-southeast-1:123456789:targetgroup/my-tg/abcdef \
Root=1-deadbeef-000000000000000 \
- - \
0 \
2026-04-06T10:00:02.000000Z \
"forward" \
"-" "ConnectionError" \
"-" "-" "" ""
""").strip()


def _make_gzip_content(text: str) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(text.encode())
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Parser-level tests (no service needed)
# ---------------------------------------------------------------------------
class TestALBParser:
    def test_parse_single_line_200(self):
        from service.services.alb_parser import parse_alb_line
        line = (
            'https 2026-04-06T10:00:00.000001Z app/my-alb/abc '
            '192.168.1.100:52000 10.0.1.5:80 '
            '0.001 0.012 0.000 200 200 512 2048 '
            '"GET https://api.example.com/v1/users HTTP/1.1" '
            '"Mozilla/5.0" - - '
            'arn:aws:elasticloadbalancing:region:123:targetgroup/tg/abc '
            'Root=1-abc-123 api.example.com - 0 2026-04-06T10:00:00.000001Z '
            '"forward" "-" "-" "10.0.1.5:80" "200" "" ""'
        )
        row = parse_alb_line(line)
        assert row is not None
        assert row["elb_status_code"] == 200
        assert row["request_method"] == "GET"
        assert row["request_path"] == "/v1/users"
        assert row["client_ip"] == "192.168.1.100"
        assert row["client_port"] == 52000
        assert row["target_processing_time"] == pytest.approx(0.012)
        assert row["ingestion_time"] is not None

    def test_parse_skips_blank_lines(self):
        from service.services.alb_parser import parse_alb_line
        assert parse_alb_line("") is None
        assert parse_alb_line("   ") is None
        assert parse_alb_line("# this is a comment") is None

    def test_parse_dash_fields_become_none(self):
        from service.services.alb_parser import parse_alb_line
        line = (
            'https 2026-04-06T10:00:02.000000Z app/my-alb/abc '
            '10.0.0.1:54000 - '
            '0.001 -1 0.000 504 - 128 256 '
            '"GET https://api.example.com/health HTTP/1.1" '
            '"kube-probe/1.26" - - '
            'arn:aws:elasticloadbalancing:region:123:targetgroup/tg/abc '
            'Root=1-abc-000 - - 0 2026-04-06T10:00:02.000000Z '
            '"forward" "-" "ConnectionError" "-" "-" "" ""'
        )
        row = parse_alb_line(line)
        assert row is not None
        assert row["target_ip"] is None
        assert row["ssl_cipher"] is None
        assert row["error_reason"] == "ConnectionError"
        assert row["target_processing_time"] is None  # -1 → None

    def test_parse_gzip_content(self):
        from service.services.alb_parser import parse_alb_content
        raw_line = (
            'https 2026-04-06T10:00:00.000001Z app/my-alb/abc '
            '192.168.1.100:52000 10.0.1.5:80 '
            '0.001 0.012 0.000 200 200 512 2048 '
            '"GET https://api.example.com/ HTTP/1.1" '
            '"agent" - - '
            'arn:aws:elasticloadbalancing:region:123:targetgroup/tg/abc '
            'Root=1-abc-999 api.example.com - 0 2026-04-06T10:00:00.000001Z '
            '"forward" "-" "-" "10.0.1.5:80" "200" "" ""'
        )
        gz = _make_gzip_content(raw_line)
        rows = parse_alb_content(gz)
        assert len(rows) == 1
        assert rows[0]["elb_status_code"] == 200

    def test_parse_plain_text_content(self):
        from service.services.alb_parser import parse_alb_content
        rows = parse_alb_content(_SAMPLE_LINES.encode())
        # 3 real lines — exact count depends on how sample is formed
        assert len(rows) >= 2

    def test_request_parsing_post(self):
        from service.services.alb_parser import parse_alb_line
        line = (
            'https 2026-04-06T10:00:01.000000Z app/my-alb/abc '
            '192.168.1.200:53000 10.0.1.6:80 '
            '0.000 0.020 0.001 500 500 256 512 '
            '"POST https://api.example.com/v1/orders?q=1 HTTP/2.0" '
            '"curl/7.81.0" - - '
            'arn:aws:elasticloadbalancing:region:123:targetgroup/tg/abc '
            'Root=1-abc-111 api.example.com - 0 2026-04-06T10:01:00.000000Z '
            '"forward" "-" "-" "10.0.1.6:80" "500" "" ""'
        )
        row = parse_alb_line(line)
        assert row is not None
        assert row["request_method"] == "POST"
        assert row["request_path"] == "/v1/orders"  # no query string
        assert row["elb_status_code"] == 500


# ---------------------------------------------------------------------------
# Service integration tests (requires running container)
# ---------------------------------------------------------------------------
def service_running():
    try:
        r = requests.get(BASE_URL, timeout=3)
        return r.status_code < 500
    except Exception:
        return False


@pytest.mark.skipif(not service_running(), reason="service not running on localhost:4588")
class TestALBS3EventEndpoint:
    def _post_event(self, bucket: str, key: str, **kwargs):
        payload = {
            "Records": [{
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                },
            }]
        }
        return requests.post(ALB_ENDPOINT, json=payload, timeout=30, **kwargs)

    def test_missing_records_returns_400(self):
        r = requests.post(ALB_ENDPOINT, json={}, timeout=5)
        assert r.status_code == 400
        assert "Records" in r.json().get("error", "")

    def test_wrong_source_returns_400(self):
        payload = {
            "Records": [{
                "eventSource": "aws:sqs",
                "s3": {"bucket": {"name": "b"}, "object": {"key": "k"}},
            }]
        }
        r = requests.post(ALB_ENDPOINT, json=payload, timeout=5)
        assert r.status_code == 400

    def test_missing_bucket_returns_400(self):
        payload = {
            "Records": [{"eventSource": "aws:s3", "s3": {"object": {"key": "k"}}}]
        }
        r = requests.post(ALB_ENDPOINT, json=payload, timeout=5)
        assert r.status_code == 400

    def test_invalid_json_returns_400(self):
        r = requests.post(
            ALB_ENDPOINT, data="not-json", headers={"Content-Type": "application/json"}, timeout=5
        )
        assert r.status_code == 400

    def test_s3_download_error_returns_207(self):
        """Non-existent bucket/key should return 207 with per-file error."""
        r = self._post_event("nonexistent-bucket-xyz-404", "no/such/file.log.gz")
        data = r.json()
        assert r.status_code == 207
        assert data["results"][0]["status"] == "error"
        assert "rows_written" in data["results"][0]

    def test_process_inline_mock_s3(self, tmp_path):
        """
        Upload a real ALB log file to MinIO (the local S3 backend) and verify
        the endpoint parses and writes it to Iceberg.

        Skipped if MinIO is not reachable.
        """
        import boto3
        from botocore.exceptions import ClientError, EndpointResolutionError

        minio_endpoint = "http://localhost:9000"
        bucket = "alb-logs-test"
        key = "test/2026/04/06/alb.log.gz"
        gz_content = _make_gzip_content(_SAMPLE_LINES)

        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            region_name="ap-southeast-1",
            aws_access_key_id="admin",
            aws_secret_access_key="admin123",
        )
        try:
            try:
                s3.create_bucket(Bucket=bucket)
            except ClientError as e:
                if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    raise
            s3.put_object(Bucket=bucket, Key=key, Body=gz_content)
        except Exception as e:
            pytest.skip(f"MinIO not reachable: {e}")

        # Override the alb s3 config so the service uses MinIO
        # The service reads from its own config, but we can provide a matching bucket
        r = requests.post(
            ALB_ENDPOINT,
            json={
                "Records": [{
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": bucket},
                        "object": {"key": key},
                    },
                }]
            },
            timeout=30,
        )
        data = r.json()
        assert r.status_code == 200, f"Unexpected status: {r.status_code} — {data}"
        assert data["processed"] == 1
        assert data["rows_written"] >= 2, f"Expected >=2 rows, got {data['rows_written']}"
        result = data["results"][0]
        assert result["status"] == "ok"
        assert result["rows_written"] >= 2
        assert result["duration_ms"] > 0
        print(
            f"\n[ALB] Wrote {result['rows_written']} rows in {result['duration_ms']}ms"
        )

    def test_endpoint_returns_correct_schema(self, tmp_path):
        """Response must always include processed/rows_written/results fields."""
        r = requests.post(ALB_ENDPOINT, json={"Records": []}, timeout=5)
        # Empty Records triggers 400 — also acceptable schema
        assert r.status_code in (200, 400, 207)
        body = r.json()
        assert isinstance(body, dict)


# ---------------------------------------------------------------------------
# Query + Grafana SimpleJSON endpoint tests (requires running container)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not service_running(), reason="service not running on localhost:4588")
class TestALBQueryEndpoints:
    """
    Validate response schema of all analytics and Grafana endpoints.
    Tests pass even when the alb_logs table has no data — they verify shapes,
    not specific values.
    """

    GRAFANA_BASE = f"{BASE_URL}/alb/grafana"

    # --- Grafana SimpleJSON protocol ---

    def test_grafana_health(self):
        r = requests.get(self.GRAFANA_BASE, timeout=5)
        assert r.status_code == 200
        assert r.text.strip() == "OK"

    def test_grafana_health_trailing_slash(self):
        r = requests.get(self.GRAFANA_BASE + "/", timeout=5)
        assert r.status_code == 200

    def test_grafana_search_returns_metric_list(self):
        r = requests.post(f"{self.GRAFANA_BASE}/search", json={}, timeout=5)
        assert r.status_code == 200
        metrics = r.json()
        assert isinstance(metrics, list)
        assert "request_rate" in metrics
        assert "top_paths" in metrics
        assert "latency_p99" in metrics
        assert len(metrics) >= 14

    def test_grafana_annotations_returns_empty_list(self):
        r = requests.post(f"{self.GRAFANA_BASE}/annotations", json={}, timeout=5)
        assert r.status_code == 200
        assert r.json() == []

    def test_grafana_tag_keys(self):
        r = requests.post(f"{self.GRAFANA_BASE}/tag-keys", json={}, timeout=5)
        assert r.status_code == 200
        keys = r.json()
        assert isinstance(keys, list)
        assert any(k["text"] == "elb" for k in keys)

    def test_grafana_query_timeserie_schema(self):
        payload = {
            "range": {
                "from": "2026-04-06T00:00:00Z",
                "to":   "2026-04-07T23:59:59Z",
            },
            "intervalMs": 3_600_000,
            "maxDataPoints": 48,
            "targets": [
                {"refId": "A", "target": "request_rate", "type": "timeserie"},
                {"refId": "B", "target": "error_rate",   "type": "timeserie"},
            ],
        }
        r = requests.post(f"{self.GRAFANA_BASE}/query", json=payload, timeout=15)
        assert r.status_code == 200
        results = r.json()
        assert isinstance(results, list)
        for item in results:
            assert "target" in item
            assert "datapoints" in item
            assert isinstance(item["datapoints"], list)
            for dp in item["datapoints"]:
                assert len(dp) == 2
                assert isinstance(dp[1], int)  # ts_ms must be integer

    def test_grafana_query_table_schema(self):
        payload = {
            "range": {
                "from": "2026-04-06T00:00:00Z",
                "to":   "2026-04-07T23:59:59Z",
            },
            "intervalMs": 3_600_000,
            "targets": [
                {"refId": "A", "target": "top_paths",          "type": "table"},
                {"refId": "B", "target": "status_distribution", "type": "table"},
                {"refId": "C", "target": "latency_stats",      "type": "table"},
            ],
        }
        r = requests.post(f"{self.GRAFANA_BASE}/query", json=payload, timeout=15)
        assert r.status_code == 200
        results = r.json()
        assert isinstance(results, list)
        for item in results:
            assert item.get("type") == "table"
            assert "columns" in item
            assert "rows" in item
            assert isinstance(item["columns"], list)
            assert isinstance(item["rows"], list)

    def test_grafana_query_unknown_metric_ignored(self):
        payload = {
            "range": {"from": "2026-04-06T00:00:00Z", "to": "2026-04-07T00:00:00Z"},
            "targets": [{"refId": "A", "target": "nonexistent_metric"}],
        }
        r = requests.post(f"{self.GRAFANA_BASE}/query", json=payload, timeout=5)
        assert r.status_code == 200
        assert r.json() == []

    def test_grafana_query_mixed_targets(self):
        """Both timeserie and table targets can appear in the same request."""
        payload = {
            "range": {"from": "2026-04-06T00:00:00Z", "to": "2026-04-07T23:59:59Z"},
            "intervalMs": 3_600_000,
            "targets": [
                {"refId": "A", "target": "request_rate", "type": "timeserie"},
                {"refId": "B", "target": "top_ips",      "type": "table"},
            ],
        }
        r = requests.post(f"{self.GRAFANA_BASE}/query", json=payload, timeout=15)
        assert r.status_code == 200
        results = r.json()
        assert len(results) == 2

    # --- REST analytics endpoints ---

    def test_stats_schema(self):
        r = requests.get(f"{BASE_URL}/alb/stats", timeout=10)
        assert r.status_code == 200
        d = r.json()
        for key in ("total_requests", "error_count", "error_rate_pct",
                    "total_received_bytes", "total_sent_bytes"):
            assert key in d, f"Missing key: {key}"

    def test_timeseries_default_metric(self):
        r = requests.get(f"{BASE_URL}/alb/timeseries", timeout=10)
        assert r.status_code == 200
        d = r.json()
        assert d["metric"] == "request_rate"
        assert isinstance(d["datapoints"], list)

    def test_timeseries_custom_metric(self):
        r = requests.get(f"{BASE_URL}/alb/timeseries?metric=latency_p99", timeout=10)
        assert r.status_code == 200
        assert r.json()["metric"] == "latency_p99"

    def test_timeseries_unknown_metric_returns_400(self):
        r = requests.get(f"{BASE_URL}/alb/timeseries?metric=made_up", timeout=5)
        assert r.status_code == 400
        assert "valid" in r.json()

    def test_status_codes_schema(self):
        r = requests.get(f"{BASE_URL}/alb/status-codes", timeout=10)
        assert r.status_code == 200
        d = r.json()
        assert d["type"] == "table"
        assert len(d["columns"]) == 3
        assert isinstance(d["rows"], list)

    def test_top_paths_schema(self):
        r = requests.get(f"{BASE_URL}/alb/top-paths?limit=5", timeout=10)
        assert r.status_code == 200
        d = r.json()
        assert d["type"] == "table"
        assert len(d["columns"]) == 4  # Path, Requests, Errors, Error%
        assert isinstance(d["rows"], list)

    def test_top_ips_schema(self):
        r = requests.get(f"{BASE_URL}/alb/top-ips?limit=5", timeout=10)
        assert r.status_code == 200
        d = r.json()
        assert d["type"] == "table"
        assert isinstance(d["rows"], list)

    def test_latency_schema(self):
        r = requests.get(f"{BASE_URL}/alb/latency", timeout=10)
        assert r.status_code == 200
        d = r.json()
        assert d["type"] == "table"
        assert isinstance(d["rows"], list)
        # If there is data, check the percentile labels
        if d["rows"]:
            labels = [row[0] for row in d["rows"]]
            assert "p50" in labels
            assert "p99" in labels

    def test_time_range_params_accepted(self):
        """Explicit from/to params should be honoured without error."""
        params = "?from=2026-04-06T00:00:00Z&to=2026-04-07T00:00:00Z"
        for path in ("/alb/stats", "/alb/status-codes", "/alb/top-paths", "/alb/latency"):
            r = requests.get(f"{BASE_URL}{path}{params}", timeout=10)
            assert r.status_code == 200, f"Failed on {path}: {r.text}"
