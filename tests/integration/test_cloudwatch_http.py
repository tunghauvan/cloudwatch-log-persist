import os
import sys
import time
import pytest
import random
import string
import requests
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.config import Config

CLOUDWATCH_ENDPOINT = os.getenv(
    "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "http://localhost:4588"
)
BUFFER_FLUSH_URL = f"{CLOUDWATCH_ENDPOINT}/flush"
LOG_GROUP_PREFIX = "/test/integration"


def flush_buffer():
    try:
        requests.post(BUFFER_FLUSH_URL, timeout=5)
    except Exception:
        pass


def get_cloudwatch_client():
    return boto3.client(
        "logs",
        endpoint_url=CLOUDWATCH_ENDPOINT,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
    )


def random_suffix(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def create_test_log_group(client) -> tuple[str, str]:
    log_group = f"{LOG_GROUP_PREFIX}/{random_suffix()}"
    log_stream = f"stream-{random_suffix()}"
    return log_group, log_stream


class TestCloudWatchIntegration:
    @classmethod
    def setup_class(cls):
        cls.client = get_cloudwatch_client()
        cls.test_resources = []

    @classmethod
    def teardown_class(cls):
        pass

    def test_health_endpoint(self):
        import requests

        response = requests.get(CLOUDWATCH_ENDPOINT)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    def test_put_and_get_log_events(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        log_events = [
            {"timestamp": timestamp, "message": "Test log message 1"},
            {"timestamp": timestamp + 1, "message": "Test log message 2"},
            {"timestamp": timestamp + 2, "message": "ERROR: Something went wrong"},
        ]

        response = self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=log_events,
        )
        assert "nextSequenceToken" in response

        flush_buffer()

        response = self.client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            limit=10,
        )
        events = response.get("events", [])
        assert len(events) == 3
        messages = [e["message"] for e in events]
        assert "Test log message 1" in messages
        assert "Test log message 2" in messages
        assert "ERROR: Something went wrong" in messages

    def test_put_multiple_batches(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        batch1 = [
            {"timestamp": timestamp + i, "message": f"Batch 1 message {i}"}
            for i in range(5)
        ]
        batch2 = [
            {"timestamp": timestamp + 5 + i, "message": f"Batch 2 message {i}"}
            for i in range(5)
        ]

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=batch1,
        )

        flush_buffer()

        response = self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=batch2,
        )
        assert "nextSequenceToken" in response

        flush_buffer()

        response = self.client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            limit=100,
        )
        events = response.get("events", [])
        assert len(events) == 10

    def test_describe_log_groups(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "message": "Test message",
                }
            ],
        )
        flush_buffer()

        response = self.client.describe_log_groups()
        log_groups = response.get("logGroups", [])
        assert len(log_groups) > 0

        group_names = [g["logGroupName"] for g in log_groups]
        assert log_group in group_names

    def test_describe_log_streams(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "message": "Test message",
                }
            ],
        )
        flush_buffer()

        response = self.client.describe_log_streams(logGroupName=log_group)
        streams = response.get("logStreams", [])
        stream_names = [s["logStreamName"] for s in streams]
        assert log_stream in stream_names

    def test_filter_log_events(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        log_events = [
            {"timestamp": timestamp, "message": "INFO: All systems operational"},
            {"timestamp": timestamp + 1, "message": "WARN: Low memory"},
            {"timestamp": timestamp + 2, "message": "ERROR: Connection failed"},
            {"timestamp": timestamp + 3, "message": "INFO: Request processed"},
        ]

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=log_events,
        )
        flush_buffer()

        response = self.client.filter_log_events(
            logGroupName=log_group,
            filterPattern="ERROR",
        )
        events = response.get("events", [])
        assert len(events) == 1
        assert "ERROR" in events[0]["message"]

        response = self.client.filter_log_events(
            logGroupName=log_group,
            filterPattern="INFO",
        )
        events = response.get("events", [])
        assert len(events) == 2

    def test_start_query_and_get_results(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        log_events = [
            {"timestamp": timestamp + i, "message": f"Log message {i}"}
            for i in range(10)
        ]

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {"timestamp": timestamp + i, "message": f"Log message {i}"}
                for i in range(10)
            ],
        )
        flush_buffer()

        time.sleep(0.5)

        end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_time = end_time - (60 * 60 * 1000)

        response = self.client.start_query(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            queryString="fields @timestamp, @message | limit 5",
        )
        assert "queryId" in response
        query_id = response["queryId"]

        max_wait = 10
        for _ in range(max_wait * 10):
            result = self.client.get_query_results(queryId=query_id)
            status = result.get("status")
            if status in ["Complete", "Failed", "Cancelled"]:
                break
            time.sleep(0.1)

        assert result["status"] == "Complete"
        assert "results" in result

    def test_get_log_events_with_time_range(self):
        log_group, log_stream = create_test_log_group(self.client)
        self.test_resources.append((log_group, log_stream))

        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        log_events = [
            {"timestamp": now - 10000, "message": "Old message"},
            {"timestamp": now - 5000, "message": "Middle message"},
            {"timestamp": now, "message": "Current message"},
        ]

        self.client.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=log_events,
        )
        flush_buffer()

        response = self.client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            startTime=now - 6000,
            endTime=now + 1000,
            limit=100,
        )
        events = response.get("events", [])
        assert len(events) == 2
        messages = [e["message"] for e in events]
        assert "Old message" not in messages
        assert "Middle message" in messages
        assert "Current message" in messages

    def test_missing_log_group_name(self):
        with pytest.raises(Exception):
            self.client.get_log_events(
                logGroupName="",
                logStreamName="stream",
            )

    def test_missing_log_stream_name(self):
        with pytest.raises(Exception):
            self.client.get_log_events(
                logGroupName="/test/group",
                logStreamName="",
            )


class TestCloudWatchQueryIntegration:
    @classmethod
    def setup_class(cls):
        cls.client = get_cloudwatch_client()

    def test_stats_query(self):
        log_group = f"{LOG_GROUP_PREFIX}/stats-{random_suffix()}"
        log_stream = f"stream-{random_suffix()}"

        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        for i in range(20):
            msg_type = random.choice(["INFO", "WARN", "ERROR"])
            self.client.put_log_events(
                logGroupName=log_group,
                logStreamName=log_stream,
                logEvents=[
                    {
                        "timestamp": timestamp + i * 100,
                        "message": f"{msg_type}: Message {i}",
                    }
                ],
            )

        flush_buffer()
        time.sleep(1)

        end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_time = end_time - (60 * 60 * 1000)

        response = self.client.start_query(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            queryString="stats count() by bin(5m)",
        )
        assert "queryId" in response


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
