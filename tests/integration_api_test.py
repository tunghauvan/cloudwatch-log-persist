import requests
import time
import json
import subprocess
import unittest
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

class TestLogIntegration(unittest.TestCase):
    BASE_URL = "http://localhost:4588"
    LOG_GROUP = "test-integration-group"
    LOG_STREAM = "test-stream-01"

    @classmethod
    def setUpClass(cls):
        """Initialize boto3 CloudWatch client pointing to local endpoint"""
        cls.client = boto3.client(
            'logs',
            endpoint_url=cls.BASE_URL,
            region_name='ap-southeast-1',
            aws_access_key_id='testing',
            aws_secret_access_key='testing'
        )

    def setUp(self):
        # Flush buffer before each test to ensure clean state if needed
        requests.post(f"{self.BASE_URL}/flush")

    def test_01_cloudwatch_put_get_events_boto3(self):
        print("\n--- Testing CloudWatch Put/Get Log Events (boto3) ---")
        now_ms = int(time.time() * 1000)
        test_message = f"Integration test message at {datetime.now()}"
        
        # 1. PutLogEvents using boto3
        try:
            response = self.client.put_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': now_ms,
                        'message': test_message
                    }
                ]
            )
            print(f"PutLogEvents: OK (nextSequenceToken: {response.get('nextSequenceToken')})")
        except ClientError as e:
            self.fail(f"PutLogEvents failed: {e}")

        # 2. Flush to warehouse
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1) # Wait for storage

        # 3. GetLogEvents and Verify Timestamp
        try:
            response = self.client.get_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                startTime=now_ms - 5000,
                endTime=now_ms + 5000
            )
        except ClientError as e:
            self.fail(f"GetLogEvents failed: {e}")
        
        found = False
        for event in response.get("events", []):
            if event["message"] == test_message:
                found = True
                ts = event["timestamp"]
                print(f"Current TS from API: {ts}")
                
                # Check with tolerance for precision
                self.assertLessEqual(abs(ts - now_ms), 1000, f"Timestamp returned {ts} does not match original {now_ms}")
                print(f"Verified Timestamp: {ts} (OK)")
        
        self.assertTrue(found, "Log message not found in GetLogEvents")

    def test_02_loki_push_query_range(self):
        print("\n--- Testing Loki Push/Query Range ---")
        now_ns = int(time.time() * 1e9)
        test_message = f"Loki integration test message {now_ns}"
        app_label = "test-loki-app"
        
        # 1. Loki Push
        payload = {
            "streams": [
                {
                    "stream": {"app": app_label, "env": "integration"},
                    "values": [
                        [str(now_ns), test_message]
                    ]
                }
            ]
        }
        res = requests.post(f"{self.BASE_URL}/loki/api/v1/push", json=payload)
        self.assertEqual(res.status_code, 200)
        print("Loki Push: OK")

        # 2. Flush to warehouse
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)

        # 3. Loki Query Range
        # Note: start/end in Loki are usually ns
        start_ns = now_ns - int(10e9) # 10s ago
        end_ns = now_ns + int(10e9)
        query = f'{{app="{app_label}"}}'
        
        params = {
            "query": query,
            "start": start_ns,
            "end": end_ns
        }
        res = requests.get(f"{self.BASE_URL}/loki/api/v1/query_range", params=params)
        self.assertEqual(res.status_code, 200)
        data = res.json()
        
        found = False
        for stream in data.get("data", {}).get("result", []):
            for val in stream.get("values", []):
                ts_ns_str, msg = val
                if msg == test_message:
                    found = True
                    # CRITICAL: Verify timestamp is in NS (19 digits)
                    self.assertEqual(len(ts_ns_str), 19, "Loki timestamp should be 19 digits (ns)")
                    # The value should be close to now_ns (within precision conversion limits)
                    # We store in us, so ns = us * 1000. 
                    self.assertLessEqual(abs(int(ts_ns_str) - now_ns), 2000000)
                    print(f"Verified Loki Timestamp: {ts_ns_str} ns (OK)")

        self.assertTrue(found, "Log message not found in Loki query_range")

    def test_03_query_range_boundary_boto3(self):
        print("\n--- Testing Query Range Boundary (boto3) ---")
        # Test if query filters out data outside range
        base_ts_ms = int(time.time() * 1000)
        boundary_group = "boundary-test"
        boundary_stream = "s1"
        
        # Log 1: Inside range
        # Log 2: Outside range (future)
        try:
            self.client.put_log_events(
                logGroupName=boundary_group,
                logStreamName=boundary_stream,
                logEvents=[
                    {"timestamp": base_ts_ms, "message": "INSIDE"},
                    {"timestamp": base_ts_ms + 10000, "message": "OUTSIDE"}
                ]
            )
        except ClientError as e:
            self.fail(f"PutLogEvents for boundary test failed: {e}")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)

        # Query only for the first log
        try:
            response = self.client.get_log_events(
                logGroupName=boundary_group,
                logStreamName=boundary_stream,
                startTime=base_ts_ms - 1000,
                endTime=base_ts_ms + 1000
            )
        except ClientError as e:
            self.fail(f"GetLogEvents for boundary test failed: {e}")
        
        events = response.get("events", [])
        messages = [e["message"] for e in events]
        
        self.assertIn("INSIDE", messages)
        self.assertNotIn("OUTSIDE", messages)
        print("Query Range Filtering: OK")

    def test_04_describe_log_groups_boto3(self):
        print("\n--- Testing Describe Log Groups (boto3) ---")
        
        # First, put some events to ensure group exists
        now_ms = int(time.time() * 1000)
        try:
            self.client.put_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                logEvents=[
                    {
                        'timestamp': now_ms,
                        'message': 'Test message for describe'
                    }
                ]
            )
        except ClientError as e:
            self.fail(f"PutLogEvents failed: {e}")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)
        
        # Describe log groups
        try:
            response = self.client.describe_log_groups()
        except ClientError as e:
            self.fail(f"DescribeLogGroups failed: {e}")
        
        log_groups = response.get('logGroups', [])
        print(f"Total log groups: {len(log_groups)}")
        
        # Verify test group exists
        found_group = None
        for group in log_groups:
            print(f"  - {group.get('logGroupName')} (streams: {group.get('logStreamCount', 0)})")
            if group.get('logGroupName') == self.LOG_GROUP:
                found_group = group
        
        self.assertIsNotNone(found_group, f"Log group '{self.LOG_GROUP}' not found")
        print(f"Describe Log Groups: OK (found '{self.LOG_GROUP}')")

    def test_05_describe_log_streams_boto3(self):
        print("\n--- Testing Describe Log Streams (boto3) ---")
        
        # Describe log streams for test group
        try:
            response = self.client.describe_log_streams(logGroupName=self.LOG_GROUP)
        except ClientError as e:
            self.fail(f"DescribeLogStreams failed: {e}")
        
        log_streams = response.get('logStreams', [])
        print(f"Total log streams in '{self.LOG_GROUP}': {len(log_streams)}")
        
        for stream in log_streams:
            stream_name = stream.get('logStreamName')
            event_count = stream.get('storedBytes', 0)
            print(f"  - {stream_name} (bytes: {event_count})")
        
        # Verify test stream exists
        found_stream = None
        for stream in log_streams:
            if stream.get('logStreamName') == self.LOG_STREAM:
                found_stream = stream
        
        self.assertIsNotNone(found_stream, f"Log stream '{self.LOG_STREAM}' not found")
        print(f"Describe Log Streams: OK (found '{self.LOG_STREAM}'")

if __name__ == "__main__":
    unittest.main()
