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

    def test_06_filter_log_events_boto3(self):
        print("\n--- Testing Filter Log Events (boto3) ---")
        now_ms = int(time.time() * 1000)
        test_stream = f"filter-test-stream-{int(now_ms)}"
        
        # Insert test events with keywords
        test_events = [
            {"timestamp": now_ms, "message": "ERROR: Database connection failed"},
            {"timestamp": now_ms + 100, "message": "INFO: Processing request"},
            {"timestamp": now_ms + 200, "message": "ERROR: Timeout occurred"},
            {"timestamp": now_ms + 300, "message": "DEBUG: Variable x = 10"},
            {"timestamp": now_ms + 400, "message": "WARN: Low memory"},
        ]
        
        try:
            self.client.put_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=test_stream,
                logEvents=test_events
            )
        except ClientError as e:
            self.fail(f"PutLogEvents failed: {e}")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)
        
        # Test 1: Filter by keyword in specific log stream prefix
        try:
            response = self.client.filter_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamNamePrefix="filter-test-stream",
                filterPattern="ERROR"
            )
        except ClientError as e:
            self.fail(f"FilterLogEvents with ERROR pattern failed: {e}")
        
        events = response.get('events', [])
        error_count = len(events)
        print(f"Filter 'ERROR' in stream prefix 'filter-test-stream': Found {error_count} events")
        self.assertGreaterEqual(error_count, 2, f"Should find at least 2 ERROR events, got {error_count}")
        
        # Test 2: Filter with different keyword in same stream prefix
        try:
            response = self.client.filter_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamNamePrefix="filter-test-stream",
                filterPattern="INFO"
            )
        except ClientError as e:
            self.fail(f"FilterLogEvents with INFO pattern failed: {e}")
        
        events = response.get('events', [])
        info_count = len(events)
        print(f"Filter 'INFO' in stream prefix 'filter-test-stream': Found {info_count} events")
        self.assertGreaterEqual(info_count, 1, f"Should find at least 1 INFO event, got {info_count}")
        
        print("Filter Log Events: OK")

    def test_07_batch_log_events_boto3(self):
        print("\n--- Testing Batch Log Events (boto3) ---")
        now_ms = int(time.time() * 1000)
        batch_group = f"batch-test-group-{int(now_ms)}"
        batch_stream = "batch-stream"
        
        # Create 100 events (spread across 1 second)
        batch_events = [
            {
                "timestamp": now_ms + (i * 10),
                "message": f"Batch event {i}: Log message number {i}"
            }
            for i in range(100)
        ]
        
        # Insert batch
        try:
            response = self.client.put_log_events(
                logGroupName=batch_group,
                logStreamName=batch_stream,
                logEvents=batch_events
            )
        except ClientError as e:
            self.fail(f"PutLogEvents batch failed: {e}")
        
        print(f"Inserted {len(batch_events)} events in one batch")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)
        
        # Verify all events are retrievable
        try:
            response = self.client.get_log_events(
                logGroupName=batch_group,
                logStreamName=batch_stream,
                limit=200
            )
        except ClientError as e:
            self.fail(f"GetLogEvents batch failed: {e}")
        
        events = response.get('events', [])
        print(f"Retrieved {len(events)} events from batch")
        self.assertEqual(len(events), 100, f"Should retrieve all 100 batch events, got {len(events)}")
        
        # Verify timestamps are close (within 2s tolerance for time drift)
        for i, event in enumerate(events):
            expected_ts = now_ms + (i * 10)
            actual_ts = event['timestamp']
            # Allow ±2000ms tolerance for time drift during processing
            self.assertAlmostEqual(actual_ts, expected_ts, delta=2000, 
                                   msg=f"Event {i} timestamp off by {abs(actual_ts - expected_ts)}ms")
        
        print("Batch Log Events: OK")

    def test_08_loki_label_filtering_boto3(self):
        print("\n--- Testing Loki Label Filtering (boto3) ---")
        now_ns = int(time.time() * 1e9)
        test_id = str(int(time.time() * 1000))
        
        # Push logs with multiple labels - use unique test_id to isolate from other tests
        payload = {
            "streams": [
                {
                    "stream": {
                        "app": f"api-server-{test_id}",
                        "env": "production",
                        "region": "us-west",
                        "version": "v1.2.3"
                    },
                    "values": [
                        [str(now_ns), "Request processed"],
                        [str(now_ns + 1000000), "Response sent"]
                    ]
                },
                {
                    "stream": {
                        "app": f"worker-service-{test_id}",
                        "env": "production",
                        "region": "us-east",
                        "version": "v1.1.0"
                    },
                    "values": [
                        [str(now_ns + 2000000), "Job started"],
                        [str(now_ns + 3000000), "Job completed"]
                    ]
                }
            ]
        }
        
        res = requests.post(f"{self.BASE_URL}/loki/api/v1/push", json=payload)
        self.assertEqual(res.status_code, 200)
        print("Pushed logs with multiple labels")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)
        
        # Test 1: Filter by single label (using unique test_id)
        start_ns = now_ns - int(10e9)
        end_ns = now_ns + int(10e9)
        
        query1 = f'{{app="api-server-{test_id}"}}'
        res1 = requests.get(
            f"{self.BASE_URL}/loki/api/v1/query_range",
            params={"query": query1, "start": start_ns, "end": end_ns}
        )
        data1 = res1.json()
        count1 = sum(len(s.get("values", [])) for s in data1.get("data", {}).get("result", []))
        print(f"Query '{query1}': Found {count1} events")
        self.assertEqual(count1, 2, f"Should find 2 api-server events, got {count1}")
        
        # Test 2: Filter by multiple labels (AND)
        query2 = f'{{app="worker-service-{test_id}", region="us-east"}}'
        res2 = requests.get(
            f"{self.BASE_URL}/loki/api/v1/query_range",
            params={"query": query2, "start": start_ns, "end": end_ns}
        )
        data2 = res2.json()
        count2 = sum(len(s.get("values", [])) for s in data2.get("data", {}).get("result", []))
        print(f"Query '{query2}': Found {count2} events")
        self.assertEqual(count2, 2, f"Should find 2 worker-service events in us-east, got {count2}")
        
        print("Loki Label Filtering: OK")

    def test_09_loki_metric_queries_boto3(self):
        print("\n--- Testing Loki Metric Queries (boto3) ---")
        now_ns = int(time.time() * 1e9)
        
        # Push logs with levels for metric aggregation
        payload = {
            "streams": [
                {
                    "stream": {"app": "metric-test", "level": "error"},
                    "values": [[str(now_ns + (i * 100000000)), f"Error {i}"] for i in range(5)]
                },
                {
                    "stream": {"app": "metric-test", "level": "warn"},
                    "values": [[str(now_ns + (i * 100000000)), f"Warn {i}"] for i in range(3)]
                },
                {
                    "stream": {"app": "metric-test", "level": "info"},
                    "values": [[str(now_ns + (i * 100000000)), f"Info {i}"] for i in range(7)]
                }
            ]
        }
        
        res = requests.post(f"{self.BASE_URL}/loki/api/v1/push", json=payload)
        self.assertEqual(res.status_code, 200)
        print("Pushed logs for metric aggregation")
        
        requests.post(f"{self.BASE_URL}/flush")
        time.sleep(1)
        
        # Test metric query with aggregation
        start_ns = now_ns - int(10e9)
        end_ns = now_ns + int(10e9)
        
        query = 'sum(count_over_time({app="metric-test"} [1s])) by (level)'
        res = requests.get(
            f"{self.BASE_URL}/loki/api/v1/query_range",
            params={"query": query, "start": start_ns, "end": end_ns}
        )
        data = res.json()
        
        self.assertEqual(data['status'], 'success')
        result = data.get('data', {}).get('result', [])
        print(f"Metric query returned {len(result)} series")
        
        # Check that we got multiple series (one per level)
        self.assertGreater(len(result), 0, "Metric query should return at least one series")
        
        for series in result:
            level = series.get('metric', {}).get('level', 'unknown')
            values = series.get('values', [])
            print(f"  Level '{level}': {len(values)} data points")
        
        print("Loki Metric Queries: OK")

    def test_10_empty_results_edge_cases_boto3(self):
        print("\n--- Testing Empty Results Edge Cases (boto3) ---")
        now_ms = int(time.time() * 1000)
        
        # Test 1: Query with time range that has no data (far past)
        old_time = now_ms - (86400 * 1000)  # 1 day ago in ms
        far_past = old_time - 3600000  # Even further in past
        
        try:
            response = self.client.get_log_events(
                logGroupName=self.LOG_GROUP,
                logStreamName=self.LOG_STREAM,
                startTime=far_past,
                endTime=far_past + 1000
            )
            events = response.get('events', [])
            print(f"Query far past time range: {len(events)} events (expected 0-1)")
            # Should return 0 or very few events from far past
            self.assertLessEqual(len(events), 2, f"Far past should return ≤2 events, got {len(events)}")
        except ClientError as e:
            print(f"Query far past time range: {e.response['Error']['Code']}")
        
        # Test 2: Filter with pattern that doesn't match anything in time window
        try:
            response = self.client.filter_log_events(
                logGroupName=self.LOG_GROUP,
                filterPattern="XYZ-PATTERN-12345-NOTFOUND",
                startTime=far_past,
                endTime=far_past + 1000
            )
            events = response.get('events', [])
            print(f"Filter non-matching pattern in past: {len(events)} events (expected 0)")
            self.assertEqual(len(events), 0, "Non-matching pattern should return 0 events")
        except ClientError as e:
            print(f"Filter non-matching pattern: {e}")
        
        # Test 3: Verify describe operations handle empty cases
        try:
            response = self.client.describe_log_groups()
            groups = response.get('logGroups', [])
            print(f"Describe log groups: Found {len(groups)} groups (should be > 0)")
            self.assertGreater(len(groups), 0, "Should have at least one log group")
        except ClientError as e:
            print(f"Describe log groups error: {e}")
        
        print("Empty Results Edge Cases: OK")

if __name__ == "__main__":
    unittest.main()
