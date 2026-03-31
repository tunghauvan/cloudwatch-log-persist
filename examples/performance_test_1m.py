#!/usr/bin/env python3
"""
Performance test: Send 1 million logs to CloudWatch Local
Tests throughput, latency, and resource consumption.
"""

import boto3
import time
import os
import psutil
import threading
import sys
from datetime import datetime

CLOUDWATCH_ENDPOINT = os.getenv("CLOUDWATCH_ENDPOINT", "http://localhost:4588")
TOTAL_LOGS = int(os.getenv("TOTAL_LOGS", "1000000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # CloudWatch limit is 10000 per call
LOG_GROUP = "/perf-test/large"
LOG_STREAM = "test-1m"

stats = {
    "logs_sent": 0,
    "logs_failed": 0,
    "batches_sent": 0,
    "batches_failed": 0,
    "start_time": None,
    "end_time": None,
    "errors": [],
}

process = psutil.Process()
initial_memory = process.memory_info().rss / 1024 / 1024


def get_memory_mb():
    return process.memory_info().rss / 1024 / 1024


def get_cpu_percent():
    return process.cpu_percent(interval=0.1)


class CloudWatchLogger:
    def __init__(self):
        self.client = boto3.client(
            "logs",
            endpoint_url=CLOUDWATCH_ENDPOINT,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

    def create_log_group_and_stream(self):
        try:
            self.client.create_log_group(logGroupNamePrefix=LOG_GROUP)
        except Exception as e:
            pass

        try:
            self.client.create_log_stream(
                logGroupName=LOG_GROUP, logStreamName=LOG_STREAM
            )
        except Exception as e:
            pass

    def send_logs(self, events):
        try:
            response = self.client.put_log_events(
                logGroupName=LOG_GROUP,
                logStreamName=LOG_STREAM,
                logEvents=events,
            )
            return True, response.get("nextSequenceToken")
        except Exception as e:
            return False, str(e)


def monitor_resources(stop_event):
    print("\n📊 Resource Monitor started...")
    print(f"{'Time':>8} | {'Memory (MB)':>12} | {'CPU %':>8} | {'Logs Sent':>12}")
    print("-" * 55)

    while not stop_event.is_set():
        elapsed = time.time() - stats["start_time"]
        memory = get_memory_mb()
        cpu = get_cpu_percent()
        logs = stats["logs_sent"]

        print(
            f"{elapsed:>8.1f} | {memory:>12.1f} | {cpu:>8.1f} | {logs:>12,}", end="\r"
        )
        time.sleep(1)


def send_logs_thread(logger, events_batch, results_queue):
    success, token_or_error = logger.send_logs(events_batch)
    results_queue.put((success, token_or_error))


def main():
    print("=" * 60)
    print("🚀 CloudWatch 1 Million Logs Performance Test")
    print("=" * 60)
    print(f"Endpoint: {CLOUDWATCH_ENDPOINT}")
    print(f"Total logs: {TOTAL_LOGS:,}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Log group: {LOG_GROUP}")
    print(f"Log stream: {LOG_STREAM}")
    print(f"Initial memory: {initial_memory:.1f} MB")
    print()

    logger = CloudWatchLogger()
    print("Creating log group and stream...")
    logger.create_log_group_and_stream()

    stats["start_time"] = time.time()

    stop_event = threading.Event()
    monitor_thread = threading.Thread(target=monitor_resources, args=(stop_event,))
    monitor_thread.start()

    print(f"\n📤 Sending {TOTAL_LOGS:,} logs in batches of {BATCH_SIZE}...")
    print()

    num_batches = TOTAL_LOGS // BATCH_SIZE
    sequence_token = None

    for batch_num in range(num_batches):
        events = [
            {
                "timestamp": int(time.time() * 1000),
                "message": f"Perf test log {batch_num * BATCH_SIZE + i}: INFO - Test message {i} at {datetime.now().isoformat()}",
            }
            for i in range(BATCH_SIZE)
        ]

        try:
            if sequence_token:
                response = logger.client.put_log_events(
                    logGroupName=LOG_GROUP,
                    logStreamName=LOG_STREAM,
                    logEvents=events,
                    sequenceToken=sequence_token,
                )
            else:
                response = logger.client.put_log_events(
                    logGroupName=LOG_GROUP,
                    logStreamName=LOG_STREAM,
                    logEvents=events,
                )

            sequence_token = response.get("nextSequenceToken")
            stats["logs_sent"] += BATCH_SIZE
            stats["batches_sent"] += 1

            if batch_num % 100 == 0 and batch_num > 0:
                elapsed = time.time() - stats["start_time"]
                rate = stats["logs_sent"] / elapsed
                print(
                    f"  Progress: {stats['logs_sent']:,}/{TOTAL_LOGS:,} ({rate:.0f} logs/sec)"
                )

        except Exception as e:
            stats["logs_failed"] += BATCH_SIZE
            stats["batches_failed"] += 1
            if len(stats["errors"]) < 5:
                stats["errors"].append(str(e))

            if "SequenceToken" in str(e):
                sequence_token = None

    stats["end_time"] = time.time()

    stop_event.set()
    monitor_thread.join()

    elapsed = stats["end_time"] - stats["start_time"]
    total_logs = stats["logs_sent"]
    rate = total_logs / elapsed if elapsed > 0 else 0
    final_memory = get_memory_mb()
    memory_increase = final_memory - initial_memory

    print("\n")
    print("=" * 60)
    print("📈 RESULTS")
    print("=" * 60)
    print(f"  Total logs sent:    {total_logs:,}")
    print(f"  Logs failed:        {stats['logs_failed']:,}")
    print(f"  Batches sent:        {stats['batches_sent']:,}")
    print(f"  Batches failed:     {stats['batches_failed']:,}")
    print(f"  Time elapsed:       {elapsed:.2f} seconds")
    print(f"  Throughput:         {rate:.0f} logs/second")
    print(f"  Throughput:         {rate * 3600 / 1000000:.2f} M logs/hour")
    print()
    print(f"  Initial memory:     {initial_memory:.1f} MB")
    print(f"  Final memory:       {final_memory:.1f} MB")
    print(f"  Memory increase:    {memory_increase:.1f} MB")
    print()

    if stats["errors"]:
        print(f"  Sample errors: {stats['errors'][:3]}")

    print("=" * 60)

    return rate, memory_increase


if __name__ == "__main__":
    rate, memory = main()
