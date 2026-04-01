#!/usr/bin/env python3
"""Generate Kubernetes-like logs and push to Loki"""

import json
import time
import random
import requests
from datetime import datetime, timezone

# Loki endpoint
LOKI_URL = "http://cloudwatch-local-service:4588/loki/api/v1/push"

# Sample log messages
LOG_MESSAGES = [
    ("INFO", "Application started successfully"),
    ("ERROR", "Connection timeout to database"),
    ("WARN", "High memory usage detected: 85%"),
    ("INFO", "Processing job ID: {}"),
    ("DEBUG", "Cache hit ratio: 0.95"),
    ("ERROR", "Failed to connect to upstream service"),
    ("INFO", "Request completed: 200 OK in {}ms"),
    ("WARN", "Slow query detected: {}s"),
    ("INFO", "User logged in: user_{}"),
    ("ERROR", "Database connection pool exhausted"),
]

# Kubernetes pod info
PODS = [
    {
        "pod_name": "api-server-7d9f4b8c5-x2v4p",
        "container": "api",
        "app": "api-server",
        "namespace": "prod",
    },
    {
        "pod_name": "api-server-7d9f4b8c5-abc12",
        "container": "api",
        "app": "api-server",
        "namespace": "prod",
    },
    {
        "pod_name": "worker-5f8d7c9b2-m4k8n",
        "container": "worker",
        "app": "worker",
        "namespace": "prod",
    },
    {
        "pod_name": "worker-5f8d7c9b2-xyz99",
        "container": "worker",
        "app": "worker",
        "namespace": "prod",
    },
    {
        "pod_name": "cache-3a4b5c6d7-e1f2g",
        "container": "redis",
        "app": "cache",
        "namespace": "prod",
    },
    {
        "pod_name": "nginx-ingress-9c8f7e6d5-a1b2c",
        "container": "nginx",
        "app": "nginx-ingress",
        "namespace": "prod",
    },
]


def generate_log_entry():
    """Generate a single log entry"""
    level, message_template = random.choice(LOG_MESSAGES)
    pod = random.choice(PODS)

    # Format message
    if "{}" in message_template:
        if "ID" in message_template:
            message = message_template.format(random.randint(10000, 99999))
        elif "ms" in message_template:
            message = message_template.format(random.randint(10, 500))
        elif "s" in message_template:
            message = message_template.format(random.randint(1, 10))
        else:
            message = message_template.format(random.randint(1, 1000))
    else:
        message = message_template

    # Create log line
    timestamp = datetime.now(timezone.utc)
    log_line = f"[{timestamp.isoformat()}] {level} {message}"

    # Create stream
    return {
        "stream": {
            "pod": pod["pod_name"],
            "container": pod["container"],
            "namespace": pod["namespace"],
            "app": pod["app"],
            "level": level.lower(),
        },
        "values": [[str(int(time.time() * 1000000000)), log_line]],
    }


def push_logs():
    """Generate and push logs to Loki"""
    streams = {}

    # Generate 10 logs
    for _ in range(10):
        entry = generate_log_entry()
        stream_key = tuple(sorted(entry["stream"].items()))

        if stream_key not in streams:
            streams[stream_key] = entry
        else:
            streams[stream_key]["values"].extend(entry["values"])

    # Prepare payload
    payload = {"streams": list(streams.values())}

    # Send to Loki
    try:
        response = requests.post(
            LOKI_URL, json=payload, headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            print(f"[{datetime.now().isoformat()}] Pushed {len(streams)} streams")
        else:
            print(
                f"[{datetime.now().isoformat()}] Error: {response.status_code} - {response.text}"
            )
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Exception: {e}")


if __name__ == "__main__":
    print("Starting Kubernetes log simulator...")
    print(f"Pushing logs to: {LOKI_URL}")

    while True:
        push_logs()
        time.sleep(2)  # Push every 2 seconds
