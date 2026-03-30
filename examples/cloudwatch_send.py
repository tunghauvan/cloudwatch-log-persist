import os
import sys
import time
import random
from datetime import datetime
from datetime import timezone

import boto3
from botocore.config import Config


def get_cloudwatch_client():
    endpoint_url = os.getenv(
        "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS", "http://localhost:4588"
    )

    return boto3.client(
        "logs",
        endpoint_url=endpoint_url,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
    )


def generate_sample_logs(count: int = 10):
    log_groups = ["/aws/lambda/my-function", "/ecs/my-service", "/ec2/my-instance"]
    log_streams = ["2024/01/01/stdout", "2024/01/02/stderr", "main"]

    logs = []
    for i in range(count):
        log = {
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "message": f"Sample log message {i}: {random.choice(['INFO', 'WARN', 'ERROR'])} - {random.choice(['Operation successful', 'Something went wrong', 'Processing data'])}",
        }
        logs.append(log)

    return logs, random.choice(log_groups), random.choice(log_streams)


def main():
    print("Initializing CloudWatch Logs client...")
    client = get_cloudwatch_client()

    endpoint = client._endpoint
    print(f"Using endpoint: {endpoint}")

    print("\nGenerating sample logs...")
    logs, log_group, log_stream = generate_sample_logs(20)

    print(f"Log Group: {log_group}")
    print(f"Log Stream: {log_stream}")
    print(f"Events: {len(logs)}")

    print("\nSending logs to CloudWatch...")
    try:
        response = client.put_log_events(
            logGroupName=log_group, logStreamName=log_stream, logEvents=logs
        )
        print(f"Success! Next sequence token: {response.get('nextSequenceToken')}")
    except Exception as e:
        print(f"Error: {e}")

    print("\nQuerying logs back...")
    try:
        response = client.get_log_events(
            logGroupName=log_group, logStreamName=log_stream, limit=5
        )
        print(f"Retrieved {len(response.get('events', []))} events:")
        for event in response.get("events", []):
            print(f"  [{event['timestamp']}] {event['message']}")
    except Exception as e:
        print(f"Error querying: {e}")


if __name__ == "__main__":
    main()
