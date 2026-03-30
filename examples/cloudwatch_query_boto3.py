import os
import boto3
import time
import json
from datetime import datetime, timezone
from botocore.config import Config


def get_cloudwatch_client():
    """Get CloudWatch Logs client"""
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


def test_describe_log_groups():
    """Test DescribeLogGroups API"""
    print("=" * 100)
    print("🔍 Test 1: DescribeLogGroups")
    print("=" * 100)

    client = get_cloudwatch_client()

    try:
        response = client.describe_log_groups()
        print(f"\n✅ Found {len(response.get('logGroups', []))} log groups:\n")
        for group in response.get("logGroups", []):
            print(f"  - {group['logGroupName']}")
            print(
                f"    Created: {datetime.fromtimestamp(group['creationTime'] / 1000, tz=timezone.utc)}"
            )
            print()
    except Exception as e:
        print(f"❌ Error: {e}")


def test_describe_log_streams():
    """Test DescribeLogStreams API"""
    print("=" * 100)
    print("🔍 Test 2: DescribeLogStreams")
    print("=" * 100)

    client = get_cloudwatch_client()

    try:
        # First get log groups
        groups_response = client.describe_log_groups()
        log_groups = groups_response.get("logGroups", [])

        if not log_groups:
            print("❌ No log groups found")
            return

        for group in log_groups:
            group_name = group["logGroupName"]
            print(f"\n📌 Log Group: {group_name}")

            response = client.describe_log_streams(logGroupName=group_name)
            streams = response.get("logStreams", [])
            print(f"   Found {len(streams)} streams:\n")

            for stream in streams:
                print(f"   - {stream['logStreamName']}")
                print(
                    f"     Created: {datetime.fromtimestamp(stream['creationTime'] / 1000, tz=timezone.utc)}"
                )
                print()

    except Exception as e:
        print(f"❌ Error: {e}")


def test_get_log_events():
    """Test GetLogEvents API"""
    print("=" * 100)
    print("🔍 Test 3: GetLogEvents")
    print("=" * 100)

    client = get_cloudwatch_client()

    try:
        # Get log groups and streams
        groups_response = client.describe_log_groups()
        log_groups = groups_response.get("logGroups", [])

        if not log_groups:
            print("❌ No log groups found")
            return

        log_group = log_groups[0]["logGroupName"]
        streams_response = client.describe_log_streams(logGroupName=log_group)
        streams = streams_response.get("logStreams", [])

        if not streams:
            print(f"❌ No streams found in {log_group}")
            return

        log_stream = streams[0]["logStreamName"]

        print(f"\n📌 Getting events from: {log_group}/{log_stream}\n")

        response = client.get_log_events(
            logGroupName=log_group, logStreamName=log_stream, limit=5
        )

        events = response.get("events", [])
        print(f"✅ Retrieved {len(events)} events:\n")

        for event in events:
            dt = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
            print(f"  [{dt.strftime('%Y-%m-%d %H:%M:%S')}] {event['message']}")

    except Exception as e:
        print(f"❌ Error: {e}")


def test_query_logs_insights():
    """Test StartQueryExecution for Logs Insights (if supported)"""
    print("\n" + "=" * 100)
    print("🔍 Test 4: StartQueryExecution (Logs Insights)")
    print("=" * 100)

    client = get_cloudwatch_client()

    try:
        # Get log groups
        groups_response = client.describe_log_groups()
        log_groups = groups_response.get("logGroups", [])

        if not log_groups:
            print("❌ No log groups found")
            return

        log_group = log_groups[0]["logGroupName"]

        # Calculate time range (last 1 hour)
        end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_time = end_time - (60 * 60 * 1000)

        print(f"\n📌 Log Group: {log_group}")
        print(
            f"📌 Time Range: {datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)} - {datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)}"
        )

        # Test different query types
        queries = [
            "fields @timestamp, @message | limit 5",
            "fields @message | filter @message like /ERROR/ | limit 3",
            "filter @message like /WARN/ | stats count()",
            "stats count() by bin(5m)",
        ]

        for query in queries:
            print(f"\n📝 Query: {query}")
            try:
                response = client.start_query(
                    logGroupName=log_group,
                    startTime=start_time,
                    endTime=end_time,
                    queryString=query,
                )
                query_id = response.get("queryId")
                print(f"   ✅ Query ID: {query_id}")

                # Wait a moment and get results
                time.sleep(0.5)

                while True:
                    results = client.get_query_results(queryId=query_id)
                    status = results.get("status")
                    print(f"   Status: {status}")

                    if status in ["Complete", "Failed", "Cancelled"]:
                        break
                    time.sleep(0.2)

                records = results.get("results", [])
                print(f"   Records: {len(records)}")

            except Exception as e:
                print(f"   ⚠️  Query not fully supported: {str(e)[:100]}")

    except Exception as e:
        print(f"❌ Error: {e}")


def test_filter_log_events():
    """Test FilterLogEvents API"""
    print("\n" + "=" * 100)
    print("🔍 Test 5: FilterLogEvents")
    print("=" * 100)

    client = get_cloudwatch_client()

    try:
        # Get log groups
        groups_response = client.describe_log_groups()
        log_groups = groups_response.get("logGroups", [])

        if not log_groups:
            print("❌ No log groups found")
            return

        log_group = log_groups[0]["logGroupName"]

        # Test filter patterns
        filter_patterns = ["", "[...]", '[... , msg != "INFO", ...]']

        for pattern in filter_patterns:
            print(f"\n📝 Filter Pattern: '{pattern if pattern else 'ALL'}'")

            try:
                response = client.filter_log_events(
                    logGroupName=log_group, filterPattern=pattern, limit=5
                )

                events = response.get("events", [])
                print(f"   ✅ Found {len(events)} events:\n")

                for event in events[:3]:
                    print(f"      {event['message'][:80]}")

            except Exception as e:
                print(f"   ⚠️  Error: {str(e)[:100]}")

    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    print("\n🚀 CloudWatch Logs boto3 Query Tests\n")

    test_describe_log_groups()
    test_describe_log_streams()
    test_get_log_events()
    test_filter_log_events()
    test_query_logs_insights()

    print("\n" + "=" * 100)
    print("✅ All tests completed!")
    print("=" * 100)


if __name__ == "__main__":
    main()
