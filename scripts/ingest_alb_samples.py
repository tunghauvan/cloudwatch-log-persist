"""
Ingest real ALB log files from samples/AWSLogs into the service.

Steps:
  1. Upload all .log.gz files from samples/AWSLogs/ to MinIO bucket "alb-logs-sample"
  2. POST S3 event notifications to /alb/s3-event (batched)
  3. Trigger /warehouse/compact-alb

Usage:
  python3 scripts/ingest_alb_samples.py [--limit N] [--bucket BUCKET]
"""

import argparse
import sys
import time
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

# ── config ──────────────────────────────────────────────────────────────────
MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_ACCESS    = "admin"
MINIO_SECRET    = "admin123"
MINIO_REGION    = "ap-southeast-1"

SERVICE_URL     = "http://localhost:4588"
ALB_INGEST_URL  = f"{SERVICE_URL}/alb/s3-event"
COMPACT_URL     = f"{SERVICE_URL}/warehouse/compact-alb"

SAMPLES_DIR     = Path(__file__).parent.parent / "samples" / "AWSLogs"
DEFAULT_BUCKET  = "alb-logs-sample"
S3_PREFIX       = "AWSLogs/"

BATCH_SIZE      = 10   # records per POST request
# ────────────────────────────────────────────────────────────────────────────


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        region_name=MINIO_REGION,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"[minio] Created bucket: {bucket}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"[minio] Bucket already exists: {bucket}")
        else:
            raise


def upload_files(s3, bucket: str, files: list[Path], limit: int) -> list[str]:
    """Upload files to MinIO, return list of S3 keys uploaded."""
    uploaded = []
    total = min(len(files), limit)
    print(f"[upload] Uploading {total} files to s3://{bucket}/{S3_PREFIX} ...")

    for i, f in enumerate(files[:limit], 1):
        key = S3_PREFIX + f.name
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=f.read_bytes())
            uploaded.append(key)
            if i % 50 == 0 or i == total:
                print(f"  [{i}/{total}] uploaded {f.name}")
        except Exception as e:
            print(f"  [ERROR] {f.name}: {e}", file=sys.stderr)

    print(f"[upload] Done — {len(uploaded)} files uploaded")
    return uploaded


def build_s3_record(bucket: str, key: str) -> dict:
    return {
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": key},
        },
    }


def ingest_batch(bucket: str, keys: list[str]) -> dict:
    payload = {"Records": [build_s3_record(bucket, k) for k in keys]}
    r = requests.post(ALB_INGEST_URL, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()


def ingest_all(bucket: str, keys: list[str]) -> tuple[int, int, int]:
    """Returns (processed, rows_written, errors)."""
    total_processed = 0
    total_rows = 0
    total_errors = 0

    batches = [keys[i:i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)]
    print(f"\n[ingest] Sending {len(keys)} keys in {len(batches)} batches of {BATCH_SIZE} ...")

    for idx, batch in enumerate(batches, 1):
        try:
            resp = ingest_batch(bucket, batch)
            processed  = resp.get("processed", 0)
            rows       = resp.get("rows_written", 0)
            results    = resp.get("results", [])
            errors     = sum(1 for r in results if r.get("status") == "error")

            total_processed += processed
            total_rows      += rows
            total_errors    += errors

            status_str = f"processed={processed} rows={rows} errors={errors}"
            progress   = f"[{idx}/{len(batches)}]"
            print(f"  {progress} {status_str}")

        except requests.HTTPError as e:
            print(f"  [batch {idx}] HTTP error: {e}", file=sys.stderr)
            total_errors += len(batch)
        except Exception as e:
            print(f"  [batch {idx}] Error: {e}", file=sys.stderr)
            total_errors += len(batch)

    return total_processed, total_rows, total_errors


def compact_alb():
    print("\n[compact] Triggering ALB compaction ...")
    r = requests.post(COMPACT_URL, timeout=300)
    r.raise_for_status()
    result = r.json()
    print(f"[compact] Result: {result}")
    return result


def main():
    parser = argparse.ArgumentParser(description="Ingest ALB sample logs")
    parser.add_argument("--limit",  type=int, default=999999, help="Max files to ingest (default: all)")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET,   help=f"MinIO bucket (default: {DEFAULT_BUCKET})")
    parser.add_argument("--no-upload",  action="store_true", help="Skip upload, just ingest from existing bucket")
    parser.add_argument("--no-compact", action="store_true", help="Skip compaction step")
    args = parser.parse_args()

    # ── check service ────────────────────────────────────────────────────────
    try:
        r = requests.get(f"{SERVICE_URL}/health", timeout=5)
        r.raise_for_status()
        print(f"[check] Service OK at {SERVICE_URL}")
    except Exception as e:
        print(f"[ERROR] Service not reachable at {SERVICE_URL}: {e}", file=sys.stderr)
        sys.exit(1)

    # ── gather files ─────────────────────────────────────────────────────────
    files = sorted(SAMPLES_DIR.glob("*.log.gz"))
    if not files:
        print(f"[ERROR] No .log.gz files found in {SAMPLES_DIR}", file=sys.stderr)
        sys.exit(1)
    print(f"[files] Found {len(files)} .log.gz files in {SAMPLES_DIR}")

    s3 = make_s3_client()

    # ── upload ───────────────────────────────────────────────────────────────
    if not args.no_upload:
        ensure_bucket(s3, args.bucket)
        keys = upload_files(s3, args.bucket, files, args.limit)
    else:
        keys = [S3_PREFIX + f.name for f in files[:args.limit]]
        print(f"[upload] Skipped — using {len(keys)} keys")

    if not keys:
        print("[ERROR] No files to ingest", file=sys.stderr)
        sys.exit(1)

    # ── ingest ───────────────────────────────────────────────────────────────
    t0 = time.time()
    processed, rows, errors = ingest_all(args.bucket, keys)
    elapsed = time.time() - t0

    print(f"\n[ingest] Summary:")
    print(f"  Files processed : {processed}")
    print(f"  Rows written    : {rows:,}")
    print(f"  Errors          : {errors}")
    print(f"  Duration        : {elapsed:.1f}s")

    if rows == 0:
        print("[WARN] No rows written — check ALB parser or file format", file=sys.stderr)

    # ── compact ──────────────────────────────────────────────────────────────
    if not args.no_compact:
        compact_alb()
    else:
        print("[compact] Skipped")

    print("\n[done]")


if __name__ == "__main__":
    main()
