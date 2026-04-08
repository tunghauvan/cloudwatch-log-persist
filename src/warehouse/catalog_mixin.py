import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa

logger = logging.getLogger("service.warehouse")


class CatalogMixin:
    """Delta Lake helpers: storage options, table URIs, schema definitions, warehouse init."""

    # ------------------------------------------------------------------
    # S3 / storage
    # ------------------------------------------------------------------

    def _get_delta_storage_options(self) -> dict:
        """Build delta-rs storage_options from config.

        Supports:
        - Explicit credentials (access_key + secret_key) with optional custom endpoint (MinIO)
        - EC2 IAM role (use_ec2_role: true) — delta-rs picks up the instance profile automatically
        """
        s3_cfg = self.config.get("s3", {})
        use_ec2_role = s3_cfg.get("use_ec2_role", False)
        region = s3_cfg.get("region", "us-east-1")

        opts: dict = {"AWS_REGION": region}

        if not use_ec2_role:
            opts["AWS_ACCESS_KEY_ID"] = s3_cfg.get("access_key", "admin")
            opts["AWS_SECRET_ACCESS_KEY"] = s3_cfg.get("secret_key", "admin123")
            endpoint = s3_cfg.get("endpoint", "").strip()
            if endpoint:
                opts["AWS_ENDPOINT_URL"] = endpoint
                opts["AWS_ALLOW_HTTP"] = "true"
                # Required for non-AWS S3-compatible stores (MinIO, etc.)
                opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

        return opts

    def _build_s3_client_config(self) -> dict:
        """Build boto3 client kwargs (used by retention vacuum / GC helpers)."""
        s3_cfg = self.config.get("s3", {})
        use_ec2_role = s3_cfg.get("use_ec2_role", False)
        region = s3_cfg.get("region", "us-east-1")

        kwargs: dict = {"region_name": region}
        if not use_ec2_role:
            kwargs["aws_access_key_id"] = s3_cfg.get("access_key", "admin")
            kwargs["aws_secret_access_key"] = s3_cfg.get("secret_key", "admin123")
            endpoint = s3_cfg.get("endpoint", "").strip()
            if endpoint:
                kwargs["endpoint_url"] = endpoint
        return kwargs

    # ------------------------------------------------------------------
    # Table URIs
    # ------------------------------------------------------------------

    def _get_delta_uri(self, table_name: str) -> str:
        """Return the S3 URI for a Delta table.

        Converts s3a:// / s3n:// → s3:// (delta-rs uses the standard s3:// scheme).
        Tables are stored under <warehouse>/delta/<table_name>/ so they don't
        collide with any existing Iceberg data.
        """
        base = self.warehouse_path
        for prefix in ("s3a://", "s3n://"):
            if base.startswith(prefix):
                base = "s3://" + base[len(prefix):]
                break
        base = base.rstrip("/")
        return f"{base}/delta/{table_name}"

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _get_arrow_schema(self) -> pa.Schema:
        """PyArrow schema for CloudWatch / Loki log tables (delta-rs uses pa.Schema)."""
        label_columns = self._get_label_columns()
        fields = [
            pa.field("log_group_name", pa.string()),
            pa.field("log_stream_name", pa.string()),
            pa.field("timestamp", pa.timestamp("us")),
            pa.field("message", pa.string()),
            pa.field("ingestion_time", pa.timestamp("us")),
            pa.field("sequence_token", pa.int64()),
        ]
        for col in label_columns:
            fields.append(pa.field(f"label_{col}", pa.string()))
        return pa.schema(fields)

    def _get_label_columns(self) -> List[str]:
        ingest_cfg = self.config.get("ingest", {})
        labels_cfg = ingest_cfg.get("labels", {})
        return [
            col.replace("-", "_").replace(" ", "_")
            for col in labels_cfg.get("columns", [])
        ]

    # ------------------------------------------------------------------
    # Warehouse init
    # ------------------------------------------------------------------

    def ensure_warehouse(self):
        """Verify S3 connectivity and log table URIs.

        Delta tables are created automatically on first write — no pre-creation needed.
        """
        for tname in (self.table_name, self.loki_table_name):
            uri = self._get_delta_uri(tname)
            logger.info(f" Delta table URI for '{tname}': {uri}")

        # Smoke-test S3 access (list bucket) so we fail fast at startup.
        try:
            import boto3
            s3_cfg = self.config.get("s3", {})
            bucket = s3_cfg.get("bucket", "")
            if bucket:
                client = boto3.client("s3", **self._build_s3_client_config())
                client.head_bucket(Bucket=bucket)
                logger.info(f" S3 bucket '{bucket}' is accessible")
        except Exception as e:
            logger.warning(f" S3 connectivity check failed: {e}")

    # ------------------------------------------------------------------
    # Compat stubs (called by server.py / routes that haven't been updated yet)
    # ------------------------------------------------------------------

    def get_table_path(self, table_name: Optional[str] = None) -> Path:
        name = table_name or self.table_name
        return self._warehouse_dir / self.namespace / name

    def list_tables(self) -> list:
        return [self.table_name, self.loki_table_name]

