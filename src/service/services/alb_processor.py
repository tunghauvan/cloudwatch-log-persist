"""
ALBProcessor: downloads an ALB log file from S3, parses it, and writes rows to
the Iceberg alb_logs table using the existing warehouse catalog.
"""

import logging
import time
from typing import Any, Dict, List

import boto3
import pyarrow as pa

logger = logging.getLogger("service.alb_processor")


class ALBProcessor:
    """
    Stateless processor that handles one S3 object at a time.

    Usage (called by the /alb/s3-event route):
        processor = ALBProcessor(warehouse, config)
        result = processor.process_s3_object(bucket, key)
    """

    def __init__(self, warehouse, config: dict):
        self.warehouse = warehouse
        self.config = config
        alb_cfg = config.get("alb", {})
        self.table_name = alb_cfg.get("table_name", "alb_logs")
        self.batch_size = int(alb_cfg.get("batch_size", 10000))

    # ------------------------------------------------------------------
    # S3
    # ------------------------------------------------------------------

    def _get_s3_client(self):
        s3_cfg = self.config.get("s3", {})
        use_ec2_role = s3_cfg.get("use_ec2_role", False)
        region = s3_cfg.get("region", "us-east-1")
        kwargs: Dict[str, Any] = {"region_name": region}

        endpoint = s3_cfg.get("endpoint")
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        if not use_ec2_role:
            kwargs["aws_access_key_id"] = s3_cfg.get("access_key")
            kwargs["aws_secret_access_key"] = s3_cfg.get("secret_key")

        return boto3.client("s3", **kwargs)

    # ------------------------------------------------------------------
    # Table initialisation
    # ------------------------------------------------------------------

    def _ensure_table(self) -> None:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from warehouse.alb_schema import ensure_alb_table
        ensure_alb_table(
            self.warehouse.catalog, self.warehouse.namespace, self.table_name
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_s3_object(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Download one S3 object, parse it as ALB access logs, and append rows
        to the alb_logs Iceberg table.

        Returns a status dict:
            {status, rows_written, duration_ms, error?}
        """
        start = time.time()
        try:
            s3 = self._get_s3_client()
            logger.info(f"[ALB] Downloading s3://{bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            content: bytes = response["Body"].read()
        except Exception as e:
            logger.error(f"[ALB] S3 download failed for s3://{bucket}/{key}: {e}")
            return {
                "status": "error",
                "error": f"S3 download failed: {e}",
                "rows_written": 0,
                "duration_ms": int((time.time() - start) * 1000),
            }

        from service.services.alb_parser import parse_alb_content
        rows = parse_alb_content(content)
        logger.info(
            f"[ALB] Parsed {len(rows)} rows from s3://{bucket}/{key} "
            f"({len(content)} bytes)"
        )

        if not rows:
            return {
                "status": "ok",
                "rows_written": 0,
                "duration_ms": int((time.time() - start) * 1000),
            }

        try:
            self._ensure_table()
            self._write_rows(rows)
        except Exception as e:
            logger.error(f"[ALB] Iceberg write failed: {e}")
            return {
                "status": "error",
                "error": f"Iceberg write failed: {e}",
                "rows_written": 0,
                "duration_ms": int((time.time() - start) * 1000),
            }

        duration_ms = int((time.time() - start) * 1000)
        logger.info(
            f"[ALB] Written {len(rows)} rows in {duration_ms}ms "
            f"(s3://{bucket}/{key})"
        )
        return {"status": "ok", "rows_written": len(rows), "duration_ms": duration_ms}

    # ------------------------------------------------------------------
    # Iceberg write
    # ------------------------------------------------------------------

    def _write_rows(self, rows: List[Dict[str, Any]]) -> None:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from warehouse.alb_schema import get_alb_arrow_schema

        table_id = f"{self.warehouse.namespace}.{self.table_name}"
        table_obj = self.warehouse.catalog.load_table(table_id)
        arrow_schema = get_alb_arrow_schema()

        for i in range(0, len(rows), self.batch_size):
            batch = rows[i : i + self.batch_size]
            arrow_table = self._rows_to_arrow(batch, arrow_schema)
            table_obj.append(arrow_table)
            logger.debug(
                f"[ALB] Appended batch {i // self.batch_size + 1}: "
                f"{len(batch)} rows"
            )

    @staticmethod
    def _rows_to_arrow(
        rows: List[Dict[str, Any]], schema: pa.Schema
    ) -> pa.Table:
        """
        Convert a list of row dicts to a typed PyArrow table.

        Datetime columns in the schema use pa.timestamp('us') (no tz).
        Aware datetime objects are converted to naive UTC before building
        the array so PyIceberg does not reject them as 'timestamptz'.
        """
        from datetime import datetime, timezone

        ts_fields = {
            f.name for f in schema if pa.types.is_timestamp(f.type) and f.type.tz is None
        }

        columns: Dict[str, list] = {name: [] for name in schema.names}
        for row in rows:
            for name in schema.names:
                val = row.get(name)
                if name in ts_fields and isinstance(val, datetime) and val.tzinfo is not None:
                    val = val.astimezone(timezone.utc).replace(tzinfo=None)
                columns[name].append(val)

        arrays = []
        for field in schema:
            arrays.append(pa.array(columns[field.name], type=field.type))

        return pa.table(dict(zip(schema.names, arrays)), schema=schema)
