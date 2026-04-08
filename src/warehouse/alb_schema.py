import logging

import pyarrow as pa

logger = logging.getLogger("warehouse.alb_schema")

try:
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
    )
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


def get_alb_iceberg_schema():
    """Return the PyIceberg Schema for the alb_logs table."""
    return Schema(
        NestedField(1,  "time",                      TimestampType(),  required=False),
        NestedField(2,  "type",                      StringType(),     required=False),
        NestedField(3,  "elb",                       StringType(),     required=False),
        NestedField(4,  "client_ip",                 StringType(),     required=False),
        NestedField(5,  "client_port",               IntegerType(),    required=False),
        NestedField(6,  "target_ip",                 StringType(),     required=False),
        NestedField(7,  "target_port",               IntegerType(),    required=False),
        NestedField(8,  "request_processing_time",   DoubleType(),     required=False),
        NestedField(9,  "target_processing_time",    DoubleType(),     required=False),
        NestedField(10, "response_processing_time",  DoubleType(),     required=False),
        NestedField(11, "elb_status_code",           IntegerType(),    required=False),
        NestedField(12, "target_status_code",        IntegerType(),    required=False),
        NestedField(13, "received_bytes",            LongType(),       required=False),
        NestedField(14, "sent_bytes",                LongType(),       required=False),
        NestedField(15, "request_method",            StringType(),     required=False),
        NestedField(16, "request_url",               StringType(),     required=False),
        NestedField(17, "request_path",              StringType(),     required=False),
        NestedField(18, "request_protocol",          StringType(),     required=False),
        NestedField(19, "user_agent",                StringType(),     required=False),
        NestedField(20, "ssl_cipher",                StringType(),     required=False),
        NestedField(21, "ssl_protocol",              StringType(),     required=False),
        NestedField(22, "domain_name",               StringType(),     required=False),
        NestedField(23, "actions_executed",          StringType(),     required=False),
        NestedField(24, "redirect_url",              StringType(),     required=False),
        NestedField(25, "error_reason",              StringType(),     required=False),
        NestedField(26, "trace_id",                  StringType(),     required=False),
        NestedField(27, "ingestion_time",            TimestampType(),  required=False),
    )


def get_alb_partition_spec():
    """Partition by event day (field 1 = time)."""
    return PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=100,
            name="event_day",
            transform=DayTransform(),
        )
    )


def get_alb_arrow_schema() -> pa.Schema:
    """
    PyArrow schema matching the Iceberg ALB schema.

    PyIceberg TimestampType() maps to pa.timestamp('us') — no timezone.
    Datetime values written here must be naive (UTC assumed).
    """
    return pa.schema([
        pa.field("time",                     pa.timestamp("us"),  nullable=True),
        pa.field("type",                     pa.string(),         nullable=True),
        pa.field("elb",                      pa.string(),         nullable=True),
        pa.field("client_ip",                pa.string(),         nullable=True),
        pa.field("client_port",              pa.int32(),          nullable=True),
        pa.field("target_ip",                pa.string(),         nullable=True),
        pa.field("target_port",              pa.int32(),          nullable=True),
        pa.field("request_processing_time",  pa.float64(),        nullable=True),
        pa.field("target_processing_time",   pa.float64(),        nullable=True),
        pa.field("response_processing_time", pa.float64(),        nullable=True),
        pa.field("elb_status_code",          pa.int32(),          nullable=True),
        pa.field("target_status_code",       pa.int32(),          nullable=True),
        pa.field("received_bytes",           pa.int64(),          nullable=True),
        pa.field("sent_bytes",               pa.int64(),          nullable=True),
        pa.field("request_method",           pa.string(),         nullable=True),
        pa.field("request_url",              pa.string(),         nullable=True),
        pa.field("request_path",             pa.string(),         nullable=True),
        pa.field("request_protocol",         pa.string(),         nullable=True),
        pa.field("user_agent",               pa.string(),         nullable=True),
        pa.field("ssl_cipher",               pa.string(),         nullable=True),
        pa.field("ssl_protocol",             pa.string(),         nullable=True),
        pa.field("domain_name",              pa.string(),         nullable=True),
        pa.field("actions_executed",         pa.string(),         nullable=True),
        pa.field("redirect_url",             pa.string(),         nullable=True),
        pa.field("error_reason",             pa.string(),         nullable=True),
        pa.field("trace_id",                 pa.string(),         nullable=True),
        pa.field("ingestion_time",           pa.timestamp("us"),  nullable=True),
        pa.field("date",                     pa.string(),         nullable=True),
        pa.field("hour",                     pa.string(),         nullable=True),
    ])


def ensure_alb_table(catalog, namespace: str, table_name: str) -> None:
    """Create the alb_logs Iceberg table if it does not already exist."""
    if not PYICEBERG_AVAILABLE:
        return
    table_id = f"{namespace}.{table_name}"
    try:
        catalog.load_table(table_id)
        logger.info(f"[ALB] Loaded existing table: {table_id}")
    except Exception:
        try:
            catalog.create_table(
                table_id,
                schema=get_alb_iceberg_schema(),
                partition_spec=get_alb_partition_spec(),
            )
            logger.info(f"[ALB] Created table: {table_id}")
        except Exception as e:
            logger.warning(f"[ALB] Could not create table {table_id}: {e}")
