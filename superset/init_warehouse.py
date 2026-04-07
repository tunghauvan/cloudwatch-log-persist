#!/usr/bin/env python3
"""
init_warehouse.py — Run at Superset container startup.

Creates (or refreshes) /app/superset_home/alb_warehouse.duckdb with:
  - Persistent S3 secret pointing at the local MinIO instance
  - A VIEW `alb_logs` over the Iceberg table in stag-log-warehouse
  - A VIEW `alb_logs_today` as a convenience pre-filtered view

Usage:
    python /app/pythonpath/init_warehouse.py
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("init_warehouse")

DB_PATH = os.environ.get(
    "ALB_DUCKDB_PATH",
    "/app/superset_home/alb_warehouse.duckdb",
)

S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "iceberg-minio:9000")
S3_KEY_ID   = os.environ.get("MINIO_ACCESS_KEY", "admin")
S3_SECRET   = os.environ.get("MINIO_SECRET_KEY", "admin123")
S3_REGION   = os.environ.get("MINIO_REGION", "ap-southeast-1")
S3_BUCKET   = os.environ.get("MINIO_BUCKET", "stag-log-warehouse")
ALB_TABLE   = os.environ.get("ALB_ICEBERG_PATH", f"s3://{S3_BUCKET}/default/alb_logs")

try:
    import duckdb
except ImportError:
    log.error("duckdb package not found — skipping warehouse init")
    sys.exit(0)


def init():
    log.info("Initialising DuckDB warehouse at %s", DB_PATH)
    conn = duckdb.connect(
        DB_PATH,
        config={"unsafe_enable_version_guessing": True},
    )

    # Extensions
    for ext in ("httpfs", "iceberg"):
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")

    # Persistent S3 secret (survives across connections)
    conn.execute(f"""
        CREATE OR REPLACE PERSISTENT SECRET minio_s3 (
            TYPE     S3,
            KEY_ID   '{S3_KEY_ID}',
            SECRET   '{S3_SECRET}',
            ENDPOINT '{S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL  false,
            REGION   '{S3_REGION}'
        )
    """)
    log.info("S3 secret 'minio_s3' created/updated")

    # Main view — points at latest Iceberg snapshot via version-guessing
    conn.execute(f"""
        CREATE OR REPLACE VIEW alb_logs AS
        SELECT *
        FROM iceberg_scan('{ALB_TABLE}')
    """)

    # Quick sanity-check
    row = conn.execute("SELECT COUNT(*) FROM alb_logs").fetchone()
    log.info("alb_logs view OK — %d rows visible", row[0])

    conn.close()
    log.info("Warehouse init complete: %s", DB_PATH)


def register_superset_db():
    """Register the DuckDB database in Superset's metadata DB (idempotent)."""
    SUPERSET_DB_URI = os.environ.get(
        "SQLALCHEMY_DATABASE_URI",
        "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
    )
    try:
        from superset.app import create_app
        from superset.extensions import db as superset_db

        app = create_app()
        with app.app_context():
            from superset.models.core import Database

            existing = superset_db.session.query(Database).filter_by(
                database_name="ALB Warehouse (DuckDB)"
            ).first()
            if existing:
                log.info("DuckDB database already registered in Superset (id=%d)", existing.id)
                return
            d = Database(
                database_name="ALB Warehouse (DuckDB)",
                sqlalchemy_uri=f"duckdb:///{DB_PATH}",
                expose_in_sqllab=True,
                allow_run_async=False,
                allow_dml=False,
                extra='{"engine_params": {"connect_args": {"config": {"unsafe_enable_version_guessing": true}}}}',
            )
            superset_db.session.add(d)
            superset_db.session.commit()
            log.info("DuckDB database registered in Superset (id=%d)", d.id)
    except Exception as e:
        log.warning("Could not register DuckDB in Superset: %s", e)


if __name__ == "__main__":
    init()
    register_superset_db()
