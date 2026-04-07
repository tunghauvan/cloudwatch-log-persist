import os

# ------------------------------------------------------------------
# Metadata database (Superset's own state — users, dashboards, etc.)
# ------------------------------------------------------------------
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)

# ------------------------------------------------------------------
# Session / caching
# ------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": REDIS_URL,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
RESULTS_BACKEND = None  # use default file-based backend

# ------------------------------------------------------------------
# Security
# ------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "your-secret-key-change-in-production")

# Allow DuckDB (local-file / in-memory) connections
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Allow displaying non-HTTPS databases in the UI
ALLOW_FULL_CSV_EXPORT = True

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ------------------------------------------------------------------
# DuckDB — engine-level initialisation applied to every new connection
# ------------------------------------------------------------------
# The alb_warehouse.duckdb file is created by superset/init_warehouse.py
# which persists extensions, S3 secret, and the alb_logs view.
#
# Two problems solved here:
#   1. File lock: DuckDB 1.x takes an exclusive write lock when opened
#      normally. Fix: open with read_only=True + config dict so multiple
#      Gunicorn workers can query concurrently.
#   2. Missing .connection: DuckDB 1.5 removed the .connection attribute
#      on DuckDBPyConnection. Superset/SQLAlchemy accesses it. Fix: shim
#      it by wrapping the connection class at import time.

import duckdb as _duckdb

if not hasattr(_duckdb.DuckDBPyConnection, "connection"):
    _duckdb.DuckDBPyConnection.connection = property(lambda self: self)


def DB_CONNECTION_MUTATOR(uri, params, username, security_manager, source):
    """
    Called by Superset before creating each SQLAlchemy engine.
    For DuckDB databases: inject read_only + config so the file is opened
    without an exclusive lock (allows concurrent Gunicorn workers).
    """
    if str(uri).startswith("duckdb"):
        existing = params.get("connect_args", {})
        existing.setdefault("read_only", True)
        existing.setdefault("config", {"unsafe_enable_version_guessing": True})
        params["connect_args"] = existing
    return uri, params
