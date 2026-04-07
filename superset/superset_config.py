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
# which runs at container startup.  Superset opens it read-only so that
# multiple gunicorn workers can query it concurrently without conflicts.

_DUCKDB_INIT_SQL = """
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
SET unsafe_enable_version_guessing = true;
"""


def _duckdb_connect(uri):
    """
    SQLAlchemy 'creator' callable used when a DuckDB engine is created.
    Runs initialisation SQL on every new connection so extensions and
    settings are always available regardless of the database file state.
    """
    import duckdb

    db_path = str(uri).replace("duckdb:///", "")
    read_only = db_path != ":memory:"
    conn = duckdb.connect(
        db_path if db_path else ":memory:",
        read_only=read_only,
        config={"unsafe_enable_version_guessing": True},
    )
    for stmt in _DUCKDB_INIT_SQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                conn.execute(stmt)
            except Exception:
                pass  # extension may already be loaded
    return conn
