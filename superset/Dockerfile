FROM apache/superset:latest

USER root

# Install required packages into the venv managed by uv
RUN VIRTUAL_ENV=/app/.venv uv pip install --no-cache \
    duckdb \
    duckdb-engine \
    pyiceberg[s3] \
    psycopg2-binary

# Switch back to superset user
USER superset
