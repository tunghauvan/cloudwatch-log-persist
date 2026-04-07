#!/usr/bin/env python3
"""
build_alb_dashboard.py
======================
Programmatically create an ALB Overview dashboard in Superset via REST API.

Charts:
  1. Total Requests (Big Number)
  2. Error Rate 4xx+5xx (Big Number)
  3. P99 Latency (Big Number)
  4. Request Volume over Time (Line chart)
  5. HTTP Status Code Distribution (Pie chart)
  6. Error Rate over Time 5xx (Line chart)
  7. Top 10 Paths by Request Count (Bar chart)
  8. Top 10 Client IPs (Bar chart)
  9. Latency Percentiles over Time P50/P95/P99 (Line chart)
 10. Bytes Transferred over Time (Area chart)
 11. Request Type Distribution GET/POST/... (Pie chart)
 12. Slow Requests Top 20 (Table)

Usage:
  python3 scripts/build_alb_dashboard.py
"""

import json
import sys
import time

import requests

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin123"
DB_ID = 1  # ALB Warehouse (DuckDB)

# ── auth ─────────────────────────────────────────────────────────────────────

# Persistent session so cookies (session cookie needed for CSRF) are preserved
_session = requests.Session()
_token = ""
_csrf = ""


def login():
    global _token, _csrf
    r = _session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db"},
    )
    r.raise_for_status()
    _token = r.json()["access_token"]

    csrf_r = _session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {_token}"},
    )
    csrf_r.raise_for_status()
    _csrf = csrf_r.json()["result"]
    return _token, _csrf


def headers(token=None, csrf=None):
    return {
        "Authorization": f"Bearer {token or _token}",
        "X-CSRFToken": csrf or _csrf,
        "Content-Type": "application/json",
        "Referer": SUPERSET_URL,
    }


def _get(path, **kw):
    return _session.get(f"{SUPERSET_URL}{path}", headers=headers(), **kw)


def _post(path, **kw):
    return _session.post(f"{SUPERSET_URL}{path}", headers=headers(), **kw)


def _delete(path, **kw):
    return _session.delete(f"{SUPERSET_URL}{path}", headers=headers(), **kw)


# ── dataset helpers ───────────────────────────────────────────────────────────

def find_or_create_dataset(name: str, sql: str) -> int:
    # Search existing by listing all and matching name
    r = _get("/api/v1/dataset/?q=(page_size:100)")
    for ds in r.json().get("result", []):
        if ds["table_name"] == name:
            print(f"  [dataset] '{name}' already exists (id={ds['id']})")
            return ds["id"]

    payload = {
        "database": DB_ID,
        "table_name": name,
        "sql": sql,
        "is_managed_externally": False,
    }
    r = _post("/api/v1/dataset/", json=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Dataset create failed {r.status_code}: {r.text[:300]}")
    ds_id = r.json()["id"]
    print(f"  [dataset] '{name}' created (id={ds_id})")
    return ds_id


# ── chart helpers ─────────────────────────────────────────────────────────────

def delete_chart_by_name(name: str):
    r = _get(f"/api/v1/chart/?q=(filters:!((col:slice_name,opr:ChartAllTextSearch,val:'{name}')))")
    for ch in r.json().get("result", []):
        if ch["slice_name"] == name:
            _delete(f"/api/v1/chart/{ch['id']}")


def create_chart(payload: dict) -> tuple[int, str]:
    delete_chart_by_name(payload["slice_name"])
    r = _post("/api/v1/chart/", json=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Chart create failed {r.status_code}: {r.text[:400]}")
    ch_id = r.json()["id"]
    name = payload["slice_name"]
    print(f"  [chart] '{name}' created (id={ch_id})")
    return ch_id, name


# ── dashboard helpers ─────────────────────────────────────────────────────────

def delete_dashboard_by_title(title: str):
    r = _get("/api/v1/dashboard/")
    for db in r.json().get("result", []):
        if db["dashboard_title"] == title:
            _delete(f"/api/v1/dashboard/{db['id']}")
            print(f"  [dashboard] Deleted old '{title}' (id={db['id']})")


def create_dashboard(title: str, chart_meta: list[tuple[int, str]]) -> int:
    """chart_meta: list of (chart_id, slice_name)"""
    delete_dashboard_by_title(title)

    chart_ids = [cid for cid, _ in chart_meta]

    # Build grid layout: 4 charts per row, each 6/24 columns wide
    CHART_W = 6
    CHARTS_PER_ROW = 4

    children_of_grid = []
    layout = {
        "GRID_ID": {
            "children": children_of_grid,
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
            "type": "GRID",
        },
        "ROOT_ID": {
            "children": ["GRID_ID"],
            "id": "ROOT_ID",
            "type": "ROOT",
        },
        "HEADER_ID": {
            "id": "HEADER_ID",
            "meta": {"text": title},
            "type": "HEADER",
        },
    }

    for row_idx in range(0, len(chart_meta), CHARTS_PER_ROW):
        row_slice = chart_meta[row_idx : row_idx + CHARTS_PER_ROW]
        row_id = f"ROW-{row_idx}"
        row_children = []
        children_of_grid.append(row_id)
        layout[row_id] = {
            "children": row_children,
            "id": row_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW",
        }
        for col_idx, (cid, cname) in enumerate(row_slice):
            chart_layout_id = f"CHART-{cid}"
            row_children.append(chart_layout_id)
            layout[chart_layout_id] = {
                "children": [],
                "id": chart_layout_id,
                "meta": {
                    "chartId": cid,
                    "height": 50,
                    "sliceName": cname,
                    "width": CHART_W,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "type": "CHART",
            }

    payload = {
        "dashboard_title": title,
        "published": True,
        "position_json": json.dumps(layout),
    }
    r = _post("/api/v1/dashboard/", json=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Dashboard create failed {r.status_code}: {r.text[:400]}")
    db_id = r.json()["id"]

    # Attach charts to the dashboard via Superset internal ORM (REST PUT doesn't expose slices)
    try:
        import subprocess, json as _json
        chart_ids_json = _json.dumps(chart_ids)
        script = f"""
from superset.app import create_app
app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    dashboard = db.session.query(Dashboard).filter_by(id={db_id}).first()
    slices = db.session.query(Slice).filter(Slice.id.in_({chart_ids})).all()
    dashboard.slices = slices
    db.session.commit()
    print(f'Linked {{len(slices)}} charts to dashboard id={db_id}')
"""
        result = subprocess.run(
            ["docker", "exec", "superset-app", "python3", "-c", script],
            capture_output=True, text=True, timeout=60,
        )
        linked_line = [l for l in result.stdout.splitlines() if "Linked" in l]
        if linked_line:
            print(f"  [dashboard] {linked_line[0]}")
        elif result.returncode != 0:
            print(f"  [dashboard] Warning: ORM link failed:\n{result.stderr[-300:]}")
    except Exception as e:
        print(f"  [dashboard] Warning: could not link charts via ORM: {e}")

    print(f"  [dashboard] '{title}' created (id={db_id})")
    print(f"  URL: {SUPERSET_URL}/superset/dashboard/{db_id}/")
    return db_id


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("[auth] Logging in ...")
    token, csrf = login()
    print("[auth] OK")

    # ── 1. Datasets ───────────────────────────────────────────────────────────
    print("\n[datasets] Creating datasets ...")

    ds_main = find_or_create_dataset("alb_logs_ds", """
SELECT
    time,
    type,
    elb,
    client_ip,
    client_port,
    target_ip,
    target_port,
    request_processing_time,
    target_processing_time,
    response_processing_time,
    elb_status_code,
    target_status_code,
    received_bytes,
    sent_bytes,
    request_method,
    request_url,
    request_path,
    request_protocol,
    user_agent,
    ssl_protocol,
    domain_name,
    actions_executed,
    error_reason,
    trace_id,
    ingestion_time,
    CASE
        WHEN elb_status_code >= 500 THEN '5xx'
        WHEN elb_status_code >= 400 THEN '4xx'
        WHEN elb_status_code >= 300 THEN '3xx'
        WHEN elb_status_code >= 200 THEN '2xx'
        ELSE 'other'
    END AS status_class
FROM alb_logs
""")

    # ── 2. Charts ─────────────────────────────────────────────────────────────
    print("\n[charts] Creating charts ...")
    chart_meta: list[tuple[int, str]] = []  # (id, name)

    # ── Row 1: Big Numbers ────────────────────────────────────────────────────

    # 1. Total Requests
    ch = create_chart({
        "slice_name": "Total Requests",
        "viz_type": "big_number_total",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "big_number_total",
            "metric": {"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "COUNT(time)"},
            "subheader": "All time",
            "y_axis_format": ",.0f",
        }),
    })
    chart_meta.append(ch)

    # 2. Error Rate (4xx+5xx)
    ch = create_chart({
        "slice_name": "Error Rate (4xx+5xx)",
        "viz_type": "big_number_total",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "100.0 * SUM(CASE WHEN elb_status_code >= 400 THEN 1 ELSE 0 END) / COUNT(*)",
                "label": "Error Rate %",
            },
            "subheader": "% requests with 4xx or 5xx",
            "y_axis_format": ".2f",
        }),
    })
    chart_meta.append(ch)

    # 3. P99 Target Latency
    ch = create_chart({
        "slice_name": "P99 Target Latency (s)",
        "viz_type": "big_number_total",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY target_processing_time)",
                "label": "P99 Latency",
            },
            "subheader": "seconds",
            "y_axis_format": ".3f",
        }),
    })
    chart_meta.append(ch)

    # 4. Avg Bytes Sent
    ch = create_chart({
        "slice_name": "Avg Response Size (KB)",
        "viz_type": "big_number_total",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "AVG(sent_bytes) / 1024.0",
                "label": "Avg KB Sent",
            },
            "subheader": "KB per response",
            "y_axis_format": ".1f",
        }),
    })
    chart_meta.append(ch)

    # ── Row 2: Request Volume + Status Distribution ───────────────────────────

    # 5. Request Volume over Time
    ch = create_chart({
        "slice_name": "Request Volume (per hour)",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "echarts_timeseries_line",
            "x_axis": "time",
            "metrics": [{"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "Requests"}],
            "groupby": [],
            "time_grain_sqla": "PT1H",
            "x_axis_time_format": "%H:%M",
            "y_axis_format": ",.0f",
            "rich_tooltip": True,
            "show_legend": False,
        }),
    })
    chart_meta.append(ch)

    # 6. HTTP Status Code Distribution (Pie)
    ch = create_chart({
        "slice_name": "Status Code Distribution",
        "viz_type": "pie",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "pie",
            "groupby": ["status_class"],
            "metric": {"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "COUNT"},
            "donut": True,
            "show_labels": True,
            "show_legend": True,
            "label_type": "key_percent",
        }),
    })
    chart_meta.append(ch)

    # 7. 5xx Error Rate over Time
    ch = create_chart({
        "slice_name": "5xx Error Rate over Time",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "echarts_timeseries_line",
            "x_axis": "time",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "100.0 * SUM(CASE WHEN elb_status_code >= 500 THEN 1 ELSE 0 END) / COUNT(*)",
                    "label": "5xx Rate %",
                }
            ],
            "groupby": [],
            "time_grain_sqla": "PT1H",
            "x_axis_time_format": "%H:%M",
            "y_axis_format": ".2f",
            "rich_tooltip": True,
        }),
    })
    chart_meta.append(ch)

    # 8. Request Method Distribution
    ch = create_chart({
        "slice_name": "Request Method Distribution",
        "viz_type": "pie",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "pie",
            "groupby": ["request_method"],
            "metric": {"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "COUNT"},
            "donut": False,
            "show_labels": True,
            "show_legend": True,
            "label_type": "key_percent",
        }),
    })
    chart_meta.append(ch)

    # ── Row 3: Latency + Bandwidth ────────────────────────────────────────────

    # 9. Latency Percentiles over Time
    ch = create_chart({
        "slice_name": "Latency Percentiles over Time",
        "viz_type": "echarts_timeseries_line",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "echarts_timeseries_line",
            "x_axis": "time",
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY target_processing_time)", "label": "P50"},
                {"expressionType": "SQL", "sqlExpression": "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY target_processing_time)", "label": "P95"},
                {"expressionType": "SQL", "sqlExpression": "PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY target_processing_time)", "label": "P99"},
            ],
            "groupby": [],
            "time_grain_sqla": "PT1H",
            "x_axis_time_format": "%H:%M",
            "y_axis_format": ".3f",
            "rich_tooltip": True,
            "show_legend": True,
        }),
    })
    chart_meta.append(ch)

    # 10. Bytes Transferred over Time (Area)
    ch = create_chart({
        "slice_name": "Bytes Transferred over Time",
        "viz_type": "echarts_area",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "echarts_area",
            "x_axis": "time",
            "metrics": [
                {"expressionType": "SQL", "sqlExpression": "SUM(received_bytes) / 1024.0 / 1024.0", "label": "Received MB"},
                {"expressionType": "SQL", "sqlExpression": "SUM(sent_bytes)     / 1024.0 / 1024.0", "label": "Sent MB"},
            ],
            "groupby": [],
            "time_grain_sqla": "PT1H",
            "x_axis_time_format": "%H:%M",
            "y_axis_format": ".2f",
            "show_legend": True,
        }),
    })
    chart_meta.append(ch)

    # 11. SSL Protocol Distribution
    ch = create_chart({
        "slice_name": "SSL Protocol Distribution",
        "viz_type": "pie",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "pie",
            "groupby": ["ssl_protocol"],
            "metric": {"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "COUNT"},
            "donut": False,
            "show_labels": True,
            "show_legend": True,
            "label_type": "key_percent",
        }),
    })
    chart_meta.append(ch)

    # 12. Requests by Type over Time (stacked)
    ch = create_chart({
        "slice_name": "Request Method over Time",
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "time",
            "metrics": [{"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "Requests"}],
            "groupby": ["request_method"],
            "time_grain_sqla": "PT1H",
            "stack": True,
            "x_axis_time_format": "%H:%M",
            "y_axis_format": ",.0f",
            "show_legend": True,
        }),
    })
    chart_meta.append(ch)

    # ── Row 4: Top paths + IPs ────────────────────────────────────────────────

    # 13. Top 15 Paths by Request Count
    ch = create_chart({
        "slice_name": "Top 15 Paths by Requests",
        "viz_type": "bar",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "bar",
            "metrics": [{"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "Requests"}],
            "groupby": ["request_path"],
            "row_limit": 15,
            "order_desc": True,
            "orientation": "horizontal",
            "y_axis_format": ",.0f",
            "show_legend": False,
        }),
    })
    chart_meta.append(ch)

    # 14. Top 15 Client IPs
    ch = create_chart({
        "slice_name": "Top 15 Client IPs",
        "viz_type": "bar",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "bar",
            "metrics": [{"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "Requests"}],
            "groupby": ["client_ip"],
            "row_limit": 15,
            "order_desc": True,
            "orientation": "horizontal",
            "y_axis_format": ",.0f",
            "show_legend": False,
        }),
    })
    chart_meta.append(ch)

    # 15. Error Path Breakdown (4xx+5xx)
    ch = create_chart({
        "slice_name": "Error Paths (4xx+5xx)",
        "viz_type": "bar",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "bar",
            "metrics": [{"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "Errors"}],
            "groupby": ["request_path"],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression": "elb_status_code >= 400",
                "filterOptionName": "filter_error",
            }],
            "row_limit": 15,
            "order_desc": True,
            "orientation": "horizontal",
            "y_axis_format": ",.0f",
            "show_legend": False,
        }),
    })
    chart_meta.append(ch)

    # 16. Requests by ELB Name
    ch = create_chart({
        "slice_name": "Requests by ELB",
        "viz_type": "pie",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "pie",
            "groupby": ["elb"],
            "metric": {"aggregate": "COUNT", "column": {"column_name": "time"}, "expressionType": "SIMPLE", "label": "COUNT"},
            "donut": False,
            "show_labels": True,
            "show_legend": True,
            "label_type": "key_percent",
        }),
    })
    chart_meta.append(ch)

    # ── Row 5: Detail Table ───────────────────────────────────────────────────

    # 17. Slow Requests Table (top 50)
    ch = create_chart({
        "slice_name": "Slowest Requests (Top 50)",
        "viz_type": "table",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "table",
            "query_mode": "raw",
            "columns": [
                "time", "elb_status_code", "request_method",
                "request_path", "client_ip",
                "target_processing_time", "sent_bytes", "error_reason",
            ],
            "metrics": [],
            "order_by_cols": ['["target_processing_time", false]'],
            "row_limit": 50,
            "include_time": False,
            "table_timestamp_format": "%Y-%m-%d %H:%M:%S",
            "page_length": 25,
            "show_cell_bars": True,
        }),
    })
    chart_meta.append(ch)

    # 18. Recent Error Requests Table
    ch = create_chart({
        "slice_name": "Recent Errors (5xx)",
        "viz_type": "table",
        "datasource_id": ds_main,
        "datasource_type": "table",
        "params": json.dumps({
            "viz_type": "table",
            "query_mode": "raw",
            "columns": [
                "time", "elb_status_code", "request_method",
                "request_path", "client_ip", "target_ip",
                "target_processing_time", "error_reason",
            ],
            "metrics": [],
            "adhoc_filters": [{
                "expressionType": "SQL",
                "sqlExpression": "elb_status_code >= 500",
                "filterOptionName": "filter_5xx",
            }],
            "order_by_cols": ['["time", false]'],
            "row_limit": 100,
            "include_time": False,
            "table_timestamp_format": "%Y-%m-%d %H:%M:%S",
            "page_length": 25,
        }),
    })
    chart_meta.append(ch)

    # ── 3. Dashboard ──────────────────────────────────────────────────────────
    print(f"\n[dashboard] Creating dashboard with {len(chart_meta)} charts ...")
    db_id = create_dashboard("ALB Access Logs Overview", chart_meta)

    print(f"\n✓ Done! Open: {SUPERSET_URL}/superset/dashboard/{db_id}/")


if __name__ == "__main__":
    main()
