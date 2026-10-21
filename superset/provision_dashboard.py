"""Create the Superset database connection, datasets, charts and dashboard via the REST API.

Run inside the superset container (make analytics does this):
    docker compose exec superset /app/.venv/bin/python /app/provision/provision_dashboard.py

Idempotent: the Trino connection and datasets are reused; the dashboard and its charts
are deleted and re-created, so edits to this file are applied on every run.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid

import requests

BASE = os.getenv("SUPERSET_URL", "http://localhost:8088")
USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
DB_NAME = "Trino Lakehouse"
DB_URI = "trino://superset@trino:8080/delta"
DASHBOARD_TITLE = "E-commerce Sales Overview"
DASHBOARD_SLUG = "ecommerce-sales"

s = requests.Session()


# ------------------------------------------------------------------ API helpers
def api(method: str, path: str, **kwargs) -> dict:
    r = s.request(method, f"{BASE}/api/v1/{path}", timeout=60, **kwargs)
    if r.status_code >= 400:
        sys.exit(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.content else {}


def find(resource: str, column: str, value: str) -> list:
    rison = f"(filters:!((col:{column},opr:eq,value:'{value}')),page_size:100)"
    return api("GET", f"{resource}/", params={"q": rison})["result"]


def login() -> None:
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/health", timeout=5).ok:
                break
        except requests.ConnectionError:
            pass
        print("waiting for Superset...")
        time.sleep(3)
    token = api("POST", "security/login",
                json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True})["access_token"]
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["X-CSRFToken"] = api("GET", "security/csrf_token/")["result"]
    s.headers["Referer"] = BASE


# ------------------------------------------------------------------ database + datasets
def ensure_database() -> int:
    existing = find("database", "database_name", DB_NAME)
    if existing:
        return existing[0]["id"]
    return api("POST", "database/", json={
        "database_name": DB_NAME,
        "sqlalchemy_uri": DB_URI,
        "expose_in_sqllab": True,
    })["id"]


def ensure_dataset(db_id: int, schema: str, table: str, sql: str | None = None) -> int:
    """Physical dataset for a Trino table, or a virtual (SQL) dataset when `sql` is given."""
    for d in find("dataset", "table_name", table):
        if d.get("schema") == schema:
            if sql is None:
                api("PUT", f"dataset/{d['id']}/refresh")  # pick up new columns
            return d["id"]
    payload = {"database": db_id, "schema": schema, "table_name": table}
    if sql:
        payload["sql"] = sql
    return api("POST", "dataset/", json=payload)["id"]


# fact_sales + product name (product_name is SCD1, identical on every version)
PRODUCT_SALES_SQL = """
SELECT f.*, p.product_name
FROM gold.fact_sales f
LEFT JOIN gold.dim_product p ON p.product_sk = f.product_sk
"""


# ------------------------------------------------------------------ chart definitions
def metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "optionName": f"metric_{uuid.uuid4().hex[:8]}"}


def where(sql: str) -> dict:
    return {"expressionType": "SQL", "clause": "WHERE", "sqlExpression": sql,
            "filterOptionName": f"filter_{uuid.uuid4().hex[:8]}"}


NET_REVENUE = metric("SUM(net_revenue)", "Net revenue")
GROSS_PROFIT = metric("SUM(gross_profit)", "Gross profit")
FACT_REVENUE = metric("SUM(recognized_revenue)", "Net revenue")
PAID = where("is_revenue")


def charts(ds: dict) -> list:
    """(name, dataset, viz_type, params, width, row) for every chart on the dashboard."""
    return [
        # ---- row 1: headline numbers
        ("Net revenue", "agg_daily_sales", "big_number_total",
         {"metric": NET_REVENUE, "y_axis_format": "SMART_NUMBER", "subheader": "paid · shipped · delivered"}, 3, 1),
        ("Paid orders", "fact_sales", "big_number_total",
         {"metric": metric("COUNT(DISTINCT order_id)", "Orders"), "adhoc_filters": [PAID],
          "y_axis_format": ",d", "subheader": "orders with recognised revenue"}, 3, 1),
        ("Average order value", "fact_sales", "big_number_total",
         {"metric": metric("SUM(recognized_revenue) / COUNT(DISTINCT order_id)", "AOV"),
          "adhoc_filters": [PAID], "y_axis_format": ",.0f", "subheader": "net revenue per paid order"}, 3, 1),
        ("Repeat customer rate", "agg_customer_orders", "big_number_total",
         {"metric": metric("1.0 * COUNT_IF(is_repeat_customer) / NULLIF(COUNT_IF(order_count > 0), 0)", "Repeat rate"),
          "y_axis_format": ".1%", "subheader": "buyers with 2+ paid orders"}, 3, 1),
        # ---- row 2: trend + segment mix
        ("Daily revenue & gross profit", "agg_daily_sales", "echarts_timeseries_line",
         {"x_axis": "order_date", "time_grain_sqla": "P1D", "metrics": [NET_REVENUE, GROSS_PROFIT],
          "groupby": [], "y_axis_format": "SMART_NUMBER", "markerEnabled": True, "show_legend": True,
          "rich_tooltip": True, "row_limit": 1000}, 8, 2),
        ("Revenue by customer segment (at order time)", "fact_sales", "pie",
         {"groupby": ["customer_segment"], "metric": FACT_REVENUE, "adhoc_filters": [PAID],
          "donut": True, "show_labels": True, "label_type": "percent", "labels_outside": False,
          "number_format": "SMART_NUMBER", "show_legend": True, "legendOrientation": "top",
          "legendType": "plain", "row_limit": 100}, 4, 2),
        # ---- row 3: category + products
        ("Revenue & profit by category", "agg_daily_sales", "echarts_timeseries_bar",
         {"x_axis": "category", "metrics": [NET_REVENUE, GROSS_PROFIT], "groupby": [],
          "x_axis_sort": "Net revenue", "x_axis_sort_asc": False, "y_axis_format": "SMART_NUMBER",
          "show_legend": True, "rich_tooltip": True, "row_limit": 100}, 6, 3),
        ("Top 10 products", "product_sales", "table",
         {"query_mode": "aggregate", "groupby": ["product_name", "category"],
          "metrics": [FACT_REVENUE, metric("SUM(quantity)", "Units"), metric("SUM(gross_profit)", "Gross profit")],
          "adhoc_filters": [PAID], "timeseries_limit_metric": FACT_REVENUE, "order_desc": True,
          "row_limit": 10, "server_pagination": False, "table_timestamp_format": "smart_date"}, 6, 3),
        # ---- row 4: data quality
        ("Quarantined rows per batch (data quality)", "pipeline_metrics", "echarts_timeseries_bar",
         {"x_axis": "batch_date", "time_grain_sqla": "P1D", "metrics": [metric("SUM(invalid_rows)", "Invalid rows")],
          "groupby": ["table_name"], "stack": "Stack",
          "adhoc_filters": [where("task = 'transform_silver' AND status = 'success'")],
          "y_axis_format": ",d", "show_legend": True, "rich_tooltip": True, "row_limit": 1000}, 12, 4),
    ]


# ------------------------------------------------------------------ dashboard
def delete_previous() -> None:
    for d in find("dashboard", "slug", DASHBOARD_SLUG):
        for c in api("GET", f"dashboard/{d['id']}/charts")["result"]:
            api("DELETE", f"chart/{c['id']}")
        api("DELETE", f"dashboard/{d['id']}")


def layout(placed: list) -> dict:
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    heights = {1: 28, 2: 60, 3: 60, 4: 50}
    for row in sorted({p[3] for p in placed}):
        row_id = f"ROW-{row}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for chart_id, name, width, r in placed:
            if r != row:
                continue
            key = f"CHART-{chart_id}"
            pos[row_id]["children"].append(key)
            pos[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                        "meta": {"chartId": chart_id, "width": width, "height": heights[row], "sliceName": name}}
    return pos


def main() -> None:
    login()
    db_id = ensure_database()
    datasets = {
        t: ensure_dataset(db_id, "gold", t) for t in ("agg_daily_sales", "agg_customer_orders", "fact_sales")
    }
    datasets["pipeline_metrics"] = ensure_dataset(db_id, "ops", "pipeline_metrics")
    datasets["product_sales"] = ensure_dataset(db_id, "gold", "product_sales", sql=PRODUCT_SALES_SQL)
    print(f"database id={db_id}, datasets={datasets}")

    delete_previous()
    dash_id = api("POST", "dashboard/", json={
        "dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True,
    })["id"]

    placed = []
    for name, table, viz, params, width, row in charts(datasets):
        ds_id = datasets[table]
        form = {"datasource": f"{ds_id}__table", "viz_type": viz, "adhoc_filters": [], **params}
        chart_id = api("POST", "chart/", json={
            "slice_name": name, "viz_type": viz, "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(form), "dashboards": [dash_id],
        })["id"]
        placed.append((chart_id, name, width, row))
        print(f"chart {chart_id:>3}  {name}")

    api("PUT", f"dashboard/{dash_id}", json={
        "position_json": json.dumps(layout(placed)),
        "json_metadata": json.dumps({"color_scheme": "supersetColors", "refresh_frequency": 0,
                                     "expanded_slices": {}, "timed_refresh_immune_slices": []}),
    })
    print(f"dashboard ready: {os.getenv('SUPERSET_PUBLIC_URL', 'http://localhost:8088')}/superset/dashboard/{DASHBOARD_SLUG}/")


if __name__ == "__main__":
    main()
