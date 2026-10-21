from __future__ import annotations

from datetime import date
from decimal import Decimal as D

from conftest import ts

from ecommerce.transforms.gold import build_agg_customer_orders, build_agg_daily_sales, build_fact_sales

HIGH = ts("9999-12-31 00:00:00")
ORDERS_SCHEMA = "order_id string, customer_id string, order_ts timestamp, order_date date, status string, updated_at timestamp"
ITEMS_SCHEMA = "order_id string, product_id string, quantity int, unit_price decimal(12,2), discount_amount decimal(12,2)"
DIM_P_SCHEMA = ("product_sk long, product_id string, category string, unit_cost decimal(12,2), "
                "effective_from timestamp, effective_to timestamp, is_current boolean")
DIM_C_SCHEMA = ("customer_sk long, customer_id string, city string, segment string, "
                "effective_from timestamp, effective_to timestamp, is_current boolean")


def _fixture(spark):
    d = date(2024, 1, 2)
    orders = spark.createDataFrame([
        ("O1", "C1", ts("2024-01-02 09:00:00"), d, "paid", ts("2024-01-02 09:00:00")),
        ("O2", "C1", ts("2024-01-02 15:00:00"), d, "delivered", ts("2024-01-02 15:00:00")),
        ("O3", "C2", ts("2024-01-02 16:00:00"), d, "cancelled", ts("2024-01-02 16:00:00")),
        ("O4", "C2", ts("2024-01-02 17:00:00"), d, "refunded", ts("2024-01-02 17:00:00")),
        ("O5", "C2", ts("2024-01-02 18:00:00"), d, "placed", ts("2024-01-02 18:00:00")),
    ], ORDERS_SCHEMA)
    items = spark.createDataFrame([
        ("O1", "P1", 2, D(100), D(0)), ("O2", "P1", 1, D(120), D(20)), ("O3", "P1", 1, D(100), D(0)),
        ("O4", "P1", 1, D(100), D(0)), ("O5", "P1", 1, D(100), D(0)),
    ], ITEMS_SCHEMA)
    # unit cost of P1 changed at 12:00 -> O1 uses 60, O2 uses 70
    dim_p = spark.createDataFrame([
        (11, "P1", "home", D(60), ts("2024-01-01 00:00:00"), ts("2024-01-02 12:00:00"), False),
        (12, "P1", "home", D(70), ts("2024-01-02 12:00:00"), HIGH, True),
    ], DIM_P_SCHEMA)
    dim_c = spark.createDataFrame([
        (21, "C1", "Taipei", "consumer", ts("2024-01-01 00:00:00"), ts("2024-01-02 10:00:00"), False),
        (22, "C1", "Taipei", "corporate", ts("2024-01-02 10:00:00"), HIGH, True),
        (23, "C2", "Tainan", "consumer", ts("2024-01-01 00:00:00"), HIGH, True),
    ], DIM_C_SCHEMA)
    return build_fact_sales(orders, items, dim_c, dim_p, "2024-01-02")


def test_fact_revenue_rules_and_point_in_time_dimensions(spark):
    fact = {r.order_id: r for r in _fixture(spark).collect()}

    o1, o2 = fact["O1"], fact["O2"]
    assert float(o1.gross_amount) == 200 and float(o1.recognized_revenue) == 200
    assert float(o1.unit_cost) == 60 and float(o1.gross_profit) == 80
    assert o1.customer_sk == 21 and o1.customer_segment == "consumer"   # version valid at 09:00
    assert float(o2.net_amount) == 100 and float(o2.unit_cost) == 70 and float(o2.gross_profit) == 30
    assert o2.customer_sk == 22 and o2.customer_segment == "corporate"

    for oid in ("O3", "O4", "O5"):  # cancelled / refunded / unpaid -> no revenue
        assert not fact[oid].is_revenue
        assert float(fact[oid].recognized_revenue) == 0 and float(fact[oid].gross_profit) == 0
    assert float(fact["O4"].refunded_amount) == 100


def test_daily_and_customer_aggregates(spark):
    fact = _fixture(spark)
    agg = build_agg_daily_sales(fact).collect()
    assert len(agg) == 1
    a = agg[0]
    assert a.order_count == 2 and a.units_sold == 3
    assert float(a.net_revenue) == 300 and float(a.gross_profit) == 110
    assert float(a.avg_order_value) == 150 and a.cancelled_order_count == 1
    assert float(a.refunded_amount) == 100

    customers = fact.select("customer_id").distinct()
    cust = {r.customer_id: r for r in build_agg_customer_orders(fact, customers).collect()}
    assert cust["C1"].order_count == 2 and cust["C1"].is_repeat_customer
    assert cust["C2"].order_count == 0 and not cust["C2"].is_repeat_customer
