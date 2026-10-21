from __future__ import annotations

from conftest import bronze_df

from ecommerce.quality import DataQualityError, check_invalid_ratio
from ecommerce.transforms.silver import (
    transform_customers,
    transform_order_items,
    transform_orders,
    transform_products,
)

import pytest

BATCH = "2024-01-01"


def reasons(result, key):
    return {r[key]: r["_dq_reason"] for r in result.invalid.collect()}


def test_orders_validation_and_latest_version_wins(spark):
    rows = [
        {"order_id": "O1", "customer_id": "C1", "order_ts": "2024-01-01 09:00:00", "status": "placed", "updated_at": "2024-01-01 09:00:00"},
        {"order_id": "O1", "customer_id": "C1", "order_ts": "2024-01-01 09:00:00", "status": " PAID ", "updated_at": "2024-01-01 10:00:00"},
        {"order_id": "O1", "customer_id": "C1", "order_ts": "2024-01-01 09:00:00", "status": " PAID ", "updated_at": "2024-01-01 10:00:00"},
        {"order_id": "O2", "customer_id": "", "order_ts": "2024-01-01 09:00:00", "status": "paid", "updated_at": "2024-01-01 09:00:00"},
        {"order_id": "O3", "customer_id": "C1", "order_ts": "2024-13-45 09:00:00", "status": "paid", "updated_at": "2024-01-01 09:00:00"},
        {"order_id": "O4", "customer_id": "C1", "order_ts": "2024-01-01 09:00:00", "status": "lost", "updated_at": "2024-01-01 09:00:00"},
        {"order_id": "O5", "customer_id": "C1", "order_ts": "2024-01-02 00:00:00", "status": "paid", "updated_at": "2024-01-02 00:00:00"},
        {"order_id": "O6", "customer_id": "C1", "order_ts": "2024-01-01 09:00:00", "status": "paid", "updated_at": "2024-01-01 08:00:00"},
    ]
    result = transform_orders(bronze_df(spark, "orders", rows), BATCH)

    valid = result.valid.collect()
    assert [r.order_id for r in valid] == ["O1"]
    assert valid[0].status == "paid"  # trimmed, lower-cased, latest updated_at
    assert str(valid[0].order_date) == "2024-01-01"

    r = reasons(result, "order_id")
    assert r["O2"] == "customer_id_missing"
    assert "order_ts_invalid" in r["O3"]
    assert r["O4"] == "status_invalid"
    assert "order_ts_in_future" in r["O5"]
    assert r["O6"] == "updated_before_order_ts"
    # quarantine keeps the raw source values
    assert {row.order_ts for row in result.invalid.collect() if row.order_id == "O3"} == {"2024-13-45 09:00:00"}


def test_order_items_rules_and_referential_integrity(spark):
    known_orders = spark.createDataFrame([("O1",), ("O2",)], "order_id string")
    known_products = spark.createDataFrame([("P1",), ("P2",), ("P3",)], "product_id string")
    rows = [
        {"order_id": "O1", "product_id": "P1", "quantity": "2", "unit_price": "100.00", "discount_amount": "10"},
        {"order_id": "O1", "product_id": "P1", "quantity": "2", "unit_price": "100.00", "discount_amount": "10"},  # dup
        {"order_id": "O1", "product_id": "P2", "quantity": "0", "unit_price": "50", "discount_amount": ""},
        {"order_id": "O2", "product_id": "P2", "quantity": "1.5", "unit_price": "50", "discount_amount": ""},
        {"order_id": "O2", "product_id": "P3", "quantity": "1", "unit_price": "50", "discount_amount": "60"},
        {"order_id": "OX", "product_id": "P1", "quantity": "1", "unit_price": "50", "discount_amount": ""},
        {"order_id": "O2", "product_id": "PX", "quantity": "1", "unit_price": "-5", "discount_amount": ""},
    ]
    result = transform_order_items(bronze_df(spark, "order_items", rows), BATCH, known_orders, known_products)

    valid = result.valid.collect()
    assert len(valid) == 1
    assert (valid[0].order_id, valid[0].product_id, valid[0].quantity) == ("O1", "P1", 2)
    assert float(valid[0].discount_amount) == 10.0

    by_key = {(r.order_id, r.product_id): r._dq_reason for r in result.invalid.collect()}
    assert by_key[("O1", "P2")] == "quantity_not_positive"
    assert by_key[("O2", "P2")] == "quantity_invalid"
    assert by_key[("O2", "P3")] == "discount_exceeds_gross"
    assert by_key[("OX", "P1")] == "orphan_order_id"
    assert set(by_key[("O2", "PX")].split(";")) == {"unit_price_invalid", "unknown_product_id"}


def test_customers_are_standardised(spark):
    rows = [
        {"customer_id": "C1", "customer_name": "A", "email": "A@Example.COM", "city": "  new   taipei ", "segment": "Small Business", "updated_at": "2024-01-01 01:00:00"},
        {"customer_id": "C2", "customer_name": "B", "email": "not-an-email", "city": "Taipei", "segment": "consumer", "updated_at": "2024-01-01 01:00:00"},
        {"customer_id": "C3", "customer_name": "C", "email": None, "city": "Taipei", "segment": "vip", "updated_at": "2024-01-01 01:00:00"},
    ]
    result = transform_customers(bronze_df(spark, "customers", rows), BATCH)
    c1 = result.valid.collect()[0]
    assert (c1.customer_id, c1.city, c1.segment, c1.email) == ("C1", "New Taipei", "small_business", "a@example.com")
    r = reasons(result, "customer_id")
    assert r == {"C2": "email_invalid", "C3": "segment_invalid"}


def test_products_validation(spark):
    rows = [
        {"product_id": "P1", "product_name": "X", "category": " Home ", "unit_cost": "10.5", "list_price": "20", "updated_at": "2024-01-01 00:01:00"},
        {"product_id": "P2", "product_name": "Y", "category": "home", "unit_cost": "-1", "list_price": "0", "updated_at": "2024-01-01 00:01:00"},
    ]
    result = transform_products(bronze_df(spark, "products", rows), BATCH)
    assert [(r.product_id, r.category) for r in result.valid.collect()] == [("P1", "home")]
    assert set(reasons(result, "product_id")["P2"].split(";")) == {"unit_cost_invalid", "list_price_invalid"}


def test_invalid_ratio_gate():
    assert check_invalid_ratio("orders", 100, 5, 0.05) == 0.05
    with pytest.raises(DataQualityError):
        check_invalid_ratio("orders", 100, 6, 0.05)
    assert check_invalid_ratio("orders", 0, 0, 0.05) == 0.0
