"""Bronze -> Silver: typing, standardisation, validation and de-duplication.

Every transform returns a SilverResult:
  * valid   - typed, standardised, de-duplicated rows ready for Silver
  * invalid - the original raw (string) values + `_dq_reason`, for quarantine
These are pure DataFrame functions so they can be unit-tested without I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Mapping, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ecommerce.quality import DQ_REASON_COL, apply_rules, deduplicate, split_valid_invalid
from ecommerce.schemas import CUSTOMER_SEGMENTS, ORDER_STATUSES, SOURCES

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
MONEY = DecimalType(12, 2)
LINEAGE_COLUMNS = ("_batch_date", "_source_file", "_ingestion_ts", "_row_hash")
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
INTEGER_REGEX = r"^\s*-?\d+\s*$"


@dataclass
class SilverResult:
    valid: DataFrame
    invalid: DataFrame


# --------------------------------------------------------------------------- helpers
def _raw(column: str) -> Column:
    return F.col(f"_raw_{column}")


def clean_string(c: Column) -> Column:
    """Trim; empty strings become NULL."""
    trimmed = F.trim(c)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def parse_ts(c: Column) -> Column:
    return F.to_timestamp(clean_string(c), TS_FORMAT)


def batch_end_ts(batch_date: str) -> Column:
    """Exclusive upper bound of the batch: midnight of the next day (UTC)."""
    next_day = (date.fromisoformat(batch_date) + timedelta(days=1)).isoformat()
    return F.to_timestamp(F.lit(f"{next_day} 00:00:00"), TS_FORMAT)


def _transform(
    bronze: DataFrame,
    table: str,
    typed: Mapping[str, Column],
    rules: Mapping[str, Column],
    dedupe_keys: Sequence[str],
    dedupe_order: Sequence[Column],
    extra_columns: Sequence[str] = (),
) -> SilverResult:
    spec = SOURCES[table]
    staged = bronze.select(
        *[F.col(c).alias(f"_raw_{c}") for c in spec.columns],
        *LINEAGE_COLUMNS,
        *extra_columns,
    )
    staged = staged.select("*", *[expr.alias(name) for name, expr in typed.items()])
    valid, invalid = split_valid_invalid(apply_rules(staged, rules))

    valid = deduplicate(valid, dedupe_keys, dedupe_order).select(*typed.keys(), *LINEAGE_COLUMNS)
    invalid = invalid.select(
        *[_raw(c).alias(c) for c in spec.columns], *LINEAGE_COLUMNS, DQ_REASON_COL
    )
    return SilverResult(valid=valid, invalid=invalid)


def _latest_first(*ts_columns: str) -> list:
    return [F.col(c).desc_nulls_last() for c in ts_columns] + [
        F.col("_ingestion_ts").desc(),
        F.col("_row_hash").desc(),
    ]


# --------------------------------------------------------------------------- orders
def transform_orders(bronze: DataFrame, batch_date: str) -> SilverResult:
    order_ts = parse_ts(_raw("order_ts"))
    typed: Dict[str, Column] = {
        "order_id": clean_string(_raw("order_id")),
        "customer_id": clean_string(_raw("customer_id")),
        "order_ts": order_ts,
        "order_date": F.to_date(order_ts),
        "status": F.lower(clean_string(_raw("status"))),
        "updated_at": parse_ts(_raw("updated_at")),
    }
    end = batch_end_ts(batch_date)
    c = F.col
    rules = {
        "order_id_missing": c("order_id").isNotNull(),
        "customer_id_missing": c("customer_id").isNotNull(),
        "order_ts_invalid": c("order_ts").isNotNull(),
        "order_ts_in_future": c("order_ts").isNull() | (c("order_ts") < end),
        "status_invalid": c("status").isin(*ORDER_STATUSES),
        "updated_at_invalid": c("updated_at").isNotNull(),
        "updated_at_in_future": c("updated_at").isNull() | (c("updated_at") < end),
        "updated_before_order_ts": c("updated_at").isNull()
        | c("order_ts").isNull()
        | (c("updated_at") >= c("order_ts")),
    }
    return _transform(bronze, "orders", typed, rules, ["order_id"], _latest_first("updated_at"))


# --------------------------------------------------------------------------- order items
def transform_order_items(
    bronze: DataFrame,
    batch_date: str,
    known_order_ids: Optional[DataFrame] = None,
    known_product_ids: Optional[DataFrame] = None,
) -> SilverResult:
    """`known_*_ids` are single-column DataFrames used for referential checks."""
    extra = []
    if known_order_ids is not None:
        keys = known_order_ids.select(F.col("order_id").alias("_ref_order_id")).distinct()
        bronze = bronze.join(keys, F.trim(F.col("order_id")) == F.col("_ref_order_id"), "left")
        extra.append("_ref_order_id")
    if known_product_ids is not None:
        keys = known_product_ids.select(F.col("product_id").alias("_ref_product_id")).distinct()
        bronze = bronze.join(keys, F.trim(F.col("product_id")) == F.col("_ref_product_id"), "left")
        extra.append("_ref_product_id")

    raw_qty = clean_string(_raw("quantity"))
    raw_discount = clean_string(_raw("discount_amount"))
    typed: Dict[str, Column] = {
        "order_id": clean_string(_raw("order_id")),
        "product_id": clean_string(_raw("product_id")),
        "quantity": F.when(raw_qty.rlike(INTEGER_REGEX), raw_qty.cast("int")),
        "unit_price": clean_string(_raw("unit_price")).cast(MONEY),
        "discount_amount": F.coalesce(raw_discount.cast(MONEY), F.lit(0).cast(MONEY)),
    }
    c = F.col
    rules = {
        "order_id_missing": c("order_id").isNotNull(),
        "product_id_missing": c("product_id").isNotNull(),
        "quantity_invalid": c("quantity").isNotNull(),
        "quantity_not_positive": c("quantity").isNull() | (c("quantity") > 0),
        "unit_price_invalid": c("unit_price").isNotNull() & (c("unit_price") >= 0),
        "discount_invalid": (raw_discount.isNull() | raw_discount.cast(MONEY).isNotNull())
        & (c("discount_amount") >= 0),
        # only meaningful when quantity and price are themselves valid
        "discount_exceeds_gross": c("quantity").isNull()
        | (c("quantity") <= 0)
        | c("unit_price").isNull()
        | (c("unit_price") < 0)
        | (c("discount_amount") <= c("quantity") * c("unit_price")),
    }
    if known_order_ids is not None:
        rules["orphan_order_id"] = c("order_id").isNull() | c("_ref_order_id").isNotNull()
    if known_product_ids is not None:
        rules["unknown_product_id"] = c("product_id").isNull() | c("_ref_product_id").isNotNull()

    return _transform(
        bronze,
        "order_items",
        typed,
        rules,
        ["order_id", "product_id"],
        _latest_first(),
        extra_columns=extra,
    )


# --------------------------------------------------------------------------- customers
def transform_customers(bronze: DataFrame, batch_date: str) -> SilverResult:
    typed: Dict[str, Column] = {
        "customer_id": clean_string(_raw("customer_id")),
        "customer_name": clean_string(_raw("customer_name")),
        "email": F.lower(clean_string(_raw("email"))),
        # "  new   taipei " -> "New Taipei"
        "city": F.initcap(F.regexp_replace(clean_string(_raw("city")), r"\s+", " ")),
        # "Small Business" / "small-business" -> "small_business"
        "segment": F.regexp_replace(F.lower(clean_string(_raw("segment"))), r"[\s\-]+", "_"),
        "updated_at": parse_ts(_raw("updated_at")),
    }
    end = batch_end_ts(batch_date)
    c = F.col
    rules = {
        "customer_id_missing": c("customer_id").isNotNull(),
        "city_missing": c("city").isNotNull(),
        "segment_invalid": c("segment").isin(*CUSTOMER_SEGMENTS),
        "email_invalid": c("email").isNull() | c("email").rlike(EMAIL_REGEX),
        "updated_at_invalid": c("updated_at").isNotNull(),
        "updated_at_in_future": c("updated_at").isNull() | (c("updated_at") < end),
    }
    return _transform(bronze, "customers", typed, rules, ["customer_id"], _latest_first("updated_at"))


# --------------------------------------------------------------------------- products
def transform_products(bronze: DataFrame, batch_date: str) -> SilverResult:
    typed: Dict[str, Column] = {
        "product_id": clean_string(_raw("product_id")),
        "product_name": clean_string(_raw("product_name")),
        "category": F.lower(clean_string(_raw("category"))),
        "unit_cost": clean_string(_raw("unit_cost")).cast(MONEY),
        "list_price": clean_string(_raw("list_price")).cast(MONEY),
        "updated_at": parse_ts(_raw("updated_at")),
    }
    end = batch_end_ts(batch_date)
    c = F.col
    rules = {
        "product_id_missing": c("product_id").isNotNull(),
        "category_missing": c("category").isNotNull(),
        "unit_cost_invalid": c("unit_cost").isNotNull() & (c("unit_cost") >= 0),
        "list_price_invalid": c("list_price").isNotNull() & (c("list_price") > 0),
        "updated_at_invalid": c("updated_at").isNotNull(),
        "updated_at_in_future": c("updated_at").isNull() | (c("updated_at") < end),
    }
    return _transform(bronze, "products", typed, rules, ["product_id"], _latest_first("updated_at"))
