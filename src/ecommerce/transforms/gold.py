"""Silver -> Gold business logic (pure DataFrame functions).

Metric definitions (also in docs/data_model.md):
  gross_amount       = quantity * unit_price
  net_amount         = gross_amount - discount_amount
  recognized_revenue = net_amount   if status in (paid, shipped, delivered) else 0
  cost_amount        = quantity * unit_cost   (unit_cost as of the order time)
  gross_profit       = recognized_revenue - cost_amount  for revenue statuses, else 0
  refunded_amount    = net_amount   if status = refunded else 0
  placed (unpaid) / cancelled / refunded orders contribute no revenue.
"""
from __future__ import annotations

from typing import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ecommerce.schemas import REVENUE_STATUSES

MONEY = DecimalType(14, 2)
AGG_MONEY = DecimalType(18, 2)
UNKNOWN_SK = -1

FACT_KEYS = ("order_id", "product_id")
AGG_DAILY_KEYS = ("order_date", "category")


def point_in_time_lookup(
    df: DataFrame,
    dim: DataFrame,
    key: str,
    ts_col: str,
    columns: Mapping[str, str],
) -> DataFrame:
    """Attach SCD2 attributes valid at `ts_col`.

    `columns` maps dimension column -> output column. If no version covers
    the timestamp (dimension arrived late), fall back to the current version.
    """
    pit = dim.select(
        F.col(key).alias("_pit_key"),
        F.col("effective_from").alias("_pit_from"),
        F.col("effective_to").alias("_pit_to"),
        *[F.col(c).alias(f"_pit_{c}") for c in columns],
    )
    cur = dim.filter("is_current").select(
        F.col(key).alias("_cur_key"), *[F.col(c).alias(f"_cur_{c}") for c in columns]
    )
    out = df.join(
        pit,
        (F.col(key) == F.col("_pit_key"))
        & (F.col(ts_col) >= F.col("_pit_from"))
        & (F.col(ts_col) < F.col("_pit_to")),
        "left",
    ).join(cur, F.col(key) == F.col("_cur_key"), "left")
    for src, dst in columns.items():
        out = out.withColumn(dst, F.coalesce(F.col(f"_pit_{src}"), F.col(f"_cur_{src}")))
    helper_cols = ["_pit_key", "_pit_from", "_pit_to", "_cur_key"]
    helper_cols += [f"_pit_{c}" for c in columns] + [f"_cur_{c}" for c in columns]
    return out.drop(*helper_cols)


def build_fact_sales(
    orders: DataFrame,
    order_items: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    batch_date: str,
) -> DataFrame:
    """Grain: one row per order line (order_id, product_id)."""
    o = orders.select(
        "order_id",
        "customer_id",
        "order_ts",
        "order_date",
        F.col("status").alias("order_status"),
        F.col("updated_at").alias("order_updated_at"),
    )
    i = order_items.select("order_id", "product_id", "quantity", "unit_price", "discount_amount")
    df = i.join(o, "order_id", "inner")

    df = point_in_time_lookup(
        df, dim_product, "product_id", "order_ts",
        {"product_sk": "product_sk", "category": "category", "unit_cost": "unit_cost"},
    )
    df = point_in_time_lookup(
        df, dim_customer, "customer_id", "order_ts",
        {"customer_sk": "customer_sk", "segment": "customer_segment", "city": "customer_city"},
    )

    is_revenue = F.col("order_status").isin(*REVENUE_STATUSES)
    gross = (F.col("quantity") * F.col("unit_price")).cast(MONEY)
    net = (gross - F.col("discount_amount")).cast(MONEY)
    cost = (F.col("quantity") * F.col("unit_cost")).cast(MONEY)
    zero = F.lit(0).cast(MONEY)

    return df.select(
        "order_id",
        "product_id",
        "customer_id",
        F.coalesce("customer_sk", F.lit(UNKNOWN_SK).cast("long")).alias("customer_sk"),
        F.coalesce("product_sk", F.lit(UNKNOWN_SK).cast("long")).alias("product_sk"),
        "order_ts",
        "order_date",
        "order_status",
        F.coalesce("category", F.lit("unknown")).alias("category"),
        "customer_segment",
        "customer_city",
        "quantity",
        F.col("unit_price").cast(MONEY).alias("unit_price"),
        F.col("unit_cost").cast(MONEY).alias("unit_cost"),
        gross.alias("gross_amount"),
        F.col("discount_amount").cast(MONEY).alias("discount_amount"),
        net.alias("net_amount"),
        is_revenue.alias("is_revenue"),
        F.when(is_revenue, net).otherwise(zero).alias("recognized_revenue"),
        cost.alias("cost_amount"),
        F.when(is_revenue, (net - cost).cast(MONEY)).otherwise(zero).alias("gross_profit"),
        F.when(F.col("order_status") == "refunded", net).otherwise(zero).alias("refunded_amount"),
        "order_updated_at",
        F.to_date(F.lit(batch_date)).alias("_batch_date"),
        F.current_timestamp().alias("_updated_ts"),
    )


def build_agg_daily_sales(fact: DataFrame) -> DataFrame:
    """Grain: order_date x category.

    avg_order_value = net_revenue / number of revenue orders that contain the
    category (category-attributed AOV).
    """
    rev = F.col("is_revenue")

    def rev_sum(col: str):
        return F.sum(F.when(rev, F.col(col)).otherwise(0)).cast(AGG_MONEY)

    agg = fact.groupBy("order_date", "category").agg(
        F.countDistinct(F.when(rev, F.col("order_id"))).alias("order_count"),
        F.sum(F.when(rev, F.col("quantity")).otherwise(0)).cast("long").alias("units_sold"),
        rev_sum("gross_amount").alias("gross_revenue"),
        rev_sum("discount_amount").alias("discount_amount"),
        F.sum("recognized_revenue").cast(AGG_MONEY).alias("net_revenue"),
        rev_sum("cost_amount").alias("cost_amount"),
        F.sum("gross_profit").cast(AGG_MONEY).alias("gross_profit"),
        F.countDistinct(F.when(F.col("order_status") == "cancelled", F.col("order_id"))).alias(
            "cancelled_order_count"
        ),
        F.sum("refunded_amount").cast(AGG_MONEY).alias("refunded_amount"),
    )
    return agg.select(
        "*",
        F.when(F.col("order_count") > 0, F.round(F.col("net_revenue") / F.col("order_count"), 2))
        .cast(AGG_MONEY)
        .alias("avg_order_value"),
        F.when(F.col("net_revenue") > 0, F.round(F.col("gross_profit") / F.col("net_revenue"), 4))
        .cast("double")
        .alias("gross_margin_pct"),
        F.current_timestamp().alias("_updated_ts"),
    )


def build_agg_customer_orders(fact: DataFrame, customer_ids: DataFrame) -> DataFrame:
    """Repurchase behaviour per customer (grain: customer_id).

    `customer_ids` lists the customers to (re)compute; customers whose orders
    were all cancelled/refunded get a row with zero orders.
    """
    orders = (
        fact.filter("is_revenue")
        .groupBy("customer_id", "order_id", "order_date")
        .agg(F.sum("recognized_revenue").alias("order_revenue"))
    )
    w = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
    ranked = orders.withColumn("_order_seq", F.row_number().over(w))
    stats = ranked.groupBy("customer_id").agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.min(F.when(F.col("_order_seq") == 2, F.col("order_date"))).alias("second_order_date"),
        F.count("order_id").cast("long").alias("order_count"),
        F.sum("order_revenue").cast(AGG_MONEY).alias("lifetime_revenue"),
    )
    out = customer_ids.select("customer_id").distinct().join(stats, "customer_id", "left")
    return out.select(
        "customer_id",
        "first_order_date",
        "second_order_date",
        "last_order_date",
        F.coalesce("order_count", F.lit(0).cast("long")).alias("order_count"),
        F.coalesce("lifetime_revenue", F.lit(0).cast(AGG_MONEY)).alias("lifetime_revenue"),
        (F.coalesce("order_count", F.lit(0)) >= 2).alias("is_repeat_customer"),
        F.datediff("second_order_date", "first_order_date").alias("days_to_second_order"),
        F.current_timestamp().alias("_updated_ts"),
    )
