"""Task 4 - build_gold.

  dim_product          SCD2 (category, unit_cost, list_price) + SCD1 (product_name)
  dim_customer         SCD2 (city, segment) + SCD1 (customer_name, email)
  fact_sales           order-line grain, MERGE for orders touched in this batch
  agg_daily_sales      order_date x category, recomputed only for affected dates
  agg_customer_orders  repurchase metrics, recomputed only for affected customers

Incremental logic: "affected" orders are those inserted/updated in Silver by
this batch. A status change on an old order (e.g. delivered -> refunded)
therefore re-computes the fact rows *and* the daily aggregate of that old
order_date, keeping history correct without a full rebuild.
"""
from __future__ import annotations

import logging
from typing import Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce.config import Settings
from ecommerce.delta_utils import delta_exists, merge_upsert, overwrite_where, read_delta, sql_in_list
from ecommerce.metrics import MetricsRecorder
from ecommerce.transforms.gold import (
    AGG_DAILY_KEYS,
    FACT_KEYS,
    build_agg_customer_orders,
    build_agg_daily_sales,
    build_fact_sales,
)
from ecommerce.transforms.scd2 import CUSTOMER_SCD, PRODUCT_SCD, apply_scd2

log = logging.getLogger(__name__)
TASK = "build_gold"
GOLD_TABLES = ("dim_product", "dim_customer", "fact_sales", "agg_daily_sales", "agg_customer_orders")


def build_dimension(spark: SparkSession, settings: Settings, name: str, batch_date: str, m: dict) -> None:
    source_table, spec = ("products", PRODUCT_SCD) if name == "dim_product" else ("customers", CUSTOMER_SCD)
    updates = read_delta(spark, settings.silver(source_table)).filter(F.col("_batch_date") == batch_date)
    n_updates = updates.count()
    stats = apply_scd2(spark, settings.gold(name), updates, spec, batch_date)
    m.update(input_rows=n_updates, output_rows=stats["inserted"], details=stats)
    log.info("gold.%s: %s", name, stats)


def build_fact(spark: SparkSession, settings: Settings, batch_date: str, m: dict) -> None:
    orders = read_delta(spark, settings.silver("orders"))
    items = read_delta(spark, settings.silver("order_items"))
    affected = (
        orders.filter(F.col("_batch_date") == batch_date).select("order_id")
        .union(items.filter(F.col("_batch_date") == batch_date).select("order_id"))
        .distinct()
    )
    fact_rows = build_fact_sales(
        orders.join(affected, "order_id", "left_semi"),
        items.join(affected, "order_id", "left_semi"),
        read_delta(spark, settings.gold("dim_customer")),
        read_delta(spark, settings.gold("dim_product")),
        batch_date,
    ).cache()
    try:
        n_rows = fact_rows.count()
        merge_upsert(fact_rows, settings.gold("fact_sales"), keys=FACT_KEYS)
        m.update(input_rows=affected.count(), output_rows=n_rows)
        log.info("gold.fact_sales: upserted %s rows", n_rows)
    finally:
        fact_rows.unpersist()


def _batch_fact(spark: SparkSession, settings: Settings, batch_date: str):
    fact = read_delta(spark, settings.gold("fact_sales"))
    return fact, fact.filter(F.col("_batch_date") == batch_date)


def build_daily_agg(spark: SparkSession, settings: Settings, batch_date: str, m: dict) -> None:
    fact, batch_rows = _batch_fact(spark, settings, batch_date)
    dates = sorted(r["order_date"] for r in batch_rows.select("order_date").distinct().collect())
    if not dates:
        m.update(input_rows=0, output_rows=0, message="no affected dates")
        return
    agg = build_agg_daily_sales(fact.filter(F.col("order_date").isin(dates))).cache()
    try:
        overwrite_where(agg, settings.gold("agg_daily_sales"), f"order_date IN {sql_in_list(dates)}")
        m.update(input_rows=len(dates), output_rows=agg.count(), details={"affected_dates": [str(d) for d in dates]})
    finally:
        agg.unpersist()


def build_customer_agg(spark: SparkSession, settings: Settings, batch_date: str, m: dict) -> None:
    fact, batch_rows = _batch_fact(spark, settings, batch_date)
    customers = batch_rows.select("customer_id").distinct()
    agg = build_agg_customer_orders(fact.join(customers, "customer_id", "left_semi"), customers).cache()
    try:
        merge_upsert(agg, settings.gold("agg_customer_orders"), keys=["customer_id"])
        n = agg.count()
        m.update(input_rows=n, output_rows=n)
    finally:
        agg.unpersist()


def run(spark: SparkSession, settings: Settings, batch_date: str, run_id: str, tables: Sequence[str] = ()) -> None:
    tables = tables or GOLD_TABLES
    metrics = MetricsRecorder(spark, settings, TASK, batch_date, run_id)
    try:
        for name in ("dim_product", "dim_customer"):
            if name in tables:
                with metrics.track(name) as m:
                    build_dimension(spark, settings, name, batch_date, m)
        if "fact_sales" in tables:
            with metrics.track("fact_sales") as m:
                build_fact(spark, settings, batch_date, m)
        if not delta_exists(spark, settings.gold("fact_sales")):
            log.warning("fact_sales does not exist yet; skipping aggregates")
            return
        if "agg_daily_sales" in tables:
            with metrics.track("agg_daily_sales") as m:
                build_daily_agg(spark, settings, batch_date, m)
        if "agg_customer_orders" in tables:
            with metrics.track("agg_customer_orders") as m:
                build_customer_agg(spark, settings, batch_date, m)
    finally:
        metrics.flush()


if __name__ == "__main__":
    from ecommerce.jobs.common import main

    main(TASK, run, allowed_tables=GOLD_TABLES)
