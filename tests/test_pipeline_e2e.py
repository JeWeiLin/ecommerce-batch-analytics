"""End-to-end: generate data, run the pipeline, re-run days and compare results."""
from __future__ import annotations

import csv

import pytest
from pyspark.sql import functions as F

from ecommerce.datagen import generate
from ecommerce.delta_utils import read_delta
from ecommerce.jobs.run_pipeline import run_batch
from ecommerce.quality import DataQualityError

GOLD = ("dim_customer", "dim_product", "fact_sales", "agg_daily_sales", "agg_customer_orders")
VOLATILE = ("_updated_ts", "_batch_date", "_ingestion_ts", "_run_id", "_source_file")


def snapshot(spark, settings):
    snap = {}
    for t in GOLD:
        df = read_delta(spark, settings.gold(t))
        snap[t] = sorted(tuple(r) for r in df.drop(*[c for c in VOLATILE if c in df.columns]).collect())
    for t in ("orders", "order_items"):
        snap[f"bronze.{t}"] = read_delta(spark, settings.bronze(t)).groupBy("_batch_date").count().orderBy("_batch_date").collect()
        snap[f"quarantine.{t}"] = read_delta(spark, settings.quarantine(t)).count()
    return snap


@pytest.mark.slow
def test_reruns_are_idempotent_and_backfill_safe(spark, settings):
    days = generate(settings.landing_root, "2024-01-01", 4, seed=7)
    for d in days[:3]:
        run_batch(spark, settings, d, run_id=f"first_{d}")
    first = snapshot(spark, settings)

    # same day again, then an *older* day after a newer one was processed
    run_batch(spark, settings, days[2], run_id="rerun_day3")
    run_batch(spark, settings, days[1], run_id="rerun_day2")
    assert snapshot(spark, settings) == first

    # SCD2 history exists and reruns produced no extra versions
    dim_c = read_delta(spark, settings.gold("dim_customer"))
    assert dim_c.filter(~F.col("is_current")).count() > 0
    assert dim_c.filter("is_current").count() == dim_c.select("customer_id").distinct().count()

    # day 4 contains status changes of older orders -> older dates are re-aggregated
    run_batch(spark, settings, days[3], run_id="day4")
    metrics = read_delta(spark, settings.ops("pipeline_metrics"))
    affected = (
        metrics.filter((F.col("run_id") == "day4") & (F.col("table_name") == "agg_daily_sales"))
        .first()["details"]
    )
    assert "2024-01-01" in affected and "2024-01-04" in affected

    fact_rev = read_delta(spark, settings.gold("fact_sales")).agg(F.sum("recognized_revenue")).first()[0]
    agg_rev = read_delta(spark, settings.gold("agg_daily_sales")).agg(F.sum("net_revenue")).first()[0]
    assert fact_rev == agg_rev


@pytest.mark.slow
def test_quality_gate_blocks_bad_batch(spark, settings):
    generate(settings.landing_root, "2024-01-01", 1, seed=7)
    run_batch(spark, settings, "2024-01-01", run_id="good")
    silver_before = read_delta(spark, settings.silver("orders")).count()

    # corrupt the next day: half of the orders have no customer_id
    generate(settings.landing_root, "2024-01-01", 2, seed=7)
    path = f"{settings.landing_dir('2024-01-02')}/orders.csv"
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows[::2]:
        r["customer_id"] = ""
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    with pytest.raises(DataQualityError):
        run_batch(spark, settings, "2024-01-02", run_id="bad")

    # silver untouched, bad rows inspectable in quarantine, failure recorded
    assert read_delta(spark, settings.silver("orders")).count() == silver_before
    q = read_delta(spark, settings.quarantine("orders")).filter("_batch_date = '2024-01-02'")
    assert q.filter(F.col("_dq_reason").contains("customer_id_missing")).count() >= len(rows) // 2
    failed = read_delta(spark, settings.ops("pipeline_metrics")).filter(
        (F.col("run_id") == "bad") & (F.col("status") == "failed")
    )
    assert failed.select("table_name").first()[0] == "orders"

    # after the source is fixed, re-running the same day succeeds
    generate(settings.landing_root, "2024-01-01", 2, seed=7)
    run_batch(spark, settings, "2024-01-02", run_id="fixed")
    assert read_delta(spark, settings.quarantine("orders")).filter("_batch_date = '2024-01-02'").count() < len(rows) // 2
