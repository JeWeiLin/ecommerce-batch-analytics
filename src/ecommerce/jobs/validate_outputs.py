"""Task 5 - validate_outputs.

Post-load checks on Gold (and the Bronze -> Silver row accounting).
Every check result is appended to ops/dq_results; any failed check with
severity "error" fails the task (and therefore the DAG run).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import List, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType, TimestampType

from ecommerce.config import Settings
from ecommerce.delta_utils import delta_exists, read_delta
from ecommerce.quality import DataQualityError
from ecommerce.transforms.gold import AGG_DAILY_KEYS, FACT_KEYS, UNKNOWN_SK

log = logging.getLogger(__name__)
TASK = "validate_outputs"

DQ_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("batch_date", DateType()),
        StructField("check_name", StringType()),
        StructField("table_name", StringType()),
        StructField("severity", StringType()),
        StructField("passed", BooleanType()),
        StructField("observed", StringType()),
        StructField("expected", StringType()),
        StructField("checked_at", TimestampType()),
    ]
)


class CheckCollector:
    def __init__(self, run_id: str, batch_date: str):
        self.run_id = run_id
        self.batch_date = date.fromisoformat(batch_date)
        self.rows: List[tuple] = []

    def add(self, name: str, table: str, passed: bool, observed, expected, severity: str = "error") -> None:
        self.rows.append(
            (self.run_id, self.batch_date, name, table, severity, bool(passed),
             str(observed), str(expected), datetime.now(timezone.utc))
        )
        level = logging.INFO if passed else (logging.ERROR if severity == "error" else logging.WARNING)
        log.log(level, "DQ %-32s %-20s passed=%s observed=%s expected=%s", name, table, passed, observed, expected)

    @property
    def failed_errors(self) -> List[tuple]:
        return [r for r in self.rows if r[4] == "error" and not r[5]]


def duplicate_key_count(df: DataFrame, keys: Sequence[str]) -> int:
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()


def check_scd2(c: CheckCollector, dim: DataFrame, table: str, key: str) -> None:
    multi_current = dim.filter("is_current").groupBy(key).count().filter(F.col("count") > 1).count()
    c.add("single_current_version", table, multi_current == 0, multi_current, 0)

    no_current = dim.groupBy(key).agg(F.max(F.col("is_current").cast("int")).alias("c")).filter("c = 0").count()
    c.add("every_key_has_current", table, no_current == 0, no_current, 0)

    bad_range = dim.filter(F.col("effective_from") >= F.col("effective_to")).count()
    c.add("valid_effective_range", table, bad_range == 0, bad_range, 0)

    w = Window.partitionBy(key).orderBy("effective_from")
    gaps = (
        dim.withColumn("_prev_to", F.lag("effective_to").over(w))
        .filter(F.col("_prev_to").isNotNull() & (F.col("_prev_to") != F.col("effective_from")))
        .count()
    )
    c.add("contiguous_versions", table, gaps == 0, gaps, 0)


def check_row_accounting(spark: SparkSession, settings: Settings, c: CheckCollector, batch_date: str) -> None:
    """bronze rows == silver valid + quarantined + duplicates (per latest silver run)."""
    path = settings.ops("pipeline_metrics")
    if not delta_exists(spark, path):
        return
    w = Window.partitionBy("table_name").orderBy(F.col("recorded_at").desc())
    latest = (
        read_delta(spark, path)
        .filter((F.col("task") == "transform_silver") & (F.col("batch_date") == batch_date) & (F.col("status") == "success"))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .collect()
    )
    for m in latest:
        table = m["table_name"]
        bronze_rows = read_delta(spark, settings.bronze(table)).filter(F.col("_batch_date") == batch_date).count()
        accounted = m["output_rows"] + m["invalid_rows"] + m["duplicate_rows"]
        c.add("bronze_rows_accounted", f"silver.{table}", bronze_rows == accounted, bronze_rows, accounted)

        q_path = settings.quarantine(table)
        if delta_exists(spark, q_path):
            q_rows = read_delta(spark, q_path).filter(F.col("_batch_date") == batch_date).count()
            c.add("quarantine_matches_metrics", f"quarantine.{table}", q_rows == m["invalid_rows"], q_rows, m["invalid_rows"])


def validate(spark: SparkSession, settings: Settings, batch_date: str, run_id: str) -> CheckCollector:
    c = CheckCollector(run_id, batch_date)

    fact = read_delta(spark, settings.gold("fact_sales"))
    agg = read_delta(spark, settings.gold("agg_daily_sales"))

    # --- primary keys ------------------------------------------------------
    dup_fact = duplicate_key_count(fact, FACT_KEYS)
    c.add("primary_key_unique", "gold.fact_sales", dup_fact == 0, dup_fact, 0)
    dup_agg = duplicate_key_count(agg, AGG_DAILY_KEYS)
    c.add("primary_key_unique", "gold.agg_daily_sales", dup_agg == 0, dup_agg, 0)

    # --- SCD2 integrity ----------------------------------------------------
    check_scd2(c, read_delta(spark, settings.gold("dim_customer")), "gold.dim_customer", "customer_id")
    check_scd2(c, read_delta(spark, settings.gold("dim_product")), "gold.dim_product", "product_id")

    # --- amounts -------------------------------------------------------------
    fact_rev = float(fact.agg(F.sum("recognized_revenue")).first()[0] or 0)
    agg_rev = float(agg.agg(F.sum("net_revenue")).first()[0] or 0)
    diff = round(abs(fact_rev - agg_rev), 2)
    c.add("revenue_reconciles_fact_vs_agg", "gold.agg_daily_sales", diff <= settings.revenue_tolerance,
          f"fact={fact_rev:.2f} agg={agg_rev:.2f} diff={diff}", f"diff<={settings.revenue_tolerance}")

    negative = fact.filter((F.col("recognized_revenue") < 0) | (F.col("quantity") <= 0)).count()
    c.add("no_negative_revenue_or_quantity", "gold.fact_sales", negative == 0, negative, 0)

    not_revenue_but_amount = fact.filter(~F.col("is_revenue") & (F.col("recognized_revenue") != 0)).count()
    c.add("non_revenue_status_has_zero_revenue", "gold.fact_sales", not_revenue_but_amount == 0, not_revenue_but_amount, 0)

    # --- batch-level ---------------------------------------------------------
    batch_orders = read_delta(spark, settings.silver("orders")).filter(F.col("_batch_date") == batch_date).count()
    c.add("batch_has_orders", "silver.orders", batch_orders >= settings.min_batch_orders,
          batch_orders, f">={settings.min_batch_orders}", severity="warn")

    batch_fact = fact.filter(F.col("_batch_date") == batch_date)
    total = batch_fact.count()
    unknown = batch_fact.filter((F.col("customer_sk") == UNKNOWN_SK) | (F.col("product_sk") == UNKNOWN_SK)).count()
    ratio = unknown / total if total else 0.0
    c.add("unknown_dimension_ratio", "gold.fact_sales", ratio <= settings.max_unknown_dimension_ratio,
          f"{ratio:.4f} ({unknown}/{total})", f"<={settings.max_unknown_dimension_ratio}", severity="warn")

    check_row_accounting(spark, settings, c, batch_date)
    return c


def run(spark: SparkSession, settings: Settings, batch_date: str, run_id: str, tables: Sequence[str] = ()) -> None:
    c = validate(spark, settings, batch_date, run_id)
    spark.createDataFrame(c.rows, schema=DQ_SCHEMA).write.format("delta").mode("append").save(settings.ops("dq_results"))
    failed = c.failed_errors
    log.info("validate_outputs: %s checks, %s failed (error)", len(c.rows), len(failed))
    if failed:
        names = ", ".join(f"{r[2]}[{r[3]}]" for r in failed)
        raise DataQualityError(f"{len(failed)} output check(s) failed for {batch_date}: {names}")


if __name__ == "__main__":
    from ecommerce.jobs.common import main

    main(TASK, run)
