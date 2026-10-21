"""Task 3 - transform_silver.

For each source table (in dependency order products -> customers -> orders
-> order_items):
  1. read only this batch's Bronze partition       (incremental)
  2. type, standardise, validate, de-duplicate
  3. write rejected rows to quarantine/<table>       (replaceWhere, idempotent)
  4. enforce the invalid-ratio threshold             (fail fast, before Silver)
  5. write Silver
       products / customers : cleaned change log, partitioned by _batch_date
                              (input for SCD2 in Gold)
       orders / order_items : current-state tables via MERGE upsert
                              (newer updated_at wins -> late re-runs are safe)
"""
from __future__ import annotations

import logging
from typing import Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce.config import Settings
from ecommerce.delta_utils import merge_upsert, overwrite_where, read_delta
from ecommerce.metrics import MetricsRecorder
from ecommerce.quality import check_invalid_ratio
from ecommerce.schemas import SILVER_TABLE_ORDER
from ecommerce.transforms import silver as T

log = logging.getLogger(__name__)
TASK = "transform_silver"


def _transform(spark: SparkSession, settings: Settings, table: str, batch_date: str, bronze):
    if table == "orders":
        return T.transform_orders(bronze, batch_date)
    if table == "customers":
        return T.transform_customers(bronze, batch_date)
    if table == "products":
        return T.transform_products(bronze, batch_date)
    if table == "order_items":
        known_orders = read_delta(spark, settings.silver("orders")).select("order_id")
        known_products = (
            read_delta(spark, settings.silver("products"))
            .filter(F.col("_batch_date") <= batch_date)
            .select("product_id")
        )
        return T.transform_order_items(bronze, batch_date, known_orders, known_products)
    raise ValueError(table)


def _write_silver(result_valid, settings: Settings, table: str, batch_date: str) -> None:
    path = settings.silver(table)
    if table in ("products", "customers"):
        overwrite_where(result_valid, path, f"_batch_date = '{batch_date}'", partition_by=["_batch_date"])
    elif table == "orders":
        # Only move forward in time; equal timestamps (= same batch re-run) are re-applied.
        merge_upsert(
            result_valid,
            path,
            keys=["order_id"],
            update_condition="s.updated_at > t.updated_at OR "
            "(s.updated_at = t.updated_at AND s._batch_date >= t._batch_date)",
        )
    elif table == "order_items":
        merge_upsert(result_valid, path, keys=["order_id", "product_id"])


def process_table(spark: SparkSession, settings: Settings, table: str, batch_date: str, m: dict) -> None:
    bronze = read_delta(spark, settings.bronze(table)).filter(F.col("_batch_date") == batch_date)
    result = _transform(spark, settings, table, batch_date, bronze)
    valid = result.valid.cache()
    invalid = result.invalid.cache()
    try:
        input_rows = bronze.count()
        invalid_rows = invalid.count()
        output_rows = valid.count()
        duplicate_rows = input_rows - invalid_rows - output_rows
        reasons = {
            r["_dq_reason"]: r["count"]
            for r in invalid.groupBy("_dq_reason").count().collect()
        }
        m.update(
            input_rows=input_rows,
            output_rows=output_rows,
            invalid_rows=invalid_rows,
            duplicate_rows=duplicate_rows,
            details={"invalid_reasons": reasons},
        )

        # Quarantine first so rejected rows are inspectable even if the gate fails.
        overwrite_where(
            invalid, settings.quarantine(table), f"_batch_date = '{batch_date}'", partition_by=["_batch_date"]
        )
        check_invalid_ratio(table, input_rows, invalid_rows, settings.max_invalid_ratio[table])
        _write_silver(valid, settings, table, batch_date)
        log.info(
            "silver.%s: in=%s valid=%s invalid=%s dup=%s reasons=%s",
            table, input_rows, output_rows, invalid_rows, duplicate_rows, reasons,
        )
    finally:
        valid.unpersist()
        invalid.unpersist()


def run(spark: SparkSession, settings: Settings, batch_date: str, run_id: str, tables: Sequence[str] = ()) -> None:
    metrics = MetricsRecorder(spark, settings, TASK, batch_date, run_id)
    try:
        for table in tables or SILVER_TABLE_ORDER:
            with metrics.track(table) as m:
                process_table(spark, settings, table, batch_date, m)
    finally:
        metrics.flush()


if __name__ == "__main__":
    from ecommerce.jobs.common import main

    main(TASK, run, allowed_tables=SILVER_TABLE_ORDER)
