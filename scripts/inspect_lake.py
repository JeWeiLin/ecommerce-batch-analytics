"""Print a quick overview of the lakehouse (row counts, gold samples, latest metrics).

    PYTHONPATH=src python scripts/inspect_lake.py
"""
from __future__ import annotations

from pyspark.sql import functions as F

from ecommerce.config import load_settings
from ecommerce.delta_utils import delta_exists, read_delta
from ecommerce.spark_session import get_spark

LAYERS = {
    "bronze": ["orders", "order_items", "customers", "products"],
    "silver": ["orders", "order_items", "customers", "products"],
    "quarantine": ["orders", "order_items", "customers", "products"],
    "gold": ["dim_customer", "dim_product", "fact_sales", "agg_daily_sales", "agg_customer_orders"],
}


def main() -> None:
    settings = load_settings()
    spark = get_spark("inspect-lake")
    try:
        print("\n== Row counts ==")
        for layer, tables in LAYERS.items():
            for t in tables:
                path = settings.table_path(layer, t)
                n = read_delta(spark, path).count() if delta_exists(spark, path) else "-"
                print(f"  {layer:<11}{t:<22}{n}")

        gold = lambda t: read_delta(spark, settings.gold(t))  # noqa: E731
        print("\n== agg_daily_sales (by day) ==")
        gold("agg_daily_sales").groupBy("order_date").agg(
            F.sum("order_count").alias("category_orders"),
            F.sum("net_revenue").alias("net_revenue"),
            F.sum("gross_profit").alias("gross_profit"),
            F.sum("refunded_amount").alias("refunded"),
        ).orderBy("order_date").show(truncate=False)

        print("== Repeat customers ==")
        gold("agg_customer_orders").agg(
            F.count(F.when(F.col("order_count") > 0, 1)).alias("buyers"),
            F.count(F.when(F.col("is_repeat_customer"), 1)).alias("repeat_buyers"),
        ).show()

        print("== SCD2 example: customers with history ==")
        dim = gold("dim_customer")
        changed = dim.filter(~F.col("is_current")).select("customer_id").limit(3)
        dim.join(changed, "customer_id").select(
            "customer_id", "city", "segment", "effective_from", "effective_to", "is_current"
        ).orderBy("customer_id", "effective_from").show(truncate=False)

        print("== Quarantine reasons ==")
        for t in LAYERS["quarantine"]:
            path = settings.quarantine(t)
            if delta_exists(spark, path):
                read_delta(spark, path).groupBy(F.lit(t).alias("table"), "_dq_reason").count().orderBy(
                    F.desc("count")
                ).show(truncate=False)

        print("== Latest pipeline metrics ==")
        read_delta(spark, settings.ops("pipeline_metrics")).orderBy(F.desc("recorded_at")).select(
            "batch_date", "task", "table_name", "status", "input_rows", "output_rows",
            "invalid_rows", "duplicate_rows", "duration_seconds",
        ).show(15, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
