"""Task 2 - ingest_bronze.

Loads the raw CSVs of one batch into Bronze Delta tables, unchanged
(all columns kept as strings), plus lineage columns.

Idempotency: each table is partitioned by `_batch_date` and written with
`replaceWhere _batch_date = <batch>`, so re-running a day replaces that day's
partition instead of appending duplicates.
"""
from __future__ import annotations

import logging
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce.config import Settings
from ecommerce.delta_utils import overwrite_where, read_delta
from ecommerce.metrics import MetricsRecorder
from ecommerce.schemas import SOURCES, SourceSpec
from ecommerce.transforms.scd2 import row_hash

log = logging.getLogger(__name__)
TASK = "ingest_bronze"


def read_source(spark: SparkSession, settings: Settings, spec: SourceSpec, batch_date: str, run_id: str) -> DataFrame:
    path = settings.source_file(batch_date, spec.file_name)
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    missing = [c for c in spec.columns if c not in raw.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")

    df = raw.select(
        *[F.col(c).cast("string").alias(c) for c in spec.columns],
        F.col("_metadata.file_path").alias("_source_file"),
    )
    return df.select(
        *spec.columns,
        F.to_date(F.lit(batch_date)).alias("_batch_date"),
        F.lit(f"{spec.name}_{batch_date}").alias("_batch_id"),
        F.lit(run_id).alias("_run_id"),
        "_source_file",
        F.current_timestamp().alias("_ingestion_ts"),
        row_hash(spec.columns).alias("_row_hash"),
    )


def run(spark: SparkSession, settings: Settings, batch_date: str, run_id: str, tables: Sequence[str] = ()) -> None:
    metrics = MetricsRecorder(spark, settings, TASK, batch_date, run_id)
    try:
        for name in tables or tuple(SOURCES):
            spec = SOURCES[name]
            with metrics.track(name) as m:
                df = read_source(spark, settings, spec, batch_date, run_id)
                target = settings.bronze(name)
                overwrite_where(df, target, f"_batch_date = '{batch_date}'", partition_by=["_batch_date"])
                written = read_delta(spark, target).filter(F.col("_batch_date") == batch_date).count()
                m.update(input_rows=written, output_rows=written)
                log.info("bronze.%s <- %s rows (batch %s)", name, written, batch_date)
    finally:
        metrics.flush()


if __name__ == "__main__":
    from ecommerce.jobs.common import main

    main(TASK, run, allowed_tables=tuple(SOURCES))
