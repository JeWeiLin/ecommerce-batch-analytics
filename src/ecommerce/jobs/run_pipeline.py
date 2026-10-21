"""Run the full pipeline locally (without Airflow) for one or more dates.

    python -m ecommerce.jobs.run_pipeline --start-date 2024-01-01 --end-date 2024-01-07

Uses one SparkSession for all steps, in the same order as the Airflow DAG.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from typing import Iterator, Optional

from pyspark.sql import SparkSession

from ecommerce.config import Settings, load_settings
from ecommerce.jobs import build_gold, check_source_files, ingest_bronze, transform_silver, validate_outputs
from ecommerce.jobs.common import default_run_id, setup_logging

log = logging.getLogger(__name__)


def daterange(start: str, end: str) -> Iterator[str]:
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        yield d.isoformat()
        d += timedelta(days=1)


def run_batch(spark: SparkSession, settings: Settings, batch_date: str, run_id: Optional[str] = None) -> None:
    run_id = run_id or default_run_id()
    log.info("===== batch %s (run_id=%s) =====", batch_date, run_id)
    check_source_files.run(settings, batch_date)
    ingest_bronze.run(spark, settings, batch_date, run_id)
    transform_silver.run(spark, settings, batch_date, run_id)
    build_gold.run(spark, settings, batch_date, run_id)
    validate_outputs.run(spark, settings, batch_date, run_id)


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", help="inclusive; defaults to --start-date")
    p.add_argument("--run-id", default=None)
    args = p.parse_args()

    from ecommerce.spark_session import get_spark

    settings = load_settings()
    spark = get_spark("ecommerce-local-pipeline")
    try:
        for d in daterange(args.start_date, args.end_date or args.start_date):
            run_batch(spark, settings, d, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
