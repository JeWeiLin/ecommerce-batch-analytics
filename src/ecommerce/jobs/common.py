"""Shared CLI entry point for Spark jobs.

Each job exposes `run(spark, settings, batch_date, run_id, tables)` so it can be
called from Airflow (via `python -m ecommerce.jobs.<job>`), from the local
runner, or directly from tests.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Callable, Optional, Sequence

from ecommerce.config import Settings, load_settings

RunFn = Callable[..., object]


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("ECOM_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)


def _valid_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def default_run_id() -> str:
    return os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"


def parse_tables(value: Optional[str], allowed: Sequence[str]) -> Sequence[str]:
    if not value:
        return tuple(allowed)
    tables = [t.strip() for t in value.split(",") if t.strip()]
    unknown = sorted(set(tables) - set(allowed))
    if unknown:
        raise SystemExit(f"Unknown table(s) {unknown}; allowed: {list(allowed)}")
    # keep dependency order
    return tuple(t for t in allowed if t in tables)


def main(task: str, run_fn: RunFn, allowed_tables: Sequence[str] = (), needs_spark: bool = True) -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description=f"E-commerce batch job: {task}")
    parser.add_argument("--batch-date", required=True, type=_valid_date, help="Logical date YYYY-MM-DD")
    parser.add_argument("--run-id", default=default_run_id())
    if allowed_tables:
        parser.add_argument("--tables", default="", help=f"Comma-separated subset of {list(allowed_tables)}")
    args = parser.parse_args()

    settings: Settings = load_settings()
    tables = parse_tables(getattr(args, "tables", ""), allowed_tables) if allowed_tables else ()

    if not needs_spark:
        run_fn(settings=settings, batch_date=args.batch_date)
        return

    from ecommerce.spark_session import get_spark

    spark = get_spark(f"ecommerce-{task}-{args.batch_date}")
    try:
        run_fn(spark, settings, args.batch_date, args.run_id, tables)
    finally:
        spark.stop()
