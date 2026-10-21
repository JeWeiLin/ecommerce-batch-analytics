"""Pipeline observability: one row per (run, task, table) in ops/pipeline_metrics."""
from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterator, List

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce.config import Settings

log = logging.getLogger(__name__)

METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("batch_date", DateType()),
        StructField("task", StringType()),
        StructField("table_name", StringType()),
        StructField("status", StringType()),
        StructField("input_rows", LongType()),
        StructField("output_rows", LongType()),
        StructField("invalid_rows", LongType()),
        StructField("duplicate_rows", LongType()),
        StructField("duration_seconds", DoubleType()),
        StructField("message", StringType()),
        StructField("details", StringType()),
        StructField("recorded_at", TimestampType()),
    ]
)


class MetricsRecorder:
    def __init__(self, spark: SparkSession, settings: Settings, task: str, batch_date: str, run_id: str):
        self.spark = spark
        self.settings = settings
        self.task = task
        self.batch_date = date.fromisoformat(batch_date)
        self.run_id = run_id
        self._rows: List[tuple] = []

    @contextmanager
    def track(self, table_name: str) -> Iterator[Dict[str, Any]]:
        """Time a unit of work; the caller fills counts into the yielded dict."""
        rec: Dict[str, Any] = {}
        started = time.time()
        status, message = "success", None
        try:
            yield rec
            message = rec.get("message")
        except Exception as exc:
            status, message = "failed", f"{type(exc).__name__}: {exc}"[:2000]
            raise
        finally:
            row = (
                self.run_id,
                self.batch_date,
                self.task,
                table_name,
                status,
                rec.get("input_rows"),
                rec.get("output_rows"),
                rec.get("invalid_rows"),
                rec.get("duplicate_rows"),
                round(time.time() - started, 3),
                message,
                json.dumps(rec.get("details"), default=str) if rec.get("details") else None,
                datetime.now(timezone.utc),
            )
            self._rows.append(row)
            log.info("METRIC %s", json.dumps(dict(zip(METRICS_SCHEMA.names, row)), default=str))

    def flush(self) -> None:
        if not self._rows:
            return
        try:
            df = self.spark.createDataFrame(self._rows, schema=METRICS_SCHEMA)
            df.write.format("delta").mode("append").save(self.settings.ops("pipeline_metrics"))
            self._rows = []
        except Exception:  # never hide the original job error because of metrics
            log.exception("Failed to persist pipeline metrics")
