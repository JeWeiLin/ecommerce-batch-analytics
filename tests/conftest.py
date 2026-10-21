from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Sequence

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce.config import load_settings
from ecommerce.schemas import SOURCES


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    os.environ.setdefault("SPARK_LOG_LEVEL", "ERROR")
    from ecommerce.spark_session import get_spark

    session = get_spark("ecommerce-tests")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path):
    return load_settings(landing_root=str(tmp_path / "landing"), lake_root=str(tmp_path / "lake"))


def bronze_df(spark: SparkSession, table: str, rows: Sequence[Dict], batch_date: str = "2024-01-01") -> DataFrame:
    """Build a DataFrame shaped like a Bronze table (string columns + lineage)."""
    cols = list(SOURCES[table].columns)
    data: List[tuple] = [tuple(None if r.get(c) is None else str(r.get(c)) for c in cols) for r in rows]
    schema = ", ".join(f"`{c}` string" for c in cols)
    df = spark.createDataFrame(data, schema)
    return df.select(
        "*",
        F.to_date(F.lit(batch_date)).alias("_batch_date"),
        F.lit("test.csv").alias("_source_file"),
        F.current_timestamp().alias("_ingestion_ts"),
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in cols]), 256).alias("_row_hash"),
    )


def ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
