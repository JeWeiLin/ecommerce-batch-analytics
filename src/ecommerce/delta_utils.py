"""Small, idempotent Delta Lake write helpers."""
from __future__ import annotations

from typing import Optional, Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:  # path missing / not reachable
        return False


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def overwrite_where(
    df: DataFrame,
    path: str,
    predicate: str,
    partition_by: Optional[Sequence[str]] = None,
) -> None:
    """Atomically replace only the rows matching `predicate`.

    Re-running the same batch replaces its slice instead of appending a
    second copy, which is what makes Bronze/Quarantine/aggregates idempotent.
    """
    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if delta_exists(spark, path):
        writer = writer.option("replaceWhere", predicate)
    writer.save(path)


def merge_upsert(
    df: DataFrame,
    path: str,
    keys: Sequence[str],
    update_condition: Optional[str] = None,
) -> None:
    """MERGE `df` into the Delta table at `path` on `keys` (create it if missing)."""
    spark = df.sparkSession
    if not delta_exists(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), on)
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def sql_in_list(values: Sequence) -> str:
    """Render values as a SQL IN list: ('2024-01-01', '2024-01-02')."""
    quoted = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
    return f"({quoted})"
