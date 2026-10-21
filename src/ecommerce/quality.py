"""Row-level data quality helpers.

A rule is a named boolean Column that is TRUE when the row is valid.
Rows failing at least one rule are routed to quarantine with every failed
rule name listed in `_dq_reason`, e.g. "quantity_not_positive;orphan_order_id".
"""
from __future__ import annotations

from typing import Mapping, Sequence, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

DQ_ERRORS_COL = "_dq_errors"
DQ_REASON_COL = "_dq_reason"


class DataQualityError(RuntimeError):
    """Raised when a batch breaches a data-quality threshold."""


def apply_rules(df: DataFrame, rules: Mapping[str, Column]) -> DataFrame:
    checks = [
        F.when(F.coalesce(cond, F.lit(False)), F.lit(None).cast("string")).otherwise(F.lit(name))
        for name, cond in rules.items()
    ]
    return df.withColumn(DQ_ERRORS_COL, F.filter(F.array(*checks), lambda x: x.isNotNull()))


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    valid = df.filter(F.size(DQ_ERRORS_COL) == 0).drop(DQ_ERRORS_COL)
    invalid = (
        df.filter(F.size(DQ_ERRORS_COL) > 0)
        .withColumn(DQ_REASON_COL, F.array_join(DQ_ERRORS_COL, ";"))
        .drop(DQ_ERRORS_COL)
    )
    return valid, invalid


def deduplicate(df: DataFrame, keys: Sequence[str], order_by: Sequence[Column]) -> DataFrame:
    """Keep the first row per key according to `order_by` (deterministic)."""
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def check_invalid_ratio(table: str, input_rows: int, invalid_rows: int, max_ratio: float) -> float:
    if input_rows == 0:
        return 0.0
    ratio = invalid_rows / input_rows
    if ratio > max_ratio:
        raise DataQualityError(
            f"[{table}] invalid ratio {ratio:.2%} ({invalid_rows}/{input_rows}) "
            f"exceeds threshold {max_ratio:.2%}; batch rejected, see quarantine table."
        )
    return ratio
