"""Generic Slowly Changing Dimension Type 2 (with optional Type 1 columns).

Target table layout:
    <sk>, <key>, <scd2 cols>, <scd1 cols>,
    effective_from, effective_to, is_current, row_hash, _batch_date, _updated_ts

* SCD2 columns (e.g. city, segment) -> a change closes the current version and
  opens a new one, so facts can be joined "as of" the order timestamp.
* SCD1 columns (e.g. email, name)  -> overwritten on every version.

Idempotency: a re-run with the same input finds identical row hashes and
changes nothing. Updates older than the current version are ignored, so a
late re-run of an old batch can never roll a dimension backwards.
Surrogate keys are deterministic (xxhash64(key, effective_from)), so a
re-built version always gets the same key.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce.delta_utils import delta_exists
from ecommerce.quality import deduplicate

HIGH_TS = "9999-12-31 00:00:00"  # not 23:59:59: overflows year 9999 when shown in UTC+ timezones


@dataclass(frozen=True)
class SCD2Spec:
    key: str
    sk: str
    scd2_columns: Sequence[str]
    scd1_columns: Sequence[str] = ()
    effective_column: str = "updated_at"


CUSTOMER_SCD = SCD2Spec(
    key="customer_id",
    sk="customer_sk",
    scd2_columns=("city", "segment"),
    scd1_columns=("customer_name", "email"),
)
PRODUCT_SCD = SCD2Spec(
    key="product_id",
    sk="product_sk",
    scd2_columns=("category", "unit_cost", "list_price"),
    scd1_columns=("product_name",),
)


def row_hash(columns: Sequence[str]) -> Column:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)


def _prepare_source(updates: DataFrame, spec: SCD2Spec) -> DataFrame:
    """Latest change per key in this batch (intraday intermediate versions collapse)."""
    latest = deduplicate(updates, [spec.key], [F.col(spec.effective_column).desc()])
    return latest.select(
        spec.key,
        *spec.scd2_columns,
        *spec.scd1_columns,
        F.col(spec.effective_column).alias("effective_from"),
        row_hash(spec.scd2_columns).alias("row_hash"),
    )


def _insert_values(spec: SCD2Spec, batch_date: str, prefix: str = "s.") -> Dict[str, str]:
    values = {
        spec.sk: f"xxhash64({prefix}{spec.key}, {prefix}effective_from)",
        spec.key: f"{prefix}{spec.key}",
    }
    for col in [*spec.scd2_columns, *spec.scd1_columns]:
        values[col] = f"{prefix}{col}"
    values.update(
        {
            "effective_from": f"{prefix}effective_from",
            "effective_to": f"timestamp'{HIGH_TS}'",
            "is_current": "true",
            "row_hash": f"{prefix}row_hash",
            "_batch_date": f"date'{batch_date}'",
            "_updated_ts": "current_timestamp()",
        }
    )
    return values


def apply_scd2(
    spark: SparkSession,
    target_path: str,
    updates: DataFrame,
    spec: SCD2Spec,
    batch_date: str,
) -> Dict[str, int]:
    source = _prepare_source(updates, spec)

    # First load: every record becomes the current version.
    if not delta_exists(spark, target_path):
        values = _insert_values(spec, batch_date, prefix="")
        initial = source.select(*[F.expr(expr).alias(name) for name, expr in values.items()])
        initial.write.format("delta").mode("overwrite").save(target_path)
        return {"inserted": initial.count(), "expired": 0, "scd1_updated": 0}

    target = DeltaTable.forPath(spark, target_path)
    current = (
        target.toDF()
        .filter("is_current")
        .select(spec.key, F.col("row_hash").alias("_t_hash"), F.col("effective_from").alias("_t_from"))
    )
    joined = source.join(current, spec.key, "left")
    new_keys = joined.filter(F.col("_t_hash").isNull())
    changed = joined.filter(
        F.col("_t_hash").isNotNull()
        & (F.col("row_hash") != F.col("_t_hash"))
        & (F.col("effective_from") > F.col("_t_from"))
    )

    cols: List[str] = source.columns
    # Classic staged-MERGE: a changed key appears twice -
    #   merge_key = key  -> matches the current row and expires it
    #   merge_key = NULL -> never matches, inserted as the new current version
    staged = (
        changed.select(F.lit(None).cast("string").alias("_merge_key"), *cols)
        .unionByName(changed.select(F.col(spec.key).alias("_merge_key"), *cols))
        .unionByName(new_keys.select(F.col(spec.key).alias("_merge_key"), *cols))
    ).cache()
    n_new, n_changed = new_keys.count(), changed.count()

    if n_new or n_changed:
        (
            target.alias("t")
            .merge(staged.alias("s"), f"t.{spec.key} = s._merge_key AND t.is_current = true")
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_to": "s.effective_from",
                    "_updated_ts": "current_timestamp()",
                }
            )
            .whenNotMatchedInsert(values=_insert_values(spec, batch_date))
            .execute()
        )
    staged.unpersist()

    scd1_updated = _apply_scd1(spark, target_path, source, spec) if spec.scd1_columns else 0
    return {"inserted": n_new + n_changed, "expired": n_changed, "scd1_updated": scd1_updated}


def _apply_scd1(spark: SparkSession, target_path: str, source: DataFrame, spec: SCD2Spec) -> int:
    """Overwrite SCD1 attributes on every version, unless the update is stale."""
    target = DeltaTable.forPath(spark, target_path)
    current = target.toDF().filter("is_current").select(spec.key, F.col("effective_from").alias("_cur_from"))
    eligible = (
        source.join(current, spec.key)
        .filter(F.col("effective_from") >= F.col("_cur_from"))
        .select(spec.key, *spec.scd1_columns)
    )
    changed_cond = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in spec.scd1_columns)
    before = target.history(1).collect()[0]["version"]
    (
        target.alias("t")
        .merge(eligible.alias("s"), f"t.{spec.key} = s.{spec.key}")
        .whenMatchedUpdate(
            condition=changed_cond,
            set={**{c: f"s.{c}" for c in spec.scd1_columns}, "_updated_ts": "current_timestamp()"},
        )
        .execute()
    )
    last = target.history(1).collect()[0]
    if last["version"] == before:
        return 0
    return int((last["operationMetrics"] or {}).get("numTargetRowsUpdated", 0))
