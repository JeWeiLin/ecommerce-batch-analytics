from __future__ import annotations

from conftest import ts

from ecommerce.delta_utils import read_delta
from ecommerce.transforms.scd2 import CUSTOMER_SCD, apply_scd2

SCHEMA = "customer_id string, customer_name string, email string, city string, segment string, updated_at timestamp"


def upd(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def versions(spark, path, customer_id):
    return (
        read_delta(spark, path)
        .filter(f"customer_id = '{customer_id}'")
        .orderBy("effective_from")
        .collect()
    )


def test_scd2_lifecycle(spark, tmp_path):
    path = str(tmp_path / "dim_customer")

    # day 1: initial load
    stats = apply_scd2(spark, path, upd(spark, [
        ("C1", "Ann", "ann@x.com", "Taipei", "consumer", ts("2024-01-01 01:00:00")),
        ("C2", "Bob", "bob@x.com", "Tainan", "corporate", ts("2024-01-01 01:00:00")),
    ]), CUSTOMER_SCD, "2024-01-01")
    assert stats["inserted"] == 2

    # day 2: C1 moves city (SCD2), C2 unchanged, C3 new
    day2 = upd(spark, [
        ("C1", "Ann", "ann@x.com", "Kaohsiung", "consumer", ts("2024-01-02 10:00:00")),
        ("C2", "Bob", "bob@x.com", "Tainan", "corporate", ts("2024-01-02 11:00:00")),
        ("C3", "Cat", "cat@x.com", "Hsinchu", "consumer", ts("2024-01-02 12:00:00")),
    ])
    stats = apply_scd2(spark, path, day2, CUSTOMER_SCD, "2024-01-02")
    assert stats == {"inserted": 2, "expired": 1, "scd1_updated": 0}

    v = versions(spark, path, "C1")
    assert [(x.city, x.is_current) for x in v] == [("Taipei", False), ("Kaohsiung", True)]
    assert v[0].effective_to == v[1].effective_from == ts("2024-01-02 10:00:00")
    assert v[0].customer_sk != v[1].customer_sk
    assert len(versions(spark, path, "C2")) == 1

    # re-run of day 2 is a no-op (idempotent)
    before = read_delta(spark, path).drop("_updated_ts").collect()
    stats = apply_scd2(spark, path, day2, CUSTOMER_SCD, "2024-01-02")
    assert stats == {"inserted": 0, "expired": 0, "scd1_updated": 0}
    assert sorted(before) == sorted(read_delta(spark, path).drop("_updated_ts").collect())

    # a stale update (older than the current version) is ignored
    stale = upd(spark, [("C1", "Ann", "ann@x.com", "Taichung", "consumer", ts("2024-01-01 12:00:00"))])
    assert apply_scd2(spark, path, stale, CUSTOMER_SCD, "2024-01-01")["inserted"] == 0
    assert [x.city for x in versions(spark, path, "C1")] == ["Taipei", "Kaohsiung"]

    # SCD1 change (email) overwrites all versions without creating a new one
    day3 = upd(spark, [("C1", "Ann", "ann@new.com", "Kaohsiung", "consumer", ts("2024-01-03 09:00:00"))])
    stats = apply_scd2(spark, path, day3, CUSTOMER_SCD, "2024-01-03")
    assert stats["inserted"] == 0 and stats["scd1_updated"] == 2
    assert {x.email for x in versions(spark, path, "C1")} == {"ann@new.com"}
