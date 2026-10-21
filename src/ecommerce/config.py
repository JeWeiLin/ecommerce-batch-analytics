"""Runtime settings, resolved from environment variables.

Every path is built from two roots so the same code runs on a laptop,
inside the Airflow container, or against MinIO (s3a://...).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Maximum share of invalid rows per source table before a batch is rejected.
DEFAULT_MAX_INVALID_RATIO: Dict[str, float] = {
    "orders": 0.05,
    "order_items": 0.05,
    "customers": 0.10,  # small daily volume -> one bad row is a larger share
    "products": 0.10,
}


@dataclass(frozen=True)
class Settings:
    landing_root: str
    lake_root: str
    max_invalid_ratio: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_MAX_INVALID_RATIO))
    min_batch_orders: int = 1
    max_unknown_dimension_ratio: float = 0.01
    revenue_tolerance: float = 0.01

    # ---- landing -------------------------------------------------------
    def landing_dir(self, batch_date: str) -> str:
        return f"{self.landing_root}/{batch_date}"

    def source_file(self, batch_date: str, file_name: str) -> str:
        return f"{self.landing_dir(batch_date)}/{file_name}"

    # ---- lakehouse layers ---------------------------------------------
    def table_path(self, layer: str, table: str) -> str:
        return f"{self.lake_root}/{layer}/{table}"

    def bronze(self, table: str) -> str:
        return self.table_path("bronze", table)

    def silver(self, table: str) -> str:
        return self.table_path("silver", table)

    def gold(self, table: str) -> str:
        return self.table_path("gold", table)

    def quarantine(self, table: str) -> str:
        return self.table_path("quarantine", table)

    def ops(self, table: str) -> str:
        return self.table_path("ops", table)

    @property
    def is_local_lake(self) -> bool:
        return "://" not in self.lake_root or self.lake_root.startswith("file://")


def load_settings(**overrides) -> Settings:
    landing = os.getenv("ECOM_LANDING_ROOT", str(PROJECT_ROOT / "data" / "landing"))
    lake = os.getenv("ECOM_LAKE_ROOT", str(PROJECT_ROOT / "data" / "lake"))

    ratios = dict(DEFAULT_MAX_INVALID_RATIO)
    global_ratio = os.getenv("ECOM_DQ_MAX_INVALID_RATIO")
    if global_ratio:
        ratios = {k: float(global_ratio) for k in ratios}

    kwargs = dict(
        landing_root=landing.rstrip("/"),
        lake_root=lake.rstrip("/"),
        max_invalid_ratio=ratios,
        min_batch_orders=int(os.getenv("ECOM_DQ_MIN_BATCH_ORDERS", "1")),
    )
    kwargs.update(overrides)
    return Settings(**kwargs)
