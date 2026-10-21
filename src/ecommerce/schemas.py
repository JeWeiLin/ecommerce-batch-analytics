"""Source data contracts and business constants."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class SourceSpec:
    name: str
    file_name: str
    columns: Tuple[str, ...]
    primary_key: Tuple[str, ...]


SOURCES: Dict[str, SourceSpec] = {
    "orders": SourceSpec(
        name="orders",
        file_name="orders.csv",
        columns=("order_id", "customer_id", "order_ts", "status", "updated_at"),
        primary_key=("order_id",),
    ),
    "order_items": SourceSpec(
        name="order_items",
        file_name="order_items.csv",
        columns=("order_id", "product_id", "quantity", "unit_price", "discount_amount"),
        primary_key=("order_id", "product_id"),
    ),
    "customers": SourceSpec(
        name="customers",
        file_name="customers.csv",
        columns=("customer_id", "customer_name", "email", "city", "segment", "updated_at"),
        primary_key=("customer_id",),
    ),
    "products": SourceSpec(
        name="products",
        file_name="products.csv",
        columns=("product_id", "product_name", "category", "unit_cost", "list_price", "updated_at"),
        primary_key=("product_id",),
    ),
}

# Silver processing order: order_items is validated against orders and products.
SILVER_TABLE_ORDER = ("products", "customers", "orders", "order_items")

ORDER_STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled", "refunded")
# Revenue is recognised once an order is paid and has not been cancelled/refunded.
REVENUE_STATUSES = ("paid", "shipped", "delivered")
CUSTOMER_SEGMENTS = ("consumer", "corporate", "small_business")

BRONZE_METADATA_COLUMNS = ("_batch_date", "_batch_id", "_run_id", "_source_file", "_ingestion_ts", "_row_hash")
