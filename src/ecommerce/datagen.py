"""Deterministic synthetic data generator (standard library only).

Writes one folder per day:  <output>/<YYYY-MM-DD>/{orders,order_items,customers,products}.csv

Each day's files are *incremental*:
  * products   - initial catalogue on day 0, then price/cost changes and new products
  * customers  - initial base on day 0, then new customers and city/segment/email changes
  * orders     - new orders + status updates of older orders (placed -> paid -> shipped ...)
  * order_items- lines of the new orders
With --dirty (default) it also injects exact duplicates, invalid values, an
unknown status, future/invalid timestamps and orphan order lines, plus messy
formatting ("  new taipei", "Small Business") that Silver must standardise.

The simulation always starts from --start-date with a fixed seed, so
generating 10 days reproduces the same first 7 days as generating 7 days.

    python -m ecommerce.datagen --start-date 2024-01-01 --days 7
"""
from __future__ import annotations

import argparse
import csv
import random
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List

from ecommerce.config import load_settings
from ecommerce.schemas import SOURCES

CITIES = ["Taipei", "New Taipei", "Taoyuan", "Taichung", "Tainan", "Kaohsiung", "Hsinchu"]
SEGMENTS = ["consumer", "corporate", "small_business"]
CATEGORIES = {
    "electronics": (800, 30000),
    "home": (200, 6000),
    "fashion": (300, 4000),
    "beauty": (150, 2500),
    "sports": (300, 8000),
    "books": (150, 900),
}
FIRST_NAMES = ["Wei", "Yu", "Chen", "Lin", "Hsu", "Mei", "Jun", "Ting", "Hao", "Ling", "Kai", "An"]
LAST_NAMES = ["Wang", "Chen", "Lin", "Huang", "Chang", "Lee", "Wu", "Liu", "Tsai", "Yang"]
# next status -> probability that the transition happens on a given day
TRANSITIONS = {"placed": ("paid", 0.75), "paid": ("shipped", 0.7), "shipped": ("delivered", 0.6), "delivered": ("refunded", 0.02)}
TS_FMT = "%Y-%m-%d %H:%M:%S"


@dataclass
class State:
    products: Dict[str, dict] = field(default_factory=dict)
    customers: Dict[str, dict] = field(default_factory=dict)
    orders: Dict[str, dict] = field(default_factory=dict)
    next_product: int = 1
    next_customer: int = 1


def _ts(dt: datetime) -> str:
    return dt.strftime(TS_FMT)


def _rand_time(rng: random.Random, day: date, start_h: float, end_h: float) -> datetime:
    seconds = rng.uniform(start_h * 3600, end_h * 3600)
    return datetime.combine(day, time()) + timedelta(seconds=int(seconds))


def _messy_city(rng: random.Random, city: str) -> str:
    r = rng.random()
    if r < 0.08:
        return f"  {city.lower()} "
    if r < 0.12:
        return city.upper()
    return city


def _messy_segment(rng: random.Random, segment: str) -> str:
    if segment == "small_business" and rng.random() < 0.3:
        return "Small Business"
    return segment.upper() if rng.random() < 0.05 else segment


def _new_product(rng: random.Random, st: State, updated: datetime) -> dict:
    pid = f"P{st.next_product:04d}"
    st.next_product += 1
    category = rng.choice(list(CATEGORIES))
    low, high = CATEGORIES[category]
    price = round(rng.uniform(low, high), 0)
    p = {
        "product_id": pid,
        "product_name": f"{category.title()} Item {pid[1:]}",
        "category": category,
        "unit_cost": round(price * rng.uniform(0.45, 0.75), 2),
        "list_price": price,
        "updated_at": _ts(updated),
        "_created": updated,
    }
    st.products[pid] = p
    return p


def _new_customer(rng: random.Random, st: State, updated: datetime) -> dict:
    cid = f"C{st.next_customer:05d}"
    st.next_customer += 1
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    c = {
        "customer_id": cid,
        "customer_name": f"{first} {last}",
        "email": f"{first}.{last}.{cid[1:]}@example.com".lower(),
        "city": rng.choice(CITIES),
        "segment": rng.choices(SEGMENTS, weights=[0.7, 0.15, 0.15])[0],
        "updated_at": _ts(updated),
        "_created": updated,
    }
    st.customers[cid] = c
    return c


def _product_row(rng, p) -> dict:
    return {k: p[k] for k in SOURCES["products"].columns}


def _customer_row(rng, c) -> dict:
    row = {k: c[k] for k in SOURCES["customers"].columns}
    row["city"] = _messy_city(rng, c["city"])
    row["segment"] = _messy_segment(rng, c["segment"])
    return row


def simulate_day(rng: random.Random, st: State, day: date, day_index: int, dirty: bool) -> Dict[str, List[dict]]:
    day_start = datetime.combine(day, time())
    day_end = datetime.combine(day, time(23, 59, 59))
    out: Dict[str, List[dict]] = {name: [] for name in SOURCES}

    # ---------------- products
    if day_index == 0:
        for _ in range(40):
            out["products"].append(_product_row(rng, _new_product(rng, st, day_start + timedelta(minutes=1))))
    else:
        for pid in rng.sample(sorted(st.products), k=rng.randint(1, 3)):
            p = st.products[pid]
            p["list_price"] = round(p["list_price"] * rng.uniform(0.85, 1.15), 0)
            if rng.random() < 0.5:
                p["unit_cost"] = round(p["unit_cost"] * rng.uniform(0.9, 1.1), 2)
            p["updated_at"] = _ts(day_start + timedelta(minutes=5))
            out["products"].append(_product_row(rng, p))
        if day_index % 3 == 0:
            out["products"].append(_product_row(rng, _new_product(rng, st, day_start + timedelta(minutes=5))))

    # ---------------- customers
    existing_customers = sorted(st.customers)
    if day_index == 0:
        for _ in range(150):
            c = _new_customer(rng, st, _rand_time(rng, day, 0, 1))
            out["customers"].append(_customer_row(rng, c))
    else:
        for cid in rng.sample(existing_customers, k=max(1, int(len(existing_customers) * 0.04))):
            c = st.customers[cid]
            change = rng.random()
            if change < 0.5:
                c["city"] = rng.choice([x for x in CITIES if x != c["city"]])
            elif change < 0.85:
                c["segment"] = rng.choice([x for x in SEGMENTS if x != c["segment"]])
            else:
                c["email"] = c["email"].replace("@example.com", "@mail.example.com")
            c["updated_at"] = _ts(_rand_time(rng, day, 0, 23.9))
            out["customers"].append(_customer_row(rng, c))
        for _ in range(rng.randint(5, 15)):
            out["customers"].append(_customer_row(rng, _new_customer(rng, st, _rand_time(rng, day, 0, 23.9))))

    # ---------------- status updates of older orders
    for oid in sorted(st.orders):
        o = st.orders[oid]
        nxt = TRANSITIONS.get(o["status"])
        if o["status"] == "placed" and rng.random() < 0.1:
            nxt = ("cancelled", 1.0)
        if nxt and rng.random() < nxt[1]:
            o["status"] = nxt[0]
            o["updated_at"] = _ts(_rand_time(rng, day, 0, 23.9))
            out["orders"].append({k: o[k] for k in SOURCES["orders"].columns})

    # ---------------- new orders
    eligible_customers = [cid for cid, c in st.customers.items() if c["_created"] < day_start + timedelta(hours=1)]
    eligible_products = [pid for pid, p in st.products.items() if p["_created"] < day_start + timedelta(hours=1)]
    for seq in range(1, rng.randint(80, 120) + 1):
        oid = f"O{day:%Y%m%d}-{seq:04d}"
        order_ts = _rand_time(rng, day, 8, 23.9)
        status = rng.choices(["placed", "paid", "cancelled"], weights=[0.35, 0.6, 0.05])[0]
        o = {
            "order_id": oid,
            "customer_id": rng.choice(eligible_customers),
            "order_ts": _ts(order_ts),
            "status": status,
            "updated_at": _ts(min(order_ts + timedelta(minutes=rng.randint(0, 30)), day_end)),
        }
        st.orders[oid] = o
        out["orders"].append(dict(o))
        for pid in rng.sample(eligible_products, k=rng.choice([1, 1, 2, 2, 3, 4])):
            qty = rng.choices([1, 2, 3, 5], weights=[0.65, 0.2, 0.1, 0.05])[0]
            price = st.products[pid]["list_price"]
            discount = round(qty * price * rng.uniform(0.05, 0.2), 2) if rng.random() < 0.15 else 0
            out["order_items"].append(
                {"order_id": oid, "product_id": pid, "quantity": qty, "unit_price": f"{price:.2f}", "discount_amount": discount}
            )

    if dirty:
        _inject_dirty_rows(rng, out, day, day_index)
    return out


def _inject_dirty_rows(rng: random.Random, out: Dict[str, List[dict]], day: date, day_index: int) -> None:
    ts = _ts(datetime.combine(day, time(12)))
    future = _ts(datetime.combine(day + timedelta(days=3), time(12)))
    # exact duplicates (~2%) - must be removed, not quarantined
    for table in ("orders", "order_items"):
        rows = out[table]
        rows.extend(dict(r) for r in rng.sample(rows, k=max(1, len(rows) // 50)))
    # invalid orders
    out["orders"] += [
        {"order_id": f"O{day:%Y%m%d}-9001", "customer_id": "", "order_ts": ts, "status": "paid", "updated_at": ts},
        {"order_id": f"O{day:%Y%m%d}-9002", "customer_id": "C00001", "order_ts": f"{day:%Y}-13-45 10:00:00", "status": "paid", "updated_at": ts},
        {"order_id": f"O{day:%Y%m%d}-9003", "customer_id": "C00002", "order_ts": ts, "status": "lost", "updated_at": ts},
        {"order_id": f"O{day:%Y%m%d}-9004", "customer_id": "C00003", "order_ts": future, "status": "paid", "updated_at": future},
    ]
    # invalid order lines
    valid_line = out["order_items"][0]
    out["order_items"] += [
        {**valid_line, "product_id": "P0001" if valid_line["product_id"] != "P0001" else "P0002", "quantity": 0},
        {**valid_line, "product_id": "P0003" if valid_line["product_id"] != "P0003" else "P0004", "quantity": -2},
        {"order_id": "O-GHOST-0001", "product_id": "P0005", "quantity": 1, "unit_price": "100.00", "discount_amount": 0},
    ]
    if day_index == 0:
        out["customers"] += [
            {"customer_id": "C99001", "customer_name": "Bad Segment", "email": "bad@example.com", "city": "Taipei", "segment": "vip", "updated_at": ts},
            {"customer_id": "C99002", "customer_name": "No City", "email": "nocity@example.com", "city": " ", "segment": "consumer", "updated_at": ts},
        ]
        out["customers"].append(dict(out["customers"][0]))  # duplicate
        out["products"].append(
            {"product_id": "P9999", "product_name": "Broken", "category": "home", "unit_cost": "-10", "list_price": "99", "updated_at": ts}
        )
    elif day_index % 3 == 0:
        out["customers"].append(
            {"customer_id": "C99003", "customer_name": "Bad Email", "email": "not-an-email", "city": "Tainan", "segment": "consumer", "updated_at": ts}
        )


def generate(output_dir: str, start_date: str, days: int, seed: int = 42, dirty: bool = True, clean: bool = False) -> List[str]:
    out_root = Path(output_dir)
    rng = random.Random(seed)
    st = State()
    written = []
    start = date.fromisoformat(start_date)
    for i in range(days):
        day = start + timedelta(days=i)
        data = simulate_day(rng, st, day, i, dirty)
        folder = out_root / day.isoformat()
        if clean and folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True, exist_ok=True)
        for name, spec in SOURCES.items():
            with (folder / spec.file_name).open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(spec.columns), extrasaction="ignore")
                writer.writeheader()
                writer.writerows(data[name])
        written.append(day.isoformat())
    return written


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--output", default=load_settings().landing_root)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--no-dirty", action="store_true", help="do not inject invalid/duplicate rows")
    p.add_argument("--clean", action="store_true", help="remove existing day folders first")
    args = p.parse_args()
    days = generate(args.output, args.start_date, args.days, args.seed, not args.no_dirty, args.clean)
    print(f"Generated {len(days)} day(s) in {args.output}: {days[0]} .. {days[-1]}")


if __name__ == "__main__":
    main()
