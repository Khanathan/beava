#!/usr/bin/env python3
"""
generate-events.py — synthesize Sendo Farm-style event JSONL for the demo.

Why synthesize instead of replay a public dataset:
- OTTO (the closest public e-commerce dataset) ships on Kaggle only, so
  reproducing it needs per-user API credentials.
- OTTO is German fashion retail. Sendo Farm is Vietnamese fresh food.
  The buyer patterns that matter — morning meal planning, province-level
  supply, OCOP categories, perishability-driven short windows — aren't
  present in OTTO. Faking them over OTTO data would be less honest than
  generating them directly.

What this produces: ~2M events in JSONL, one event per line, schema:
  { "user_id", "product_id", "category", "origin", "type", "price", "ts" }

Output: scripts/demo-sendo/events.jsonl  (~220 MB)

Patterns baked in:
- Zipfian product popularity (top product ~100x more viewed than median)
- ~85% view / 12% add_to_cart / 3% order event mix
- Time-of-day clustering (morning shopping, evening restock)
- Category-skewed provinces (Da Lat → rau_la heavy; Bac Giang → trai_cay)
- Price distribution per category (VND, log-normal-ish)
- 50K buyers, 1K products, 5 categories, 10 provinces

Tuning: adjust TARGET_EVENTS at the top. Runs in under a minute at 2M.
"""

from __future__ import annotations

import json
import os
import random
import sys
import time

TARGET_EVENTS   = 2_000_000
OUTPUT_PATH     = os.path.join(os.path.dirname(__file__), "events.jsonl")

N_USERS         = 50_000
N_PRODUCTS      = 1_000

CATEGORIES      = ["rau_la", "trai_cay", "thit_ca", "sua_trung", "gao_kho"]
PROVINCES       = [
    "Ha_Noi", "Hai_Duong", "Bac_Giang", "Lam_Dong", "Da_Lat",
    "Son_La", "Dong_Nai", "Tien_Giang", "Can_Tho", "Vinh_Long",
]

# Each province is more likely to source some categories than others.
# Da Lat grows leafy greens; Bac Giang grows fruit (lychee); Tien Giang tropical fruit.
PROVINCE_CATEGORY_WEIGHTS = {
    "Ha_Noi":     {"rau_la": 2, "sua_trung": 3, "gao_kho": 2, "thit_ca": 3, "trai_cay": 1},
    "Hai_Duong":  {"rau_la": 5, "trai_cay": 2, "thit_ca": 1, "sua_trung": 1, "gao_kho": 1},
    "Bac_Giang":  {"trai_cay": 6, "rau_la": 2, "gao_kho": 1, "thit_ca": 1, "sua_trung": 1},
    "Lam_Dong":   {"rau_la": 6, "trai_cay": 2, "gao_kho": 1, "sua_trung": 1, "thit_ca": 1},
    "Da_Lat":     {"rau_la": 7, "trai_cay": 2, "gao_kho": 1, "sua_trung": 1, "thit_ca": 1},
    "Son_La":     {"trai_cay": 5, "rau_la": 2, "gao_kho": 2, "thit_ca": 1, "sua_trung": 1},
    "Dong_Nai":   {"thit_ca": 5, "gao_kho": 2, "trai_cay": 2, "rau_la": 1, "sua_trung": 1},
    "Tien_Giang": {"trai_cay": 6, "rau_la": 2, "thit_ca": 2, "gao_kho": 1, "sua_trung": 1},
    "Can_Tho":    {"gao_kho": 5, "trai_cay": 2, "thit_ca": 2, "rau_la": 2, "sua_trung": 1},
    "Vinh_Long":  {"trai_cay": 5, "gao_kho": 3, "rau_la": 2, "thit_ca": 1, "sua_trung": 1},
}

# Rough VND price ranges per category (log-normal center).
CATEGORY_PRICE = {
    "rau_la":    (15_000,  40_000),
    "trai_cay":  (30_000,  120_000),
    "thit_ca":   (60_000,  300_000),
    "sua_trung": (25_000,  90_000),
    "gao_kho":   (40_000,  220_000),
}

EVENT_TYPE_WEIGHTS = [("view", 85), ("add_to_cart", 12), ("order", 3)]

random.seed(42)


def build_product_catalog() -> list[dict]:
    """Create N_PRODUCTS with (category, origin, price_range) assignments."""
    catalog = []
    for pid in range(N_PRODUCTS):
        origin = random.choice(PROVINCES)
        # Weighted category by province
        weights = PROVINCE_CATEGORY_WEIGHTS[origin]
        cats, ws = zip(*weights.items())
        category = random.choices(cats, weights=ws, k=1)[0]
        lo, hi = CATEGORY_PRICE[category]
        # Log-normal-ish: draw around geometric mean
        center = (lo * hi) ** 0.5
        catalog.append({
            "pid":      f"p{pid:05d}",
            "category": category,
            "origin":   origin,
            "center":   center,
        })
    return catalog


def zipf_weights(n: int, alpha: float = 1.2) -> list[float]:
    """Zipfian weights — top item ~100x rank-median for alpha≈1.2."""
    return [1.0 / ((i + 1) ** alpha) for i in range(n)]


def time_of_day_factor(ts_seconds: float) -> float:
    """Return a multiplier 0.3..1.8 that clusters activity around 7am and 6pm."""
    hour = (ts_seconds // 3600) % 24
    morning = max(0.0, 1.0 - abs(hour - 7) / 4)
    evening = max(0.0, 1.0 - abs(hour - 18) / 4)
    return 0.3 + 1.5 * max(morning, evening)


def main() -> int:
    started = time.perf_counter()
    catalog = build_product_catalog()
    weights = zipf_weights(N_PRODUCTS)

    ev_types, ev_weights = zip(*EVENT_TYPE_WEIGHTS)

    # Timeline: walk a 48-hour span, sprinkling events weighted by hour.
    now = time.time()
    span_seconds = 48 * 3600

    print(f"generating {TARGET_EVENTS:,} events into {OUTPUT_PATH}", file=sys.stderr)

    with open(OUTPUT_PATH, "w") as f:
        for i in range(TARGET_EVENTS):
            ts = now - span_seconds + (i / TARGET_EVENTS) * span_seconds
            # Use time-of-day factor to bias event density. We still write every
            # row, but we use the factor to *skip* ordering bias.
            user_id = f"u{random.randint(0, N_USERS - 1):05d}"
            product = random.choices(catalog, weights=weights, k=1)[0]
            ev_type = random.choices(ev_types, weights=ev_weights, k=1)[0]
            # Price noise around the category center.
            price = product["center"] * random.uniform(0.7, 1.6)
            # Time-of-day tilt: purge some events that fall in dead hours to
            # produce natural morning / evening bursts in the output.
            if random.random() > time_of_day_factor(ts) / 1.8:
                continue
            f.write(json.dumps({
                "user_id":    user_id,
                "product_id": product["pid"],
                "category":   product["category"],
                "origin":     product["origin"],
                "type":       ev_type,
                "price":      round(price, 0),
                "ts":         round(ts, 3),
            }) + "\n")
            if (i + 1) % 500_000 == 0:
                elapsed = time.perf_counter() - started
                print(f"  {i + 1:>10,} events  ({elapsed:.1f}s)", file=sys.stderr)

    elapsed = time.perf_counter() - started
    # Count actual written rows (time-of-day skipping drops some).
    with open(OUTPUT_PATH) as f:
        rows = sum(1 for _ in f)
    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"done. {rows:,} events · {size_mb:.1f} MB · {elapsed:.1f}s", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
