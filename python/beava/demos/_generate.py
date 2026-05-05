"""Deterministic dataset generator for ``bv.demo()``.

Run via::

    python -m beava.demos._generate

Generates three datasets (``adtech`` / ``fraud`` / ``ecommerce``) totalling
~3 MB on disk. Uses only stdlib ``random``, seeded deterministically per
dataset so re-running produces byte-identical output.

Each dataset is ~10K events across 2-3 event types, designed to exercise
sketch / decay / velocity / windowed / geo ops meaningfully (not just
``bv.count``). See each generator's docstring for the op-family coverage
map.
"""
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

SEED = 42

HERE = Path(__file__).parent


def _write(target_dir: Path, schema: list[dict[str, Any]], events: list[dict[str, Any]]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    with (target_dir / "schema.json").open("w") as f:
        json.dump(schema, f, indent=2)
    with (target_dir / "events.jsonl").open("w") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")


def generate_adtech() -> None:
    """Adtech: Impression / Click / Conversion across 500 users x 20 campaigns.

    Exercises: count, n_unique (campaign breadth), time_since (last click latency),
    quantile (conversion value distribution), decayed_count (recent activity),
    top_k (popular pages), inter_arrival_stats (impression cadence).
    """
    rng = random.Random(SEED)
    base_ts = 1_700_000_000_000  # epoch ms
    n_users = 500
    n_campaigns = 20
    pages = [
        "/home", "/pricing", "/blog", "/docs", "/checkout",
        "/landing/a", "/landing/b", "/promo",
    ]
    schema: list[dict[str, Any]] = [
        {
            "kind": "event",
            "name": "Impression",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "campaign_id", "type": "str"},
                {"name": "page", "type": "str"},
                {"name": "ts", "type": "int"},
            ],
        },
        {
            "kind": "event",
            "name": "Click",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "campaign_id", "type": "str"},
                {"name": "ts", "type": "int"},
            ],
        },
        {
            "kind": "event",
            "name": "Conversion",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "campaign_id", "type": "str"},
                {"name": "value", "type": "float"},
                {"name": "ts", "type": "int"},
            ],
        },
    ]
    events: list[dict[str, Any]] = []
    n_imp, n_click, n_conv = 6_000, 3_000, 1_000
    for i in range(n_imp):
        events.append({
            "_event": "Impression",
            "user_id": f"u_{rng.randrange(n_users)}",
            "campaign_id": f"c_{rng.randrange(n_campaigns)}",
            "page": rng.choice(pages),
            "ts": base_ts + i * 100 + rng.randrange(0, 80),
        })
    for i in range(n_click):
        events.append({
            "_event": "Click",
            "user_id": f"u_{rng.randrange(n_users)}",
            "campaign_id": f"c_{rng.randrange(n_campaigns)}",
            "ts": base_ts + i * 250 + rng.randrange(0, 200),
        })
    for i in range(n_conv):
        events.append({
            "_event": "Conversion",
            "user_id": f"u_{rng.randrange(n_users)}",
            "campaign_id": f"c_{rng.randrange(n_campaigns)}",
            "value": round(rng.lognormvariate(3.5, 0.6), 2),
            "ts": base_ts + i * 1_000 + rng.randrange(0, 800),
        })
    events.sort(key=lambda e: e["ts"])
    _write(HERE / "adtech", schema, events)


def generate_fraud() -> None:
    """Fraud: Txn / Login across 300 users x 50 merchants.

    Exercises: sum, n_unique (merchant breadth), geo_velocity, geo_distance,
    quantile (txn amount p99), streak (consecutive same-merchant txns),
    histogram (amount distribution), z_score (anomaly).
    """
    rng = random.Random(SEED + 1)
    base_ts = 1_700_000_000_000
    n_users = 300
    merchants = [f"m_{i}" for i in range(50)]
    schema: list[dict[str, Any]] = [
        {
            "kind": "event",
            "name": "Txn",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "amount", "type": "float"},
                {"name": "merchant", "type": "str"},
                {"name": "ip", "type": "str"},
                {"name": "lat", "type": "float"},
                {"name": "lon", "type": "float"},
                {"name": "ts", "type": "int"},
            ],
        },
        {
            "kind": "event",
            "name": "Login",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "ip", "type": "str"},
                {"name": "device_id", "type": "str"},
                {"name": "ts", "type": "int"},
            ],
        },
    ]
    events: list[dict[str, Any]] = []
    n_txn, n_login = 7_500, 2_500
    for i in range(n_txn):
        events.append({
            "_event": "Txn",
            "user_id": f"u_{rng.randrange(n_users)}",
            "amount": round(rng.lognormvariate(3.0, 1.0), 2),
            "merchant": rng.choice(merchants),
            "ip": (
                f"10.{rng.randrange(0, 256)}.{rng.randrange(0, 256)}."
                f"{rng.randrange(0, 256)}"
            ),
            "lat": round(rng.uniform(25.0, 49.0), 4),
            "lon": round(rng.uniform(-125.0, -67.0), 4),
            "ts": base_ts + i * 200 + rng.randrange(0, 180),
        })
    for i in range(n_login):
        events.append({
            "_event": "Login",
            "user_id": f"u_{rng.randrange(n_users)}",
            "ip": (
                f"10.{rng.randrange(0, 256)}.{rng.randrange(0, 256)}."
                f"{rng.randrange(0, 256)}"
            ),
            "device_id": f"d_{rng.randrange(1000)}",
            "ts": base_ts + i * 600 + rng.randrange(0, 500),
        })
    events.sort(key=lambda e: e["ts"])
    _write(HERE / "fraud", schema, events)


def generate_ecommerce() -> None:
    """Ecommerce: PageView / AddToCart / Purchase across 800 users.

    Exercises: count, top_k (popular pages / SKUs), last_n (recent cart adds),
    histogram (price distribution), inter_arrival_stats, event_type_mix,
    rate_of_change (daily purchase volume).
    """
    rng = random.Random(SEED + 2)
    base_ts = 1_700_000_000_000
    n_users = 800
    pages = [f"/p/{i}" for i in range(200)]
    skus = [f"sku_{i:04d}" for i in range(500)]
    schema: list[dict[str, Any]] = [
        {
            "kind": "event",
            "name": "PageView",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "page", "type": "str"},
                {"name": "ts", "type": "int"},
            ],
        },
        {
            "kind": "event",
            "name": "AddToCart",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "sku", "type": "str"},
                {"name": "price", "type": "float"},
                {"name": "ts", "type": "int"},
            ],
        },
        {
            "kind": "event",
            "name": "Purchase",
            "fields": [
                {"name": "user_id", "type": "str"},
                {"name": "total", "type": "float"},
                {"name": "n_items", "type": "int"},
                {"name": "ts", "type": "int"},
            ],
        },
    ]
    events: list[dict[str, Any]] = []
    n_pv, n_atc, n_pur = 7_000, 2_000, 1_000
    for i in range(n_pv):
        events.append({
            "_event": "PageView",
            "user_id": f"u_{rng.randrange(n_users)}",
            "page": rng.choice(pages),
            "ts": base_ts + i * 100 + rng.randrange(0, 80),
        })
    for i in range(n_atc):
        events.append({
            "_event": "AddToCart",
            "user_id": f"u_{rng.randrange(n_users)}",
            "sku": rng.choice(skus),
            "price": round(rng.uniform(5.0, 250.0), 2),
            "ts": base_ts + i * 400 + rng.randrange(0, 300),
        })
    for i in range(n_pur):
        events.append({
            "_event": "Purchase",
            "user_id": f"u_{rng.randrange(n_users)}",
            "total": round(rng.uniform(10.0, 800.0), 2),
            "n_items": rng.randrange(1, 8),
            "ts": base_ts + i * 1_200 + rng.randrange(0, 1000),
        })
    events.sort(key=lambda e: e["ts"])
    _write(HERE / "ecommerce", schema, events)


def main() -> None:
    generate_adtech()
    generate_fraud()
    generate_ecommerce()


if __name__ == "__main__":
    main()
