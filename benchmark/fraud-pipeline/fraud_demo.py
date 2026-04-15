#!/usr/bin/env python3
"""Fraud demo generator — benign baseline + injected fraud archetypes.

Pushes a realistic mixed stream to a local Tally server (default localhost:6400)
for ~60s wall-clock. Benign traffic runs continuously; four fraud archetypes fire
at scheduled moments against dedicated user IDs. The wall-clock timestamp of each
fraud burst is printed at the end as a JSON block — scientists paste these into
`tl.fork(extract_at=[...])` to snapshot features at the moment of each fraud.

Usage:
    # Terminal 1 — start "prod":
    TALLY_ADMIN_TOKEN=dev-admin-token ./target/release/tally serve \\
        --http-port 6400 --tcp-port 6401 --data-dir /tmp/tally-prod

    # Terminal 2 — run the demo:
    python3 benchmark/fraud-pipeline/fraud_demo.py

    # Output ends with a JSON block listing fraud timestamps + user IDs.
    # Hand that block to the fork example in docs/data-scientist-demo.md.

Fraud archetypes:
    card_testing       20 tiny $1-$5 charges across 20 merchants in 2 min
    velocity_burst     50 charges across 30 merchants in 10 min
    geo_hop            10 charges from 5 countries in 1h
    high_value_anomaly single $10k charge on a user whose history is ~$30
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))  # for `import tally`

import tally as tl  # noqa: E402

# -- Pipeline definitions (inline so the demo is self-contained) ----------
# Exercises: count / sum / avg / min / max / stddev / count_distinct (HLL) /
# last / filter / group_by over 30m / 1h / 24h / 7d windows across 5 entity
# tables. We intentionally keep aggregation and derived-signal computation
# separate — `.with_columns` chained after `.agg` is not supported in v0;
# callers compute ratio-style risk signals client-side from the raw features.

@tl.stream
class RawTransactions:
    user_id: str
    merchant_id: str
    device_id: str
    ip_address: str
    amount: float
    country: str
    status: str
    currency: str


@tl.table(key="user_id")
def UserTransactions(txs: RawTransactions) -> tl.Table:
    return txs.group_by("user_id").agg(
        tx_count_30m=tl.count(window="30m"),
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_count_7d=tl.count(window="7d"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_sum_24h=tl.sum("amount", window="24h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        tx_max_24h=tl.max("amount", window="24h"),
        tx_min_24h=tl.min("amount", window="24h"),
        tx_stddev_24h=tl.stddev("amount", window="24h"),
        unique_merchants_1h=tl.count_distinct("merchant_id", window="1h"),
        unique_merchants_24h=tl.count_distinct("merchant_id", window="24h"),
        unique_countries_24h=tl.count_distinct("country", window="24h"),
        unique_devices_24h=tl.count_distinct("device_id", window="24h"),
        unique_ips_24h=tl.count_distinct("ip_address", window="24h"),
        last_country=tl.last("country"),
        last_merchant=tl.last("merchant_id"),
        last_amount=tl.last("amount"),
    )


@tl.table(key="user_id")
def UserFailedTxns(txs: RawTransactions) -> tl.Table:
    return (
        txs.filter(tl.col("status") == "failed")
        .group_by("user_id")
        .agg(
            failed_count_30m=tl.count(window="30m"),
            failed_count_1h=tl.count(window="1h"),
            failed_count_24h=tl.count(window="24h"),
            failed_sum_24h=tl.sum("amount", window="24h"),
        )
    )


@tl.table(key="merchant_id")
def MerchantActivity(txs: RawTransactions) -> tl.Table:
    return txs.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        merch_unique_users_24h=tl.count_distinct("user_id", window="24h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
        merch_stddev_24h=tl.stddev("amount", window="24h"),
    )


@tl.table(key="device_id")
def DeviceActivity(txs: RawTransactions) -> tl.Table:
    return txs.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_tx_count_24h=tl.count(window="24h"),
        device_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        device_unique_users_24h=tl.count_distinct("user_id", window="24h"),
        device_unique_merchants_24h=tl.count_distinct("merchant_id", window="24h"),
    )


@tl.table(key="ip_address")
def IPActivity(txs: RawTransactions) -> tl.Table:
    return txs.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_tx_count_24h=tl.count(window="24h"),
        ip_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        ip_unique_users_24h=tl.count_distinct("user_id", window="24h"),
        ip_unique_devices_24h=tl.count_distinct("device_id", window="24h"),
    )


ALL_DATASETS = [
    RawTransactions, UserTransactions, UserFailedTxns,
    MerchantActivity, DeviceActivity, IPActivity,
]

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "NG", "CN", "AU"]
STATUSES = ["success"] * 8 + ["failed"] * 2  # 20% benign failure rate


def _zipf_id(prefix: str, n: int, alpha: float = 1.2) -> str:
    u = random.random()
    rank = int((u * n ** (1 - alpha) + (1 - u)) ** (1 / (1 - alpha)))
    rank = max(1, min(rank, n))
    return f"{prefix}{rank:06d}"


def _benign_event() -> dict:
    return {
        "user_id": _zipf_id("user_", 10000),
        "merchant_id": _zipf_id("merch_", 2000),
        "device_id": _zipf_id("dev_", 5000),
        "ip_address": _zipf_id("ip_", 8000),
        "amount": round(random.lognormvariate(3.5, 1.5), 2),
        "country": random.choice(COUNTRIES),
        "status": random.choice(STATUSES),
        "currency": "USD",
    }


BENIGN_TPS = 50  # events/sec baseline
FRAUD_USERS = {
    "card_testing": "user_fraud_001",
    "velocity_burst": "user_fraud_002",
    "geo_hop": "user_fraud_003",
    "high_value_anomaly": "user_fraud_004",
}


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _seed_history(app: tl.App, user_id: str, n: int = 40) -> None:
    """Give a fraud user a benign history so anomalies have a baseline."""
    events = []
    for _ in range(n):
        e = _benign_event()
        e["user_id"] = user_id
        e["amount"] = round(random.lognormvariate(3.3, 0.4), 2)  # tight ~$25-$40
        e["status"] = "success"
        events.append(e)
    app.push_many(RawTransactions, events)


def _card_testing(app: tl.App) -> None:
    uid = FRAUD_USERS["card_testing"]
    events = []
    for i in range(20):
        events.append({
            "user_id": uid,
            "merchant_id": f"merch_probe_{i:03d}",
            "device_id": "dev_attacker_01",
            "ip_address": "ip_attacker_01",
            "amount": round(random.uniform(1.0, 5.0), 2),
            "country": "US",
            "status": random.choice(["failed", "failed", "failed", "success"]),
            "currency": "USD",
        })
    app.push_many(RawTransactions, events)


def _velocity_burst(app: tl.App) -> None:
    uid = FRAUD_USERS["velocity_burst"]
    events = []
    for i in range(50):
        events.append({
            "user_id": uid,
            "merchant_id": f"merch_burst_{i % 30:03d}",
            "device_id": "dev_attacker_02",
            "ip_address": f"ip_burst_{i % 5:03d}",
            "amount": round(random.lognormvariate(4.0, 0.8), 2),
            "country": "US",
            "status": "success",
            "currency": "USD",
        })
    app.push_many(RawTransactions, events)


def _geo_hop(app: tl.App) -> None:
    uid = FRAUD_USERS["geo_hop"]
    countries = ["US", "NG", "CN", "BR", "DE"]
    events = []
    for i in range(10):
        events.append({
            "user_id": uid,
            "merchant_id": f"merch_geo_{i:03d}",
            "device_id": f"dev_geo_{i % 3:03d}",
            "ip_address": f"ip_geo_{i:03d}",
            "amount": round(random.lognormvariate(4.5, 0.8), 2),
            "country": countries[i % len(countries)],
            "status": "success",
            "currency": "USD",
        })
    app.push_many(RawTransactions, events)


def _high_value_anomaly(app: tl.App) -> None:
    uid = FRAUD_USERS["high_value_anomaly"]
    app.push_many(RawTransactions, [{
        "user_id": uid,
        "merchant_id": "merch_jewelry_01",
        "device_id": "dev_anom_01",
        "ip_address": "ip_anom_01",
        "amount": 10000.00,
        "country": "US",
        "status": "success",
        "currency": "USD",
    }])


FRAUD_BURSTS = [
    ("card_testing", 15.0, _card_testing),        # fires 15s in
    ("velocity_burst", 25.0, _velocity_burst),    # 25s
    ("geo_hop", 40.0, _geo_hop),                  # 40s
    ("high_value_anomaly", 50.0, _high_value_anomaly),  # 50s
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fraud demo generator")
    parser.add_argument("--host", default="localhost:6400")
    parser.add_argument("--duration", type=float, default=60.0, help="Seconds")
    parser.add_argument("--tps", type=int, default=BENIGN_TPS, help="Benign events/sec")
    args = parser.parse_args()

    app = tl.App(args.host)
    app.register(*ALL_DATASETS)

    print(f"Seeding history for {len(FRAUD_USERS)} fraud users...")
    for uid in FRAUD_USERS.values():
        _seed_history(app, uid)
    app.flush()

    print(f"Pushing benign traffic @ {args.tps} tps for {args.duration:.0f}s "
          f"with 4 fraud bursts injected...\n")

    fraud_events: list[dict] = []
    start = time.monotonic()
    next_burst = 0
    sent_benign = 0
    batch = []

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= args.duration:
            break

        # Fire scheduled fraud bursts.
        while next_burst < len(FRAUD_BURSTS) and elapsed >= FRAUD_BURSTS[next_burst][1]:
            name, _, fn = FRAUD_BURSTS[next_burst]
            ts = _now_iso()
            if batch:
                app.push_many(RawTransactions, batch)
                batch = []
            fn(app)
            app.flush()
            fraud_events.append({
                "archetype": name,
                "user_id": FRAUD_USERS[name],
                "fired_at": ts,
            })
            print(f"  [{ts}] fraud burst: {name} (user={FRAUD_USERS[name]})")
            next_burst += 1

        # Benign background traffic.
        batch.append(_benign_event())
        sent_benign += 1
        if len(batch) >= 100:
            app.push_many(RawTransactions, batch)
            batch = []
        # Pace to ~tps.
        target = sent_benign / args.tps
        if elapsed < target:
            time.sleep(target - elapsed)

    if batch:
        app.push_many(RawTransactions, batch)
    app.flush()
    app.close()

    print(f"\nDone. Benign events: {sent_benign:,}  Fraud bursts: {len(fraud_events)}\n")
    print("=" * 60)
    print("FRAUD_TIMESTAMPS — paste into tl.fork(extract_at=[...])")
    print("=" * 60)
    print(json.dumps(fraud_events, indent=2))


if __name__ == "__main__":
    main()
