#!/usr/bin/env python3
"""
Realistic fraud detection pipeline benchmark for Tally.

Models a mid-size fintech (Brex/Marqeta-class) with:
- 5 entity types: user, merchant, device, IP, card
- 47 features across 4 window tiers (30m, 1h, 24h, 7d)
- Cross-key lookups (user→merchant risk, user→device velocity)
- Derived signals (velocity spikes, amount anomalies, failure rates)
- Realistic event distribution (Zipfian user IDs, burst patterns)

Usage:
    # Start server first: TALLY_WORKER_THREADS=8 ./target/release/tally
    python3 benchmark/fraud-pipeline/bench_fraud.py --events 100000 --clients 8
    python3 benchmark/fraud-pipeline/bench_fraud.py --events 500000 --clients 1  # single-client
    python3 benchmark/fraud-pipeline/bench_fraud.py --profile  # show feature count + memory
"""

import sys
import os
import time
import random
import math
import argparse
import json
import multiprocessing

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python'))
import tally as tl

# ---------------------------------------------------------------------------
# Pipeline definition: 5 entity types, 47 features
# ---------------------------------------------------------------------------

@tl.source
class RawTransactions:
    """Raw payment events. Each event has user_id, merchant_id, device_id, ip."""
    pass

# --- Entity 1: User transaction behavior (25 features) ---

@tl.dataset(depends_on=[RawTransactions])
class UserTransactions:
    features = tl.group_by("user_id").agg(
        # Volume across window tiers
        tx_count_30m=tl.count(window="30m"),
        tx_count_1h=tl.count(window="1h"),
        tx_count_24h=tl.count(window="24h"),
        tx_count_7d=tl.count(window="7d"),
        # Amount aggregations
        tx_sum_1h=tl.sum("amount", window="1h"),
        tx_sum_24h=tl.sum("amount", window="24h"),
        tx_avg_1h=tl.avg("amount", window="1h"),
        tx_avg_24h=tl.avg("amount", window="24h"),
        tx_max_24h=tl.max("amount", window="24h"),
        tx_min_24h=tl.min("amount", window="24h"),
        tx_stddev_24h=tl.stddev("amount", window="24h"),
        # Cardinality
        unique_merchants_1h=tl.distinct_count("merchant_id", window="1h"),
        unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
        unique_countries_24h=tl.distinct_count("country", window="24h"),
        unique_devices_24h=tl.distinct_count("device_id", window="24h"),
        unique_ips_24h=tl.distinct_count("ip_address", window="24h"),
        # Context
        last_country=tl.last("country"),
        last_merchant=tl.last("merchant_id"),
        last_amount=tl.last("amount"),
    )
    # Derived signals
    velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")
    amount_vs_avg = tl.derive("last_amount / tx_avg_24h")
    spend_acceleration = tl.derive("tx_sum_1h / (tx_sum_24h / 24)")
    high_value_ratio = tl.derive("tx_max_24h / tx_avg_24h")
    merchant_diversity_1h = tl.derive("unique_merchants_1h / tx_count_1h")
    country_hop_flag = tl.derive("unique_countries_24h > 3")

# --- Entity 2: Failed transactions (4 features) ---

@tl.dataset(depends_on=[RawTransactions], filter="status == 'failed'")
class UserFailedTxns:
    features = tl.group_by("user_id").agg(
        failed_count_30m=tl.count(window="30m"),
        failed_count_1h=tl.count(window="1h"),
        failed_count_24h=tl.count(window="24h"),
        failed_sum_24h=tl.sum("amount", window="24h"),
    )

# --- Entity 3: Merchant risk profile (8 features) ---

@tl.dataset(depends_on=[RawTransactions])
class MerchantActivity:
    features = tl.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        merch_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
        merch_stddev_24h=tl.stddev("amount", window="24h"),
    )

# --- Entity 4: Device fingerprint (5 features) ---

@tl.dataset(depends_on=[RawTransactions])
class DeviceActivity:
    features = tl.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_tx_count_24h=tl.count(window="24h"),
        device_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        device_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        device_unique_merchants_24h=tl.distinct_count("merchant_id", window="24h"),
    )

# --- Entity 5: IP address activity (5 features) ---

@tl.dataset(depends_on=[RawTransactions])
class IPActivity:
    features = tl.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_tx_count_24h=tl.count(window="24h"),
        ip_unique_users_1h=tl.distinct_count("user_id", window="1h"),
        ip_unique_users_24h=tl.distinct_count("user_id", window="24h"),
        ip_unique_devices_24h=tl.distinct_count("device_id", window="24h"),
    )

ALL_DATASETS = [
    RawTransactions,
    UserTransactions,
    UserFailedTxns,
    MerchantActivity,
    DeviceActivity,
    IPActivity,
]

FEATURE_COUNT = (
    19 + 6  # UserTransactions: 19 agg + 6 derive
    + 4      # UserFailedTxns
    + 8      # MerchantActivity
    + 5      # DeviceActivity
    + 5      # IPActivity
)  # = 47

# ---------------------------------------------------------------------------
# Realistic event generation
# ---------------------------------------------------------------------------

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "NG", "CN", "AU"]
STATUSES = ["success", "success", "success", "success", "success",
            "success", "success", "success", "failed", "failed"]  # 20% failure rate

def zipf_id(prefix: str, n: int, alpha: float = 1.2) -> str:
    """Generate Zipfian-distributed IDs (few hot, many cold)."""
    u = random.random()
    # Inverse CDF approximation for Zipf
    rank = int((u * n ** (1 - alpha) + (1 - u)) ** (1 / (1 - alpha)))
    rank = max(1, min(rank, n))
    return f"{prefix}{rank:06d}"

def generate_event(
    n_users: int = 10000,
    n_merchants: int = 2000,
    n_devices: int = 5000,
    n_ips: int = 8000,
) -> dict:
    """Generate a single realistic payment event."""
    return {
        "user_id": zipf_id("user_", n_users),
        "merchant_id": zipf_id("merch_", n_merchants),
        "device_id": zipf_id("dev_", n_devices),
        "ip_address": zipf_id("ip_", n_ips),
        "amount": round(random.lognormvariate(3.5, 1.5), 2),  # median ~$33, long tail
        "country": random.choice(COUNTRIES),
        "status": random.choice(STATUSES),
        "currency": "USD",
    }


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def run_worker(proc_id: int, n_events: int, batch_size: int = 1000) -> dict:
    """Single worker process: register, generate, push, flush."""
    app = tl.App("localhost:6400")
    app.register(*ALL_DATASETS)

    events = [generate_event() for _ in range(n_events)]

    start = time.monotonic()
    sent = 0
    while sent < len(events):
        chunk = events[sent:sent + batch_size]
        app.push_many(RawTransactions, chunk)
        sent += len(chunk)
    app.flush()
    elapsed = time.monotonic() - start

    return {
        "proc_id": proc_id,
        "events": n_events,
        "elapsed": elapsed,
        "eps": n_events / elapsed,
    }


def profile_pipeline():
    """Register pipeline, push some events, show feature counts and memory."""
    app = tl.App("localhost:6400")
    app.register(*ALL_DATASETS)

    print(f"\n{'='*60}")
    print(f" Fraud Detection Pipeline Profile")
    print(f"{'='*60}")
    print(f" Entities:  5 (user, merchant, device, IP, failed_txns)")
    print(f" Features:  {FEATURE_COUNT} total")
    print(f"   UserTransactions:  25 (19 agg + 6 derive)")
    print(f"   UserFailedTxns:     4")
    print(f"   MerchantActivity:   8")
    print(f"   DeviceActivity:     5")
    print(f"   IPActivity:         5")
    print(f" Windows:   30m, 1h, 24h, 7d")
    print(f" Operators: count, sum, avg, min, max, stddev,")
    print(f"            distinct_count (HLL), last, derive")
    print()

    # Push 1000 events to warm up
    print(" Pushing 1000 warm-up events...")
    events = [generate_event() for _ in range(1000)]
    for i in range(0, len(events), 100):
        app.push_many(RawTransactions, events[i:i+100])
    app.flush()
    time.sleep(0.5)

    # Sample a feature read
    sample_user = events[0]["user_id"]
    features = app.get(sample_user)
    print(f"\n Sample features for {sample_user}:")
    if hasattr(features, '_data'):
        for k, v in sorted(features._data.items()):
            if v is not None:
                print(f"   {k}: {v}")
    else:
        print(f"   (raw): {features}")

    # Memory
    import urllib.request
    try:
        resp = urllib.request.urlopen("http://localhost:6401/debug/memory")
        mem = json.loads(resp.read())
        print(f"\n Memory:")
        print(f"   Total entities: {mem['entity_count']}")
        print(f"   Estimated bytes: {mem['estimated_bytes']:,}")
        print(f"   Per stream:")
        for s in mem["per_stream"]:
            if s["key_count"] > 0:
                per_key = s["estimated_bytes"] / s["key_count"] if s["key_count"] else 0
                print(f"     {s['name']}: {s['key_count']} keys, "
                      f"{s['estimated_bytes']:,} bytes "
                      f"({per_key:.0f} bytes/key)")
    except Exception as e:
        print(f"   (could not read /debug/memory: {e})")

    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Fraud detection pipeline benchmark")
    parser.add_argument("--events", type=int, default=100000, help="Total events")
    parser.add_argument("--clients", type=int, default=1, help="Parallel client processes")
    parser.add_argument("--batch-size", type=int, default=1000, help="Events per batch")
    parser.add_argument("--profile", action="store_true", help="Show pipeline profile + memory")
    args = parser.parse_args()

    if args.profile:
        profile_pipeline()
        return

    events_per = args.events // args.clients
    total = events_per * args.clients

    print(f"\n{'='*60}")
    print(f" Fraud Detection Pipeline Benchmark")
    print(f"{'='*60}")
    print(f" Pipeline:  5 entity types, {FEATURE_COUNT} features")
    print(f" Events:    {total:,} ({args.clients} proc x {events_per:,})")
    print(f" Batch:     {args.batch_size}")
    print(f" Data:      Zipfian users (10K), merchants (2K),")
    print(f"            devices (5K), IPs (8K)")
    print(f"{'='*60}\n")

    start = time.monotonic()
    if args.clients == 1:
        result = run_worker(0, total, args.batch_size)
        results = [result]
    else:
        with multiprocessing.Pool(args.clients) as pool:
            results = pool.starmap(
                run_worker,
                [(i, events_per, args.batch_size) for i in range(args.clients)],
            )
    wall = time.monotonic() - start

    # Per-worker stats
    for r in sorted(results, key=lambda x: x["elapsed"]):
        print(f"  [proc-{r['proc_id']}] {r['events']:,} events in "
              f"{r['elapsed']:.2f}s = {r['eps']:,.0f} eps")

    print(f"\n  Total:      {total:,} events")
    print(f"  Wall time:  {wall:.2f}s")
    print(f"  Throughput: {total / wall:,.0f} events/sec")
    print(f"  Per-event:  {wall / total * 1e6:.1f} µs")

    # Memory after load
    import urllib.request
    try:
        resp = urllib.request.urlopen("http://localhost:6401/debug/memory")
        mem = json.loads(resp.read())
        total_bytes = mem["estimated_bytes"]
        entities = mem["entity_count"]
        print(f"\n  Memory:     {total_bytes / 1024 / 1024:.1f} MB "
              f"({entities:,} entities, "
              f"{total_bytes / entities:.0f} bytes/entity)")
    except:
        pass

    print()


if __name__ == "__main__":
    main()
