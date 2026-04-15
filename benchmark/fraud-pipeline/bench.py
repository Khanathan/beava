#!/usr/bin/env python3
"""Single-process duration-based benchmark client.

One instance of this script = one client process. The shell orchestrator
(`run_bench.sh`) spawns N of these concurrently via `python3 ... &` so each
runs in a genuinely independent OS process (no shared GIL, no multiprocessing
fork overhead).

Each client registers pipelines, generates events on the fly, and pushes as
fast as it can for ``--duration`` seconds. It emits a checkpoint JSON line
every ``--checkpoint`` seconds showing running throughput, then a final
summary line. The shell harness consumes the checkpoint stream to show live
EPS; the final line is authoritative.

Stdout lines:
    {"proc_id": int, "phase": "checkpoint", "t": float, "events": int}
    {"proc_id": int, "phase": "final",      "t": float, "events": int}
"""

import argparse
import json
import os
import random
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import tally as tl  # noqa: E402


# ---------------------------------------------------------------------------
# Pipelines (single stream; two workload variants)
# ---------------------------------------------------------------------------

@tl.stream
class Transactions:
    user_id: str
    merchant_id: str
    device_id: str
    ip_address: str
    amount: float
    country: str
    status: str
    currency: str


@tl.table(key="user_id")
def SimpleUserStats(t: Transactions) -> tl.Table:
    return t.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
    )


@tl.table(key="user_id")
def UserTransactions(t: Transactions) -> tl.Table:
    return t.group_by("user_id").agg(
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
def UserFailedTxns(t: Transactions) -> tl.Table:
    return (
        t.filter(tl.col("status") == "failed")
        .group_by("user_id")
        .agg(
            failed_count_30m=tl.count(window="30m"),
            failed_count_1h=tl.count(window="1h"),
            failed_count_24h=tl.count(window="24h"),
            failed_sum_24h=tl.sum("amount", window="24h"),
        )
    )


@tl.table(key="merchant_id")
def MerchantActivity(t: Transactions) -> tl.Table:
    return t.group_by("merchant_id").agg(
        merch_tx_count_1h=tl.count(window="1h"),
        merch_tx_count_24h=tl.count(window="24h"),
        merch_tx_sum_24h=tl.sum("amount", window="24h"),
        merch_avg_amount=tl.avg("amount", window="24h"),
        merch_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        merch_max_amount_24h=tl.max("amount", window="24h"),
    )


@tl.table(key="device_id")
def DeviceActivity(t: Transactions) -> tl.Table:
    return t.group_by("device_id").agg(
        device_tx_count_1h=tl.count(window="1h"),
        device_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        device_unique_merchants_24h=tl.count_distinct("merchant_id", window="24h"),
    )


@tl.table(key="ip_address")
def IPActivity(t: Transactions) -> tl.Table:
    return t.group_by("ip_address").agg(
        ip_tx_count_1h=tl.count(window="1h"),
        ip_unique_users_1h=tl.count_distinct("user_id", window="1h"),
        ip_unique_devices_24h=tl.count_distinct("device_id", window="24h"),
    )


SIMPLE_PIPELINES = [Transactions, SimpleUserStats]
COMPLEX_PIPELINES = [
    Transactions, UserTransactions, UserFailedTxns,
    MerchantActivity, DeviceActivity, IPActivity,
]


# ---------------------------------------------------------------------------
# Event generation (Zipfian key distribution — realistic fraud skew)
# ---------------------------------------------------------------------------

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "NG", "CN", "AU"]
STATUSES = ["success"] * 8 + ["failed"] * 2


def _zipf_id(prefix: str, n: int, alpha: float = 1.2) -> str:
    u = random.random()
    rank = int((u * n ** (1 - alpha) + (1 - u)) ** (1 / (1 - alpha)))
    rank = max(1, min(rank, n))
    return f"{prefix}{rank:06d}"


def _event() -> dict:
    return {
        "user_id":     _zipf_id("user_",  10000),
        "merchant_id": _zipf_id("merch_", 2000),
        "device_id":   _zipf_id("dev_",   5000),
        "ip_address":  _zipf_id("ip_",    8000),
        "amount":      round(random.lognormvariate(3.5, 1.5), 2),
        "country":     random.choice(COUNTRIES),
        "status":      random.choice(STATUSES),
        "currency":    "USD",
    }


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["simple", "complex"], required=True)
    p.add_argument("--duration", type=float, required=True, help="Seconds to push events")
    p.add_argument("--proc-id", type=int, required=True)
    p.add_argument("--host", default="localhost:6400")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--checkpoint", type=float, default=2.0, help="Seconds between checkpoint lines")
    args = p.parse_args()

    random.seed(args.proc_id * 7919 + 17)

    pipelines = SIMPLE_PIPELINES if args.mode == "simple" else COMPLEX_PIPELINES
    app = tl.App(args.host)
    app.register(*pipelines)

    t0 = time.monotonic()
    t_last_ckpt = t0
    sent = 0

    # Pre-generate one batch; we'll refresh each push so the buffer stays
    # resident but values vary.
    batch = [_event() for _ in range(args.batch)]

    while True:
        t = time.monotonic()
        if t - t0 >= args.duration:
            break
        # Refresh a few slots each batch so the stream isn't a loop of
        # identical events (small cost, ~0.3 µs/event).
        for i in range(min(100, args.batch)):
            batch[i] = _event()
        app.push_many(Transactions, batch)
        sent += len(batch)

        if t - t_last_ckpt >= args.checkpoint:
            _emit({"proc_id": args.proc_id, "phase": "checkpoint",
                   "t": t - t0, "events": sent})
            t_last_ckpt = t

    app.flush()
    t_final = time.monotonic() - t0
    app.close()

    _emit({"proc_id": args.proc_id, "phase": "final",
           "t": t_final, "events": sent})


if __name__ == "__main__":
    main()
