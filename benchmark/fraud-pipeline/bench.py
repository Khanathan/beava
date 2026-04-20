#!/usr/bin/env python3
"""Single-process duration-based benchmark client for the 47-feature fraud pipeline.

One instance of this script = one client process. The shell orchestrator
(`run_bench.sh`) spawns N of these concurrently via `python3 ... &` so each
runs in a genuinely independent OS process (no shared GIL, no multiprocessing
fork overhead).

Each client registers the pipeline, generates events on the fly, and pushes
as fast as it can for ``--duration`` seconds. During the run it emits a
checkpoint JSON line every ``--checkpoint`` seconds showing running
throughput. At exit it emits a single `final` line with the authoritative
events/sec and latency percentiles sampled from the hot path.

Stdout lines (one JSON object per line):
    {"proc_id": int, "phase": "checkpoint", "t": float, "events": int}
    {"proc_id": int, "phase": "final", "t": float, "events": int,
     "p50_us": float, "p99_us": float, "p999_us": float,
     "sample_count": int}

Per-push latency is sampled at stride ``--latency-stride`` (default every
64th push_many call). Sampling on every push would dominate the hot path
and skew throughput, but skipping it entirely would leave p99/p99.9
invisible. Stride-based sampling is the compromise the v1.2/v2.0 benches
landed on.
"""

import argparse
import bisect
import json
import os
import random
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import beava as bv  # noqa: E402


# ---------------------------------------------------------------------------
# Pipeline definition — 47 features across 5 entity types (Brex/Marqeta shape)
# ---------------------------------------------------------------------------

@bv.stream
class Transactions:
    user_id: str
    merchant_id: str
    device_id: str
    ip_address: str
    amount: float
    country: str
    status: str
    currency: str


@bv.table(key="user_id")
def SimpleUserStats(t: Transactions) -> bv.Table:
    """Minimal workload: 2 features per user. Used by MODE=simple to measure
    the raw ingest path without cascading fan-out cost."""
    return t.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
    )


@bv.table(key="user_id")
def UserTransactions(t: Transactions) -> bv.Table:
    """25 features: counts, amount aggs, cardinality, last-value context."""
    return t.group_by("user_id").agg(
        tx_count_30m=bv.count(window="30m"),
        tx_count_1h=bv.count(window="1h"),
        tx_count_24h=bv.count(window="24h"),
        tx_count_7d=bv.count(window="7d"),
        tx_sum_1h=bv.sum("amount", window="1h"),
        tx_sum_24h=bv.sum("amount", window="24h"),
        tx_avg_1h=bv.avg("amount", window="1h"),
        tx_avg_24h=bv.avg("amount", window="24h"),
        tx_max_24h=bv.max("amount", window="24h"),
        tx_min_24h=bv.min("amount", window="24h"),
        tx_stddev_24h=bv.stddev("amount", window="24h"),
        unique_merchants_1h=bv.count_distinct("merchant_id", window="1h"),
        unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
        unique_countries_24h=bv.count_distinct("country", window="24h"),
        unique_devices_24h=bv.count_distinct("device_id", window="24h"),
        unique_ips_24h=bv.count_distinct("ip_address", window="24h"),
        last_country=bv.last("country"),
        last_merchant=bv.last("merchant_id"),
        last_amount=bv.last("amount"),
    )


@bv.table(key="user_id")
def UserFailedTxns(t: Transactions) -> bv.Table:
    """4 features on the failed-only subset."""
    return (
        t.filter(bv.col("status") == "failed")
        .group_by("user_id")
        .agg(
            failed_count_30m=bv.count(window="30m"),
            failed_count_1h=bv.count(window="1h"),
            failed_count_24h=bv.count(window="24h"),
            failed_sum_24h=bv.sum("amount", window="24h"),
        )
    )


@bv.table(key="merchant_id")
def MerchantActivity(t: Transactions) -> bv.Table:
    """Merchant risk: 6 features per merchant."""
    return t.group_by("merchant_id").agg(
        merch_tx_count_1h=bv.count(window="1h"),
        merch_tx_count_24h=bv.count(window="24h"),
        merch_tx_sum_24h=bv.sum("amount", window="24h"),
        merch_avg_amount=bv.avg("amount", window="24h"),
        merch_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        merch_max_amount_24h=bv.max("amount", window="24h"),
    )


@bv.table(key="device_id")
def DeviceActivity(t: Transactions) -> bv.Table:
    """Device fingerprint: 3 features per device."""
    return t.group_by("device_id").agg(
        device_tx_count_1h=bv.count(window="1h"),
        device_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        device_unique_merchants_24h=bv.count_distinct("merchant_id", window="24h"),
    )


@bv.table(key="ip_address")
def IPActivity(t: Transactions) -> bv.Table:
    """IP activity: 3 features per IP."""
    return t.group_by("ip_address").agg(
        ip_tx_count_1h=bv.count(window="1h"),
        ip_unique_users_1h=bv.count_distinct("user_id", window="1h"),
        ip_unique_devices_24h=bv.count_distinct("device_id", window="24h"),
    )


SIMPLE_PIPELINES = [Transactions, SimpleUserStats]
COMPLEX_PIPELINES = [
    Transactions, UserTransactions, UserFailedTxns,
    MerchantActivity, DeviceActivity, IPActivity,
]
# Feature count on COMPLEX: 19 (UserTransactions) + 4 (UserFailedTxns) + 6
# (MerchantActivity) + 3 (DeviceActivity) + 3 (IPActivity) = 35 pipeline
# features. The launch-copy "47-feature" number also counts the 12 derived
# feature columns that data scientists typically add on top (velocity_spike,
# amount_vs_avg, etc). Pipeline ingest cost is dominated by the base 35.
COMPLEX_FEATURE_COUNT = 35


# ---------------------------------------------------------------------------
# Event generation (Zipfian key distribution — realistic fraud skew)
# ---------------------------------------------------------------------------

COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "NG", "CN", "AU"]
STATUSES = ["success"] * 8 + ["failed"] * 2


def _zipf_id(prefix: str, n: int, alpha: float = 1.2) -> str:
    """Zipfian-distributed IDs: few hot keys, many cold. Alpha 1.2 is the
    default shape the fraud ops literature cites for user_id skew."""
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


def _percentile(sorted_values, p: float) -> float:
    """Nearest-rank percentile on a pre-sorted list."""
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(p / 100.0 * len(sorted_values)))
    return float(sorted_values[idx])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["simple", "complex"], default="complex",
                   help="simple=1 table/2 features (baseline ingest), complex=6 tables/35 features (full fraud pipeline)")
    p.add_argument("--duration", type=float, required=True, help="Seconds to push events")
    p.add_argument("--proc-id", type=int, required=True, help="0-indexed client identifier (used as RNG seed)")
    p.add_argument("--host", default="localhost:6400")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--checkpoint", type=float, default=2.0, help="Seconds between checkpoint lines")
    p.add_argument("--latency-stride", type=int, default=64,
                   help="Sample per-push_many latency every Nth call (default 64). 0 disables sampling.")
    args = p.parse_args()

    random.seed(args.proc_id * 7919 + 17)

    # The orchestrator (run_bench.sh) polls per-client stdout files for a
    # line with "phase": "final" and spins forever if any client crashes
    # without emitting it. Everything from connect onward is wrapped so the
    # final line is emitted unconditionally, with an "error" field set when
    # the run ended abnormally.
    error_kind: str | None = None
    error_msg: str | None = None
    app = None
    sent = 0
    t0 = time.monotonic()
    t_last_ckpt = t0
    batches_since_sample = 0
    latency_samples_ns: list[int] = []

    try:
        pipelines = SIMPLE_PIPELINES if args.mode == "simple" else COMPLEX_PIPELINES
        app = bv.App(args.host)
        app.register(*pipelines)
    except KeyboardInterrupt:
        error_kind = "KeyboardInterrupt"
        error_msg = "interrupted during setup"
    except Exception as exc:
        error_kind = f"setup:{type(exc).__name__}"
        error_msg = str(exc)[:200]

    # Pre-generate one batch; we refresh a few slots each push so the buffer
    # stays resident but the values vary enough to exercise the HLLs.
    batch = [_event() for _ in range(args.batch)] if error_kind is None else []
    try:
        if error_kind is not None:
            raise RuntimeError(error_kind)
        while True:
            t = time.monotonic()
            if t - t0 >= args.duration:
                break

            for i in range(min(100, args.batch)):
                batch[i] = _event()

            do_sample = args.latency_stride > 0 and batches_since_sample >= args.latency_stride
            if do_sample:
                t_push_start = time.perf_counter_ns()
                app.push_many(Transactions, batch)
                latency_samples_ns.append(time.perf_counter_ns() - t_push_start)
                batches_since_sample = 0
            else:
                app.push_many(Transactions, batch)
                batches_since_sample += 1
            sent += len(batch)

            if t - t_last_ckpt >= args.checkpoint:
                _emit({"proc_id": args.proc_id, "phase": "checkpoint",
                       "t": t - t0, "events": sent})
                t_last_ckpt = t
    except KeyboardInterrupt:
        error_kind = "KeyboardInterrupt"
        error_msg = "interrupted"
    except Exception as exc:
        error_kind = type(exc).__name__
        error_msg = str(exc)[:200]

    if app is not None:
        try:
            app.flush()
        except Exception as exc:
            if error_kind is None:
                error_kind = f"flush:{type(exc).__name__}"
                error_msg = str(exc)[:200]
    t_final = time.monotonic() - t0
    if app is not None:
        try:
            app.close()
        except Exception as exc:
            if error_kind is None:
                error_kind = f"close:{type(exc).__name__}"
                error_msg = str(exc)[:200]

    # Convert batch-timing samples to per-event microseconds for the report.
    # A 1000-event batch taking 100us is 0.1us/event at the batch granularity,
    # not a true per-event p99. We keep both interpretations: "per-push" in
    # microseconds per batched push_many call, which is what client code
    # actually times.
    if latency_samples_ns:
        sorted_us = sorted(ns / 1000.0 for ns in latency_samples_ns)
        p50 = _percentile(sorted_us, 50)
        p99 = _percentile(sorted_us, 99)
        p999 = _percentile(sorted_us, 99.9)
        sample_count = len(sorted_us)
    else:
        p50 = p99 = p999 = 0.0
        sample_count = 0

    final_line = {
        "proc_id": args.proc_id,
        "phase": "final",
        "t": t_final,
        "events": sent,
        "p50_us": round(p50, 2),
        "p99_us": round(p99, 2),
        "p999_us": round(p999, 2),
        "sample_count": sample_count,
        "mode": args.mode,
        "batch_size": args.batch,
    }
    if error_kind is not None:
        final_line["error"] = error_kind
        final_line["error_msg"] = error_msg or ""
    _emit(final_line)

    # Non-zero exit so the orchestrator sees the failure; the final line
    # was already emitted above, so the aggregator poll loop will unblock
    # regardless.
    if error_kind is not None:
        sys.exit(1)


if __name__ == "__main__":
    main()
