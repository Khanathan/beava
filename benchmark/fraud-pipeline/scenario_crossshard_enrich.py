#!/usr/bin/env python3
"""Phase 56 perf-gate scenario — cross-shard EnrichFromTable workload.

This client mirrors ``bench.py``'s shape (duration-driven push loop,
checkpoint+final JSONL on stdout) but registers a pipeline that FORCES
≥1 cross-shard EnrichFromTable per event.

Pipeline shape (D-D3, 56-CONTEXT):

    @bv.source_table(key="country_code")            # shard_key=country_code
    class Countries:
        country_code: str
        gdp_usd: int
        continent: str

    @bv.stream(shard_key="user_id")                 # shard_key=user_id
    class Txns:
        user_id: str
        country_code: str
        amount: float

    # 1 EnrichFromTable per event — uniform country_code + Zipf user_id.
    # Because country_code is drawn uniformly from a 50-country pool while
    # user_id is drawn from 10_000 user_ids, the right-side shard almost
    # always differs from the left-side shard: at N=8 shards, the probability
    # of cross-shard routing per event is ~(1 − 1/8) = 87.5%.
    Enriched = Txns.join(Countries, on=["country_code"], type="left")

    @bv.table(key="user_id")
    def UserEnrichedStats(e: Enriched) -> bv.Table:
        return e.group_by("user_id").agg(
            tx_count_1h=bv.count(window="1h"),
            last_gdp=bv.last("gdp_usd"),
            last_continent=bv.last("continent"),
        )

The output of the cross-shard enrichment read is measured by the
``beava_enrich_cross_shard_total{table="Countries"}`` counter at /metrics.
The aggregate throughput is captured by ``run_bench.sh``'s stdout-parse
path and written to ``summary.json`` as ``throughput.aggregate_eps``.

This file is picked up by ``run_bench.sh`` when the env var
``BEAVA_ENRICH_CROSSSHARD_SCENARIO=1`` is set.
"""

import argparse
import json
import os
import random
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "python"))

import beava as bv  # noqa: E402


# ---------------------------------------------------------------------------
# Pipeline — Stream(shard_key=user_id) joined against SourceTable(key=country_code)
# ---------------------------------------------------------------------------

@bv.source_table(key="country_code")
class Countries:
    country_code: str
    gdp_usd: int
    continent: str


@bv.stream(shard_key="user_id")
class Txns:
    user_id: str
    country_code: str
    amount: float


# Stream ↔ Table enrichment — produces a keyless derivation. A downstream
# keyed table aggregates it so we touch the same hot path as fraud-pipeline.
# The enrichment itself is what drives cross-shard reads (hash(country_code)%N
# differs from hash(user_id)%N ~87.5% of the time at N=8).
Enriched = Txns.join(Countries, on=["country_code"], type="left")

# Downstream aggregation — inline construction (derivation-as-function
# decorators require class annotations, which StreamDerivation instances
# don't satisfy). The inline chain produces an equivalent keyed
# TableDerivation that the registration pipeline walks via `depends_on`.
UserEnrichedStats = Enriched.group_by("user_id").agg(
    tx_count_1h=bv.count(window="1h"),
    last_country=bv.last("country_code"),
    last_amount=bv.last("amount"),
)
UserEnrichedStats._name = "UserEnrichedStats"


PIPELINES = [Txns, Countries, Enriched, UserEnrichedStats]


# ---------------------------------------------------------------------------
# Reference country set. 50 countries keyed by ISO-ish codes.
# ---------------------------------------------------------------------------

_COUNTRY_CODES = [
    "US", "GB", "DE", "FR", "JP", "BR", "IN", "NG", "CN", "AU",
    "CA", "MX", "IT", "ES", "NL", "SE", "NO", "FI", "DK", "IE",
    "CH", "AT", "BE", "PT", "GR", "PL", "CZ", "HU", "RO", "TR",
    "RU", "UA", "KR", "SG", "MY", "TH", "VN", "ID", "PH", "ZA",
    "EG", "MA", "KE", "AR", "CL", "CO", "PE", "NZ", "IL", "AE",
]
_CONTINENTS = {
    "US": "NA", "CA": "NA", "MX": "NA",
    "BR": "SA", "AR": "SA", "CL": "SA", "CO": "SA", "PE": "SA",
    "GB": "EU", "DE": "EU", "FR": "EU", "IT": "EU", "ES": "EU",
    "NL": "EU", "SE": "EU", "NO": "EU", "FI": "EU", "DK": "EU",
    "IE": "EU", "CH": "EU", "AT": "EU", "BE": "EU", "PT": "EU",
    "GR": "EU", "PL": "EU", "CZ": "EU", "HU": "EU", "RO": "EU",
    "TR": "EU", "RU": "EU", "UA": "EU",
    "JP": "AS", "CN": "AS", "IN": "AS", "KR": "AS", "SG": "AS",
    "MY": "AS", "TH": "AS", "VN": "AS", "ID": "AS", "PH": "AS",
    "IL": "AS", "AE": "AS",
    "AU": "OC", "NZ": "OC",
    "NG": "AF", "ZA": "AF", "EG": "AF", "MA": "AF", "KE": "AF",
}


def _zipf_user(n: int = 10_000, alpha: float = 1.2) -> str:
    u = random.random()
    rank = int((u * n ** (1 - alpha) + (1 - u)) ** (1 / (1 - alpha)))
    rank = max(1, min(rank, n))
    return f"user_{rank:06d}"


def _event() -> dict:
    """Uniform country_code (50) × Zipfian user_id (10_000) guarantees that
    ~87.5% of events trigger a cross-shard enrichment read at N=8."""
    return {
        "user_id": _zipf_user(),
        "country_code": random.choice(_COUNTRY_CODES),
        "amount": round(random.lognormvariate(3.5, 1.5), 2),
    }


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _percentile(sorted_values, p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(p / 100.0 * len(sorted_values)))
    return float(sorted_values[idx])


def _seed_countries_once(app: "bv.App") -> None:
    """Pre-load all 50 countries into the Countries source table. Only proc-0
    actually writes; other procs short-circuit to avoid source_lsn collision.
    The server deduplicates by (table, key) so repeat upserts are no-ops."""
    for i, code in enumerate(_COUNTRY_CODES, start=1):
        # GDP synthetic — roughly scaled to 0.1–4T USD; deterministic per code.
        gdp = (hash(code) & 0x3FFFFF) + 100_000
        fields = {
            "country_code": code,
            "gdp_usd": int(gdp),
            "continent": _CONTINENTS.get(code, "??"),
        }
        app._client.upsert_table_row(Countries, code, fields, source_lsn=i)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["simple", "complex"], default="complex")
    p.add_argument("--duration", type=float, required=True)
    p.add_argument("--proc-id", type=int, required=True)
    p.add_argument("--host", default="localhost:6400")
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--checkpoint", type=float, default=5.0)
    p.add_argument("--latency-stride", type=int, default=64)
    # Phase 59.5-W3.5: --target-eps lets the bench self-throttle below
    # server capacity so we can measure where the REAL bottleneck is
    # (vs. "server's inbox overflows → clients exit" collapse).
    # 0 = unthrottled (default, matches pre-W3.5 behavior).
    p.add_argument("--target-eps", type=float, default=0.0)
    args = p.parse_args()

    random.seed(args.proc_id * 7919 + 17)

    error_kind = None
    error_msg = None
    app = None
    sent = 0
    t0 = time.monotonic()
    t_last_ckpt = t0
    batches_since_sample = 0
    latency_samples_ns: list[int] = []

    # File-based barrier: proc-0 seeds Countries; procs 1..N wait for a
    # fresh sentinel (mtime ≥ this proc's start) before pushing, so proc-0's
    # upserts don't compete with the push flood for shard inbox capacity.
    proc_start_time = time.time()
    seed_sentinel = os.path.join(
        os.environ.get("TMPDIR", "/tmp"),
        f"beava-bench-crossshard-seeded-{args.host.replace(':', '_')}",
    )
    try:
        app = bv.App(args.host)
        app.register(*PIPELINES)
        if args.proc_id == 0:
            try:
                os.unlink(seed_sentinel)
            except FileNotFoundError:
                pass
            _seed_countries_once(app)
            with open(seed_sentinel, "w") as f:
                f.write(str(time.monotonic()))
        else:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                try:
                    mt = os.path.getmtime(seed_sentinel)
                except FileNotFoundError:
                    mt = 0.0
                if mt >= proc_start_time:
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError(
                    f"proc-0 did not seed Countries within 30 s "
                    f"(sentinel={seed_sentinel} missing or stale)"
                )
    except KeyboardInterrupt:
        error_kind = "KeyboardInterrupt"
        error_msg = "interrupted during setup"
    except Exception as exc:
        error_kind = f"setup:{type(exc).__name__}"
        error_msg = str(exc)[:200]

    batch = [_event() for _ in range(args.batch)] if error_kind is None else []
    # Phase 59.5-W3.5: per-batch throttle derived from --target-eps.
    # target_eps=0 disables throttling (original behavior).
    batch_interval_s = (
        args.batch / args.target_eps if args.target_eps > 0 else 0.0
    )
    next_push_deadline = time.monotonic()
    try:
        if error_kind is not None:
            raise RuntimeError(error_kind)
        while True:
            t = time.monotonic()
            if t - t0 >= args.duration:
                break

            # Throttle: pace batches so sustained rate matches --target-eps.
            if batch_interval_s > 0 and t < next_push_deadline:
                time.sleep(next_push_deadline - t)
                t = time.monotonic()

            # Refresh a slice of the batch so values churn but the list stays
            # resident (matches bench.py's pattern).
            for i in range(min(100, args.batch)):
                batch[i] = _event()

            do_sample = args.latency_stride > 0 and batches_since_sample >= args.latency_stride
            if do_sample:
                t_push_start = time.perf_counter_ns()
                app.push_many(Txns, batch)
                latency_samples_ns.append(time.perf_counter_ns() - t_push_start)
                batches_since_sample = 0
            else:
                app.push_many(Txns, batch)
                batches_since_sample += 1
            sent += len(batch)

            if batch_interval_s > 0:
                next_push_deadline += batch_interval_s
                # If we're badly behind (e.g., server slowed down), don't
                # try to catch up in a tight loop — re-anchor to now.
                now_m = time.monotonic()
                if next_push_deadline < now_m - batch_interval_s:
                    next_push_deadline = now_m + batch_interval_s

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
        "scenario": "crossshard_enrich",
    }
    if error_kind is not None:
        final_line["error"] = error_kind
        final_line["error_msg"] = error_msg or ""
    _emit(final_line)

    if error_kind is not None:
        sys.exit(1)


if __name__ == "__main__":
    main()
