#!/usr/bin/env python3
"""30-day deterministic replay benchmark for Tally.

Synthesizes a user-specified number of fraud events (default: 30M) spread
over a 30-day timestamp window, replays them into a running Tally instance
using multi-process ``push_many(batch_size=1000)`` workers, and prints a
single compact report to stdout:

    events_total=30000000
    elapsed_seconds=27.413
    events_per_sec=1094371
    p50_push_us=41.0
    p99_push_us=180.0
    keys_total=99874
    final_state_mb=312.4

Dual purpose:

1. **Launch benchmark** — this is the canonical script that produces the
   headline number for the v2.1 launch. The seed is pinned to 42 so
   anyone running on the same hardware gets a reproducible result.

2. **Backfill tool** — point ``--host/--port`` at your own Tally instance
   and the same CLI will replay deterministic synthetic traffic, or
   (with ``--input path.jsonl``) your own captured event log. Events use
   event-time bucketing (Phase 8 SCHM-03) so a 30-day window replayed in
   30 seconds of wall-clock produces the same feature values you would
   have computed streaming in real time.

Usage::

    # Launch headline run (requires production-sized box)
    python benchmark/replay/replay_30d.py --events 30000000 --workers 8

    # Smoke run against a local dev server
    python benchmark/replay/replay_30d.py --events 100000 --workers 4

    # Backfill from a JSONL trace
    python benchmark/replay/replay_30d.py --input events.jsonl --workers 8
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
import urllib.error
import urllib.request
from typing import List, Optional, Tuple

# --- Path hack so the script runs without installing the SDK -----------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
for _p in (_PROJECT_ROOT, os.path.join(_PROJECT_ROOT, "python")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import tally as tl  # noqa: E402
from tally import dataset, group_by, source  # noqa: E402

from benchmark.replay.generator import generate  # noqa: E402


# ---------------------------------------------------------------------------
# Pipeline definition (fraud shape, medium-complexity)
# ---------------------------------------------------------------------------

def _build_pipeline():
    """Define the replay pipeline.

    Built inside a function so (a) decorators only run when we need them
    and (b) multiprocessing workers can rebuild an equivalent definition
    without sharing live objects across the fork boundary.
    """

    @source
    class RawTxns:
        pass

    @dataset(depends_on=[RawTxns])
    class Transactions:
        features = group_by("user_id").agg(
            tx_count_1h=tl.count(window="1h"),
            tx_sum_1h=tl.sum("amount", window="1h"),
            avg_amount_1h=tl.avg("amount", window="1h"),
            max_amount_24h=tl.max("amount", window="24h"),
            failed_count_30m=tl.count(window="30m", where="status == 'failed'"),
        )
        failure_rate = tl.derive("failed_count_30m / tx_count_1h")

    return [RawTxns, Transactions], RawTxns


# ---------------------------------------------------------------------------
# Worker (module-level for multiprocessing pickling)
# ---------------------------------------------------------------------------

def _worker(
    events: List[dict],
    host: str,
    port: int,
    batch_size: int,
) -> Tuple[int, Optional[str]]:
    """Push ``events`` to Tally in chunks of ``batch_size`` and flush.

    Creates its own ``tl.App`` connection (TCP sockets don't cross the
    fork boundary cleanly, and multiplexing one socket across workers
    serializes the hot path — which is exactly what we're trying to avoid).

    Returns:
        ``(batches_sent, last_error_or_none)``. A non-None error string
        is surfaced to main() which turns it into a non-zero exit code.
    """
    try:
        # Worker must re-declare the same pipeline so stream_class has a
        # valid `_tally_stream_name` attribute. This does NOT re-register
        # on the server — main() already did that once before spawning.
        (_, raw_txns) = _build_pipeline()
        app = tl.App(f"{host}:{port}", timeout=60.0)

        batches_sent = 0
        for i in range(0, len(events), batch_size):
            chunk = events[i:i + batch_size]
            app.push_many(raw_txns, chunk)
            batches_sent += 1
        # flush() blocks until the server has drained this worker's queue —
        # required so our wall-clock includes actual ingestion, not just
        # the time to hand bytes to the kernel.
        app.flush()
        app.close()
        return batches_sent, None
    except Exception as exc:  # noqa: BLE001 — boundary between processes, report anything
        return 0, f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Driver helpers
# ---------------------------------------------------------------------------

def _shard_events(events: List[dict], workers: int) -> List[List[dict]]:
    """Partition events into ``workers`` shards by ``hash(user_id) % workers``.

    Per RESEARCH Q2: hashing on the key keeps each worker's touched-key
    set disjoint, which minimizes cross-shard contention on the server's
    per-stream DashMap locks (Phase 14).
    """
    shards: List[List[dict]] = [[] for _ in range(workers)]
    for ev in events:
        # Stable hash: Python's built-in hash() is randomized per process
        # on str, so we use a deterministic fold instead.
        key = ev.get("user_id", "")
        h = 0
        for ch in key:
            h = (h * 131 + ord(ch)) & 0xFFFFFFFF
        shards[h % workers].append(ev)
    return shards


def _http_get_json(url: str, timeout: float = 5.0) -> Optional[dict]:
    """Fetch a URL and parse JSON. Returns None on any failure (best-effort)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError):
        return None


def _http_get_text(url: str, timeout: float = 5.0) -> Optional[str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return None


def _extract_push_latency(latency_json: Optional[dict]) -> Tuple[float, float]:
    """Pull (p50_us, p99_us) for the PUSH command from /debug/latency JSON.

    The endpoint groups histograms by command. We match case-insensitively
    on the command name because the server's rendering has varied
    historically ("PUSH" vs "Push").
    """
    if not latency_json:
        return (0.0, 0.0)
    per_command = latency_json.get("per_command") or []
    for entry in per_command:
        cmd = str(entry.get("command", "")).lower()
        if cmd == "push":
            return (
                float(entry.get("p50_us") or 0.0),
                float(entry.get("p99_us") or 0.0),
            )
    return (0.0, 0.0)


def _extract_keys_total(metrics_text: Optional[str]) -> int:
    """Parse `tally_keys_total <N>` out of the /metrics Prometheus body."""
    if not metrics_text:
        return 0
    for line in metrics_text.splitlines():
        line = line.strip()
        if line.startswith("tally_keys_total "):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return 0
    return 0


def _extract_memory_mb(memory_json: Optional[dict]) -> float:
    """Pull a best-effort total memory footprint in MB from /debug/memory.

    The debug/memory endpoint has varied in shape over the project's life;
    this function tries a few known keys and falls back to summing per-stream
    totals. Returns 0.0 if nothing usable is found.
    """
    if not memory_json:
        return 0.0
    for key in ("total_bytes", "grand_total_bytes", "estimated_bytes"):
        if key in memory_json and isinstance(memory_json[key], (int, float)):
            return round(float(memory_json[key]) / (1024 * 1024), 2)
    # Fall back: sum per-stream totals if present.
    per_stream = memory_json.get("per_stream") or memory_json.get("streams") or []
    total = 0
    for s in per_stream:
        v = s.get("estimated_bytes") or s.get("total_bytes") or 0
        try:
            total += int(v)
        except (TypeError, ValueError):
            pass
    return round(total / (1024 * 1024), 2) if total else 0.0


def _load_events(args) -> List[dict]:
    """Either read JSONL from ``--input`` or synthesize via generate()."""
    if args.input:
        with open(args.input, "r", encoding="utf-8") as fh:
            events = [json.loads(line) for line in fh if line.strip()]
        # Sort for time-ordered replay (matters for window semantics).
        events.sort(key=lambda e: e.get("ts", 0))
        return events
    return generate(args.events, seed=args.seed, days=args.days)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Deterministic 30-day replay benchmark for Tally. Doubles as a "
            "historical-backfill tool — see README for --input usage."
        ),
    )
    p.add_argument("--events", type=int, default=30_000_000,
                   help="Number of events to replay (default: 30000000)")
    p.add_argument("--workers", type=int, default=8,
                   help="Number of parallel worker processes (default: 8)")
    p.add_argument("--batch-size", type=int, default=1000,
                   help="Events per push_many call (default: 1000)")
    p.add_argument("--host", default="localhost",
                   help="Tally TCP host (default: localhost)")
    p.add_argument("--port", type=int, default=6400,
                   help="Tally TCP port (default: 6400)")
    p.add_argument("--mgmt-port", type=int, default=6401,
                   help="Tally HTTP management port for post-run report queries (default: 6401)")
    p.add_argument("--days", type=int, default=30,
                   help="Timestamp window width in days (default: 30)")
    p.add_argument("--seed", type=int, default=42,
                   help="RNG seed for the deterministic generator (default: 42)")
    p.add_argument("--warmup", dest="warmup", action="store_true", default=True,
                   help="Send a small warmup batch before the measured run (default: on)")
    p.add_argument("--no-warmup", dest="warmup", action="store_false",
                   help="Skip warmup (useful in CI where wall-clock is tight)")
    p.add_argument("--input", default=None,
                   help="Optional JSONL file of events; bypasses the synthetic generator (backfill mode)")
    p.add_argument("--output", default=None,
                   help="Optional path to write the report as JSON in addition to stdout")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)

    # Register pipelines on the server (one-shot, from main process).
    streams, _raw_txns = _build_pipeline()
    try:
        app = tl.App(f"{args.host}:{args.port}", timeout=30.0)
        app.register(*streams)
        app.close()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: could not connect to Tally at {args.host}:{args.port}: {exc}",
              file=sys.stderr)
        return 2

    # Optional warmup — not timed, discarded.
    if args.warmup:
        warmup_events = generate(10_000, seed=args.seed - 1 if args.seed > 0 else 1,
                                 days=args.days)
        warmup_shards = _shard_events(warmup_events, max(1, args.workers // 2))
        ctx = mp.get_context("spawn")
        with ctx.Pool(len(warmup_shards)) as pool:
            pool.starmap(
                _worker,
                [(shard, args.host, args.port, args.batch_size) for shard in warmup_shards],
            )

    # Synthesize (or load) events.
    events = _load_events(args)
    total_events = len(events)
    shards = _shard_events(events, args.workers)

    # --- Measured run --------------------------------------------------------
    # IMPORTANT: t0 is captured AFTER pipeline registration and AFTER the
    # event list is materialized. Per RESEARCH Pitfall 5, measuring
    # generation time as part of "ingestion throughput" would inflate the
    # number artificially.
    ctx = mp.get_context("spawn")
    t0 = time.perf_counter()
    with ctx.Pool(args.workers) as pool:
        results = pool.starmap(
            _worker,
            [(shard, args.host, args.port, args.batch_size) for shard in shards],
        )
    elapsed = time.perf_counter() - t0

    # Check worker results.
    for batches, err in results:
        if err:
            print(f"ERROR: worker failure: {err}", file=sys.stderr)
            return 3

    eps = total_events / elapsed if elapsed > 0 else 0.0

    # --- Post-run metrics from management API --------------------------------
    base = f"http://{args.host}:{args.mgmt_port}"
    latency_json = _http_get_json(f"{base}/debug/latency")
    memory_json = _http_get_json(f"{base}/debug/memory")
    metrics_text = _http_get_text(f"{base}/metrics")

    p50, p99 = _extract_push_latency(latency_json)
    keys_total = _extract_keys_total(metrics_text)
    final_state_mb = _extract_memory_mb(memory_json)

    # --- Report --------------------------------------------------------------
    # key=value lines — stable contract consumed by test_replay_30d.py and
    # by the launch-blog tooling. Do NOT reorder without updating callers.
    report = {
        "events_total": total_events,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_sec": round(eps, 1),
        "p50_push_us": round(p50, 2),
        "p99_push_us": round(p99, 2),
        "keys_total": keys_total,
        "final_state_mb": final_state_mb,
    }
    print("=== Tally 30-day Replay Report ===")
    for k, v in report.items():
        print(f"{k}={v}")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"# wrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
