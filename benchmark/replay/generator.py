"""Deterministic fraud-event generator for the 30-day replay benchmark.

The goal of this module is one-liner simple: call ``generate(n)`` and get
``n`` realistic-shaped fraud transaction events whose stream is fully
determined by ``(seed, n, days, now_ms)``. Two calls with the same inputs
produce byte-identical output — a property the launch benchmark relies on
so the headline events/sec number is reproducible on any machine.

Event schema (6 fields, stable)::

    {
        "user_id":     str,   # "user_<int>" drawn from USER_POOL
        "merchant_id": str,   # "merchant_<int>" drawn from MERCHANT_POOL
        "amount":      float, # log-normal distributed
        "status":      str,   # "success" (~95%) or "failed" (~5%)
        "country":     str,   # one of 5 ISO-like codes
        "ts":          int,   # ms since epoch, uniform over [now_ms - days*86_400_000, now_ms]
    }

The returned list is sorted ascending by ``ts``. Time-ordering matters
because the Beava server's sliding-window operators bucket on the event
timestamp (Phase 8 SCHM-03 backfill semantics): replaying 30 days of
pre-stamped events into ~30 seconds of wall-clock time produces the same
feature values as would accumulate in real time.

**Why not import benchmark/beava-throughput/bench.py?** That module has
import-time side effects (argparse, path hacks, etc.). Copying the shape
constants here is simpler and lets the generator run standalone. The
canonical fraud-event shape source is `benchmark/beava-throughput/bench.py`
`make_event` — keep the keys in sync if that schema evolves.
"""

from __future__ import annotations

import random
import time
from typing import List, Optional

# --- Schema ------------------------------------------------------------------
# Canonical fraud-event keys. Mirrors the shape used by bench.py's medium
# pipeline (user_id, merchant_id, amount, status, country) and adds `ts`
# so the server buckets on event-time for windowed operators.
SCHEMA_KEYS = {"user_id", "merchant_id", "amount", "status", "country", "ts"}

# --- Pool sizing -------------------------------------------------------------
# 100k users × 5k merchants produce realistic fraud-pipeline fan-out at the
# 1.1M eps baseline (per 19-05-SUMMARY.md). Smaller pools cause pathological
# hot-key contention on Phase 14's per-stream locks; larger pools inflate
# state size without changing the benchmark story.
USER_POOL = 100_000
MERCHANT_POOL = 5_000

# Country distribution: uniform over 5 codes — keeps it simple.
COUNTRIES = ("US", "UK", "DE", "FR", "JP")

# Status weights: ~5% of events are 'failed' to exercise the where-clause
# operators in the fraud pipeline.
_STATUS_CHOICES = ("success", "failed")
_STATUS_WEIGHTS = (95, 5)


def generate(
    n: int,
    *,
    seed: int = 42,
    days: int = 30,
    now_ms: Optional[int] = None,
) -> List[dict]:
    """Generate *n* deterministic fraud events.

    Args:
        n: Number of events to produce.
        seed: RNG seed; same seed → same event stream. Default 42 is the
            launch-benchmark canonical value.
        days: Width of the timestamp window. Events are drawn uniformly
            over ``[now_ms - days*86_400_000, now_ms]``.
        now_ms: Upper bound of the timestamp window in milliseconds since
            epoch. If ``None``, defaults to the current wall-clock (for
            production runs). Tests always pin this to a fixed value so
            they stay wall-clock independent.

    Returns:
        A list of ``n`` event dicts, sorted ascending by ``ts``.
    """
    if n < 0:
        raise ValueError(f"n must be >= 0, got {n}")

    if now_ms is None:
        now_ms = time.time_ns() // 1_000_000
    window_ms = days * 86_400_000
    lo_ms = now_ms - window_ms

    # CRITICAL: use a local Random instance, not the module-level `random`.
    # The global RNG is shared process-wide and any other import that
    # touches it would break determinism.
    rng = random.Random(seed)

    events: List[dict] = [None] * n  # type: ignore[list-item]
    for i in range(n):
        events[i] = {
            "user_id": f"user_{rng.randrange(USER_POOL)}",
            "merchant_id": f"merchant_{rng.randrange(MERCHANT_POOL)}",
            # log-normal(mu=3.5, sigma=1.2) → median ~$33, heavy right tail.
            # Round to 2 decimals so printed output is readable.
            "amount": round(rng.lognormvariate(3.5, 1.2), 2),
            "status": rng.choices(_STATUS_CHOICES, weights=_STATUS_WEIGHTS, k=1)[0],
            "country": rng.choice(COUNTRIES),
            "ts": rng.randint(lo_ms, now_ms),
        }

    # Sort by ts so replay is monotonic in event time (required for
    # window-operator correctness; out-of-order events would land in
    # already-evicted buckets).
    events.sort(key=lambda e: e["ts"])
    return events


# ---------------------------------------------------------------------------
# Tiny CLI shim (Phase 26-03)
# ---------------------------------------------------------------------------
# The Phase 26-03 full-stack smoke calls
#   python3 benchmark/replay/generator.py --register-only --target localhost:6400
# as a "declare the pipelines on a running server" step before the replay.
# Delegates to replay_30d.py so there is exactly one canonical definition of
# the pipeline DAG; this file stays focused on deterministic event generation.


def _cli(argv=None) -> int:
    import argparse
    import os as _os
    import sys as _sys

    p = argparse.ArgumentParser(
        description="Deterministic fraud-event generator. "
                    "Invoked with --register-only, registers the replay pipelines "
                    "on a running Beava server and exits."
    )
    p.add_argument("--register-only", action="store_true", default=False,
                   help="Register pipelines on the Beava server and exit.")
    p.add_argument("--target", default=None,
                   help="host:port of the Beava server (alias for --host/--port).")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=6400)
    p.add_argument("--preview", type=int, default=0,
                   help="If >0, print that many generated events as JSONL and exit "
                        "(no server contact).")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--days", type=int, default=30)
    args = p.parse_args(argv)

    if args.preview > 0:
        import json as _json
        events = generate(args.preview, seed=args.seed, days=args.days)
        for ev in events:
            print(_json.dumps(ev, sort_keys=True))
        return 0

    if not args.register_only:
        p.print_help(_sys.stderr)
        return 2

    # Delegate to the replay CLI's --register-only path so the pipeline DAG
    # stays defined in exactly one place.
    _here = _os.path.dirname(_os.path.abspath(__file__))
    _root = _os.path.abspath(_os.path.join(_here, "..", ".."))
    for _p in (_root, _os.path.join(_root, "python")):
        if _p not in _sys.path:
            _sys.path.insert(0, _p)
    from benchmark.replay.replay_30d import main as _replay_main  # noqa: E402

    forward = ["--register-only"]
    if args.target:
        forward += ["--target", args.target]
    else:
        forward += ["--host", args.host, "--port", str(args.port)]
    return _replay_main(forward)


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(_cli())
