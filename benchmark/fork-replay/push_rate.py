"""Rate-limited event pusher for the fork-replay bench.

Pushes events at a target rate (events/sec) against an upstream Beava
cluster for a configurable duration. Uses a sleep-based rate limiter —
simple and accurate within ±5% at rates <= ~50K eps.

Output: one JSON line at exit with `events_pushed`, `wall_seconds`,
`achieved_eps`. Stderr carries progress.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time

from pipeline_def import Event, UserCounts

import beava as bv


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="Upstream as HOST:PORT")
    ap.add_argument("--rate", type=float, required=True, help="Target events/sec")
    ap.add_argument("--duration", type=float, required=True, help="Seconds to push")
    ap.add_argument(
        "--entities",
        type=int,
        default=1000,
        help="Number of distinct user_ids to sample from (default 1000)",
    )
    ap.add_argument("--batch", type=int, default=100, help="Push batch size (default 100)")
    ap.add_argument(
        "--register",
        action="store_true",
        help="Register the pipeline before pushing (once per bench run)",
    )
    args = ap.parse_args()

    app = bv.App(args.host)
    if args.register:
        app.register(Event, UserCounts)
        print("pipeline registered", file=sys.stderr)

    # Rate-limited push loop. Batch sleep-calibrated: target the next
    # batch-deadline wall-clock and sleep the remainder. That's accurate
    # to a ms even at low rates where time.sleep() resolution dominates.
    seconds_per_batch = args.batch / args.rate
    deadline = time.monotonic() + seconds_per_batch
    start = time.monotonic()
    stop_at = start + args.duration
    total = 0
    user_ids = [f"u{i}" for i in range(args.entities)]
    last_progress = start

    while True:
        now = time.monotonic()
        if now >= stop_at:
            break
        # Build + push a batch. Events are plain dicts — @bv.stream turns
        # the class into a StreamSource descriptor, not a dataclass, so we
        # pass the class as the first arg to push_many and raw dicts as
        # the payloads (same pattern as benchmark/fraud-pipeline/bench.py).
        events = [
            {
                "user_id": user_ids[random.randrange(args.entities)],
                "amount": round(random.uniform(1, 500), 2),
            }
            for _ in range(args.batch)
        ]
        app.push_many(Event, events)
        total += args.batch
        # Sleep to next batch deadline. If we're already behind, skip
        # the sleep (limiter ramps up to catch up but can't sustain a
        # rate above what the network allows).
        now = time.monotonic()
        sleep_s = deadline - now
        if sleep_s > 0:
            time.sleep(sleep_s)
        deadline += seconds_per_batch
        if now - last_progress >= 5.0:
            achieved = total / (now - start) if now > start else 0
            print(
                f"  t={now - start:5.1f}s events={total:>10,} eps={achieved:>7,.0f}",
                file=sys.stderr,
            )
            last_progress = now

    wall = time.monotonic() - start
    summary = {
        "events_pushed": total,
        "wall_seconds": round(wall, 3),
        "achieved_eps": round(total / wall if wall > 0 else 0, 1),
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
