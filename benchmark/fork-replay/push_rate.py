"""Event pusher for the fork-replay bench.

Pushes events against an upstream Beava cluster. Two modes:

- Rate-limited: `--rate R --duration D` pushes at a target EPS for D
  seconds via a batch-deadline sleep scheduler. Accurate within ±5%
  at target rates <= ~50 K eps.
- Unthrottled: `--rate 0 [--target-events N]` pushes as fast as the
  single Python client can manage. Stops after `--target-events N`
  (if set) or `--duration D` seconds, whichever comes first.

Unthrottled mode lands ~40-50 K eps on one client via push_many;
fine for seeding a fork-replay bench at 5 M events in ~2 minutes.

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
    ap.add_argument(
        "--rate",
        type=float,
        required=True,
        help="Target events/sec. Set 0 or negative for unthrottled.",
    )
    ap.add_argument(
        "--duration",
        type=float,
        required=True,
        help="Max seconds to push (upper bound; --target-events can end earlier).",
    )
    ap.add_argument(
        "--target-events",
        type=int,
        default=0,
        help="Stop after N events pushed (0 = no cap; only --duration terminates).",
    )
    ap.add_argument(
        "--entities",
        type=int,
        default=1000,
        help="Number of distinct user_ids to sample from (default 1000)",
    )
    ap.add_argument("--batch", type=int, default=1000, help="Push batch size (default 1000)")
    ap.add_argument(
        "--register",
        action="store_true",
        help="Register the pipeline before pushing (once per bench run)",
    )
    args = ap.parse_args()
    unthrottled = args.rate <= 0
    target_events = args.target_events if args.target_events > 0 else None

    app = bv.App(args.host)
    if args.register:
        app.register(Event, UserCounts)
        print("pipeline registered", file=sys.stderr)

    # Push loop — rate-limited when args.rate > 0, else unthrottled.
    # The batch-deadline sleep scheduler targets the next wall-clock
    # batch boundary and sleeps the residual. Accurate to a ms at
    # target rates below ~50 K eps. Unthrottled mode skips the sleep
    # entirely and lets a single Python client sustain ~40-50 K eps.
    seconds_per_batch = args.batch / args.rate if not unthrottled else 0.0
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
        if target_events is not None and total >= target_events:
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
        # Sleep to next batch deadline (rate-limited mode only). If we
        # are behind the deadline, skip the sleep and let the limiter
        # ramp up — it cannot sustain a rate above what the single
        # Python client can push.
        now = time.monotonic()
        if not unthrottled:
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
