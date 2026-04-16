#!/usr/bin/env python3
"""Register the UserFeatures pipeline and push sample events into Beava.

This script:
  1. Reads ``pipeline.json`` (sibling to this file) and registers it over
     the TCP ``OP_REGISTER`` opcode via the Python SDK. Uses the same wire
     format as the HTTP ``POST /pipelines`` endpoint, but goes through
     TCP so it works identically with `docker compose up` (where the
     Docker bridge peer IP would otherwise fail the HTTP admin gate).
  2. Reads ``sample_events.jsonl`` (200 hand-generated transactions with
     a Zipfian user distribution — u123 is the hot one with ~60 events).
  3. Pushes every event to the ``UserFeatures`` stream, then flushes.

Usage:
    python3 examples/fraud/push_events.py [--url localhost:6400]
"""

import argparse
import json
import pathlib
import sys
import time

import beava as bv
from beava._protocol import OP_REGISTER, encode_register


# The event schema on the wire. Field names match pipeline.json; the
# stream name (``UserFeatures``) must match too because the server keys
# its internal routing off the registered name, not the Python class.
@bv.stream
class UserFeatures:
    user_id: str
    merchant_id: str
    amount: float
    country: str
    status: str


def register_from_json(app: bv.App, pipeline_path: pathlib.Path) -> None:
    """Send pipeline.json as an OP_REGISTER frame.

    The HTTP ``/pipelines`` endpoint and the TCP ``OP_REGISTER`` opcode
    accept the identical v2.0 flat JSON shape (``{name, key_field,
    features: [...]}``), so we just ship the file bytes straight through.
    """
    with pipeline_path.open() as f:
        definition = json.load(f)
    payload = encode_register(definition)
    # Going through the private ``_send`` keeps this 3 lines long. Public
    # ``App.register()`` takes Python descriptors, not raw JSON, so we
    # side-step it deliberately here.
    app._send(OP_REGISTER, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="localhost:6400",
                        help="Beava TCP endpoint (default: localhost:6400)")
    parser.add_argument("--events", default=None,
                        help="Path to JSONL file (default: sample_events.jsonl "
                             "next to this script)")
    parser.add_argument("--pipeline", default=None,
                        help="Path to pipeline.json (default: pipeline.json "
                             "next to this script)")
    args = parser.parse_args()

    here = pathlib.Path(__file__).parent
    events_path = pathlib.Path(args.events) if args.events else here / "sample_events.jsonl"
    pipeline_path = pathlib.Path(args.pipeline) if args.pipeline else here / "pipeline.json"
    for p in (events_path, pipeline_path):
        if not p.exists():
            print(f"error: {p} not found", file=sys.stderr)
            return 1

    with events_path.open() as f:
        events = [json.loads(line) for line in f if line.strip()]

    app = bv.App(args.url)

    # 1. Register the pipeline (idempotent on the server side).
    register_from_json(app, pipeline_path)

    # 2. Push all events.
    start = time.monotonic()
    for e in events:
        app.push(UserFeatures, e)
    app.flush()
    elapsed = time.monotonic() - start

    print(f"pushed {len(events):>4} events in {elapsed*1000:>4.0f} ms "
          f"({len(events)/elapsed:,.0f} eps)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
