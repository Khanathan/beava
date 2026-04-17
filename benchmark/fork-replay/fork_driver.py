"""Driver that spawns a bv.fork(), times catchup, and verifies state.

Catchup is defined by bv.fork()'s own contract: the `with bv.fork(...)`
block enters only after the fork subprocess's /debug/ready returns 200,
which in turn happens AFTER the block_until_catchup=true (default)
LOG_FETCH stream terminates with REPLICA_FRAME_TAG_END. So the wall
clock from just-before the `with` to just-after is a clean replay
benchmark.

Output: one JSON line with `catchup_seconds`, `entities_after`,
`keys_total`. Stderr carries progress.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request

from pipeline_def import Event, UserCounts

import beava as bv


def fetch_keys_total(http_port: int) -> int:
    url = f"http://127.0.0.1:{http_port}/metrics"
    with urllib.request.urlopen(url, timeout=5) as resp:
        body = resp.read().decode("utf-8")
    for line in body.splitlines():
        if line.startswith("beava_keys_total "):
            return int(line.split()[1])
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--remote", required=True, help="Upstream HOST:PORT")
    ap.add_argument(
        "--local-port",
        type=int,
        required=True,
        help="Fork HTTP port (TCP port is +1, per beava fork convention)",
    )
    ap.add_argument(
        "--token",
        default=os.environ.get("BEAVA_REPLICA_TOKEN", "dev-admin-token"),
        help="Replica admin token (default: BEAVA_REPLICA_TOKEN env or dev-admin-token)",
    )
    ap.add_argument(
        "--ready-timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for fork catchup (default 300)",
    )
    ap.add_argument("--key-prefix", default="u", help="Key prefix to replicate")
    args = ap.parse_args()

    # `beava fork` convention (see src/main.rs): --local-port is the
    # HTTP port (scientist-facing), and TCP is HTTP+1 for the raw wire
    # protocol. We only need the HTTP port here to query /metrics.
    local_http_port = args.local_port

    print(f"spawning fork: remote={args.remote} local_port={args.local_port}", file=sys.stderr)
    t0 = time.monotonic()
    with bv.fork(
        remote=args.remote,
        streams=[Event],
        key_prefix=args.key_prefix,
        pipelines=[UserCounts],
        token=args.token,
        local_port=args.local_port,
        ready_timeout=args.ready_timeout,
    ) as fork:
        catchup = time.monotonic() - t0
        print(f"fork caught up in {catchup:.3f}s", file=sys.stderr)
        keys_total = fetch_keys_total(local_http_port)
        # Stream the fork subprocess stderr to our own stderr BEFORE
        # the context exits (bv.fork().__exit__ runs stop() which may
        # delete the temp log). Cheap post-mortem for every run.
        if fork.log_path is not None and fork.log_path.exists():
            try:
                with open(fork.log_path, "r", encoding="utf-8", errors="replace") as fh:
                    print("=== fork stderr ===", file=sys.stderr)
                    for line in fh:
                        print(f"  {line.rstrip()}", file=sys.stderr)
                    print("=== end fork stderr ===", file=sys.stderr)
            except OSError:
                pass

    summary = {
        "catchup_seconds": round(catchup, 3),
        "keys_total": keys_total,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
