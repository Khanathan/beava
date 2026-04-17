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
import random
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


def sample_count_features(app: bv.App, sample_keys: list[str]) -> dict[str, float]:
    """Read UserCounts.count_1h for each sample key. Returns {key: count}.

    Missing keys map to 0.0 so forks with partial replay surface as
    "missing" rather than "different".
    """
    out: dict[str, float] = {}
    for key in sample_keys:
        try:
            # App.get(key) returns a FeatureResult across all tables keyed
            # by that entity. UserCounts.count_1h is nested under the
            # qualified "UserCounts.count_1h" name, but the FeatureResult
            # flattens it to the short name when unambiguous.
            features = app.get(key)
            val = (
                getattr(features, "count_1h", None)
                if hasattr(features, "count_1h")
                else features.to_dict().get("UserCounts.count_1h", 0)
            )
            if val is None:
                val = features.to_dict().get("UserCounts.count_1h", 0)
            try:
                out[key] = float(val)
            except (TypeError, ValueError):
                out[key] = 0.0
        except Exception:
            out[key] = 0.0
    return out


def diff_counts(upstream: dict[str, float], fork: dict[str, float]) -> dict:
    """Compute upstream-vs-fork feature diff. Returns summary stats.

    Any key whose counts disagree is a correctness failure.
    """
    mismatch = 0
    total_upstream = 0.0
    total_fork = 0.0
    diffs: list[tuple[str, float, float]] = []
    for key in upstream:
        u = upstream[key]
        f = fork.get(key, 0.0)
        total_upstream += u
        total_fork += f
        if abs(u - f) > 1e-6:
            mismatch += 1
            if len(diffs) < 5:
                diffs.append((key, u, f))
    return {
        "sampled_keys": len(upstream),
        "mismatched_keys": mismatch,
        "upstream_count_sum": total_upstream,
        "fork_count_sum": total_fork,
        "count_sum_delta_pct": (
            round(100.0 * (total_fork - total_upstream) / max(1.0, total_upstream), 4)
        ),
        "first_mismatches": [
            {"key": k, "upstream": u, "fork": f} for (k, u, f) in diffs
        ],
    }


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
    ap.add_argument(
        "--sample-keys",
        type=int,
        default=20,
        help="After catchup, compare UserCounts.count_1h between upstream and fork for N random sample keys (0 disables).",
    )
    ap.add_argument(
        "--entity-count",
        type=int,
        default=1000,
        help="Must match push_rate.py's --entities. Sample keys are picked uniformly from u0..u{N-1}.",
    )
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

        # Feature-value diff: prove the fork didn't just match the entity
        # count but also the aggregate values. Picks sample keys uniformly
        # at random, reads UserCounts.count_1h from upstream and from the
        # fork, and reports per-key mismatches. Keeps the fork's App open
        # inside the with-block so the subprocess stays alive for reads.
        diff: dict = {}
        if args.sample_keys > 0:
            random.seed(42)  # stable, repeatable samples across runs
            sample_keys = [
                f"u{random.randrange(args.entity_count)}"
                for _ in range(args.sample_keys)
            ]
            fork_app = bv.App(f"127.0.0.1:{args.local_port + 1}", timeout=10.0)
            upstream_host, _, upstream_port = args.remote.rpartition(":")
            upstream_app = bv.App(args.remote, timeout=10.0)
            try:
                upstream_vals = sample_count_features(upstream_app, sample_keys)
                fork_vals = sample_count_features(fork_app, sample_keys)
                diff = diff_counts(upstream_vals, fork_vals)
            except Exception as e:
                diff = {"error": f"feature compare failed: {e}"}

        # Stream the fork subprocess stderr BEFORE the context exits
        # (bv.fork().__exit__ runs stop() which deletes the temp log).
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
        "feature_diff": diff,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
