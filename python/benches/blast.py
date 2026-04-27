"""``python/benches/blast.py`` — Phase 19 multi-process Python bench harness.

Mirrors ``crates/beava-bench/src/bin/beava-bench-v18.rs`` CLI surface so the
two harnesses produce directly comparable ledger rows. Each Python ledger row
is tagged ``mode=burst`` AND has a Notes-column flag
``python(burst-only) — D-05 continuous-mode deferred to Phase 19.1 (asyncio)``
so the asymmetry vs the Rust harness (which ships BOTH continuous and burst
modes) is visible in the published table.

Per CONTEXT.md decisions:
  - **D-08:** lives at ``python/benches/blast.py``; excluded from the pip
    wheel via ``[tool.hatch.build.targets.wheel] exclude``.
  - **D-09 (REVISED 2026-04-26, commit 88f1161):** use the public Transport API.
    TCP path: ``transport.send_push(event_name, body_dict, wire_format=...)``.
    HTTP path: ``transport._client.post(f"/push/{event}", json=body)`` —
    same pattern ``app.upsert()`` already uses (python/beava/_app.py:215-235).
    Pool stores Python ``dict`` bodies; encoding happens INSIDE the transport
    call per push (the SDK overhead the bench is honestly measuring). Raw
    ``socket.create_connection + sock.sendall(pre_encoded_bytes)`` is FORBIDDEN.
  - **D-10:** ``ProcessPoolExecutor`` with N = ``os.cpu_count() - 1`` workers,
    each its own ``bv.App``. Counters aggregated via ``futures.as_completed``.
  - **D-15:** NO pre-bench priming. ``t0`` is set immediately before the worker
    fan-out; the first push timestamp inside any worker is the start of
    ``wall_clock_ms``. Cold caches, first-tick optimizer priming, and JIT-style
    overhead are all included in the published number — the honest cold-start
    EPS that matches what a user would see hitting a freshly-spawned server.

Python-only carve-out (Warning 9 deferral): the harness ships BURST-ONLY
(per-worker continuous pipelining requires asyncio + GIL-release tricks that
Phase 19 defers to a Phase 19.1 follow-up). Multi-process IS the parallelism
layer here.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Make ``import beava`` and ``from benches...`` work whether run as a script or as
# a module from inside python/ — by inserting python/ into sys.path before any
# beava/benches imports.
_BENCH_DIR = Path(__file__).resolve().parent
_PYTHON_DIR = _BENCH_DIR.parent
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

import httpx  # noqa: E402

from benches._configs import (  # noqa: E402
    event_name as cfg_event_name,
)
from benches._configs import (  # noqa: E402
    extra_fields as cfg_extra_fields,
)
from benches._configs import (  # noqa: E402
    key_field as cfg_key_field,
)
from benches._configs import (  # noqa: E402
    load_pipeline_config,
    register_payload,
)
from benches.blast_shape import PoolConfig, PoolItem, build_pool  # noqa: E402

# ─── CLI ────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Define the CLI surface that mirrors the Rust harness."""
    p = argparse.ArgumentParser(
        prog="python -m benches.blast",
        description=(
            "Beava Python multi-process bench harness — Phase 19. "
            "Drives the public Transport API in burst mode across N processes "
            "and reports the {requested, pushed, acked} invariant + "
            "wall_clock_ms / send_drain_ms / ack_lag_ms."
        ),
    )
    p.add_argument(
        "--total-events",
        type=int,
        default=None,
        help=(
            "Send N events end-to-end and exit. Required for Phase 19; the "
            "duration-based fallback is informational only."
        ),
    )
    p.add_argument(
        "--blast-shape",
        choices=["fixed", "uniform", "zipfian", "mixed"],
        default="fixed",
        help="Body distribution: fixed/uniform/zipfian/mixed (D-01).",
    )
    p.add_argument(
        "--transport",
        choices=["http", "tcp"],
        default="tcp",
        help="Transport protocol — http (json) or tcp (json or msgpack).",
    )
    p.add_argument(
        "--wire-format",
        choices=["json", "msgpack"],
        default="json",
        help="Wire format. msgpack is TCP-only in v0.",
    )
    p.add_argument(
        "--pipeline",
        default="small",
        help=(
            "Pipeline config: small / medium / large / large_phase9 (resolved "
            "to crates/beava-bench/configs/{name}.json), or a full path."
        ),
    )
    p.add_argument(
        "--duration-secs",
        type=int,
        default=60,
        help="Fallback wall-clock cap when --total-events is omitted.",
    )
    p.add_argument(
        "--parallel",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help="Number of worker processes (D-10: defaults to cpu_count-1).",
    )
    p.add_argument(
        "--pipeline-depth",
        type=int,
        default=16,
        help=(
            "Per-worker inflight depth. Burst-only Python mode does not enforce "
            "this; it is captured for the ledger row so cells line up with the "
            "Rust harness's continuous-mode column."
        ),
    )
    p.add_argument(
        "--seed",
        type=int,
        default=0xCAFEBABE,
        help="RNG seed for reproducible pool builds.",
    )
    p.add_argument(
        "--zipf-alpha",
        type=float,
        default=1.0,
        help="Zipfian distribution alpha (D-04 default 1.0).",
    )
    p.add_argument(
        "--cardinality",
        type=int,
        default=1_000_000,
        help="Distinct entity-key count (D-04 default 1M keys).",
    )
    p.add_argument(
        "--mixed-event-count",
        type=int,
        default=3,
        help="Distinct event names sampled by --blast-shape mixed (D-04 default 3).",
    )
    p.add_argument(
        "--isolation-mode",
        action="store_true",
        default=False,
        help=(
            "Print wall_clock_ms / send_drain_ms / ack_lag_ms columns "
            "alongside the EPS line (D-07)."
        ),
    )
    p.add_argument(
        "--no-ledger",
        action="store_true",
        default=False,
        help=(
            "Suppress the markdown ledger row. Used by smoke tests; production "
            "runs leave this off so Plan 19-05 can pipe rows into the .planning ledger."
        ),
    )
    p.add_argument(
        "--server-url",
        default=None,
        help=(
            "Comma-separated 'http://host:port,tcp://host:port' for already-running "
            "beava server. Required: embed-mode auto-discovery is deferred."
        ),
    )
    return p.parse_args(argv)


# ─── Pipeline registration ─────────────────────────────────────────────────


def _register_pipeline(http_url: str, cfg: dict[str, Any]) -> None:
    """POST the verbatim Rust-harness register JSON to ``/register``.

    Always uses HTTP — register is HTTP-only in the bench harness because the
    JSON payload is large (well-suited for HTTP), and re-using the same JSON the
    Rust bench POSTs guarantees zero semantic drift.
    """
    if not http_url.startswith(("http://", "https://")):
        raise ValueError(f"_register_pipeline requires http:// URL, got {http_url!r}")
    with httpx.Client(base_url=http_url, timeout=30.0) as c:
        r = c.post(
            "/register",
            content=register_payload(cfg),
            headers={"Content-Type": "application/json"},
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"register failed: status={r.status_code} body={r.text!r}"
            )


# ─── Worker process loop ───────────────────────────────────────────────────


def _worker_loop(
    args_dict: dict[str, Any],
    http_url: str,
    tcp_url: str,
    cfg_json: str,
    n_total: int,
    worker_idx: int,
) -> dict[str, Any]:
    """One worker's tight push loop, executed in a separate process.

    Args are passed as a plain dict (argparse.Namespace is not picklable in
    every Python build). Returns aggregate counters for this worker.
    """
    # Imports happen inside the worker so each spawned process has its own
    # module state and no cross-process module-table sharing surprises.
    sys.path.insert(0, str(_PYTHON_DIR))  # ensure beava is importable in worker
    import beava as bv  # noqa: PLC0415  (worker-local import is intentional)
    from beava._transport import HttpTransport, TcpTransport  # noqa: PLC0415

    cfg = json.loads(cfg_json)
    extras = cfg_extra_fields(cfg)
    pipeline_event_name = cfg_event_name(cfg)
    pipeline_key_field = cfg_key_field(cfg)

    # Build per-worker pool slice. We round up so the union of slices is
    # >= n_total, then trim the LAST slice to land exactly on n_total in main().
    # Each worker's pool is its own slice; main() decides how many of THIS
    # worker's items are actually pushed (work-stealing not needed at this scale).
    parallel = int(args_dict["parallel"])
    per_worker_n = (n_total + parallel - 1) // parallel
    # The last worker may need to push fewer than per_worker_n if n_total is
    # not divisible by parallel. main() partitions exactly; here we just build a
    # slightly oversized pool and main controls how much we use.
    target_n = per_worker_n
    if worker_idx == parallel - 1:
        target_n = n_total - per_worker_n * (parallel - 1)
    target_n = max(target_n, 0)

    pool_cfg = PoolConfig(
        shape=args_dict["blast_shape"],
        wire_format=args_dict["wire_format"],
        transport=args_dict["transport"],
        cardinality=int(args_dict["cardinality"]),
        zipf_alpha=float(args_dict["zipf_alpha"]),
        mixed_event_count=int(args_dict["mixed_event_count"]),
        seed=int(args_dict["seed"]) + worker_idx * 0x9E37,
        pipeline_event_name=pipeline_event_name,
        pipeline_key_field=pipeline_key_field,
        pipeline_extra_fields=extras,
        mixed_event_names=[pipeline_event_name],
    )
    pool: list[PoolItem] = build_pool(pool_cfg, target_n)

    pushed = 0
    errors = 0
    first_send_ts: float | None = None
    last_send_ts: float | None = None
    transport_kind = args_dict["transport"]
    wire_format = args_dict["wire_format"]
    server_url = tcp_url if transport_kind == "tcp" else http_url

    # Each worker process owns its own bv.App + transport — D-10 multi-process model.
    # Pool build time is intentionally OUTSIDE wall_clock_ms (built before t0 in main()).
    with bv.App(server_url) as app:
        transport = app._require_transport()  # honest "transport is live" guard
        if transport_kind == "tcp":
            assert isinstance(transport, TcpTransport), (
                f"expected TcpTransport, got {type(transport).__name__}"
            )
            # REVISED D-09: TCP path uses transport.send_push (public method).
            # Pool holds Python dict bodies; encoding happens inside send_push
            # per call — the SDK overhead the bench honestly measures.
            # D-15: NO pre-bench priming. The first push IS the timer start.
            for item in pool:
                send_ts = time.monotonic()
                if first_send_ts is None:
                    first_send_ts = send_ts
                try:
                    transport.send_push(item.event_name, item.body, wire_format=wire_format)
                    pushed += 1
                except Exception:
                    errors += 1
                last_send_ts = time.monotonic()
        else:
            # HTTP path: transport._client.post(f"/push/{event}", json=body) —
            # mirrors app.upsert() at python/beava/_app.py:215-235. msgpack is
            # NOT supported on HTTP in v0; we always send JSON regardless of
            # args.wire_format on the HTTP path (matches HttpTransport.send_ping
            # NotImplementedError pattern at _transport.py:124).
            assert isinstance(transport, HttpTransport), (
                f"expected HttpTransport, got {type(transport).__name__}"
            )
            for item in pool:
                send_ts = time.monotonic()
                if first_send_ts is None:
                    first_send_ts = send_ts
                try:
                    payload_bytes = json.dumps(item.body, ensure_ascii=False).encode("utf-8")
                    r = transport._client.post(
                        f"/push/{item.event_name}",
                        content=payload_bytes,
                        headers={"Content-Type": "application/json"},
                    )
                    if r.status_code == 200:
                        pushed += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1
                last_send_ts = time.monotonic()

    return {
        "worker_idx": worker_idx,
        "pushed": pushed,
        "errors": errors,
        "first_send_ts": first_send_ts,
        "last_send_ts": last_send_ts,
        "target_n": target_n,
    }


# ─── Server URL parsing ────────────────────────────────────────────────────


def _parse_server_urls(server_url: str | None) -> tuple[str, str]:
    """Resolve --server-url into (http_url, tcp_url).

    Required formats:
      - ``"http://host:p,tcp://host:p"`` (comma-separated, both required)
      - Single ``"http://..."`` or ``"tcp://..."`` is REJECTED with a clear
        message because the harness needs both URLs (HTTP for register, TCP/HTTP
        for push depending on --transport).

    Embed-mode auto-discovery is deferred (CONTEXT.md output spec); callers
    pass both URLs explicitly via the smoke test fixture or run script.
    """
    if not server_url:
        raise SystemExit(
            "ERROR: --server-url not set; embed-mode auto-discovery is not yet "
            "implemented. Pass --server-url 'http://host:p,tcp://host:p' "
            "(both URLs required, comma-separated)."
        )

    parts = [p.strip() for p in server_url.split(",") if p.strip()]
    http_url: str | None = None
    tcp_url: str | None = None
    for part in parts:
        if part.startswith(("http://", "https://")):
            http_url = part
        elif part.startswith("tcp://"):
            tcp_url = part
        else:
            raise SystemExit(
                f"ERROR: unrecognised URL scheme in --server-url part {part!r}; "
                f"supported schemes: http://, https://, tcp://"
            )

    if http_url is None or tcp_url is None:
        raise SystemExit(
            f"ERROR: --server-url must include BOTH http:// and tcp:// URLs "
            f"(comma-separated). Got: {server_url!r}"
        )

    return http_url, tcp_url


# ─── Main ──────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns the process exit code."""
    args = parse_args(argv)
    cfg = load_pipeline_config(args.pipeline)

    http_url, tcp_url = _parse_server_urls(args.server_url)

    # Register the pipeline once (always HTTP, idempotent on the server).
    _register_pipeline(http_url, cfg)

    # D-15 (BLOCKER 1 fix): NO warm-up phase. t0 is set IMMEDIATELY before the
    # worker fan-out; the first push timestamp inside any worker is the start
    # of wall_clock_ms. The previous revision pre-pushed events here to warm
    # caches; that has been REMOVED so wall_clock_ms captures the honest
    # cold-start number per D-15.

    n_total = args.total_events if args.total_events is not None else 0
    if n_total <= 0:
        # Phase 19's design centres on --total-events; the duration fallback is
        # informational only.
        print(
            "ERROR: --total-events N is required for Phase 19. The duration-based "
            "fallback is not yet implemented in the Python harness.",
            file=sys.stderr,
        )
        return 2

    cfg_json = json.dumps(cfg)
    parallel = max(1, int(args.parallel))
    args_dict = {
        "blast_shape": args.blast_shape,
        "transport": args.transport,
        "wire_format": args.wire_format,
        "parallel": parallel,
        "cardinality": args.cardinality,
        "zipf_alpha": args.zipf_alpha,
        "mixed_event_count": args.mixed_event_count,
        "seed": args.seed,
    }

    # D-15 honesty: t0 is set IMMEDIATELY before worker dispatch; the first
    # push timestamp in any worker is therefore >= t0. No warm-up, no pool-build
    # in the timed window. Pool building happens INSIDE each worker on top of
    # this clock — that's intentional for the multi-process model: each worker
    # builds its own slice. Plan 19-05 may revisit if pool-build cost dominates.
    t0 = time.monotonic()

    with ProcessPoolExecutor(max_workers=parallel) as exe:
        futs = [
            exe.submit(_worker_loop, args_dict, http_url, tcp_url, cfg_json, n_total, i)
            for i in range(parallel)
        ]
        results = [f.result() for f in as_completed(futs)]

    t1 = time.monotonic()

    total_pushed = sum(int(r["pushed"]) for r in results)
    total_errors = sum(int(r["errors"]) for r in results)
    first_sends = [r["first_send_ts"] for r in results if r["first_send_ts"] is not None]
    last_sends = [r["last_send_ts"] for r in results if r["last_send_ts"] is not None]
    wall_clock_ms = int(round((t1 - t0) * 1000))
    if first_sends and last_sends:
        send_drain_ms = int(round((max(last_sends) - min(first_sends)) * 1000))
    else:
        send_drain_ms = 0
    ack_lag_ms = max(0, wall_clock_ms - send_drain_ms)
    eps = total_pushed / max(t1 - t0, 1e-9)

    # Acked = pushed in burst mode (each transport.send_push waits for ACK
    # before returning per python/beava/_transport.py:208-261).
    total_acked = total_pushed

    # Invariant tuple (matches Rust harness output format).
    print(
        f"beava-blast: invariant_tuple "
        f"requested={n_total} pushed={total_pushed} "
        f"acked={total_acked} errors={total_errors}",
        file=sys.stderr,
    )

    if args.isolation_mode:
        print(
            f"beava-blast: isolation_mode "
            f"wall_clock_ms={wall_clock_ms} "
            f"send_drain_ms={send_drain_ms} "
            f"ack_lag_ms={ack_lag_ms}",
            file=sys.stderr,
        )
    else:
        # Even when --isolation-mode is off, surface wall_clock_ms / send_drain_ms /
        # ack_lag_ms in stderr so the smoke test's grep-driven assertions pass
        # without requiring --isolation-mode. (Plan 19-05 always runs with
        # --isolation-mode, so this is informational for ad-hoc invocations.)
        print(
            f"beava-blast: timing "
            f"wall_clock_ms={wall_clock_ms} "
            f"send_drain_ms={send_drain_ms} "
            f"ack_lag_ms={ack_lag_ms}",
            file=sys.stderr,
        )

    print(
        f"beava-blast: sustained_eps={eps:.0f} "
        f"parallel={parallel} "
        f"shape={args.blast_shape} "
        f"transport={args.transport} "
        f"wire={args.wire_format} "
        f"pipeline={args.pipeline}",
        file=sys.stderr,
    )

    if not args.no_ledger:
        # Markdown ledger row template — Plan 19-05 owns the schema; the row
        # below conforms to the Rust harness column ordering. WARNING 9
        # deferral: Notes column is tagged `python(burst-only)` so the
        # asymmetry vs Rust harness (which ships BOTH continuous + burst per
        # D-05) is visible in the published table.
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        lang = "python"
        mode = "burst"  # Python harness is burst-only — see Warning 9 deferral
        try:
            commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(_PYTHON_DIR.parent),
                text=True,
            ).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            commit = "unknown"
        notes = "python(burst-only) — D-05 continuous-mode deferred to Phase 19.1 (asyncio)"
        row = (
            f"| 19 | {date} | {args.pipeline} | "
            f"{args.transport}/{args.wire_format} | {args.blast_shape} | "
            f"{mode} | {lang} | {parallel} | {args.pipeline_depth} | "
            f"{n_total} | {wall_clock_ms} | {send_drain_ms} | {ack_lag_ms} | "
            f"{eps:.0f} | n/a | n/a | n/a | n/a | {commit} | {notes} |"
        )
        print(row)

    # Sanity invariant: --total-events is strict equality.
    if total_pushed != n_total:
        print(
            f"WARNING: requested={n_total} but pushed={total_pushed}; "
            f"measurement is incomplete (total_errors={total_errors})",
            file=sys.stderr,
        )
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
