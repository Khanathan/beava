"""``python/benches/read_bench_tcp.py`` — Plan 12-09 TCP+msgpack read bench.

Drives single-key OP_GET frames over TCP with CT_MSGPACK against a freshly
spawned beava server warmed up with N synthetic events. Reports r/sec and
p50/p95/p99 latency per the read fast-path that the Python SDK now uses
by default on tcp:// transports.

Key differences vs ``read_bench.py`` (HTTP+JSON):
  - Spawns the server, registers the same pipeline, warms up via HTTP /push
    (writes are JSON; the read fast-path is the test target, not writes).
  - Reads via ``TcpTransport._tcp_get_single`` (OP_GET + CT_MSGPACK) on
    (private helper; renamed in Phase 13.5.1 D-04 — public-API surface
    for reads is ``send_get`` only)
    multiple long-lived sockets driven from a thread pool — strict-FIFO per
    socket, parallelism via separate sockets.
  - Reports cells/sec where each cell = one (feature, key) pair, matching
    the read_bench.py output shape so throughput-baselines.md can compare
    apples-to-apples.

Plan 12-09 / D-A: TCP+msgpack is the production fast path; this driver is
the canonical perf measurement harness for that surface.
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

# Make ``import beava`` work either as a script or as a module.
_BENCH_DIR = Path(__file__).resolve().parent
_PYTHON_DIR = _BENCH_DIR.parent
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

import httpx  # noqa: E402

import beava as bv  # noqa: E402
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
from benches.blast_shape import PoolConfig, build_pool  # noqa: E402

_REPO_ROOT = _PYTHON_DIR.parent
_BEAVA_BIN = _REPO_ROOT / "target" / "release" / "beava"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python read_bench_tcp.py",
        description=(
            "Plan 12-09: TCP+msgpack read-bench harness. Spawns beava, "
            "warms up state, drives OP_GET (CT_MSGPACK) reads via "
            "TcpTransport from a thread pool. Reports r/sec + cells/sec + "
            "p50/p95/p99 latency."
        ),
    )
    p.add_argument(
        "--pipeline",
        default="fraud-team",
        help="Pipeline config (resolved to crates/beava-bench/configs/<name>.json) or full path.",
    )
    p.add_argument(
        "--total-reads",
        type=int,
        default=50_000,
        help="Total OP_GET reads to issue (default 50_000).",
    )
    p.add_argument(
        "--warmup-events",
        type=int,
        default=100_000,
        help="Number of /push events to send before reads start (default 100_000).",
    )
    p.add_argument(
        "--parallel",
        type=int,
        default=32,
        help="Worker threads / TCP connections (default 32).",
    )
    p.add_argument(
        "--zipf-alpha",
        type=float,
        default=1.0,
        help="Zipfian alpha for warmup keys (default 1.0).",
    )
    p.add_argument(
        "--cardinality",
        type=int,
        default=10_000,
        help="Distinct keys (default 10_000).",
    )
    p.add_argument(
        "--feature",
        type=str,
        default=None,
        help="Feature name to read (default: first feature in pipeline config).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=0xCAFEBABE,
        help="RNG seed for reproducible warmup pools.",
    )
    return p.parse_args(argv)


def _spawn_server() -> tuple[subprocess.Popen[bytes], str, str, Path]:
    """Spawn beava with OS-assigned ports, parse stdout for bind addrs.

    Returns (proc, http_url, tcp_url, scratch_dir). Caller must clean up.
    """
    if not _BEAVA_BIN.is_file():
        raise SystemExit(f"ERROR: beava binary not found at {_BEAVA_BIN}")
    scratch = Path(tempfile.mkdtemp(prefix="beava-readbench-tcp-"))
    wal_dir = scratch / "wal"
    snap_dir = scratch / "snap"
    wal_dir.mkdir()
    snap_dir.mkdir()
    import os

    env = {
        **os.environ,
        "BEAVA_LISTEN_ADDR": "127.0.0.1:0",
        "BEAVA_TCP_PORT": "0",
        "BEAVA_ADMIN_ADDR": "127.0.0.1:0",
        "BEAVA_WAL_DIR": str(wal_dir),
        "BEAVA_SNAPSHOT_DIR": str(snap_dir),
        # Keep INFO level so server.http_bound + server.tcp_bound log lines
        # are emitted (we parse them from stdout below).
        "BEAVA_LOG_LEVEL": "info",
    }
    proc = subprocess.Popen(
        [str(_BEAVA_BIN), "--config", "/dev/null"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
    )

    http_addr: list[str] = []
    tcp_addr: list[str] = []
    ready = threading.Event()

    def _reader() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip()
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = rec.get("kind", "")
            if kind == "server.http_bound":
                http_addr.append(rec["addr"])
            elif kind == "server.tcp_bound":
                tcp_addr.append(rec["addr"])
            if http_addr and tcp_addr:
                ready.set()

    threading.Thread(target=_reader, daemon=True).start()
    if not ready.wait(timeout=15.0):
        proc.kill()
        proc.wait()
        shutil.rmtree(scratch, ignore_errors=True)
        raise SystemExit(
            f"beava did not emit bind log lines within 15s "
            f"(http_addr={http_addr}, tcp_addr={tcp_addr})"
        )
    http_url = f"http://{http_addr[0]}"
    tcp_url = f"tcp://{tcp_addr[0]}"
    # Also wait for /health = 200.
    deadline = time.time() + 15.0
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = httpx.get(f"{http_url}/health", timeout=0.5)
            if r.status_code == 200:
                return proc, http_url, tcp_url, scratch
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.1)
    proc.kill()
    proc.wait()
    shutil.rmtree(scratch, ignore_errors=True)
    raise SystemExit(f"/health did not respond within 15s (last_err={last_err!r})")


def _kill_server(proc: subprocess.Popen[bytes], scratch: Path) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    shutil.rmtree(scratch, ignore_errors=True)


def _warmup(http_url: str, cfg: dict[str, Any], n_events: int, seed: int,
            zipf_alpha: float, cardinality: int) -> set[str]:
    """Push N events via HTTP /push. Returns the set of warm keys."""
    extras = cfg_extra_fields(cfg)
    pipeline_event_name = str(cfg["event_name"])
    pipeline_key_field = cfg_key_field(cfg)
    pool_cfg = PoolConfig(
        shape="zipfian",
        wire_format="json",
        transport="http",
        cardinality=cardinality,
        zipf_alpha=zipf_alpha,
        mixed_event_count=1,
        seed=seed,
        pipeline_event_name=pipeline_event_name,
        pipeline_key_field=pipeline_key_field,
        pipeline_extra_fields=extras,
        mixed_event_names=[pipeline_event_name],
    )
    pool = build_pool(pool_cfg, n_events)
    pushed_keys: set[str] = set()
    with httpx.Client(base_url=http_url, timeout=30.0) as c:
        for item in pool:
            body = item.body
            try:
                key_val = body.get(pipeline_key_field)
                if key_val is not None:
                    pushed_keys.add(str(key_val))
            except AttributeError:
                pass
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            r = c.post(
                f"/push/{item.event_name}",
                content=payload,
                headers={"Content-Type": "application/json"},
            )
            if r.status_code != 200:
                raise SystemExit(
                    f"warmup push failed: status={r.status_code} body={r.text!r}"
                )
    return pushed_keys


def _worker(
    tcp_url: str,
    feature: str,
    keys: list[str],
    n_reads: int,
    seed: int,
    out_latencies_us: list[float],
    out_ok: list[int],
    out_err: list[int],
    lat_lock: threading.Lock,
) -> None:
    """One worker thread: open a TcpTransport, fire N reads, append latencies."""
    import random as _random
    rng = _random.Random(seed)
    if not keys:
        return
    with bv.App(tcp_url) as app:
        # Drive directly via the transport to avoid App.get's per-call dispatch
        # (the dispatch is `hasattr(transport, ...)` which is cheap, but we
        # measure the wire path here).
        transport = app._require_transport()  # type: ignore[attr-defined]
        local_lat: list[float] = []
        local_ok = 0
        local_err = 0
        for _ in range(n_reads):
            k = rng.choice(keys)
            t0 = time.perf_counter()
            try:
                _ = transport._tcp_get_single(feature, k)
                local_lat.append((time.perf_counter() - t0) * 1_000_000)
                local_ok += 1
            except Exception:  # noqa: BLE001
                local_err += 1
        with lat_lock:
            out_latencies_us.extend(local_lat)
            out_ok.append(local_ok)
            out_err.append(local_err)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = load_pipeline_config(args.pipeline)
    feature = args.feature or (cfg.get("features", ["cnt"])[0] if cfg.get("features") else "cnt")

    print(f"  pipeline={args.pipeline} feature={feature}", file=sys.stderr)
    print("  spawning beava (release build)...", file=sys.stderr)
    proc, http_url, tcp_url, scratch = _spawn_server()
    rc = 0
    try:
        with httpx.Client(base_url=http_url, timeout=30.0) as c:
            r = c.post(
                "/register",
                content=register_payload(cfg),
                headers={"Content-Type": "application/json"},
            )
            if r.status_code != 200:
                raise SystemExit(f"register failed: status={r.status_code} body={r.text!r}")
        print("  /register OK", file=sys.stderr)
        print(f"  warming up: {args.warmup_events} events...", file=sys.stderr)
        warmup_t0 = time.perf_counter()
        keys = sorted(_warmup(
            http_url=http_url,
            cfg=cfg,
            n_events=args.warmup_events,
            seed=args.seed,
            zipf_alpha=args.zipf_alpha,
            cardinality=args.cardinality,
        ))
        warmup_secs = time.perf_counter() - warmup_t0
        print(
            f"  warmup: {args.warmup_events} events in {warmup_secs:.1f}s "
            f"({len(keys)} keys); driving {args.total_reads} reads at "
            f"parallel={args.parallel}...",
            file=sys.stderr,
        )

        per_worker = args.total_reads // args.parallel
        latencies_us: list[float] = []
        ok_counts: list[int] = []
        err_counts: list[int] = []
        lat_lock = threading.Lock()
        threads: list[threading.Thread] = []
        wall_t0 = time.perf_counter()
        for i in range(args.parallel):
            t = threading.Thread(
                target=_worker,
                args=(
                    tcp_url,
                    feature,
                    keys,
                    per_worker,
                    args.seed ^ (0x1234 * i + 1),
                    latencies_us,
                    ok_counts,
                    err_counts,
                    lat_lock,
                ),
                daemon=True,
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        wall_elapsed = time.perf_counter() - wall_t0

        ok = sum(ok_counts)
        err = sum(err_counts)
        wall_clock_ms = int(round(wall_elapsed * 1000))
        reads_per_sec = ok / max(wall_elapsed, 1e-9)
        # Each read = 1 cell (1 feature × 1 key); kept the same naming as
        # read_bench.py for compat.
        cells_per_sec = reads_per_sec
        if latencies_us:
            sorted_lat = sorted(latencies_us)
            n = len(sorted_lat)
            p50 = sorted_lat[int(n * 0.50)]
            p95 = sorted_lat[min(int(n * 0.95), n - 1)]
            p99 = sorted_lat[min(int(n * 0.99), n - 1)]
        else:
            p50 = p95 = p99 = 0.0
        print(
            f"beava-readbench-tcp: requests={ok} errors={err} "
            f"wall_clock_ms={wall_clock_ms} requests_per_sec={int(reads_per_sec)} "
            f"cells_per_sec={int(cells_per_sec)}",
            file=sys.stderr,
        )
        print(
            f"beava-readbench-tcp: latency_p50_us={int(p50)} p95_us={int(p95)} p99_us={int(p99)}",
            file=sys.stderr,
        )
        if err > 0:
            print(f"  WARNING: {err} errors", file=sys.stderr)
        # Print to stdout for grep-friendly capture.
        print(json.dumps({
            "phase": "12-09",
            "harness": "read_bench_tcp",
            "pipeline": args.pipeline,
            "feature": feature,
            "parallel": args.parallel,
            "total_reads": args.total_reads,
            "ok": ok,
            "err": err,
            "wall_ms": wall_clock_ms,
            "reads_per_sec": int(reads_per_sec),
            "p50_us": int(p50),
            "p95_us": int(p95),
            "p99_us": int(p99),
        }))
        # Sanity: a finite non-zero p99 + ok>0 is the smoke contract.
        if ok == 0:
            rc = 2
        # Suppress unused statistics import if mypy strict.
        _ = statistics.median
    finally:
        _kill_server(proc, scratch)
    return rc


if __name__ == "__main__":
    sys.exit(main())
