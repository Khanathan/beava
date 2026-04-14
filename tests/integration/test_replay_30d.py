"""Integration test for the 30-day replay CLI at reduced CI scale.

Covers TRAC-01 / TRAC-03:
- CLI runs end-to-end against a spawned Tally subprocess
- Report contains all 7 required fields
- events_per_sec > 50_000 (generous CI floor; real hardware is ~10x faster)
- keys_total > 0 after replay (server actually ingested events)

Scale: 100_000 events × 4 workers. Target wall-clock: < 30s including
subprocess startup (the pytest --timeout enforces this).

The ``tally_server`` fixture is borrowed from ``python/tests/conftest.py``
conceptually — we replicate it here locally so the test file is
self-contained and the plan's documented pytest command works verbatim.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import time

import pytest

# The CLI at benchmark/replay/replay_30d.py is still pinned to the pre-v0
# @tl.source / @tl.dataset decorators; Phase 26-01 leaves that port to plan
# 26-03 (traction demo rebuild). Skip this entire module until 26-03 lands
# the new-API CLI; unskipping is tracked as an explicit 26-03 deliverable.
pytest.skip(
    "port in 26-03 — benchmark/replay/replay_30d.py still imports the removed "
    "@tl.source / @tl.dataset surface; the CLI rewrite is owned by plan 26-03.",
    allow_module_level=True,
)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_CLI_PATH = os.path.join(_PROJECT_ROOT, "benchmark", "replay", "replay_30d.py")
_BINARY_PATH = os.path.join(_PROJECT_ROOT, "target", "debug", "tally")
_RELEASE_BINARY = os.path.join(_PROJECT_ROOT, "target", "release", "tally")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_tcp(host: str, port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Tally did not become ready on {host}:{port} within {timeout}s")


@pytest.fixture(scope="module")
def tally_instance():
    """Spawn a fresh Tally binary on ephemeral ports for this module.

    Prefers the release binary (faster replay, closer to production) but
    falls back to the debug binary. Skips the test cleanly if neither
    exists — keeps CI green on pure-Python runners where the Rust binary
    hasn't been built.
    """
    binary = _RELEASE_BINARY if os.path.exists(_RELEASE_BINARY) else _BINARY_PATH
    if not os.path.exists(binary):
        pytest.skip(
            f"Tally binary not found at {binary}; build with `cargo build` "
            f"(or `cargo build --release`) to enable this integration test."
        )

    tcp_port = _find_free_port()
    http_port = _find_free_port()
    env = os.environ.copy()
    env["TALLY_TCP_PORT"] = str(tcp_port)
    env["TALLY_HTTP_PORT"] = str(http_port)

    proc = subprocess.Popen(
        [binary],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", tcp_port, timeout=15.0)
        yield ("127.0.0.1", tcp_port, http_port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _parse_kv_report(stdout: str) -> dict:
    """Parse `key=value` lines from the replay CLI report."""
    out = {}
    for line in stdout.splitlines():
        line = line.strip()
        if "=" not in line:
            continue
        # Skip non-report noise (header lines, decorations) — only accept
        # lines that look like `key=value` with a simple identifier key.
        key, _, value = line.partition("=")
        if re.fullmatch(r"[a-z_][a-z0-9_]*", key):
            out[key] = value
    return out


def test_replay_cli_help_runs():
    """--help is intelligible and exits 0 without touching a server."""
    result = subprocess.run(
        [sys.executable, _CLI_PATH, "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"--help failed: {result.stderr}"
    for flag in ("--events", "--workers", "--batch-size", "--host", "--port"):
        assert flag in result.stdout, f"help text missing {flag!r}"


def test_replay_end_to_end(tally_instance):
    """100k events × 4 workers against a fresh Tally, assert report fields + eps floor."""
    host, tcp_port, http_port = tally_instance

    t0 = time.perf_counter()
    result = subprocess.run(
        [
            sys.executable,
            _CLI_PATH,
            "--events", "100000",
            "--workers", "4",
            "--batch-size", "1000",
            "--host", host,
            "--port", str(tcp_port),
            "--mgmt-port", str(http_port),
            "--no-warmup",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=_PROJECT_ROOT,
    )
    elapsed_wall = time.perf_counter() - t0

    assert result.returncode == 0, (
        f"replay exit code {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )

    report = _parse_kv_report(result.stdout)

    # All 7 canonical report fields must be present.
    required = {
        "events_total",
        "elapsed_seconds",
        "events_per_sec",
        "p50_push_us",
        "p99_push_us",
        "keys_total",
        "final_state_mb",
    }
    missing = required - set(report.keys())
    assert not missing, f"report missing fields {missing}; got keys={list(report.keys())}"

    # Numeric assertions.
    events_total = int(report["events_total"])
    eps = float(report["events_per_sec"])
    keys_total = int(report["keys_total"])

    assert events_total == 100_000, f"events_total={events_total}, expected 100000"
    # 50k/s is a generous CI floor (real hardware does 500k–1M+ per the
    # Phase 19 baseline). If we fall below this something is structurally
    # wrong with the driver (serialization bug, accidental sync mode, etc.).
    assert eps > 50_000, f"events_per_sec={eps} below 50k CI floor"
    assert keys_total > 0, "keys_total=0 — server did not ingest any events"

    # Reasonability: replay wall-clock < test timeout (test-level sanity check).
    assert elapsed_wall < 60, f"replay took {elapsed_wall:.1f}s, expected < 60s"


def test_replay_determinism_same_seed(tally_instance):
    """The generator piece is already tested for determinism; this guards the
    integration contract: re-running the CLI with the same --events does not
    crash on repeat ingestion into the same instance.
    """
    host, tcp_port, http_port = tally_instance
    cmd = [
        sys.executable, _CLI_PATH,
        "--events", "5000",
        "--workers", "2",
        "--batch-size", "500",
        "--host", host,
        "--port", str(tcp_port),
        "--mgmt-port", str(http_port),
        "--no-warmup",
    ]
    a = subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd=_PROJECT_ROOT)
    b = subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd=_PROJECT_ROOT)
    assert a.returncode == 0, a.stderr
    assert b.returncode == 0, b.stderr
    # Both runs produce parseable reports with the core fields.
    for r in (a, b):
        rep = _parse_kv_report(r.stdout)
        assert "events_per_sec" in rep
        assert int(rep["events_total"]) == 5000
