"""Phase 44-01: E2E test — ``tl.fork(extract_at=[...])`` + ``fork.extract_history()``.

Flow:
  1. Boot a prod ``tally`` server on a free port.
  2. Push 5 events to ``Transactions`` at spaced-out wall-clock timestamps:
        u1 +~1s (amount=10), u2 +~2s (amount=5),
        u1 +~5s (amount=20), u2 +~8s (amount=15),
        u1 +~10s (amount=30)
     (Server assigns SystemTime::now() per PUSH; we sleep between them to
     guarantee monotonic ordering and enough spread for extract_at to fall
     cleanly between pushes.)
  3. Fork with ``extract_at=[T0+3s, T0+7s, T0+12s]``, scientist-authored
     ``TxnSummary = group_by(user_id).agg(count, total)``.
  4. Assert ``fork.extract_history()`` captures the expected rolling counts
     and totals at each checkpoint.

Runs 3x consecutively inside a parametrized test to exercise any flakiness.
"""
from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
import types as _types
from datetime import datetime, timezone
from pathlib import Path

import pytest

import tally as tl

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RELEASE_BIN = PROJECT_ROOT / "target" / "release" / "tally"
DEBUG_BIN = PROJECT_ROOT / "target" / "debug" / "tally"
ADMIN_TOKEN = "test-admin"


def _pick_binary() -> Path | None:
    candidates = [p for p in (RELEASE_BIN, DEBUG_BIN) if p.exists()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_tcp(host: str, port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"tally did not become ready on {host}:{port}")


def _iso_z(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    s = dt.replace(tzinfo=None).isoformat()
    if "." in s:
        head, frac = s.split(".", 1)
        s = f"{head}.{frac[:3]}"
    return s + "Z"


@pytest.fixture
def prod_server():
    binary = _pick_binary()
    if binary is None:
        pytest.skip("tally binary not built; run `cargo build` to enable")

    prod_tcp = _find_free_port()
    prod_http = _find_free_port()
    tmp = tempfile.TemporaryDirectory()
    env = os.environ.copy()
    env.update(
        TALLY_TCP_PORT=str(prod_tcp),
        TALLY_HTTP_PORT=str(prod_http),
        TALLY_ADMIN_TOKEN=ADMIN_TOKEN,
        TALLY_SNAPSHOT_PATH=str(Path(tmp.name) / "tally.snapshot"),
        TALLY_SNAPSHOT="1",
        TALLY_EVENT_LOG="1",
        TALLY_DATA_DIR=tmp.name,
    )
    proc = subprocess.Popen(
        [str(binary)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", prod_tcp, timeout=15.0)
        yield (prod_tcp, prod_http, str(binary))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        tmp.cleanup()


@pytest.mark.timeout(180)
@pytest.mark.parametrize("run_idx", [0, 1, 2])
def test_extract_history_three_checkpoints(prod_server, run_idx):
    """Run 3x to catch timing-related flakiness in the extract_at semantics."""
    prod_tcp, _prod_http, binary_path = prod_server

    # ---- Scientist pipelines ---------------------------------------------
    @tl.stream
    class Transactions:
        user_id: str
        amount: float

    def _summary(t: Transactions) -> tl.Table:
        return t.group_by("user_id").agg(
            count=tl.count(window="1h"),
            total=tl.sum("amount", window="1h"),
        )
    _summary.__name__ = "txn_summary"
    TxnSummary = tl.table(key="user_id")(_summary)

    # Inject key_field for prod registration (same trick as test_fork_python_api).
    _orig_to_register = Transactions._to_register_json
    _orig_collect = Transactions._collect_registrations
    def _keyed_register_json(self):
        reg = _orig_to_register()
        reg["key_field"] = "user_id"
        return reg
    def _keyed_collect(self):
        regs = _orig_collect()
        for r in regs:
            if r.get("name") == "Transactions":
                r["key_field"] = "user_id"
        return regs
    Transactions._to_register_json = _types.MethodType(_keyed_register_json, Transactions)
    Transactions._collect_registrations = _types.MethodType(_keyed_collect, Transactions)

    # ---- Seed prod with events at known wall-clock offsets --------------
    # The server stamps each PUSH with SystemTime::now(). We sleep between
    # pushes to space them out cleanly; the extract_at thresholds sit in
    # the gaps. With ~800ms gaps, the 100ms-scale wall-clock skew between
    # Python time.time() and the server's SystemTime::now() at ingest is
    # well within bounds.
    prod = tl.App(f"127.0.0.1:{prod_tcp}")
    push_times_ms: list[int] = []

    try:
        prod.register(Transactions, TxnSummary)
        # Wait a hair so the register lands before we stamp t0.
        time.sleep(0.2)

        t0_ms = int(time.time() * 1000)

        # Bracket each push with (before_ms, after_ms) wall-clock samples.
        # The server's SystemTime::now() at ingest lies inside that bracket,
        # so a threshold chosen from `after[i]` is guaranteed > server_ts[i]
        # and a threshold chosen from `before[i]` is guaranteed < server_ts[i].
        # We use wide 1.5s gaps so the "before[N+1] vs after[N]" window is
        # well-separated even under test-suite-induced jitter.
        push_before_ms: list[int] = []
        push_after_ms: list[int] = []

        def _push(payload):
            push_before_ms.append(int(time.time() * 1000))
            prod.push_sync(Transactions, payload)
            push_after_ms.append(int(time.time() * 1000))

        _push({"user_id": "u1", "amount": 10.0})  # P1
        time.sleep(1.5)
        _push({"user_id": "u2", "amount": 5.0})   # P2
        time.sleep(1.5)
        _push({"user_id": "u1", "amount": 20.0})  # P3
        time.sleep(1.5)
        _push({"user_id": "u2", "amount": 15.0})  # P4
        time.sleep(1.5)
        _push({"user_id": "u1", "amount": 30.0})  # P5
        push_times_ms.extend(push_after_ms)

        prod.flush()
        # Let the fsync timer land the event log.
        time.sleep(1.2)
    finally:
        prod.close()

    # ---- Choose extract_at thresholds between pushes --------------------
    # Semantics: cursor snapshots BEFORE applying event E if E.ts > threshold.
    # Pushes 1..5 landed at push_times_ms[0..4]. We want:
    #   checkpoint_a: after push 1 & push 2 (u1=1/10, u2=1/5) -> before push 3
    #   checkpoint_b: after push 1,2,3,4 (u1=2/30, u2=2/20)   -> before push 5
    #     WAIT — spec says at T0+7s: u1 count=2 total=30, u2 count=1 total=5.
    # Re-read the spec carefully:
    #   At T0+3s: u1 count=1 total=10, u2 count=1 total=5
    #   At T0+7s: u1 count=2 total=30, u2 count=1 total=5  <- only u1 has 2nd
    #   At T0+12s: u1 count=3 total=60, u2 count=2 total=20
    # Plan intended u1 pushes at +1s/+5s/+10s and u2 at +2s/+8s. We have
    # to emulate that ordering. Re-order pushes:
    #   P1: u1 amount=10        (at t0+~0.2s)
    #   P2: u2 amount=5         (at t0+~1.0s)
    #   P3: u1 amount=20        (at t0+~1.8s)
    #   P4: u2 amount=15        (at t0+~2.6s)
    #   P5: u1 amount=30        (at t0+~3.4s)
    # That matches: after P1+P2 (checkpoint_a), u1=1/10, u2=1/5. Good.
    # After P1+P2+P3 (checkpoint_b), u1=2/30, u2=1/5. Good.
    # After all 5 (checkpoint_c), u1=3/60, u2=2/20. Good.
    #
    # So thresholds:
    #   checkpoint_a ∈ (push_times_ms[1], push_times_ms[2])
    #   checkpoint_b ∈ (push_times_ms[2], push_times_ms[3])
    #   checkpoint_c > push_times_ms[4]  (trailing — snapshot at end-of-log)
    # Use the bracket: threshold must sit in (after[i], before[i+1]) so
    # it's strictly greater than server_ts[i] and strictly less than
    # server_ts[i+1]. Pick the midpoint of that bracket.
    cp_a = (push_after_ms[1] + push_before_ms[2]) // 2
    cp_b = (push_after_ms[2] + push_before_ms[3]) // 2
    cp_c = push_after_ms[4] + 500  # after the last push (trailing)

    # ---- Fork with extract_at -------------------------------------------
    fork_http = _find_free_port()
    for _ in range(10):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", fork_http + 1))
            break
        except OSError:
            fork_http = _find_free_port()

    with tl.fork(
        remote=f"127.0.0.1:{prod_tcp}",
        streams=[Transactions],
        keys=["u1", "u2"],
        since=t0_ms - 1000,  # include everything we pushed
        token=ADMIN_TOKEN,
        pipelines=[TxnSummary],
        extract_at=[cp_a, cp_b, cp_c],
        local_port=fork_http,
        binary_path=binary_path,
        ready_timeout=30.0,
    ) as fork:
        # Catchup is complete once the `with` block enters (block_until_catchup
        # default=true). extract_history() should now have all 3 snapshots.
        history = fork.extract_history()

        # Expected ISO-8601 timestamps (server formats seconds only).
        iso_a = _iso_z((cp_a // 1000) * 1000)
        iso_b = _iso_z((cp_b // 1000) * 1000)
        iso_c = _iso_z((cp_c // 1000) * 1000)

        assert iso_a in history, (
            f"missing checkpoint {iso_a}; got keys {list(history.keys())}"
        )
        assert iso_b in history, (
            f"missing checkpoint {iso_b}; got keys {list(history.keys())}"
        )
        assert iso_c in history, (
            f"missing checkpoint {iso_c}; got keys {list(history.keys())}"
        )

        # --- checkpoint a: after P1+P2 ---
        snap_a = history[iso_a]
        assert "u1" in snap_a, snap_a
        assert "u2" in snap_a, snap_a
        assert int(snap_a["u1"]["count"]) == 1, snap_a["u1"]
        assert abs(float(snap_a["u1"]["total"]) - 10.0) < 1e-6, snap_a["u1"]
        assert int(snap_a["u2"]["count"]) == 1, snap_a["u2"]
        assert abs(float(snap_a["u2"]["total"]) - 5.0) < 1e-6, snap_a["u2"]

        # --- checkpoint b: after P1+P2+P3 ---
        snap_b = history[iso_b]
        assert int(snap_b["u1"]["count"]) == 2, snap_b["u1"]
        assert abs(float(snap_b["u1"]["total"]) - 30.0) < 1e-6, snap_b["u1"]
        assert int(snap_b["u2"]["count"]) == 1, snap_b["u2"]
        assert abs(float(snap_b["u2"]["total"]) - 5.0) < 1e-6, snap_b["u2"]

        # --- checkpoint c: end of replay ---
        snap_c = history[iso_c]
        assert int(snap_c["u1"]["count"]) == 3, snap_c["u1"]
        assert abs(float(snap_c["u1"]["total"]) - 60.0) < 1e-6, snap_c["u1"]
        assert int(snap_c["u2"]["count"]) == 2, snap_c["u2"]
        assert abs(float(snap_c["u2"]["total"]) - 20.0) < 1e-6, snap_c["u2"]
