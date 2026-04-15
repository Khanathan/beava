"""Phase 39-01: Python-native E2E test for ``tl.fork()``.

Mirrors ``tests/integration/test_fork_demo.py`` but entirely in Python —
the scientist authors pipelines as Python decorators, calls
``tl.fork(...)``, queries features, pushes more events to prod, and
asserts the live tail lands.

Skipped cleanly if the ``tally`` binary hasn't been built.
"""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

import tally as tl

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RELEASE_BIN = PROJECT_ROOT / "target" / "release" / "tally"
DEBUG_BIN = PROJECT_ROOT / "target" / "debug" / "tally"
ADMIN_TOKEN = "prod-admin-token"


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


@pytest.fixture
def prod_server():
    """Start a standalone prod tally instance. Yields ``(tcp_port, http_port)``."""
    binary = _pick_binary()
    if binary is None:
        pytest.skip("tally binary not built; run `cargo build` to enable this test")

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


@pytest.mark.timeout(120)
def test_scientist_fork_workflow_pure_python(prod_server):
    """The canonical Option-M demo, authored entirely in Python.

    No ``tally fork`` shell command, no hand-written JSON — the scientist
    uses ``@tl.stream`` / ``@tl.table`` decorators and ``tl.fork(...)``.
    """
    prod_tcp, _prod_http, binary_path = prod_server

    # ---- Scientist authors pipelines in pure Python ----------------------
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

    # ---- Seed prod with the same stream + pipeline so events can land ----
    # Prod needs the Transactions stream to declare `key_field="user_id"` so
    # the server-side OP_LOG_FETCH handler emits events during catchup — the
    # v0 replica contract is "key-bearing events only" (src/server/tcp.rs::
    # handle_log_fetch skips keyless streams). The @tl.stream decorator
    # treats sources as semantically keyless, so we inject key_field into
    # the REGISTER JSON for the prod-side registration only. The fork
    # receives a matching keyed shape via its `pipelines=[TxnSummary]`
    # (the aggregation's key_field flows through _collect_registrations).
    import types as _types
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

    prod = tl.App(f"127.0.0.1:{prod_tcp}")
    try:
        prod.register(Transactions, TxnSummary)
        for user, amount in [
            ("u1", 10.0),
            ("u1", 20.0),
            ("u1", 30.0),
            ("u2", 5.0),
            ("u2", 15.0),
        ]:
            prod.push_sync(Transactions, {"user_id": user, "amount": amount})
        prod.flush()
        # Give the fsync timer a beat to land the event-log so catchup can
        # replay all 5 events from disk.
        time.sleep(1.2)
    finally:
        prod.close()

    # ---- tl.fork: scientist command ------------------------------------
    # `tally fork --local-port P` binds HTTP on P and TCP on P+1. Pick a
    # pair where both are free; retry a few times to dodge transient races.
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
        token=ADMIN_TOKEN,
        pipelines=[TxnSummary],
        local_port=fork_http,
        binary_path=binary_path,
        ready_timeout=30.0,
    ) as fork:
        # ---- Catchup: all 5 seed events should have flowed ---------------
        deadline = time.monotonic() + 15.0
        u1 = u2 = None
        while time.monotonic() < deadline:
            got1 = fork.get(TxnSummary, key="u1")
            got2 = fork.get(TxnSummary, key="u2")
            if (
                got1
                and got2
                and int(got1.get("count", 0)) == 3
                and int(got2.get("count", 0)) == 2
            ):
                u1, u2 = got1, got2
                break
            time.sleep(0.2)
        assert u1 is not None, "u1 never produced features after catchup"
        assert u2 is not None, "u2 never produced features after catchup"
        assert int(u1["count"]) == 3, u1
        assert abs(float(u1["total"]) - 60.0) < 1e-6, u1
        assert int(u2["count"]) == 2, u2
        assert abs(float(u2["total"]) - 20.0) < 1e-6, u2

        # ---- Live tail: push more events to prod, fork should follow ----
        prod2 = tl.App(f"127.0.0.1:{prod_tcp}")
        try:
            prod2.push_sync(Transactions, {"user_id": "u1", "amount": 100.0})
            # u3 is out of scope — fork was started with keys=[u1,u2].
            prod2.push_sync(Transactions, {"user_id": "u3", "amount": 50.0})
            prod2.flush()
        finally:
            prod2.close()

        deadline = time.monotonic() + 15.0
        live = None
        while time.monotonic() < deadline:
            got = fork.get(TxnSummary, key="u1")
            if got and int(got.get("count", 0)) == 4:
                live = got
                break
            time.sleep(0.2)
        assert live is not None, "u1 never reached count=4 after live push"
        assert int(live["count"]) == 4, live
        assert abs(float(live["total"]) - 160.0) < 1e-6, live

        # ---- Out-of-scope u3: fork filters it out ------------------------
        u3 = fork.get(TxnSummary, key="u3")
        # Either None or a zero/absent count is acceptable — the v0
        # contract is that out-of-scope keys never land on the replica.
        if u3 is not None:
            assert not u3.get("count"), f"u3 should be out-of-scope: {u3}"

        # ---- local_url exposes the raw port for power-users --------------
        assert fork.local_url == f"http://127.0.0.1:{fork_http}"

    # Context manager exited — subprocess and temp files cleaned up.
    # `fork` is stopped; a subsequent query should raise.
    with pytest.raises(tl.ForkError):
        fork.get(TxnSummary, key="u1")
