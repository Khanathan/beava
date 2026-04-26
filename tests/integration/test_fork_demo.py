"""Phase 37-01: load-bearing E2E demo test for `tally fork`.

The canonical scientist workflow:

    1. Prod tally is running somewhere with the `Transactions` stream.
    2. Scientist authors a pipeline (count + sum per user), writes it to
       a REGISTER JSON file, launches `tally fork --remote ... --streams
       Transactions --keys u1,u2 --pipeline-file /tmp/p.json --token ...`.
    3. Waits for /debug/ready, then queries the replica over HTTP for
       per-user aggregates.
    4. Pushes more events to prod — the fork tails them live and the
       aggregate updates.
    5. Local PUSH against the fork is rejected (replica-mode invariant).

If this test passes, the Option M fork workflow is demo-ready.

Skipped cleanly if the `tally` binary hasn't been built.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RELEASE_BIN = _PROJECT_ROOT / "target" / "release" / "tally"
_DEBUG_BIN = _PROJECT_ROOT / "target" / "debug" / "tally"

PROD_ADMIN_TOKEN = "prod-admin-token"
OP_PUSH = 0x01
TYPE_F64 = 0x03
TYPE_STR = 0x04
STATUS_OK = 0x00
STATUS_ERROR = 0x01


# ---------------------------------------------------------------------------
# Harness helpers
# ---------------------------------------------------------------------------


def _pick_binary() -> Path | None:
    candidates = [p for p in (_RELEASE_BIN, _DEBUG_BIN) if p.exists()]
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


def _wait_for_http(http_port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{http_port}/health", timeout=0.5
            ) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"tally HTTP not ready on :{http_port}")


def _wait_for_ready(http_port: int, timeout: float = 30.0) -> None:
    """Phase 37-01: poll /debug/ready until 200. Because the fork's HTTP
    listener only binds after catchup, this becomes reachable exactly when
    the replica is query-ready."""
    deadline = time.monotonic() + timeout
    last_err: str = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{http_port}/debug/ready", timeout=0.5
            ) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode("utf-8"))
                    assert body.get("ready") is True, body
                    return
        except Exception as e:
            last_err = repr(e)
            time.sleep(0.1)
    raise RuntimeError(
        f"tally fork /debug/ready did not return 200 on :{http_port}: {last_err}"
    )


def _register_stream_http(http_port: int, token: str, body: dict) -> None:
    raw = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/pipelines",
        data=raw,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 201), f"register {body['name']}: {resp.status}"


def _write_u16_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b


def _build_push_frame(stream_name: str, user_id: str, amount: float) -> bytes:
    """PUSH frame with user_id:str + amount:f64 (2 fields)."""
    body = bytearray()
    body += _write_u16_string(stream_name)
    body += struct.pack(">H", 2)  # field_count = 2
    body += _write_u16_string("user_id")
    body.append(TYPE_STR)
    body += _write_u16_string(user_id)
    body += _write_u16_string("amount")
    body.append(TYPE_F64)
    body += struct.pack(">d", amount)
    total_len = 1 + len(body)
    return struct.pack(">I", total_len) + bytes([OP_PUSH]) + bytes(body)


def _push_event(
    host: str, port: int, stream_name: str, user_id: str, amount: float
) -> tuple[int, bytes]:
    """Push one event and return (status, payload)."""
    with socket.create_connection((host, port), timeout=5.0) as sock:
        sock.sendall(_build_push_frame(stream_name, user_id, amount))
        header = b""
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))
            if not chunk:
                raise RuntimeError("peer closed before ack")
            header += chunk
        total_len = struct.unpack(">I", header)[0]
        body = b""
        while len(body) < total_len:
            chunk = sock.recv(total_len - len(body))
            if not chunk:
                raise RuntimeError("peer truncated ack")
            body += chunk
        status = body[0]
        return status, body[1:]


def _debug_key(http_port: int, token: str, key: str) -> dict | None:
    req = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/debug/key/{key}",
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 404:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def _scientist_pipeline(stream: str) -> dict:
    """REGISTER JSON matching `convert_register_request` in the server.

    Simple enough to be deterministic: count + sum per user_id over 1h.
    Data scientist's intent: 'give me per-user transaction count and total'.
    """
    return {
        "name": stream,
        "key_field": "user_id",
        "features": [
            {"name": "count_1h", "type": "count", "window": "1h"},
            {
                "name": "sum_amount_1h",
                "type": "sum",
                "field": "amount",
                "window": "1h",
            },
        ],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def prod_and_fork():
    """Spawn prod tally + `tally fork` subprocess, yielding endpoints."""
    binary = _pick_binary()
    if binary is None:
        pytest.skip("tally binary not built; run `cargo build` to enable this test")

    tmp_prod = tempfile.TemporaryDirectory()
    tmp_fork = tempfile.TemporaryDirectory()
    pipeline_file = Path(tmp_fork.name) / "scientist_pipeline.json"
    pipeline_file.write_text(json.dumps(_scientist_pipeline("Transactions")))

    prod_tcp = _find_free_port()
    prod_http = _find_free_port()
    # `tally fork --local-port P` → HTTP on P, TCP on P+1. Pick an even
    # port we know isn't used and check P+1 is also free.
    fork_http = _find_free_port()
    fork_tcp = fork_http + 1
    # Best-effort check that fork_tcp is free too; if not, pick another.
    for _ in range(10):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", fork_tcp))
                break
        except OSError:
            fork_http = _find_free_port()
            fork_tcp = fork_http + 1

    prod_env = os.environ.copy()
    prod_env.update(
        TALLY_TCP_PORT=str(prod_tcp),
        TALLY_HTTP_PORT=str(prod_http),
        TALLY_ADMIN_TOKEN=PROD_ADMIN_TOKEN,
        TALLY_SNAPSHOT_PATH=str(Path(tmp_prod.name) / "tally.snapshot"),
        TALLY_SNAPSHOT="1",
        TALLY_EVENT_LOG="1",
        TALLY_DATA_DIR=tmp_prod.name,
    )
    prod = subprocess.Popen(
        [str(binary)],
        env=prod_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_tcp("127.0.0.1", prod_tcp)
        _wait_for_http(prod_http)
        # Prod also needs the Transactions stream registered (so OP_PUSH
        # against prod doesn't fail with "unknown stream") — the fork
        # registers its own copy via --pipeline-file.
        _register_stream_http(
            prod_http, PROD_ADMIN_TOKEN, _scientist_pipeline("Transactions")
        )

        # Seed 5 events: u1 x 3 (amounts 10, 20, 30), u2 x 2 (amounts 5, 15).
        for user, amount in [
            ("u1", 10.0),
            ("u1", 20.0),
            ("u1", 30.0),
            ("u2", 5.0),
            ("u2", 15.0),
        ]:
            st, _ = _push_event("127.0.0.1", prod_tcp, "Transactions", user, amount)
            assert st == STATUS_OK, f"prod PUSH {user}@{amount} failed"
        # Let the background fsync timer flush the event log.
        time.sleep(1.2)

        # Spawn `tally fork` — scientist-facing path.
        fork_env = os.environ.copy()
        fork_env.update(
            # These TALLY_* vars are defaults; `tally fork` overrides the
            # ports from --local-port anyway. Keep the rest for snapshot dir
            # isolation.
            TALLY_SNAPSHOT_PATH=str(Path(tmp_fork.name) / "tally.snapshot"),
            TALLY_SNAPSHOT="0",
            TALLY_EVENT_LOG="1",
            TALLY_DATA_DIR=tmp_fork.name,
            # Required by the fork's admin routes (debug/key).
            TALLY_ADMIN_TOKEN=PROD_ADMIN_TOKEN,
        )
        fork_args = [
            str(binary),
            "fork",
            "--remote",
            f"127.0.0.1:{prod_tcp}",
            "--streams",
            "Transactions",
            "--keys",
            "u1,u2",
            "--token",
            PROD_ADMIN_TOKEN,
            "--local-port",
            str(fork_http),
            "--pipeline-file",
            str(pipeline_file),
        ]
        fork = subprocess.Popen(
            fork_args,
            env=fork_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            try:
                _wait_for_ready(fork_http, timeout=30.0)
            except RuntimeError as e:
                try:
                    err = fork.stderr.read() if fork.stderr else b""
                except Exception:
                    err = b""
                raise RuntimeError(
                    f"{e}\n--- fork stderr ---\n{err.decode(errors='replace')}"
                )
            yield {
                "prod_tcp": prod_tcp,
                "prod_http": prod_http,
                "fork_http": fork_http,
                "fork_tcp": fork_tcp,
                "fork_proc": fork,
                "prod_proc": prod,
            }
        finally:
            fork.terminate()
            try:
                fork.wait(timeout=5)
            except subprocess.TimeoutExpired:
                fork.kill()
                fork.wait(timeout=5)
    finally:
        prod.terminate()
        try:
            prod.wait(timeout=5)
        except subprocess.TimeoutExpired:
            prod.kill()
            prod.wait(timeout=5)
        tmp_prod.cleanup()
        tmp_fork.cleanup()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
def test_scientist_fork_workflow(prod_and_fork):
    """The canonical Option M demo: scientist forks a prod stream, registers
    a count+sum pipeline, queries per-user aggregates, pushes more events to
    prod, sees live-updated aggregates, and confirms the fork rejects writes.
    """
    fork_http = prod_and_fork["fork_http"]
    fork_tcp = prod_and_fork["fork_tcp"]
    prod_tcp = prod_and_fork["prod_tcp"]

    # (1) Historical catchup: 5 seed events (u1 x3, u2 x2) should produce:
    #     u1: count=3, sum=60   u2: count=2, sum=20
    deadline = time.monotonic() + 15.0
    got_u1: dict | None = None
    got_u2: dict | None = None
    while time.monotonic() < deadline:
        u1 = _debug_key(fork_http, PROD_ADMIN_TOKEN, "u1")
        u2 = _debug_key(fork_http, PROD_ADMIN_TOKEN, "u2")
        if u1 and u2:
            f1 = u1.get("computed_features", {})
            f2 = u2.get("computed_features", {})
            if (
                isinstance(f1.get("count_1h"), (int, float))
                and isinstance(f1.get("sum_amount_1h"), (int, float))
                and isinstance(f2.get("count_1h"), (int, float))
                and isinstance(f2.get("sum_amount_1h"), (int, float))
            ):
                got_u1 = f1
                got_u2 = f2
                if (
                    int(f1["count_1h"]) == 3
                    and abs(float(f1["sum_amount_1h"]) - 60.0) < 1e-6
                    and int(f2["count_1h"]) == 2
                    and abs(float(f2["sum_amount_1h"]) - 20.0) < 1e-6
                ):
                    break
        time.sleep(0.2)
    assert got_u1 is not None and got_u2 is not None, "catchup never produced features"
    assert int(got_u1["count_1h"]) == 3, f"u1 count: {got_u1}"
    assert abs(float(got_u1["sum_amount_1h"]) - 60.0) < 1e-6, f"u1 sum: {got_u1}"
    assert int(got_u2["count_1h"]) == 2, f"u2 count: {got_u2}"
    assert abs(float(got_u2["sum_amount_1h"]) - 20.0) < 1e-6, f"u2 sum: {got_u2}"

    # (2) Live tail: push 2 more events to prod. u1 (amount=100) is in scope,
    # u3 (amount=50) is OUT of scope — replica was started with --keys u1,u2.
    st, _ = _push_event("127.0.0.1", prod_tcp, "Transactions", "u1", 100.0)
    assert st == STATUS_OK
    st, _ = _push_event("127.0.0.1", prod_tcp, "Transactions", "u3", 50.0)
    assert st == STATUS_OK

    # Wait for the SUBSCRIBE tail to deliver the in-scope u1 event.
    # After: u1 count=4, sum=160.
    deadline = time.monotonic() + 15.0
    live: dict | None = None
    while time.monotonic() < deadline:
        u1 = _debug_key(fork_http, PROD_ADMIN_TOKEN, "u1")
        if u1:
            f = u1.get("computed_features", {})
            c = f.get("count_1h")
            if isinstance(c, (int, float)) and int(c) == 4:
                live = f
                break
        time.sleep(0.2)
    assert live is not None, "u1 never reached count=4 after live push"
    assert int(live["count_1h"]) == 4, live
    assert abs(float(live["sum_amount_1h"]) - 160.0) < 1e-6, live

    # (3) Out-of-scope key u3 — the fork was scoped to u1/u2 only, so the
    # upstream filter drops u3. Debug endpoint returns None (404).
    u3 = _debug_key(fork_http, PROD_ADMIN_TOKEN, "u3")
    # v0 behavior: out-of-scope keys simply don't land on the replica.
    # Either None (not present) or empty computed_features is acceptable.
    if u3 is not None:
        assert not u3.get("computed_features", {}).get("count_1h"), (
            f"u3 should be out-of-scope but got {u3}"
        )

    # (4) Local PUSH against the fork is rejected (replica-mode invariant).
    st, payload = _push_event(
        "127.0.0.1", fork_tcp, "Transactions", "u1", 999.0
    )
    assert st == STATUS_ERROR, f"expected STATUS_ERROR from fork PUSH, got {st}"
    msg = payload.decode("utf-8", errors="replace")
    assert "replica mode" in msg, f"expected 'replica mode' in error, got {msg!r}"
