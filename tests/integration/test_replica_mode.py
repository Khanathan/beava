"""Phase 36-01: end-to-end replica-mode server boot test.

Spawns:
  * a "prod" beava binary on ephemeral ports with a Transactions stream
    registered and a handful of seed events.
  * a "replica" beava binary on separate ports, launched with the
    full `--replica-*` flag set and a `--replica-pipeline-file` that
    defines a `count_1h` aggregate per user_id.

Verifies:
  1. Replica boots into catchup (LOG_FETCH → END), then opens listeners.
  2. Querying the replica after catchup returns the count aggregate per
     key (10 events for u1, 10 for u2).
  3. Pushing 5 more events to prod propagates to the replica (SUBSCRIBE
     tail) and the aggregate updates.
  4. Attempting a local PUSH directly against the replica returns a
     STATUS_ERROR with the "replica mode: local PUSH disabled" message.

Skipped cleanly if the `beava` binary hasn't been built.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RELEASE_BIN = _PROJECT_ROOT / "target" / "release" / "beava"
_DEBUG_BIN = _PROJECT_ROOT / "target" / "debug" / "beava"

PROD_ADMIN_TOKEN = "prod-admin-token"
OP_PUSH = 0x01
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
    raise RuntimeError(f"beava did not become ready on {host}:{port}")


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
    raise RuntimeError(f"beava HTTP not ready on :{http_port}")


def _register_stream_http(http_port: int, token: str, name: str) -> None:
    body = json.dumps(
        {
            "name": name,
            "key_field": "user_id",
            "features": [{"name": "count_1h", "type": "count", "window": "1h"}],
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:{http_port}/pipelines",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 201), f"register {name}: {resp.status}"


def _write_u16_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b


def _build_push_frame(stream_name: str, user_id: str) -> bytes:
    body = bytearray()
    body += _write_u16_string(stream_name)
    body += struct.pack(">H", 1)  # field_count = 1
    body += _write_u16_string("user_id")
    body.append(TYPE_STR)
    body += _write_u16_string(user_id)
    total_len = 1 + len(body)
    return struct.pack(">I", total_len) + bytes([OP_PUSH]) + bytes(body)


def _push_event(host: str, port: int, stream_name: str, user_id: str) -> tuple[int, bytes]:
    """Push one event and return (status, payload)."""
    with socket.create_connection((host, port), timeout=5.0) as sock:
        sock.sendall(_build_push_frame(stream_name, user_id))
        header = b""
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))
            if not chunk:
                raise RuntimeError("prod closed before ack")
            header += chunk
        total_len = struct.unpack(">I", header)[0]
        body = b""
        while len(body) < total_len:
            chunk = sock.recv(total_len - len(body))
            if not chunk:
                raise RuntimeError("prod truncated ack")
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


def _build_register_pipeline_json(stream_name: str, feature_name: str) -> dict:
    """Build a REGISTER JSON payload matching the HTTP POST /pipelines
    shape the server's `convert_register_request` accepts."""
    return {
        "name": stream_name,
        "key_field": "user_id",
        "features": [
            {"name": feature_name, "type": "count", "window": "1h"},
        ],
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def prod_and_replica():
    """Spawn a prod beava + a replica beava, yielding their endpoints."""
    binary = _pick_binary()
    if binary is None:
        pytest.skip("beava binary not built; run `cargo build` to enable this test")

    tmp_prod = tempfile.TemporaryDirectory()
    tmp_replica = tempfile.TemporaryDirectory()
    pipeline_file = Path(tmp_replica.name) / "pipeline.json"
    pipeline_file.write_text(
        json.dumps(_build_register_pipeline_json("Transactions", "count_1h"))
    )

    prod_tcp = _find_free_port()
    prod_http = _find_free_port()
    replica_tcp = _find_free_port()
    replica_http = _find_free_port()

    prod_env = os.environ.copy()
    prod_env.update(
        BEAVA_TCP_PORT=str(prod_tcp),
        BEAVA_HTTP_PORT=str(prod_http),
        BEAVA_ADMIN_TOKEN=PROD_ADMIN_TOKEN,
        BEAVA_SNAPSHOT_PATH=str(Path(tmp_prod.name) / "beava.snapshot"),
        BEAVA_SNAPSHOT="1",
        BEAVA_EVENT_LOG="1",
        BEAVA_DATA_DIR=tmp_prod.name,
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
        _register_stream_http(prod_http, PROD_ADMIN_TOKEN, "Transactions")

        # Seed 20 events on prod (10 per user).
        for i in range(10):
            for user in ("u1", "u2"):
                st, _ = _push_event("127.0.0.1", prod_tcp, "Transactions", user)
                assert st == STATUS_OK, f"prod PUSH {user} #{i} failed"
        # Give the background fsync timer one tick so LOG_FETCH will see
        # every write. (The server also fsyncs on LOG_FETCH but that's
        # belt-and-braces.)
        time.sleep(1.2)

        # Spin up replica pointing at prod.
        replica_env = os.environ.copy()
        replica_env.update(
            BEAVA_TCP_PORT=str(replica_tcp),
            BEAVA_HTTP_PORT=str(replica_http),
            BEAVA_ADMIN_TOKEN=PROD_ADMIN_TOKEN,
            BEAVA_SNAPSHOT_PATH=str(Path(tmp_replica.name) / "beava.snapshot"),
            BEAVA_SNAPSHOT="0",
            BEAVA_EVENT_LOG="1",
            BEAVA_DATA_DIR=tmp_replica.name,
        )
        replica_args = [
            str(binary),
            "--replica-from",
            f"127.0.0.1:{prod_tcp}",
            "--replica-since",
            "0",
            "--replica-streams",
            "Transactions",
            "--replica-keys",
            "u1,u2",
            "--replica-token",
            PROD_ADMIN_TOKEN,
            "--replica-pipeline-file",
            str(pipeline_file),
        ]
        replica = subprocess.Popen(
            replica_args,
            env=replica_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            try:
                _wait_for_tcp("127.0.0.1", replica_tcp, timeout=30.0)
                _wait_for_http(replica_http, timeout=30.0)
            except RuntimeError as e:
                # Dump what we have of replica stderr to aid debug.
                try:
                    err = replica.stderr.read() if replica.stderr else b""
                except Exception:
                    err = b""
                raise RuntimeError(
                    f"{e}\n--- replica stderr ---\n{err.decode(errors='replace')}"
                )
            yield {
                "prod_tcp": prod_tcp,
                "prod_http": prod_http,
                "replica_tcp": replica_tcp,
                "replica_http": replica_http,
                "replica_proc": replica,
                "prod_proc": prod,
            }
        finally:
            replica.terminate()
            try:
                replica.wait(timeout=5)
            except subprocess.TimeoutExpired:
                replica.kill()
                replica.wait(timeout=5)
    finally:
        prod.terminate()
        try:
            prod.wait(timeout=5)
        except subprocess.TimeoutExpired:
            prod.kill()
            prod.wait(timeout=5)
        tmp_prod.cleanup()
        tmp_replica.cleanup()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
def test_replica_catches_up_and_tails(prod_and_replica):
    """Full happy path: historical catchup then live tail + rejection."""
    replica_http = prod_and_replica["replica_http"]
    replica_tcp = prod_and_replica["replica_tcp"]
    prod_tcp = prod_and_replica["prod_tcp"]

    # (1) Catchup: after replica's listeners opened, the 20 seed events
    # should have flowed through. Query count_1h for u1 and u2.
    # The count value may take a brief moment to reflect after LOG_FETCH
    # finishes even though listeners are open; poll briefly.
    deadline = time.monotonic() + 15.0
    u1_count: int | None = None
    u2_count: int | None = None
    while time.monotonic() < deadline:
        u1 = _debug_key(replica_http, PROD_ADMIN_TOKEN, "u1")
        u2 = _debug_key(replica_http, PROD_ADMIN_TOKEN, "u2")
        if u1 is not None and u2 is not None:
            feats_u1 = u1.get("computed_features", {})
            feats_u2 = u2.get("computed_features", {})
            c1 = feats_u1.get("count_1h")
            c2 = feats_u2.get("count_1h")
            if isinstance(c1, (int, float)) and isinstance(c2, (int, float)):
                u1_count = int(c1)
                u2_count = int(c2)
                if u1_count == 10 and u2_count == 10:
                    break
        time.sleep(0.2)
    if u1_count != 10 or u2_count != 10:
        # Dump what we have for diagnosis.
        u1 = _debug_key(replica_http, PROD_ADMIN_TOKEN, "u1")
        u2 = _debug_key(replica_http, PROD_ADMIN_TOKEN, "u2")
        raise AssertionError(
            f"catchup failed: u1={u1_count} u2={u2_count}\n"
            f"u1 debug={u1}\nu2 debug={u2}"
        )

    # (2) Live tail: push 5 more events to prod (all u1), wait, re-query.
    for _ in range(5):
        st, _ = _push_event("127.0.0.1", prod_tcp, "Transactions", "u1")
        assert st == STATUS_OK
    deadline = time.monotonic() + 15.0
    live_count: int | None = None
    while time.monotonic() < deadline:
        u1 = _debug_key(replica_http, PROD_ADMIN_TOKEN, "u1")
        if u1 is not None:
            c = u1.get("computed_features", {}).get("count_1h")
            if isinstance(c, (int, float)):
                live_count = int(c)
                if live_count == 15:
                    break
        time.sleep(0.2)
    assert live_count == 15, f"expected u1 count_1h=15 after live tail, got {live_count}"

    # (3) Reject local PUSH on the replica.
    st, payload = _push_event("127.0.0.1", replica_tcp, "Transactions", "u1")
    assert st == STATUS_ERROR, f"expected STATUS_ERROR from replica PUSH, got {st}"
    msg = payload.decode("utf-8", errors="replace")
    assert "replica mode" in msg, f"expected 'replica mode' in error, got {msg!r}"
