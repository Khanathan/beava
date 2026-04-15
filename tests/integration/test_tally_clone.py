"""Phase 28-04: end-to-end `tally_cli clone` against a real tally server.

Spins up the real `tally` binary on ephemeral ports, seeds a base snapshot
with three entities (u_a/orders, u_b/orders, u_c/orders), registers the
`orders` stream so scope validation passes, runs `tally_cli clone
--streams orders --keys u_a,u_b --dump-json` as a subprocess, and asserts:

  * exit 0
  * JSON dump contains `snapshot_taken_at`
  * in-scope entities (u_a, u_b) are present
  * out-of-scope entity (u_c) is absent — server's Scope filter stripped it
  * `tally sync` remains a stub (unchanged from 28-02).

Out-of-scope `.get()` rejection (`OutOfScopeError`) is covered by the Rust
unit test `client::tests::frozen_get_rejects_*` in src/client/mod.rs.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path

import pytest


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TALLY_BIN = _PROJECT_ROOT / "target" / "debug" / "tally"
_CLI_BIN = _PROJECT_ROOT / "target" / "debug" / "tally_cli"

ADMIN_TOKEN = "clone-test-admin"


# ---------------------------------------------------------------------------
# Snapshot seeding helpers (mirrors test_replica_snapshot_fetch_asyncio.py).
# ---------------------------------------------------------------------------


def _enc_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        if n < 0x80:
            out.append(n)
            return bytes(out)
        out.append((n & 0x7F) | 0x80)
        n >>= 7


def _enc_varint_string(s: str) -> bytes:
    b = s.encode("utf-8")
    return _enc_varint(len(b)) + b


def _write_base_snapshot_file(path: Path, entities: list[tuple[str, list[str]]]) -> None:
    """Write a v7 base snapshot with the given (entity_key, [stream_name]) pairs."""
    buf = bytearray()
    # SnapshotHeader: snapshot_type = Base (unit variant 0), sequence = 1.
    buf += _enc_varint(0)
    buf += _enc_varint(1)
    # entities Vec
    buf += _enc_varint(len(entities))
    for key, streams in entities:
        buf += _enc_varint_string(key)
        buf += _enc_varint(len(streams))
        for stream_name in streams:
            buf += _enc_varint_string(stream_name)
            buf += _enc_varint(0)  # operators: empty
            buf.append(0)  # Option::None for last_event_at
        buf += _enc_varint(0)  # static_features: empty
        buf += _enc_varint(0)  # table_rows: empty
    # pipelines + backfill_complete: empty
    buf += _enc_varint(0)
    buf += _enc_varint(0)
    path.write_bytes(bytes([0x07, 0x00]) + bytes(buf))


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
    raise RuntimeError(f"tally did not become ready on {host}:{port}")


def _register_stream_http(http_port: int, name: str) -> None:
    """Register a stream with one count feature so scope validation accepts it."""
    import urllib.request

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
            "Authorization": f"Bearer {ADMIN_TOKEN}",
        },
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 201), f"register {name}: {resp.status}"


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def tally_server_with_fixture():
    if not _TALLY_BIN.exists():
        pytest.skip(f"{_TALLY_BIN} not built; run `cargo build --bin tally`")
    if not _CLI_BIN.exists():
        pytest.skip(f"{_CLI_BIN} not built; run `cargo build --bin tally_cli`")

    tmp = tempfile.TemporaryDirectory()
    snap_path = Path(tmp.name) / "tally.snapshot.base.0000000001"
    _write_base_snapshot_file(
        snap_path,
        entities=[
            ("u_a", ["orders"]),
            ("u_b", ["orders"]),
            ("u_c", ["orders"]),
        ],
    )

    tcp_port = _find_free_port()
    http_port = _find_free_port()
    env = os.environ.copy()
    env["TALLY_TCP_PORT"] = str(tcp_port)
    env["TALLY_HTTP_PORT"] = str(http_port)
    env["TALLY_ADMIN_TOKEN"] = ADMIN_TOKEN
    env["TALLY_SNAPSHOT_PATH"] = str(Path(tmp.name) / "tally.snapshot")
    env["TALLY_SNAPSHOT"] = "1"

    proc = subprocess.Popen(
        [str(_TALLY_BIN)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", tcp_port, timeout=15.0)
        _register_stream_http(http_port, "orders")
        yield {"tcp_port": tcp_port, "http_port": http_port, "token": ADMIN_TOKEN}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        tmp.cleanup()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_clone_filters_by_scope_keys(tally_server_with_fixture):
    """End-to-end: seeded snapshot (u_a, u_b, u_c) → clone scoped to
    keys=[u_a,u_b] → JSON contains u_a and u_b but not u_c."""
    server = tally_server_with_fixture
    result = subprocess.run(
        [
            str(_CLI_BIN), "clone",
            "--remote", f"127.0.0.1:{server['tcp_port']}",
            "--streams", "orders",
            "--keys", "u_a,u_b",
            "--token", server["token"],
            "--dump-json",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"tally_cli clone exited {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    dump = json.loads(result.stdout)
    assert "snapshot_taken_at" in dump
    assert dump["scope"]["streams"] == ["orders"]
    assert dump["scope"]["keys"] == ["u_a", "u_b"]

    pairs = {(e["stream"], e["key"]) for e in dump["entities"]}
    assert ("orders", "u_a") in pairs, f"u_a missing from {pairs}"
    assert ("orders", "u_b") in pairs, f"u_b missing from {pairs}"
    assert ("orders", "u_c") not in pairs, (
        f"u_c should have been filtered out by server scope, got {pairs}"
    )


@pytest.mark.timeout(60)
def test_clone_without_dump_json_prints_summary(tally_server_with_fixture):
    """Without --dump-json, tally clone prints a one-line summary and exits 0."""
    server = tally_server_with_fixture
    result = subprocess.run(
        [
            str(_CLI_BIN), "clone",
            "--remote", f"127.0.0.1:{server['tcp_port']}",
            "--streams", "orders",
            "--keys", "u_a",
            "--token", server["token"],
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "snapshot_taken_at=" in result.stdout
    assert "entities=" in result.stdout


@pytest.mark.timeout(60)
def test_clone_bad_token_fails_loud(tally_server_with_fixture):
    """Bad admin token → non-zero exit after retry budget."""
    server = tally_server_with_fixture
    # max_attempts is hardcoded at 5 in run_clone; default retries would take
    # ~30s. We lower the test timeout generously but let the real retry policy
    # run because it's part of what 28-04 is shipping.
    result = subprocess.run(
        [
            str(_CLI_BIN), "clone",
            "--remote", f"127.0.0.1:{server['tcp_port']}",
            "--streams", "orders",
            "--token", "wrong-token",
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode != 0, (
        f"expected non-zero exit for bad token, got {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    # Loud failure message on stderr.
    assert "tally clone failed" in result.stderr or "auth" in result.stderr.lower()


def test_tally_sync_still_stubbed():
    """tally sync remains a stub (unchanged from 28-02)."""
    if not _CLI_BIN.exists():
        pytest.skip("tally_cli not built")
    result = subprocess.run(
        [str(_CLI_BIN), "sync", "--remote", "127.0.0.1:1"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0
    assert "not implemented yet" in result.stdout


def test_out_of_scope_error_covered_by_rust_unit_tests():
    """OutOfScopeError semantics live in src/client/mod.rs#tests
    (frozen_get_rejects_unlisted_stream, frozen_get_rejects_key_not_in_keys_set,
    frozen_get_accepts_prefix_match_and_rejects_non_prefix).

    Exposing a subprocess-level assertion would require a `tally_cli get`
    subcommand, which is Phase 30 (Python binding) territory. Marked here so
    the coverage matrix is explicit.
    """
    pytest.skip("Covered by Rust unit tests in src/client/mod.rs#tests")
