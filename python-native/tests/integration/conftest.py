"""Plan 30-02 integration-test fixtures.

Spins up a real `tally` server subprocess, seeds a base snapshot file so
`OP_SNAPSHOT_FETCH` has content to return, and yields a struct the tests
can use to target it.

Design notes
------------

Why seed via a base snapshot file (not `App.push`)?

`OP_SNAPSHOT_FETCH` reads the server's *persisted* base snapshot on disk
— live-pushed events only appear in a future snapshot once the server
checkpoints, and v0 exposes no manual "take snapshot now" opcode. The
proven pattern for priming a replica test is the base-snapshot file;
that's exactly what `tests/integration/test_tally_clone.py` (Phase
28-04) uses and we mirror it here.

Why under `python-native/tests/integration/` and not `python/tests/integration/`?

The plan nominally said `python/tests/integration/`, but Plan 30-01
discovered that pytest rooted at `python/` shadows the installed wheel
with the source tree (because `python/pyproject.toml` declares
`testpaths = ["tests"]`). Running from `python-native/` keeps the
installed `tally._native` extension as the winning import. See Plan
30-01 Deviation 2.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_TALLY_BIN_DEBUG = _PROJECT_ROOT / "target" / "debug" / "tally"
_TALLY_BIN_RELEASE = _PROJECT_ROOT / "target" / "release" / "tally"
_CLI_BIN_DEBUG = _PROJECT_ROOT / "target" / "debug" / "tally_cli"
_CLI_BIN_RELEASE = _PROJECT_ROOT / "target" / "release" / "tally_cli"


def _pick_binary(debug: Path, release: Path, label: str) -> Path:
    """Prefer release; fall back to debug. Skip if neither exists."""
    if release.exists():
        return release
    if debug.exists():
        return debug
    pytest.skip(f"{label} binary not built (tried {release} and {debug})")


# -- Binary discovery exposed to tests ----------------------------------------


@pytest.fixture(scope="session")
def tally_server_bin() -> Path:
    return _pick_binary(_TALLY_BIN_DEBUG, _TALLY_BIN_RELEASE, "tally server")


@pytest.fixture(scope="session")
def tally_cli_bin() -> Path:
    return _pick_binary(_CLI_BIN_DEBUG, _CLI_BIN_RELEASE, "tally_cli")


# -- Snapshot seeding helpers (mirrors tests/integration/test_tally_clone.py) -


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
    """Write a v7 base snapshot with the given (entity_key, [stream_name]) pairs.

    Empty operators + no last_event_at => minimal valid entity the server can
    serve via OP_SNAPSHOT_FETCH. Good enough for presence / scope / count
    assertions; value-shape assertions are left to `Pipeline.get(...) is not None`.
    """
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
    buf += _enc_varint(0)  # pipelines: empty
    buf += _enc_varint(0)  # backfill_complete: empty
    # v7 header: magic bytes + version tag.
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


def _register_stream_http(http_port: int, name: str, token: str) -> None:
    """POST /pipelines with a single count feature so scope validation accepts
    the stream name in the test's CloneArgs."""
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


# -- Public fixture -----------------------------------------------------------


ADMIN_TOKEN = "plan-30-02-test-token"


class _SeededServer:
    """Typed view the tests receive from `seeded_server`."""

    def __init__(
        self,
        *,
        remote: str,
        token: str,
        tcp_port: int,
        http_port: int,
        streams: list[str],
        in_scope_keys: list[str],
        out_of_scope_keys: list[str],
    ) -> None:
        self.remote = remote
        self.token = token
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.streams = streams
        self.in_scope_keys = in_scope_keys
        self.out_of_scope_keys = out_of_scope_keys


@pytest.fixture
def seeded_server(tally_server_bin):
    """Spawn a tally server with a base snapshot pre-seeded with fixture
    entities. Yields a `_SeededServer` describing the live instance; tears
    down on exit.

    Fixture keys:
      - Stream `Transactions`
      - In-scope entities: u1, u2 (Pipeline scope will include these)
      - Extra entity: u3 (loaded in snapshot; used in out-of-scope assertions
        where the Pipeline's declared scope is `keys=[u1, u2]`)
    """
    streams = ["Transactions"]
    in_scope = ["u1", "u2"]
    extra = ["u3"]
    all_entities = [(k, streams) for k in in_scope + extra]

    tmp = tempfile.TemporaryDirectory()
    snap_path = Path(tmp.name) / "tally.snapshot.base.0000000001"
    _write_base_snapshot_file(snap_path, all_entities)

    tcp_port = _find_free_port()
    http_port = _find_free_port()
    env = os.environ.copy()
    env["TALLY_TCP_PORT"] = str(tcp_port)
    env["TALLY_HTTP_PORT"] = str(http_port)
    env["TALLY_ADMIN_TOKEN"] = ADMIN_TOKEN
    env["TALLY_SNAPSHOT_PATH"] = str(Path(tmp.name) / "tally.snapshot")
    env["TALLY_SNAPSHOT"] = "1"

    proc = subprocess.Popen(
        [str(tally_server_bin)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", tcp_port, timeout=15.0)
        for stream_name in streams:
            _register_stream_http(http_port, stream_name, ADMIN_TOKEN)
        yield _SeededServer(
            remote=f"127.0.0.1:{tcp_port}",
            token=ADMIN_TOKEN,
            tcp_port=tcp_port,
            http_port=http_port,
            streams=streams,
            in_scope_keys=in_scope,
            out_of_scope_keys=extra,
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        tmp.cleanup()
