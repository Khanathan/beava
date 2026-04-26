"""Phase 27-02, Task 3: cross-language wire-contract test for OP_SUBSCRIBE.

Two raw-socket asyncio clients open `OP_SUBSCRIBE` sessions against the real
Rust tally binary. Scope A = [orders]; Scope B = [clicks]. A test driver
interleaves pushes to both streams; the test asserts each client receives
ONLY its scope-matching events, in the accept order of the matching pushes.

Per `27-CONTEXT.md §Per-connection ordering guarantee` we deliberately DO
NOT assert a cross-subscriber total order. Per-subscriber acceptance order
is the only ordering the server commits to.

Frame decoding is shallow: the test parses the documented event-frame shape
`[u32 len][u8 tag=0x03][u64 secs][u32 nanos][u32 payload_len][payload]` and
then uses `json.loads` on the payload (tally's JSON PUSH path sends the
client's event dict verbatim). No postcard decoder is needed — SUBSCRIBE
frames carry JSON payload bytes, not postcard.

Skipped cleanly if the `tally` binary hasn't been built.
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
from pathlib import Path
from typing import Tuple

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_RELEASE_BIN = _PROJECT_ROOT / "target" / "release" / "tally"
_DEBUG_BIN = _PROJECT_ROOT / "target" / "debug" / "tally"

ADMIN_TOKEN = "asyncio-test-admin"
OP_PUSH = 0x01
OP_SUBSCRIBE = 0x11
TAG_EVENT = 0x03
STATUS_ERROR = 0x01
TYPE_STR = 0x04


# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------


def _write_u16_string(s: str) -> bytes:
    b = s.encode("utf-8")
    assert len(b) < 2**16
    return struct.pack(">H", len(b)) + b


def _write_scope(streams: list[str]) -> bytes:
    buf = bytearray()
    buf += struct.pack(">H", len(streams))
    for s in streams:
        buf += _write_u16_string(s)
    buf.append(0)  # has_keys=0
    buf.append(0)  # has_prefix=0
    buf += _write_u16_string("all")
    return bytes(buf)


def _build_subscribe_frame(token: str, streams: list[str]) -> bytes:
    payload = _write_u16_string(token) + _write_scope(streams)
    total_len = 1 + len(payload)  # opcode + payload
    return struct.pack(">I", total_len) + bytes([OP_SUBSCRIBE]) + payload


def _build_push_frame(stream_name: str, user_id: str) -> bytes:
    body = bytearray()
    body += _write_u16_string(stream_name)
    # one field: user_id = TYPE_STR(user_id)
    body += struct.pack(">H", 1)
    body += _write_u16_string("user_id")
    body.append(TYPE_STR)
    body += _write_u16_string(user_id)
    total_len = 1 + len(body)
    return struct.pack(">I", total_len) + bytes([OP_PUSH]) + bytes(body)


# ---------------------------------------------------------------------------
# Frame I/O
# ---------------------------------------------------------------------------


async def _read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = await reader.read(n - len(buf))
        if not chunk:
            raise RuntimeError(f"connection closed, got {len(buf)} of {n} bytes")
        buf += chunk
    return buf


async def _read_event_frame(reader: asyncio.StreamReader) -> Tuple[int, int, int, bytes]:
    """Read one subscribe event frame.

    Returns (tag, ts_secs, ts_nanos, payload_bytes). `tag` should always be
    `TAG_EVENT` (0x03) on a live subscribe stream.
    """
    len_bytes = await _read_exactly(reader, 4)
    (total_len,) = struct.unpack(">I", len_bytes)
    body = await _read_exactly(reader, total_len)
    tag = body[0]
    (secs,) = struct.unpack(">Q", body[1:9])
    (nanos,) = struct.unpack(">I", body[9:13])
    (payload_len,) = struct.unpack(">I", body[13:17])
    payload = body[17 : 17 + payload_len]
    return tag, secs, nanos, payload


# ---------------------------------------------------------------------------
# Harness
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
    import json
    import urllib.request

    body = json.dumps(
        {
            "name": name,
            "key_field": "user_id",
            "features": [
                {"name": "count_1h", "type": "count", "window": "1h"}
            ],
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


@pytest.fixture
def tally_server():
    """Spawn a fresh tally binary on ephemeral ports. No seeded snapshot —
    subscribe delivers live events only, so the server starts with an empty
    state and we register `orders` + `clicks` via HTTP."""
    binary = _pick_binary()
    if binary is None:
        pytest.skip("tally binary not built; run `cargo build` to enable this test")

    tmp = tempfile.TemporaryDirectory()
    tcp_port = _find_free_port()
    http_port = _find_free_port()
    env = os.environ.copy()
    env["TALLY_TCP_PORT"] = str(tcp_port)
    env["TALLY_HTTP_PORT"] = str(http_port)
    env["TALLY_ADMIN_TOKEN"] = ADMIN_TOKEN
    env["TALLY_SNAPSHOT_PATH"] = str(Path(tmp.name) / "tally.snapshot")
    env["TALLY_SNAPSHOT"] = "1"

    proc = subprocess.Popen(
        [str(binary)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", tcp_port, timeout=15.0)
        _register_stream_http(http_port, "orders")
        _register_stream_http(http_port, "clicks")
        yield ("127.0.0.1", tcp_port, http_port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        tmp.cleanup()


async def _open_subscriber(host: str, port: int, streams: list[str]) -> tuple[
    asyncio.StreamReader, asyncio.StreamWriter
]:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(_build_subscribe_frame(ADMIN_TOKEN, streams))
    await writer.drain()
    return reader, writer


async def _push_event(host: str, port: int, stream_name: str, user_id: str) -> None:
    """Open a short-lived push connection and send one PUSH frame. We
    discard the server's STATUS_OK response to avoid stalling the BufWriter
    on the server side."""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(_build_push_frame(stream_name, user_id))
    await writer.drain()
    # Response: [u32 len][u8 status][payload]
    len_bytes = await _read_exactly(reader, 4)
    (n,) = struct.unpack(">I", len_bytes)
    _rest = await _read_exactly(reader, n)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_multi_subscriber_scope_isolation_and_ordering(tally_server):
    """Two subscribers with disjoint scopes see only their matching events.

    We deliberately do NOT assert a cross-subscriber total order — the
    server commits only to per-subscriber accept-order (per
    27-CONTEXT.md §Per-connection ordering guarantee).
    """
    host, tcp_port, _http_port = tally_server

    async def _run():
        # Open both subscribers before any push fires, so registration
        # completes before the ingest hook starts enumerating.
        r_a, w_a = await _open_subscriber(host, tcp_port, ["orders"])
        r_b, w_b = await _open_subscriber(host, tcp_port, ["clicks"])

        # Give the server a brief moment to register both sessions before
        # the first push. The server-side registration is synchronous with
        # the SUBSCRIBE frame read, so 50 ms is overkill but hurts nothing.
        await asyncio.sleep(0.1)

        # Interleave pushes: orders(k1), clicks(k1), orders(k2), clicks(k2),
        # orders(k3). Subscriber A should see [k1, k2, k3] on stream=orders,
        # subscriber B should see [k1, k2] on stream=clicks.
        pushes = [
            ("orders", "k1"),
            ("clicks", "k1"),
            ("orders", "k2"),
            ("clicks", "k2"),
            ("orders", "k3"),
        ]
        expected_a = ["k1", "k2", "k3"]
        expected_b = ["k1", "k2"]

        for stream, key in pushes:
            await _push_event(host, tcp_port, stream, key)

        async def _drain(reader: asyncio.StreamReader, n: int) -> list[str]:
            out: list[str] = []
            for _ in range(n):
                tag, _secs, _nanos, payload = await asyncio.wait_for(
                    _read_event_frame(reader), timeout=5.0
                )
                assert tag == TAG_EVENT, f"expected event tag 0x03, got 0x{tag:02x}"
                ev = json.loads(payload.decode("utf-8"))
                out.append(ev["user_id"])
            return out

        keys_a, keys_b = await asyncio.gather(
            _drain(r_a, len(expected_a)),
            _drain(r_b, len(expected_b)),
        )

        assert keys_a == expected_a, f"subscriber A saw {keys_a}, expected {expected_a}"
        assert keys_b == expected_b, f"subscriber B saw {keys_b}, expected {expected_b}"

        # Close both subscribers cleanly.
        for w in (w_a, w_b):
            w.close()
            try:
                await w.wait_closed()
            except Exception:
                pass

    asyncio.run(_run())
