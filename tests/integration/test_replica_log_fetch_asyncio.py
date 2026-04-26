"""Phase 35-01: cross-language wire-contract test for OP_LOG_FETCH.

Two asyncio clients open raw-socket `OP_LOG_FETCH` requests against the real
Rust tally binary after a driver pushes a known set of events. The test
asserts:

  1. wire_contract_reads_event_frames_then_end — the response frame stream
     carries the documented shape `[u32 len][u8 tag=0x03][u64 ts_ms]
     [u32 payload_len][payload]` for each event, then a single
     `[u32 len=1][u8 tag=0x04]` END frame.
  2. scope_isolation_across_two_clients — two clients with disjoint
     stream scopes see disjoint payload byte-sets from a shared log.

Log-payload bytes are the server's on-disk representation of the PUSH
frame (format byte `0x01` = binary, followed by the Plan 11-06 TLV body).
We do NOT decode the binary body — substring matching on the user_id
bytes is enough to verify scope filtering without pulling in a
language-level codec.

Skipped cleanly if the `tally` binary hasn't been built.
"""

from __future__ import annotations

import asyncio
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
OP_LOG_FETCH = 0x13
TAG_EVENT = 0x03
TAG_END = 0x04
STATUS_ERROR = 0x01
TYPE_STR = 0x04


# ---------------------------------------------------------------------------
# Wire helpers (duplicated from test_replica_subscribe_asyncio.py to keep
# this test self-contained — the two tests intentionally mirror one another
# so each is readable in isolation).
# ---------------------------------------------------------------------------


def _write_u16_string(s: str) -> bytes:
    b = s.encode("utf-8")
    assert len(b) < 2**16
    return struct.pack(">H", len(b)) + b


def _write_scope(streams: list[str], keys: list[str] | None = None) -> bytes:
    buf = bytearray()
    buf += struct.pack(">H", len(streams))
    for s in streams:
        buf += _write_u16_string(s)
    if keys is None:
        buf.append(0)  # has_keys=0
    else:
        buf.append(1)
        buf += struct.pack(">I", len(keys))
        for k in keys:
            buf += _write_u16_string(k)
    buf.append(0)  # has_prefix=0
    buf += _write_u16_string("all")
    return bytes(buf)


def _build_log_fetch_frame(
    token: str, from_ts_millis: int, streams: list[str], keys: list[str] | None = None
) -> bytes:
    payload = (
        _write_u16_string(token)
        + struct.pack(">Q", from_ts_millis)
        + _write_scope(streams, keys)
    )
    total_len = 1 + len(payload)
    return struct.pack(">I", total_len) + bytes([OP_LOG_FETCH]) + payload


def _build_push_frame(stream_name: str, user_id: str) -> bytes:
    body = bytearray()
    body += _write_u16_string(stream_name)
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


async def _read_log_fetch_frame(
    reader: asyncio.StreamReader,
) -> Tuple[int, int, bytes]:
    """Read one log-fetch response frame.

    Returns (tag, timestamp_ms, payload). For the END frame the payload is
    empty and timestamp_ms is 0. For STATUS_ERROR (tag=0x01) the "payload"
    field carries the error message bytes and timestamp_ms is 0.
    """
    len_bytes = await _read_exactly(reader, 4)
    (total_len,) = struct.unpack(">I", len_bytes)
    body = await _read_exactly(reader, total_len)
    tag = body[0]
    if tag == TAG_EVENT:
        assert total_len >= 1 + 8 + 4, f"event frame too short: {total_len}"
        (ts_ms,) = struct.unpack(">Q", body[1:9])
        (payload_len,) = struct.unpack(">I", body[9:13])
        payload = body[13 : 13 + payload_len]
        return tag, ts_ms, payload
    if tag == TAG_END:
        assert total_len == 1, f"END frame must be body-less, got len={total_len}"
        return tag, 0, b""
    if tag == STATUS_ERROR:
        return tag, 0, body[1:]
    raise AssertionError(f"unknown tag 0x{tag:02x} in log-fetch response")


# ---------------------------------------------------------------------------
# Harness (mirrors test_replica_subscribe_asyncio.py)
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
    """Spawn a fresh tally binary on ephemeral ports with event_log enabled
    so PUSH writes land on disk and LOG_FETCH can read them back."""
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
    env["TALLY_EVENT_LOG"] = "1"
    env["TALLY_DATA_DIR"] = tmp.name

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


async def _push_event(host: str, port: int, stream_name: str, user_id: str) -> None:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(_build_push_frame(stream_name, user_id))
    await writer.drain()
    len_bytes = await _read_exactly(reader, 4)
    (n,) = struct.unpack(">I", len_bytes)
    _rest = await _read_exactly(reader, n)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


async def _log_fetch(
    host: str,
    port: int,
    streams: list[str],
    keys: list[str] | None = None,
    from_ts_millis: int = 0,
) -> list[tuple[int, bytes]]:
    """Issue one OP_LOG_FETCH and return (ts_ms, payload) for every event
    frame, terminated implicitly by the END frame."""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(_build_log_fetch_frame(ADMIN_TOKEN, from_ts_millis, streams, keys))
    await writer.drain()

    events: list[tuple[int, bytes]] = []
    while True:
        tag, ts_ms, payload = await asyncio.wait_for(
            _read_log_fetch_frame(reader), timeout=10.0
        )
        if tag == TAG_END:
            break
        if tag == STATUS_ERROR:
            raise AssertionError(
                f"unexpected STATUS_ERROR: {payload.decode('utf-8', errors='replace')}"
            )
        assert tag == TAG_EVENT, f"unexpected tag 0x{tag:02x}"
        events.append((ts_ms, payload))

    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    return events


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_wire_contract_reads_event_frames_then_end(tally_server):
    """Push 5 events to `orders`, fetch with from_ts=0, verify frame count
    and END frame and timestamp monotonicity on the wire."""
    host, tcp_port, _http_port = tally_server

    async def _run():
        for k in ("k1", "k2", "k3", "k4", "k5"):
            await _push_event(host, tcp_port, "orders", k)
        # Let the server flush its buffered log writer before we read.
        await asyncio.sleep(0.3)

        events = await _log_fetch(host, tcp_port, ["orders"])
        assert len(events) == 5, f"expected 5 events, got {len(events)}"

        # Timestamps are non-decreasing within a single stream log.
        ts_list = [ts for ts, _ in events]
        assert ts_list == sorted(ts_list), (
            f"event timestamps should be non-decreasing, got {ts_list}"
        )

        # Every payload carries the expected `user_id` key somewhere in
        # the binary-tagged body. Substring check is sufficient for wire
        # contract (the binary body decoder lives in the Rust client).
        ids_seen = set()
        for _ts, payload in events:
            for k in ("k1", "k2", "k3", "k4", "k5"):
                if k.encode("utf-8") in payload:
                    ids_seen.add(k)
        assert ids_seen == {"k1", "k2", "k3", "k4", "k5"}, (
            f"expected all 5 user_ids in payloads, got {ids_seen}"
        )

    asyncio.run(_run())


@pytest.mark.timeout(60)
def test_scope_isolation_across_two_clients(tally_server):
    """Two clients with disjoint scopes (orders vs clicks) see disjoint
    event sets from a shared log."""
    host, tcp_port, _http_port = tally_server

    async def _run():
        # Interleaved pushes across two streams.
        pushes = [
            ("orders", "u_a1"),
            ("clicks", "u_b1"),
            ("orders", "u_a2"),
            ("clicks", "u_b2"),
            ("orders", "u_a3"),
        ]
        for stream, key in pushes:
            await _push_event(host, tcp_port, stream, key)
        await asyncio.sleep(0.3)

        events_a, events_b = await asyncio.gather(
            _log_fetch(host, tcp_port, ["orders"]),
            _log_fetch(host, tcp_port, ["clicks"]),
        )
        assert len(events_a) == 3, f"orders: expected 3, got {len(events_a)}"
        assert len(events_b) == 2, f"clicks: expected 2, got {len(events_b)}"

        # Client A (orders) payloads must contain no "u_b" keys.
        for _ts, payload in events_a:
            assert b"u_b1" not in payload, "orders scope leaked u_b1"
            assert b"u_b2" not in payload, "orders scope leaked u_b2"
        # Client B (clicks) payloads must contain no "u_a" keys.
        for _ts, payload in events_b:
            assert b"u_a1" not in payload, "clicks scope leaked u_a1"
            assert b"u_a2" not in payload, "clicks scope leaked u_a2"
            assert b"u_a3" not in payload, "clicks scope leaked u_a3"

    asyncio.run(_run())
