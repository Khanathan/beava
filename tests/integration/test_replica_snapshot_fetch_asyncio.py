"""Phase 27-01, Task 3: cross-language wire-contract test for OP_SNAPSHOT_FETCH.

A raw-socket Python asyncio client round-trips the `OP_SNAPSHOT_FETCH` (0x12)
wire protocol against the real Rust beava binary. Proves:

  * The admin-token + Scope request encoding defined in
    `src/server/protocol.rs` can be produced from Python.
  * The server emits exactly two response frames in the documented shape:
      - Header frame: `[u32 BE len=13][u8 tag=0x01][u64 BE secs][u32 BE nanos]`
      - Payload frame: `[u32 BE len][u8 tag=0x02][postcard bytes]`
  * The payload top-level decodes as a `BaseSnapshotState` (shallow parse:
    SnapshotHeader, then the entities Vec length, then the first entity's
    key string — all flat postcard primitives).

Per user direction on Plan 27-01, this test deliberately does NOT hand-roll
a full postcard decoder for nested operator/table/static-feature types.
Nested bytes are only counted / structure-checked at the outer layer.
Cross-language comparison of operator internals is covered in Rust.

The test is skipped cleanly if the `beava` binary hasn't been built —
the Rust integration tests cover the same wire contract.
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
_RELEASE_BIN = _PROJECT_ROOT / "target" / "release" / "beava"
_DEBUG_BIN = _PROJECT_ROOT / "target" / "debug" / "beava"

ADMIN_TOKEN = "asyncio-test-admin"
OP_SNAPSHOT_FETCH = 0x12
TAG_HEADER = 0x01
TAG_PAYLOAD = 0x02
STATUS_ERROR = 0x01


# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------


def _write_u16_string(s: str) -> bytes:
    b = s.encode("utf-8")
    assert len(b) < 2**16, "string too long for u16 length prefix"
    return struct.pack(">H", len(b)) + b


def _write_scope(streams: list[str], keys: list[str] | None, prefix: str | None, pull: str) -> bytes:
    buf = bytearray()
    buf += struct.pack(">H", len(streams))
    for s in streams:
        buf += _write_u16_string(s)
    if keys is None:
        buf.append(0)
    else:
        buf.append(1)
        buf += struct.pack(">I", len(keys))
        for k in keys:
            buf += _write_u16_string(k)
    if prefix is None:
        buf.append(0)
    else:
        buf.append(1)
        buf += _write_u16_string(prefix)
    buf += _write_u16_string(pull)
    return bytes(buf)


def _build_snapshot_fetch_frame(token: str, scope_bytes: bytes) -> bytes:
    payload = _write_u16_string(token) + scope_bytes
    total_len = 1 + len(payload)  # opcode + payload
    return struct.pack(">I", total_len) + bytes([OP_SNAPSHOT_FETCH]) + payload


def _decode_varint(buf: bytes, offset: int) -> tuple[int, int]:
    """Decode a postcard varint (LEB128 unsigned). Returns (value, new_offset)."""
    value = 0
    shift = 0
    while True:
        b = buf[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return value, offset
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _decode_varint_string(buf: bytes, offset: int) -> tuple[str, int]:
    length, offset = _decode_varint(buf, offset)
    s = buf[offset : offset + length].decode("utf-8")
    return s, offset + length


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


async def _read_frame(reader: asyncio.StreamReader) -> Tuple[int, bytes]:
    len_bytes = await _read_exactly(reader, 4)
    (total_len,) = struct.unpack(">I", len_bytes)
    tag_byte = await _read_exactly(reader, 1)
    tag = tag_byte[0]
    body = await _read_exactly(reader, total_len - 1)
    return tag, body


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _pick_binary() -> Path | None:
    # Prefer whichever binary is newer — avoids stale release builds shadowing
    # a freshly-rebuilt debug binary in CI / local dev cycles.
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
    raise RuntimeError(f"beava did not become ready on {host}:{port}")


def _write_base_snapshot_file(path: Path, entities: list[tuple[str, list[str]]]) -> None:
    """Write a minimal v7 base snapshot that the server will pick up at startup.

    Layout: `[0x07][0x00][postcard(BaseSnapshotState)]`.

    The `BaseSnapshotState` is hand-encoded in postcard format so this test is
    independent of the Rust snapshot writer. The encoded entities each have
    empty operators/static_features/table_rows — we only care about the
    (key, stream_name) shape for the 27-01 Scope-filter contract.
    """
    buf = bytearray()

    # --- SnapshotHeader ---
    # snapshot_type: enum SnapshotType { Base = 0, Delta { base_seq } = 1 }
    buf += _enc_varint(0)  # Base, unit variant
    # sequence: u64 = 1
    buf += _enc_varint(1)

    # --- entities: Vec<(String, SerializableEntityState)> ---
    buf += _enc_varint(len(entities))
    for key, streams in entities:
        buf += _enc_varint_string(key)
        # SerializableEntityState { streams: Vec<(String, SerializableStreamEntityState)>,
        #                           static_features: Vec<(String, StaticFeature)>,
        #                           table_rows: Vec<(String, SerializableTableRow)> }
        buf += _enc_varint(len(streams))
        for stream_name in streams:
            buf += _enc_varint_string(stream_name)
            # SerializableStreamEntityState { operators: Vec<(String, OperatorState)>,
            #                                 last_event_at: Option<SystemTime> }
            buf += _enc_varint(0)  # operators: empty
            buf.append(0)  # Option::None for last_event_at
        buf += _enc_varint(0)  # static_features: empty
        buf += _enc_varint(0)  # table_rows: empty

    # --- pipelines: Vec<SerializablePipeline> = [] ---
    buf += _enc_varint(0)
    # --- backfill_complete: Vec<(String, String)> = [] ---
    buf += _enc_varint(0)

    full = bytes([0x07, 0x00]) + bytes(buf)
    path.write_bytes(full)


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


@pytest.fixture
def beava_server():
    """Spawn a fresh beava binary on ephemeral ports with a seeded snapshot."""
    binary = _pick_binary()
    if binary is None:
        pytest.skip("beava binary not built; run `cargo build` to enable this test")

    tmp = tempfile.TemporaryDirectory()
    snap_path = Path(tmp.name) / "beava.snapshot.base.0000000001"

    # Seed entities:
    #   u1 → [orders]
    #   u2 → [clicks]
    #   u3 → [orders, clicks]
    _write_base_snapshot_file(
        snap_path,
        entities=[
            ("u1", ["orders"]),
            ("u2", ["clicks"]),
            ("u3", ["orders", "clicks"]),
        ],
    )

    tcp_port = _find_free_port()
    http_port = _find_free_port()
    env = os.environ.copy()
    env["BEAVA_TCP_PORT"] = str(tcp_port)
    env["BEAVA_HTTP_PORT"] = str(http_port)
    env["BEAVA_ADMIN_TOKEN"] = ADMIN_TOKEN
    env["BEAVA_SNAPSHOT_PATH"] = str(Path(tmp.name) / "beava.snapshot")
    # Make sure snapshotting is on so the server's snap_dir lookup works.
    env["BEAVA_SNAPSHOT"] = "1"

    proc = subprocess.Popen(
        [str(binary)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_tcp("127.0.0.1", tcp_port, timeout=15.0)

        # Stream definitions are NOT in the seeded snapshot (pipelines=[]),
        # so register `orders` and `clicks` via the register HTTP endpoint
        # before the scope validation gate runs. Use the public axum register
        # path exactly like the sdk does.
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


def _register_stream_http(http_port: int, name: str) -> None:
    """Register a stream with a single count feature via POST /pipelines."""
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


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


async def _fetch_snapshot(
    host: str, port: int, token: str, streams: list[str]
) -> tuple[int, int, bytes]:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        scope = _write_scope(streams, None, None, "all")
        frame = _build_snapshot_fetch_frame(token, scope)
        writer.write(frame)
        await writer.drain()

        tag, body = await _read_frame(reader)
        # NOTE: STATUS_ERROR (0x01) and TAG_HEADER (0x01) collide in the first
        # byte. Distinguish by body length — a header frame body is exactly
        # 12 bytes (u64 secs + u32 nanos). Anything else with tag 0x01 is an
        # error frame whose body is the UTF-8 error string.
        if tag == STATUS_ERROR and len(body) != 12:
            raise RuntimeError(f"server error: {body.decode('utf-8', errors='replace')}")
        assert tag == TAG_HEADER, f"expected header tag 0x01, got 0x{tag:02x}"
        assert len(body) == 12, f"header body must be 12 bytes, got {len(body)}"
        (secs,) = struct.unpack(">Q", body[:8])
        (nanos,) = struct.unpack(">I", body[8:])

        tag2, payload = await _read_frame(reader)
        assert tag2 == TAG_PAYLOAD, f"expected payload tag 0x02, got 0x{tag2:02x}"
        return secs, nanos, payload
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def _shallow_decode_entity_keys(payload: bytes, expected_count: int) -> list[str]:
    """Shallow postcard decode: snapshot header + entity count + first key.

    Per user direction on 27-01: decode only flat postcard primitives
    (enum tag, varint, string). Do NOT walk into SerializableEntityState.
    Returns the entity COUNT via `expected_count` for assertion and the
    first entity KEY for a second assertion. Nested entity bytes are
    treated as opaque.
    """
    offset = 0
    # snapshot_type enum tag — "Base" unit variant = 0.
    _tag, offset = _decode_varint(payload, offset)
    # sequence: u64
    _seq, offset = _decode_varint(payload, offset)
    # entities Vec len
    n_entities, offset = _decode_varint(payload, offset)
    assert n_entities == expected_count, (
        f"entity count mismatch: payload says {n_entities}, expected {expected_count}"
    )
    if n_entities == 0:
        return []
    # First entity key string (flat primitive — safe to decode).
    first_key, _offset = _decode_varint_string(payload, offset)
    return [first_key]


@pytest.mark.timeout(60)
def test_snapshot_fetch_streams_only_roundtrip(beava_server):
    host, tcp_port, _http_port = beava_server

    # Scope: orders only → expect entities = [u1, u3] (2 entities).
    secs, nanos, payload = asyncio.run(
        _fetch_snapshot(host, tcp_port, ADMIN_TOKEN, ["orders"])
    )
    now = time.time()
    assert 0 < secs <= now + 5, f"snapshot_taken_at out of range: {secs} vs now {now}"
    assert 0 <= nanos < 1_000_000_000
    assert len(payload) > 0, "payload frame must be non-empty"

    # Shallow decode: confirm entity count and first entity key.
    keys = _shallow_decode_entity_keys(payload, expected_count=2)
    assert keys == ["u1"], f"first key should be 'u1', got {keys}"


@pytest.mark.timeout(60)
def test_snapshot_fetch_rejects_wrong_token(beava_server):
    host, tcp_port, _http_port = beava_server

    async def _run() -> str:
        reader, writer = await asyncio.open_connection(host, tcp_port)
        try:
            scope = _write_scope(["orders"], None, None, "all")
            frame = _build_snapshot_fetch_frame("wrong-token", scope)
            writer.write(frame)
            await writer.drain()
            tag, body = await _read_frame(reader)
            assert tag == STATUS_ERROR, f"expected STATUS_ERROR, got 0x{tag:02x}"
            return body.decode("utf-8")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    msg = asyncio.run(_run())
    assert "unauthorized" in msg, f"expected unauthorized error, got {msg!r}"


@pytest.mark.timeout(60)
def test_snapshot_fetch_rejects_unknown_stream(beava_server):
    host, tcp_port, _http_port = beava_server

    async def _run() -> str:
        reader, writer = await asyncio.open_connection(host, tcp_port)
        try:
            scope = _write_scope(["does_not_exist"], None, None, "all")
            frame = _build_snapshot_fetch_frame(ADMIN_TOKEN, scope)
            writer.write(frame)
            await writer.drain()
            tag, body = await _read_frame(reader)
            assert tag == STATUS_ERROR, f"expected STATUS_ERROR, got 0x{tag:02x}"
            return body.decode("utf-8")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    msg = asyncio.run(_run())
    assert "unknown stream" in msg and "does_not_exist" in msg, (
        f"expected UnknownStream error, got {msg!r}"
    )
