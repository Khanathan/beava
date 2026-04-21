"""Phase 59.6 Wave 2 (TPC-PERF-11) — Python SDK typed-row push tests.

Scope:

- Constants land on the public protocol surface (OP_PUSH_TYPED_BATCH,
  WIRE_TYPED_PIPELINE).
- ``BeavaClient._pack_typed_batch`` emits the wire shape that matches the
  Rust ``decode_typed_row_push_batch`` decoder byte-for-byte.
- ``App.push_many`` routes to the typed path when:
  * server advertises WIRE_TYPED_PIPELINE capability AND
  * stream has a compiled ``_beava_schema``.
  Falls back to legacy OP_PUSH_BATCH otherwise.

No real server is required; we use a fake socket that records the frames
the client sends so we can assert opcode routing without TCP I/O.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

import pytest

from beava._client import BeavaClient
from beava._protocol import (
    OP_PUSH_BATCH,
    OP_PUSH_TYPED_BATCH,
    WIRE_BINARY_PASSTHROUGH,
    WIRE_TYPED_PIPELINE,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_op_push_typed_batch_constant():
    assert OP_PUSH_TYPED_BATCH == 0x19


def test_wire_typed_pipeline_bit():
    assert WIRE_TYPED_PIPELINE == 2
    # Bit 1 is distinct from Phase 59's bit 0.
    assert WIRE_TYPED_PIPELINE != WIRE_BINARY_PASSTHROUGH


# ---------------------------------------------------------------------------
# _pack_typed_batch wire shape
# ---------------------------------------------------------------------------


def _txns_schema_json(inline_str_cap: int = 15) -> dict:
    """Matches ``CompiledSchema.to_json()`` for ``class Txns: user_id: str; amount: float``.

    Layout: user_id InlineStr @ 0 (slot 16), amount F64 @ 16; row_size 24.
    """
    return {
        "inline_str_cap": inline_str_cap,
        "fields": [
            {"name": "user_id", "ty": "inline_str", "offset": 0, "nullable": False},
            {"name": "amount", "ty": "f64", "offset": 16, "nullable": False},
        ],
        "row_size": 24,
    }


def _decode_typed_frame(frame: bytes, schema_json: dict):
    """Mirror of the Rust decoder — verify what _pack_typed_batch produced.

    Returns ``(stream_name, schema_id, rows, ack_token)``.
    Each row is a dict with the decoded field values (inline_str + f64).
    """
    pos = 0
    (name_len,) = struct.unpack_from(">H", frame, pos)
    pos += 2
    stream_name = frame[pos : pos + name_len].decode("utf-8")
    pos += name_len
    (schema_id,) = struct.unpack_from(">I", frame, pos)
    pos += 4
    (row_count,) = struct.unpack_from(">I", frame, pos)
    pos += 4
    row_size = int(schema_json["row_size"])
    inline_cap = int(schema_json["inline_str_cap"])
    rows = []
    for _ in range(row_count):
        row_bytes = frame[pos : pos + row_size]
        pos += row_size
        row_dict: dict = {}
        for f in schema_json["fields"]:
            off = int(f["offset"])
            ty = f["ty"]
            if ty == "inline_str":
                slot = row_bytes[off : off + inline_cap + 1]
                # Trim at first NUL within the cap-sized prefix.
                end = slot.find(b"\x00")
                if end < 0 or end > inline_cap:
                    end = inline_cap
                row_dict[f["name"]] = slot[:end].decode("utf-8")
            elif ty == "f64":
                (v,) = struct.unpack_from("<d", row_bytes, off)
                row_dict[f["name"]] = v
            elif ty == "i64":
                (v,) = struct.unpack_from("<q", row_bytes, off)
                row_dict[f["name"]] = v
            elif ty == "bool":
                row_dict[f["name"]] = bool(row_bytes[off])
        rows.append(row_dict)
    (arena_len,) = struct.unpack_from(">I", frame, pos)
    pos += 4
    _arena = frame[pos : pos + arena_len]
    pos += arena_len
    (ack_token,) = struct.unpack_from(">Q", frame, pos)
    pos += 8
    return stream_name, schema_id, rows, ack_token


def test_pack_typed_batch_happy_path():
    schema = _txns_schema_json()
    events = [
        {"user_id": "alice", "amount": 1.5},
        {"user_id": "bob", "amount": -2.25},
        {"user_id": "", "amount": 0.0},
    ]
    frame = BeavaClient._pack_typed_batch(
        "Txns", schema, events, ack_token=0x1234_5678_9ABC_DEF0, schema_id=0
    )
    stream_name, schema_id, rows, ack = _decode_typed_frame(frame, schema)
    assert stream_name == "Txns"
    assert schema_id == 0  # Wave 2 shortcut
    assert ack == 0x1234_5678_9ABC_DEF0
    assert rows == [
        {"user_id": "alice", "amount": 1.5},
        {"user_id": "bob", "amount": -2.25},
        {"user_id": "", "amount": 0.0},
    ]


def test_pack_typed_batch_respects_explicit_schema_id():
    schema = _txns_schema_json()
    frame = BeavaClient._pack_typed_batch(
        "Txns", schema, [{"user_id": "x", "amount": 1.0}], ack_token=42, schema_id=7
    )
    _stream, schema_id, _rows, _ack = _decode_typed_frame(frame, schema)
    assert schema_id == 7


def test_pack_typed_batch_truncates_inline_str_over_cap():
    schema = _txns_schema_json(inline_str_cap=5)
    # cap=5 means 6-byte slot; row_size = 6 + 8 = 14. Adjust schema.
    schema["fields"][1]["offset"] = 6
    schema["row_size"] = 14
    frame = BeavaClient._pack_typed_batch(
        "Txns", schema, [{"user_id": "alicebob", "amount": 1.0}], ack_token=0
    )
    _stream, _sid, rows, _ack = _decode_typed_frame(frame, schema)
    # 8-byte string truncates to the 5-byte cap.
    assert rows[0]["user_id"] == "alice"


# ---------------------------------------------------------------------------
# push_many dispatch
# ---------------------------------------------------------------------------


@dataclass
class _RecordedFrame:
    opcode: int
    payload: bytes


class _FakeSocket:
    """Minimal socket stand-in: records what's sent, drops recv.

    ``fileno()`` returns ``-1`` so ``select.select`` inside
    ``drain_errors_nonblock`` raises ``ValueError`` (or ``OSError``),
    which the drain treats as "socket in bad state; let next real op
    surface it" → returns without blocking. This keeps our test focused
    on the opcode dispatch without needing a real socket.
    """

    def __init__(self) -> None:
        self.sent: bytearray = bytearray()

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def recv(self, n: int) -> bytes:  # never called on fire-and-forget
        return b""

    def settimeout(self, t) -> None:
        pass

    def setblocking(self, b) -> None:
        pass

    def gettimeout(self):
        return 5.0

    def fileno(self) -> int:
        # Sentinel: `select.select` on an invalid fd raises (OSError
        # on macOS / ValueError on Linux); the client treats both as
        # "bad socket; bail" and returns without blocking.
        return -1

    def close(self) -> None:
        pass


def _parse_sent_frames(raw: bytes) -> list[_RecordedFrame]:
    """Split a record buffer into successive wire frames."""
    out: list[_RecordedFrame] = []
    pos = 0
    while pos + 4 <= len(raw):
        (length,) = struct.unpack_from(">I", raw, pos)
        pos += 4
        if pos + length > len(raw):
            break
        opcode = raw[pos]
        payload = bytes(raw[pos + 1 : pos + length])
        pos += length
        out.append(_RecordedFrame(opcode=opcode, payload=payload))
    return out


def _make_app_with_fake_socket(capability_bits: int):
    """Build an App wired to a fake BeavaClient so we can assert frames."""
    from beava._app import App

    # Build the App and then replace its client's socket with our fake.
    app = App("localhost:6400")
    app._client._sock = _FakeSocket()
    app._client.server_capability_bits = capability_bits
    return app


class _FakeStream:
    """Stream descriptor mimic — attach _beava_stream_name + optional
    _beava_schema so App.push_many can route on those attributes."""

    def __init__(self, name: str, schema=None):
        self._beava_stream_name = name
        if schema is not None:
            self._beava_schema = schema


def test_push_many_uses_typed_when_negotiated_and_schema_present():
    from beava._schema_compile import CompiledSchema, CompiledFieldSpec

    schema = CompiledSchema(
        inline_str_cap=15,
        fields=[
            CompiledFieldSpec(name="user_id", ty="inline_str", offset=0, nullable=False),
            CompiledFieldSpec(name="amount", ty="f64", offset=16, nullable=False),
        ],
        row_size=24,
    )
    app = _make_app_with_fake_socket(WIRE_BINARY_PASSTHROUGH | WIRE_TYPED_PIPELINE)
    stream = _FakeStream("Txns", schema=schema)

    app.push_many(stream, [{"user_id": "alice", "amount": 1.5}])

    frames = _parse_sent_frames(bytes(app._client._sock.sent))
    assert len(frames) == 1
    assert frames[0].opcode == OP_PUSH_TYPED_BATCH, (
        f"expected typed path opcode 0x{OP_PUSH_TYPED_BATCH:02x}; "
        f"got 0x{frames[0].opcode:02x}"
    )


def test_push_many_falls_back_when_capability_not_advertised():
    from beava._schema_compile import CompiledSchema, CompiledFieldSpec

    schema = CompiledSchema(
        inline_str_cap=15,
        fields=[
            CompiledFieldSpec(name="user_id", ty="inline_str", offset=0, nullable=False),
            CompiledFieldSpec(name="amount", ty="f64", offset=16, nullable=False),
        ],
        row_size=24,
    )
    # Server advertises only the Phase-59 binary passthrough bit, no typed.
    app = _make_app_with_fake_socket(WIRE_BINARY_PASSTHROUGH)
    stream = _FakeStream("Txns", schema=schema)

    app.push_many(stream, [{"user_id": "alice", "amount": 1.5}])
    frames = _parse_sent_frames(bytes(app._client._sock.sent))
    assert len(frames) == 1
    assert frames[0].opcode == OP_PUSH_BATCH
    assert frames[0].opcode != OP_PUSH_TYPED_BATCH


def test_push_many_falls_back_when_schema_missing():
    # Server advertises WIRE_TYPED_PIPELINE but the stream has no compiled
    # _beava_schema (un-annotated class / dict source).
    app = _make_app_with_fake_socket(WIRE_BINARY_PASSTHROUGH | WIRE_TYPED_PIPELINE)
    stream = _FakeStream("Clicks", schema=None)

    app.push_many(stream, [{"user_id": "x"}])
    frames = _parse_sent_frames(bytes(app._client._sock.sent))
    assert len(frames) == 1
    assert frames[0].opcode == OP_PUSH_BATCH
