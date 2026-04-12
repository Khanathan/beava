"""Binary protocol encoding/decoding for the Tally wire format.

Matches the Rust server's protocol.rs byte-for-byte:
- Frame: [4-byte BE u32 length][opcode][payload]  (length = 1 + len(payload))
- Response: [4-byte BE u32 length][status][payload]  (length = 1 + len(payload))
- String: [2-byte BE u16 length][UTF-8 bytes]

All encoding functions return ``bytes`` suitable for sending over TCP.
"""

from __future__ import annotations

import json
import struct

from tally._types import ProtocolError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OP_PUSH: int = 0x01
OP_GET: int = 0x02
OP_SET: int = 0x03
OP_MSET: int = 0x04
OP_REGISTER: int = 0x05
OP_MGET: int = 0x06
OP_PUSH_ASYNC: int = 0x07
OP_FLUSH: int = 0x08
OP_PUSH_BATCH: int = 0x0A

STATUS_OK: int = 0x00
STATUS_ERROR: int = 0x01

# Binary event payload type tags (PERF-02)
TYPE_NULL: int = 0x00
TYPE_BOOL: int = 0x01
TYPE_I64: int = 0x02
TYPE_F64: int = 0x03
TYPE_STR: int = 0x04

# Maximum frame size (64 MB) -- reject before allocating buffer (DoS protection)
MAX_FRAME_SIZE: int = 64 * 1024 * 1024

# Pre-compiled struct instances (hot path)
_U16 = struct.Struct(">H")
_I64 = struct.Struct(">q")
_F64 = struct.Struct(">d")


# ---------------------------------------------------------------------------
# Low-level encoding
# ---------------------------------------------------------------------------


def encode_frame(opcode: int, payload: bytes) -> bytes:
    """Encode a wire frame: [4-byte BE length][opcode][payload].

    Length = 1 (opcode byte) + len(payload).
    """
    length = 1 + len(payload)
    return struct.pack(">I", length) + bytes([opcode]) + payload


def encode_string(s: str) -> bytes:
    """Encode a protocol string: [u16 BE length][UTF-8 bytes]."""
    s_bytes = s.encode("utf-8")
    return struct.pack(">H", len(s_bytes)) + s_bytes


# ---------------------------------------------------------------------------
# Command payload encoders
# ---------------------------------------------------------------------------


_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1

# Maximum byte length for any u16-length-prefixed wire field
# (stream_name, field key, or TYPE_STR value). Matches the Rust server
# decoder which reads a u16 BE length prefix.
_U16_MAX: int = 0xFFFF


def _check_u16_len(field: str, value_bytes: bytes) -> None:
    """Raise ProtocolError if ``value_bytes`` exceeds u16::MAX (65535).

    Provides a typed error for oversized fields instead of a raw
    struct.error from ``_U16.pack``. Called from ``encode_push_binary``
    before every u16 length write on the binary PUSH hot path.
    """
    if len(value_bytes) > _U16_MAX:
        raise ProtocolError(
            f"{field} exceeds {_U16_MAX} bytes: got {len(value_bytes)}"
        )


def _encode_event_body(event: dict) -> bytes:
    """Encode just the event fields (no stream_name prefix).

    Wire format: ``[u16 field_count][for each: [u16 key_len][key utf-8][u8 type_tag][value_bytes]]``

    This is the per-event payload inside an OP_PUSH_BATCH frame.
    Extracted from encode_push_binary (D-03: zero new serialization code).

    Type tags:

    - ``TYPE_NULL`` (0) — 0 value bytes
    - ``TYPE_BOOL`` (1) — 1 value byte (0 = false, 1 = true)
    - ``TYPE_I64`` (2)  — 8 value bytes (big-endian signed 64-bit)
    - ``TYPE_F64`` (3)  — 8 value bytes (big-endian IEEE754 double; NaN/Inf rejected)
    - ``TYPE_STR`` (4)  — ``[u16 BE len][utf-8]``

    Raises ``ProtocolError`` for unsupported value types, integers outside
    the signed 64-bit range, or non-finite floats.

    **Critical:** ``isinstance(value, bool)`` MUST come before
    ``isinstance(value, int)`` because ``bool`` is a subclass of ``int`` in
    Python. Otherwise ``True`` would encode as ``TYPE_I64`` and the
    server-side decoder would return an integer instead of a bool.
    """
    buf = bytearray()
    if len(event) > _U16_MAX:
        raise ProtocolError(
            f"event field_count exceeds {_U16_MAX}: got {len(event)}"
        )
    buf += _U16.pack(len(event))
    for key, value in event.items():
        key_bytes = key.encode("utf-8")
        _check_u16_len(f"field key {key!r}", key_bytes)
        buf += _U16.pack(len(key_bytes))
        buf += key_bytes
        if value is None:
            buf.append(TYPE_NULL)
        elif isinstance(value, bool):  # MUST come before int check
            buf.append(TYPE_BOOL)
            buf.append(0x01 if value else 0x00)
        elif isinstance(value, int):
            if value < _I64_MIN or value > _I64_MAX:
                raise ProtocolError(
                    f"integer field {key!r} out of i64 range: {value}"
                )
            buf.append(TYPE_I64)
            buf += _I64.pack(value)
        elif isinstance(value, float):
            if value != value or value == float("inf") or value == float("-inf"):
                raise ProtocolError(
                    f"float field {key!r} is not finite: {value}"
                )
            buf.append(TYPE_F64)
            buf += _F64.pack(value)
        elif isinstance(value, str):
            v_bytes = value.encode("utf-8")
            _check_u16_len(f"string value for key {key!r}", v_bytes)
            buf.append(TYPE_STR)
            buf += _U16.pack(len(v_bytes))
            buf += v_bytes
        else:
            raise ProtocolError(
                f"unsupported event field type for key {key!r}: {type(value).__name__}"
            )
    return bytes(buf)


def encode_push_binary(stream_name: str, event: dict) -> bytes:
    """Encode a PUSH payload in the Phase 11 binary format (PERF-02).

    Wire format matches the Rust ``decode_event_binary``:

    - ``[u16 BE name_len][name utf-8]``
    - ``[u16 BE field_count]``
    - For each field: ``[u16 BE key_len][key utf-8][u8 type_tag][value bytes]``

    Delegates to :func:`_encode_event_body` for the field encoding (D-03).
    """
    buf = bytearray()
    name_bytes = stream_name.encode("utf-8")
    _check_u16_len("stream_name", name_bytes)
    buf += _U16.pack(len(name_bytes))
    buf += name_bytes
    buf += _encode_event_body(event)
    return bytes(buf)


def encode_push_batch(stream_name: str, events, batch_id: int) -> bytes:
    """Encode an OP_PUSH_BATCH payload (D-02 wire format).

    Wire format: ``[u16 stream_len][stream][u32 batch_id][u32 count]``
                 ``[for each: [u32 event_len][event_bytes]]``

    events is an iterable of dicts. Each event is encoded via
    :func:`_encode_event_body` (D-03: reuse existing serialization).
    """
    buf = bytearray()
    name_bytes = stream_name.encode("utf-8")
    _check_u16_len("stream_name", name_bytes)
    buf += _U16.pack(len(name_bytes))
    buf += name_bytes
    buf += struct.pack(">II", batch_id, 0)  # batch_id + placeholder count
    count_offset = 2 + len(name_bytes) + 4  # position of the count field
    count = 0
    for event in events:
        event_bytes = _encode_event_body(event)
        buf += struct.pack(">I", len(event_bytes))
        buf += event_bytes
        count += 1
    # Patch the count field in-place
    struct.pack_into(">I", buf, count_offset, count)
    return bytes(buf)


def encode_get(key: str) -> bytes:
    """Encode GET payload: [u16-string key]."""
    return encode_string(key)


def encode_set(key: str, features: dict) -> bytes:
    """Encode SET payload: [u16-string key][JSON feature map bytes]."""
    return encode_string(key) + json.dumps(features).encode("utf-8")


def encode_mset(entries: dict[str, dict]) -> bytes:
    """Encode MSET payload: [u32 count][entries...].

    Each entry: [u16-string key][u32 json_len][json bytes].
    """
    parts = bytearray()
    parts.extend(struct.pack(">I", len(entries)))
    for key, features in entries.items():
        parts.extend(encode_string(key))
        json_bytes = json.dumps(features).encode("utf-8")
        parts.extend(struct.pack(">I", len(json_bytes)))
        parts.extend(json_bytes)
    return bytes(parts)


def encode_mget(keys: list[str]) -> bytes:
    """Encode MGET payload: [u32 count][u16-string key_1]...[u16-string key_n]."""
    parts = bytearray()
    parts.extend(struct.pack(">I", len(keys)))
    for key in keys:
        parts.extend(encode_string(key))
    return bytes(parts)


def encode_register(definition: dict) -> bytes:
    """Encode REGISTER payload: entire payload is JSON bytes."""
    return json.dumps(definition).encode("utf-8")


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


def parse_response(data: bytes) -> tuple[int, bytes]:
    """Parse a response frame: [4-byte BE length][status byte][payload].

    Returns (status, payload_bytes).

    Raises ProtocolError if:
    - Data is too short for the header
    - Frame length exceeds MAX_FRAME_SIZE
    - Data is truncated (fewer bytes than length claims)
    - Status is STATUS_ERROR (payload is the error message)
    """
    if len(data) < 4:
        raise ProtocolError("response too short: need at least 4 bytes for length header")

    length = struct.unpack(">I", data[:4])[0]

    if length > MAX_FRAME_SIZE:
        raise ProtocolError(
            f"frame too large: {length} bytes exceeds limit of {MAX_FRAME_SIZE}"
        )

    if len(data) < 4 + length:
        raise ProtocolError(
            f"response truncated: expected {length} bytes after header, got {len(data) - 4}"
        )

    if length < 1:
        raise ProtocolError("frame length must be at least 1 (status byte)")

    status = data[4]
    payload = data[5 : 4 + length]

    if status == STATUS_ERROR:
        raise ProtocolError(payload.decode("utf-8", errors="replace"))

    return status, payload
