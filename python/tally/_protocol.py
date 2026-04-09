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

STATUS_OK: int = 0x00
STATUS_ERROR: int = 0x01

# Maximum frame size (64 MB) -- reject before allocating buffer (DoS protection)
MAX_FRAME_SIZE: int = 64 * 1024 * 1024


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


def encode_push(stream_name: str, event: dict) -> bytes:
    """Encode PUSH payload: [u16-string stream_name][JSON event bytes]."""
    return encode_string(stream_name) + json.dumps(event).encode("utf-8")


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
