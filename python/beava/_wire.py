"""Wire protocol codec for Beava's binary-framed TCP transport.

Frame envelope (v0, locked 2026-04-23 — matches crates/beava-core/src/wire.rs):

    [u32 length BE][u16 op BE][u8 content_type][payload: length - 3 bytes]

``length`` = bytes of ``op + content_type + payload`` (not counting the length field
itself).  Minimum valid length is 3 (empty payload).  All multi-byte integers are
big-endian (network byte order).

Opcode constants (v0 client-initiated set per docs/wire-spec.md § Opcode Table):

    OP_PING           = 0x0000
    OP_REGISTER       = 0x0001
    OP_PUSH           = 0x0010  (data-plane push — fixed Phase 13.5 Plan 01)
    OP_GET            = 0x0020
    OP_GET_RESPONSE   = 0x0023
    OP_BATCH_GET      = 0x0024
    OP_RESET          = 0x0040  (test_mode-gated; Phase 13.4 D-03)
    OP_ERROR_RESPONSE = 0xFFFF

Content types (Phase 3 JSON-only; CT_MSGPACK reserved Phase 6+):

    CT_JSON    = 0x01
    CT_MSGPACK = 0x02  (reserved)
"""

from __future__ import annotations

import json
import socket
import struct
from dataclasses import dataclass
from typing import Any, cast

# ─── Opcode constants ────────────────────────────────────────────────────────

OP_PING: int = 0x0000
OP_REGISTER: int = 0x0001
OP_PUSH: int = 0x0010  # data-plane push (Phase 13.5 Plan 01 — fixed from 0x0002)

# Plan 12-07 / 12-09 + Phase 13.4: get / batch-get / reset opcodes.
OP_GET: int = 0x0020  # single feature, single key
OP_MGET: int = 0x0021  # single feature, many keys
OP_GET_MULTI: int = 0x0022  # many features, many keys
OP_GET_RESPONSE: int = 0x0023  # response opcode (Plan 12-07)
OP_BATCH_GET: int = 0x0024  # batch-get (Phase 13.4 wire spec)
OP_RESET: int = 0x0040  # test-mode reset (Phase 13.4 D-03)

OP_ERROR_RESPONSE: int = 0xFFFF

# ─── Content-type constants ──────────────────────────────────────────────────

CT_JSON: int = 0x01
CT_MSGPACK: int = 0x02  # activated Phase 18-09

# ─── Codec limits ────────────────────────────────────────────────────────────

#: Default max payload size — matches server default DEFAULT_TCP_MAX_FRAME_BYTES = 4 MiB.
MAX_FRAME_BYTES: int = 4 * 1024 * 1024


# ─── Exceptions ──────────────────────────────────────────────────────────────

class FrameTooLarge(Exception):
    """Raised when a declared frame length exceeds MAX_FRAME_BYTES.

    Message always contains 'too_large' so test_decode_too_large_raises_frame_too_large
    can assert on the string.
    """


class IncompleteFrame(Exception):
    """Raised when the buffer does not contain a complete frame."""


# ─── Frame dataclass ─────────────────────────────────────────────────────────

@dataclass
class Frame:
    """A decoded TCP wire frame."""

    op: int
    ct: int
    payload: bytes


# ─── Codec ───────────────────────────────────────────────────────────────────

def encode_frame(op: int, ct: int, payload: bytes) -> bytes:
    """Encode a frame to bytes.

    Layout: [u32 length BE][u16 op BE][u8 ct][payload]
    where length = 2 (op) + 1 (ct) + len(payload).

    Args:
        op: Opcode (u16).
        ct: Content type (u8).
        payload: Raw payload bytes.

    Returns:
        Complete frame bytes ready for sendall().
    """
    length = 2 + 1 + len(payload)
    header = struct.pack(">IHB", length, op, ct)
    return header + payload


def decode_frame(buf: bytes, max_frame_bytes: int = MAX_FRAME_BYTES) -> Frame:
    """Decode a frame from a bytes buffer.

    The buffer must contain the full frame (header + payload).  Use
    :func:`read_frame` to read incrementally from a socket.

    Args:
        buf: Bytes containing at least one complete frame (may have trailing data).
        max_frame_bytes: Maximum allowed payload size.  Defaults to MAX_FRAME_BYTES (4 MiB).

    Returns:
        Decoded :class:`Frame`.

    Raises:
        IncompleteFrame: Buffer is too short to decode a complete frame.
        FrameTooLarge: Declared length exceeds ``max_frame_bytes``.
    """
    # Need at least 4 bytes for the length prefix
    if len(buf) < 4:
        raise IncompleteFrame(f"need >=4 bytes for length prefix; got {len(buf)}")

    (length,) = struct.unpack(">I", buf[:4])

    # Guard against malformed frames that claim fewer bytes than op+ct require.
    # Mirrors Rust server FrameError::LengthUnderflow (wire.rs line ~215).
    if length < 3:
        raise IncompleteFrame(
            f"frame length {length} < 3: cannot cover op(2) + content_type(1)"
        )

    # Check declared length against limit (limit includes op+ct overhead)
    limit = max_frame_bytes + 3  # 3 = op(2) + ct(1)
    if length > limit:
        raise FrameTooLarge(
            f"frame too_large: declared_length={length} exceeds limit={limit} "
            f"(max_frame_bytes={max_frame_bytes})"
        )

    # Need 4 (len) + length bytes total
    total_needed = 4 + length
    if len(buf) < total_needed:
        raise IncompleteFrame(
            f"need {total_needed} bytes; got {len(buf)} "
            f"(declared_length={length})"
        )

    op = struct.unpack(">H", buf[4:6])[0]
    ct = buf[6]
    payload = buf[7:total_needed]
    return Frame(op=op, ct=ct, payload=payload)


def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    """Read exactly ``n`` bytes from ``sock``, looping until all arrive."""
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise IncompleteFrame(
                f"socket closed after {n - remaining}/{n} bytes"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket, max_frame_bytes: int = MAX_FRAME_BYTES) -> Frame:
    """Read exactly one frame from a connected socket.

    Blocks until the complete frame is available.  Handles partial reads
    by looping over ``socket.recv``.

    Args:
        sock: Connected TCP socket.
        max_frame_bytes: Maximum allowed payload size.

    Returns:
        Decoded :class:`Frame`.

    Raises:
        IncompleteFrame: Socket closed before a complete frame was received.
        FrameTooLarge: Declared length exceeds ``max_frame_bytes``.
    """
    # Read the 4-byte length prefix
    len_bytes = _recv_exactly(sock, 4)
    (length,) = struct.unpack(">I", len_bytes)

    # Guard against malformed frames that claim fewer bytes than op+ct require.
    # Mirrors Rust server FrameError::LengthUnderflow (wire.rs line ~215).
    if length < 3:
        raise IncompleteFrame(
            f"frame length {length} < 3: cannot cover op(2) + content_type(1)"
        )

    limit = max_frame_bytes + 3
    if length > limit:
        raise FrameTooLarge(
            f"frame too_large: declared_length={length} exceeds limit={limit} "
            f"(max_frame_bytes={max_frame_bytes})"
        )

    # Read the rest of the frame: op(2) + ct(1) + payload(length-3)
    rest = _recv_exactly(sock, length)
    op = struct.unpack(">H", rest[:2])[0]
    ct = rest[2]
    payload = rest[3:]
    return Frame(op=op, ct=ct, payload=payload)


# ─── Response parsing ────────────────────────────────────────────────────────

def parse_register_response(frame: Frame) -> dict:  # type: ignore[type-arg]
    """Parse a register response frame into a dict or raise RegistrationError.

    Args:
        frame: Response frame from the server.

    Returns:
        Parsed JSON body dict on success (OP_REGISTER with status='ok').

    Raises:
        RegistrationError: Server returned OP_ERROR_RESPONSE or an unexpected op.
    """
    # Import here to avoid circular import (_errors ← _wire ← _transport)
    from beava._errors import RegistrationError

    body: Any = json.loads(frame.payload.decode("utf-8"))

    if frame.op == OP_REGISTER:
        # Success or non-fatal (e.g., noop)
        return cast(dict[str, Any], body)

    if frame.op == OP_ERROR_RESPONSE:
        error = body.get("error", {})
        raise RegistrationError(
            code=error.get("code", "unknown"),
            path=error.get("path", ""),
            message=error.get("reason") or error.get("message", ""),
            errors=[],
        )

    raise RegistrationError(
        code="unexpected_frame",
        message=f"expected OP_REGISTER (0x0001) or OP_ERROR_RESPONSE (0xFFFF); "
                f"got op={frame.op:#06x}",
    )
