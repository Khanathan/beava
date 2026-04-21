"""Phase 59 Wave 3 D-B1 / D-E4: Python SDK wire-format handshake tests.

Unit + mock-server tests for the ``OP_NEGOTIATE_WIRE_FORMAT`` client helper.
Covers:
  1. Constants are defined with the expected values.
  2. ``BeavaClient.negotiate_wire_format()`` against a Phase 59+ mock server
     returns ``(1, 2)`` and caches the bits + version.
  3. Against a pre-59 mock server that replies STATUS_ERROR "unknown opcode":
     returns ``(0, 0)`` sentinel and does NOT raise (D-E4).
  4. Against a server that returns a truncated OK body: returns ``(0, 0)``
     sentinel (defensive; never crashes).
  5. ``BEAVA_WIRE_NEGOTIATE=1`` env triggers auto-handshake in ``__init__``;
     absent env → no auto-call (``server_capability_bits is None``).

All tests use an in-process mock server socket — no external beava server
required.
"""

from __future__ import annotations

import os
import socket
import struct
import threading

import pytest

from beava._client import BeavaClient
from beava._protocol import (
    OP_NEGOTIATE_WIRE_FORMAT,
    STATUS_ERROR,
    STATUS_OK,
    WIRE_BINARY_PASSTHROUGH,
    WIRE_VERSION_TAG_CLIENT,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_op_negotiate_wire_format_constant() -> None:
    assert OP_NEGOTIATE_WIRE_FORMAT == 0x18


def test_wire_binary_passthrough_is_bit_zero() -> None:
    assert WIRE_BINARY_PASSTHROUGH == 1


def test_wire_version_tag_client_is_2() -> None:
    assert WIRE_VERSION_TAG_CLIENT == 2


# ---------------------------------------------------------------------------
# Mock-server helpers
# ---------------------------------------------------------------------------


def _bind_ephemeral() -> tuple[socket.socket, int]:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    return srv, port


def _run_phase59_mock(srv: socket.socket) -> None:
    """Phase 59+ mock server: receives OP_NEGOTIATE, replies STATUS_OK + bits."""
    conn, _ = srv.accept()
    try:
        # Read frame: [u32 BE len][u8 opcode][payload]
        hdr = conn.recv(4)
        length = struct.unpack(">I", hdr)[0]
        frame = b""
        while len(frame) < length:
            frame += conn.recv(length - len(frame))
        opcode = frame[0]
        assert opcode == OP_NEGOTIATE_WIRE_FORMAT
        # Ignore body; just respond as a real Phase 59 server would.
        body = struct.pack(">IH", WIRE_BINARY_PASSTHROUGH, 2)
        resp = struct.pack(">I", 1 + len(body)) + bytes([STATUS_OK]) + body
        conn.sendall(resp)
    finally:
        conn.close()
        srv.close()


def _run_pre59_mock(srv: socket.socket) -> None:
    """Pre-59 mock server: rejects 0x18 as unknown opcode."""
    conn, _ = srv.accept()
    try:
        hdr = conn.recv(4)
        length = struct.unpack(">I", hdr)[0]
        frame = b""
        while len(frame) < length:
            frame += conn.recv(length - len(frame))
        err = b"unknown opcode: 0x18"
        resp = struct.pack(">I", 1 + len(err)) + bytes([STATUS_ERROR]) + err
        conn.sendall(resp)
    finally:
        conn.close()
        srv.close()


def _run_truncated_ok_mock(srv: socket.socket) -> None:
    """Server returns STATUS_OK with a body shorter than 6 bytes (broken)."""
    conn, _ = srv.accept()
    try:
        hdr = conn.recv(4)
        length = struct.unpack(">I", hdr)[0]
        frame = b""
        while len(frame) < length:
            frame += conn.recv(length - len(frame))
        body = b"\x00\x01"  # 2 bytes, need 6
        resp = struct.pack(">I", 1 + len(body)) + bytes([STATUS_OK]) + body
        conn.sendall(resp)
    finally:
        conn.close()
        srv.close()


# ---------------------------------------------------------------------------
# negotiate_wire_format() scenarios
# ---------------------------------------------------------------------------


def test_negotiate_wire_format_against_phase59_server() -> None:
    srv, port = _bind_ephemeral()
    thread = threading.Thread(target=_run_phase59_mock, args=(srv,), daemon=True)
    thread.start()
    try:
        # Ensure env flag does NOT trigger auto-negotiate in __init__ during
        # this manual call path.
        os.environ.pop("BEAVA_WIRE_NEGOTIATE", None)
        c = BeavaClient("127.0.0.1", port, timeout=2.0)
        bits, ver = c.negotiate_wire_format()
        assert bits == WIRE_BINARY_PASSTHROUGH
        assert ver == 2
        assert c.server_capability_bits == WIRE_BINARY_PASSTHROUGH
        assert c.server_version_tag == 2
        c.close()
    finally:
        thread.join(timeout=2.0)


def test_negotiate_wire_format_against_pre59_server_falls_back_silently() -> None:
    srv, port = _bind_ephemeral()
    thread = threading.Thread(target=_run_pre59_mock, args=(srv,), daemon=True)
    thread.start()
    try:
        os.environ.pop("BEAVA_WIRE_NEGOTIATE", None)
        c = BeavaClient("127.0.0.1", port, timeout=2.0)
        # Must NOT raise — D-E4 contract.
        bits, ver = c.negotiate_wire_format()
        assert bits == 0
        assert ver == 0
        assert c.server_capability_bits == 0
        assert c.server_version_tag == 0
        c.close()
    finally:
        thread.join(timeout=2.0)


def test_negotiate_wire_format_against_truncated_ok_response_falls_back() -> None:
    srv, port = _bind_ephemeral()
    thread = threading.Thread(target=_run_truncated_ok_mock, args=(srv,), daemon=True)
    thread.start()
    try:
        os.environ.pop("BEAVA_WIRE_NEGOTIATE", None)
        c = BeavaClient("127.0.0.1", port, timeout=2.0)
        bits, ver = c.negotiate_wire_format()
        # Defensive fall-back: truncated body = same as pre-59 sentinel.
        assert bits == 0
        assert ver == 0
        c.close()
    finally:
        thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# BEAVA_WIRE_NEGOTIATE env opt-in
# ---------------------------------------------------------------------------


def test_env_opt_in_triggers_auto_negotiate_on_connect() -> None:
    srv, port = _bind_ephemeral()
    thread = threading.Thread(target=_run_phase59_mock, args=(srv,), daemon=True)
    thread.start()
    try:
        os.environ["BEAVA_WIRE_NEGOTIATE"] = "1"
        try:
            c = BeavaClient("127.0.0.1", port, timeout=2.0)
            # __init__ auto-called negotiate — cache is populated without
            # a manual call.
            assert c.server_capability_bits == WIRE_BINARY_PASSTHROUGH
            assert c.server_version_tag == 2
            c.close()
        finally:
            os.environ.pop("BEAVA_WIRE_NEGOTIATE", None)
    finally:
        thread.join(timeout=2.0)


def test_default_off_no_auto_negotiate() -> None:
    # No mock server needed — the client MUST NOT attempt a connection
    # during __init__ when the env flag is off. Any socket activity would
    # ECONNREFUSED on an unbound port and surface here.
    os.environ.pop("BEAVA_WIRE_NEGOTIATE", None)
    c = BeavaClient("127.0.0.1", 1, timeout=0.1)
    assert c.server_capability_bits is None
    assert c.server_version_tag is None
    c.close()
