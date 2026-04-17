"""Phase 43 T4: server_busy backpressure surfaces as ServerBusyError.

When the server's memory ceiling gate is set, PUSH/PUSH_BATCH responses
return STATUS_ERROR with a message prefixed `server_busy:`. The SDK
must raise :class:`ServerBusyError` (a :class:`ProtocolError` subclass)
so callers can distinguish from generic protocol errors and apply an
application-level backoff instead of the transport-level retry loop.
"""

from __future__ import annotations

import socket
import struct
import threading

import pytest

import beava as bv
from beava._app import App
from beava._protocol import STATUS_ERROR, STATUS_OK
from beava._retry import NO_RETRY
from beava._types import ProtocolError, ServerBusyError


def _make_response_frame(status: int, payload: bytes) -> bytes:
    length = 1 + len(payload)
    return struct.pack(">I", length) + bytes([status]) + payload


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def _start_server(handler) -> tuple[int, threading.Event]:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    ready = threading.Event()
    done = threading.Event()

    def _run():
        try:
            ready.set()
            srv.settimeout(5.0)
            conn, addr = srv.accept()
            try:
                handler(conn, addr)
            finally:
                conn.close()
        finally:
            srv.close()
            done.set()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    ready.wait(timeout=5.0)
    return port, done


def _stub_push_response(conn: socket.socket, payload: bytes) -> None:
    """Consume one client frame, send the given payload as a STATUS_ERROR response."""
    header = _recv_exact(conn, 4)
    length = struct.unpack(">I", header)[0]
    _recv_exact(conn, length)  # opcode + payload
    conn.sendall(_make_response_frame(STATUS_ERROR, payload))


def test_server_busy_prefix_raises_server_busy_error():
    """STATUS_ERROR body starting with `server_busy:` raises ServerBusyError."""

    def handler(conn, _addr):
        _stub_push_response(
            conn,
            b"server_busy: memory_limit_exceeded - writes rejected until "
            b"RSS falls below 95% of BEAVA_MEMORY_LIMIT_MB",
        )

    port, done = _start_server(handler)
    try:
        app = App(f"127.0.0.1:{port}", retry_policy=NO_RETRY)
        with pytest.raises(ServerBusyError) as excinfo:
            app._send(0x01, b"any-payload")
        assert "memory_limit_exceeded" in str(excinfo.value)
        # ServerBusyError must also be a ProtocolError so existing
        # `except ProtocolError` handlers keep catching it.
        assert isinstance(excinfo.value, ProtocolError)
    finally:
        done.wait(timeout=2.0)


def test_plain_error_still_raises_protocol_error_not_server_busy():
    """A STATUS_ERROR without the `server_busy:` prefix must NOT be
    upgraded to ServerBusyError — callers would mis-diagnose it."""

    def handler(conn, _addr):
        _stub_push_response(conn, b"some other protocol error, unrelated to memory")

    port, done = _start_server(handler)
    try:
        app = App(f"127.0.0.1:{port}", retry_policy=NO_RETRY)
        with pytest.raises(ProtocolError) as excinfo:
            app._send(0x01, b"any-payload")
        # Not a ServerBusyError, just a ProtocolError.
        assert not isinstance(excinfo.value, ServerBusyError)
    finally:
        done.wait(timeout=2.0)


def test_server_busy_error_is_exported_on_public_surface():
    assert bv.ServerBusyError is ServerBusyError
    assert issubclass(bv.ServerBusyError, bv.ProtocolError)
