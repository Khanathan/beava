"""Tests for TallyClient TCP connection, frame I/O, auto-reconnect, and timeout."""

from __future__ import annotations

import json
import socket
import struct
import threading
import time

import pytest

from tally._client import TallyClient
from tally._protocol import MAX_FRAME_SIZE, STATUS_OK, STATUS_ERROR, encode_frame
from tally._types import ConnectionError, ProtocolError


# ---------------------------------------------------------------------------
# Helpers: minimal TCP mock server
# ---------------------------------------------------------------------------


def _make_response_frame(status: int, payload: bytes) -> bytes:
    """Build a response frame: [4-byte BE length][status][payload]."""
    length = 1 + len(payload)
    return struct.pack(">I", length) + bytes([status]) + payload


def _start_mock_server(
    handler,
    *,
    accept_count: int = 1,
    ready_event: threading.Event | None = None,
) -> tuple[int, threading.Event]:
    """Start a mock TCP server on a random port.

    ``handler`` is called with ``(conn, addr)`` for each accepted connection.
    ``accept_count`` is the number of connections to accept before the server exits.
    Returns ``(port, done_event)`` where ``done_event`` is set when the server thread finishes.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    port = srv.getsockname()[1]

    done = threading.Event()
    ready = ready_event or threading.Event()

    def _run():
        try:
            ready.set()
            for _ in range(accept_count):
                srv.settimeout(5.0)
                conn, addr = srv.accept()
                try:
                    handler(conn, addr)
                except Exception:
                    pass
                finally:
                    try:
                        conn.close()
                    except OSError:
                        pass
        finally:
            srv.close()
            done.set()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    ready.wait(timeout=5.0)
    return port, done


def _recv_exact(conn: socket.socket, n: int) -> bytes:
    """Read exactly *n* bytes from *conn*."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def _recv_frame(conn: socket.socket) -> tuple[int, bytes]:
    """Read one client frame: [4-byte length][opcode][payload]."""
    header = _recv_exact(conn, 4)
    length = struct.unpack(">I", header)[0]
    body = _recv_exact(conn, length)
    opcode = body[0]
    payload = body[1:]
    return opcode, payload


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTallyClientConnect:
    """send_command connects lazily and returns correct status and payload."""

    def test_send_command_returns_status_and_payload(self):
        response_payload = json.dumps({"tx_count": 7}).encode("utf-8")

        def handler(conn, _addr):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, response_payload))

        port, done = _start_mock_server(handler)
        client = TallyClient("127.0.0.1", port)
        try:
            status, payload = client.send_command(0x01, b"test-payload")
            assert status == STATUS_OK
            assert payload == response_payload
        finally:
            client.close()
            done.wait(timeout=2.0)

    def test_lazy_connect_no_socket_until_first_command(self):
        """TallyClient does NOT connect until send_command is called."""
        client = TallyClient("127.0.0.1", 9999)
        assert client._sock is None
        client.close()


class TestAutoReconnect:
    """After server closes connection, next send_command auto-reconnects."""

    def test_reconnect_after_server_disconnect(self):
        response_payload = b"ok"
        call_count = 0

        def handler(conn, _addr):
            nonlocal call_count
            call_count += 1
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, response_payload))
            # Server closes connection after first response.

        port, done = _start_mock_server(handler, accept_count=2)
        client = TallyClient("127.0.0.1", port)
        try:
            # First command succeeds.
            status1, _ = client.send_command(0x01, b"first")
            assert status1 == STATUS_OK

            # Small delay to let server close connection.
            time.sleep(0.05)

            # Second command should auto-reconnect.
            status2, _ = client.send_command(0x01, b"second")
            assert status2 == STATUS_OK
            assert call_count == 2
        finally:
            client.close()
            done.wait(timeout=2.0)

    def test_close_then_send_reconnects(self):
        """After explicit close(), next send_command reconnects."""
        response_payload = b"ok"

        def handler(conn, _addr):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, response_payload))

        port, done = _start_mock_server(handler, accept_count=2)
        client = TallyClient("127.0.0.1", port)
        try:
            status1, _ = client.send_command(0x01, b"first")
            assert status1 == STATUS_OK

            client.close()
            assert client._sock is None

            # Should reconnect.
            status2, _ = client.send_command(0x01, b"second")
            assert status2 == STATUS_OK
        finally:
            client.close()
            done.wait(timeout=2.0)


class TestTimeout:
    """Timeout is applied to the socket."""

    def test_timeout_applied_to_socket(self):
        def handler(conn, _addr):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, b"ok"))

        port, done = _start_mock_server(handler)
        client = TallyClient("127.0.0.1", port, timeout=2.0)
        try:
            client.send_command(0x01, b"check-timeout")
            assert client._sock is not None
            assert client._sock.gettimeout() == 2.0
        finally:
            client.close()
            done.wait(timeout=2.0)

    def test_default_timeout_is_five_seconds(self):
        def handler(conn, _addr):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, b"ok"))

        port, done = _start_mock_server(handler)
        client = TallyClient("127.0.0.1", port)
        try:
            client.send_command(0x01, b"check-default")
            assert client._sock.gettimeout() == 5.0
        finally:
            client.close()
            done.wait(timeout=2.0)


class TestRecvExact:
    """_recv_exact raises ConnectionError on EOF."""

    def test_recv_exact_raises_on_eof(self):
        def handler(conn, _addr):
            # Close immediately without sending anything.
            conn.close()

        port, done = _start_mock_server(handler)
        client = TallyClient("127.0.0.1", port)
        try:
            client._connect()
            with pytest.raises(ConnectionError, match="server closed connection"):
                client._recv_exact(10)
        finally:
            client.close()
            done.wait(timeout=2.0)


class TestOversizedFrame:
    """Oversized response frame (length > MAX_FRAME_SIZE) raises ProtocolError."""

    def test_oversized_frame_raises_protocol_error(self):
        def handler(conn, _addr):
            _recv_frame(conn)
            # Send a response with length > MAX_FRAME_SIZE.
            fake_length = MAX_FRAME_SIZE + 1
            conn.sendall(struct.pack(">I", fake_length))
            # Don't bother sending body -- client should reject after reading length.

        port, done = _start_mock_server(handler)
        client = TallyClient("127.0.0.1", port)
        try:
            with pytest.raises(ProtocolError, match="too large"):
                client.send_command(0x01, b"trigger-oversize")
        finally:
            client.close()
            done.wait(timeout=2.0)


class TestContextManager:
    """Context manager calls close on exit."""

    def test_context_manager_closes_socket(self):
        def handler(conn, _addr):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, b"ok"))

        port, done = _start_mock_server(handler)
        with TallyClient("127.0.0.1", port) as client:
            client.send_command(0x01, b"ctx")
            assert client._sock is not None

        # After context manager exit, socket should be None.
        assert client._sock is None
        done.wait(timeout=2.0)


# ---------------------------------------------------------------------------
# Phase 11: drain_errors_nonblock + send_frame_no_recv
# ---------------------------------------------------------------------------


class TestPhase11ClientPrimitives:
    """Tests for the fire-and-forget drain + no-recv send primitives."""

    def test_drain_errors_nonblock_sock_none(self):
        """Drain is a no-op when the socket has never been opened."""
        client = TallyClient("127.0.0.1", 9999)
        assert client._sock is None
        client.drain_errors_nonblock()
        # must NOT trigger a connect
        assert client._sock is None

    def test_drain_errors_nonblock_pending_error(self):
        """A stored pending error is raised and cleared on next drain."""
        client = TallyClient("127.0.0.1", 9999)
        client._pending_error = ProtocolError("previous")
        with pytest.raises(ProtocolError, match="previous"):
            client.drain_errors_nonblock()
        assert client._pending_error is None

    def test_drain_errors_nonblock_no_data(self):
        """Drain on a connected socket with nothing readable is a silent no-op."""
        a, b = socket.socketpair()
        try:
            client = TallyClient("", 0)
            client._sock = a
            client.drain_errors_nonblock()  # no data on b side
        finally:
            a.close()
            b.close()

    def test_drain_errors_nonblock_ok_frame_discarded(self):
        """A STATUS_OK frame (stray ACK) is silently consumed."""
        a, b = socket.socketpair()
        try:
            client = TallyClient("", 0)
            client._sock = a
            ok_frame = _make_response_frame(STATUS_OK, b"")
            b.sendall(ok_frame)
            time.sleep(0.01)
            client.drain_errors_nonblock()  # should not raise
        finally:
            a.close()
            b.close()

    def test_drain_errors_nonblock_error_frame(self):
        """A STATUS_ERROR frame is raised as ProtocolError with the message."""
        a, b = socket.socketpair()
        try:
            client = TallyClient("", 0)
            client._sock = a
            err_frame = _make_response_frame(STATUS_ERROR, b"boom")
            b.sendall(err_frame)
            time.sleep(0.01)
            with pytest.raises(ProtocolError, match="boom"):
                client.drain_errors_nonblock()
        finally:
            a.close()
            b.close()

    def test_send_frame_no_recv_sends_bytes(self):
        """send_frame_no_recv writes a full frame and does NOT read."""
        a, b = socket.socketpair()
        try:
            client = TallyClient("", 0)
            client._sock = a
            client.send_frame_no_recv(0x07, b"xyz")
            expected = encode_frame(0x07, b"xyz")
            received = b.recv(len(expected))
            assert received == expected
            # client should not have touched the read buffer
            assert client._sock is a
        finally:
            a.close()
            b.close()

    def test_send_frame_no_recv_does_not_block_on_recv(self):
        """Ensure send_frame_no_recv returns immediately without reading a response."""
        a, b = socket.socketpair()
        try:
            client = TallyClient("", 0)
            client._sock = a
            # No response ever sent from b; this must still return.
            start = time.perf_counter()
            client.send_frame_no_recv(0x07, b"hi")
            elapsed = time.perf_counter() - start
            assert elapsed < 0.5, f"send_frame_no_recv blocked for {elapsed}s"
        finally:
            a.close()
            b.close()
