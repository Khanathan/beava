"""Tests for beava._transport.TcpTransport.

These tests require a running beava server via the `beava_server` fixture.
They are expected to FAIL (ImportError) until python/beava/_transport.py is
created in Task 1.b.
"""

from __future__ import annotations

import pytest

from beava._errors import RegistrationError
from beava._transport import TcpTransport

# A minimal valid event registration payload (matches Phase 12.6-06 wire contract).
# Plan 12.6-06 D-03 hard rip: `event_time_field` and `tolerate_delay_ms` keys
# removed from EventDescriptor; sending them now raises a structured 400 with
# `unknown_field_event_time_v0` / `unknown_field_tolerate_delay_v0`.
VALID_REGISTER_PAYLOAD = (
    b'{"nodes":[{'
    b'"kind":"event",'
    b'"name":"TcpTestEvent",'
    b'"schema":{"fields":{"event_time":"i64","amount":"f64"},"optional_fields":[]},'
    b'"dedupe_key":null,"dedupe_window_ms":null,'
    b'"keep_events_for_ms":null'
    b"}]}"
)

# Payload that uses a reserved _beava_ prefix — server returns invalid_registration.
INVALID_REGISTER_PAYLOAD = (
    b'{"nodes":[{'
    b'"kind":"event",'
    b'"name":"_beava_reserved",'
    b'"schema":{"fields":{"x":"f64"},"optional_fields":[]},'
    b'"dedupe_key":null,"dedupe_window_ms":null,'
    b'"keep_events_for_ms":null'
    b"}]}"
)


def _make_tcp(tcp_url: str) -> TcpTransport:
    """Parse tcp://host:port and return a TcpTransport."""
    # strip "tcp://"
    host_port = tcp_url[len("tcp://"):]
    host, port_str = host_port.rsplit(":", 1)
    return TcpTransport(host=host, port=int(port_str))


class TestTcpTransportPing:
    def test_tcp_transport_ping(self, beava_server: tuple[str, str]) -> None:
        """send_ping() returns dict with 'server_version' and 'registry_version'."""
        _, tcp_url = beava_server
        with _make_tcp(tcp_url) as t:
            resp = t.send_ping()
        assert "server_version" in resp
        assert "registry_version" in resp
        assert isinstance(resp["registry_version"], int)
        assert resp["registry_version"] >= 0


class TestTcpTransportRegister:
    def test_tcp_transport_register_success(self, beava_server: tuple[str, str]) -> None:
        """Successful TCP register returns dict with registry_version >= 1."""
        _, tcp_url = beava_server
        with _make_tcp(tcp_url) as t:
            result = t.send_register(VALID_REGISTER_PAYLOAD)
        assert result["registry_version"] >= 1
        assert result["status"] == "ok"

    def test_tcp_transport_register_validation_error(
        self, beava_server: tuple[str, str]
    ) -> None:
        """Reserved-name payload raises RegistrationError(code='invalid_registration')."""
        _, tcp_url = beava_server
        with _make_tcp(tcp_url) as t:
            with pytest.raises(RegistrationError) as exc_info:
                t.send_register(INVALID_REGISTER_PAYLOAD)
        assert exc_info.value.code == "invalid_registration"


class TestTcpTransportConnectionReuse:
    def test_tcp_transport_connection_reuse(self, beava_server: tuple[str, str]) -> None:
        """Single TcpTransport calls send_ping() three times; socket id stays stable."""
        _, tcp_url = beava_server
        t = _make_tcp(tcp_url)
        try:
            r1 = t.send_ping()
            sock_id_1 = id(t._socket)
            r2 = t.send_ping()
            sock_id_2 = id(t._socket)
            r3 = t.send_ping()
            sock_id_3 = id(t._socket)
        finally:
            t.close()

        # All pings should succeed and return consistent registry_version
        for resp in (r1, r2, r3):
            assert "registry_version" in resp

        # Socket object identity must be stable (no reconnect between calls)
        assert sock_id_1 == sock_id_2 == sock_id_3, "socket was replaced between ping calls"


class TestTcpTransportStrictFifo:
    def test_tcp_transport_strict_fifo(self, beava_server: tuple[str, str]) -> None:
        """Two pings sent back-to-back; responses arrive in FIFO order.

        Phase 2.5 Criterion 4: strict-FIFO per-connection pipelining.
        We send two ping frames in one sendall then read two responses — both
        must be OP_PING responses.
        """
        import socket

        from beava._wire import CT_JSON, OP_PING, encode_frame, read_frame

        _, tcp_url = beava_server
        host_port = tcp_url[len("tcp://"):]
        host, port_str = host_port.rsplit(":", 1)

        sock = socket.create_connection((host, int(port_str)), timeout=10.0)
        try:
            # Pipeline: send two frames in one syscall
            frame1 = encode_frame(op=OP_PING, ct=CT_JSON, payload=b"{}")
            frame2 = encode_frame(op=OP_PING, ct=CT_JSON, payload=b"{}")
            sock.sendall(frame1 + frame2)

            resp1 = read_frame(sock)
            resp2 = read_frame(sock)
        finally:
            sock.close()

        assert resp1.op == OP_PING, f"first response op={resp1.op:#x}, expected OP_PING"
        assert resp2.op == OP_PING, f"second response op={resp2.op:#x}, expected OP_PING"


class TestTcpTransportContextManager:
    def test_tcp_transport_close_on_context_exit(self, beava_server: tuple[str, str]) -> None:
        """Context-manager exit closes the socket."""
        _, tcp_url = beava_server
        t = _make_tcp(tcp_url)
        with t:
            t.send_ping()
            assert t._socket is not None

        # After __exit__, socket should be None (closed)
        assert t._socket is None, "socket should be None after context manager exit"
