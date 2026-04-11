"""Tests for the App class: register, push, get, set, mset."""

from __future__ import annotations

import json
import socket
import struct
import threading

import pytest

from tally._app import App
from tally._protocol import (
    MAX_FRAME_SIZE,
    OP_GET,
    OP_MGET,
    OP_MSET,
    OP_PUSH,
    OP_REGISTER,
    OP_SET,
    STATUS_ERROR,
    STATUS_OK,
)
from tally._types import FeatureResult, ProtocolError

import tally as st


# ---------------------------------------------------------------------------
# Helpers: mock TCP server
# ---------------------------------------------------------------------------


def _make_response_frame(status: int, payload: bytes) -> bytes:
    """Build a response frame: [4-byte BE length][status][payload]."""
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


def _recv_frame(conn: socket.socket) -> tuple[int, bytes]:
    """Read one client frame: [4-byte length][opcode][payload]."""
    header = _recv_exact(conn, 4)
    length = struct.unpack(">I", header)[0]
    body = _recv_exact(conn, length)
    opcode = body[0]
    payload = body[1:]
    return opcode, payload


def _start_mock_server(handler, *, accept_count: int = 1) -> tuple[int, threading.Event]:
    """Start a mock TCP server returning (port, done_event)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    port = srv.getsockname()[1]
    done = threading.Event()
    ready = threading.Event()

    def _run():
        try:
            ready.set()
            for _ in range(accept_count):
                srv.settimeout(5.0)
                conn, _ = srv.accept()
                try:
                    handler(conn)
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


# ---------------------------------------------------------------------------
# Sample stream/view classes for testing
# ---------------------------------------------------------------------------


@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")
    rate = st.derive("tx_sum_1h / tx_count_1h")


@st.view(key="user_id")
class UserRisk:
    score = st.derive("Transactions.tx_count_1h > 10")


# ---------------------------------------------------------------------------
# Tests: address parsing
# ---------------------------------------------------------------------------


class TestAddressParsing:
    def test_host_and_port(self):
        host, port = App._parse_address("localhost:6400")
        assert host == "localhost"
        assert port == 6400

    def test_default_port(self):
        host, port = App._parse_address("localhost")
        assert host == "localhost"
        assert port == 6400

    def test_custom_port(self):
        host, port = App._parse_address("127.0.0.1:9999")
        assert host == "127.0.0.1"
        assert port == 9999


# ---------------------------------------------------------------------------
# Tests: register
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_sends_register_frame(self):
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(_make_response_frame(STATUS_OK, b""))

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            app.register(Transactions)

        done.wait(timeout=2.0)
        assert received["opcode"] == OP_REGISTER
        reg_json = json.loads(received["payload"])
        assert reg_json["name"] == "Transactions"
        assert reg_json["key_field"] == "user_id"
        assert len(reg_json["features"]) == 3

    def test_register_multiple_classes(self):
        call_count = 0

        def handler(conn):
            nonlocal call_count
            # Handle two REGISTER commands on the same connection.
            for _ in range(2):
                _recv_frame(conn)
                conn.sendall(_make_response_frame(STATUS_OK, b""))
                call_count += 1

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            app.register(Transactions, UserRisk)

        done.wait(timeout=2.0)
        assert call_count == 2

    def test_register_error_raises_protocol_error(self):
        error_msg = "unknown feature type"

        def handler(conn):
            _recv_frame(conn)
            conn.sendall(
                _make_response_frame(STATUS_ERROR, error_msg.encode("utf-8"))
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            with pytest.raises(ProtocolError, match=error_msg):
                app.register(Transactions)

        done.wait(timeout=2.0)


# ---------------------------------------------------------------------------
# Tests: push
# ---------------------------------------------------------------------------


class TestPush:
    def test_push_returns_none(self):
        """Phase 11: fire-and-forget push() returns None."""
        def handler(conn):
            opcode, _ = _recv_frame(conn)
            # No response written — push() does not read
            assert opcode == 0x07  # OP_PUSH_ASYNC

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.push(Transactions, {"user_id": "u1", "amount": 50.0})
        done.wait(timeout=2.0)
        assert result is None

    def test_push_sync_sends_push_frame_and_returns_feature_result(self):
        """Phase 11: push_sync preserves v1.1 inline-response semantics using binary encoder."""
        features = {"tx_count_1h": 7, "tx_sum_1h": 350.0, "rate": 50.0}
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(
                _make_response_frame(STATUS_OK, json.dumps(features).encode("utf-8"))
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.push_sync(Transactions, {"user_id": "u1", "amount": 50.0})

        done.wait(timeout=2.0)
        assert received["opcode"] == OP_PUSH
        assert isinstance(result, FeatureResult)
        assert result.tx_count_1h == 7
        assert result.tx_sum_1h == 350.0
        assert result.rate == 50.0

    def test_push_sync_payload_contains_stream_name(self):
        """Phase 11: binary-encoded push_sync payload still starts with [u16 len][name]."""
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["payload"] = payload
            conn.sendall(
                _make_response_frame(STATUS_OK, json.dumps({}).encode("utf-8"))
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            app.push_sync(Transactions, {"user_id": "u1"})

        done.wait(timeout=2.0)
        # PUSH payload starts with [u16 stream_name_len][stream_name bytes]
        payload = received["payload"]
        name_len = struct.unpack(">H", payload[:2])[0]
        stream_name = payload[2 : 2 + name_len].decode("utf-8")
        assert stream_name == "Transactions"

    def test_flush_sends_op_flush_and_waits_for_ack(self):
        """Phase 11: flush() sends OP_FLUSH and blocks until STATUS_OK."""
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(_make_response_frame(STATUS_OK, b""))

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            app.flush()
        done.wait(timeout=2.0)
        assert received["opcode"] == 0x08  # OP_FLUSH
        assert received["payload"] == b""

    def test_error_on_next_call_after_bad_async(self):
        """Phase 11: error from a prior async push surfaces on the next call via drain."""
        events = {}

        def handler(conn):
            # Accept the async push, then proactively send a STATUS_ERROR frame
            opcode, _ = _recv_frame(conn)
            events["first_opcode"] = opcode
            conn.sendall(_make_response_frame(STATUS_ERROR, b"bad async"))
            # Give the client a moment to drain
            try:
                # Should never receive a second frame because drain raises
                conn.settimeout(0.5)
                _recv_frame(conn)
            except (socket.timeout, OSError):
                pass

        port, done = _start_mock_server(handler)
        app = App(f"127.0.0.1:{port}")
        try:
            # Fire-and-forget push triggers the handler which queues the error
            app.push(Transactions, {"user_id": "u1"})
            # Wait briefly for the server's error frame to arrive
            import time as _t
            _t.sleep(0.1)
            # Next call must surface the error via drain_errors_nonblock
            raised = False
            try:
                app.flush()
            except ProtocolError as e:
                raised = True
                assert "bad async" in str(e)
            assert raised, "pending async error did not surface on next call"
        finally:
            app.close()
        done.wait(timeout=2.0)


# ---------------------------------------------------------------------------
# Tests: get
# ---------------------------------------------------------------------------


class TestGet:
    def test_get_returns_feature_result(self):
        features = {"tx_count_1h": 3, "lifetime_value": 4500.0}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            assert opcode == OP_GET
            conn.sendall(
                _make_response_frame(STATUS_OK, json.dumps(features).encode("utf-8"))
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.get("u123")

        done.wait(timeout=2.0)
        assert isinstance(result, FeatureResult)
        assert result.tx_count_1h == 3
        assert result.lifetime_value == 4500.0

    def test_get_unknown_key_returns_empty_feature_result(self):
        def handler(conn):
            _recv_frame(conn)
            conn.sendall(
                _make_response_frame(STATUS_OK, json.dumps({}).encode("utf-8"))
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.get("unknown_key")

        done.wait(timeout=2.0)
        assert isinstance(result, FeatureResult)
        assert result.to_dict() == {}


# ---------------------------------------------------------------------------
# Tests: set
# ---------------------------------------------------------------------------


class TestSet:
    def test_set_sends_set_frame(self):
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(_make_response_frame(STATUS_OK, b""))

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.set("u123", {"lifetime_value": 4500.0})

        done.wait(timeout=2.0)
        assert received["opcode"] == OP_SET
        assert result is None

        # Verify payload structure: [u16 key_len][key bytes][JSON bytes]
        payload = received["payload"]
        key_len = struct.unpack(">H", payload[:2])[0]
        key = payload[2 : 2 + key_len].decode("utf-8")
        json_part = json.loads(payload[2 + key_len :])
        assert key == "u123"
        assert json_part == {"lifetime_value": 4500.0}


# ---------------------------------------------------------------------------
# Tests: mset
# ---------------------------------------------------------------------------


class TestMset:
    def test_mset_sends_mset_frame(self):
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(_make_response_frame(STATUS_OK, b""))

        port, done = _start_mock_server(handler)
        entries = {
            "u1": {"lifetime_value": 100.0},
            "u2": {"lifetime_value": 200.0},
        }
        with App(f"127.0.0.1:{port}") as app:
            result = app.mset(entries)

        done.wait(timeout=2.0)
        assert received["opcode"] == OP_MSET
        assert result is None

        # Verify MSET payload: [u32 count][entries...]
        payload = received["payload"]
        count = struct.unpack(">I", payload[:4])[0]
        assert count == 2


# ---------------------------------------------------------------------------
# Tests: mget
# ---------------------------------------------------------------------------


class TestMget:
    def test_mget_sends_mget_frame_and_returns_dict(self):
        response_data = {
            "k1": {"tx_count_1h": 5, "tx_sum_1h": 100.0},
            "k2": {"tx_count_1h": 3, "tx_sum_1h": 50.0},
        }
        received = {}

        def handler(conn):
            opcode, payload = _recv_frame(conn)
            received["opcode"] = opcode
            received["payload"] = payload
            conn.sendall(
                _make_response_frame(
                    STATUS_OK, json.dumps(response_data).encode("utf-8")
                )
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.mget(["k1", "k2"])

        done.wait(timeout=2.0)
        assert received["opcode"] == OP_MGET

        # Verify payload: [u32 count=2][u16-string "k1"][u16-string "k2"]
        payload = received["payload"]
        count = struct.unpack(">I", payload[:4])[0]
        assert count == 2

        # Result is dict[str, FeatureResult]
        assert isinstance(result, dict)
        assert set(result.keys()) == {"k1", "k2"}
        assert isinstance(result["k1"], FeatureResult)
        assert result["k1"].tx_count_1h == 5
        assert result["k1"].tx_sum_1h == 100.0
        assert result["k2"].tx_count_1h == 3

    def test_mget_unknown_key_returns_empty_feature_result(self):
        response_data = {"k1": {"a": 1}, "k_unknown": {}}

        def handler(conn):
            _recv_frame(conn)
            conn.sendall(
                _make_response_frame(
                    STATUS_OK, json.dumps(response_data).encode("utf-8")
                )
            )

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.mget(["k1", "k_unknown"])

        done.wait(timeout=2.0)
        assert isinstance(result["k_unknown"], FeatureResult)
        assert result["k_unknown"].to_dict() == {}

    def test_mget_empty_response(self):
        def handler(conn):
            _recv_frame(conn)
            conn.sendall(_make_response_frame(STATUS_OK, b"{}"))

        port, done = _start_mock_server(handler)
        with App(f"127.0.0.1:{port}") as app:
            result = app.mget(["k1"])

        done.wait(timeout=2.0)
        assert isinstance(result, dict)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Tests: __init__.py exports
# ---------------------------------------------------------------------------


class TestInitExports:
    def test_app_exported_from_tally(self):
        assert hasattr(st, "App")
        assert st.App is App

    def test_all_public_api_available(self):
        expected = [
            "FeatureResult", "TallyError", "ConnectionError", "ProtocolError",
            "count", "sum", "avg", "min", "max", "distinct_count", "last",
            "derive", "lookup", "stream", "view", "App",
        ]
        for name in expected:
            assert hasattr(st, name), f"tally.{name} not found"
