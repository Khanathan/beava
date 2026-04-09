"""Byte-level conformance tests for tally._protocol.

Every test verifies exact byte sequences that match the Rust server's
protocol.rs encoding. This ensures Python SDK and Rust server are
wire-compatible.
"""

import json
import struct

import pytest

from tally._protocol import (
    OP_PUSH,
    OP_GET,
    OP_SET,
    OP_MSET,
    OP_REGISTER,
    STATUS_OK,
    STATUS_ERROR,
    MAX_FRAME_SIZE,
    encode_frame,
    encode_string,
    encode_push,
    encode_get,
    encode_set,
    encode_mset,
    encode_register,
    parse_response,
)
from tally._types import ProtocolError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_opcodes(self):
        assert OP_PUSH == 0x01
        assert OP_GET == 0x02
        assert OP_SET == 0x03
        assert OP_MSET == 0x04
        assert OP_REGISTER == 0x05

    def test_status_codes(self):
        assert STATUS_OK == 0x00
        assert STATUS_ERROR == 0x01

    def test_max_frame_size(self):
        assert MAX_FRAME_SIZE == 64 * 1024 * 1024


# ---------------------------------------------------------------------------
# encode_frame
# ---------------------------------------------------------------------------


class TestEncodeFrame:
    def test_with_payload(self):
        # Frame: [4-byte BE length=6][opcode=0x01][payload=b"hello"]
        result = encode_frame(0x01, b"hello")
        assert result == b"\x00\x00\x00\x06\x01hello"

    def test_empty_payload(self):
        # Frame: [4-byte BE length=1][opcode=0x02]
        result = encode_frame(0x02, b"")
        assert result == b"\x00\x00\x00\x01\x02"

    def test_length_is_opcode_plus_payload(self):
        payload = b"x" * 100
        result = encode_frame(0x03, payload)
        length = struct.unpack(">I", result[:4])[0]
        assert length == 1 + len(payload)
        assert result[4] == 0x03
        assert result[5:] == payload


# ---------------------------------------------------------------------------
# encode_string
# ---------------------------------------------------------------------------


class TestEncodeString:
    def test_basic_string(self):
        # String: [u16 BE length=2][UTF-8 bytes="hi"]
        result = encode_string("hi")
        assert result == b"\x00\x02hi"

    def test_empty_string(self):
        result = encode_string("")
        assert result == b"\x00\x00"

    def test_longer_string(self):
        result = encode_string("Transactions")
        expected = struct.pack(">H", 12) + b"Transactions"
        assert result == expected

    def test_utf8_encoding(self):
        s = "caf\u00e9"  # 5 bytes in UTF-8
        s_bytes = s.encode("utf-8")
        result = encode_string(s)
        assert result == struct.pack(">H", len(s_bytes)) + s_bytes


# ---------------------------------------------------------------------------
# encode_push
# ---------------------------------------------------------------------------


class TestEncodePush:
    def test_basic_push(self):
        payload = encode_push("Transactions", {"user_id": "u123", "amount": 50.0})
        # Starts with encode_string("Transactions") = \x00\x0cTransactions
        assert payload[:14] == b"\x00\x0cTransactions"
        # Remainder is JSON
        json_part = payload[14:]
        parsed = json.loads(json_part)
        assert parsed["user_id"] == "u123"
        assert parsed["amount"] == 50.0

    def test_full_frame(self):
        payload = encode_push("S", {"k": 1})
        frame = encode_frame(OP_PUSH, payload)
        # frame starts with 4-byte length
        length = struct.unpack(">I", frame[:4])[0]
        assert length == 1 + len(payload)
        assert frame[4] == OP_PUSH


# ---------------------------------------------------------------------------
# encode_get
# ---------------------------------------------------------------------------


class TestEncodeGet:
    def test_basic_get(self):
        result = encode_get("u123")
        assert result == b"\x00\x04u123"

    def test_empty_key(self):
        result = encode_get("")
        assert result == b"\x00\x00"


# ---------------------------------------------------------------------------
# encode_set
# ---------------------------------------------------------------------------


class TestEncodeSet:
    def test_basic_set(self):
        payload = encode_set("u123", {"x": 1})
        # Starts with encode_string("u123")
        assert payload[:6] == b"\x00\x04u123"
        # Remainder is JSON
        json_part = payload[6:]
        parsed = json.loads(json_part)
        assert parsed == {"x": 1}


# ---------------------------------------------------------------------------
# encode_mset
# ---------------------------------------------------------------------------


class TestEncodeMset:
    def test_basic_mset(self):
        entries = {"u1": {"a": 1}, "u2": {"b": 2}}
        payload = encode_mset(entries)
        # Starts with u32 count
        count = struct.unpack(">I", payload[:4])[0]
        assert count == 2

        # Parse entries manually
        pos = 4
        parsed_entries = {}
        for _ in range(count):
            key_len = struct.unpack(">H", payload[pos : pos + 2])[0]
            pos += 2
            key = payload[pos : pos + key_len].decode("utf-8")
            pos += key_len
            json_len = struct.unpack(">I", payload[pos : pos + 4])[0]
            pos += 4
            json_bytes = payload[pos : pos + json_len]
            pos += json_len
            parsed_entries[key] = json.loads(json_bytes)

        assert "u1" in parsed_entries
        assert "u2" in parsed_entries
        assert parsed_entries["u1"] == {"a": 1}
        assert parsed_entries["u2"] == {"b": 2}

    def test_empty_mset(self):
        payload = encode_mset({})
        assert payload == struct.pack(">I", 0)

    def test_single_entry(self):
        payload = encode_mset({"k": {"v": 42}})
        count = struct.unpack(">I", payload[:4])[0]
        assert count == 1


# ---------------------------------------------------------------------------
# encode_register
# ---------------------------------------------------------------------------


class TestEncodeRegister:
    def test_basic_register(self):
        definition = {"name": "Tx", "key_field": "uid", "features": []}
        payload = encode_register(definition)
        parsed = json.loads(payload)
        assert parsed == definition

    def test_register_is_json_bytes(self):
        definition = {"name": "S"}
        payload = encode_register(definition)
        assert isinstance(payload, bytes)
        assert json.loads(payload) == definition


# ---------------------------------------------------------------------------
# parse_response
# ---------------------------------------------------------------------------


class TestParseResponse:
    def test_ok_with_payload(self):
        # Build response: [4-byte BE length][status=0x00][payload]
        json_payload = b'{"a":1}'
        length = 1 + len(json_payload)
        data = struct.pack(">I", length) + bytes([STATUS_OK]) + json_payload
        status, payload = parse_response(data)
        assert status == STATUS_OK
        assert payload == json_payload

    def test_ok_empty_payload(self):
        data = struct.pack(">I", 1) + bytes([STATUS_OK])
        status, payload = parse_response(data)
        assert status == STATUS_OK
        assert payload == b""

    def test_error_status(self):
        error_msg = b"something went wrong"
        length = 1 + len(error_msg)
        data = struct.pack(">I", length) + bytes([STATUS_ERROR]) + error_msg
        with pytest.raises(ProtocolError, match="something went wrong"):
            parse_response(data)

    def test_oversized_frame_rejected(self):
        # Fake a frame claiming to be larger than MAX_FRAME_SIZE
        huge_length = MAX_FRAME_SIZE + 1
        data = struct.pack(">I", huge_length) + bytes([STATUS_OK])
        with pytest.raises(ProtocolError, match="frame too large"):
            parse_response(data)

    def test_truncated_header_rejected(self):
        with pytest.raises(ProtocolError):
            parse_response(b"\x00\x00")

    def test_truncated_body_rejected(self):
        # Claim length=10 but only provide 2 bytes of body
        data = struct.pack(">I", 10) + bytes([STATUS_OK]) + b"x"
        with pytest.raises(ProtocolError):
            parse_response(data)
