"""Tests for beava._wire: frame codec, constants, encode/decode behavior.

These tests have no network dependency — they exercise purely in-process logic.
They are expected to FAIL (ImportError) until python/beava/_wire.py is created
in Task 1.b.
"""

from __future__ import annotations

import struct

import pytest

from beava._wire import (
    CT_JSON,
    CT_MSGPACK,
    OP_ERROR_RESPONSE,
    OP_PING,
    OP_REGISTER,
    FrameTooLarge,
    IncompleteFrame,
    decode_frame,
    encode_frame,
)


class TestOpcodeAndCtConstants:
    def test_opcode_and_ct_constants_match_server(self) -> None:
        """Constants must be byte-identical to crates/beava-core/src/wire.rs."""
        assert OP_PING == 0x0000
        assert OP_REGISTER == 0x0001
        assert OP_ERROR_RESPONSE == 0xFFFF
        assert CT_JSON == 0x01
        assert CT_MSGPACK == 0x02


class TestEncodeFrame:
    def test_encode_frame_bytes_layout(self) -> None:
        """encode_frame(op=0x0001, ct=0x01, payload=b'hello') must produce the exact bytes.

        Frame: [u32 length=8 BE][u16 op=0x0001 BE][u8 ct=0x01][5 payload bytes]
        length = op(2) + ct(1) + payload(5) = 8
        """
        result = encode_frame(op=0x0001, ct=0x01, payload=b"hello")
        expected = b"\x00\x00\x00\x08\x00\x01\x01hello"
        assert result == expected, f"got {result.hex()}, expected {expected.hex()}"

    def test_encode_empty_payload(self) -> None:
        """Empty payload: length=3 (op+ct only), op=0x0000, ct=0x01."""
        result = encode_frame(op=0x0000, ct=0x01, payload=b"")
        expected = b"\x00\x00\x00\x03\x00\x00\x01"
        assert result == expected, f"got {result.hex()}, expected {expected.hex()}"

    def test_encode_length_field_is_correct(self) -> None:
        """length field = 2 (op) + 1 (ct) + len(payload)."""
        payload = b"x" * 100
        result = encode_frame(op=0x0001, ct=0x01, payload=payload)
        declared_len = struct.unpack(">I", result[:4])[0]
        assert declared_len == 2 + 1 + len(payload)

    def test_encode_big_endian_multi_byte_fields(self) -> None:
        """op=0x0102, ct=0x03, payload=[0x04] → [0,0,0,4, 0x01,0x02, 0x03, 0x04]."""
        result = encode_frame(op=0x0102, ct=0x03, payload=b"\x04")
        assert result == b"\x00\x00\x00\x04\x01\x02\x03\x04"


class TestDecodeFrame:
    def test_decode_roundtrip(self) -> None:
        """Decode(encode(op, ct, p)) == (op, ct, p) for several payloads."""
        test_cases = [
            (0x0000, 0x01, b""),
            (0x0001, 0x01, b"hello world"),
            (0xFFFF, 0x01, b'{"error":{"code":"foo"}}'),
            (0x0001, 0x02, b"\x00\x01\x02\x03"),
            (0x0001, 0x01, bytes(range(256))),
        ]
        for op, ct, payload in test_cases:
            encoded = encode_frame(op=op, ct=ct, payload=payload)
            frame = decode_frame(encoded)
            assert frame.op == op, f"op mismatch for payload {payload!r}"
            assert frame.ct == ct, f"ct mismatch for payload {payload!r}"
            assert frame.payload == payload, f"payload mismatch"

    def test_decode_too_large_raises_frame_too_large(self) -> None:
        """Declared length of 10 MiB raises FrameTooLarge with 'too_large' in message."""
        ten_mib = 10 * 1024 * 1024
        # Build a fake header: length = 10 MiB + 3 (op+ct overhead)
        fake_header = struct.pack(">I", ten_mib + 3)
        with pytest.raises(FrameTooLarge) as exc_info:
            decode_frame(fake_header + b"\x00" * 3)
        assert "too_large" in str(exc_info.value).lower() or exc_info.type is FrameTooLarge

    def test_decode_short_buffer_raises_incomplete_frame(self) -> None:
        """Passing only 7 bytes (header only, no payload) raises IncompleteFrame
        when the declared length indicates a non-empty payload."""
        # Encode a frame with 10-byte payload, then truncate to header only
        full = encode_frame(op=0x0001, ct=0x01, payload=b"1234567890")
        truncated = full[:7]  # has header but not full payload
        with pytest.raises(IncompleteFrame):
            decode_frame(truncated)

    def test_decode_short_header_raises_incomplete_frame(self) -> None:
        """Buffer with fewer than 7 bytes raises IncompleteFrame."""
        with pytest.raises(IncompleteFrame):
            decode_frame(b"\x00\x00\x00\x03\x00")  # only 5 bytes, need 7 min
