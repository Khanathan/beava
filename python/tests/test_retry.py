"""Tests for RetryPolicy (Phase 43 T3): delay schedule + integration with BeavaClient."""

from __future__ import annotations

import socket
import struct
import threading

import pytest

import beava as bv
from beava._client import BeavaClient
from beava._protocol import STATUS_OK, encode_frame
from beava._retry import DEFAULT_POLICY, NO_RETRY, RetryPolicy
from beava._types import ConnectionError


# ---------------------------------------------------------------------------
# Unit: RetryPolicy delay schedule + validation
# ---------------------------------------------------------------------------


class TestRetryPolicyDelaySchedule:
    def test_jitterless_schedule_matches_formula(self):
        policy = RetryPolicy(
            max_retries=5,
            initial_delay_s=0.01,
            max_delay_s=1.0,
            backoff_factor=2.0,
            jitter=False,
        )
        # Attempt 1 -> 0.01 * 2^0 = 0.01
        # Attempt 2 -> 0.01 * 2^1 = 0.02
        # Attempt 3 -> 0.01 * 2^2 = 0.04
        assert policy.compute_delay(1) == pytest.approx(0.01)
        assert policy.compute_delay(2) == pytest.approx(0.02)
        assert policy.compute_delay(3) == pytest.approx(0.04)
        assert policy.compute_delay(4) == pytest.approx(0.08)

    def test_max_delay_caps_the_schedule(self):
        policy = RetryPolicy(
            max_retries=10,
            initial_delay_s=0.1,
            max_delay_s=0.3,
            backoff_factor=2.0,
            jitter=False,
        )
        # 0.1, 0.2, 0.4 -> capped at 0.3, then 0.3 forever.
        assert policy.compute_delay(1) == pytest.approx(0.1)
        assert policy.compute_delay(2) == pytest.approx(0.2)
        assert policy.compute_delay(3) == pytest.approx(0.3)  # cap kicks in
        assert policy.compute_delay(4) == pytest.approx(0.3)
        assert policy.compute_delay(7) == pytest.approx(0.3)

    def test_jitter_produces_half_to_full_of_base(self):
        policy = RetryPolicy(
            max_retries=1,
            initial_delay_s=0.1,
            max_delay_s=1.0,
            backoff_factor=2.0,
            jitter=True,
        )
        # 200 samples: all must fall in [0.5 * 0.1, 1.0 * 0.1) = [0.05, 0.10).
        samples = [policy.compute_delay(1) for _ in range(200)]
        for s in samples:
            assert 0.05 <= s < 0.10, f"jittered delay out of [0.05, 0.10): {s}"
        # Spread check: at least three distinct values across 200 samples.
        assert len(set(samples)) >= 3

    def test_attempt_zero_rejected(self):
        policy = RetryPolicy()
        with pytest.raises(ValueError, match="attempt must be >= 1"):
            policy.compute_delay(0)

    def test_validation_rejects_bad_params(self):
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            RetryPolicy(max_retries=-1)
        with pytest.raises(ValueError, match="initial_delay_s must be >= 0"):
            RetryPolicy(initial_delay_s=-0.1)
        with pytest.raises(ValueError, match="max_delay_s .* must be >="):
            RetryPolicy(initial_delay_s=1.0, max_delay_s=0.5)
        with pytest.raises(ValueError, match="backoff_factor must be >= 1.0"):
            RetryPolicy(backoff_factor=0.5)


# ---------------------------------------------------------------------------
# Unit: public surface exports
# ---------------------------------------------------------------------------


def test_public_surface_exports_retry_policy():
    assert bv.RetryPolicy is RetryPolicy
    assert isinstance(bv.DEFAULT_POLICY, RetryPolicy)
    assert bv.DEFAULT_POLICY.max_retries == 3
    assert bv.NO_RETRY.max_retries == 0


# ---------------------------------------------------------------------------
# Integration: BeavaClient uses the policy for transient connect failures
# ---------------------------------------------------------------------------


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


def _start_server(handler, accept_count: int) -> tuple[int, threading.Event]:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    port = srv.getsockname()[1]
    ready = threading.Event()
    done = threading.Event()

    def _run():
        try:
            ready.set()
            for _ in range(accept_count):
                srv.settimeout(5.0)
                try:
                    conn, addr = srv.accept()
                except socket.timeout:
                    break
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


def test_retry_recovers_after_transient_disconnect():
    """Server accepts first connection but hangs up before reading; the second
    accepted connection is clean. A BeavaClient configured with
    ``max_retries>=1`` must succeed on the second attempt."""
    accepted = [0]

    def handler(conn, _addr):
        accepted[0] += 1
        if accepted[0] == 1:
            # Drop the connection immediately — client sees broken pipe /
            # connection-closed on its first send.
            conn.close()
            return
        # Second connection: behave normally.
        header = _recv_exact(conn, 4)
        length = struct.unpack(">I", header)[0]
        _recv_exact(conn, length)  # consume opcode + payload
        conn.sendall(_make_response_frame(STATUS_OK, b"ok"))

    port, done = _start_server(handler, accept_count=2)
    policy = RetryPolicy(
        max_retries=3, initial_delay_s=0.001, max_delay_s=0.01, jitter=False
    )
    client = BeavaClient("127.0.0.1", port, retry_policy=policy)
    try:
        status, payload = client.send_command(0x01, b"hello")
        assert status == STATUS_OK
        assert payload == b"ok"
        assert accepted[0] == 2, f"expected 2 connect attempts, got {accepted[0]}"
    finally:
        client.close()
        done.wait(timeout=2.0)


def test_retry_exhausted_raises_after_max_attempts():
    """If every connection is slammed shut, the client gives up after
    max_retries+1 total attempts and raises the underlying ConnectionError."""
    attempts = [0]

    def handler(conn, _addr):
        attempts[0] += 1
        conn.close()

    port, done = _start_server(handler, accept_count=5)
    policy = RetryPolicy(
        max_retries=2, initial_delay_s=0.001, max_delay_s=0.01, jitter=False
    )
    client = BeavaClient("127.0.0.1", port, retry_policy=policy)
    try:
        with pytest.raises(ConnectionError):
            client.send_command(0x01, b"hello")
        # 1 initial + 2 retries = 3 total connects.
        assert attempts[0] == 3, f"expected 3 connect attempts, got {attempts[0]}"
    finally:
        client.close()
        done.wait(timeout=2.0)


def test_no_retry_policy_fails_on_first_failure():
    """NO_RETRY gives the pre-Phase-43 single-shot behaviour."""
    attempts = [0]

    def handler(conn, _addr):
        attempts[0] += 1
        conn.close()

    port, done = _start_server(handler, accept_count=3)
    client = BeavaClient("127.0.0.1", port, retry_policy=NO_RETRY)
    try:
        with pytest.raises(ConnectionError):
            client.send_command(0x01, b"hello")
        assert attempts[0] == 1, f"NO_RETRY should allow 1 attempt, got {attempts[0]}"
    finally:
        client.close()
        done.wait(timeout=2.0)


def test_default_policy_used_when_unspecified():
    """BeavaClient() with no retry_policy picks up DEFAULT_POLICY."""
    client = BeavaClient("127.0.0.1", 9999)
    assert client._retry_policy is DEFAULT_POLICY
    client.close()
