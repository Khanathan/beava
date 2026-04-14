"""Unit tests for the deterministic replay event generator.

Covers TRAC-02: same seed → byte-identical stream, 30-day timestamp spread,
~5% failure rate, stable schema shape.
"""

from __future__ import annotations

import json
import time

import pytest

from benchmark.replay.generator import generate, SCHEMA_KEYS

PINNED_NOW_MS = 1_700_000_000_000  # 2023-11-14T22:13:20Z — keeps tests wall-clock independent


def test_determinism():
    """Same (seed, n, now_ms) → byte-identical event stream."""
    a = generate(1000, seed=42, now_ms=PINNED_NOW_MS)
    b = generate(1000, seed=42, now_ms=PINNED_NOW_MS)
    assert a == b
    # Stronger: JSON byte-equality under sort_keys (guards against dict-order flukes).
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_timestamp_spread():
    """min(ts)..max(ts) covers ≥ 99% of the requested 30-day window."""
    n = 20_000
    days = 30
    events = generate(n, seed=42, days=days, now_ms=PINNED_NOW_MS)
    ts = [e["ts"] for e in events]
    span_ms = max(ts) - min(ts)
    window_ms = days * 86_400_000
    # With 20k uniform draws, the observed span is overwhelmingly within 1% of window.
    assert span_ms >= int(window_ms * 0.99), (
        f"span {span_ms}ms < 99% of window {window_ms}ms"
    )
    # All timestamps fall inside [now_ms - window, now_ms].
    assert min(ts) >= PINNED_NOW_MS - window_ms
    assert max(ts) <= PINNED_NOW_MS


def test_timestamps_sorted_ascending():
    """Replay requires time-ordered events so window operators bucket correctly."""
    events = generate(5_000, seed=42, now_ms=PINNED_NOW_MS)
    ts = [e["ts"] for e in events]
    assert ts == sorted(ts)


def test_failure_rate():
    """status='failed' ≈ 5% (±1%) over 10k events."""
    n = 10_000
    events = generate(n, seed=42, now_ms=PINNED_NOW_MS)
    failed = sum(1 for e in events if e["status"] == "failed")
    ratio = failed / n
    assert abs(ratio - 0.05) < 0.01, f"failure ratio {ratio} outside 0.04..0.06"


def test_schema_shape():
    """Every event carries exactly the 6 canonical keys."""
    events = generate(200, seed=42, now_ms=PINNED_NOW_MS)
    assert SCHEMA_KEYS == {"user_id", "merchant_id", "amount", "status", "country", "ts"}
    for e in events:
        assert set(e.keys()) == SCHEMA_KEYS
        assert isinstance(e["user_id"], str)
        assert isinstance(e["merchant_id"], str)
        assert isinstance(e["amount"], float)
        assert e["status"] in ("success", "failed")
        assert isinstance(e["country"], str)
        assert isinstance(e["ts"], int)


def test_generator_is_fast():
    """generate(10) returns quickly (≤ 50ms); guards against accidental O(n²)."""
    t0 = time.perf_counter()
    events = generate(10, seed=42, now_ms=PINNED_NOW_MS)
    dt = time.perf_counter() - t0
    assert len(events) == 10
    assert dt < 0.05, f"generate(10) took {dt*1000:.1f}ms, expected < 50ms"


def test_seed_change_differs():
    """Different seed → different stream (sanity check on RNG wiring)."""
    a = generate(200, seed=42, now_ms=PINNED_NOW_MS)
    b = generate(200, seed=43, now_ms=PINNED_NOW_MS)
    assert a != b
