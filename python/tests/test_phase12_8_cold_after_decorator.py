"""Phase 12.8 Plan 02 — RED tests for ``@bv.event(cold_after=...)`` decorator.

Per CONTEXT D-01 (locked 2026-05-01): cold-entity TTL is configured per-source
via ``@bv.event(cold_after='7d')``. The default ``cold_after=None`` (omitted)
preserves the existing no-expiry behavior — zero behavior change for legacy
code that doesn't add the kwarg.

This file pins the v0 surface contract:

  1. ``cold_after`` accepts the same duration-string suffixes the existing
     ``dedupe_window`` / ``keep_events_for`` parsers do (``s|m|h|d`` per
     ``python/beava/_schema.py`` ``_DURATION_RE``). The CONTEXT.md mention
     of ``s|m|h|d|w`` was a planner hint based on prior Phase 5 docs; the
     actual parser table has no ``w``. Plan 02 follows the existing parser
     shape per ``feedback_logistics_autonomy``.

  2. Range: ``1s ≤ cold_after ≤ 365d``. Both bounds enforced at decoration
     time. Out-of-range raises ``TypeError`` with forward-looking framing
     ("must be ≥ 1s" / "must be ≤ 365d in v0") per CONTEXT D-01 / 12.7 D-02
     error-framing convention.

  3. ``cold_after='forever'`` is REJECTED — would defeat the cold-TTL
     purpose. Use ``cold_after=None`` (omit the kwarg) for unbounded
     retention.

  4. The parsed value is persisted onto ``EventSource._cold_after_ms`` and
     emitted in the wire JSON as ``cold_after_ms: <int|null>``. The Rust
     ``EventDescriptor`` round-trip (companion test
     ``crates/beava-core/tests/event_descriptor_cold_after.rs``) verifies
     the server-side preservation.

These tests are RED at HEAD — the decorator does not yet accept
``cold_after``. Plan 02 Task 2.b lands GREEN.
"""

from __future__ import annotations

import pytest

import beava as bv

# ---------------------------------------------------------------------------
# Happy path: cold_after is parsed correctly across the supported unit range.
# ---------------------------------------------------------------------------


def test_cold_after_omitted_yields_none() -> None:
    """``@bv.event class Tx`` (no cold_after) → ``Tx._cold_after_ms is None``.

    Default = no expiry. Legacy code keeps working unchanged.
    """

    @bv.event
    class Tx:
        amount: float

    assert Tx._cold_after_ms is None


def test_cold_after_7d_parses_to_604800000_ms() -> None:
    """``cold_after='7d'`` → 7 * 86_400_000 ms = 604_800_000 ms."""

    @bv.event(cold_after="7d")
    class Tx:
        amount: float

    assert Tx._cold_after_ms == 7 * 86_400_000


def test_cold_after_30d_parses_to_2592000000_ms() -> None:
    """``cold_after='30d'`` → 30 * 86_400_000 ms = 2_592_000_000 ms."""

    @bv.event(cold_after="30d")
    class Tx:
        amount: float

    assert Tx._cold_after_ms == 30 * 86_400_000


def test_cold_after_1s_parses_to_1000_ms_lower_boundary() -> None:
    """Lower-boundary acceptance: ``cold_after='1s'`` → 1_000 ms.

    Boundary is inclusive — the smallest legal value is exactly 1 second.
    """

    @bv.event(cold_after="1s")
    class Tx:
        amount: float

    assert Tx._cold_after_ms == 1_000


# ---------------------------------------------------------------------------
# Range-validation errors: < 1s and > 365d both rejected at decoration time.
# ---------------------------------------------------------------------------


def test_cold_after_below_1s_rejected() -> None:
    """``cold_after='500ms'`` → TypeError matching "cold_after must be ≥ 1s"."""
    with pytest.raises(TypeError, match=r"cold_after must be ≥ 1s"):

        @bv.event(cold_after="500ms")
        class Tx:
            amount: float


def test_cold_after_above_365d_rejected() -> None:
    """``cold_after='400d'`` → TypeError matching "must be ≤ 365d in v0"."""
    with pytest.raises(TypeError, match=r"cold_after must be ≤ 365d in v0"):

        @bv.event(cold_after="400d")
        class Tx:
            amount: float


# ---------------------------------------------------------------------------
# Garbage input + unsupported units fall through to the existing
# validate_duration_string() TypeError path (no special-casing).
# ---------------------------------------------------------------------------


def test_cold_after_no_unit_rejected() -> None:
    """``cold_after='999'`` (no unit) → TypeError from validate_duration_string."""
    with pytest.raises(TypeError, match=r"invalid duration string"):

        @bv.event(cold_after="999")
        class Tx:
            amount: float


def test_cold_after_unknown_unit_rejected() -> None:
    """``cold_after='1y'`` → TypeError from validate_duration_string.

    The existing parser does not support ``y`` (years) — see
    ``_DURATION_RE`` / ``_UNIT_TO_MS`` in ``python/beava/_schema.py``.
    """
    with pytest.raises(TypeError, match=r"invalid duration string"):

        @bv.event(cold_after="1y")
        class Tx:
            amount: float


def test_cold_after_forever_rejected() -> None:
    """``cold_after='forever'`` → TypeError.

    'forever' would defeat the cold-TTL purpose. Use ``cold_after=None``
    (omit the kwarg) for unbounded retention. Per CONTEXT D-01.
    """
    with pytest.raises(TypeError, match=r"forever"):

        @bv.event(cold_after="forever")
        class Tx:
            amount: float


# ---------------------------------------------------------------------------
# Wire JSON: _to_register_json() emits cold_after_ms (int | None).
# ---------------------------------------------------------------------------


def test_cold_after_emitted_in_register_json() -> None:
    """``cold_after='7d'`` → ``_to_register_json()['cold_after_ms'] == 604_800_000``."""

    @bv.event(cold_after="7d")
    class Tx:
        amount: float

    j = Tx._to_register_json()
    assert j["cold_after_ms"] == 604_800_000


def test_cold_after_omitted_emitted_as_null_in_register_json() -> None:
    """``cold_after`` omitted → ``_to_register_json()['cold_after_ms'] is None``.

    Wire-level explicit null preserves "no expiry" semantics across the
    Python→Rust boundary; the Rust struct's ``#[serde(default)]`` annotation
    handles older payloads that omit the key entirely (forward-compat).
    """

    @bv.event
    class Tx:
        amount: float

    j = Tx._to_register_json()
    assert j["cold_after_ms"] is None
