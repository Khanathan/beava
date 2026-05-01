"""Tests for beava package-level public exports.

These tests are written FIRST (TDD red commit) before the implementation exists.
All tests are expected to FAIL at this point with ImportError / ModuleNotFoundError.
"""

import beava as bv


def test_public_exports_present() -> None:
    """All required public names must be importable from the beava package."""
    # Type primitives
    assert bv.Optional is not None
    assert bv.Field is not None

    # Error types
    assert bv.ValidationError is not None
    assert bv.RegistrationError is not None
    assert bv.BinaryNotFoundError is not None


def test_package_exports_stubs_for_phase3() -> None:
    """Stub attributes event, col, App must exist.

    Plan 12.7-06: ``bv.table`` removed per `project_v0_events_only_scope`
    (locked 2026-04-30) — v0 ships events-only. Stubs return in v0.1+ if
    tables revive.
    """
    assert hasattr(bv, "event")
    assert not hasattr(bv, "table"), "bv.table must be absent in v0 (events-only)"
    assert hasattr(bv, "col")
    assert hasattr(bv, "App")
