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
    """Stub attributes event, table, col, App must exist.

    These may raise NotImplementedError when called; the attribute itself must be present.
    """
    assert hasattr(bv, "event")
    assert hasattr(bv, "table")
    assert hasattr(bv, "col")
    assert hasattr(bv, "App")
