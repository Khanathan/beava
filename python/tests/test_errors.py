"""Tests for beava error types (ValidationError, RegistrationError, BinaryNotFoundError).

These tests are written FIRST (TDD red commit) before the implementation exists.
All tests are expected to FAIL at this point with ImportError / ModuleNotFoundError.
"""

from beava import ValidationError, RegistrationError, BinaryNotFoundError
from beava._errors import VALIDATION_ERROR_KINDS


def test_validation_error_is_frozen_dataclass() -> None:
    """ValidationError must be a frozen dataclass."""
    ve = ValidationError(kind="cycle", path="A", message="m")
    assert hasattr(ValidationError, "__dataclass_fields__")
    assert ValidationError.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert ve.kind == "cycle"
    assert ve.path == "A"
    assert ve.message == "m"


def test_validation_error_str_repr() -> None:
    """str(ValidationError(...)) must return '[{kind}] {path}: {message}'."""
    ve = ValidationError(
        kind="schema_mismatch",
        path="Transaction.event_time",
        message="field 'x' not in schema",
    )
    assert str(ve) == "[schema_mismatch] Transaction.event_time: field 'x' not in schema"


def test_validation_error_kinds_enumerated() -> None:
    """VALIDATION_ERROR_KINDS frozenset contains exactly the 9 specified kinds."""
    expected = frozenset({
        "cycle",
        "missing_upstream",
        "schema_mismatch",
        "bad_return_type",
        "unknown_field_type",
        "table_key_invalid",
        "event_time_field_invalid",
        "registration_conflict",
        "duplicate_name",
    })
    assert VALIDATION_ERROR_KINDS == expected


def test_registration_error_structure() -> None:
    """RegistrationError must be an Exception with code/path/message/errors attributes."""
    err = RegistrationError(
        code="registration_conflict",
        path="Transaction",
        message="m",
        errors=[],
    )
    assert err.code == "registration_conflict"
    assert err.path == "Transaction"
    assert err.message == "m"
    assert err.errors == []
    assert isinstance(err, Exception)


def test_binary_not_found_is_exception() -> None:
    """BinaryNotFoundError must be an Exception with string message."""
    msg = "beava binary not on PATH"
    err = BinaryNotFoundError(msg)
    assert isinstance(err, Exception)
    assert str(err) == msg
