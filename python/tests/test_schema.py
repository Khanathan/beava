"""Tests for schema extraction (extract_schema) and duration helpers.

These tests are written RED-first — they will fail until _schema.py exists.
"""

from __future__ import annotations

import datetime
import typing

import pytest

import beava as bv
from beava._schema import duration_to_ms, extract_schema, validate_duration_string

# ---------------------------------------------------------------------------
# Basic type extraction
# ---------------------------------------------------------------------------


def test_extract_schema_basic_types() -> None:
    """extract_schema maps str/float/int/bool/bytes to correct FieldType strings."""
    from beava._types import py_type_to_field_type

    class X:
        a: int
        b: float
        c: str
        d: bool
        e: bytes

    schema = extract_schema(X)
    assert set(schema.keys()) == {"a", "b", "c", "d", "e"}
    assert py_type_to_field_type(schema["a"].py_type) == "i64"
    assert py_type_to_field_type(schema["b"].py_type) == "f64"
    assert py_type_to_field_type(schema["c"].py_type) == "str"
    assert py_type_to_field_type(schema["d"].py_type) == "bool"
    assert py_type_to_field_type(schema["e"].py_type) == "bytes"


def test_extract_schema_datetime() -> None:
    """extract_schema handles datetime.datetime fields."""
    from beava._types import py_type_to_field_type

    class X:
        ts: datetime.datetime

    schema = extract_schema(X)
    assert schema["ts"].py_type is datetime.datetime
    assert py_type_to_field_type(schema["ts"].py_type) == "datetime"


# ---------------------------------------------------------------------------
# bv.Optional handling
# ---------------------------------------------------------------------------


def test_extract_schema_bv_optional() -> None:
    """bv.Optional[T] is recognized as optional; plain annotated field is not."""

    class X:
        a: int
        b: bv.Optional[str]  # type: ignore[valid-type]

    schema = extract_schema(X)
    assert schema["a"].optional is False
    assert schema["a"].py_type is int
    assert schema["b"].optional is True
    assert schema["b"].py_type is str


def test_extract_schema_rejects_typing_optional() -> None:
    """typing.Optional[T] raises TypeError directing user to bv.Optional."""

    class X:
        a: typing.Optional[str]

    with pytest.raises(TypeError, match="bv.Optional"):
        extract_schema(X)


# ---------------------------------------------------------------------------
# Unsupported type rejection
# ---------------------------------------------------------------------------


def test_extract_schema_rejects_unsupported_types() -> None:
    """Unsupported field types raise TypeError with a helpful message."""

    class X:
        a: list[int]  # type: ignore[valid-type]

    with pytest.raises(TypeError, match="supported: str, int, float, bool, bytes, datetime"):
        extract_schema(X)

    class Y:
        a: dict  # type: ignore[valid-type]

    with pytest.raises(TypeError, match="supported: str, int, float, bool, bytes, datetime"):
        extract_schema(Y)


# ---------------------------------------------------------------------------
# Field metadata merging
# ---------------------------------------------------------------------------


def test_extract_schema_merges_field_metadata() -> None:
    """bv.Field(desc=..., default=...) metadata is merged into FieldSpec."""

    class X:
        a: str = bv.Field(desc="primary", default="x")  # type: ignore[assignment]

    schema = extract_schema(X)
    assert schema["a"].desc == "primary"
    assert schema["a"].default == "x"


# ---------------------------------------------------------------------------
# Duration string validation
# ---------------------------------------------------------------------------


def test_duration_string_shape_valid() -> None:
    """validate_duration_string accepts correctly formatted duration strings."""
    validate_duration_string("5s")
    validate_duration_string("24h")
    validate_duration_string("7d")
    validate_duration_string("100ms")
    validate_duration_string("forever")


def test_duration_string_shape_invalid() -> None:
    """validate_duration_string rejects malformed duration strings."""
    with pytest.raises(TypeError):
        validate_duration_string("5")
    with pytest.raises(TypeError):
        validate_duration_string("abc")
    with pytest.raises(TypeError):
        validate_duration_string("5 seconds")


# ---------------------------------------------------------------------------
# Duration to ms conversion
# ---------------------------------------------------------------------------


def test_duration_to_ms() -> None:
    """duration_to_ms converts valid duration strings to milliseconds."""
    assert duration_to_ms("5s") == 5_000
    assert duration_to_ms("24h") == 86_400_000
    assert duration_to_ms("7d") == 604_800_000
    assert duration_to_ms("100ms") == 100
    with pytest.raises(ValueError):
        duration_to_ms("forever")
