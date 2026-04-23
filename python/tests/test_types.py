"""Tests for beava type primitives (Optional, Field, MISSING, py_type_to_field_type).

These tests are written FIRST (TDD red commit) before the implementation exists.
All tests are expected to FAIL at this point with ImportError / ModuleNotFoundError.
"""

import datetime
import typing

import beava as bv
from beava._types import py_type_to_field_type, MISSING as MISSING_sentinel


def test_optional_produces_marker() -> None:
    """bv.Optional[T] must be distinct from typing.Optional[T] and support equality + nesting."""
    # Not the same as typing.Optional
    assert bv.Optional[str] is not typing.Optional[str]

    # Same type produces equal markers
    assert bv.Optional[str] == bv.Optional[str]

    # Nested Optional collapses: Optional[Optional[int]] == Optional[int]
    assert bv.Optional[bv.Optional[int]] == bv.Optional[int]


def test_field_stores_metadata() -> None:
    """bv.Field(desc=..., default=...) must store desc and default; default to MISSING."""
    f = bv.Field(desc="who", default="anon")
    assert f.desc == "who"
    assert f.default == "anon"

    # Field with no args uses MISSING sentinel as default
    f_no_default = bv.Field()
    assert f_no_default.default is MISSING_sentinel


def test_field_type_mapping() -> None:
    """py_type_to_field_type must map Python types to server FieldType strings."""
    assert py_type_to_field_type(str) == "str"
    assert py_type_to_field_type(int) == "i64"
    assert py_type_to_field_type(float) == "f64"
    assert py_type_to_field_type(bool) == "bool"
    assert py_type_to_field_type(bytes) == "bytes"
    assert py_type_to_field_type(datetime.datetime) == "datetime"

    # Unsupported type raises TypeError with helpful message
    import pytest
    with pytest.raises(TypeError) as exc_info:
        py_type_to_field_type(list)  # type: ignore[arg-type]
    msg = str(exc_info.value)
    # Message must contain unsupported type name AND the supported types list
    assert "list" in msg
    assert "supported: str, int, float, bool, bytes, datetime" in msg
