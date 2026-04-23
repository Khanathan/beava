"""Tests for @bv.table decorator — class form and function form.

These tests are written RED-first — they will fail until _tables.py exists.

Note: deliberately no ``from __future__ import annotations`` so that parameter
annotations in function-form tests are evaluated eagerly at def-time and
capture the decorated EventSource / TableSource objects from local scope.
"""

import pytest

import beava as bv

# ---------------------------------------------------------------------------
# Class form: basic
# ---------------------------------------------------------------------------


def test_table_class_form_basic() -> None:
    """@bv.table(key='user_id') produces TableSource with correct JSON shape."""

    @bv.table(key="user_id")
    class UserProfile:
        user_id: str
        name: str

    j = UserProfile._to_register_json()
    assert j["kind"] == "table"
    assert j["name"] == "UserProfile"
    assert j["primary_key"] == ["user_id"]
    assert j["mode"] == "upsert"
    assert j["ttl_ms"] is None
    assert j["schema"]["fields"] == {"user_id": "str", "name": "str"}
    assert UserProfile._beava_kind == "table"


# ---------------------------------------------------------------------------
# Multiple key fields
# ---------------------------------------------------------------------------


def test_table_multiple_key() -> None:
    """@bv.table(key=[...]) supports composite primary keys."""

    @bv.table(key=["region", "user_id"])
    class X:
        region: str
        user_id: str
        value: int

    j = X._to_register_json()
    assert j["primary_key"] == ["region", "user_id"]


# ---------------------------------------------------------------------------
# Key is required
# ---------------------------------------------------------------------------


def test_table_key_required() -> None:
    """@bv.table without key raises TypeError — key is mandatory."""
    # No parens (bare decorator applied to class)
    with pytest.raises(TypeError, match="key"):

        @bv.table
        class X:
            a: str

    # Empty parens — key still missing
    with pytest.raises(TypeError, match="key"):

        @bv.table()
        class Y:
            a: str


# ---------------------------------------------------------------------------
# Key validation against schema
# ---------------------------------------------------------------------------


def test_table_key_must_be_in_schema() -> None:
    """@bv.table(key='missing') raises TypeError if key not in schema."""
    with pytest.raises(TypeError, match="missing"):

        @bv.table(key="missing")
        class X:
            a: str


# ---------------------------------------------------------------------------
# TTL conversion
# ---------------------------------------------------------------------------


def test_table_ttl_converts() -> None:
    """ttl duration string is converted to ms; 'forever' maps to null."""

    @bv.table(key="user_id", ttl="7d")
    class X:
        user_id: str

    j = X._to_register_json()
    assert j["ttl_ms"] == 604_800_000

    @bv.table(key="user_id", ttl="forever")
    class Y:
        user_id: str

    j2 = Y._to_register_json()
    assert j2["ttl_ms"] is None


# ---------------------------------------------------------------------------
# Function form (derivation)
# ---------------------------------------------------------------------------


def test_table_function_form() -> None:
    """@bv.table on a function produces TableDerivation with correct shape."""

    @bv.event
    class TxSrc:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def Counts(source: TxSrc) -> object:  # type: ignore[valid-type]
        return source

    assert Counts._name == "Counts"
    assert Counts._beava_kind == "derivation"

    j = Counts._to_register_json()
    assert j["output_kind"] == "table"
    assert j["table_primary_key"] == ["user_id"]
    assert j["upstreams"] == ["TxSrc"]
