"""Unit tests for v0 schema primitives: Optional, Field, extract_schema, suggest."""

from __future__ import annotations

import pytest

from tally._types_core import (
    MISSING,
    Field,
    FieldSpec,
    Optional,
    _FieldMarker,
    _OptionalSpec,
)
from tally._schema_v0 import (
    extract_schema,
    schema_mismatch_error,
    suggest,
)


# ---------------------------------------------------------------------------
# Optional marker
# ---------------------------------------------------------------------------


class TestOptional:
    def test_optional_produces_distinct_marker(self):
        spec = Optional[int]
        assert isinstance(spec, _OptionalSpec)
        assert spec.inner is int

    def test_nested_optional_collapses(self):
        spec = Optional[Optional[int]]
        assert isinstance(spec, _OptionalSpec)
        assert spec.inner is int  # single-layer

    def test_optional_is_not_typing_optional(self):
        import typing

        assert Optional[int] != typing.Optional[int]

    def test_extract_schema_recognises_optional(self):
        class X:
            a: int
            b: Optional[str]

        schema = extract_schema(X)
        assert schema["a"].optional is False
        assert schema["b"].optional is True
        assert schema["b"].py_type is str


# ---------------------------------------------------------------------------
# Field() metadata
# ---------------------------------------------------------------------------


class TestField:
    def test_field_no_args(self):
        m = Field()
        assert isinstance(m, _FieldMarker)
        assert m.desc is None
        assert m.default is MISSING

    def test_field_with_desc_and_default(self):
        m = Field(desc="user id", default="anon")
        assert m.desc == "user id"
        assert m.default == "anon"

    def test_field_metadata_captured_in_schema(self):
        class Users:
            user_id: str = Field(desc="primary key")
            nickname: str = Field(desc="display name", default="anon")

        schema = extract_schema(Users)

        u = schema["user_id"]
        assert u.py_type is str
        assert u.optional is False
        assert u.desc == "primary key"
        assert u.default is MISSING

        n = schema["nickname"]
        assert n.desc == "display name"
        assert n.default == "anon"


# ---------------------------------------------------------------------------
# extract_schema behaviour
# ---------------------------------------------------------------------------


class TestExtractSchema:
    def test_plain_annotations(self):
        class C:
            a: int
            b: str

        schema = extract_schema(C)
        assert list(schema.keys()) == ["a", "b"]
        assert schema["a"].py_type is int
        assert schema["b"].py_type is str
        assert all(f.optional is False for f in schema.values())

    def test_declaration_order_preserved(self):
        class C:
            z: int
            a: str
            m: float

        assert list(extract_schema(C).keys()) == ["z", "a", "m"]

    def test_unsupported_type_raises_with_field_name(self):
        class C:
            bad: list[dict]  # not a primitive

        with pytest.raises(TypeError) as ei:
            extract_schema(C)
        assert "'bad'" in str(ei.value)
        assert "C" in str(ei.value)

    def test_method_on_class_body_raises_surgical_error(self):
        class C:
            a: int

            def foo(self):  # pragma: no cover
                return 1

        with pytest.raises(TypeError) as ei:
            extract_schema(C)
        msg = str(ei.value)
        assert "'foo'" in msg
        assert "schema only" in msg
        assert "function" in msg  # points at function-form decorator

    def test_datetime_types_allowed(self):
        from datetime import date, datetime

        class C:
            ts: datetime
            d: date

        schema = extract_schema(C)
        assert schema["ts"].py_type is datetime
        assert schema["d"].py_type is date

    def test_returns_fieldspec_instances(self):
        class C:
            a: int

        schema = extract_schema(C)
        assert isinstance(schema["a"], FieldSpec)


# ---------------------------------------------------------------------------
# Levenshtein suggest()
# ---------------------------------------------------------------------------


class TestSuggest:
    def test_close_match_within_distance(self):
        assert suggest("amout", ["amount", "user_id"]) == "amount"

    def test_no_close_match_returns_none(self):
        assert suggest("xyz", ["amount", "user_id"]) is None

    def test_exact_match(self):
        assert suggest("amount", ["amount", "user_id"]) == "amount"

    def test_empty_haystack(self):
        assert suggest("x", []) is None

    def test_first_match_wins_on_tie(self):
        # "ab" distance to "ax" = 1, to "ay" = 1 -> "ax" wins (first)
        assert suggest("ab", ["ax", "ay"]) == "ax"

    def test_distance_threshold_is_two(self):
        # 3 edits -> no suggestion
        assert suggest("abcd", ["wxyz"]) is None


# ---------------------------------------------------------------------------
# schema_mismatch_error builder
# ---------------------------------------------------------------------------


class TestSchemaMismatchError:
    def test_message_includes_field_context_and_suggestion(self):
        class S:
            amount: float
            user_id: str

        schema = extract_schema(S)
        msg = schema_mismatch_error("amout", schema, "Purchases")
        assert "'amout'" in msg
        assert "Purchases" in msg
        assert "'amount'" in msg  # suggestion
        assert "amount" in msg and "user_id" in msg  # available list

    def test_no_suggestion_when_far(self):
        class S:
            amount: float

        schema = extract_schema(S)
        msg = schema_mismatch_error("xxxxxx", schema, "Purchases")
        assert "did you mean" not in msg
        assert "available" in msg
