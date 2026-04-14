"""Tests for @tl.stream and @tl.table class-form decorators + .describe()."""

from __future__ import annotations

import pytest

from tally._col import col  # noqa: F401  (sanity — confirms chain imports)
from tally._stream import Stream, StreamSource, stream
from tally._table import Table, TableSource, table
from tally._types_core import Field, Optional


# ---------------------------------------------------------------------------
# @tl.stream class form
# ---------------------------------------------------------------------------


class TestStreamDecorator:
    def test_bare_decorator_produces_stream_source(self):
        @stream
        class Clicks:
            user_id: str
            url: str

        assert isinstance(Clicks, StreamSource)
        assert isinstance(Clicks, Stream)
        assert Clicks._tally_stream_name == "Clicks"

    def test_describe_shape(self):
        @stream
        class Clicks:
            user_id: str
            url: str

        assert Clicks.describe() == {
            "name": "Clicks",
            "kind": "stream",
            "key": None,
            "fields": {
                "user_id": {"type": "str", "optional": False, "desc": None},
                "url": {"type": "str", "optional": False, "desc": None},
            },
        }

    def test_parameterized_decorator_with_history_ttl(self):
        @stream(history_ttl="90d")
        class Clicks:
            user_id: str

        assert Clicks._history_ttl == "90d"
        assert Clicks.describe()["history_ttl"] == "90d"

    def test_optional_and_field_metadata_in_describe(self):
        @stream
        class Events:
            user_id: str = Field(desc="primary id")
            country: Optional[str]

        d = Events.describe()
        assert d["fields"]["user_id"]["desc"] == "primary id"
        assert d["fields"]["country"]["optional"] is True
        assert d["fields"]["country"]["type"] == "str"

    def test_method_in_class_body_raises_surgical_error(self):
        with pytest.raises(TypeError) as ei:
            @stream
            class Bad:
                a: int

                def foo(self):  # pragma: no cover
                    return 1

        assert "'foo'" in str(ei.value)
        assert "schema only" in str(ei.value)
        assert "function" in str(ei.value)

    def test_compile_shape(self):
        @stream
        class Clicks:
            user_id: str
            url: str

        reg = Clicks._compile()
        assert reg["name"] == "Clicks"
        assert reg["key_field"] is None
        assert reg["features"] == []
        assert reg["fields"]["user_id"] == {"type": "str", "optional": False}

    def test_collect_registrations(self):
        @stream
        class Clicks:
            user_id: str

        regs = Clicks._collect_registrations()
        assert len(regs) == 1
        assert regs[0]["name"] == "Clicks"

    def test_repr(self):
        @stream
        class Clicks:
            user_id: str

        assert repr(Clicks) == "StreamSource('Clicks')"

    def test_field_order_preserved(self):
        @stream
        class E:
            z: int
            a: str
            m: float

        assert list(E.describe()["fields"].keys()) == ["z", "a", "m"]


# ---------------------------------------------------------------------------
# @tl.table class form
# ---------------------------------------------------------------------------


class TestTableDecorator:
    def test_simple_single_key(self):
        @table(key="user_id")
        class Users:
            user_id: str
            name: str

        assert isinstance(Users, TableSource)
        assert isinstance(Users, Table)
        d = Users.describe()
        assert d == {
            "name": "Users",
            "kind": "table",
            "key": ["user_id"],
            "mode": "append",
            "fields": {
                "user_id": {"type": "str", "optional": False, "desc": None},
                "name": {"type": "str", "optional": False, "desc": None},
            },
        }

    def test_composite_key(self):
        @table(key=["user_id", "merchant_id"])
        class UM:
            user_id: str
            merchant_id: str
            score: float

        assert UM.describe()["key"] == ["user_id", "merchant_id"]

    def test_ttl_stored(self):
        @table(key="user_id", ttl="30d")
        class Users:
            user_id: str

        d = Users.describe()
        assert d["ttl"] == "30d"
        assert Users._compile()["entity_ttl"] == "30d"

    def test_mode_append_default(self):
        @table(key="user_id")
        class Users:
            user_id: str

        assert Users._mode == "append"

    def test_mode_append_explicit(self):
        @table(key="user_id", mode="append")
        class Users:
            user_id: str

        assert Users._mode == "append"

    def test_mode_changelog_raises_not_implemented(self):
        with pytest.raises(NotImplementedError) as ei:
            @table(key="user_id", mode="changelog")
            class Users:
                user_id: str

        msg = str(ei.value)
        assert "changelog" in msg
        assert "v0.1" in msg

    def test_mode_invalid_raises_value_error(self):
        with pytest.raises(ValueError) as ei:
            @table(key="user_id", mode="weird")
            class Users:
                user_id: str

        assert "weird" in str(ei.value)

    def test_missing_key_raises_type_error(self):
        with pytest.raises(TypeError) as ei:
            @table()
            class Users:
                user_id: str

        assert "key" in str(ei.value)

    def test_key_not_in_schema_raises_with_suggestion(self):
        with pytest.raises(TypeError) as ei:
            @table(key="usr_id")
            class Users:
                user_id: str
                name: str

        msg = str(ei.value)
        assert "'usr_id'" in msg
        assert "'user_id'" in msg  # Levenshtein suggestion
        assert "available" in msg

    def test_composite_key_partial_missing_raises(self):
        with pytest.raises(TypeError):
            @table(key=["user_id", "bogus"])
            class UM:
                user_id: str
                merchant_id: str

    def test_composite_compile_emits_key_fields(self):
        @table(key=["user_id", "merchant_id"])
        class UM:
            user_id: str
            merchant_id: str

        reg = UM._compile()
        assert reg["key_field"] is None
        assert reg["key_fields"] == ["user_id", "merchant_id"]

    def test_single_key_compile_emits_key_field_string(self):
        @table(key="user_id")
        class Users:
            user_id: str

        reg = Users._compile()
        assert reg["key_field"] == "user_id"
        assert "key_fields" not in reg

    def test_repr(self):
        @table(key="user_id")
        class Users:
            user_id: str

        assert repr(Users) == "TableSource('Users', key=['user_id'])"

    def test_method_in_class_body_raises(self):
        with pytest.raises(TypeError):
            @table(key="user_id")
            class Bad:
                user_id: str

                def foo(self):  # pragma: no cover
                    return 1
