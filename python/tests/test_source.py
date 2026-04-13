"""Tests for the @source decorator (v2.0 API).

Replaces keyless stream tests from test_stream.py.

Verifies:
- @source creates a SourceDef
- SourceDef._tally_stream_name matches class name
- SourceDef._compile() / _to_register_json() produce correct JSON
- _collect_registrations() yields self
- @source with EventSet schema
- @source with entity_ttl / history_ttl options
- Bare vs parameterized decorator usage
"""

from __future__ import annotations

import tally as tl
from tally._source import source, SourceDef
from tally._schema import EventSet, Field


# -----------------------------------------------------------------------
# Basic @source decorator
# -----------------------------------------------------------------------


class TestSourceDecorator:
    def test_source_creates_source_def(self) -> None:
        """@source returns a SourceDef instance."""

        @source
        class RawEvents:
            pass

        assert isinstance(RawEvents, SourceDef)

    def test_source_name_is_class_name(self) -> None:
        """SourceDef._tally_stream_name matches the decorated class name."""

        @source
        class Transactions:
            pass

        assert Transactions._tally_stream_name == "Transactions"

    def test_source_repr(self) -> None:
        @source
        class Transactions:
            pass

        assert repr(Transactions) == "SourceDef('Transactions')"

    def test_bare_decorator_no_parens(self) -> None:
        """@source (no parentheses) works."""

        @source
        class Clicks:
            pass

        assert isinstance(Clicks, SourceDef)
        assert Clicks._tally_stream_name == "Clicks"

    def test_parameterized_decorator_empty(self) -> None:
        """@source() with empty parens works."""

        @source()
        class Clicks:
            pass

        assert isinstance(Clicks, SourceDef)


# -----------------------------------------------------------------------
# _compile / _to_register_json
# -----------------------------------------------------------------------


class TestSourceCompile:
    def test_compile_produces_keyless_json(self) -> None:
        """Compiled JSON has key_field=None and empty features."""

        @source
        class RawEvents:
            pass

        j = RawEvents._compile()
        assert j["name"] == "RawEvents"
        assert j["key_field"] is None
        assert j["features"] == []

    def test_to_register_json_matches_compile(self) -> None:
        """_to_register_json() delegates to _compile()."""

        @source
        class RawEvents:
            pass

        assert RawEvents._to_register_json() == RawEvents._compile()

    def test_compile_with_entity_ttl(self) -> None:
        @source(entity_ttl="5m")
        class Events:
            pass

        j = Events._compile()
        assert j["entity_ttl"] == "5m"

    def test_compile_with_history_ttl(self) -> None:
        @source(history_ttl="72h")
        class Events:
            pass

        j = Events._compile()
        assert j["history_ttl"] == "72h"

    def test_compile_with_both_ttls(self) -> None:
        @source(entity_ttl="10m", history_ttl="48h")
        class Events:
            pass

        j = Events._compile()
        assert j["entity_ttl"] == "10m"
        assert j["history_ttl"] == "48h"

    def test_compile_without_ttls_omits_keys(self) -> None:
        @source
        class Events:
            pass

        j = Events._compile()
        assert "entity_ttl" not in j
        assert "history_ttl" not in j


# -----------------------------------------------------------------------
# _collect_registrations
# -----------------------------------------------------------------------


class TestSourceCollectRegistrations:
    def test_collect_yields_self(self) -> None:
        """_collect_registrations returns a list containing only self's JSON."""

        @source
        class RawEvents:
            pass

        regs = RawEvents._collect_registrations()
        assert len(regs) == 1
        assert regs[0]["name"] == "RawEvents"
        assert regs[0]["key_field"] is None


# -----------------------------------------------------------------------
# EventSet schema support
# -----------------------------------------------------------------------


class TestSourceWithEventSet:
    def test_source_with_event_schema(self) -> None:
        """@source picks up EventSet from 'event' class attribute."""

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        @source
        class Transactions:
            event = TxnEvent

        assert Transactions._event_schema is TxnEvent

    def test_source_without_event_schema(self) -> None:
        """@source with no 'event' attribute has None schema."""

        @source
        class RawEvents:
            pass

        assert RawEvents._event_schema is None

    def test_source_non_eventset_event_attr_ignored(self) -> None:
        """Non-EventSet 'event' attribute is ignored."""

        @source
        class RawEvents:
            event = "not a schema"

        assert RawEvents._event_schema is None
