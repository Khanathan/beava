"""Tests for the new v2.0 API: EventSet, FeatureSet, Field, @tl.source, @tl.dataset."""

from __future__ import annotations

import pytest


class TestSchema:
    """Tests for EventSet, FeatureSet, and Field from _schema.py."""

    def test_eventset_collects_fields(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        assert "user_id" in TxnEvent._fields
        assert "amount" in TxnEvent._fields
        assert len(TxnEvent._fields) == 2

    def test_featureset_collects_fields(self):
        from tally._schema import FeatureSet, Field

        class TxnFeatures(FeatureSet):
            tx_count: int = Field()

        assert "tx_count" in TxnFeatures._fields
        assert len(TxnFeatures._fields) == 1

    def test_field_stores_attributes(self):
        from tally._schema import Field

        f = Field(dtype=float, description="amount in USD", default=0.0)
        assert f.dtype is float
        assert f.description == "amount in USD"
        assert f.default == 0.0

    def test_eventset_instantiation(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        evt = TxnEvent(user_id="u1", amount=50.0)
        assert evt.user_id == "u1"
        assert evt.amount == 50.0

    def test_eventset_missing_required_raises(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        with pytest.raises(TypeError):
            TxnEvent(user_id="u1")  # missing amount

    def test_field_infers_dtype_from_annotation(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        assert TxnEvent._fields["user_id"].dtype is str
        assert TxnEvent._fields["amount"].dtype is float

    def test_field_with_default_is_optional(self):
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field(default=0.0)

        evt = TxnEvent(user_id="u1")
        assert evt.amount == 0.0

    def test_bare_annotation_creates_field(self):
        """Annotation without explicit Field() should auto-create a Field."""
        from tally._schema import EventSet, Field

        class TxnEvent(EventSet):
            user_id: str
            amount: float = Field()

        assert "user_id" in TxnEvent._fields
        assert "amount" in TxnEvent._fields


class TestSource:
    """Tests for @tl.source decorator from _source.py."""

    def test_source_compile_basic(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        result = Transactions._compile()
        assert result == {"name": "Transactions", "key_field": None, "features": []}

    def test_source_stores_event_schema(self):
        from tally._schema import EventSet, Field
        from tally._source import source

        class TxnEvent(EventSet):
            user_id: str = Field()

        @source
        class Transactions:
            event = TxnEvent

        assert Transactions._event_schema is TxnEvent

    def test_source_has_tally_stream_name(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        assert Transactions._tally_stream_name == "Transactions"

    def test_source_to_register_json_compat(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        assert Transactions._to_register_json() == Transactions._compile()

    def test_source_collect_registrations(self):
        from tally._source import source

        @source
        class Transactions:
            pass

        regs = Transactions._collect_registrations()
        assert len(regs) == 1
        assert regs[0] == Transactions._compile()

    def test_source_with_entity_ttl(self):
        from tally._source import source

        @source(entity_ttl="5m")
        class Transactions:
            pass

        result = Transactions._compile()
        assert result["entity_ttl"] == "5m"

    def test_source_with_history_ttl(self):
        from tally._source import source

        @source(history_ttl="72h")
        class Transactions:
            pass

        result = Transactions._compile()
        assert result["history_ttl"] == "72h"
