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
