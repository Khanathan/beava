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


class TestGroupByAgg:
    """Tests for group_by() free function and GroupedDataset."""

    def test_group_by_agg_returns_grouped_dataset(self):
        from tally._dataset import group_by, GroupedDataset
        from tally._operators import Count

        gd = group_by("user_id").agg(tx_count=Count(window="1h"))
        assert isinstance(gd, GroupedDataset)
        assert gd._key == "user_id"
        assert "tx_count" in gd._features

    def test_group_by_agg_features_match_operator_json(self):
        from tally._dataset import group_by
        from tally._operators import Count, Sum

        gd = group_by("user_id").agg(
            tx_count=Count(window="1h"),
            tx_sum=Sum("amount", window="1h"),
        )
        assert gd._features["tx_count"].to_json("tx_count") == {
            "name": "tx_count", "type": "count", "window": "1h"
        }
        assert gd._features["tx_sum"].to_json("tx_sum") == {
            "name": "tx_sum", "type": "sum", "field": "amount", "window": "1h"
        }

    def test_group_by_agg_rejects_non_operator(self):
        from tally._dataset import group_by

        with pytest.raises(TypeError):
            group_by("user_id").agg(bad="not an operator")


class TestDataset:
    """Tests for @tl.dataset decorator and DatasetDef."""

    def _make_source(self):
        from tally._source import source

        @source
        class RawTxns:
            pass

        return RawTxns

    def test_dataset_compile_basic(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        result = UserTxns._compile()
        assert result["name"] == "UserTxns"
        assert result["key_field"] == "user_id"
        assert result["depends_on"] == ["RawTxns"]
        assert len(result["features"]) == 1
        assert result["features"][0] == {"name": "tx_count", "type": "count", "window": "1h"}

    def test_dataset_depends_on_resolves_names(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[SourceA])
        class DS:
            features = group_by("key").agg(c=Count(window="1h"))

        assert DS._compile()["depends_on"] == ["SourceA"]

    def test_dataset_with_ttls(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src], entity_ttl="5m", history_ttl="72h")
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        result = UserTxns._compile()
        assert result["entity_ttl"] == "5m"
        assert result["history_ttl"] == "72h"

    def test_dataset_tally_stream_name_compat(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        assert UserTxns._tally_stream_name == "UserTxns"
        assert UserTxns._to_register_json() == UserTxns._compile()

    def test_dataset_collect_registrations_includes_upstream(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(tx_count=Count(window="1h"))

        regs = UserTxns._collect_registrations()
        assert len(regs) == 2  # source + dataset
        assert regs[0]["name"] == "RawTxns"
        assert regs[1]["name"] == "UserTxns"

    def test_dataset_with_derive_features(self):
        from tally._dataset import dataset, group_by
        from tally._operators import Count, Derive

        src = self._make_source()

        @dataset(depends_on=[src])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=Count(window="1h"),
                failed_count=Count(window="1h", where="status == 'failed'"),
            )
            failure_rate = Derive("failed_count / tx_count")

        result = UserTxns._compile()
        feature_names = [f["name"] for f in result["features"]]
        assert "tx_count" in feature_names
        assert "failed_count" in feature_names
        assert "failure_rate" in feature_names


class TestUnion:
    """Tests for tl.union() and UnionSource."""

    def test_union_basic(self):
        from tally._dataset import union, UnionSource
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        u = union(SourceA, SourceB)
        assert isinstance(u, UnionSource)
        names = u._get_depends_on_names()
        assert names == ["SourceA", "SourceB"]

    def test_dataset_with_union_depends_on(self):
        from tally._dataset import dataset, group_by, union
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[union(SourceA, SourceB)])
        class Combined:
            features = group_by("user_id").agg(total=Count(window="1h"))

        result = Combined._compile()
        assert sorted(result["depends_on"]) == ["SourceA", "SourceB"]

    def test_union_collect_registrations_includes_all_sources(self):
        from tally._dataset import dataset, group_by, union
        from tally._operators import Count
        from tally._source import source

        @source
        class SourceA:
            pass

        @source
        class SourceB:
            pass

        @dataset(depends_on=[union(SourceA, SourceB)])
        class Combined:
            features = group_by("user_id").agg(total=Count(window="1h"))

        regs = Combined._collect_registrations()
        reg_names = [r["name"] for r in regs]
        assert "SourceA" in reg_names
        assert "SourceB" in reg_names
        assert "Combined" in reg_names
