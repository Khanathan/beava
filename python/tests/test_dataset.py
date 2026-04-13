"""Tests for the @dataset decorator, group_by, union, and DatasetDef (v2.0 API).

Replaces keyed stream tests from test_stream.py and all tests from test_view.py.

Verifies:
- @dataset(depends_on=[...]) creates a DatasetDef
- group_by("key").agg(...) defines keyed aggregation
- DatasetDef._compile() produces correct RegisterRequest JSON
- _collect_registrations() yields self + dependencies
- Multiple operators in agg()
- Derive features alongside agg features
- Derive-only dataset (view-equivalent)
- tl.union() produces multi-parent depends_on
- select() / drop() projection
- Error cases
- TTL fields
- filter parameter
"""

from __future__ import annotations

import pytest

import tally as tl
from tally._source import source, SourceDef
from tally._dataset import dataset, group_by, union, DatasetDef, GroupedDataset, UnionSource
from tally._operators import Count, Sum, Avg, Min, Max, DistinctCount, Last, Derive, Lookup
from tally._schema import EventSet, FeatureSet, Field


# -----------------------------------------------------------------------
# Basic @dataset decorator
# -----------------------------------------------------------------------


class TestDatasetDecorator:
    def test_dataset_creates_dataset_def(self) -> None:
        """@dataset returns a DatasetDef instance."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )

        assert isinstance(UserTxns, DatasetDef)

    def test_dataset_name_is_class_name(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )

        assert UserTxns._tally_stream_name == "UserTxns"

    def test_dataset_repr(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )

        assert repr(UserTxns) == "DatasetDef('UserTxns')"

    def test_dataset_stores_depends_on(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )

        assert UserTxns._depends_on == [Raw]


# -----------------------------------------------------------------------
# group_by and agg
# -----------------------------------------------------------------------


class TestGroupByAgg:
    def test_group_by_returns_grouped_dataset(self) -> None:
        g = group_by("user_id")
        assert isinstance(g, GroupedDataset)
        assert g._key == "user_id"

    def test_agg_returns_new_grouped_dataset(self) -> None:
        g = group_by("user_id").agg(
            tx_count=tl.count(window="1h"),
        )
        assert isinstance(g, GroupedDataset)
        assert "tx_count" in g._features

    def test_agg_multiple_operators(self) -> None:
        """Multiple operators in agg() are all captured."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="30m"),
                tx_sum=tl.sum("amount", window="1h"),
                tx_avg=tl.avg("amount", window="1h"),
                tx_min=tl.min("amount", window="1h"),
                tx_max=tl.max("amount", window="1h"),
                unique_merchants=tl.distinct_count("merchant_id", window="24h"),
                last_country=tl.last("country"),
            )

        j = UserTxns._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert feature_names == {
            "tx_count", "tx_sum", "tx_avg", "tx_min", "tx_max",
            "unique_merchants", "last_country",
        }

    def test_agg_rejects_non_operator(self) -> None:
        """agg() with non-OperatorBase value raises TypeError."""
        with pytest.raises(TypeError, match="OperatorBase"):
            group_by("user_id").agg(bad=42)

    def test_grouped_dataset_repr(self) -> None:
        g = group_by("user_id").agg(tx_count=tl.count(window="1h"))
        assert "user_id" in repr(g)


# -----------------------------------------------------------------------
# _compile / _to_register_json
# -----------------------------------------------------------------------


class TestDatasetCompile:
    def test_basic_compile(self) -> None:
        """Compiled JSON has correct structure."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="30m"),
            )

        j = UserTxns._compile()
        assert j["name"] == "UserTxns"
        assert j["key_field"] == "user_id"
        assert j["depends_on"] == ["Raw"]
        assert len(j["features"]) == 1
        assert j["features"][0] == {"name": "tx_count", "type": "count", "window": "30m"}

    def test_to_register_json_matches_compile(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("k").agg(c=tl.count(window="1h"))

        assert D._to_register_json() == D._compile()

    def test_compile_multi_feature(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="30m"),
                tx_sum=tl.sum("amount", window="1h"),
            )
            rate = tl.derive("tx_sum / tx_count")

        j = UserTxns._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert feature_names == {"tx_count", "tx_sum", "rate"}

    def test_compile_with_where_clause(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(
                failed=tl.count(window="1h", where="status == 'failed'"),
            )

        j = D._compile()
        feat = j["features"][0]
        assert feat["where"] == "status == 'failed'"

    def test_compile_depends_on_multiple(self) -> None:
        """Multiple depends_on classes resolved to string names."""

        @source
        class A:
            pass

        @source
        class B:
            pass

        @dataset(depends_on=[A, B])
        class C:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = C._compile()
        assert j["depends_on"] == ["A", "B"]


# -----------------------------------------------------------------------
# Derive features (extra features alongside agg)
# -----------------------------------------------------------------------


class TestDatasetDerive:
    def test_derive_alongside_agg(self) -> None:
        """Derive features defined in class body alongside group_by.agg."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
                failed=tl.count(window="1h", where="status == 'failed'"),
            )
            failure_rate = tl.derive("failed / tx_count")

        j = UserTxns._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert "failure_rate" in feature_names
        derive_feat = [f for f in j["features"] if f["name"] == "failure_rate"][0]
        assert derive_feat["type"] == "derive"
        assert derive_feat["expr"] == "failed / tx_count"

    def test_multiple_derives(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(
                c_1h=tl.count(window="1h"),
                c_24h=tl.count(window="24h"),
            )
            velocity = tl.derive("(c_1h / 1) / (c_24h / 24)")
            spike = tl.derive("c_1h > 10")

        j = D._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert "velocity" in feature_names
        assert "spike" in feature_names

    def test_cross_stream_derive_expression(self) -> None:
        """Derive referencing another stream's feature (cross-stream)."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))
            ratio = tl.derive("OtherStream.feature / c")

        j = D._compile()
        derive_feat = [f for f in j["features"] if f["name"] == "ratio"][0]
        assert "OtherStream.feature" in derive_feat["expr"]


# -----------------------------------------------------------------------
# Derive-only dataset (view-equivalent)
# -----------------------------------------------------------------------


class TestDeriveOnlyDataset:
    """Tests for datasets with only derive features (no agg operators).

    This is the v2.0 equivalent of the old view decorator.
    """

    def test_derive_only_dataset_no_agg(self) -> None:
        """Dataset with derives but no group_by.agg is valid."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserRisk:
            score = tl.derive("Transactions.tx_count_1h > 10")

        assert isinstance(UserRisk, DatasetDef)
        j = UserRisk._compile()
        assert j["key_field"] is None  # No group_by means no key
        assert len(j["features"]) == 1
        assert j["features"][0]["type"] == "derive"

    def test_derive_only_with_lookup(self) -> None:
        """Dataset with lookup feature (view-equivalent)."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class FraudSignals:
            merchant_cb = tl.lookup(
                "MerchantActivity.chargeback_count_24h", on="merchant_id"
            )
            risk = tl.derive("Transactions.velocity_spike > 3 and merchant_cb > 5")

        j = FraudSignals._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert feature_names == {"merchant_cb", "risk"}

    def test_derive_only_multiple_derives(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Signals:
            a = tl.derive("x + y")
            b = tl.derive("x - y")

        j = Signals._compile()
        assert len(j["features"]) == 2

    def test_empty_dataset_is_valid(self) -> None:
        """Dataset with no features at all is valid (like empty view)."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Empty:
            pass

        j = Empty._compile()
        assert j["features"] == []


# -----------------------------------------------------------------------
# tl.union
# -----------------------------------------------------------------------


class TestUnion:
    def test_union_creates_union_source(self) -> None:
        @source
        class A:
            pass

        @source
        class B:
            pass

        u = union(A, B)
        assert isinstance(u, UnionSource)

    def test_union_in_depends_on_flattens(self) -> None:
        """union(A, B) in depends_on produces depends_on=["A", "B"]."""

        @source
        class A:
            pass

        @source
        class B:
            pass

        @dataset(depends_on=[union(A, B)])
        class Combined:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = Combined._compile()
        assert j["depends_on"] == ["A", "B"]

    def test_union_repr(self) -> None:
        @source
        class A:
            pass

        @source
        class B:
            pass

        u = union(A, B)
        assert "A" in repr(u)
        assert "B" in repr(u)


# -----------------------------------------------------------------------
# _collect_registrations
# -----------------------------------------------------------------------


class TestCollectRegistrations:
    def test_collect_includes_self_and_deps(self) -> None:
        """_collect_registrations includes upstream SourceDef + self."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserTxns:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        regs = UserTxns._collect_registrations()
        names = [r["name"] for r in regs]
        assert names == ["Raw", "UserTxns"]

    def test_collect_deduplicates(self) -> None:
        """Same dependency referenced twice is only included once."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw, Raw])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        regs = D._collect_registrations()
        names = [r["name"] for r in regs]
        # Raw should appear once, plus D itself
        assert names.count("Raw") == 1

    def test_collect_with_union_deps(self) -> None:
        """Union sources are flattened in _collect_registrations."""

        @source
        class A:
            pass

        @source
        class B:
            pass

        @dataset(depends_on=[union(A, B)])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        regs = D._collect_registrations()
        names = [r["name"] for r in regs]
        assert "A" in names
        assert "B" in names
        assert "D" in names

    def test_collect_transitive_deps(self) -> None:
        """Transitive dependencies are collected through chain."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Mid:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        @dataset(depends_on=[Mid])
        class Final:
            score = tl.derive("Mid.c > 5")

        regs = Final._collect_registrations()
        names = [r["name"] for r in regs]
        # Raw -> Mid -> Final
        assert "Raw" in names
        assert "Mid" in names
        assert "Final" in names


# -----------------------------------------------------------------------
# TTL fields
# -----------------------------------------------------------------------


class TestDatasetTtl:
    def test_entity_ttl(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], entity_ttl="5m")
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert j["entity_ttl"] == "5m"

    def test_history_ttl(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], history_ttl="72h")
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert j["history_ttl"] == "72h"

    def test_both_ttls(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], entity_ttl="10m", history_ttl="48h")
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert j["entity_ttl"] == "10m"
        assert j["history_ttl"] == "48h"

    def test_no_ttls_omits_keys(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert "entity_ttl" not in j
        assert "history_ttl" not in j


# -----------------------------------------------------------------------
# Filter parameter
# -----------------------------------------------------------------------


class TestDatasetFilter:
    def test_filter_in_compile(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], filter="_event.status == 'failed'")
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert j["filter"] == "_event.status == 'failed'"

    def test_no_filter_omits_key(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = D._compile()
        assert "filter" not in j


# -----------------------------------------------------------------------
# Projection (select / drop)
# -----------------------------------------------------------------------


class TestDatasetProjection:
    def test_select_produces_projection(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(
                a=tl.count(window="1h"),
                b=tl.sum("amount", window="1h"),
            )

        selected = D.select(["a"])
        j = selected._compile()
        assert j["projection"] == {"select": ["a"]}

    def test_drop_produces_projection(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(
                a=tl.count(window="1h"),
                b=tl.sum("amount", window="1h"),
            )

        dropped = D.drop(["b"])
        j = dropped._compile()
        assert j["projection"] == {"drop": ["b"]}

    def test_select_returns_new_dataset_def(self) -> None:
        """select() returns a new DatasetDef, does not mutate original."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(a=tl.count(window="1h"))

        selected = D.select(["a"])
        assert selected is not D
        assert D._projection is None
        assert selected._projection == {"select": ["a"]}

    def test_no_projection_omits_key(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(a=tl.count(window="1h"))

        j = D._compile()
        assert "projection" not in j


# -----------------------------------------------------------------------
# EventSet / FeatureSet schema
# -----------------------------------------------------------------------


class TestDatasetSchema:
    def test_dataset_with_event_schema(self) -> None:
        class TxnEvent(EventSet):
            user_id: str = Field()
            amount: float = Field()

        @source
        class Raw:
            event = TxnEvent

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("user_id").agg(c=tl.count(window="1h"))

        # Source has the schema, dataset inherits dependency
        assert Raw._event_schema is TxnEvent


# -----------------------------------------------------------------------
# Error cases
# -----------------------------------------------------------------------


class TestDatasetErrors:
    def test_agg_non_operator_raises(self) -> None:
        """agg() with non-OperatorBase raises TypeError."""
        with pytest.raises(TypeError, match="OperatorBase"):
            group_by("uid").agg(bad="not_an_operator")

    def test_non_operator_class_attrs_ignored(self) -> None:
        """Non-operator, non-features class attributes are ignored."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class D:
            features = group_by("uid").agg(c=tl.count(window="1h"))
            helper_value = 42
            helper_string = "not an operator"

        j = D._compile()
        feature_names = {f["name"] for f in j["features"]}
        assert feature_names == {"c"}
        assert "helper_value" not in feature_names


# -----------------------------------------------------------------------
# Full CLAUDE.md-equivalent example
# -----------------------------------------------------------------------


class TestFullExample:
    def test_fraud_detection_pipeline(self) -> None:
        """Full pipeline mirroring the CLAUDE.md Transactions example."""

        @source
        class RawTxns:
            pass

        @dataset(depends_on=[RawTxns])
        class Transactions:
            features = group_by("user_id").agg(
                tx_count_30m=tl.count(window="30m"),
                tx_count_1h=tl.count(window="1h"),
                tx_count_24h=tl.count(window="24h"),
                tx_sum_1h=tl.sum("amount", window="1h"),
                avg_amount_1h=tl.avg("amount", window="1h"),
                max_amount_24h=tl.max("amount", window="24h"),
                unique_merchants=tl.distinct_count("merchant_id", window="24h"),
                failed_tx_30m=tl.count(window="30m", where="status == 'failed'"),
                last_country=tl.last("country"),
                last_merchant=tl.last("merchant_id"),
            )
            failure_rate = tl.derive("failed_tx_30m / tx_count_30m")
            velocity_spike = tl.derive("(tx_count_1h / 1) / (tx_count_24h / 24)")

        j = Transactions._compile()
        assert j["name"] == "Transactions"
        assert j["key_field"] == "user_id"
        assert j["depends_on"] == ["RawTxns"]
        # 10 agg + 2 derive = 12
        assert len(j["features"]) == 12

        feature_names = {f["name"] for f in j["features"]}
        assert "tx_count_30m" in feature_names
        assert "failure_rate" in feature_names
        assert "velocity_spike" in feature_names
