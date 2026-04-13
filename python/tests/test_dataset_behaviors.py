"""Behavioral tests migrated from the old DataFrame test file to the v2.0 API.

These tests verify pipeline behaviors (compilation, multi-stream, cascade,
aggregation, derives, error handling, deduplication) using the new
@tl.source/@tl.dataset/group_by API.

Tests NOT ported (old API surface only):
- Column operator overloading (col + 1, col > 5)
- Table getitem/setitem returning Column/setting Expr
- EventProxy attribute access (table.event["field"])
- Join construction and compilation (join API removed)
- Table.join with left/cross-key semantics
- Old decorator backward compatibility tests
- Table re-aggregation
"""

from __future__ import annotations

import pytest

import tally as tl
from tally._source import source, SourceDef
from tally._dataset import dataset, group_by, union, DatasetDef, GroupedDataset
from tally._operators import (
    Avg,
    Count,
    Derive,
    DistinctCount,
    Last,
    Lookup,
    Max,
    Min,
    Sum,
)


# -----------------------------------------------------------------------
# Source basics (migrated from TestStream)
# -----------------------------------------------------------------------


class TestSourceBasics:
    """Migrated from TestStream: source creation and registration."""

    def test_source_creates_keyless_source(self) -> None:
        """Stream("raw_events") -> @source class RawEvents."""

        @source
        class RawEvents:
            pass

        assert isinstance(RawEvents, SourceDef)
        assert RawEvents._name == "RawEvents"

    def test_source_register_json(self) -> None:
        """Stream._to_register_json -> SourceDef._compile."""

        @source
        class RawEvents:
            pass

        j = RawEvents._compile()
        assert j["name"] == "RawEvents"
        assert j["key_field"] is None
        assert j["features"] == []

    def test_source_collect_registrations(self) -> None:
        """SourceDef._collect_registrations returns self."""

        @source
        class RawEvents:
            pass

        regs = RawEvents._collect_registrations()
        assert len(regs) == 1
        assert regs[0]["name"] == "RawEvents"


# -----------------------------------------------------------------------
# GroupBy and aggregation (migrated from TestGroupBy)
# -----------------------------------------------------------------------


class TestGroupByBasics:
    """Migrated from TestGroupBy: group_by().agg() behavior."""

    def test_group_by_agg_creates_grouped_dataset(self) -> None:
        """GroupBy.agg -> group_by().agg() returns GroupedDataset."""
        gd = group_by("user_id").agg(
            tx_count=Count(window="1h"),
        )
        assert isinstance(gd, GroupedDataset)
        assert gd._key == "user_id"
        assert "tx_count" in gd._features

    def test_agg_multiple_features(self) -> None:
        """GroupBy.agg with multiple operators."""
        gd = group_by("user_id").agg(
            tx_count=Count(window="1h"),
            tx_sum=Sum("amount", window="1h"),
            avg_amount=Avg("amount", window="1h"),
        )
        assert len(gd._features) == 3
        assert isinstance(gd._features["tx_count"], Count)
        assert isinstance(gd._features["tx_sum"], Sum)
        assert isinstance(gd._features["avg_amount"], Avg)

    def test_agg_rejects_non_operator(self) -> None:
        """GroupBy.agg raises TypeError for non-operator values."""
        gd = group_by("user_id")
        with pytest.raises(TypeError, match="must be an OperatorBase"):
            gd.agg(bad=42)

    def test_group_by_repr(self) -> None:
        gd = group_by("uid")
        assert "uid" in repr(gd)


# -----------------------------------------------------------------------
# Dataset registration JSON (migrated from TestTable)
# -----------------------------------------------------------------------


class TestDatasetRegistration:
    """Migrated from TestTable: register JSON compilation."""

    def test_register_json_basic(self) -> None:
        """Table._to_register_json -> DatasetDef._compile."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserFeatures:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
                tx_sum=tl.sum("amount", window="1h"),
            )

        j = UserFeatures._compile()
        assert j["name"] == "UserFeatures"
        assert j["key_field"] == "user_id"
        assert len(j["features"]) == 2
        feat_names = {f["name"] for f in j["features"]}
        assert feat_names == {"tx_count", "tx_sum"}

    def test_register_json_with_derive(self) -> None:
        """Table with derive -> dataset with derive feature."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserFeatures:
            features = group_by("user_id").agg(
                count_1h=tl.count(window="1h"),
            )
            velocity = tl.derive("count_1h / 24")

        j = UserFeatures._compile()
        derive_feat = [f for f in j["features"] if f["type"] == "derive"][0]
        assert derive_feat["name"] == "velocity"
        assert derive_feat["expr"] == "count_1h / 24"

    def test_register_json_with_source_depends(self) -> None:
        """Table(source=raw) -> dataset(depends_on=[raw])."""

        @source
        class RawEvents:
            pass

        @dataset(depends_on=[RawEvents])
        class T:
            features = group_by("uid").agg(
                c=tl.count(window="1h"),
            )

        j = T._compile()
        assert j["depends_on"] == ["RawEvents"]

    def test_register_json_ttl(self) -> None:
        """Table TTL -> dataset TTL."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], entity_ttl="5m", history_ttl="72h")
        class T:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = T._compile()
        assert j["entity_ttl"] == "5m"
        assert j["history_ttl"] == "72h"

    def test_register_json_no_ttl(self) -> None:
        """No TTL means no TTL fields in JSON."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class T:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        j = T._compile()
        assert "entity_ttl" not in j
        assert "history_ttl" not in j

    def test_dataset_repr(self) -> None:

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class MyDataset:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        assert "MyDataset" in repr(MyDataset)


# -----------------------------------------------------------------------
# Dataset with filter (migrated from TestStream.filter tests)
# -----------------------------------------------------------------------


class TestDatasetFilter:
    """Migrated from TestStream filter tests: filter expressions on datasets."""

    def test_filter_in_dataset(self) -> None:
        """Stream.filter -> dataset(filter=...)."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw], filter="status == 'failed'")
        class FailedTxns:
            features = group_by("user_id").agg(
                failed_count=tl.count(window="1h"),
            )

        j = FailedTxns._compile()
        assert j["filter"] == "status == 'failed'"

    def test_no_filter_omits_field(self) -> None:
        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class AllTxns:
            features = group_by("user_id").agg(c=tl.count(window="1h"))

        j = AllTxns._compile()
        assert "filter" not in j


# -----------------------------------------------------------------------
# Collect registrations / dependency chain (migrated from old DataFrame tests)
# -----------------------------------------------------------------------


class TestCollectRegistrations:
    """Migrated from old DataFrame collect_registrations tests."""

    def test_collect_registrations_chain(self) -> None:
        """enriched._collect_registrations -> chain of source + datasets."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class UserFeatures:
            features = group_by("user_id").agg(
                c=tl.count(window="1h"),
            )

        regs = UserFeatures._collect_registrations()
        assert len(regs) == 2
        names = [r["name"] for r in regs]
        assert names[0] == "Raw"
        assert names[1] == "UserFeatures"

    def test_collect_registrations_multi_dependency(self) -> None:
        """Multiple dependencies collected correctly."""

        @source
        class Txns:
            pass

        @source
        class Logins:
            pass

        @dataset(depends_on=[Txns, Logins])
        class UserRisk:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
            )

        regs = UserRisk._collect_registrations()
        names = [r["name"] for r in regs]
        assert "Txns" in names
        assert "Logins" in names
        assert "UserRisk" in names

    def test_shared_source_deduplicates(self) -> None:
        """Migrated from TestDeduplication: shared source should not duplicate."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class T1:
            features = group_by("uid").agg(c=tl.count(window="1h"))

        @dataset(depends_on=[Raw])
        class T2:
            features = group_by("mid").agg(c=tl.count(window="1h"))

        # Simulate what register_all does
        seen: set[str] = set()
        ordered: list[dict] = []
        for ds in [T1, T2]:
            for reg in ds._collect_registrations():
                name = reg["name"]
                if name not in seen:
                    seen.add(name)
                    ordered.append(reg)

        names = [r["name"] for r in ordered]
        assert names.count("Raw") == 1  # deduplicated
        assert "T1" in names
        assert "T2" in names

    def test_collect_registrations_union_source(self) -> None:
        """Union source collects all sub-sources."""

        @source
        class A:
            pass

        @source
        class B:
            pass

        @dataset(depends_on=[union(A, B)])
        class Combined:
            features = group_by("key").agg(c=tl.count(window="1h"))

        regs = Combined._collect_registrations()
        names = [r["name"] for r in regs]
        assert "A" in names
        assert "B" in names
        assert "Combined" in names


# -----------------------------------------------------------------------
# Full pipeline compilation (migrated from TestFullPipelineCompilation)
# -----------------------------------------------------------------------


class TestFullPipelineCompilation:
    """Migrated from TestFullPipelineCompilation: end-to-end pipeline compilation."""

    def test_source_to_dataset_pipeline(self) -> None:
        """raw -> map -> group_by -> table with features.

        In new API: source -> dataset(depends_on) with group_by.agg + derives.
        """

        @source
        class TransactionsRaw:
            pass

        @dataset(depends_on=[TransactionsRaw])
        class UserFeatures:
            features = group_by("user_id").agg(
                tx_count_1h=tl.count(window="1h"),
                tx_sum_1h=tl.sum("amount_usd", window="1h"),
                avg_amount=tl.avg("amount_usd", window="1h"),
                last_country=tl.last("country"),
            )
            velocity = tl.derive("tx_count_1h / 24")
            amount_vs_avg = tl.derive("_event.amount / avg_amount")

        regs = UserFeatures._collect_registrations()
        assert len(regs) == 2  # source + dataset

        # Check dataset registration
        ds_reg = regs[1]
        assert ds_reg["key_field"] == "user_id"
        feat_names = {f["name"] for f in ds_reg["features"]}
        assert "tx_count_1h" in feat_names
        assert "tx_sum_1h" in feat_names
        assert "velocity" in feat_names
        assert "amount_vs_avg" in feat_names

    def test_multi_stream_derive_pipeline(self) -> None:
        """Two tables joined into a view -> two sources + derive-only dataset."""

        @source
        class Txns:
            pass

        @source
        class Logins:
            pass

        @dataset(depends_on=[Txns])
        class TxnFeatures:
            features = group_by("user_id").agg(
                count_1h=tl.count(window="1h"),
                sum_1h=tl.sum("amount", window="1h"),
            )

        @dataset(depends_on=[Logins])
        class LoginFeatures:
            features = group_by("user_id").agg(
                login_count=tl.count(window="1h"),
            )

        @dataset(depends_on=[TxnFeatures, LoginFeatures])
        class UserRisk:
            tx_to_login = tl.derive("TxnFeatures.count_1h / LoginFeatures.login_count")
            suspicious = tl.derive(
                "TxnFeatures.count_1h > 10 and LoginFeatures.login_count < 2"
            )

        regs = UserRisk._collect_registrations()
        names = [r["name"] for r in regs]
        assert "Txns" in names
        assert "Logins" in names
        assert "TxnFeatures" in names
        assert "LoginFeatures" in names
        assert "UserRisk" in names

        # View-equivalent: derive-only dataset has no key
        view_reg = regs[-1]
        assert view_reg["key_field"] is None
        assert len(view_reg["features"]) == 2

    def test_all_operator_types_in_pipeline(self) -> None:
        """Pipeline with count, sum, avg, min, max, distinct_count, last operators."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Features:
            features = group_by("user_id").agg(
                tx_count=tl.count(window="1h"),
                tx_sum=tl.sum("amount", window="1h"),
                tx_avg=tl.avg("amount", window="1h"),
                tx_min=tl.min("amount", window="1h"),
                tx_max=tl.max("amount", window="1h"),
                unique_merchants=tl.distinct_count("merchant_id", window="24h"),
                last_country=tl.last("country"),
            )

        j = Features._compile()
        feat_types = {f["name"]: f["type"] for f in j["features"]}
        assert feat_types["tx_count"] == "count"
        assert feat_types["tx_sum"] == "sum"
        assert feat_types["tx_avg"] == "avg"
        assert feat_types["tx_min"] == "min"
        assert feat_types["tx_max"] == "max"
        assert feat_types["unique_merchants"] == "distinct_count"
        assert feat_types["last_country"] == "last"

    def test_derive_expressions_compile(self) -> None:
        """Multiple derive expressions compile to correct JSON."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Features:
            features = group_by("user_id").agg(
                count_30m=tl.count(window="30m"),
                count_1h=tl.count(window="1h"),
                count_24h=tl.count(window="24h"),
                failed_30m=tl.count(window="30m", where="status == 'failed'"),
            )
            failure_rate = tl.derive("failed_30m / count_30m")
            velocity_spike = tl.derive("(count_1h / 1) / (count_24h / 24)")

        j = Features._compile()
        derives = {f["name"]: f for f in j["features"] if f["type"] == "derive"}
        assert derives["failure_rate"]["expr"] == "failed_30m / count_30m"
        assert derives["velocity_spike"]["expr"] == "(count_1h / 1) / (count_24h / 24)"

    def test_where_clause_in_aggregation(self) -> None:
        """Where clause on count operator compiles correctly."""

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Features:
            features = group_by("user_id").agg(
                failed_count=tl.count(window="30m", where="status == 'failed'"),
            )

        j = Features._compile()
        feat = j["features"][0]
        assert feat["type"] == "count"
        assert feat["where"] == "status == 'failed'"


# -----------------------------------------------------------------------
# Projection (select/drop) in compiled output
# -----------------------------------------------------------------------


class TestProjection:
    """Migrated projection behavior from DataFrame API."""

    def test_select_projection(self) -> None:

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Features:
            features = group_by("user_id").agg(
                a=tl.count(window="1h"),
                b=tl.sum("amount", window="1h"),
            )

        projected = Features.select(["a"])
        j = projected._compile()
        assert j["projection"] == {"select": ["a"]}

    def test_drop_projection(self) -> None:

        @source
        class Raw:
            pass

        @dataset(depends_on=[Raw])
        class Features:
            features = group_by("user_id").agg(
                a=tl.count(window="1h"),
                b=tl.sum("amount", window="1h"),
            )

        projected = Features.drop(["b"])
        j = projected._compile()
        assert j["projection"] == {"drop": ["b"]}


# -----------------------------------------------------------------------
# Error cases
# -----------------------------------------------------------------------


class TestErrorCases:
    """Error handling tests migrated from DataFrame test classes."""

    def test_dataset_agg_rejects_non_operator(self) -> None:
        """GroupBy.agg rejects non-operator values."""
        gd = group_by("user_id")
        with pytest.raises(TypeError, match="must be an OperatorBase"):
            gd.agg(bad="not_an_operator")

    def test_dataset_agg_rejects_int(self) -> None:
        gd = group_by("user_id")
        with pytest.raises(TypeError):
            gd.agg(bad=42)
