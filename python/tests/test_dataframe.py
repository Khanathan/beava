"""Tests for the DataFrame-style API (Stream, Table, GroupBy, JoinedTable).

Verifies:
- Stream: source, map, filter, group_by
- Table: getitem/setitem, count, join, lookup, _to_register_json
- GroupBy: agg -> Table
- JoinedTable: compilation to view JSON with lookups
- Full pipeline compilation: DAG walks, deduplication, dependency order
- Backward compatibility: @st.stream decorator output matches Table output
"""

from __future__ import annotations

import pytest

from tally._dataframe import Dataset, GroupBy, JoinedTable, Stream, Table
from tally._expr import Column, Expr
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
# Stream basics
# -----------------------------------------------------------------------


class TestStream:
    def test_source_creates_keyless_stream(self) -> None:
        s = Stream("raw_events")
        assert s._name == "raw_events"
        assert s._parent is None

    def test_source_register_json(self) -> None:
        s = Stream("raw_events")
        j = s._to_register_json()
        assert j["name"] == "raw_events"
        assert j["key_field"] is None
        assert j["features"] == []
        assert "depends_on" not in j

    def test_getitem_returns_column(self) -> None:
        s = Stream("raw")
        col = s["amount"]
        assert isinstance(col, Column)
        assert col.name == "amount"

    def test_map_creates_child_stream(self) -> None:
        raw = Stream("raw")
        enriched = raw.map(amount_usd=raw["amount"] * raw["fx_rate"])
        assert enriched._parent is raw
        assert "amount_usd" in enriched._derives
        assert enriched._derives["amount_usd"] == "(amount * fx_rate)"

    def test_map_register_json(self) -> None:
        raw = Stream("raw")
        enriched = raw.map(
            amount_usd=raw["amount"] * raw["fx_rate"],
            is_high=raw["amount"] > 1000,
        )
        j = enriched._to_register_json()
        assert j["key_field"] is None
        assert j["depends_on"] == ["raw"]
        assert len(j["features"]) == 2
        feat_names = {f["name"] for f in j["features"]}
        assert feat_names == {"amount_usd", "is_high"}
        # Check that features are derive type
        for f in j["features"]:
            assert f["type"] == "derive"

    def test_map_string_expr(self) -> None:
        raw = Stream("raw")
        enriched = raw.map(double="amount * 2")
        assert enriched._derives["double"] == "amount * 2"

    def test_map_rejects_non_expr(self) -> None:
        raw = Stream("raw")
        with pytest.raises(TypeError, match="must be Expr or str"):
            raw.map(bad=42)

    def test_filter_creates_child_stream(self) -> None:
        raw = Stream("raw")
        failed = raw.filter(raw["status"] == "failed")
        assert failed._parent is raw
        assert failed._filter_expr == "(status == 'failed')"

    def test_filter_register_json(self) -> None:
        raw = Stream("raw")
        failed = raw.filter(raw["status"] == "failed")
        j = failed._to_register_json()
        assert j["depends_on"] == ["raw"]
        assert j["filter"] == "(status == 'failed')"

    def test_filter_string_expr(self) -> None:
        raw = Stream("raw")
        failed = raw.filter("status == 'failed'")
        assert failed._filter_expr == "status == 'failed'"

    def test_group_by_returns_groupby(self) -> None:
        raw = Stream("raw")
        gb = raw.group_by("user_id")
        assert isinstance(gb, GroupBy)
        assert gb._key == "user_id"
        assert gb._source is raw

    def test_collect_registrations_chain(self) -> None:
        raw = Stream("raw")
        enriched = raw.map(x=raw["a"] + 1)
        regs = enriched._collect_registrations()
        assert len(regs) == 2
        assert regs[0]["name"] == "raw"
        assert regs[1]["name"] == "raw__mapped"

    def test_repr(self) -> None:
        s = Stream("raw_events")
        assert "raw_events" in repr(s)


# -----------------------------------------------------------------------
# GroupBy
# -----------------------------------------------------------------------


class TestGroupBy:
    def test_agg_returns_table(self) -> None:
        raw = Stream("raw")
        table = raw.group_by("user_id").agg(
            tx_count=Count(window="1h"),
        )
        assert isinstance(table, Table)
        assert table._key == "user_id"
        assert "tx_count" in table._features

    def test_agg_multiple_features(self) -> None:
        raw = Stream("raw")
        table = raw.group_by("user_id").agg(
            tx_count=Count(window="1h"),
            tx_sum=Sum("amount", window="1h"),
            avg_amount=Avg("amount", window="1h"),
        )
        assert len(table._features) == 3
        assert isinstance(table._features["tx_count"], Count)
        assert isinstance(table._features["tx_sum"], Sum)
        assert isinstance(table._features["avg_amount"], Avg)

    def test_agg_rejects_non_operator(self) -> None:
        raw = Stream("raw")
        gb = raw.group_by("user_id")
        with pytest.raises(TypeError, match="must be an OperatorBase"):
            gb.agg(bad=42)

    def test_auto_name(self) -> None:
        raw = Stream("transactions")
        table = raw.group_by("user_id").agg(c=Count(window="1h"))
        assert table._name == "transactions_by_user_id"

    def test_repr(self) -> None:
        raw = Stream("raw")
        gb = raw.group_by("uid")
        assert "uid" in repr(gb)


# -----------------------------------------------------------------------
# Table basics
# -----------------------------------------------------------------------


class TestTable:
    def test_direct_creation(self) -> None:
        t = Table("UserFeatures", key="user_id")
        assert t._name == "UserFeatures"
        assert t._key == "user_id"
        assert len(t._features) == 0

    def test_getitem_returns_column(self) -> None:
        t = Table("T", key="uid")
        col = t["amount"]
        assert isinstance(col, Column)
        assert col.name == "amount"
        assert col.table is t

    def test_setitem_operator(self) -> None:
        t = Table("T", key="uid")
        t["tx_count"] = Count(window="1h")
        assert "tx_count" in t._features
        assert isinstance(t._features["tx_count"], Count)

    def test_setitem_expr(self) -> None:
        t = Table("T", key="uid")
        t["velocity"] = t["count_1h"] / 24
        assert "velocity" in t._features
        assert isinstance(t._features["velocity"], Derive)
        assert t._features["velocity"].expr == "(count_1h / 24)"

    def test_setitem_rejects_bad_type(self) -> None:
        t = Table("T", key="uid")
        with pytest.raises(TypeError, match="Cannot assign"):
            t["bad"] = 42

    def test_count_method(self) -> None:
        t = Table("T", key="uid")
        op = t.count(window="1h")
        assert isinstance(op, Count)
        assert op.window == "1h"

    def test_count_with_where(self) -> None:
        t = Table("T", key="uid")
        op = t.count(window="1h", where="status == 'failed'")
        assert isinstance(op, Count)
        assert op.where_clause == "status == 'failed'"

    def test_event_proxy(self) -> None:
        t = Table("T", key="uid")
        col = t.event["amount"]
        assert col.name == "_event.amount"

    def test_tally_stream_name(self) -> None:
        t = Table("MyTable", key="uid")
        assert t._tally_stream_name == "MyTable"

    def test_register_json_basic(self) -> None:
        t = Table("UserFeatures", key="user_id")
        t["tx_count"] = Count(window="1h")
        t["tx_sum"] = Sum("amount", window="1h")
        j = t._to_register_json()
        assert j["name"] == "UserFeatures"
        assert j["key_field"] == "user_id"
        assert len(j["features"]) == 2
        feat_names = {f["name"] for f in j["features"]}
        assert feat_names == {"tx_count", "tx_sum"}

    def test_register_json_with_derive(self) -> None:
        t = Table("T", key="uid")
        t["count_1h"] = Count(window="1h")
        t["velocity"] = t["count_1h"] / 24
        j = t._to_register_json()
        derive_feat = [f for f in j["features"] if f["type"] == "derive"][0]
        assert derive_feat["name"] == "velocity"
        assert derive_feat["expr"] == "(count_1h / 24)"

    def test_register_json_with_source(self) -> None:
        raw = Stream("raw_events")
        t = Table("T", key="uid", source=raw)
        t["c"] = Count(window="1h")
        j = t._to_register_json()
        assert j["depends_on"] == ["raw_events"]

    def test_register_json_ttl(self) -> None:
        t = Table("T", key="uid", entity_ttl="5m", history_ttl="72h")
        j = t._to_register_json()
        assert j["entity_ttl"] == "5m"
        assert j["history_ttl"] == "72h"

    def test_register_json_no_ttl(self) -> None:
        t = Table("T", key="uid")
        j = t._to_register_json()
        assert "entity_ttl" not in j
        assert "history_ttl" not in j

    def test_lookup(self) -> None:
        merchants = Table("MerchantActivity", key="merchant_id")
        merchants["cbacks"] = Count(window="24h", where="type == 'chargeback'")

        users = Table("Users", key="user_id")
        op = users.lookup(merchants["cbacks"], on="merchant_id")
        assert isinstance(op, Lookup)
        assert op.target == "MerchantActivity.cbacks"
        assert op.on == "merchant_id"

    def test_repr(self) -> None:
        t = Table("MyTable", key="uid")
        assert "MyTable" in repr(t)


# -----------------------------------------------------------------------
# Table.join -> JoinedTable
# -----------------------------------------------------------------------


class TestJoinedTable:
    def test_join_creates_joined_table(self) -> None:
        t1 = Table("Txns", key="user_id")
        t1["count_1h"] = Count(window="1h")
        t2 = Table("Logins", key="user_id")
        t2["login_count"] = Count(window="1h")
        jt = t1.join(t2, on="user_id")
        assert isinstance(jt, JoinedTable)
        assert jt._left is t1
        assert jt._right is t2

    def test_join_default_key(self) -> None:
        t1 = Table("Txns", key="user_id")
        t2 = Table("Logins", key="user_id")
        jt = t1.join(t2)
        assert jt._join_key == "user_id"

    def test_joined_getitem(self) -> None:
        t1 = Table("T1", key="uid")
        t2 = Table("T2", key="uid")
        jt = t1.join(t2)
        col = jt["some_feat"]
        assert isinstance(col, Column)

    def test_joined_setitem_expr(self) -> None:
        t1 = Table("Txns", key="uid")
        t1["count_1h"] = Count(window="1h")
        t2 = Table("Logins", key="uid")
        t2["login_count"] = Count(window="1h")
        jt = t1.join(t2)
        jt["ratio"] = jt["count_1h"] / jt["login_count"]
        assert "ratio" in jt._extra_features
        assert isinstance(jt._extra_features["ratio"], Derive)

    def test_joined_setitem_operator(self) -> None:
        t1 = Table("T1", key="uid")
        t2 = Table("T2", key="uid")
        jt = t1.join(t2)
        jt["score"] = Derive("a + b")
        assert "score" in jt._extra_features

    def test_cross_key_join_register_json(self) -> None:
        """Cross-key join: right-side features become lookups."""
        users = Table("Users", key="user_id")
        users["count_1h"] = Count(window="1h")
        merchants = Table("Merchants", key="merchant_id")
        merchants["cbacks"] = Count(window="24h")
        merchants["volume"] = Sum("amount", window="24h")

        jt = users.join(merchants, on="merchant_id", how="left")
        jt["risk"] = Derive("count_1h > 10 and cbacks > 5")

        j = jt._to_register_json()
        assert j["type"] == "view"
        assert j["key_field"] == "user_id"

        # Right-side features should be lookups
        feat_types = {f["name"]: f["type"] for f in j["features"]}
        assert feat_types["cbacks"] == "lookup"
        assert feat_types["volume"] == "lookup"
        assert feat_types["risk"] == "derive"

        # Check lookup targets
        lookup_feats = [f for f in j["features"] if f["type"] == "lookup"]
        for lf in lookup_feats:
            assert lf["on"] == "merchant_id"
            assert lf["target"].startswith("Merchants.")

    def test_same_key_join_register_json(self) -> None:
        """Same-key join: view with derive features only."""
        txns = Table("Txns", key="user_id")
        txns["count_1h"] = Count(window="1h")
        logins = Table("Logins", key="user_id")
        logins["login_count"] = Count(window="1h")

        jt = txns.join(logins, on="user_id")
        jt["ratio"] = Derive("Txns.count_1h / Logins.login_count")

        j = jt._to_register_json()
        assert j["type"] == "view"
        assert j["key_field"] == "user_id"
        assert len(j["features"]) == 1  # only the derive
        assert j["features"][0]["name"] == "ratio"

    def test_collect_registrations(self) -> None:
        raw = Stream("raw")
        t1 = Table("T1", key="uid", source=raw)
        t1["c"] = Count(window="1h")
        t2 = Table("T2", key="uid")
        t2["c2"] = Count(window="1h")
        jt = t1.join(t2)
        regs = jt._collect_registrations()
        names = [r["name"] for r in regs]
        # Should include: raw, T1, T2, joined view
        assert "raw" in names
        assert "T1" in names
        assert "T2" in names
        assert jt._name in names

    def test_repr(self) -> None:
        t1 = Table("T1", key="uid")
        t2 = Table("T2", key="uid")
        jt = t1.join(t2)
        assert "T1" in repr(jt) and "T2" in repr(jt)


# -----------------------------------------------------------------------
# Full pipeline compilation
# -----------------------------------------------------------------------


class TestFullPipelineCompilation:
    def test_stream_to_table_pipeline(self) -> None:
        """raw -> map -> group_by -> table with features."""
        raw = Stream("transactions_raw")
        enriched = raw.map(
            amount_usd=raw["amount"] * raw["fx_rate"],
            is_high_value=raw["amount"] > 1000,
        )
        user_features = enriched.group_by("user_id").agg(
            tx_count_1h=Count(window="1h"),
            tx_sum_1h=Sum("amount_usd", window="1h"),
            avg_amount=Avg("amount_usd", window="1h"),
            last_country=Last("country"),
        )
        user_features["velocity"] = user_features["tx_count_1h"] / 24
        user_features["amount_vs_avg"] = (
            user_features.event["amount"] / user_features["avg_amount"]
        )

        regs = user_features._collect_registrations()
        assert len(regs) == 3  # raw, enriched, table
        names = [r["name"] for r in regs]
        assert names[0] == "transactions_raw"
        assert names[1] == "transactions_raw__mapped"
        # Table name auto-generated
        assert "user_id" in names[2]

        # Check table registration
        table_reg = regs[2]
        assert table_reg["key_field"] == "user_id"
        feat_names = {f["name"] for f in table_reg["features"]}
        assert "tx_count_1h" in feat_names
        assert "tx_sum_1h" in feat_names
        assert "velocity" in feat_names
        assert "amount_vs_avg" in feat_names

    def test_join_pipeline(self) -> None:
        """Two tables joined into a view with derived features."""
        txns = Table("Transactions", key="user_id")
        txns["count_1h"] = Count(window="1h")
        txns["sum_1h"] = Sum("amount", window="1h")

        logins = Table("Logins", key="user_id")
        logins["login_count"] = Count(window="1h")

        risk = txns.join(logins, on="user_id")
        risk["tx_to_login"] = Derive("Transactions.count_1h / Logins.login_count")
        risk["suspicious"] = Derive(
            "Transactions.count_1h > 10 and Logins.login_count < 2"
        )

        regs = risk._collect_registrations()
        names = [r["name"] for r in regs]
        assert "Transactions" in names
        assert "Logins" in names
        # View
        view_reg = regs[-1]
        assert view_reg["type"] == "view"
        assert len(view_reg["features"]) == 2

    def test_cross_key_lookup_pipeline(self) -> None:
        """Table with cross-key lookup."""
        merchants = Table("MerchantActivity", key="merchant_id")
        merchants["cbacks_24h"] = Count(window="24h", where="type == 'chargeback'")

        users = Table("UserFeatures", key="user_id")
        users["count_1h"] = Count(window="1h")
        users["merchant_cbacks"] = users.lookup(merchants["cbacks_24h"], on="merchant_id")
        users["high_risk"] = (users["count_1h"] > 10) & (users["merchant_cbacks"] > 5)

        j = users._to_register_json()
        feat_types = {f["name"]: f["type"] for f in j["features"]}
        assert feat_types["count_1h"] == "count"
        assert feat_types["merchant_cbacks"] == "lookup"
        assert feat_types["high_risk"] == "derive"

        # Check lookup target
        lookup_feat = [f for f in j["features"] if f["name"] == "merchant_cbacks"][0]
        assert lookup_feat["target"] == "MerchantActivity.cbacks_24h"
        assert lookup_feat["on"] == "merchant_id"


# -----------------------------------------------------------------------
# Backward compatibility: Table vs @st.stream produce same JSON
# -----------------------------------------------------------------------


class TestBackwardCompatibility:
    def test_table_matches_decorator_json(self) -> None:
        """Table API produces JSON equivalent to @st.stream decorator."""
        from tally._stream import stream
        from tally._operators import Count, Sum, Derive

        # Decorator style
        @stream(key="user_id")
        class Transactions:
            tx_count_1h = Count(window="1h")
            tx_sum_1h = Sum("amount", window="1h")
            velocity = Derive("tx_count_1h / 24")

        decorator_json = Transactions._to_register_json()

        # Table style
        t = Table("Transactions", key="user_id")
        t["tx_count_1h"] = Count(window="1h")
        t["tx_sum_1h"] = Sum("amount", window="1h")
        t["velocity"] = Derive("tx_count_1h / 24")

        table_json = t._to_register_json()

        # Name and key should match
        assert decorator_json["name"] == table_json["name"]
        assert decorator_json["key_field"] == table_json["key_field"]

        # Features should match (compare as sets of dicts)
        dec_feats = {f["name"]: f for f in decorator_json["features"]}
        tab_feats = {f["name"]: f for f in table_json["features"]}
        assert dec_feats.keys() == tab_feats.keys()
        for name in dec_feats:
            assert dec_feats[name] == tab_feats[name], (
                f"Feature {name} differs: {dec_feats[name]} vs {tab_feats[name]}"
            )


# -----------------------------------------------------------------------
# Table.group_by (re-aggregation)
# -----------------------------------------------------------------------


class TestTableReAggregation:
    def test_table_group_by_returns_groupby(self) -> None:
        t = Table("T", key="uid")
        t["c"] = Count(window="1h")
        gb = t.group_by("merchant_id")
        assert isinstance(gb, GroupBy)
        assert gb._key == "merchant_id"

    def test_table_group_by_agg(self) -> None:
        t = Table("T", key="uid")
        t["c"] = Count(window="1h")
        new_t = t.group_by("merchant_id").agg(
            merchant_count=Count(window="24h"),
        )
        assert isinstance(new_t, Table)
        assert new_t._key == "merchant_id"
        assert "merchant_count" in new_t._features


# -----------------------------------------------------------------------
# Deduplication in collect_registrations
# -----------------------------------------------------------------------


class TestDeduplication:
    def test_shared_source_deduplicates(self) -> None:
        """Two tables from same source should not duplicate the source."""
        raw = Stream("raw")
        t1 = Table("T1", key="uid", source=raw)
        t1["c"] = Count(window="1h")
        t2 = Table("T2", key="mid", source=raw)
        t2["c"] = Count(window="1h")

        # Simulate what register_all does
        seen: set[str] = set()
        ordered: list[dict] = []
        for dataset in [t1, t2]:
            for reg in dataset._collect_registrations():
                name = reg["name"]
                if name not in seen:
                    seen.add(name)
                    ordered.append(reg)

        names = [r["name"] for r in ordered]
        assert names.count("raw") == 1  # deduplicated
        assert "T1" in names
        assert "T2" in names
