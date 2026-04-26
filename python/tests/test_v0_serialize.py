"""Plan 21-03 / Task 3: REGISTER JSON serializer + topological collection."""

from __future__ import annotations

import pytest

import tally as tl
from tally._serialize import compile_to_register_json, collect_registrations


# ---------------------------------------------------------------------------
# Source payloads
# ---------------------------------------------------------------------------


class TestSourcePayloads:
    def test_stream_source(self):
        @tl.stream
        class Clicks:
            user_id: str
            url: str

        j = compile_to_register_json(Clicks)
        assert j == {
            "name": "Clicks",
            "kind": "stream",
            "key_field": None,
            "fields": {
                "user_id": {"type": "str", "optional": False},
                "url": {"type": "str", "optional": False},
            },
        }

    def test_stream_source_with_history_ttl(self):
        @tl.stream(history_ttl="30d")
        class Logins:
            user_id: str

        j = compile_to_register_json(Logins)
        assert j["history_ttl"] == "30d"

    def test_table_source_single_key(self):
        @tl.table(key="user_id", ttl="7d")
        class Users:
            user_id: str
            name: str

        j = compile_to_register_json(Users)
        assert j["kind"] == "table"
        assert j["key_field"] == "user_id"
        assert j["mode"] == "append"
        assert j["entity_ttl"] == "7d"
        assert "key_fields" not in j

    def test_table_source_composite_key(self):
        @tl.table(key=["account_id", "region"])
        class Accounts:
            account_id: str
            region: str
            balance: float

        j = compile_to_register_json(Accounts)
        assert j["key_field"] is None
        assert j["key_fields"] == ["account_id", "region"]


# ---------------------------------------------------------------------------
# Op-chain derivations
# ---------------------------------------------------------------------------


class TestOpChainDerivations:
    def test_stream_filter_select(self):
        @tl.stream
        class Clicks:
            user_id: str
            page: str
            amount: float

        @tl.stream
        def Checkouts(clicks: Clicks) -> tl.Stream:
            return clicks.filter(tl.col("page") == "/checkout").select(
                "user_id", "amount"
            )

        j = compile_to_register_json(Checkouts)
        assert j["name"] == "Checkouts"
        assert j["kind"] == "stream"
        assert j["depends_on"] == ["Clicks"]
        ops = j["ops"]
        assert len(ops) == 2
        assert ops[0]["op"] == "filter"
        assert ops[1]["op"] == "select"
        assert ops[1]["fields"] == ["user_id", "amount"]


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestAggregationPayload:
    def test_groupby_sum_count(self):
        @tl.stream
        class Clicks:
            user_id: str
            amount: float

        @tl.table(key="user_id")
        def UserSpend(clicks: Clicks) -> tl.Table:
            return clicks.group_by("user_id").agg(
                n=tl.count(window="1h"),
                total=tl.sum("amount", window="1h"),
            )

        j = compile_to_register_json(UserSpend)
        assert j["kind"] == "table"
        assert j["key_field"] == "user_id"
        assert j["depends_on"] == ["Clicks"]
        agg = j["aggregation"]
        assert agg["source"] == "Clicks"
        assert agg["keys"] == ["user_id"]
        feats = agg["features"]
        assert len(feats) == 2
        assert feats[0]["name"] == "n"
        assert feats[0]["type"] == "count"
        assert feats[0]["window"] == "1h"
        assert feats[1]["name"] == "total"
        assert feats[1]["type"] == "sum"
        assert feats[1]["field"] == "amount"
        assert feats[1]["supports_retraction"] is True

    def test_percentile_feature_emits_hybrid_params(self):
        @tl.stream
        class Req:
            endpoint: str
            latency_ms: float

        @tl.table(key="endpoint")
        def EndpointP95(req: Req) -> tl.Table:
            return req.group_by("endpoint").agg(
                p95=tl.percentile("latency_ms", 0.95, window="5m"),
            )

        j = compile_to_register_json(EndpointP95)
        f = j["aggregation"]["features"][0]
        assert f["type"] == "percentile"
        assert f["quantile"] == 0.95
        assert f["exact_threshold"] == 256
        assert f["hybrid_alpha"] == 0.01


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


class TestJoinPayload:
    def test_stream_stream_join(self):
        @tl.stream
        class Orders:
            order_id: str
            amount: float

        @tl.stream
        class Payments:
            order_id: str
            method: str

        @tl.stream
        def OP(orders: Orders, payments: Payments) -> tl.Stream:
            return orders.join(payments, on="order_id", within="30m")

        j = compile_to_register_json(OP)
        assert j["kind"] == "stream"
        assert j["join"]["on"] == ["order_id"]
        assert j["join"]["within"] == "30m"
        assert j["join"]["type"] == "inner"
        assert j["join"]["shape"] == "stream_stream"
        assert "Orders" in j["depends_on"]
        assert "Payments" in j["depends_on"]

    def test_table_table_join(self):
        @tl.table(key="user_id")
        class UserA:
            user_id: str
            name: str

        @tl.table(key="user_id")
        class UserB:
            user_id: str
            email: str

        @tl.table(key="user_id")
        def UserJoined(a: UserA, b: UserB) -> tl.Table:
            return a.join(b, on="user_id")

        j = compile_to_register_json(UserJoined)
        assert j["kind"] == "table"
        assert j["key_field"] == "user_id"
        assert "within" not in j["join"]
        assert j["join"]["shape"] == "table_table"


# ---------------------------------------------------------------------------
# Union
# ---------------------------------------------------------------------------


class TestUnionPayload:
    def test_union_two_streams(self):
        @tl.stream
        class A:
            k: str
            v: int

        @tl.stream
        class B:
            k: str
            v: int

        @tl.stream
        def AB(a: A, b: B) -> tl.Stream:
            return tl.union(a, b)

        j = compile_to_register_json(AB)
        assert j["kind"] == "stream"
        assert j["union"]["sources"] == ["A", "B"]
        assert set(j["depends_on"]) == {"A", "B"}


# ---------------------------------------------------------------------------
# collect_registrations: topological order + dedupe
# ---------------------------------------------------------------------------


class TestCollectRegistrations:
    def test_end_to_end_pipeline_topological_order(self):
        @tl.stream
        class Clicks:
            user_id: str
            page: str
            amount: float

        @tl.stream
        def Checkouts(clicks: Clicks) -> tl.Stream:
            return clicks.filter(tl.col("page") == "/checkout")

        @tl.table(key="user_id")
        def UserSpend(co: Checkouts) -> tl.Table:
            return co.group_by("user_id").agg(
                n=tl.count(window="1h"),
                total=tl.sum("amount", window="1h"),
            )

        regs = collect_registrations(UserSpend)
        names = [r["name"] for r in regs]
        assert names == ["Clicks", "Checkouts", "UserSpend"]
        # Last frame is the aggregation payload
        assert regs[-1]["aggregation"]["features"][0]["type"] == "count"

    def test_dedupe_shared_upstream(self):
        @tl.stream
        class S:
            user_id: str
            amount: float

        @tl.stream
        def A(s: S) -> tl.Stream:
            return s.filter(tl.col("amount") > 0)

        @tl.stream
        def B(s: S) -> tl.Stream:
            return s.filter(tl.col("amount") < 100)

        @tl.stream
        def Combined(a: A, b: B) -> tl.Stream:
            return tl.union(a, b)

        regs = collect_registrations(Combined)
        names = [r["name"] for r in regs]
        # S appears once despite being depended on twice.
        assert names.count("S") == 1
        # Topologically S precedes A and B, which both precede Combined.
        assert names.index("S") < names.index("A")
        assert names.index("S") < names.index("B")
        assert names.index("A") < names.index("Combined")
        assert names.index("B") < names.index("Combined")


# ---------------------------------------------------------------------------
# Validate() covers aggregation + join pipelines cleanly
# ---------------------------------------------------------------------------


def test_validate_empty_for_canonical_pipeline():
    @tl.stream
    class Clicks:
        user_id: str
        page: str
        amount: float

    @tl.stream
    def Checkouts(clicks: Clicks) -> tl.Stream:
        return clicks.filter(tl.col("page") == "/checkout")

    @tl.table(key="user_id")
    def UserSpend(co: Checkouts) -> tl.Table:
        return co.group_by("user_id").agg(
            n=tl.count(window="1h"),
            total=tl.sum("amount", window="1h"),
        )

    errors = tl.validate(Clicks, Checkouts, UserSpend)
    assert errors == []
