"""Plan 21-03 / Task 2: join + union stubs."""

from __future__ import annotations

import pytest

import tally as tl
from tally._join import JoinSpec
from tally._union import UnionSpec


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _build_two_streams():
    @tl.stream
    class Orders:
        order_id: str
        user_id: str
        amount: float

    @tl.stream
    class Payments:
        order_id: str
        method: str
        amount: float  # collides with Orders.amount

    return Orders, Payments


def _build_stream_and_table():
    @tl.stream
    class Clicks:
        user_id: str
        url: str

    @tl.table(key="user_id")
    class Users:
        user_id: str
        name: str

    return Clicks, Users


def _build_two_tables():
    @tl.table(key="user_id")
    class UserA:
        user_id: str
        name: str

    @tl.table(key="user_id")
    class UserB:
        user_id: str
        email: str
        name: str  # collides

    return UserA, UserB


# ---------------------------------------------------------------------------
# Stream ↔ Stream
# ---------------------------------------------------------------------------


class TestStreamStreamJoin:
    def test_happy_path_returns_stream(self):
        Orders, Payments = _build_two_streams()
        joined = Orders.join(Payments, on="order_id", within="30m")
        assert isinstance(joined, tl.Stream)
        d = joined.describe()
        assert d["kind"] == "stream"
        names = list(d["fields"].keys())
        assert names == ["order_id", "user_id", "amount", "method", "amount_right"]

    def test_within_required(self):
        Orders, Payments = _build_two_streams()
        with pytest.raises(TypeError, match=r"Stream↔Stream join requires within"):
            Orders.join(Payments, on="order_id")

    def test_collision_suffix(self):
        Orders, Payments = _build_two_streams()
        joined = Orders.join(Payments, on="order_id", within="30m")
        # 'amount' collides → left keeps, right becomes 'amount_right'.
        assert "amount" in joined._schema
        assert "amount_right" in joined._schema

    def test_outer_join_deferred(self):
        Orders, Payments = _build_two_streams()
        with pytest.raises(RuntimeError, match=r"outer joins deferred to v0.1"):
            Orders.join(Payments, on="order_id", within="30m", type="outer")

    def test_right_join_rejected(self):
        Orders, Payments = _build_two_streams()
        with pytest.raises(TypeError, match=r"must be 'inner' or 'left'"):
            Orders.join(Payments, on="order_id", within="30m", type="right")

    def test_left_join_allowed(self):
        Orders, Payments = _build_two_streams()
        joined = Orders.join(Payments, on="order_id", within="30m", type="left")
        assert joined._join_spec.type_ == "left"

    def test_unknown_join_key_raises(self):
        Orders, Payments = _build_two_streams()
        with pytest.raises(TypeError, match=r"not in Orders"):
            Orders.join(Payments, on="no_such_field", within="30m")

    def test_compile_raises_phase_23(self):
        Orders, Payments = _build_two_streams()
        joined = Orders.join(Payments, on="order_id", within="30m")
        with pytest.raises(NotImplementedError, match=r"ships in Phase 23"):
            joined._join_spec._compile_for_server()


# ---------------------------------------------------------------------------
# Stream ↔ Table (enrichment)
# ---------------------------------------------------------------------------


class TestStreamTableJoin:
    def test_happy_path_returns_stream(self):
        Clicks, Users = _build_stream_and_table()
        enriched = Clicks.join(Users, on="user_id")
        assert isinstance(enriched, tl.Stream)
        assert "user_id" in enriched._schema
        assert "url" in enriched._schema
        assert "name" in enriched._schema

    def test_within_forbidden(self):
        Clicks, Users = _build_stream_and_table()
        with pytest.raises(TypeError, match=r"does not accept within"):
            Clicks.join(Users, on="user_id", within="30m")

    def test_outer_deferred(self):
        Clicks, Users = _build_stream_and_table()
        with pytest.raises(RuntimeError, match=r"outer joins deferred"):
            Clicks.join(Users, on="user_id", type="outer")

    def test_join_spec_shape_label(self):
        Clicks, Users = _build_stream_and_table()
        enriched = Clicks.join(Users, on="user_id")
        assert enriched._join_spec.shape == "stream_table"


# ---------------------------------------------------------------------------
# Table ↔ Table
# ---------------------------------------------------------------------------


class TestTableTableJoin:
    def test_happy_path_returns_table(self):
        UserA, UserB = _build_two_tables()
        joined = UserA.join(UserB, on="user_id")
        assert isinstance(joined, tl.Table)
        # Full-key match; output key preserved.
        assert joined._key == ["user_id"]
        # Collision suffix on 'name'.
        assert "name" in joined._schema
        assert "name_right" in joined._schema
        assert "email" in joined._schema

    def test_partial_key_rejected(self):
        @tl.table(key=["user_id", "region"])
        class A:
            user_id: str
            region: str
            name: str

        @tl.table(key=["user_id", "region"])
        class B:
            user_id: str
            region: str
            email: str

        with pytest.raises(RuntimeError, match=r"full-key match"):
            A.join(B, on="user_id")

    def test_within_forbidden(self):
        UserA, UserB = _build_two_tables()
        with pytest.raises(TypeError, match=r"does not accept within"):
            UserA.join(UserB, on="user_id", within="30m")

    def test_non_table_right_side_rejected(self):
        UserA, _ = _build_two_tables()

        @tl.stream
        class S:
            user_id: str
            url: str

        with pytest.raises(TypeError, match=r"can only join another Table"):
            UserA.join(S, on="user_id")

    def test_compile_raises_phase_23(self):
        UserA, UserB = _build_two_tables()
        joined = UserA.join(UserB, on="user_id")
        with pytest.raises(NotImplementedError, match=r"ships in Phase 23"):
            joined._join_spec._compile_for_server()


# ---------------------------------------------------------------------------
# tl.union
# ---------------------------------------------------------------------------


class TestUnion:
    def test_happy_path_two_streams(self):
        @tl.stream
        class A:
            user_id: str
            amount: float

        @tl.stream
        class B:
            user_id: str
            amount: float

        u = tl.union(A, B)
        assert isinstance(u, tl.Stream)
        d = u.describe()
        assert list(d["fields"].keys()) == ["user_id", "amount"]

    def test_happy_path_three_streams(self):
        @tl.stream
        class A:
            k: str
            v: int

        @tl.stream
        class B:
            k: str
            v: int

        @tl.stream
        class C:
            k: str
            v: int

        u = tl.union(A, B, C)
        assert u._union_spec.sources == [A, B, C]

    def test_schema_mismatch_on_field_name(self):
        @tl.stream
        class A:
            user_id: str
            amount: float

        @tl.stream
        class B:
            user_id: str
            value: float

        with pytest.raises(TypeError, match=r"schemas differ"):
            tl.union(A, B)

    def test_schema_mismatch_on_type(self):
        @tl.stream
        class A:
            k: str
            v: int

        @tl.stream
        class B:
            k: str
            v: float

        with pytest.raises(TypeError, match=r"type mismatch"):
            tl.union(A, B)

    def test_requires_at_least_two(self):
        @tl.stream
        class A:
            k: str

        with pytest.raises(TypeError, match=r"requires 2 or more"):
            tl.union(A)

    def test_non_stream_arg_rejected(self):
        @tl.stream
        class A:
            k: str

        @tl.table(key="k")
        class T:
            k: str

        with pytest.raises(TypeError, match=r"arguments must be Streams"):
            tl.union(A, T)

    def test_compile_raises_phase_22(self):
        @tl.stream
        class A:
            k: str
            v: int

        @tl.stream
        class B:
            k: str
            v: int

        u = tl.union(A, B)
        with pytest.raises(NotImplementedError, match=r"ships in Phase 22"):
            u._union_spec._compile_for_server()


# ---------------------------------------------------------------------------
# Public export sanity
# ---------------------------------------------------------------------------


def test_public_exports():
    assert callable(tl.union)
    assert callable(tl.count)
    assert callable(tl.sum)
    # Instantiating a couple of them:
    assert isinstance(tl.count(window="1h"), object)
    assert isinstance(tl.first("x"), object)
