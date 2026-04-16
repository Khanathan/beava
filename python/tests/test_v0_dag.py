"""Tests for function-form decorators and DAG discovery.

Covers:
  * ``@bv.stream def X(a: A, b: B) -> Stream`` and ``@bv.table def ...``
  * Upstream discovery from parameter type hints (no ``depends_on=`` kwarg)
  * Return-type annotation enforcement
  * ``build_dag`` adjacency + topological order
  * ``CycleError`` with named ``A → B → C → A`` path
  * ``MissingDependency`` when upstream class isn't in the descriptor set
"""

from __future__ import annotations

import pytest

from beava._col import col
from beava._dag import CycleError, MissingDependency, build_dag
from beava._stream import Stream, StreamDerivation, StreamSource, stream
from beava._table import Table, TableDerivation, TableSource, table


# ---------------------------------------------------------------------------
# Function-form decorators
# ---------------------------------------------------------------------------


class TestStreamFunctionForm:
    def test_basic_function_derivation(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        assert isinstance(Checkouts, StreamDerivation)
        assert Checkouts._name == "Checkouts"
        assert Checkouts._upstreams == [Clicks]
        assert len(Checkouts._ops) == 1
        assert Checkouts._ops[0]["op"] == "filter"

    def test_function_missing_return_annotation_raises(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        with pytest.raises(TypeError) as ei:
            @stream
            def Bad(clicks: Clicks):
                return clicks
        assert "return type" in str(ei.value)

    def test_function_wrong_return_type_raises(self):
        @stream
        class Clicks:
            user_id: str

        @table(key="user_id")
        class Users:
            user_id: str

        # Function annotated -> Stream but returns a Table
        with pytest.raises(TypeError) as ei:
            @stream
            def Bad(u: Users) -> Stream:
                return u  # type: ignore[return-value]
        msg = str(ei.value)
        assert "Stream" in msg or "Table" in msg

    def test_function_no_parameters_raises(self):
        with pytest.raises(TypeError) as ei:
            @stream
            def NoUpstream() -> Stream:  # type: ignore[empty-body]
                return None  # type: ignore[return-value]
        assert "no upstreams" in str(ei.value)

    def test_fan_in_multi_upstream(self):
        @stream
        class A:
            x: int

        @stream
        class B:
            x: int

        @stream
        def C(a: A, b: B) -> Stream:
            return a.select("x")

        assert len(C._upstreams) == 2
        assert C._upstreams == [A, B]


class TestTableFunctionForm:
    def test_basic_table_derivation(self):
        @table(key="user_id")
        class Users:
            user_id: str
            name: str

        @table(key="user_id")
        def ActiveUsers(users: Users) -> Table:
            return users.filter(col("name") != "")

        assert isinstance(ActiveUsers, TableDerivation)
        assert ActiveUsers._name == "ActiveUsers"
        assert ActiveUsers._upstreams == [Users]

    def test_table_derivation_wrong_return_type_raises(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        with pytest.raises(TypeError):
            @table(key="user_id")
            def Bad(c: Clicks) -> Table:
                return c.filter(col("page") == "/checkout")  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# build_dag / topological_order
# ---------------------------------------------------------------------------


class TestBuildDag:
    def test_single_source_single_derivation(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        dag = build_dag([Clicks, Checkouts])
        assert "Clicks" in dag.nodes
        assert "Checkouts" in dag.nodes
        assert dag.edges["Checkouts"] == ["Clicks"]
        assert dag.edges["Clicks"] == []

    def test_topological_order_source_first(self):
        @stream
        class Clicks:
            user_id: str
            page: str

        @stream
        def Checkouts(clicks: Clicks) -> Stream:
            return clicks.filter(col("page") == "/checkout")

        dag = build_dag([Checkouts, Clicks])  # reversed input order
        order = dag.topological_order()
        assert order.index("Clicks") < order.index("Checkouts")

    def test_fan_in_recorded_in_order(self):
        @stream
        class A:
            x: int

        @stream
        class B:
            x: int

        @stream
        def C(a: A, b: B) -> Stream:
            return a

        dag = build_dag([A, B, C])
        assert dag.edges["C"] == ["A", "B"]

    def test_missing_upstream_raises_missing_dependency(self):
        @stream
        class A:
            x: int

        @stream
        def B(a: A) -> Stream:
            return a

        with pytest.raises(MissingDependency) as ei:
            # Only B passed; A is referenced but not in descriptor set
            build_dag([B])
        msg = str(ei.value)
        assert "A" in msg


class TestCycleDetection:
    def test_self_cycle_detected(self):
        # Construct by hand: a derivation that lists itself as upstream.
        @stream
        class A:
            x: int

        @stream
        def B(a: A) -> Stream:
            return a

        # Force a self-cycle at the DAG layer
        B._upstreams = [B]

        dag = build_dag([B])
        with pytest.raises(CycleError) as ei:
            dag.topological_order()
        assert "B" in str(ei.value)
        assert "→" in str(ei.value)

    def test_mutual_cycle_named_path(self):
        @stream
        class Seed:
            x: int

        @stream
        def A(s: Seed) -> Stream:
            return s

        @stream
        def B(s: Seed) -> Stream:
            return s

        # Rewrite upstreams: A ← B, B ← A (cycle).
        A._upstreams = [B]
        B._upstreams = [A]

        dag = build_dag([A, B])
        with pytest.raises(CycleError) as ei:
            dag.topological_order()
        msg = str(ei.value)
        assert "→" in msg
        assert "A" in msg and "B" in msg

    def test_cycle_error_has_cycle_path_attribute(self):
        @stream
        class Seed:
            x: int

        @stream
        def A(s: Seed) -> Stream:
            return s

        A._upstreams = [A]
        dag = build_dag([A])
        try:
            dag.topological_order()
        except CycleError as e:
            assert isinstance(e.cycle_path, list)
            assert e.cycle_path[0] == e.cycle_path[-1]
