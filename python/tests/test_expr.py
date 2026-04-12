"""Tests for the expression tree (Expr, Column, BinOp, Ref, Literal).

Verifies:
- Operator overloading builds correct expression trees
- Expression serialization to string format matches server expectations
- Column aggregation methods return correct OperatorBase types
- EventProxy/EventColumn produce _event.field references
"""

from __future__ import annotations

import pytest

from tally._expr import (
    BinOp,
    Column,
    EventColumn,
    EventProxy,
    Expr,
    Literal,
    Ref,
    UnaryOp,
    _wrap,
)
from tally._operators import Avg, Count, DistinctCount, Last, Max, Min, Sum


# We need a minimal table-like object for Column
class _FakeTable:
    _name = "FakeTable"


def _col(name: str) -> Column:
    return Column(_FakeTable(), name)


# -----------------------------------------------------------------------
# Literal serialization
# -----------------------------------------------------------------------


class TestLiteral:
    def test_int(self) -> None:
        assert Literal(42).to_expr_string() == "42"

    def test_float(self) -> None:
        assert Literal(3.14).to_expr_string() == "3.14"

    def test_string(self) -> None:
        assert Literal("hello").to_expr_string() == "'hello'"

    def test_bool_true(self) -> None:
        assert Literal(True).to_expr_string() == "true"

    def test_bool_false(self) -> None:
        assert Literal(False).to_expr_string() == "false"


# -----------------------------------------------------------------------
# Ref serialization
# -----------------------------------------------------------------------


class TestRef:
    def test_simple_name(self) -> None:
        assert Ref("tx_count_1h").to_expr_string() == "tx_count_1h"

    def test_dotted_name(self) -> None:
        assert Ref("_event.amount").to_expr_string() == "_event.amount"


# -----------------------------------------------------------------------
# BinOp serialization
# -----------------------------------------------------------------------


class TestBinOp:
    def test_add(self) -> None:
        expr = BinOp("+", Ref("a"), Ref("b"))
        assert expr.to_expr_string() == "(a + b)"

    def test_nested(self) -> None:
        inner = BinOp("+", Ref("a"), Literal(1))
        outer = BinOp("/", inner, Ref("b"))
        assert outer.to_expr_string() == "((a + 1) / b)"


# -----------------------------------------------------------------------
# UnaryOp serialization
# -----------------------------------------------------------------------


class TestUnaryOp:
    def test_not(self) -> None:
        expr = UnaryOp("not", Ref("x"))
        assert expr.to_expr_string() == "(not x)"

    def test_neg(self) -> None:
        expr = UnaryOp("-", Ref("x"))
        assert expr.to_expr_string() == "(- x)"


# -----------------------------------------------------------------------
# _wrap helper
# -----------------------------------------------------------------------


class TestWrap:
    def test_wrap_expr_passthrough(self) -> None:
        r = Ref("x")
        assert _wrap(r) is r

    def test_wrap_column(self) -> None:
        col = _col("amount")
        result = _wrap(col)
        assert isinstance(result, Ref)
        assert result.to_expr_string() == "amount"

    def test_wrap_int(self) -> None:
        result = _wrap(42)
        assert isinstance(result, Literal)
        assert result.to_expr_string() == "42"

    def test_wrap_float(self) -> None:
        result = _wrap(3.14)
        assert isinstance(result, Literal)

    def test_wrap_string(self) -> None:
        result = _wrap("hello")
        assert isinstance(result, Literal)
        assert result.to_expr_string() == "'hello'"


# -----------------------------------------------------------------------
# Column operator overloading
# -----------------------------------------------------------------------


class TestColumnOperators:
    def test_add_literal(self) -> None:
        expr = _col("a") + 5
        assert expr.to_expr_string() == "(a + 5)"

    def test_radd_literal(self) -> None:
        expr = 5 + _col("a")
        assert expr.to_expr_string() == "(5 + a)"

    def test_sub(self) -> None:
        expr = _col("a") - _col("b")
        assert expr.to_expr_string() == "(a - b)"

    def test_rsub(self) -> None:
        expr = 10 - _col("a")
        assert expr.to_expr_string() == "(10 - a)"

    def test_mul(self) -> None:
        expr = _col("amount") * _col("fx_rate")
        assert expr.to_expr_string() == "(amount * fx_rate)"

    def test_rmul(self) -> None:
        expr = 2 * _col("amount")
        assert expr.to_expr_string() == "(2 * amount)"

    def test_truediv(self) -> None:
        expr = _col("failed") / _col("total")
        assert expr.to_expr_string() == "(failed / total)"

    def test_rtruediv(self) -> None:
        expr = 100 / _col("count")
        assert expr.to_expr_string() == "(100 / count)"

    def test_gt(self) -> None:
        expr = _col("amount") > 1000
        assert expr.to_expr_string() == "(amount > 1000)"

    def test_lt(self) -> None:
        expr = _col("amount") < 10
        assert expr.to_expr_string() == "(amount < 10)"

    def test_ge(self) -> None:
        expr = _col("count") >= 5
        assert expr.to_expr_string() == "(count >= 5)"

    def test_le(self) -> None:
        expr = _col("count") <= 100
        assert expr.to_expr_string() == "(count <= 100)"

    def test_eq(self) -> None:
        expr = _col("status") == "failed"
        assert expr.to_expr_string() == "(status == 'failed')"

    def test_ne(self) -> None:
        expr = _col("status") != "success"
        assert expr.to_expr_string() == "(status != 'success')"

    def test_and(self) -> None:
        expr = (_col("a") > 5) & (_col("b") < 10)
        assert expr.to_expr_string() == "((a > 5) and (b < 10))"

    def test_or(self) -> None:
        expr = (_col("a") > 5) | (_col("b") < 10)
        assert expr.to_expr_string() == "((a > 5) or (b < 10))"

    def test_invert(self) -> None:
        expr = ~(_col("active"))
        assert expr.to_expr_string() == "(not active)"

    def test_neg(self) -> None:
        expr = -_col("amount")
        assert expr.to_expr_string() == "(- amount)"

    def test_complex_expression(self) -> None:
        """Compound: (tx_count_1h > 10) and (chargeback_count > 5)"""
        expr = (_col("tx_count_1h") > 10) & (_col("chargeback_count") > 5)
        assert expr.to_expr_string() == "((tx_count_1h > 10) and (chargeback_count > 5))"

    def test_arithmetic_chain(self) -> None:
        """(count_1h / 1) / (count_24h / 24) — velocity spike"""
        expr = (_col("count_1h") / 1) / (_col("count_24h") / 24)
        assert expr.to_expr_string() == "((count_1h / 1) / (count_24h / 24))"


# -----------------------------------------------------------------------
# Expr chaining (expr op expr)
# -----------------------------------------------------------------------


class TestExprChaining:
    def test_expr_add_expr(self) -> None:
        e1 = _col("a") + 1
        e2 = e1 + _col("b")
        assert "(a + 1)" in e2.to_expr_string()

    def test_expr_gt_literal(self) -> None:
        e = (_col("a") + _col("b")) > 100
        assert e.to_expr_string() == "((a + b) > 100)"

    def test_expr_and_expr(self) -> None:
        left = _col("a") > 10
        right = _col("b") < 5
        combined = left & right
        assert combined.to_expr_string() == "((a > 10) and (b < 5))"


# -----------------------------------------------------------------------
# Column aggregation methods
# -----------------------------------------------------------------------


class TestColumnAggregation:
    def test_sum(self) -> None:
        op = _col("amount").sum(window="1h")
        assert isinstance(op, Sum)
        assert op.field == "amount"
        assert op.window == "1h"

    def test_avg(self) -> None:
        op = _col("amount").avg(window="1h")
        assert isinstance(op, Avg)
        assert op.field == "amount"

    def test_mean_alias(self) -> None:
        op = _col("amount").mean(window="1h")
        assert isinstance(op, Avg)

    def test_min(self) -> None:
        op = _col("amount").min(window="1h")
        assert isinstance(op, Min)
        assert op.field == "amount"

    def test_max(self) -> None:
        op = _col("amount").max(window="24h")
        assert isinstance(op, Max)
        assert op.field == "amount"

    def test_nunique(self) -> None:
        op = _col("merchant_id").nunique(window="24h")
        assert isinstance(op, DistinctCount)
        assert op.field == "merchant_id"

    def test_distinct_count_alias(self) -> None:
        op = _col("merchant_id").distinct_count(window="24h")
        assert isinstance(op, DistinctCount)

    def test_last(self) -> None:
        op = _col("country").last()
        assert isinstance(op, Last)
        assert op.field == "country"

    def test_count(self) -> None:
        op = _col("amount").count(window="1h")
        assert isinstance(op, Count)
        assert op.window == "1h"


# -----------------------------------------------------------------------
# EventProxy and EventColumn
# -----------------------------------------------------------------------


class TestEventProxy:
    def test_event_column_name(self) -> None:
        table = _FakeTable()
        proxy = EventProxy(table)
        col = proxy["amount"]
        assert isinstance(col, EventColumn)
        assert col.name == "_event.amount"

    def test_event_column_in_expr(self) -> None:
        table = _FakeTable()
        proxy = EventProxy(table)
        expr = proxy["amount"] / _col("avg_amount")
        assert expr.to_expr_string() == "(_event.amount / avg_amount)"

    def test_event_column_repr(self) -> None:
        table = _FakeTable()
        col = EventColumn(table, "amount")
        assert "amount" in repr(col)
