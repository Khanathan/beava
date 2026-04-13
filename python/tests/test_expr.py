"""Tests for expression tree node serialization (Literal, Ref, BinOp, UnaryOp).

Verifies:
- Literal serialization to string format matching server expectations
- Ref serialization for field references
- BinOp serialization for arithmetic/comparison/boolean
- UnaryOp serialization for not/neg

Note: Column, EventProxy, EventColumn, and _wrap tests were removed as part of
the DataFrame API cleanup (Plan 19-03). Those classes are DataFrame-specific
constructs that will be deleted with _expr.py in Plan 04.
"""

from __future__ import annotations

from tally._expr import (
    BinOp,
    Literal,
    Ref,
    UnaryOp,
)


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
