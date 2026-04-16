"""Unit tests for the ``bv.col`` expression DSL."""

from __future__ import annotations

import pytest

from beava._col import Col, col


class TestArithmetic:
    def test_add(self):
        assert (col("x") + col("y")).to_expr_string() == "(x + y)"

    def test_sub(self):
        assert (col("x") - col("y")).to_expr_string() == "(x - y)"

    def test_mul(self):
        assert (col("x") * 2).to_expr_string() == "(x * 2)"

    def test_div(self):
        assert (col("x") / col("y")).to_expr_string() == "(x / y)"

    def test_reverse_add(self):
        assert (1 + col("x")).to_expr_string() == "(1 + x)"

    def test_reverse_sub(self):
        assert (10 - col("x")).to_expr_string() == "(10 - x)"


class TestComparison:
    def test_gt_with_int_literal(self):
        assert (col("a") > 100).to_expr_string() == "(a > 100)"

    def test_ge(self):
        assert (col("a") >= 5).to_expr_string() == "(a >= 5)"

    def test_lt(self):
        assert (col("a") < 5).to_expr_string() == "(a < 5)"

    def test_le(self):
        assert (col("a") <= 5).to_expr_string() == "(a <= 5)"

    def test_eq_with_string_literal(self):
        assert (col("status") == "failed").to_expr_string() == "(status == 'failed')"

    def test_ne(self):
        assert (col("status") != "ok").to_expr_string() == "(status != 'ok')"

    def test_string_with_single_quote_escaped(self):
        assert (col("s") == "it's").to_expr_string() == "(s == 'it\\'s')"


class TestBoolean:
    def test_and(self):
        expr = (col("a") > 1) & (col("b") < 2)
        assert expr.to_expr_string() == "((a > 1) and (b < 2))"

    def test_or(self):
        expr = (col("a") > 1) | (col("b") < 2)
        assert expr.to_expr_string() == "((a > 1) or (b < 2))"

    def test_not(self):
        assert (~col("x")).to_expr_string() == "(not x)"


class TestLiterals:
    def test_boolean_literal(self):
        assert (col("flag") == True).to_expr_string() == "(flag == true)"  # noqa: E712

    def test_boolean_false(self):
        assert (col("flag") == False).to_expr_string() == "(flag == false)"  # noqa: E712

    def test_null_literal_via_isnull(self):
        assert col("x").isnull().to_expr_string() == "(x == null)"

    def test_float_literal(self):
        assert (col("x") > 3.5).to_expr_string() == "(x > 3.5)"


class TestCast:
    def test_cast_to_float(self):
        assert col("x").cast("float").to_expr_string() == "cast(x, float)"

    def test_cast_non_string_raises(self):
        with pytest.raises(TypeError):
            col("x").cast(int)


class TestReferencedFields:
    def test_simple(self):
        expr = col("a") + col("b")
        assert expr.referenced_fields() == {"a", "b"}

    def test_nested(self):
        expr = (col("a") + col("b")) * (col("c") - col("a"))
        assert expr.referenced_fields() == {"a", "b", "c"}

    def test_with_literal(self):
        expr = col("x") > 100
        assert expr.referenced_fields() == {"x"}

    def test_cast_included(self):
        expr = col("x").cast("float") + col("y")
        assert expr.referenced_fields() == {"x", "y"}

    def test_isnull(self):
        assert col("x").isnull().referenced_fields() == {"x"}


class TestCol:
    def test_col_returns_expr_ast(self):
        assert isinstance(col("x"), Col)

    def test_empty_name_raises(self):
        with pytest.raises(TypeError):
            col("")

    def test_non_string_raises(self):
        with pytest.raises(TypeError):
            col(123)  # type: ignore[arg-type]

    def test_qualified_field_access(self):
        # Phase 22 needs Stream.field references — just confirm they pass through.
        assert col("Transactions.amount").to_expr_string() == "Transactions.amount"
