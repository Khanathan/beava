"""Tests for bv.col expression DSL — grammar, serialization, and type inference.

All tests import via ``import beava as bv`` and ``from beava._col import infer_output_type``.
These tests are written RED-first (Plan 03-02 Task 1.a); they will fail until
``python/beava/_col.py`` is implemented in Task 1.b.
"""

import pytest

import beava as bv
from beava._col import infer_output_type

# ---------------------------------------------------------------------------
# Field rendering
# ---------------------------------------------------------------------------


def test_field_renders_bare() -> None:
    assert bv.col("x").to_expr_string() == "x"
    assert bv.col("Stream.x").to_expr_string() == "Stream.x"


# ---------------------------------------------------------------------------
# Arithmetic — every binary op must be parenthesized
# ---------------------------------------------------------------------------


def test_arithmetic_parenthesized() -> None:
    assert (bv.col("a") + 1).to_expr_string() == "(a + 1)"
    assert (1 + bv.col("a")).to_expr_string() == "(1 + a)"
    assert (bv.col("a") - bv.col("b")).to_expr_string() == "(a - b)"
    assert (bv.col("a") * 2.5).to_expr_string() == "(a * 2.5)"
    assert (bv.col("a") / 2).to_expr_string() == "(a / 2)"


# ---------------------------------------------------------------------------
# Comparison — all six operators
# ---------------------------------------------------------------------------


def test_comparison_parenthesized() -> None:
    assert (bv.col("a") > 100).to_expr_string() == "(a > 100)"
    assert (bv.col("a") >= 0).to_expr_string() == "(a >= 0)"
    assert (bv.col("a") < 5).to_expr_string() == "(a < 5)"
    assert (bv.col("a") <= 5).to_expr_string() == "(a <= 5)"
    assert (bv.col("a") == bv.col("b")).to_expr_string() == "(a == b)"
    assert (bv.col("a") != "foo").to_expr_string() == "(a != 'foo')"


# ---------------------------------------------------------------------------
# Boolean combinators — emit "and" / "or" / "not" keywords
# ---------------------------------------------------------------------------


def test_boolean_combinators_emit_keywords() -> None:
    expr_and = (bv.col("a") > 0) & (bv.col("b") < 5)
    assert expr_and.to_expr_string() == "((a > 0) and (b < 5))"

    expr_or = (bv.col("a") > 0) | (bv.col("b") < 5)
    assert expr_or.to_expr_string() == "((a > 0) or (b < 5))"

    expr_not = ~bv.col("flag")
    assert expr_not.to_expr_string() == "(not flag)"


# ---------------------------------------------------------------------------
# isnull shorthand
# ---------------------------------------------------------------------------


def test_isnull() -> None:
    assert bv.col("x").isnull().to_expr_string() == "(x == null)"


# ---------------------------------------------------------------------------
# cast — type coercion call
# ---------------------------------------------------------------------------


def test_cast() -> None:
    assert bv.col("x").cast("float").to_expr_string() == "cast(x, float)"
    assert bv.col("x").cast("int").to_expr_string() == "cast(x, int)"
    with pytest.raises(TypeError):
        bv.col("x").cast(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# String literal escaping
# ---------------------------------------------------------------------------


def test_string_literal_escaping() -> None:
    # Plain string — no escaping needed
    assert (bv.col("page") == "/checkout").to_expr_string() == "(page == '/checkout')"
    # Apostrophe inside value must be backslash-escaped
    assert (bv.col("s") == "it's").to_expr_string() == "(s == 'it\\'s')"
    # Backslash inside value must be doubled
    assert (bv.col("s") == "back\\slash").to_expr_string() == "(s == 'back\\\\slash')"


# ---------------------------------------------------------------------------
# Bool / null literals
# ---------------------------------------------------------------------------


def test_bool_and_null_literals() -> None:
    assert (bv.col("flag") == True).to_expr_string() == "(flag == true)"  # noqa: E712
    assert (bv.col("flag") == False).to_expr_string() == "(flag == false)"  # noqa: E712
    assert (bv.col("x") == None).to_expr_string() == "(x == null)"  # noqa: E711


# ---------------------------------------------------------------------------
# referenced_fields
# ---------------------------------------------------------------------------


def test_referenced_fields() -> None:
    assert bv.col("x").referenced_fields() == {"x"}

    compound = (bv.col("a") > 0) & (bv.col("b") < bv.col("c"))
    assert compound.referenced_fields() == {"a", "b", "c"}

    # String literal is NOT a field reference
    page_expr = bv.col("page") == "/checkout"
    assert page_expr.referenced_fields() == {"page"}

    # Cast target "float" is NOT a field reference
    cast_expr = bv.col("x").cast("float")
    assert cast_expr.referenced_fields() == {"x"}


# ---------------------------------------------------------------------------
# col() argument validation
# ---------------------------------------------------------------------------


def test_col_requires_nonempty_string() -> None:
    with pytest.raises(TypeError):
        bv.col("")
    with pytest.raises(TypeError):
        bv.col(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# infer_output_type — arithmetic
# ---------------------------------------------------------------------------


def test_infer_output_type_arithmetic() -> None:
    assert infer_output_type("i64", "i64", "+") == "i64"
    assert infer_output_type("i64", "f64", "+") == "f64"
    assert infer_output_type("f64", "i64", "-") == "f64"
    assert infer_output_type("f64", "f64", "*") == "f64"
    # Division always widens to f64
    assert infer_output_type("i64", "i64", "/") == "f64"


# ---------------------------------------------------------------------------
# infer_output_type — comparison ops return bool regardless of operand types
# ---------------------------------------------------------------------------


def test_infer_output_type_comparison_returns_bool() -> None:
    assert infer_output_type("i64", "i64", ">") == "bool"
    assert infer_output_type("str", "str", "==") == "bool"
    assert infer_output_type("f64", "f64", "!=") == "bool"


# ---------------------------------------------------------------------------
# infer_output_type — boolean combinators require bool operands
# ---------------------------------------------------------------------------


def test_infer_output_type_boolean_requires_bool() -> None:
    assert infer_output_type("bool", "bool", "and") == "bool"
    assert infer_output_type("bool", "bool", "or") == "bool"
    with pytest.raises(TypeError):
        infer_output_type("i64", "bool", "and")


# ---------------------------------------------------------------------------
# infer_output_type — non-numeric arithmetic is rejected
# ---------------------------------------------------------------------------


def test_infer_output_type_rejects_non_numeric_arithmetic() -> None:
    with pytest.raises(TypeError):
        infer_output_type("str", "i64", "+")
    with pytest.raises(TypeError):
        infer_output_type("i64", "datetime", "-")
    with pytest.raises(TypeError):
        # bool is NOT treated as numeric
        infer_output_type("bool", "bool", "+")
