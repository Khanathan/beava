"""Unit tests for the Python reference evaluator (_eval_reference.py).

These tests verify that the reference evaluator faithfully mirrors Rust's
eval.rs semantics — covering:
  - Three-valued null logic (§D-04 truth tables)
  - Cast rules per target type (§D-05)
  - isnull always returns bool, never None
  - The (x == null) → isnull(x) rewrite is applied before evaluation
  - Arithmetic type promotion (int+int, float+float, mixed)
  - NaN comparisons return False (IEEE-754)
  - Cross-type comparisons return None
  - i64 overflow saturates at I64_MAX / I64_MIN

All tests are GREEN in the Task 1.b commit (reference evaluator exists).
"""

from __future__ import annotations

import math

import pytest

from beava._col import _BinOp, _Field, _Literal
from beava._eval_reference import I64_MAX, I64_MIN, evaluate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def field(name: str) -> _Field:
    return _Field(name)  # type: ignore[call-arg]


def lit(v: object) -> _Literal:
    return _Literal(v)


def binop(op: str, left: object, right: object) -> _BinOp:
    from beava._col import _ExprAST

    lv = left if isinstance(left, _ExprAST) else lit(left)
    rv = right if isinstance(right, _ExprAST) else lit(right)
    return _BinOp(op, lv, rv)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test 1: Three-valued null logic — AND truth table (§D-04)
# ---------------------------------------------------------------------------


def test_null_logic_and_truth_table() -> None:
    empty: dict[str, object] = {}
    # true AND true → True
    assert evaluate(binop("and", lit(True), lit(True)), empty) is True
    # true AND false → False
    assert evaluate(binop("and", lit(True), lit(False)), empty) is False
    # true AND null → None
    assert evaluate(binop("and", lit(True), lit(None)), empty) is None
    # false AND null → False (short-circuit)
    assert evaluate(binop("and", lit(False), lit(None)), empty) is False
    # null AND false → False (short-circuit)
    assert evaluate(binop("and", lit(None), lit(False)), empty) is False
    # null AND null → None
    assert evaluate(binop("and", lit(None), lit(None)), empty) is None
    # null AND true → None
    assert evaluate(binop("and", lit(None), lit(True)), empty) is None


# ---------------------------------------------------------------------------
# Test 2: Three-valued null logic — OR truth table (§D-04)
# ---------------------------------------------------------------------------


def test_null_logic_or_truth_table() -> None:
    empty: dict[str, object] = {}
    # false OR false → False
    assert evaluate(binop("or", lit(False), lit(False)), empty) is False
    # true OR false → True (short-circuit)
    assert evaluate(binop("or", lit(True), lit(False)), empty) is True
    # true OR null → True (short-circuit)
    assert evaluate(binop("or", lit(True), lit(None)), empty) is True
    # null OR true → True (short-circuit)
    assert evaluate(binop("or", lit(None), lit(True)), empty) is True
    # false OR null → None
    assert evaluate(binop("or", lit(False), lit(None)), empty) is None
    # null OR false → None
    assert evaluate(binop("or", lit(None), lit(False)), empty) is None
    # null OR null → None
    assert evaluate(binop("or", lit(None), lit(None)), empty) is None


# ---------------------------------------------------------------------------
# Test 3: NOT — three-valued (§D-04)
# ---------------------------------------------------------------------------


def test_null_logic_not() -> None:
    from beava._col import _UnaryOp

    empty: dict[str, object] = {}
    assert evaluate(_UnaryOp("not", lit(True)), empty) is False  # type: ignore[arg-type]
    assert evaluate(_UnaryOp("not", lit(False)), empty) is True  # type: ignore[arg-type]
    assert evaluate(_UnaryOp("not", lit(None)), empty) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test 4: Arithmetic null propagation (§D-04)
# ---------------------------------------------------------------------------


def test_arithmetic_null_propagation() -> None:
    empty: dict[str, object] = {}
    assert evaluate(binop("+", lit(None), lit(1)), empty) is None
    assert evaluate(binop("*", lit(1), lit(None)), empty) is None
    assert evaluate(binop("-", lit(None), lit(None)), empty) is None
    # null / 0 → None (null comes first)
    assert evaluate(binop("/", lit(None), lit(0)), empty) is None


# ---------------------------------------------------------------------------
# Test 5: isnull always returns bool, never None
# ---------------------------------------------------------------------------


def test_isnull_always_returns_bool() -> None:
    from beava._col import _Call

    # isnull(None) → True
    expr_null = _Call("isnull", [lit(None)])  # type: ignore[arg-type]
    result = evaluate(expr_null, {})  # type: ignore[arg-type]
    assert result is True
    assert isinstance(result, bool)

    # isnull(non-null value) → False
    expr_val = _Call("isnull", [lit(42)])  # type: ignore[arg-type]
    result2 = evaluate(expr_val, {})  # type: ignore[arg-type]
    assert result2 is False
    assert isinstance(result2, bool)

    # isnull(missing field) → True (missing = Null)
    expr_field = _Call("isnull", [field("x")])  # type: ignore[arg-type]
    result3 = evaluate(expr_field, {})  # type: ignore[arg-type]
    assert result3 is True


# ---------------------------------------------------------------------------
# Test 6: (x == null) rewrite → isnull(x) applied before evaluation
# ---------------------------------------------------------------------------


def test_eq_null_rewrite_applied() -> None:
    # Build the raw BinOp("==", field, null) AST — the rewrite must convert
    # this to isnull(field) before evaluation, yielding True/False not None.
    eq_null_expr = binop("==", field("x"), lit(None))

    # row["x"] = None → isnull(x) = True
    assert evaluate(eq_null_expr, {"x": None}) is True  # type: ignore[arg-type]
    # row["x"] = 42 → isnull(x) = False
    assert evaluate(eq_null_expr, {"x": 42}) is False  # type: ignore[arg-type]
    # row missing "x" (treated as Null) → True
    assert evaluate(eq_null_expr, {}) is True  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test 7: Arithmetic type promotion (int+float → float; int+int → int)
# ---------------------------------------------------------------------------


def test_arithmetic_type_promotion() -> None:
    empty: dict[str, object] = {}
    # int + int → int
    result = evaluate(binop("+", lit(1), lit(2)), empty)
    assert result == 3
    assert isinstance(result, int) and not isinstance(result, bool)

    # int + float → float
    result2 = evaluate(binop("+", lit(1), lit(2.5)), empty)
    assert result2 == 3.5
    assert isinstance(result2, float)

    # float + int → float
    result3 = evaluate(binop("-", lit(4.0), lit(1)), empty)
    assert result3 == 3.0
    assert isinstance(result3, float)

    # int / int → int (truncating, not float; matches Rust v1 decision)
    result4 = evaluate(binop("/", lit(10), lit(3)), empty)
    assert result4 == 3
    assert isinstance(result4, int) and not isinstance(result4, bool)


# ---------------------------------------------------------------------------
# Test 8: Integer division by zero → None
# ---------------------------------------------------------------------------


def test_int_div_by_zero_is_none() -> None:
    empty: dict[str, object] = {}
    assert evaluate(binop("/", lit(1), lit(0)), empty) is None


# ---------------------------------------------------------------------------
# Test 9: Float division by zero → Inf (IEEE-754, matching Rust)
# ---------------------------------------------------------------------------


def test_float_div_by_zero_is_inf() -> None:
    empty: dict[str, object] = {}
    result = evaluate(binop("/", lit(1.0), lit(0.0)), empty)
    assert isinstance(result, float)
    assert math.isinf(result) and result > 0


# ---------------------------------------------------------------------------
# Test 10: i64 overflow saturates at I64_MAX / I64_MIN
# ---------------------------------------------------------------------------


def test_i64_overflow_saturates() -> None:
    empty: dict[str, object] = {}
    # MAX + 1 → MAX
    assert evaluate(binop("+", lit(I64_MAX), lit(1)), empty) == I64_MAX
    # MIN - 1 → MIN
    assert evaluate(binop("-", lit(I64_MIN), lit(1)), empty) == I64_MIN


# ---------------------------------------------------------------------------
# Test 11: NaN comparisons return False (IEEE-754)
# ---------------------------------------------------------------------------


def test_nan_comparisons_return_false() -> None:
    empty: dict[str, object] = {}
    nan = float("nan")
    assert evaluate(binop(">", lit(nan), lit(1.0)), empty) is False
    assert evaluate(binop("==", lit(nan), lit(nan)), empty) is False
    assert evaluate(binop("<", lit(nan), lit(1.0)), empty) is False
    assert evaluate(binop(">=", lit(nan), lit(nan)), empty) is False
    assert evaluate(binop("!=", lit(nan), lit(nan)), empty) is False


# ---------------------------------------------------------------------------
# Test 12: Cross-type comparison returns None
# ---------------------------------------------------------------------------


def test_cross_type_comparison_is_none() -> None:
    empty: dict[str, object] = {}
    assert evaluate(binop(">", lit(1), lit("x")), empty) is None
    assert evaluate(binop("==", lit(True), lit(1)), empty) is None


# ---------------------------------------------------------------------------
# Test 13: Cast rules — str target (§D-05)
# ---------------------------------------------------------------------------


def test_cast_to_str() -> None:
    from beava._col import _Call

    def cast(value: object, target: str) -> object:
        from beava._col import _BareIdent, _Literal

        return evaluate(
            _Call("cast", [lit(value), _Literal(_BareIdent(target))]),  # type: ignore[arg-type]
            {},
        )

    assert cast(42, "str") == "42"
    assert cast(True, "str") == "true"
    assert cast(False, "str") == "false"
    assert cast("hello", "str") == "hello"
    assert cast(None, "str") is None
    assert cast(b"bytes", "str") is None


# ---------------------------------------------------------------------------
# Test 14: Cast rules — int target (§D-05)
# ---------------------------------------------------------------------------


def test_cast_to_int() -> None:
    from beava._col import _BareIdent, _Call, _Literal

    def cast(value: object, target: str) -> object:
        return evaluate(
            _Call("cast", [lit(value), _Literal(_BareIdent(target))]),  # type: ignore[arg-type]
            {},
        )

    assert cast(3.9, "int") == 3  # truncate toward zero
    assert cast(-3.9, "int") == -3  # truncate toward zero (not floor)
    assert cast("42", "int") == 42
    assert cast("abc", "int") is None  # parse failure → None
    assert cast(True, "int") == 1
    assert cast(False, "int") == 0
    assert cast(None, "int") is None
    assert cast(b"x", "int") is None


# ---------------------------------------------------------------------------
# Test 15: Cast rules — float target (§D-05)
# ---------------------------------------------------------------------------


def test_cast_to_float() -> None:
    from beava._col import _BareIdent, _Call, _Literal

    def cast(value: object, target: str) -> object:
        return evaluate(
            _Call("cast", [lit(value), _Literal(_BareIdent(target))]),  # type: ignore[arg-type]
            {},
        )

    assert cast(7, "float") == 7.0
    assert cast(True, "float") == 1.0
    assert cast(False, "float") == 0.0
    assert cast("3.14", "float") == pytest.approx(3.14)
    assert cast("abc", "float") is None
    assert cast(None, "float") is None


# ---------------------------------------------------------------------------
# Test 16: Cast rules — bool target (§D-05)
# ---------------------------------------------------------------------------


def test_cast_to_bool() -> None:
    from beava._col import _BareIdent, _Call, _Literal

    def cast(value: object, target: str) -> object:
        return evaluate(
            _Call("cast", [lit(value), _Literal(_BareIdent(target))]),  # type: ignore[arg-type]
            {},
        )

    assert cast(1, "bool") is True
    assert cast(0, "bool") is False
    assert cast(1.0, "bool") is True
    assert cast(0.0, "bool") is False
    assert cast(float("nan"), "bool") is False
    assert cast("true", "bool") is True
    assert cast("false", "bool") is False
    assert cast("other", "bool") is None
    assert cast(None, "bool") is None
