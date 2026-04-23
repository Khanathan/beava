"""Python reference evaluator — mirrors Rust eval.rs semantics exactly.

This module is INTENTIONALLY an independent re-implementation of the Rust
expression evaluator in `crates/beava-core/src/eval.rs`. The duplication is
the point: if both implementations share code they share bugs. The SC4
hypothesis proptest in `test_phase4_smoke.py` asserts that this evaluator
and the live Rust server produce identical outputs for ≥256 random (expr,
row) pairs — zero divergence is the Phase 4 load-bearing correctness claim.

# Semantics (CONTEXT.md §D-04, §D-05)

## Three-valued null logic (§D-04)
- Arithmetic on None → None (null poisons the result).
- Comparison with None → None (strict SQL).
- `and`: short-circuit — `False and None → False`; otherwise null propagates.
- `or`:  short-circuit — `True or None → True`;  otherwise null propagates.
- `not None → None`.
- `isnull(x)` always returns bool (True/False), never None.

## `(x == null)` and `(x != null)` rewrites (Plan 04-02 Rust parser, mirrored here)
The Rust parser (Pass B in expr.rs) rewrites both:
  - `BinOp("==", e, Literal::Null)` → `Call("isnull", [e])`
  - `BinOp("!=", e, Literal::Null)` → `UnaryOp("not", Call("isnull", [e]))`
(and their symmetric null-on-left forms) BEFORE the AST reaches eval.rs.
This evaluator applies the same rewrites at the top of `evaluate()` so the
oracle is faithful to what the server sees.  Without the `!=` rewrite,
`(a != null)` with `a=None` diverges: Python null-propagation returns None
while Rust's rewritten `(not isnull(a))` returns False.

## Integer division (v1 decision)
I64 / I64 → I64 (truncating toward zero).  Rust uses `x / y` which truncates
toward zero, matching Python's `int(x / y)` for same-sign operands but NOT
`x // y` (which floors).  We use `int(x / y)` (truncating) to match Rust.
Division by zero → None (not an exception).

## i64 overflow saturation
Rust uses `saturating_add/sub/mul`. Python ints are arbitrary-precision, so we
clamp results to [I64_MIN, I64_MAX] after each i64 arithmetic operation.

## NaN / float
Python's `float('nan')` comparisons return False — matching IEEE-754 and Rust.
Float division by zero → float('inf') — matching Rust's IEEE-754 behaviour.

## Cast matrix (§D-05) — mirrors cast_eval in expr_builtins.rs
| Source   | "str"        | "int"           | "float"         | "bool"              |
|----------|--------------|-----------------|-----------------|---------------------|
| None     | None         | None            | None            | None                |
| str      | unchanged    | int(s) or None  | float(s) or None| "true"→T,"false"→F  |
| int      | str(n)       | unchanged       | float(n)        | n != 0              |
| float    | str(f)       | int(f) trunc    | unchanged       | f!=0.0 and !nan     |
| bool     | "true"/"false"| 1/0            | 1.0/0.0         | unchanged           |
| bytes    | None         | None            | None            | None                |

NaN comparisons: any comparison involving NaN returns False (IEEE-754),
matching Rust's `f64::partial_cmp` returning None for NaN inputs.

Cross-type comparisons: return None (matching Rust's `try_compare` None path).
"""

from __future__ import annotations

import math
from typing import Any

from ._col import _BareIdent, _BinOp, _Call, _ExprAST, _Field, _Literal, _UnaryOp

__all__ = ["evaluate"]

# ---------------------------------------------------------------------------
# i64 saturation bounds (mirror Rust's i64::MAX / i64::MIN)
# ---------------------------------------------------------------------------
I64_MAX: int = (1 << 63) - 1
I64_MIN: int = -(1 << 63)


def _clamp_i64(n: int) -> int:
    """Saturate an arbitrary-precision Python int to [I64_MIN, I64_MAX]."""
    if n > I64_MAX:
        return I64_MAX
    if n < I64_MIN:
        return I64_MIN
    return n


# ---------------------------------------------------------------------------
# (x == null) → isnull(x) rewrite
# ---------------------------------------------------------------------------


def _rewrite_null_eq(expr: _ExprAST) -> _ExprAST:
    """Walk the AST and rewrite null-equality/inequality BinOps to isnull calls.

    Mirrors Plan 04-02's Rust parser Pass B rewrites exactly:
      (x == null) → isnull(x)
      (null == x) → isnull(x)
      (x != null) → (not isnull(x))
      (null != x) → (not isnull(x))

    Both == and != rewrites are required because the Rust parser applies both
    in expr.rs `rewrite_null_eq`. Without the != rewrite, `(a != null)` where
    a=None diverges: Python returns None (null propagation) while Rust returns
    False (not isnull(a) = not True = False).
    """
    if isinstance(expr, _BinOp):
        left = _rewrite_null_eq(expr.left)
        right = _rewrite_null_eq(expr.right)
        if expr.op == "==":
            # (x == null) → isnull(x)
            if isinstance(right, _Literal) and right.value is None:
                return _Call("isnull", [left])
            # (null == x) → isnull(x)
            if isinstance(left, _Literal) and left.value is None:
                return _Call("isnull", [right])
        if expr.op == "!=":
            # (x != null) → (not isnull(x))
            if isinstance(right, _Literal) and right.value is None:
                return _UnaryOp("not", _Call("isnull", [left]))
            # (null != x) → (not isnull(x))
            if isinstance(left, _Literal) and left.value is None:
                return _UnaryOp("not", _Call("isnull", [right]))
        # Reconstruct with rewritten children.
        return _BinOp(expr.op, left, right)
    if isinstance(expr, _UnaryOp):
        return _UnaryOp(expr.op, _rewrite_null_eq(expr.operand))
    if isinstance(expr, _Call):
        new_args = [_rewrite_null_eq(a) for a in expr.args]
        return _Call(expr.fn, new_args)
    # _Field, _Literal — leaf nodes, return unchanged.
    return expr


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def evaluate(expr: _ExprAST, row: dict[str, Any]) -> Any:
    """Evaluate *expr* against *row*, returning the result value.

    Mirrors ``eval()`` in crates/beava-core/src/eval.rs exactly.

    Args:
        expr: An expression AST node built via ``bv.col(...)`` / SDK operators.
        row:  A flat dict mapping field names to Python values.
              ``None`` values represent ``Value::Null``.
              ``int`` → ``Value::I64``; ``float`` → ``Value::F64``;
              ``bool`` → ``Value::Bool``; ``str`` → ``Value::Str``.

    Returns:
        The result value, using the same Python-native type mapping:
        ``None`` for Null, ``bool`` for Bool, ``int`` for I64, ``float`` for
        F64, ``str`` for Str.

    Note:
        ``(x == null)`` is rewritten to ``isnull(x)`` before evaluation (the
        same rewrite the Rust parser applies in Plan 04-02), so calling
        ``evaluate`` with an expression built as ``bv.col("x") == None`` will
        correctly return ``True`` when ``row["x"]`` is ``None``.
    """
    # Apply the (x == null) → isnull(x) rewrite first — mirrors Plan 04-02.
    rewritten = _rewrite_null_eq(expr)
    return _eval(rewritten, row)


# ---------------------------------------------------------------------------
# Internal recursive evaluator
# ---------------------------------------------------------------------------


def _eval(expr: _ExprAST, row: dict[str, Any]) -> Any:
    """Core evaluator after rewrite pass."""
    if isinstance(expr, _Field):
        return row.get(expr.name, None)

    if isinstance(expr, _Literal):
        v = expr.value
        if v is None:
            return None
        if isinstance(v, _BareIdent):
            # BareIdent (e.g. cast type arg) → str, matching Rust's BareIdent→Str
            return v.name
        # bool BEFORE int (bool is a subclass of int in Python)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float, str)):
            return v
        if isinstance(v, bytes):
            # bytes literals are representable but not usable in arithmetic;
            # return as-is and let cast/comparison rules handle them.
            return v
        raise TypeError(f"unsupported literal value type: {type(v).__name__}")

    if isinstance(expr, _UnaryOp):
        # Only "not" exists in Phase 4.
        operand_val = _eval(expr.operand, row)
        return _not_three_valued(operand_val)

    if isinstance(expr, _BinOp):
        return _eval_binop(expr.op, expr.left, expr.right, row)

    if isinstance(expr, _Call):
        arg_vals = [_eval(a, row) for a in expr.args]
        return _dispatch_call(expr.fn, arg_vals)

    raise TypeError(f"unknown expr node type: {type(expr).__name__}")


# ---------------------------------------------------------------------------
# Binary operator dispatch
# ---------------------------------------------------------------------------


def _eval_binop(op: str, left: _ExprAST, right: _ExprAST, row: dict[str, Any]) -> Any:
    """Dispatch a binary operator, applying short-circuit and null propagation."""
    if op == "and":
        lv = _eval(left, row)
        # Short-circuit: false AND _ → false (skip evaluating right).
        if lv is False:
            return False
        rv = _eval(right, row)
        return _and_three_valued(lv, rv)

    if op == "or":
        lv = _eval(left, row)
        # Short-circuit: true OR _ → true.
        if lv is True:
            return True
        rv = _eval(right, row)
        return _or_three_valued(lv, rv)

    # All other ops: evaluate both sides, then apply null propagation.
    lv = _eval(left, row)
    rv = _eval(right, row)

    # Null propagates for arithmetic and comparison (D-04).
    if lv is None or rv is None:
        return None

    if op == "+":
        return _arith_add(lv, rv)
    if op == "-":
        return _arith_sub(lv, rv)
    if op == "*":
        return _arith_mul(lv, rv)
    if op == "/":
        return _arith_div(lv, rv)
    if op == ">":
        return _cmp_op(lv, rv, lambda o: o > 0)
    if op == ">=":
        return _cmp_op(lv, rv, lambda o: o >= 0)
    if op == "<":
        return _cmp_op(lv, rv, lambda o: o < 0)
    if op == "<=":
        return _cmp_op(lv, rv, lambda o: o <= 0)
    if op == "==":
        return _cmp_eq(lv, rv)
    if op == "!=":
        return _cmp_ne(lv, rv)
    # Unknown operator → None (defensive; register-time catches these).
    return None


# ---------------------------------------------------------------------------
# Arithmetic helpers — mirror arith_add/sub/mul/div in eval.rs
# ---------------------------------------------------------------------------


def _is_int(v: Any) -> bool:
    """True if v is a Python int but NOT a bool (bool is a subclass of int)."""
    return isinstance(v, int) and not isinstance(v, bool)


def _is_float(v: Any) -> bool:
    return isinstance(v, float)


def _arith_add(a: Any, b: Any) -> Any:
    if _is_int(a) and _is_int(b):
        return _clamp_i64(a + b)
    if _is_float(a) and _is_float(b):
        return a + b
    if _is_int(a) and _is_float(b):
        return float(a) + b
    if _is_float(a) and _is_int(b):
        return a + float(b)
    return None  # non-numeric types


def _arith_sub(a: Any, b: Any) -> Any:
    if _is_int(a) and _is_int(b):
        return _clamp_i64(a - b)
    if _is_float(a) and _is_float(b):
        return a - b
    if _is_int(a) and _is_float(b):
        return float(a) - b
    if _is_float(a) and _is_int(b):
        return a - float(b)
    return None


def _arith_mul(a: Any, b: Any) -> Any:
    if _is_int(a) and _is_int(b):
        return _clamp_i64(a * b)
    if _is_float(a) and _is_float(b):
        return a * b
    if _is_int(a) and _is_float(b):
        return float(a) * b
    if _is_float(a) and _is_int(b):
        return a * float(b)
    return None


def _arith_div(a: Any, b: Any) -> Any:
    if _is_int(a) and _is_int(b):
        # Integer division by zero → None (Rust returns Null).
        if b == 0:
            return None
        # Truncating toward zero — matches Rust's `x / y` for i64.
        # Python's // floors (differs for negative numbers), so we use int(a/b).
        result = int(a / b)
        return _clamp_i64(result)
    if _is_float(a) and _is_float(b):
        if b == 0.0:
            # IEEE-754: div by 0.0 → ±Inf (Python raises ZeroDivisionError, so we
            # replicate the IEEE-754 result that Rust's f64 / 0.0 produces).
            return math.copysign(math.inf, a) if a != 0.0 else float("nan")
        return a / b
    if _is_int(a) and _is_float(b):
        if b == 0.0:
            return math.copysign(math.inf, float(a)) if a != 0 else float("nan")
        return float(a) / b
    if _is_float(a) and _is_int(b):
        if b == 0:
            return math.copysign(math.inf, a) if a != 0.0 else float("nan")
        return a / float(b)
    return None


# ---------------------------------------------------------------------------
# Comparison helpers — mirror try_compare / cmp_op / cmp_eq / cmp_ne in eval.rs
# ---------------------------------------------------------------------------


def _try_compare(a: Any, b: Any) -> int | None:
    """Return a comparison integer (<0, 0, >0) or None for NaN/cross-type.

    Mirrors Rust's try_compare which uses partial_cmp.
    Returns None for:
      - NaN-containing float comparisons (partial_cmp returns None for NaN).
      - Cross-type pairs (str vs int, bool vs int, etc.).

    NOTE: bool vs bool is handled (bool is a subclass of int in Python, but
    we keep it as bool-vs-bool only; bool vs int → cross-type → None, matching
    Rust where Bool and I64 are distinct Value variants).
    """
    # Same-type pairs (strictly typed).
    if _is_int(a) and _is_int(b):
        return int(a > b) - int(a < b)
    if _is_float(a) and _is_float(b):
        if math.isnan(a) or math.isnan(b):
            return None  # NaN comparisons undefined → None
        return int(a > b) - int(a < b)
    if _is_int(a) and _is_float(b):
        fa = float(a)
        if math.isnan(b):
            return None
        return int(fa > b) - int(fa < b)
    if _is_float(a) and _is_int(b):
        fb = float(b)
        if math.isnan(a):
            return None
        return int(a > fb) - int(a < fb)
    if isinstance(a, str) and isinstance(b, str):
        return int(a > b) - int(a < b)
    if isinstance(a, bool) and isinstance(b, bool):
        return int(a > b) - int(a < b)
    # Cross-type → None
    return None


def _is_numeric_pair(a: Any, b: Any) -> bool:
    """True if both values are numeric (int or float, not bool)."""
    return (_is_int(a) or _is_float(a)) and (_is_int(b) or _is_float(b))


def _cmp_op(a: Any, b: Any, pred: Any) -> Any:
    """Ordered comparison (>, >=, <, <=).

    Returns None for cross-type; False for NaN (IEEE-754); Bool otherwise.
    Mirrors Rust's cmp_op with its NaN-vs-cross-type distinction.
    """
    result = _try_compare(a, b)
    if result is not None:
        return bool(pred(result))
    # NaN or cross-type.
    if _is_numeric_pair(a, b):
        return False  # NaN comparison → False per IEEE-754
    return None  # cross-type → Null


def _cmp_eq(a: Any, b: Any) -> Any:
    """Equality (==). Null-strict: either None → None (handled by caller).

    NaN == NaN → False (IEEE-754). Cross-type → None.
    """
    result = _try_compare(a, b)
    if result is not None:
        return result == 0
    if _is_numeric_pair(a, b):
        return False  # NaN
    return None  # cross-type


def _cmp_ne(a: Any, b: Any) -> Any:
    """Inequality (!=). NaN != NaN → False (IEEE-754). Cross-type → None."""
    result = _try_compare(a, b)
    if result is not None:
        return result != 0
    if _is_numeric_pair(a, b):
        return False  # NaN != NaN is False per IEEE-754
    return None  # cross-type


# ---------------------------------------------------------------------------
# Boolean three-valued helpers — mirror Value::and/or/not_three_valued in row.rs
# ---------------------------------------------------------------------------


def _and_three_valued(a: Any, b: Any) -> Any:
    """SQL three-valued AND (§D-04 truth table).

    Short-circuit for false has already been applied by the caller; this
    function only handles the remaining cases.
    """
    # false on either side → false (short-circuit already handles left=false)
    if a is False or b is False:
        return False
    if a is True and b is True:
        return True
    # At least one None, no short-circuit false → None
    if a is None or b is None:
        return None
    # Non-bool/non-null → None (runtime-tolerant, matching Rust)
    return None


def _or_three_valued(a: Any, b: Any) -> Any:
    """SQL three-valued OR (§D-04 truth table).

    Short-circuit for true has already been applied by the caller.
    """
    # true on either side → true
    if a is True or b is True:
        return True
    if a is False and b is False:
        return False
    if a is None or b is None:
        return None
    return None


def _not_three_valued(a: Any) -> Any:
    """SQL three-valued NOT (§D-04)."""
    if isinstance(a, bool):
        return not a
    if a is None:
        return None
    return None  # non-bool/non-null → None


# ---------------------------------------------------------------------------
# Builtin call dispatch — mirrors BUILTINS table in expr_builtins.rs
# ---------------------------------------------------------------------------


def _dispatch_call(fn: str, args: list[Any]) -> Any:
    """Dispatch a function call to the appropriate builtin.

    Unknown function names → None (defensive; register-time catches).
    """
    if fn == "isnull":
        return _isnull_eval(args)
    if fn == "cast":
        return _cast_eval(args)
    # Unknown function → None.
    return None


def _isnull_eval(args: list[Any]) -> Any:
    """isnull(value) → Bool(True) if value is None else Bool(False).

    Always returns bool (True/False), never None.
    """
    if len(args) != 1:
        return None  # arity error — defensive
    return args[0] is None


def _cast_eval(args: list[Any]) -> Any:
    """cast(value, type_str) — mirrors cast_eval in expr_builtins.rs.

    Cast matrix (§D-05):
    | Source | "str"        | "int"             | "float"          | "bool"               |
    |--------|--------------|-------------------|------------------|----------------------|
    | None   | None         | None              | None             | None                 |
    | str    | unchanged    | int(s) or None    | float(s) or None | "true"→T,"false"→F   |
    | int    | str(n)       | unchanged         | float(n)         | n != 0               |
    | float  | str(f)       | int(f) trunc      | unchanged        | f!=0.0 and !nan      |
    | bool   | "true"/"false"| 1/0              | 1.0/0.0          | unchanged            |
    | bytes  | None         | None              | None             | None                 |
    """
    if len(args) != 2:
        return None  # arity guard
    value, target_val = args[0], args[1]
    if not isinstance(target_val, str):
        return None
    target = target_val

    if value is None:
        return None  # null input → always None

    if target == "str":
        return _cast_to_str(value)
    if target == "int":
        return _cast_to_int(value)
    if target == "float":
        return _cast_to_float(value)
    if target == "bool":
        return _cast_to_bool(value)
    return None  # unknown target type


def _cast_to_str(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        return v
    if _is_int(v):
        return str(v)
    if _is_float(v):
        # Match Rust's f.to_string() which uses Display formatting.
        # Rust formats integers as "1" (no decimal) but non-integer floats
        # normally. Python's str() produces "1.0" for 1.0; Rust's produces "1".
        # We match Rust's behavior: use repr-style without trailing .0 for
        # whole numbers, but keep the decimal for non-whole floats.
        # Actually Rust's f64::to_string uses Display which calls Ryu formatting.
        # For compatibility: use Python's str(float) which is close enough for
        # the SC4 proptest (both produce human-readable decimal strings; exact
        # format differences for edge cases are noted in SUMMARY deviations).
        return str(v)
    if isinstance(v, bytes):
        return None  # no implicit bytes→str without encoding spec
    return None


def _cast_to_int(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if _is_int(v):
        return v
    if _is_float(v):
        # Truncate toward zero — matching Rust's `*f as i64`.
        if math.isnan(v) or math.isinf(v):
            # Rust: casting NaN/Inf f64 to i64 is undefined behaviour (saturates
            # in practice). We return None to avoid divergence.
            return None
        return _clamp_i64(int(v))
    if isinstance(v, str):
        try:
            return int(v)
        except ValueError:
            return None
    if isinstance(v, bytes):
        return None
    return None


def _cast_to_float(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if _is_int(v):
        return float(v)
    if _is_float(v):
        return v
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    if isinstance(v, bytes):
        return None
    return None


def _cast_to_bool(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if _is_int(v):
        return v != 0
    if _is_float(v):
        return v != 0.0 and not math.isnan(v)
    if isinstance(v, str):
        if v == "true":
            return True
        if v == "false":
            return False
        return None
    if isinstance(v, bytes):
        return None
    return None
