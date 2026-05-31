"""TDD red — bv.when().then().otherwise() builder (PR 4).

What this file checks:
  1. The builder produces the same wire output as ``bv.if_else``.
  2. The intermediate object from ``.then()`` is NOT an ``_Expr`` — passing
     it where an expression is expected fails naturally, which is the
     whole safety point of the builder shape.
  3. All three arg positions coerce Python literals, same as ``bv.if_else``.

Why a builder at all: ``bv.when(cond).then(a).otherwise(b)`` reads like
English and makes the condition visually prominent. It's the idiomatic form
for humans writing feature definitions; ``bv.if_else`` is for generated
code and cases where the builder feels like overhead.

Status: fails until Step 9 of PR 4 adds ``bv.when`` to the module.
"""
from __future__ import annotations

import beava as bv
from beava._col import _Call, _Expr


# ── wire output matches bv.if_else ───────────────────────────────────────────


def test_when_then_otherwise_emits_if_else_wire_string() -> None:
    """The builder and the direct function are two spellings of the same
    thing. Their wire output must be identical so the server sees no
    difference."""
    direct = bv.if_else(bv.col("flag"), bv.col("a"), bv.col("b"))
    builder = bv.when(bv.col("flag")).then(bv.col("a")).otherwise(bv.col("b"))
    assert builder.to_expr_string() == direct.to_expr_string()
    assert builder.to_expr_string() == "if_else(flag, a, b)"


def test_when_then_otherwise_produces_call_node() -> None:
    """The final result of the builder chain is a ``_Call`` node."""
    expr = bv.when(bv.col("c")).then(1).otherwise(0)
    assert isinstance(expr, _Call)
    assert expr.name == "if_else"
    assert len(expr.args) == 3


# ── intermediate is not an _Expr ─────────────────────────────────────────────


def test_when_then_result_is_not_an_expr() -> None:
    """The object returned by ``.then()`` must NOT be an ``_Expr``.
    If it were, a user could accidentally pass it to a registration call
    before calling ``.otherwise()``, silently producing a broken expression.
    Keeping it a non-_Expr type forces ``.otherwise()`` to always be called."""
    intermediate = bv.when(bv.col("c")).then(1)
    assert not isinstance(intermediate, _Expr)


def test_when_result_is_not_an_expr() -> None:
    """The object returned by ``bv.when()`` itself is also not an ``_Expr``,
    so a user can't accidentally pass an incomplete builder as an expression."""
    when_obj = bv.when(bv.col("c"))
    assert not isinstance(when_obj, _Expr)


# ── argument coercion ─────────────────────────────────────────────────────────


def test_builder_coerces_literal_condition() -> None:
    """Plain Python values in the condition position are wrapped as literals."""
    expr = bv.when(True).then(bv.col("x")).otherwise(bv.col("y"))
    assert expr.to_expr_string() == "if_else(true, x, y)"


def test_builder_coerces_literal_branches() -> None:
    """Plain Python values in then/otherwise are wrapped as literals."""
    expr = bv.when(bv.col("flag")).then(100).otherwise(0)
    assert expr.to_expr_string() == "if_else(flag, 100, 0)"

    expr_str = bv.when(bv.col("flag")).then("yes").otherwise("no")
    assert expr_str.to_expr_string() == "if_else(flag, 'yes', 'no')"


# ── composition ───────────────────────────────────────────────────────────────


def test_builder_composes_with_string_methods() -> None:
    """Builder branches can themselves be method-chained expressions."""
    expr = (
        bv.when(bv.col("email").contains("@"))
        .then(bv.col("email").lower())
        .otherwise(bv.lit("unknown"))
    )
    assert (
        expr.to_expr_string()
        == "if_else(contains(email, '@'), lower(email), 'unknown')"
    )


def test_builder_nested_as_otherwise() -> None:
    """A builder result can be used as the otherwise-branch of another
    builder, producing the same nested wire string as chained if_else."""
    expr = (
        bv.when(bv.col("a"))
        .then(bv.col("x"))
        .otherwise(bv.when(bv.col("b")).then(bv.col("y")).otherwise(bv.col("z")))
    )
    assert expr.to_expr_string() == "if_else(a, x, if_else(b, y, z))"
