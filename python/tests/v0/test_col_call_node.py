"""Tests for the new ``_Call`` AST node (PR 2).

Pins:
  * wire output for 0 / 1 / many-arg calls (commas + spaces between args)
  * ``_Call`` is hashable — every ``_Expr`` subclass restores ``__hash__``
    explicitly because the overridden ``__eq__`` would otherwise make the
    class unhashable
  * ``_Call.__eq__`` returns an ``_Expr`` (a ``_BinOp("==", …)``), NOT a
    Python ``bool`` — matches the ``_Expr`` base-class convention so AST
    equality builds a comparison node rather than collapsing to a truth
    value

Pure-Python AST tests — no embed-mode binary needed, no skipif gate.

"""
from __future__ import annotations

import beava as bv
from beava._col import _BinOp, _Call, _Literal


def test_call_renders_with_commas() -> None:
    """One-arg call renders as ``name(arg)`` on the wire."""
    expr = _Call("log1p", (bv.col("x"),))
    assert expr.to_expr_string() == "log1p(x)"


def test_call_renders_multi_arg() -> None:
    """Multi-arg call joins args with ``", "`` (comma + space)."""
    expr = _Call("clip", (bv.col("x"), _Literal(0), _Literal(100)))
    assert expr.to_expr_string() == "clip(x, 0, 100)"


def test_call_zero_arg() -> None:
    """Zero-arg call renders as ``name()`` with empty parens."""
    expr = _Call("now", ())
    assert expr.to_expr_string() == "now()"


def test_call_is_hashable() -> None:
    """``_Call`` must be usable as a dict key / set member.

    Building a set forces Python to call ``__hash__``; if the class were
    unhashable (default state after overriding ``__eq__``), this raises
    ``TypeError: unhashable type``.
    """
    a = _Call("foo", (bv.col("x"),))
    b = _Call("foo", (bv.col("x"),))
    _ = {a, b}


def test_call_eq_returns_expr() -> None:
    """``a == b`` on ``_Expr`` instances builds a ``_BinOp("==", …)`` node,
    not a Python ``bool`` — same convention as every other ``_Expr``."""
    a = _Call("foo", (bv.col("x"),))
    assert isinstance(a == a, _BinOp)
