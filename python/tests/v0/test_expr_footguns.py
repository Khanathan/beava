"""Tests for the three ``_Expr`` footgun guards (PR 2).

The guards turn three normally-silent Python operations into loud
``TypeError``\\ s when called on a beava expression:

  * ``__bool__`` — fires for any Python construct that needs a truth
    value: ``if x:``, ``and``, ``or``, ``not``, ternary ``a if c else b``.
    Without the guard, a beava expression is truthy by default, so
    ``"yes" if (col > 0) else "no"`` would silently pick ``"yes"``. That
    is the silent-first-branch footgun PR 5's ``@bv.expr`` decorator
    rewrites away at the source level. Outside ``@bv.expr``, the guard
    turns the same code into a loud error.
  * ``__iter__`` — looping over an expression makes no sense at SDK
    time; pin the error so users get a clear signal.
  * ``__len__`` — ``len(expr)`` is the wrong tool; the guard points at
    ``bv.length(...)`` which builds a per-event length feature.

Pure-Python AST tests — no embed-mode binary needed.

These tests fail until PR 2 Step 3 installs the three guards on the
``_Expr`` base class.
"""
from __future__ import annotations

import pytest

import beava as bv


def test_bool_raises_with_hint() -> None:
    """``__bool__`` must point at the right replacements: ``bv.if_else``
    for conditionals, ``&`` / ``|`` for combining predicates."""
    with pytest.raises(TypeError, match=r"bv\.if_else.*&.*\|"):
        bool(bv.col("x") > 0)


def test_ternary_outside_bv_expr_raises() -> None:
    """A bare ternary on an expression must fail loud.

    Without the guard, the truthy default would silently pick the first
    branch — the canonical PR 5 motivating footgun.
    """
    with pytest.raises(TypeError):
        result = "yes" if bv.col("x") > 0 else "no"
        _ = result


def test_and_or_outside_bv_expr_raises() -> None:
    """Python's ``and`` / ``or`` keywords short-circuit via ``__bool__``.

    Users must reach for ``&`` / ``|`` instead, which build AST nodes
    that short-circuit at the server, not in Python.
    """
    with pytest.raises(TypeError):
        _ = bv.col("x") > 0 and bv.col("y") > 0
    with pytest.raises(TypeError):
        _ = bv.col("x") > 0 or bv.col("y") > 0


def test_iter_raises() -> None:
    """Looping over an expression has no meaning at SDK time; the error
    message must contain ``not iterable`` so the pointer is obvious."""
    with pytest.raises(TypeError, match="not iterable"):
        for _ in bv.col("x"):
            pass


def test_len_raises_with_pointer_to_bv_length() -> None:
    """``len(expr)`` must point at ``bv.length(x)`` — the helper that
    builds a length feature instead of asking Python for a count."""
    with pytest.raises(TypeError, match=r"bv\.length"):
        len(bv.col("x"))


def test_amp_pipe_still_work() -> None:
    """Guards must NOT block ``&`` / ``|`` — those are the supported
    boolean combinators that produce ``_BinOp("and"/"or", …)`` nodes."""
    result = (bv.col("x") > 0) & (bv.col("y") < 10)
    assert result.to_expr_string() == "((x > 0) and (y < 10))"
