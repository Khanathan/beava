"""Tests for event dot-access — ``e.email`` → ``bv.col("email")`` (PR 2).

Two code paths, both must work and both must reject ``_*`` names:

  * **Class-level** — ``Click.email`` where ``Click`` is the result of
    ``@bv.event class Click``. ``Click`` is the class itself (not an
    instance); the only ``__getattr__`` Python consults is the one on
    the metaclass. PR 2 installs a small metaclass per ``@bv.event``
    class.
  * **Instance-level** — ``d.email`` where ``d`` is an
    ``EventDerivation`` (the result of a chain step like
    ``Click.with_columns(...)``). Regular instance lookup, so
    ``__getattr__`` on ``_ChainMixin`` handles it.

Both paths route through one shared helper so the ``_*`` guard and the
column-lookup rule are defined exactly once.

Pure-Python AST tests — no embed-mode binary needed.

These tests fail until PR 2 Step 4 wires both paths.
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._col import _Col


@bv.event
class Click:
    user_id: str
    email: str
    amount_usd: float


def test_class_level_dot_access_returns_col() -> None:
    """``Click.email`` resolves to ``bv.col("email")`` via the metaclass.

    Class attribute lookup does NOT trigger ``__getattr__`` on the class
    itself — only ``__getattr__`` on the metaclass catches it.
    """
    expr = Click.email
    assert isinstance(expr, _Col)
    assert expr.to_expr_string() == "email"


def test_class_level_underscore_raises() -> None:
    """``_*`` lookups must NOT route to ``bv.col(...)`` — otherwise
    ``repr``, pickle, IDE introspection, and the framework's own private
    attributes all get intercepted."""
    with pytest.raises(AttributeError):
        _ = Click._not_a_field


def test_class_level_existing_attr_still_works() -> None:
    """``Click._chain`` is a real attribute set by ``_make_event_source``.

    ``__getattr__`` only fires on FAILED normal lookup, so the metaclass
    must not shadow the real ``_chain``.
    """
    assert isinstance(Click._chain, list)


def test_derivation_dot_access() -> None:
    """``derivation.email`` — instance-level path through
    ``_ChainMixin.__getattr__``."""
    d = Click.with_columns(double_amount=bv.col("amount_usd") * 2)
    expr = d.email
    assert isinstance(expr, _Col)
    assert expr.to_expr_string() == "email"


def test_derivation_underscore_raises() -> None:
    """Same ``_*`` rule applies on the instance path."""
    d = Click.with_columns(double_amount=bv.col("amount_usd") * 2)
    with pytest.raises(AttributeError):
        _ = d._not_a_field


def test_dot_access_inside_bv_event_def() -> None:
    """The canonical RFC §7 shape — ``@bv.event def F(e: Click): …``.

    Inside the function body, ``e`` is bound to ``Click`` (the class
    itself, because the decorator calls ``fn(Click)``), so ``e.email``
    exercises the metaclass path. This pins the whole decoration →
    metaclass lookup → column reference chain.
    """

    @bv.event
    def Tagged(e: Click):
        return e.with_columns(domain=e.email)

    assert Tagged._kind == "event_derivation"
