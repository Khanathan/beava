"""TDD red — ``@bv.expr`` recursion guards (PR 5).

Recursion comes in two flavors, caught two ways:

  * **Direct self-recursion** — caught at *decoration time* by a static
    scan of the lowered body for a call to the function's own name. Needed
    because ``if``/``elif`` are expanded immediately, so a recursive body
    would build its branches forever and crash with a raw ``RecursionError``.
  * **Mutual recursion** (``a → b → a``) — caught at *call time* by a
    per-thread frame stack: re-entering a function already on the stack is
    a loop. Spans several files, so it can't be seen from one source.

Cross-file mutual recursion uses the ``_expr_recursion_moda`` /
``_expr_recursion_modb`` companion modules so the cycle report shows
distinct per-hop file paths. A >5-hop cycle exercises the
``... (k more) ...`` middle elision.

The mutual-recursion functions are module-level so name resolution finds
them in module globals (a function nested in a test body would make them
free variables the rebuilt function can't see).

Status: fails at import until ``bv.expr`` exists (PR 5 Step 8).
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._errors import RegistrationError


# ── direct self-recursion → rejected at decoration time ──────────────────────


def test_direct_self_recursion_rejected_at_decoration() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def fact(n):
            return fact(n)

    assert exc.value.code == "expr_recursive_call"


def test_direct_self_recursion_through_branch_rejected() -> None:
    """Self-calls hidden inside an ``if`` only become visible after lowering,
    so the static scan must run after the walk."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(n, c):
            if c:
                return f(n)
            return n

    assert exc.value.code == "expr_recursive_call"


# ── mutual recursion a → b → a → rejected at call time ───────────────────────


@bv.expr
def a(x):
    return b(x)


@bv.expr
def b(x):
    return a(x)


def test_mutual_recursion_rejected_at_call_time() -> None:
    """Neither ``a`` nor ``b`` calls itself, so decoration passes; the loop
    is caught when ``a`` is asked to build while already on the stack."""
    with pytest.raises(RegistrationError) as exc:
        a(bv.col("x"))

    assert exc.value.code == "expr_recursive_call"


# ── cross-file mutual recursion → per-hop file paths differ ──────────────────


def test_cross_file_mutual_recursion_reports_both_files() -> None:
    """Two functions in two different files call each other in a loop:
    ``a`` (in file A) calls ``b`` (in file B), and ``b`` calls ``a`` right
    back. Neither one calls itself, so nothing looks wrong when each is first
    written. The loop only shows up when ``a`` actually runs and ends up
    asking itself to build again. We check it is caught, and that the error
    names both files so the user can see the loop crosses file boundaries."""
    from . import _expr_recursion_moda as moda

    with pytest.raises(RegistrationError) as exc:
        moda.a(bv.col("x"))

    assert exc.value.code == "expr_recursive_call"
    message = str(exc.value)
    assert "_expr_recursion_moda" in message
    assert "_expr_recursion_modb" in message


# ── >5-hop cycle → middle elided as "... (k more) ..." ───────────────────────


@bv.expr
def h1(x):
    return h2(x)


@bv.expr
def h2(x):
    return h3(x)


@bv.expr
def h3(x):
    return h4(x)


@bv.expr
def h4(x):
    return h5(x)


@bv.expr
def h5(x):
    return h6(x)


@bv.expr
def h6(x):
    return h1(x)


def test_long_cycle_elides_middle() -> None:
    """A six-hop cycle ``h1 → … → h6 → h1`` trims the middle but keeps the
    first and last hop so the closing edge stays visible."""
    with pytest.raises(RegistrationError) as exc:
        h1(bv.col("x"))

    assert exc.value.code == "expr_recursive_call"
    assert "more" in str(exc.value)
