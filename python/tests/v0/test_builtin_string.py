"""TDD red — string-builtin Python sugar (PR 3 Step 1).

What this file checks: each new string method (`.lower()`, `.length()`,
`.contains(...)`, etc.) takes a Python expression and produces the right
text that gets sent over the wire to the server.

How it checks: build an expression in Python, ask for its wire string,
compare to what we expect. If they match, the SDK serializes correctly.

Status today: every test fails because the methods don't exist yet on
``_Expr``. Step 5 of PR 3 adds them; tests turn green at that point.

Why start from ``Click.email`` instead of ``bv.col("email")``: this
double-checks that PR 2's dot-access (the metaclass that lets you write
``Click.email``) plays nicely with PR 3's methods. If both worked alone
but broke when combined, this catches it.
"""
from __future__ import annotations

import beava as bv


@bv.event
class Click:
    """A pretend event type. PR 2's metaclass turns ``Click.email`` (etc)
    into an expression object so we can call methods on it like a
    column reference."""

    user_id: str
    email: str
    url: str
    host: str


# ── lower ───────────────────────────────────────────────────────────────────


def test_lower_method_emits_call_node() -> None:
    """``Click.email.lower()`` must produce the wire string
    ``lower(email)``. PR 5's ``@bv.expr`` translator turns Python source
    into this exact shape, so the format has to be stable."""
    expr = Click.email.lower()
    assert expr.to_expr_string() == "lower(email)"


def test_lower_via_bv_col_matches_dot_access() -> None:
    """Two ways to reference a column: dot-access on the event class
    (``Click.email``) or the explicit helper (``bv.col("email")``).
    Both must produce IDENTICAL wire output so users can pick whichever
    reads better in their code without anything changing on the wire."""
    assert (
        Click.email.lower().to_expr_string()
        == bv.col("email").lower().to_expr_string()
    )


# ── length ──────────────────────────────────────────────────────────────────


def test_length_method_emits_call_node() -> None:
    """``Click.email.length()`` must produce ``length(email)``. The
    method form is the canonical way; a ``bv.length(x)`` top-level
    helper also exists (see ``test_footgun_length_pointer.py``)."""
    expr = Click.email.length()
    assert expr.to_expr_string() == "length(email)"


# ── contains ────────────────────────────────────────────────────────────────


def test_contains_method_with_string_literal() -> None:
    """``Click.email.contains("@")`` must produce ``contains(email,
    '@')``. The plain Python string "@" gets auto-wrapped as a literal
    and rendered with single quotes (the wire grammar requires single
    quotes around string literals)."""
    expr = Click.email.contains("@")
    assert expr.to_expr_string() == "contains(email, '@')"


# ── starts_with ─────────────────────────────────────────────────────────────


def test_starts_with_method() -> None:
    """Same pattern as contains — two-arg call, literal needle gets
    quoted on the wire."""
    expr = Click.url.starts_with("https://")
    assert expr.to_expr_string() == "starts_with(url, 'https://')"


# ── ends_with ───────────────────────────────────────────────────────────────


def test_ends_with_method() -> None:
    """Same pattern as starts_with."""
    expr = Click.host.ends_with(".com")
    assert expr.to_expr_string() == "ends_with(host, '.com')"


# ── replace ─────────────────────────────────────────────────────────────────


def test_replace_method_three_args() -> None:
    """``Click.email.replace("a", "b")`` must produce ``replace(email,
    'a', 'b')``. Three-arg shape — pins that the second AND third
    string args both get auto-wrapped as literals, not just the first."""
    expr = Click.email.replace("a", "b")
    assert expr.to_expr_string() == "replace(email, 'a', 'b')"


# ── composition / chains ────────────────────────────────────────────────────


def test_method_chain_lower_then_contains() -> None:
    """Chaining methods must work: lowercase the email, then check if
    the result contains "@". This is the natural way users want to
    write things (``email.lower().contains("@")``), and PR 5's
    ``@bv.expr`` examples rely on chains reading idiomatically."""
    expr = Click.email.lower().contains("@")
    assert expr.to_expr_string() == "contains(lower(email), '@')"
