"""TDD red — ``@bv.expr`` accepted rewrites (PR 5).

One accepted case per rewrite #1–#6 (RFC §5.7 plus the Python-only
chained-comparison addition), each asserting the lowered tree's
``.to_expr_string()``. Plus the canonical RFC §8 example reused as a
fixture, with the ``is_in`` line substituted away (``is_in`` is not in
the v0 surface).

Pure-AST tests — no embed-mode binary needed; the decorator lowers and
compiles at decoration time, and calling the wrapper with columns builds
an ``_Expr`` whose wire string we pin. The one end-to-end registration
test is gated on the engine being available.

Status: fails at import until ``bv.expr`` exists (PR 5 Step 8 exports it).

Patterns mirrored: ``test_builtin_cond.py``, ``test_event_dot_access.py``.
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._col import _Call

from ._helpers import _engine_available


# ── #1 if / elif / else → nested if_else ─────────────────────────────────────


def test_if_else_chain_lowers_to_if_else() -> None:
    """A plain ``if c: return a; else: return b`` collapses to one
    ``if_else(c, a, b)`` call."""

    @bv.expr
    def pick(c, a, b):
        if c:
            return a
        else:
            return b

    expr = pick(bv.col("c"), bv.col("a"), bv.col("b"))
    assert expr.to_expr_string() == "if_else(c, a, b)"


def test_elif_chain_nests_if_else() -> None:
    """``elif`` is an ``if`` inside the previous ``else``, so a three-way
    chain nests as ``if_else(c1, …, if_else(c2, …, …))``."""

    @bv.expr
    def grade(s):
        if s > 90:
            return 1
        elif s > 80:
            return 2
        else:
            return 3

    expr = grade(bv.col("s"))
    assert expr.to_expr_string() == "if_else((s > 90), 1, if_else((s > 80), 2, 3))"


# ── #2 ternary → if_else ─────────────────────────────────────────────────────


def test_ternary_lowers_to_if_else() -> None:
    """``a if c else b`` keeps both branches as data via ``if_else`` rather
    than asking the condition for a truth value (which would trip the
    footgun guard)."""

    @bv.expr
    def secure(url):
        return 1 if url.starts_with("https://") else 0

    expr = secure(bv.col("url"))
    assert expr.to_expr_string() == "if_else(starts_with(url, 'https://'), 1, 0)"


# ── #3 local variables → substitution ────────────────────────────────────────


def test_local_variables_are_substituted() -> None:
    """Each local name is copied in wherever it is read, so the server
    never needs a notion of variables."""

    @bv.expr
    def score(amount):
        log_amount = bv.log1p(amount)
        result = log_amount * 2
        return result

    expr = score(bv.col("amount"))
    assert expr.to_expr_string() == "(log1p(amount) * 2)"


# ── #4 is None / is not None → isnull ────────────────────────────────────────


def test_is_none_lowers_to_isnull() -> None:
    """``x is None`` becomes ``x.isnull()`` — left alone, ``is`` checks
    identity and would always be False on an expression."""

    @bv.expr
    def f(x):
        return x is None

    assert f(bv.col("x")).to_expr_string() == "(x == null)"


def test_is_not_none_lowers_to_not_isnull() -> None:
    """``x is not None`` becomes ``~(x.isnull())`` → wire ``(not (x == null))``."""

    @bv.expr
    def f(x):
        return x is not None

    assert f(bv.col("x")).to_expr_string() == "(not (x == null))"


def test_none_on_left_is_position_independent() -> None:
    """The ``None`` side is found by position, so ``None is x`` is the same
    as ``x is None``."""

    @bv.expr
    def f(x):
        return None is x

    assert f(bv.col("x")).to_expr_string() == "(x == null)"


# ── #5 and / or / not → & / | / ~ ────────────────────────────────────────────


def test_and_not_rewrites_to_binop_chain() -> None:
    """``and``/``or``/``not`` are swapped to ``&``/``|``/``~`` at the syntax
    level before the body runs; the wire output still reads ``and``/``not``."""

    @bv.expr
    def f(a, b):
        return a and not b

    assert f(bv.col("a"), bv.col("b")).to_expr_string() == "(a and (not b))"


def test_or_rewrites_to_binop() -> None:
    @bv.expr
    def f(a, b):
        return a or b

    assert f(bv.col("a"), bv.col("b")).to_expr_string() == "(a or b)"


# ── #6 chained comparisons → AND-chain (Python-only addition) ─────────────────


def test_two_operator_chain_pairs_adjacent() -> None:
    """``a < b < c`` is a single ``Compare`` node; we rewrite it to the
    AND of adjacent pairs ``(a < b) & (b < c)``."""

    @bv.expr
    def f(a, b, c):
        return a < b < c

    assert f(bv.col("a"), bv.col("b"), bv.col("c")).to_expr_string() == (
        "((a < b) and (b < c))"
    )


def test_three_operator_chain_pairs_adjacent() -> None:
    """``a < b < c < d`` → ``(a < b) & (b < c) & (c < d)``; middle operands
    appear twice, which is safe (nodes are pure)."""

    @bv.expr
    def f(a, b, c, d):
        return a < b < c < d

    expr = f(bv.col("a"), bv.col("b"), bv.col("c"), bv.col("d"))
    assert expr.to_expr_string() == "(((a < b) and (b < c)) and (c < d))"


# ── shape: unary minus on a literal folds to a negative literal ───────────────


def test_unary_minus_literal_folds() -> None:
    """``-5`` is stored by Python as "negate 5"; the literal-fold arm turns
    it into the literal ``-5`` (a minus on a non-literal is rejected — see
    ``test_expr_reject.py``)."""

    @bv.expr
    def f():
        return -5

    assert f().to_expr_string() == "-5"


# ── return edge cases that are ACCEPTED ──────────────────────────────────────


def test_return_none_lowers_to_null() -> None:
    """``return None`` carries a real value — the null literal — so it is
    accepted and lowers to ``null``. The engine then propagates null through
    any operation, the same as ``bv.lit(None)``."""

    @bv.expr
    def f(x):
        return None

    assert f(bv.col("x")).to_expr_string() == "null"


def test_dead_code_after_return_is_dropped() -> None:
    """Code after a return on the same path can never run, so it is silently
    dropped (same as Python), not rejected. The function still lowers to the
    returned value."""

    @bv.expr
    def f(x):
        return x + 1
        y = x * 999  # noqa: F841 — unreachable on purpose; must be ignored

    assert f(bv.col("x")).to_expr_string() == "(x + 1)"


# ── canonical RFC §8 fixture (no is_in) ──────────────────────────────────────


@bv.expr
def email_bucket(email):
    # #4 (is None) + return-in-branch fold + hashing + string.lower
    if email is None:
        return 0
    return bv.hash_mod(email.lower(), 1024)


@bv.expr
def is_external_secure(url):
    # #2 (ternary) + #5 (and / not) + string predicates
    return 1 if url.starts_with("https://") and not url.contains("internal.") else 0


@bv.expr
def risk_score(amount_usd, dwell_ms, country):
    # #1 (if/elif) + #2 (ternary) + #3 (local assigns) + #5 (or), is_in removed
    log_amount = bv.log1p(amount_usd)
    short_dwell = bv.clip(dwell_ms, 0, 1_000)
    c = country.lower()
    geo_bonus = 3.0 if c == "ru" or c == "kp" or c == "ir" else 0.0
    if log_amount > 6.0 and short_dwell < 200:
        geo_bonus = geo_bonus + 5.0
    elif log_amount > 4.0 or short_dwell < 500:
        geo_bonus += 2.0
    return geo_bonus


@bv.expr
def is_suspicious_referrer(referrer):
    # #3 (local assign) + #2 (ternary) + #5 (or) + method chaining
    clean = referrer.lower().replace("https://", "").replace("http://", "")
    return (
        1
        if clean.ends_with(".xyz") or clean.ends_with(".tk") or clean.contains("bit.ly")
        else 0
    )


def test_email_bucket_wire_string() -> None:
    """If the email is missing, the bucket is 0; otherwise it is a number
    derived from the lowercased email. The two separate returns become one
    pick-one-of-two expression. This checks we get that exact shape."""
    expr = email_bucket(bv.col("email"))
    assert expr.to_expr_string() == (
        "if_else((email == null), 0, hash_mod(lower(email), 1024))"
    )


def test_is_external_secure_wire_string() -> None:
    """Returns 1 when the link is secure (starts with https) and is not an
    internal link, otherwise 0. This checks the "and" / "not" condition and
    the one-or-the-other choice both come out right."""
    expr = is_external_secure(bv.col("referrer"))
    assert expr.to_expr_string() == (
        "if_else((starts_with(referrer, 'https://') and "
        "(not contains(referrer, 'internal.'))), 1, 0)"
    )


def test_risk_score_outer_node_is_if_else() -> None:
    """Builds a risk number through a few if/else branches. The result is a
    big nested choice, so rather than match the whole text we check it is a
    pick-one-of-two and that the country check is present with no ``is_in``."""
    expr = risk_score(bv.col("amount_usd"), bv.col("dwell_ms"), bv.col("country"))
    assert isinstance(expr, _Call)
    assert expr.name == "if_else"
    rendered = expr.to_expr_string()
    # The is_in substitution lowers to an `or`-chain over lower(country).
    assert "(lower(country) == 'ru')" in rendered
    assert "is_in" not in rendered


def test_is_suspicious_referrer_outer_node_is_if_else() -> None:
    """Cleans up the referrer text, then returns 1 if it looks shady, else 0.
    This checks the result is a pick-one-of-two and that the cleanup steps
    (lowercase, then strip the two prefixes) are chained in the right order."""
    expr = is_suspicious_referrer(bv.col("referrer"))
    assert isinstance(expr, _Call)
    assert expr.name == "if_else"
    rendered = expr.to_expr_string()
    assert "replace(replace(lower(referrer), 'https://', ''), 'http://', '')" in rendered


@bv.event
class Click:
    user_id: str
    email: str
    country: str
    referrer: str
    amount_usd: float
    dwell_ms: int
    ts: int


@pytest.mark.skipif(
    not _engine_available(),
    reason="end-to-end registration needs the beava binary",
)
def test_canonical_fixture_registers_end_to_end(app) -> None:
    """Wires all four helper functions into one feature table and checks the
    server accepts it. This proves the whole example works together, not just
    each piece on its own."""

    @bv.event
    def ClickFeatures(e: Click):
        e = e.with_columns(
            email_bkt=email_bucket(e.email),
            secure_ext=is_external_secure(e.referrer),
            risk=risk_score(e.amount_usd, e.dwell_ms, e.country),
            suspicious_ref=is_suspicious_referrer(e.referrer),
        )
        return e.group_by("user_id").agg(
            clicks_24h=bv.count(window="24h"),
            distinct_emails_24h=bv.n_unique("email_bkt", window="24h"),
            risky_clicks_1h=bv.count(where=bv.col("risk") >= 5.0, window="1h"),
            avg_amount_24h=bv.mean("amount_usd", window="24h"),
            suspicious_refs_24h=bv.sum("suspicious_ref", window="24h"),
            secure_clicks_24h=bv.sum("secure_ext", window="24h"),
        )

    # Round-trips: registration accepts the lowered expression strings.
    app.register(Click, ClickFeatures)
