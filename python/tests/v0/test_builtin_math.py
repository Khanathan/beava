"""TDD red — math-builtin Python sugar (PR 3 Step 1).

What this file checks: ``bv.log1p(x)`` and ``bv.clip(x, lo, hi)`` produce
the right wire strings.

Why these are top-level helpers instead of methods: ``5.log1p()`` reads
weird and ``column.clip(0, 100)`` looks like it's mutating the column.
``bv.log1p(...)`` / ``bv.clip(...)`` reads naturally as math.

Status today: every test fails because ``bv.log1p`` / ``bv.clip`` don't
exist yet. Step 5 of PR 3 adds them.
"""
from __future__ import annotations

import beava as bv


# ── log1p ───────────────────────────────────────────────────────────────────


def test_log1p_with_column() -> None:
    """Most common case: take log1p of a column value."""
    expr = bv.log1p(bv.col("amount"))
    assert expr.to_expr_string() == "log1p(amount)"


def test_log1p_with_literal() -> None:
    """Plain Python numbers must also work — they get auto-wrapped as
    literals and render bare (no quotes, since they're numbers not
    strings)."""
    expr = bv.log1p(5)
    assert expr.to_expr_string() == "log1p(5)"


# ── clip ────────────────────────────────────────────────────────────────────


def test_clip_three_args() -> None:
    """Three-arg call — pins that args are joined with ``", "``
    (comma + space) in the wire output. Other separators would break
    the server parser."""
    expr = bv.clip(bv.col("dwell_ms"), 0, 100)
    assert expr.to_expr_string() == "clip(dwell_ms, 0, 100)"


def test_clip_all_literals() -> None:
    """All-literal args is legal — the SDK doesn't reject silly inputs
    like ``clip(50, 0, 100)`` (which is always just 50). Future
    optimization passes can fold this at register time, but the SDK's
    job is just to serialize."""
    expr = bv.clip(50, 0, 100)
    assert expr.to_expr_string() == "clip(50, 0, 100)"
