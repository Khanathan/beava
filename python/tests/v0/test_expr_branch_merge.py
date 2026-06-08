"""TDD red — ``@bv.expr`` local-variable + branch merging (PR 5, RFC §5.7 #3).

One case per row of the convergence table, plus the aug-assign desugar and
the non-converging reject. The merge is the fiddly part: a variable may be
set in one branch but not another, so we test each shape the table calls out.

Pure-AST tests — no embed-mode binary; assert wire output with
``.to_expr_string()`` and the reject with ``RegistrationError``.

Status: fails at import until ``bv.expr`` exists (PR 5 Step 8).
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._errors import RegistrationError

# ── row: set in EVERY branch → plain merge into one if_else ───────────────────


def test_all_branches_assign_simple_merge() -> None:
    @bv.expr
    def f(c, a, b):
        if c:
            y = a
        else:
            y = b
        return y + 1

    # the value of y merges both branches into a single if else
    assert f(bv.col("c"), bv.col("a"), bv.col("b")).to_expr_string() == (
        "(if_else(c, a, b) + 1)"
    )


# ── row: set in SOME branches + had a prior value → other arm carries old ─────


def test_outer_binding_asymmetric_branch_carries_old_value() -> None:
    @bv.expr
    def f(c, a):
        y = 0
        if c:
            y = a
        return y + 1

    # The missing else carries the prior value (0) into the merge.
    assert f(bv.col("c"), bv.col("a")).to_expr_string() == "(if_else(c, a, 0) + 1)"


# ── row: a branch ends in return → continuation folds into the OTHER arm ──────


def test_early_return_folds_continuation_into_not_taken_arm() -> None:
    @bv.expr
    def f(c, early, x):
        if c:
            return early
        y = x * 2
        return y + 1

# The else branch folds the continuation into the merge,
# so the value of y is the whole expression after the if_else, not just x * 2.
    assert f(bv.col("c"), bv.col("early"), bv.col("x")).to_expr_string() == (
        "if_else(c, early, ((x * 2) + 1))"
    )


# ── sequential reassignment (substitution chains) ────────────────────────────


def test_sequential_reassignment() -> None:
    @bv.expr
    def f(x):
        y = x + 1
        y = y * 2
        return y

    assert f(bv.col("x")).to_expr_string() == "((x + 1) * 2)"


# ── aug-assign desugar yields the same tree as the explicit form ──────────────


def test_aug_assign_matches_explicit_form() -> None:
    """``x += 1`` desugars to ``x = x + 1`` in a pre-pass, so the lowered tree
    is identical to the hand-written form. (``%=`` desugars too but ``%`` is
    outside the v0 vocabulary — it lands in the reject suite.)"""

    @bv.expr
    def add_aug(x):
        y = x
        y += 1
        return y

    @bv.expr
    def add_explicit(x):
        y = x
        y = y + 1
        return y

    @bv.expr
    def sub_aug(x):
        y = x
        y -= 1
        return y

    @bv.expr
    def sub_explicit(x):
        y = x
        y = y - 1
        return y

    @bv.expr
    def mul_aug(x):
        y = x
        y *= 2
        return y

    @bv.expr
    def mul_explicit(x):
        y = x
        y = y * 2
        return y

    @bv.expr
    def div_aug(x):
        y = x
        y /= 2
        return y

    @bv.expr
    def div_explicit(x):
        y = x
        y = y / 2
        return y

    col = bv.col("x")
    assert add_aug(col).to_expr_string() == add_explicit(col).to_expr_string()
    assert sub_aug(col).to_expr_string() == sub_explicit(col).to_expr_string()
    assert mul_aug(col).to_expr_string() == mul_explicit(col).to_expr_string()
    assert div_aug(col).to_expr_string() == div_explicit(col).to_expr_string()


# ── row: set in SOME branches with NO prior value → reject ────────────────────


def test_non_converging_branch_binding_rejects() -> None:
    """On the else path ``y`` was never set, so there is a path where the
    name has no value — reject at decoration time, never guess."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c, a):
            if c:
                y = a
            return y + 1
    # Python allows this but will break at runtime if c is false
    # we reject it at register-time
    assert exc.value.code == "expr_branch_local_binding"


# ── leak guard: a local set only in a branch that RETURNS must not leak ───────


def test_returned_branch_local_does_not_leak_to_continuation() -> None:
    """``y`` is set inside the branch that returns, so the code after the
    ``if`` runs only on the path where ``y`` was never set. The continuation is
    processed against the not-taken branch's env, so reading ``y`` rejects —
    proving each branch keeps its own env copy rather than sharing one."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c, a, x):
            if c:
                y = a
                return y
            z = y + 1  # y was only set on the path that already returned
            return z

    assert exc.value.code == "expr_unknown_name"
