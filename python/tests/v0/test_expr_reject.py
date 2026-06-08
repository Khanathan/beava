"""TDD red — ``@bv.expr`` default-deny rejects (PR 5).

The lowering walk is an allowlist: one handler per supported Python shape
and a final catch-all that rejects everything else. This file pins one case
per error code plus the default-deny floor (an unhandled node type and the
shape holes ``%``/``Mod`` and unary ``-`` on a non-literal), each asserting
the error code AND that the message carries the offending source line.

All rejects fire at decoration time (the walk runs when ``@bv.expr`` is
applied), so each case wraps the ``def`` in ``pytest.raises``.

Pure-AST tests — no embed-mode binary.

Status: fails at import until ``bv.expr`` exists (PR 5 Step 8).
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._errors import RegistrationError

# ── expr_unsupported_python_op — explicit-reject arms ────────────────────────


def test_for_loop_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(xs):
            total = 0
            for x in xs:
                total = total + x
            return total

    assert exc.value.code == "expr_unsupported_python_op"
    assert "for x in xs" in str(exc.value)


def test_walrus_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            return (y := x) + 1  # noqa: F841 — walrus is the rejected construct

    assert exc.value.code == "expr_unsupported_python_op"


# ── expr_unsupported_python_op — shape holes (default-deny, no special arm) ───


def test_modulo_rejected_cleanly() -> None:
    """``%``/``Mod`` is outside ``+ - * /``; default-deny rejects it instead
    of leaking a raw ``TypeError`` (the old denylist hole)."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(a, b):
            return a % b

    assert exc.value.code == "expr_unsupported_python_op"
    assert "a % b" in str(exc.value)


def test_power_rejected_cleanly() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(a, b):
            return a ** b

    assert exc.value.code == "expr_unsupported_python_op"


def test_unary_minus_on_non_literal_rejected() -> None:
    """``-5`` folds to a literal (see ``test_expr_accept``); ``-x`` has no
    expression negation operator, so it rejects."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            return -x

    assert exc.value.code == "expr_unsupported_python_op"
    assert "-x" in str(exc.value)


def test_is_on_non_none_rejected() -> None:
    """``is`` is only accepted against ``None``; any other use rejects."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            return x is True

    assert exc.value.code == "expr_unsupported_python_op"


# ── expr_unsupported_python_op — the catch-all floor (unhandled node type) ────


def test_set_literal_hits_catch_all() -> None:
    """A set literal has no handler at all, so the final catch-all rejects it
    — the safety floor the old denylist lacked."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f():
            return {1, 2}

    assert exc.value.code == "expr_unsupported_python_op"


def test_list_comprehension_hits_catch_all() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(xs):
            return [x for x in xs]

    assert exc.value.code == "expr_unsupported_python_op"


# ── expr_bad_assign_target ───────────────────────────────────────────────────


def test_tuple_unpacking_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(a, b):
            x, y = a, b
            return x + y

    assert exc.value.code == "expr_bad_assign_target"


def test_attribute_target_assign_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c):
            c.x = 1
            return c

    assert exc.value.code == "expr_bad_assign_target"


def test_subscript_target_assign_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c):
            c[0] = 1
            return c

    assert exc.value.code == "expr_bad_assign_target"


# ── expr_unknown_name ────────────────────────────────────────────────────────


def test_name_used_before_assignment_rejected() -> None:
    """``y`` is read before it was ever given a value."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            return y + x  # noqa: F821 — `y` is undefined on purpose (the reject)

    assert exc.value.code == "expr_unknown_name"
    assert "y + x" in str(exc.value)


# ── expr_missing_return — a path that yields no value ────────────────────────


def test_bare_return_rejected() -> None:
    """``return`` with no expression yields nothing for the path to use. (This
    is distinct from ``return None``, which carries the null literal and is
    accepted — see ``test_expr_accept``.)"""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            return

    assert exc.value.code == "expr_missing_return"


def test_missing_return_on_branch_path_rejected() -> None:
    """The ``if`` returns, but the not-taken path falls off the end with no
    value of its own."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c, x):
            if c:
                return x

    assert exc.value.code == "expr_missing_return"


def test_no_return_at_all_rejected() -> None:
    """A body that never returns produces no value."""
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(x):
            y = x + 1  # noqa: F841 — no return follows

    assert exc.value.code == "expr_missing_return"


# ── returning a multi-value shape → catch-all (no tuple/list in expressions) ──


def test_return_tuple_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(a, b):
            return a, b

    assert exc.value.code == "expr_unsupported_python_op"


def test_return_list_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(a, b):
            return [a, b]

    assert exc.value.code == "expr_unsupported_python_op"


# ── expr_branch_local_binding (also covered in test_expr_branch_merge) ────────


def test_branch_local_binding_rejected() -> None:
    with pytest.raises(RegistrationError) as exc:

        @bv.expr
        def f(c, a):
            if c:
                z = a
            return z + 1

    assert exc.value.code == "expr_branch_local_binding"
