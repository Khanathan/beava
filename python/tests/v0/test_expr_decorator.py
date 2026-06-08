"""``@bv.expr`` decoration-target contract (PR 5).

``@bv.expr`` rewrites a function's source, so the target must be a plain
function it can read. The wrong target fails fast with a ``TypeError`` —
matching the ``@bv.event`` / ``@bv.table`` convention, and distinct from the
``RegistrationError`` *body* rejects in ``test_expr_reject.py``.

The expected usage is the decorator form ``@bv.expr``. Through that syntax the
only reachable wrong target is a **class** (you cannot write ``@bv.expr`` above
an int, a builtin, or a lambda) — so that is the primary case here, written
with real ``@bv.expr`` syntax. The REPL/``exec`` no-source case is also a real
``@bv.expr`` scenario (a function defined where its source can't be read).

``bv.expr`` is an exported callable, so a handful of direct-call cases guard
misuse of that form too; they are clearly secondary.

Pure-Python — no embed-mode binary.
"""
from __future__ import annotations

import pytest

import beava as bv

# ── via @bv.expr decorator syntax (the realistic cases) ──────────────────────


def test_class_via_decorator_syntax_rejected() -> None:
    """The one wrong target reachable through ``@bv.expr`` syntax: a class."""
    with pytest.raises(TypeError, match="not a class"):

        @bv.expr
        class Features:
            amount: float


def test_sourceless_function_rejected() -> None:
    """Stands in for applying ``@bv.expr`` in a REPL / notebook: the function
    exists but its source can't be read, so it must fail with a clear message,
    not a raw ``OSError``. (Built with ``exec`` here for reproducibility.)"""
    namespace: dict[str, object] = {}
    exec("def g(x):\n    return x + 1", namespace)  # exec gives g no source file
    with pytest.raises(TypeError, match="source file"):
        bv.expr(namespace["g"])


# ── tests against unintended usage by calling bv.expr(...) directly ────────────
#   (defensive — it is an exported callable)


def test_non_function_rejected() -> None:
    with pytest.raises(TypeError, match=r"@bv\.expr"):
        bv.expr(5)


def test_builtin_rejected() -> None:
    with pytest.raises(TypeError, match=r"@bv\.expr"):
        bv.expr(len)


def test_lambda_rejected() -> None:
    with pytest.raises(TypeError, match="lambda"):
        bv.expr(lambda x: x + 1)


def test_callable_object_rejected() -> None:
    class Callable:
        def __call__(self, x: object) -> object:
            return x

    with pytest.raises(TypeError, match=r"@bv\.expr"):
        bv.expr(Callable())
