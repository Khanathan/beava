"""TDD red — confirm the PR 2 footgun pointer is honest (PR 3 Step 1).

Background: in PR 2 we added a safety check — calling ``len(expr)`` on
a beava expression raises a clear error telling the user "use
bv.length(x) instead". But ``bv.length`` didn't exist yet in PR 2.
So anyone following the hint hit a second confusing error:
"AttributeError: module beava has no attribute length".

PR 3 adds ``bv.length`` so the error message is now *honest* — the
helper it points at actually exists.

This file pins two things:
  1. The PR 2 error message still mentions ``bv.length`` (so nobody
     accidentally reworded it).
  2. ``bv.length(x)`` actually works and produces ``length(x)`` on
     the wire.

Status today: check #1 already passes (PR 2 work). Check #2 fails until
Step 5 of PR 3 adds ``bv.length`` to the public surface.
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._col import _Call


def test_len_message_still_names_bv_length() -> None:
    """Regression check: if someone reworded the PR 2 footgun error
    message and dropped the ``bv.length`` hint, this test catches it.
    Without the hint the error is confusing — users get told "don't do
    that" but not what to do instead."""
    with pytest.raises(TypeError, match=r"bv\.length"):
        len(bv.col("x"))


def test_bv_length_produces_call_node() -> None:
    """The honest-pointer promise: when the footgun message says "use
    bv.length(x)", that has to actually work. Build it, confirm it's
    a real function-call AST node (not a stub raising NotImplementedError
    or similar), and confirm it serializes to ``length(x)``."""
    expr = bv.length(bv.col("x"))
    assert isinstance(expr, _Call)
    assert expr.to_expr_string() == "length(x)"
