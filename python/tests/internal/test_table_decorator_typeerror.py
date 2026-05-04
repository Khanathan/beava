"""Phase 13.5.1 Plan 01 — RED unit tests for @bv.table strict TypeError (D-01).

Per Phase 13.5.1 D-01 (USER-LOCKED, see CONTEXT.md):
    A ``@bv.table``-decorated function whose parameter has no annotation must
    raise ``TypeError`` with a message that names the function, names the
    parameter, and shows a corrected ``def Fn(p: Click): ...`` signature.

    Verbatim message template (em-dash U+2014 preserved)::

        TypeError: @bv.table function 'UserClicks' parameter 'clicks' must be
        annotated with the upstream event class — e.g.
        def UserClicks(clicks: Click): ...

This file is the RED commit for Plan 13.5.1-01. At HEAD, ``@bv.table``
silently falls back to ``inspect.Parameter.empty`` for missing annotations
and the failure surfaces *downstream* as ``AttributeError`` (e.g. ``type
object '_empty' has no attribute 'group_by'``) when the decorator body
invokes ``.group_by`` / ``.agg`` on the sentinel.

Plan 13.5.1-04 turns these tests GREEN by inserting an explicit
``if ann is inspect.Parameter.empty: raise TypeError(...)`` check inside
``python/beava/_table.py::_resolve_upstream_proxies``.

# RED-AT-COMMIT-TIME: pytest must exit NON-ZERO at HEAD because the current
# behavior is ``AttributeError`` from the sentinel-arg call, not the
# ``TypeError`` mandated by D-01. Failure mode captured in the matching
# Plan 13.5.1-01 commit message.

Plan-checker contract for Phase 13.5.1: this file uses NO ``MagicMock``
against the Transport surface (D-05 anti-pattern); the @bv.table decorator
is a pure Python compile-time helper with no transport dependency.
"""
from __future__ import annotations

import pytest

import beava as bv


# D-01 USER-LOCKED message stable-prefix (substring match resilient to
# whitespace tweaks; the verbatim em-dash continuation lives downstream).
_D01_PREFIX = (
    r"@bv\.table function '\w+' parameter '\w+' must be annotated"
)


@bv.event
class Click:
    """Minimal upstream event used as the (correct) annotation in tests."""

    user_id: str
    page: str


# ---------------------------------------------------------------------------
# RED tests (1-3): each MUST raise TypeError per D-01; current HEAD raises
# AttributeError, so these tests fail until Plan 13.5.1-04 lands the fix.
# ---------------------------------------------------------------------------


def test_keyed_form_unannotated_parameter_raises_typeerror() -> None:
    """``@bv.table(key="user_id")`` + unannotated parameter → strict TypeError.

    The single-key call shape is the most common in v0; it must be the
    canonical surface where D-01's strict TypeError is enforced.
    """
    with pytest.raises(TypeError, match=_D01_PREFIX) as excinfo:

        @bv.table(key="user_id")
        def UserClicks(clicks):  # noqa: ANN001 — intentionally unannotated
            return clicks.group_by("user_id").agg(
                c=bv.count(window="forever")
            )

    msg = str(excinfo.value)
    # Function name surfaced
    assert "UserClicks" in msg, msg
    # Parameter name surfaced
    assert "clicks" in msg, msg
    # Corrected-signature snippet hint surfaced (D-01 verbatim suffix)
    assert "must be annotated with the upstream event class" in msg, msg


def test_composite_key_form_unannotated_parameter_raises_typeerror() -> None:
    """``@bv.table(key=[...])`` (composite key) + unannotated → strict TypeError.

    ADR-003 composite-key call shape; same D-01 contract applies.
    """
    with pytest.raises(TypeError, match=_D01_PREFIX) as excinfo:

        @bv.table(key=["user_id", "page"])
        def UserPageClicks(clicks):  # noqa: ANN001 — intentionally unannotated
            return clicks.group_by("user_id", "page").agg(
                c=bv.count(window="forever")
            )

    msg = str(excinfo.value)
    assert "UserPageClicks" in msg, msg
    assert "clicks" in msg, msg
    assert "must be annotated with the upstream event class" in msg, msg


def test_bare_global_form_unannotated_parameter_raises_typeerror() -> None:
    """Bare ``@bv.table`` (no parens, ADR-003 global form) + unannotated → strict TypeError.

    Per ADR-003 Decision B, ``@bv.table`` with no kwargs is the *global*
    table form (``key_cols=[]``). D-01 contract still applies — missing
    parameter annotation is rejected at decoration time.
    """
    with pytest.raises(TypeError, match=_D01_PREFIX) as excinfo:

        @bv.table
        def TotalClicks(clicks):  # noqa: ANN001 — intentionally unannotated
            return clicks.agg(c=bv.count(window="forever"))

    msg = str(excinfo.value)
    assert "TotalClicks" in msg, msg
    assert "clicks" in msg, msg
    assert "must be annotated with the upstream event class" in msg, msg


# ---------------------------------------------------------------------------
# Test 4 (positive control): properly-annotated decoration still succeeds.
# This must stay GREEN at HEAD AND after Plan 13.5.1-04 — D-01 only adds a
# raise on the empty path; the happy path is untouched.
# ---------------------------------------------------------------------------


def test_positive_control_annotated_parameter_returns_table_descriptor() -> None:
    """``@bv.table(key="user_id")`` + ``def Fn(clicks: Click)`` → TableDescriptor.

    Sanity check that the strict-TypeError path doesn't regress the happy
    path. Mirrors the canonical fraud-team shape from
    ``python/tests/v0/test_core.py``.
    """

    @bv.table(key="user_id")
    def UserClicks(clicks: Click):
        return clicks.group_by("user_id").agg(
            c=bv.count(window="forever"),
        )

    # Decorator returns a TableDescriptor; opaque to user code, but we can
    # at least assert its type-name to detect a regression in the happy path.
    assert type(UserClicks).__name__ == "TableDescriptor"
