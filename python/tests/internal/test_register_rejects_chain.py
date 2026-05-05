"""Phase 13.5.2 D-01 — RED contract test.

`app.register()` must reject `EventDerivation` instances (raw chain
expressions) at the top of the call with `RegistrationError(code='invalid_descriptor',
path='descriptors[N]', message=...)` whose message body contains the
multi-line `@bv.event def Foo(click: Click): ...` rewrite hint.

The rejection fires BEFORE `_require_transport`, so these tests do not
need an `app` fixture or a running embed engine — `bv.App(test_mode=True)`
is constructed inline; transport stays uninitialized.

These tests MUST FAIL at HEAD before Plan 13.5.2-03 (GREEN) lands.
"""
from __future__ import annotations

import pytest

import beava as bv
from beava._errors import RegistrationError
from beava._events import EventDerivation


def test_register_rejects_event_derivation_instance() -> None:
    """A bare chain registered as a top-level descriptor → RegistrationError."""

    @bv.event
    class Click:
        user_id: str
        page: str

    Tagged = Click.with_columns(source=bv.lit("web")).named("Tagged")
    assert isinstance(Tagged, EventDerivation), (
        "test precondition: `.named()` must still return an EventDerivation"
    )

    app = bv.App(test_mode=True)
    with pytest.raises(RegistrationError) as ei:
        app.register(Click, Tagged)

    err = ei.value
    assert err.code == "invalid_descriptor", f"got code={err.code!r}"
    assert err.path == "descriptors[1]", f"got path={err.path!r}"
    assert "Wrap the chain in @bv.event" in err.message, (
        f"hint missing from message: {err.message!r}"
    )
    assert "@bv.event" in err.message and "def " in err.message, (
        "message must show the canonical @bv.event def Foo(...) rewrite"
    )


def test_register_rejects_chain_at_index_zero() -> None:
    """The path index reflects the actual position of the offending descriptor."""

    @bv.event
    class Click:
        user_id: str

    Tagged = Click.with_columns(source=bv.lit("web")).named("Tagged")

    app = bv.App(test_mode=True)
    with pytest.raises(RegistrationError) as ei:
        app.register(Tagged, Click)

    assert ei.value.path == "descriptors[0]", f"got path={ei.value.path!r}"


def test_register_rejects_unnamed_chain() -> None:
    """A chain WITHOUT `.named(...)` (auto-named __derived_N) is also rejected."""

    @bv.event
    class Click:
        user_id: str

    AutoNamed = Click.with_columns(source=bv.lit("web"))  # no .named()
    assert isinstance(AutoNamed, EventDerivation), (
        "test precondition: with_columns alone returns EventDerivation"
    )

    app = bv.App(test_mode=True)
    with pytest.raises(RegistrationError) as ei:
        app.register(Click, AutoNamed)

    assert ei.value.code == "invalid_descriptor"
    assert "Wrap the chain in @bv.event" in ei.value.message


def test_register_accepts_event_class_and_table_function_positive_control() -> None:
    """Positive control: the happy path (class + @bv.table function) still works.

    Guards against accidentally over-rejecting in the GREEN impl.

    NOTE: this test exercises a real `register()` call which DOES require the
    embed engine — uses the v0 `app` fixture pattern inline.
    """

    @bv.event
    class Click:
        user_id: str
        page: str

    @bv.table(key="user_id")
    def UserClicks(click: Click):
        return click.group_by("user_id").agg(c=bv.count(window="forever"))

    # Real engine via context-manager — embed-mode spawn for the happy path.
    with bv.App(test_mode=True) as a:
        result = a.register(Click, UserClicks)
        assert isinstance(result, dict), "register must return server response dict"
