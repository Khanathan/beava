"""Tests for @bv.event decorator — class form and function form.

Plan 12.6-08 (no-event-time pivot, 2026-04-30): the @bv.event decorator no
longer accepts event_time field declarations on the class form, the
tolerate_delay parameter, or the event_time_field parameter. The strict-deny
contract is pinned by ``test_v0_no_event_time.py``.

Note: deliberately no ``from __future__ import annotations`` so that parameter
annotations in function-form tests are evaluated eagerly at def-time and
capture the decorated EventSource / TableSource objects from local scope.
"""

import pytest

import beava as bv

# ---------------------------------------------------------------------------
# Class form: basic
# ---------------------------------------------------------------------------


def test_event_class_form_basic() -> None:
    """@bv.event on a class produces an EventSource with correct JSON shape."""

    @bv.event
    class Transaction:
        amount: float
        user_id: str

    assert Transaction._name == "Transaction"
    assert Transaction._beava_kind == "event"

    j = Transaction._to_register_json()
    assert j["kind"] == "event"
    assert j["name"] == "Transaction"
    assert j["schema"]["fields"] == {"amount": "f64", "user_id": "str"}
    assert j["schema"]["optional_fields"] == []
    assert j["dedupe_key"] is None
    assert j["dedupe_window_ms"] is None
    assert j["keep_events_for_ms"] is None
    # Plan 12.6-08 D-03: event_time_field / tolerate_delay_ms keys deleted.
    assert "event_time_field" not in j
    assert "tolerate_delay_ms" not in j


def test_event_with_optional_field() -> None:
    """Optional fields appear in optional_fields list and still in fields dict."""

    @bv.event
    class X:
        a: str
        memo: bv.Optional[str]  # type: ignore[valid-type]

    j = X._to_register_json()
    assert "memo" in j["schema"]["optional_fields"]
    assert j["schema"]["fields"]["memo"] == "str"


# ---------------------------------------------------------------------------
# Duration options
# ---------------------------------------------------------------------------


def test_event_duration_options() -> None:
    """keep_events_for is converted to ms in JSON output."""

    @bv.event(keep_events_for="7d")
    class X:
        a: float

    j = X._to_register_json()
    assert j["keep_events_for_ms"] == 604_800_000


# ---------------------------------------------------------------------------
# Dedupe options
# ---------------------------------------------------------------------------


def test_event_dedupe_options() -> None:
    """dedupe_key + dedupe_window are stored and converted to ms."""

    @bv.event(dedupe_key="order_id", dedupe_window="24h")
    class X:
        order_id: str
        amount: float

    j = X._to_register_json()
    assert j["dedupe_key"] == "order_id"
    assert j["dedupe_window_ms"] == 86_400_000


def test_event_dedupe_key_must_be_in_schema() -> None:
    """dedupe_key not in schema raises TypeError at decoration time."""
    with pytest.raises(TypeError, match="missing_field"):

        @bv.event(dedupe_key="missing_field")
        class X:
            a: float


# ---------------------------------------------------------------------------
# Function form (derivation)
# ---------------------------------------------------------------------------


def test_event_function_form() -> None:
    """@bv.event on a function produces EventDerivation with upstreams."""

    @bv.event
    class TxSrc:
        amount: float
        user_id: str

    @bv.event
    def Checkouts(source: TxSrc) -> object:  # type: ignore[valid-type]
        return source

    assert Checkouts._name == "Checkouts"
    assert Checkouts._beava_kind == "derivation"
    assert Checkouts._upstreams == ["TxSrc"]

    j = Checkouts._to_register_json()
    assert j["kind"] == "derivation"
    assert j["upstreams"] == ["TxSrc"]
    assert j["output_kind"] == "event"


# ---------------------------------------------------------------------------
# Unsupported field type at decoration
# ---------------------------------------------------------------------------


def test_unsupported_field_type_errors_at_decoration() -> None:
    """Unsupported field types raise TypeError at decoration time."""
    with pytest.raises(TypeError, match="supported: str, int, float, bool, bytes, datetime"):

        @bv.event
        class X:
            a: list[int]  # type: ignore[valid-type]


# ---------------------------------------------------------------------------
# Reserved name prefix (server enforces, not client)
# ---------------------------------------------------------------------------


def test_reserved_name_prefix_decoration_succeeds() -> None:
    """Decoration of _beava_internal succeeds — server rejects on register."""

    @bv.event
    class _beava_internal:
        a: int

    j = _beava_internal._to_register_json()
    assert j["name"] == "_beava_internal"
