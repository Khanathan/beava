"""Tests for python/beava/_app.py — App client: context manager, register, validate, ping.

RED commit: all fail because _app.py does not exist and bv.App is still _AppStub.

Most tests that talk to a real server use the `beava_server` fixture from conftest.py.
"""

from __future__ import annotations

import json

import pytest

import beava as bv
from beava._errors import RegistrationError, ValidationError
from beava._events import EventDerivation, EventSource
from beava._schema import FieldSpec

# ---------------------------------------------------------------------------
# Shared descriptor factories (inline, no import from test_validate)
# ---------------------------------------------------------------------------


def _make_event(name: str, upstreams: list[str] | None = None) -> EventSource:
    # Plan 12.6-08: EventSource no longer accepts event_time_field /
    # tolerate_delay_ms per the no-event-time pivot.
    src = EventSource(
        name=name,
        schema={"x": FieldSpec(name="x", py_type=str, optional=False)},
        dedupe_key=None,
        dedupe_window_ms=None,
        keep_events_for_ms=None,
    )
    if upstreams is not None:
        src._upstreams = upstreams  # type: ignore[assignment]
    return src


def _make_derivation(name: str, upstreams: list[str]) -> EventDerivation:
    return EventDerivation(
        name=name,
        schema={"x": FieldSpec(name="x", py_type=str, optional=False)},
        upstreams=upstreams,
        ops=[],
        output_kind="event",
    )


# ---------------------------------------------------------------------------
# Transaction / UserProfile descriptors used across multiple tests
# ---------------------------------------------------------------------------


@bv.event
class Transaction:  # type: ignore[no-redef]
    amount: float
    user_id: str


@bv.table(key="user_id")
class UserProfile:  # type: ignore[no-redef]
    user_id: str
    balance: float


# ---------------------------------------------------------------------------
# Context manager tests (no register calls)
# ---------------------------------------------------------------------------


def test_app_context_manager_http(beava_server: tuple[str, str]) -> None:
    """bv.App(http_url) used as a context manager exits cleanly."""
    http_url, _tcp_url = beava_server
    with bv.App(http_url) as app:
        assert app is not None
    # no exception = pass


def test_app_context_manager_tcp(beava_server: tuple[str, str]) -> None:
    """bv.App(tcp_url) used as a context manager exits cleanly."""
    _http_url, tcp_url = beava_server
    with bv.App(tcp_url) as app:
        assert app is not None


# ---------------------------------------------------------------------------
# Embed mode requires context manager
# ---------------------------------------------------------------------------


def test_app_embed_mode_requires_context_manager() -> None:
    """bv.App() without URL raises RuntimeError if register() is called without entering."""
    app = bv.App()
    with pytest.raises(RuntimeError, match="context manager"):
        app.register(Transaction)


# ---------------------------------------------------------------------------
# Register success: HTTP + TCP
# ---------------------------------------------------------------------------


def test_app_register_http_success(beava_server: tuple[str, str]) -> None:
    """app.register() over HTTP returns registry_version and status."""
    http_url, _tcp_url = beava_server
    with bv.App(http_url) as app:
        resp = app.register(Transaction, UserProfile)
    assert isinstance(resp, dict)
    assert resp.get("registry_version") == 1
    assert resp.get("status") == "ok"


def test_app_register_tcp_success(beava_server: tuple[str, str]) -> None:
    """app.register() over TCP returns registry_version and status."""
    _http_url, tcp_url = beava_server
    with bv.App(tcp_url) as app:
        resp = app.register(Transaction, UserProfile)
    assert isinstance(resp, dict)
    assert resp.get("registry_version") == 1
    assert resp.get("status") == "ok"


# ---------------------------------------------------------------------------
# Additive re-post: registry_version increments correctly
# ---------------------------------------------------------------------------


def test_app_register_returns_registry_version_on_additive_repost(
    beava_server: tuple[str, str],
) -> None:
    """Version increments on each additive post; stays same on no-op re-post."""
    http_url, _tcp_url = beava_server
    with bv.App(http_url) as app:
        r1 = app.register(Transaction)
        assert r1["registry_version"] == 1

        r2 = app.register(Transaction, UserProfile)
        assert r2["registry_version"] == 2

        r3 = app.register(Transaction, UserProfile)
        assert r3["registry_version"] == 2


# ---------------------------------------------------------------------------
# Local validation failure → RegistrationError, no wire I/O
# ---------------------------------------------------------------------------


def test_app_register_raises_on_local_validation_failure(
    beava_server: tuple[str, str],
) -> None:
    """Cyclic descriptors raise RegistrationError before any network call."""
    http_url, _tcp_url = beava_server
    a = _make_derivation("CycleA", upstreams=["CycleB"])
    b = _make_derivation("CycleB", upstreams=["CycleA"])

    with bv.App(http_url) as app:
        # Pre-call registry version = 0
        import httpx

        r = httpx.get(f"{http_url}/registry")
        pre_version = r.json().get("registry_version", 0)

        with pytest.raises(RegistrationError) as exc_info:
            app.register(a, b)

        err = exc_info.value
        assert err.code == "cycle"
        assert isinstance(err.errors, list)
        assert len(err.errors) >= 1
        assert all(isinstance(e, ValidationError) for e in err.errors)

        # Registry must not have changed
        r2 = httpx.get(f"{http_url}/registry")
        assert r2.json().get("registry_version", 0) == pre_version


# ---------------------------------------------------------------------------
# Server-side rejection → RegistrationError
# ---------------------------------------------------------------------------


def test_app_register_raises_on_server_rejection(
    beava_server: tuple[str, str],
) -> None:
    """A descriptor with a reserved-prefix name passes local validation but server rejects it."""
    http_url, _tcp_url = beava_server

    # Use _beava_ prefix which the server rejects (NameReservedPrefix rule)
    bad = _make_event("_beava_reserved")

    with bv.App(http_url) as app:
        with pytest.raises(RegistrationError) as exc_info:
            app.register(bad)
        # Server returns a structured error; code should be non-empty
        assert exc_info.value.code


# ---------------------------------------------------------------------------
# validate() — zero network I/O
# ---------------------------------------------------------------------------


def test_app_validate_returns_list_without_network_io(
    beava_server: tuple[str, str],
) -> None:
    """validate() returns [] for valid descriptors and errors for cyclic ones; no wire I/O."""
    http_url, _tcp_url = beava_server

    import httpx

    app = bv.App(http_url)
    try:
        errs = app.validate(Transaction, UserProfile)
        assert errs == [], f"Expected no errors, got: {errs}"

        r_before = httpx.get(f"{http_url}/registry")
        pre_version = r_before.json().get("registry_version", 0)

        broken1 = _make_derivation("BrokenA", upstreams=["BrokenB"])
        broken2 = _make_derivation("BrokenB", upstreams=["BrokenA"])
        errs2 = app.validate(broken1, broken2)
        assert len(errs2) >= 1
        assert errs2[0].kind == "cycle"

        r_after = httpx.get(f"{http_url}/registry")
        assert r_after.json().get("registry_version", 0) == pre_version
    finally:
        app.close()


# ---------------------------------------------------------------------------
# close() is idempotent
# ---------------------------------------------------------------------------


def test_app_close_idempotent(beava_server: tuple[str, str]) -> None:
    """Calling close() multiple times raises no exception."""
    http_url, _tcp_url = beava_server
    app = bv.App(http_url)
    app.close()
    app.close()  # must not raise


# ---------------------------------------------------------------------------
# Topological order in compiled payload
# ---------------------------------------------------------------------------


def test_app_register_topological_order_in_payload(
    beava_server: tuple[str, str],
) -> None:
    """Upstream descriptors appear before dependents in the REGISTER nodes array."""
    http_url, _tcp_url = beava_server

    # Use manual construction to avoid @bv.event function-form annotation issues
    # (from __future__ import annotations stringifies all annotations in this file).
    txns = _make_event("TxnSource2")
    checkouts = _make_derivation("CheckoutsDeriv2", upstreams=["TxnSource2"])

    # Spy on the transport: capture what payload bytes would be sent.
    captured: list[dict] = []  # type: ignore[type-arg]

    from beava._transport import HttpTransport

    class SpyTransport(HttpTransport):
        def send_register(self, payload_json: bytes) -> dict:  # type: ignore[type-arg]
            captured.append(json.loads(payload_json.decode("utf-8")))
            return super().send_register(payload_json)

    app = bv.App(http_url)
    # Replace the transport with our spy
    app._transport = SpyTransport(http_url)  # type: ignore[attr-defined]
    try:
        # Pass derivation before its upstream intentionally — topo-sort must fix order
        app.register(checkouts, txns)
    finally:
        app.close()

    assert captured, "No payload was captured"
    nodes = captured[0]["nodes"]
    names = [n["name"] for n in nodes]
    assert names.index("TxnSource2") < names.index("CheckoutsDeriv2"), (
        f"Expected TxnSource2 before CheckoutsDeriv2, got: {names}"
    )


# ---------------------------------------------------------------------------
# Embed mode end-to-end
# ---------------------------------------------------------------------------


def test_app_embed_mode_end_to_end(beava_binary: object) -> None:
    """bv.App() spawns a subprocess, registers, tears it down on exit."""
    @bv.event
    class EmbedEvent:  # type: ignore[no-redef]
        amount: float

    with bv.App() as app:
        resp = app.register(EmbedEvent)
    assert isinstance(resp, dict)
    assert resp.get("registry_version") == 1


# ---------------------------------------------------------------------------
# app.ping()
# ---------------------------------------------------------------------------


def test_app_ping_tcp_succeeds(beava_server: tuple[str, str]) -> None:
    """app.ping() over TCP returns a dict with server_version and registry_version."""
    _http_url, tcp_url = beava_server
    with bv.App(tcp_url) as app:
        resp = app.ping()
    assert isinstance(resp, dict)
    assert "server_version" in resp
    assert isinstance(resp.get("registry_version"), int)
    assert resp["registry_version"] >= 0


def test_app_ping_http_raises_not_implemented(beava_server: tuple[str, str]) -> None:
    """app.ping() over HTTP raises NotImplementedError mentioning tcp://."""
    http_url, _tcp_url = beava_server
    with bv.App(http_url) as app:
        with pytest.raises(NotImplementedError, match="tcp"):
            app.ping()
