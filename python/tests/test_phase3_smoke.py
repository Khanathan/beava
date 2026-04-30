"""Phase 3 acceptance smoke tests — ROADMAP success criteria SC1..SC7 + embed mode.

Each test maps to one Phase 3 ROADMAP success criterion. Tests run against a real
Rust ``beava`` binary via the ``beava_server`` fixture (Plans 03-04).

Module-level descriptors are defined for reuse across criterion tests.
"""

import httpx
import pytest

import beava as bv
from beava._events import EventDerivation, EventSource
from beava._tables import TableDerivation, TableSource

# ---------------------------------------------------------------------------
# Module-level shared descriptors
# ---------------------------------------------------------------------------

# Two event sources.
# Plan 12.6-08 (no-event-time pivot): event_time fields removed from fixtures.
# The server stamps wall-clock arrival time on every push automatically.
@bv.event
class TxEvent:
    """Minimal event source."""

    user_id: str
    amount: float


@bv.event
class LoginEvent:
    """Second event source for multi-event DAG tests."""

    user_id: str
    session_id: str


# Table source keyed on user_id
@bv.table(key="user_id")
class UserProfileTable:
    """Per-user lookup table."""

    user_id: str
    balance: float


# Function-form event derivation (passthrough — no op chain in Phase 3)
@bv.event
def CheckoutDerivation(src: TxEvent):  # type: ignore[no-untyped-def]
    """Minimal derivation; Phase 4 adds .filter etc."""
    return src


# ---------------------------------------------------------------------------
# SC6 descriptors — two independent events + one table for the star test.
# Defined at module scope so decoration happens before any server starts.
# ---------------------------------------------------------------------------

@bv.event
class SC6EventA:
    user_id: str
    amount: float


@bv.event
class SC6EventB:
    user_id: str
    session_id: str


@bv.table(key="user_id")
class SC6Table:
    user_id: str
    balance: float


# ---------------------------------------------------------------------------
# SC1 — @bv.event both forms
# ---------------------------------------------------------------------------


def test_c1_event_decorator_both_forms() -> None:
    """ROADMAP Phase 3 success criterion #1.

    @bv.event class form extracts schema and registers event descriptor;
    function form resolves upstreams.
    """
    # Class form: produces EventSource with correct schema
    assert isinstance(TxEvent, EventSource), (
        f"Expected EventSource, got {type(TxEvent)}"
    )
    assert TxEvent._name == "TxEvent"
    assert "user_id" in TxEvent._schema
    assert "amount" in TxEvent._schema
    assert TxEvent._beava_kind == "event"
    assert TxEvent._upstreams == []

    # Class form: schema field py_types are correct
    assert TxEvent._schema["user_id"].py_type is str
    assert TxEvent._schema["amount"].py_type is float

    # Plan 12.6-08: EventSource no longer carries _event_time_field per the
    # no-event-time pivot.
    assert not hasattr(TxEvent, "_event_time_field")

    # Second class form: LoginEvent
    assert isinstance(LoginEvent, EventSource)
    assert LoginEvent._name == "LoginEvent"
    assert "session_id" in LoginEvent._schema

    # Function form: produces EventDerivation with upstream resolved
    assert isinstance(CheckoutDerivation, EventDerivation), (
        f"Expected EventDerivation, got {type(CheckoutDerivation)}"
    )
    assert CheckoutDerivation._name == "CheckoutDerivation"
    assert CheckoutDerivation._beava_kind == "derivation"
    assert "TxEvent" in CheckoutDerivation._upstreams

    # to_register_json shapes are correct
    tx_json = TxEvent._to_register_json()
    assert tx_json["kind"] == "event"
    assert tx_json["name"] == "TxEvent"
    assert "fields" in tx_json["schema"]

    co_json = CheckoutDerivation._to_register_json()
    assert co_json["kind"] == "derivation"
    assert co_json["upstreams"] == ["TxEvent"]


# ---------------------------------------------------------------------------
# SC2 — @bv.table both forms
# ---------------------------------------------------------------------------


def test_c2_table_decorator_both_forms() -> None:
    """ROADMAP Phase 3 success criterion #2.

    @bv.table(key=..., ttl=...) class + function forms; key validation at decoration.
    """
    # Class form: produces TableSource with primary_key
    assert isinstance(UserProfileTable, TableSource), (
        f"Expected TableSource, got {type(UserProfileTable)}"
    )
    assert UserProfileTable._name == "UserProfileTable"
    assert UserProfileTable._primary_key == ["user_id"]
    assert UserProfileTable._beava_kind == "table"

    # Class form with TTL
    @bv.table(key="user_id", ttl="7d")
    class TemporaryProfile:
        user_id: str
        score: float

    assert isinstance(TemporaryProfile, TableSource)
    assert TemporaryProfile._ttl_ms is not None
    assert TemporaryProfile._ttl_ms == 7 * 24 * 60 * 60 * 1000

    # Function form: produces TableDerivation
    @bv.table(key="user_id")
    def UserScoreTable(src: TxEvent):  # type: ignore[no-untyped-def]
        return src

    assert isinstance(UserScoreTable, TableDerivation), (
        f"Expected TableDerivation, got {type(UserScoreTable)}"
    )
    assert UserScoreTable._name == "UserScoreTable"
    assert "TxEvent" in UserScoreTable._upstreams
    assert UserScoreTable._table_primary_key == ["user_id"]

    # Key validation: missing key field raises TypeError at decoration time
    with pytest.raises(TypeError, match="user_id"):
        @bv.table(key="user_id")
        class BadTable:
            amount: float  # user_id not declared

    # Bare @bv.table without key= raises TypeError
    with pytest.raises(TypeError, match="key="):
        @bv.table  # type: ignore[arg-type]
        class NoKeyTable:
            user_id: str


# ---------------------------------------------------------------------------
# SC3 — bv.col canonical form
# ---------------------------------------------------------------------------


def test_c3_col_canonical_form() -> None:
    """ROADMAP Phase 3 success criterion #3.

    bv.col("x") > 100 expression produces expected to_expr_string() canonical form.
    """
    # Simple comparison
    expr = bv.col("amount") > 100
    assert expr.to_expr_string() == "(amount > 100)"

    # Compound boolean: (a > 0) and (b < 5)
    compound = (bv.col("a") > 0) & (bv.col("b") < 5)
    assert compound.to_expr_string() == "((a > 0) and (b < 5))"

    # Arithmetic inside comparison
    arith = bv.col("price") + bv.col("tax")
    assert arith.to_expr_string() == "(price + tax)"

    # Chained: (amount * 1.1) > 500
    scaled = (bv.col("amount") * 1.1) > 500
    assert scaled.to_expr_string() == "((amount * 1.1) > 500)"

    # NOT / or
    not_expr = ~(bv.col("is_flagged"))
    assert not_expr.to_expr_string() == "(not is_flagged)"

    # isinstance check for bv.Col
    assert isinstance(bv.col("x"), bv.Col)


# ---------------------------------------------------------------------------
# SC4 — app.register both transports
# ---------------------------------------------------------------------------


def test_c4_register_both_transports(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #4.

    app.register(*descriptors) dispatches HTTP/TCP per URL scheme, returns registry_version.
    """
    http_url, tcp_url = beava_server

    # HTTP transport: register TxEvent + UserProfileTable → version 1
    with bv.App(http_url) as app:
        resp = app.register(TxEvent, UserProfileTable)

    assert resp.get("status") == "ok", f"HTTP register returned: {resp}"
    assert resp.get("registry_version") == 1, (
        f"Expected registry_version=1, got: {resp}"
    )

    # TCP transport: same server already has TxEvent/UserProfileTable at version 1.
    # Register an additional descriptor to bump to version 2.
    with bv.App(tcp_url) as app:
        resp2 = app.register(LoginEvent)

    assert resp2.get("status") == "ok", f"TCP register returned: {resp2}"
    assert resp2.get("registry_version") == 2, (
        f"Expected registry_version=2, got: {resp2}"
    )


# ---------------------------------------------------------------------------
# SC5 — app.validate zero network IO
# ---------------------------------------------------------------------------


def test_c5_validate_no_io(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #5.

    app.validate(*descriptors) zero-network-IO returns list[ValidationError].
    """
    http_url, _tcp_url = beava_server

    # Snapshot registry_version before validate
    version_before = httpx.get(f"{http_url}/registry").json()["version"]

    # Build a cyclic DAG to trigger validation errors (no upstream in batch)
    # CheckoutDerivation depends on TxEvent; if we only pass CheckoutDerivation
    # without TxEvent, validate should return a missing_upstream error.
    errs = bv.App(http_url).validate(CheckoutDerivation)
    assert isinstance(errs, list)
    assert len(errs) > 0, "Expected at least one ValidationError for missing upstream"
    first = errs[0]
    assert isinstance(first, bv.ValidationError)
    assert first.kind == "missing_upstream"

    # Snapshot registry_version after validate — must be unchanged (zero network I/O)
    version_after = httpx.get(f"{http_url}/registry").json()["version"]
    assert version_before == version_after, (
        f"validate() changed registry_version from {version_before} to {version_after}; "
        "validate must be zero-network-IO"
    )

    # Valid batch returns empty list
    valid_errs = bv.App(http_url).validate(TxEvent, UserProfileTable)
    assert valid_errs == [], f"Expected no errors for valid batch, got: {valid_errs}"


# ---------------------------------------------------------------------------
# SC6 — identical registry state across transports (star test)
# ---------------------------------------------------------------------------


def test_c6_identical_registry_state_across_transports(
    beava_binary: object,
) -> None:
    """ROADMAP Phase 3 success criterion #6.

    Register 2 events + 1 table once via http:// and once via tcp://;
    GET /registry shows identical state both times.
    """
    from beava._embed import spawn_embedded_server, teardown_process

    descriptors = [SC6EventA, SC6EventB, SC6Table]

    # Round 1: register via http://
    proc_a, http_url_a, _tcp_url_a = spawn_embedded_server()
    try:
        with bv.App(http_url_a) as app:
            app.register(*descriptors)
        state_a = httpx.get(f"{http_url_a}/registry").json()
    finally:
        teardown_process(proc_a)

    # Round 2: register via tcp://
    proc_b, http_url_b, tcp_url_b = spawn_embedded_server()
    try:
        with bv.App(tcp_url_b) as app:
            app.register(*descriptors)
        # GET /registry is HTTP-only; query the second server's HTTP port
        state_b = httpx.get(f"{http_url_b}/registry").json()
    finally:
        teardown_process(proc_b)

    # Strip the unstable _dev_only sentinel before comparison
    for s in (state_a, state_b):
        s.pop("_dev_only", None)

    assert state_a == state_b, (
        f"Registry state diverges between HTTP and TCP registration:\n"
        f"HTTP: {state_a}\n"
        f"TCP:  {state_b}"
    )


# ---------------------------------------------------------------------------
# SC7 — TCP ping + connection reuse
# ---------------------------------------------------------------------------


def test_c7_tcp_ping_and_connection_reuse(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #7.

    TCP ping round-trip succeeds; connection reuse across register/validate calls.
    """
    _http_url, tcp_url = beava_server

    with bv.App(tcp_url) as app:
        from beava._transport import TcpTransport

        # Ping #1
        p1 = app.ping()
        assert "server_version" in p1, f"ping response missing server_version: {p1}"
        assert "registry_version" in p1, f"ping response missing registry_version: {p1}"
        assert isinstance(p1["registry_version"], int)

        # Capture socket identity for connection-reuse check
        assert isinstance(app._transport, TcpTransport)
        # Force connection open
        _ = app._transport._ensure_connected()
        sock_id_1 = id(app._transport._socket)

        # Ping #2 — same socket
        p2 = app.ping()
        assert p2["server_version"] == p1["server_version"], (
            "server_version changed between pings"
        )
        sock_id_2 = id(app._transport._socket)
        assert sock_id_1 == sock_id_2, "TCP socket was replaced between ping calls"

        # Register something — still same socket
        app.register(TxEvent)
        sock_id_3 = id(app._transport._socket)
        assert sock_id_2 == sock_id_3, "TCP socket was replaced after register call"

        # Ping #3 after register
        p3 = app.ping()
        assert p3["registry_version"] >= 1, (
            f"Expected registry_version >= 1 after register, got: {p3}"
        )
        sock_id_4 = id(app._transport._socket)
        assert sock_id_3 == sock_id_4, "TCP socket was replaced between register and ping"


# ---------------------------------------------------------------------------
# Extra — embed mode end-to-end
# ---------------------------------------------------------------------------


def test_extra_embed_mode_end_to_end(beava_binary: object) -> None:
    """Embed mode: bv.App() auto-spawns subprocess; registers; subprocess reaped on exit."""
    with bv.App() as app:
        resp = app.register(TxEvent)

    assert resp.get("registry_version") == 1, (
        f"Expected registry_version=1 from embed mode, got: {resp}"
    )
    assert resp.get("status") == "ok", f"Expected status=ok from embed mode, got: {resp}"
