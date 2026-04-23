"""Phase 3 acceptance smoke tests — ROADMAP success criteria SC1..SC7 + embed mode.

Each test maps to one Phase 3 ROADMAP success criterion. Tests run against a real
Rust ``beava`` binary via the ``beava_server`` fixture (Plans 03-04).

Module-level descriptors are defined for reuse across criterion tests.
"""

import pytest

import beava as bv

# ---------------------------------------------------------------------------
# Module-level shared descriptors
# ---------------------------------------------------------------------------

# Two event sources
@bv.event
class TxEvent:
    """Minimal event with an event_time field."""

    user_id: str
    amount: float
    event_time: int


@bv.event
class LoginEvent:
    """Second event source for multi-event DAG tests."""

    user_id: str
    session_id: str
    event_time: int


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
# Helper: two-server descriptors for SC6 (spawned inside the test body)
# SC6 uses beava_binary directly and spawns its own two subprocesses.
# ---------------------------------------------------------------------------

# Simple standalone event used for the SC6 "same 2-event + 1-table DAG" test.
# Defined here at module scope so it can be decorated cleanly before any server
# is running (decoration is pure Python — no network I/O).
@bv.event
class SC6EventA:
    user_id: str
    amount: float
    event_time: int


@bv.event
class SC6EventB:
    user_id: str
    session_id: str
    event_time: int


@bv.table(key="user_id")
class SC6Table:
    user_id: str
    balance: float


# ---------------------------------------------------------------------------
# SC1 — @bv.event both forms
# ---------------------------------------------------------------------------


def test_c1_event_decorator_both_forms() -> None:
    """ROADMAP Phase 3 success criterion #1 (red stub).

    @bv.event class form extracts schema and registers event descriptor;
    function form resolves upstreams.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC2 — @bv.table both forms
# ---------------------------------------------------------------------------


def test_c2_table_decorator_both_forms() -> None:
    """ROADMAP Phase 3 success criterion #2 (red stub).

    @bv.table(key=..., ttl=...) class + function forms; key validation at decoration.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC3 — bv.col canonical form
# ---------------------------------------------------------------------------


def test_c3_col_canonical_form() -> None:
    """ROADMAP Phase 3 success criterion #3 (red stub).

    bv.col("x") > 100 expression produces expected to_expr_string() canonical form.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC4 — app.register both transports
# ---------------------------------------------------------------------------


def test_c4_register_both_transports(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #4 (red stub).

    app.register(*descriptors) dispatches HTTP/TCP per URL scheme, returns registry_version.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC5 — app.validate zero network IO
# ---------------------------------------------------------------------------


def test_c5_validate_no_io(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #5 (red stub).

    app.validate(*descriptors) zero-network-IO returns list[ValidationError].
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC6 — identical registry state across transports (star test)
# ---------------------------------------------------------------------------


def test_c6_identical_registry_state_across_transports(
    beava_binary: object,
) -> None:
    """ROADMAP Phase 3 success criterion #6 (red stub).

    Register 2 events + 1 table once via http:// and once via tcp://;
    GET /registry shows identical state both times.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# SC7 — TCP ping + connection reuse
# ---------------------------------------------------------------------------


def test_c7_tcp_ping_and_connection_reuse(beava_server: tuple[str, str]) -> None:
    """ROADMAP Phase 3 success criterion #7 (red stub).

    TCP ping round-trip succeeds; connection reuse across register/validate calls.
    """
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")


# ---------------------------------------------------------------------------
# Extra — embed mode end-to-end
# ---------------------------------------------------------------------------


def test_extra_embed_mode_end_to_end(beava_binary: object) -> None:
    """Embed mode: bv.App() auto-spawns subprocess; subprocess reaped on exit (red stub)."""
    pytest.fail("red stub — implement in Plan 03-06 Task 1.b")
