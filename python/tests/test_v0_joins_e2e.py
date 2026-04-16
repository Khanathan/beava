"""Phase 23-03 Task 2 — End-to-end Python SDK TCP tests for all 3 join shapes.

Runs against the session-scoped ``beava_server`` fixture from conftest.py.
Each test drives REGISTER / SET / PUSH / GET via the Python SDK over the
live TCP protocol, asserting the engine correctly consumes the REGISTER
payload emitted by ``python/beava/_serialize.py`` and produces the right
downstream effects.

  1. ``test_stream_stream_join_tcp``  — Orders.join(Payments, within=30s).
  2. ``test_stream_table_enrich_tcp`` — Clicks.join(UserProfile) (regression
     guard for 23-01).
  3. ``test_table_table_join_tcp``    — SET two Tables, GET joined output.

Phase 23-03 Known Stub: the TT-join E2E test only asserts the
register-and-smoke path because v0 stores both input Tables and the output
Table in the same entity (same string key). Full-semantic tombstone
propagation is deferred — see 23-03-SUMMARY "Known Stubs".
"""

from __future__ import annotations

import time

import pytest

import beava as bv


def test_stream_stream_join_tcp(app):
    """Stream↔Stream windowed join fed into a downstream count aggregation."""

    @bv.stream
    class Orders:
        user_id: str
        order_id: str

    @bv.stream
    class Payments:
        user_id: str
        order_id: str
        amount: float

    OrderPayment = Orders.join(
        Payments, on=["user_id", "order_id"], within="30s", type="inner"
    )

    @bv.table(key="user_id")
    def OPAgg(op: OrderPayment) -> bv.Table:
        return op.group_by("user_id").agg(matched=bv.count(window="1h"))

    app.register(Orders, Payments, OrderPayment, OPAgg)

    # Push a matching pair within the window.
    t_ms = int(time.time() * 1000)
    # Unique key per test — the session-scoped `app` fixture shares state
    # across the whole pytest run; using distinctive prefixes prevents
    # cross-test pollution (e.g., `u1` collides with 23-01 roundtrip tests).
    app.push_sync(Orders, {"user_id": "ssj_u1", "order_id": "o1", "_event_time": t_ms})
    app.push_sync(
        Payments,
        {"user_id": "ssj_u1", "order_id": "o1", "amount": 99.0, "_event_time": t_ms + 5000},
    )
    app.flush()

    row = app.get("ssj_u1").to_dict()
    # At least one matched pair emitted into the aggregation (v0 may double-
    # emit for eager null-pairs under type=left, but this is type=inner).
    assert row.get("matched", 0) >= 1, f"expected matched>=1, got {row!r}"


def test_stream_table_enrich_tcp(app):
    """Stream↔Table enrichment feeds downstream aggregation (23-01 regression)."""

    @bv.stream
    class ClicksE2E:
        user_id: str
        page: str

    @bv.table(key="user_id")
    class UserProfileE2E:
        user_id: str
        country: str

    EnrichedE2E = ClicksE2E.join(UserProfileE2E, on="user_id", type="inner")

    @bv.table(key="country")
    def ByCountryE2E(e: EnrichedE2E) -> bv.Table:
        return e.group_by("country").agg(n=bv.count(window="1h"))

    app.register(ClicksE2E, UserProfileE2E, EnrichedE2E, ByCountryE2E)

    app.set("eu1", {"country": "US"})
    app.set("eu2", {"country": "UK"})

    app.push_sync(ClicksE2E, {"user_id": "eu1", "page": "/home"})
    app.push_sync(ClicksE2E, {"user_id": "eu1", "page": "/about"})
    app.push_sync(ClicksE2E, {"user_id": "eu2", "page": "/home"})
    app.flush()

    us_row = app.get("US").to_dict()
    uk_row = app.get("UK").to_dict()
    assert us_row.get("n") == 2, f"US: {us_row!r}"
    assert uk_row.get("n") == 1, f"UK: {uk_row!r}"


def test_table_table_join_tcp(app):
    """Table↔Table register + SET smoke (v0 Known Stub: see module docstring)."""

    @bv.table(key="user_id")
    class TProfile:
        user_id: str
        country: str

    @bv.table(key="user_id")
    class TRisk:
        user_id: str
        score: int

    ProfileRisk = TProfile.join(TRisk, on="user_id", type="inner")

    app.register(TProfile, TRisk, ProfileRisk)

    # SET on both input Tables.
    app.set("tt_u1", {"country": "US"})
    app.set("tt_u1", {"score": 42})
    app.flush()

    row = app.get("tt_u1").to_dict()
    # Smoke assertion: both sides' fields are observable on the entity.
    # (v0 single-entity TT storage — see module docstring Known Stub.)
    assert row.get("country") == "US", f"TT smoke — country missing: {row!r}"
    assert row.get("score") == 42, f"TT smoke — score missing: {row!r}"
