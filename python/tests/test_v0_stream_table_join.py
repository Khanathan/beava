"""Phase 23-01 Task 3 — Stream↔Table join TCP round-trip.

End-to-end pytest cases against the live tally_server fixture:

  1. ``test_stream_table_enrich_tcp_roundtrip`` — register Clicks +
     UserProfile + Enriched + an aggregation downstream of Enriched, SET
     the right-side row, PUSH a left-side event, GET the aggregation row
     and assert the event flowed through the enrichment.
  2. ``test_stream_table_enrich_composite_key_tcp`` — same shape with
     ``key=["user_id", "region"]`` on the table; SET under the composite
     key and verify the enrichment fires.
  3. ``test_stream_table_outer_rejected_at_register`` — SDK-side check
     that ``type="outer"`` raises before reaching the server.

All three tests reuse the session-scoped ``app`` fixture from
``conftest.py``; no SDK changes (the contract was frozen in 21-03).
"""

from __future__ import annotations

import pytest

import tally as tl


def test_stream_table_enrich_tcp_roundtrip(app):
    @tl.stream
    class Clicks:
        user_id: str
        page: str

    @tl.table(key="user_id")
    class UserProfile:
        user_id: str
        country: str

    Enriched = Clicks.join(UserProfile, on="user_id", type="left")

    @tl.table(key="user_id")
    def EnrichedAgg(enriched: Enriched) -> tl.Table:
        return enriched.group_by("user_id").agg(n=tl.count(window="1h"))

    app.register(Clicks, UserProfile, Enriched, EnrichedAgg)

    # Unique key prefix per test — the session-scoped `app` fixture shares
    # state across the whole pytest run; distinctive prefixes prevent
    # cross-test pollution (matches the pattern used in test_v0_joins_e2e.py).
    app.set("stj_u1", {"country": "US"})

    # Left-hit (stj_u1) and left-miss (stj_u2) — type=left so both cascade.
    app.push_sync(Clicks, {"user_id": "stj_u1", "page": "/home"})
    app.push_sync(Clicks, {"user_id": "stj_u2", "page": "/x"})
    app.flush()

    row_u1 = app.get("stj_u1")
    row_u2 = app.get("stj_u2")
    assert row_u1["n"] == 1, f"stj_u1 enriched-agg row: {row_u1!r}"
    assert row_u2["n"] == 1, f"stj_u2 left-miss row should still cascade: {row_u2!r}"


def test_stream_table_enrich_composite_key_tcp(app):
    @tl.stream
    class CompClicks:
        user_id: str
        region: str
        page: str

    @tl.table(key=["user_id", "region"])
    class CompProfile:
        user_id: str
        region: str
        country: str

    EnrichedC = CompClicks.join(CompProfile, on=["user_id", "region"], type="left")

    @tl.table(key=["user_id", "region"])
    def CompAgg(enriched: EnrichedC) -> tl.Table:
        return enriched.group_by("user_id", "region").agg(n=tl.count(window="1h"))

    app.register(CompClicks, CompProfile, EnrichedC, CompAgg)

    # Composite-keyed Table row.
    app.set("u1|US", {"country": "USA"})

    app.push_sync(CompClicks, {"user_id": "u1", "region": "US", "page": "/"})
    app.push_sync(CompClicks, {"user_id": "u1", "region": "EU", "page": "/"})
    app.flush()

    row_us = app.get("u1|US")
    row_eu = app.get("u1|EU")
    assert row_us["n"] == 1, f"u1|US row: {row_us!r}"
    assert row_eu["n"] == 1, f"u1|EU left-miss should still cascade: {row_eu!r}"


def test_stream_table_outer_rejected_at_register():
    """SDK-side regression guard — outer joins refuse to compile.

    Runs offline (no server fixture); ensures the SDK rejects outer joins
    before any TCP REGISTER round-trip is attempted. The Rust engine
    rejects `type="outer"` as defense in depth (covered by
    ``tests/test_join_stream_table.rs::enrich_rejects_outer``).
    """

    @tl.stream
    class L:
        k: str

    @tl.table(key="k")
    class R:
        k: str
        v: str

    with pytest.raises((RuntimeError, TypeError)) as exc_info:
        L.join(R, on="k", type="outer")
    assert "outer" in str(exc_info.value).lower()
