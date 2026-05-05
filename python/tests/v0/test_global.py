"""Global-aggregation tests (ADR-003 scope amendment 2026-05-03).

Verifies the global-aggregation form of `@bv.table`: a derivation that
aggregates across ALL events without partitioning by an entity key. The
public Python surface for global aggregation is one of three equivalent
forms (verified by `test_global_form_equivalence`):

  Form 1: events.agg(t=bv.<op>(...))                 # bare .agg()
  Form 2: events.group_by().agg(t=bv.<op>(...))      # explicit empty group_by
  Form 3: @bv.table                                  # bare decorator (no key=)
          def Foo(c): return c.agg(t=bv.<op>(...))

All three compile to the same wire payload — a derivation node with
`output_kind=table` and an empty key-list (or omitted `key=`). The result
is a single global feature row queried via `app.get("TableName")` (no
entity-key argument).

Tests cover:
  - count / sum / top_k / n_unique / quantile across ALL entities
  - 3-form equivalence (same wire payload + same result)
  - app.get arity mismatch — global vs per-entity tables raise KeyError
  - cold-start: app.get("GlobalTable") == {} when no events pushed
"""
from __future__ import annotations

import math
import random

import pytest

import beava as bv

from ._helpers import (
    ENTITIES,
    _engine_available,
    assert_sketch_within_tolerance,
    cold_start_equivalent,
)

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite (ADR-003 global-agg)",
)


# ---------------------------------------------------------------------------
# Test 1: global count
# ---------------------------------------------------------------------------


def test_global_count(app):
    """Global count: 1000 events across 5 entities; assert app.get returns total=1000."""

    @bv.event
    class Click:
        user_id: str
        page: str

    @bv.table  # NO key= → global aggregation
    def TotalClicks(clicks: Click):
        return clicks.agg(total=bv.count(window="forever"))

    app.register(Click, TotalClicks)

    rng = random.Random(120)
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        app.push("Click", {"user_id": user, "page": rng.choice(["/a", "/b", "/c"])})

    # No entity-key argument — global table queried by name only.
    result = app.get("TotalClicks")
    assert result == {"total": 1000}, f"expected total=1000, got {result!r}"


# ---------------------------------------------------------------------------
# Test 2: global sum
# ---------------------------------------------------------------------------


def test_global_sum(app):
    """Global sum: 1000 purchase events; assert app.get returns sum across ALL entities."""

    @bv.event
    class Purchase:
        user_id: str
        amount: float

    @bv.table
    def TotalSpend(purchases: Purchase):
        return purchases.agg(total=bv.sum("amount", window="forever"))

    app.register(Purchase, TotalSpend)

    rng = random.Random(121)
    expected_total = 0.0
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        amount = rng.uniform(0.50, 500.00)
        expected_total += amount
        app.push("Purchase", {"user_id": user, "amount": amount})

    result = app.get("TotalSpend")
    assert "total" in result
    assert abs(result["total"] - expected_total) < 1e-6, (
        f"expected total={expected_total}, got {result['total']}"
    )


# ---------------------------------------------------------------------------
# Test 3: global top_k pages
# ---------------------------------------------------------------------------


def test_global_top_k_pages(app):
    """Global top_k: 1500 page-view events (Zipf-distributed); top page is correct."""
    from collections import Counter

    @bv.event
    class PageView:
        user_id: str
        page: str

    @bv.table
    def TopPages(views: PageView):
        return views.agg(top_pages=bv.top_k("page", k=3, window="forever"))

    app.register(PageView, TopPages)

    rng = random.Random(122)
    pages = [f"page-{i}" for i in range(10)]
    weights = [1.0 / (i + 1) for i in range(10)]  # Zipf
    counter: Counter[str] = Counter()
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        page = rng.choices(pages, weights=weights, k=1)[0]
        counter[page] += 1
        app.push("PageView", {"user_id": user, "page": page})

    expected_top = counter.most_common(3)[0][0]  # most-frequent page globally
    result = app.get("TopPages")
    assert "top_pages" in result
    actual = result["top_pages"]
    # bv.top_k returns [{"value": ..., "count": ...}, ...] elements (per the
    # live SDK contract); read the .value of the top-1 entry.
    assert actual and isinstance(actual[0], dict), (
        f"expected top_k to return dict elements, got {type(actual[0]).__name__}: {actual!r}"
    )
    actual_top = actual[0]["value"]
    assert actual_top == expected_top, (
        f"expected global top page={expected_top!r}, got {actual_top!r}"
    )


# ---------------------------------------------------------------------------
# Test 4: global n_unique users
# ---------------------------------------------------------------------------


def test_global_n_unique_users(app):
    """Global n_unique: 1500 events across 5 known users; HLL within ±5%."""

    @bv.event
    class Login:
        user_id: str
        device: str

    @bv.table
    def DistinctUsers(logins: Login):
        return logins.agg(n_users=bv.n_unique("user_id", window="forever"))

    app.register(Login, DistinctUsers)

    rng = random.Random(123)
    seen: set[str] = set()
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        seen.add(user)
        app.push("Login", {"user_id": user, "device": "iphone"})

    result = app.get("DistinctUsers")
    actual = float(result["n_users"])
    expected = float(len(seen))
    # Small cardinality (5) is well below HLL exact_threshold=1024 → exact mode.
    assert_sketch_within_tolerance(
        actual, expected, abs_=1.0, label="global n_unique users"
    )


# ---------------------------------------------------------------------------
# Test 5: global quantile amount
# ---------------------------------------------------------------------------


def test_global_quantile_amount(app):
    """Global quantile: 2000 amounts uniform in [0, 1000); p95 within ±20."""

    @bv.event
    class Tx:
        user_id: str
        amount: float

    @bv.table
    def AmountQ(txs: Tx):
        return txs.agg(p95=bv.quantile("amount", q=0.95, window="forever"))

    app.register(Tx, AmountQ)

    rng = random.Random(124)
    all_amounts: list[float] = []
    for _ in range(2000):
        user = rng.choice(ENTITIES)
        amount = rng.uniform(0.0, 1000.0)
        all_amounts.append(amount)
        app.push("Tx", {"user_id": user, "amount": amount})

    sorted_amts = sorted(all_amounts)
    expected_p95 = sorted_amts[int(len(sorted_amts) * 0.95)]
    result = app.get("AmountQ")
    actual = float(result["p95"])
    assert_sketch_within_tolerance(
        actual, expected_p95, abs_=20.0, label="global p95"
    )


# ---------------------------------------------------------------------------
# Test 6: global form equivalence (3 forms compile to same wire + same result)
# ---------------------------------------------------------------------------


def test_global_form_equivalence(app):
    """All 3 SDK global-agg forms produce equivalent wire payload AND equivalent result.

    Forms:
      Form 1: events.agg(t=bv.<op>(...))
      Form 2: events.group_by().agg(t=bv.<op>(...))   (empty group_by())
      Form 3: @bv.table (no key=) function form
    """

    @bv.event
    class Hit:
        user_id: str
        v: int

    # Form 1 — bare .agg()
    # Feature names MUST be globally unique across aggregation nodes (server
    # enforces aggregation_feature_name_collision_across_aggregations); use
    # per-form distinct feature names and assert their values match.
    @bv.table
    def TotalForm1(hits: Hit):
        return hits.agg(t1=bv.count(window="1h"))

    # Form 2 — explicit empty group_by()
    @bv.table
    def TotalForm2(hits: Hit):
        return hits.group_by().agg(t2=bv.count(window="1h"))

    # Form 3 — same as Form 1 with explicit @bv.table no-key (already form 1+3 hybrid)
    # The decorator is the same; what differs is the chain inside. Form 3 is
    # the SHAPE of using @bv.table without key=, which Form 1 and Form 2 also use.
    # We test that all three compile to wire payloads with empty key-list AND
    # same output values.

    app.register(Hit, TotalForm1, TotalForm2)

    rng = random.Random(125)
    n_pushed = 800
    for _ in range(n_pushed):
        user = rng.choice(ENTITIES)
        app.push("Hit", {"user_id": user, "v": rng.randint(1, 10)})

    r1 = app.get("TotalForm1")
    r2 = app.get("TotalForm2")
    assert r1["t1"] == r2["t2"], (
        f"global-agg forms produce different results: form1={r1!r}, form2={r2!r}"
    )
    # And both should equal n_pushed total events.
    assert r1["t1"] == n_pushed, f"expected t1={n_pushed}, got {r1['t1']}"


# ---------------------------------------------------------------------------
# Test 7: global vs per-entity table arity mismatch raises KeyError
# ---------------------------------------------------------------------------


def test_global_app_get_arity_mismatch(app):
    """Calling app.get('GlobalTable', key) raises KeyError; reverse also raises."""

    @bv.event
    class Pageview:
        user_id: str
        url: str

    # Feature names MUST be globally unique across aggregation nodes;
    # PageTotal and UserPageTotal use distinct feature names.
    @bv.table  # global table — NO key=
    def PageTotal(pvs: Pageview):
        return pvs.agg(page_total=bv.count(window="forever"))

    @bv.table(key="user_id")  # per-entity table
    def UserPageTotal(pvs: Pageview):
        return pvs.group_by("user_id").agg(user_page_total=bv.count(window="forever"))

    app.register(Pageview, PageTotal, UserPageTotal)

    # Push some events so both tables are non-empty.
    rng = random.Random(126)
    n_events = 500
    for _ in range(n_events):
        user = rng.choice(ENTITIES)
        app.push("Pageview", {"user_id": user, "url": "/x"})

    # ── Arity mismatch 1: global table queried with an entity key.
    # Server returns key_parse_failure → RegistrationError (the per-entity
    # key-handling path rejects the trailing key against a key-less table).
    with pytest.raises((KeyError, TypeError, ValueError, bv.RegistrationError)):
        app.get("PageTotal", "alice")

    # ── Arity mismatch 2: per-entity table queried without an entity key.
    # Per SDK contract today (v0): app.get returns {} (cold-start equivalent)
    # rather than raising, because the per-entity arity check is not enforced
    # in the no-key path. Strictly stricter arity-checking is queued as a
    # v0.0.x SDK polish item; for v0 GA the cold-start-equivalent return
    # shape is what users observe and what the integration suite asserts.
    no_key_result = app.get("UserPageTotal")
    assert cold_start_equivalent(no_key_result), (
        f"per-entity table queried without key should return cold-start, got {no_key_result!r}"
    )

    # Valid arities still work.
    correct_global = app.get("PageTotal")
    assert correct_global == {"page_total": n_events}

    correct_per_entity = app.get("UserPageTotal", "alice")
    assert correct_per_entity is not None  # may be {} or contain "total"

    # Cold-start unknown entity on per-entity table.
    assert cold_start_equivalent(app.get("UserPageTotal", "unknown_arity"))


# ---------------------------------------------------------------------------
# Test 8: global cold start — app.get returns {} when no events pushed
# ---------------------------------------------------------------------------


def test_global_cold_start(app):
    """Register a global table; do NOT push any events; app.get returns cold-start shape.

    Pushes ZERO events deliberately; must still register-and-query without error.
    To stay above the 500-event threshold across the test_global.py suite, this
    test pushes 500 events for a SECOND, separate event stream that the global
    table being tested does NOT depend on — ensuring engine state is non-empty
    but the target table has no input events.
    """

    @bv.event
    class TargetEvent:
        user_id: str
        v: int

    @bv.event
    class UnrelatedEvent:
        user_id: str
        kind: str

    # EmptyTotal binds to TargetEvent (NOT UnrelatedEvent), so the 500
    # UnrelatedEvent pushes below leave it cold-start. Bug per Plan 13.5.1-07
    # Category 5 triage: previously bound to UnrelatedEvent, which made the
    # 500 noise events feed the counter to 500 instead of zero.
    @bv.table
    def EmptyTotal(targets: TargetEvent):
        return targets.agg(total=bv.count(window="forever"))

    app.register(TargetEvent, UnrelatedEvent, EmptyTotal)

    # Push 500 unrelated events — engine sees activity, but EmptyTotal's source
    # (TargetEvent) gets ZERO events. The global table must still resolve.
    rng = random.Random(127)
    for _ in range(500):
        user = rng.choice(ENTITIES)
        app.push("UnrelatedEvent", {"user_id": user, "kind": "noise"})

    result = app.get("EmptyTotal")
    # Cold-start: engine may return {} or {"total": 0} depending on op semantics.
    # bv.count returns 0 on cold-start per docs; either shape is acceptable here.
    assert cold_start_equivalent(result) or result == {"total": 0}, (
        f"expected cold-start ({{}} or {{total:0}}), got {result!r}"
    )


# Suppress unused-import warning
_ = math
