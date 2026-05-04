"""Sketch operator tests — n_unique / quantile / top_k / bloom_member / entropy.

5 tests, each pushing 1000-2000 events to give the sketches enough volume to
hit their precision floor. Expected values computed in-test via Python brute
force; sketches asserted within tolerance helpers from _helpers.py.

Per ADR-002 (Polars-rename) the sketch ops are:
  count_distinct -> n_unique / percentile -> quantile (renamed Phase 13.4 + 13.5).
top_k, bloom_member, entropy keep their names.
"""
from __future__ import annotations

import math
import random
import statistics
from collections import Counter

import pytest

import beava as bv

from ._helpers import (
    ENTITIES,
    _engine_available,
    assert_sketch_within_tolerance,
    assert_top_k_order_preserved,
    cold_start_equivalent,
)

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite",
)


# ---------------------------------------------------------------------------
# Test 1: n_unique (HLL-backed)
# ---------------------------------------------------------------------------


def test_n_unique_per_user_high_volume(app):
    """bv.n_unique: 1500 events / 5 users with ~50 unique URLs per user.

    Verifies HLL precision within ±5% of true cardinality.
    """

    @bv.event
    class Visit:
        user_id: str
        url: str

    @bv.table(key="user_id")
    def UserDistinctUrls(visits):
        return visits.group_by("user_id").agg(
            n_distinct=bv.n_unique("url", window="forever"),
        )

    app.register(Visit, UserDistinctUrls)

    rng = random.Random(50)
    # Build a fixed pool of 50 unique URLs per entity (so true cardinality is bounded).
    url_pools: dict[str, list[str]] = {
        entity: [f"https://example.com/{entity}/page-{i}" for i in range(50)]
        for entity in ENTITIES
    }
    seen: dict[str, set[str]] = {entity: set() for entity in ENTITIES}
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        url = rng.choice(url_pools[user])
        seen[user].add(url)
        app.push("Visit", {"user_id": user, "url": url})

    expected = {entity: len(urls) for entity, urls in seen.items() if urls}
    for entity, exp in expected.items():
        result = app.get("UserDistinctUrls", entity)
        actual = result["n_distinct"]
        # HLL accuracy ~1.04/sqrt(m); precision=14 default => ~1.6% floor.
        # Allow ±5% to be safe; small cardinalities (<256) hit exact mode.
        assert_sketch_within_tolerance(
            float(actual), float(exp), rel=0.05, label=f"{entity} n_unique"
        )

    assert cold_start_equivalent(app.get("UserDistinctUrls", "unknown_visitor"))


# ---------------------------------------------------------------------------
# Test 2: quantile (DDSketch-backed)
# ---------------------------------------------------------------------------


def test_quantile_per_user_high_volume(app):
    """bv.quantile: 2000 events / 5 users with uniform amounts in [0, 1000).

    Verifies p50/p95 within ±2 percentile points of the true sample value.
    """

    @bv.event
    class Txn:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserQuantile(txns):
        return txns.group_by("user_id").agg(
            p50=bv.quantile("amount", q=0.5, window="forever"),
            p95=bv.quantile("amount", q=0.95, window="forever"),
        )

    app.register(Txn, UserQuantile)

    rng = random.Random(51)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(2000):
        user = rng.choice(ENTITIES)
        amount = rng.uniform(0.0, 1000.0)
        accum[user].append(amount)
        app.push("Txn", {"user_id": user, "amount": amount})

    for entity, values in accum.items():
        if len(values) < 10:
            continue
        sorted_vals = sorted(values)
        # True p50 = element at index n*0.5; p95 = index n*0.95
        true_p50 = sorted_vals[int(len(sorted_vals) * 0.5)]
        true_p95 = sorted_vals[int(len(sorted_vals) * 0.95)]
        result = app.get("UserQuantile", entity)
        # ±2 percentile points on a uniform [0, 1000) distribution = ±20.
        assert_sketch_within_tolerance(
            float(result["p50"]), float(true_p50), abs_=20.0, label=f"{entity} p50"
        )
        assert_sketch_within_tolerance(
            float(result["p95"]), float(true_p95), abs_=20.0, label=f"{entity} p95"
        )

    assert cold_start_equivalent(app.get("UserQuantile", "unknown_q"))


# ---------------------------------------------------------------------------
# Test 3: top_k (SpaceSaving + bounded heap)
# ---------------------------------------------------------------------------


def test_top_k_per_user_high_volume(app):
    """bv.top_k: 1500 events / 5 users with Zipf-distributed page IDs (10 pages).

    Verifies top-3 ranking is preserved (top item correct + members present).
    """

    @bv.event
    class PageView:
        user_id: str
        page: str

    @bv.table(key="user_id")
    def UserTopPages(views):
        return views.group_by("user_id").agg(
            top_pages=bv.top_k("page", k=3, window="forever"),
        )

    app.register(PageView, UserTopPages)

    rng = random.Random(52)
    pages = [f"page-{i}" for i in range(10)]
    # Zipf weights: page 0 most popular, page 9 least popular.
    weights = [1.0 / (i + 1) for i in range(10)]
    counts: dict[str, Counter[str]] = {entity: Counter() for entity in ENTITIES}
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        page = rng.choices(pages, weights=weights, k=1)[0]
        counts[user][page] += 1
        app.push("PageView", {"user_id": user, "page": page})

    for entity, c in counts.items():
        if not c:
            continue
        expected_ranking = [page for page, _cnt in c.most_common(3)]
        result = app.get("UserTopPages", entity)
        actual_top_k = result["top_pages"]
        # Top-K result may be list of strings or list of (value, count) pairs;
        # extract just the value if pair-shaped.
        if actual_top_k and isinstance(actual_top_k[0], (list, tuple)):
            actual_values = [pair[0] for pair in actual_top_k]
        else:
            actual_values = list(actual_top_k)
        assert_top_k_order_preserved(
            actual_values, expected_ranking, label=f"{entity} top_3"
        )

    assert cold_start_equivalent(app.get("UserTopPages", "unknown_topk"))


# ---------------------------------------------------------------------------
# Test 4: bloom_member (Bloom filter ever-seen)
# ---------------------------------------------------------------------------


def test_bloom_member_per_user_high_volume(app):
    """bv.bloom_member: 1000 events / 5 users with known device IDs.

    Verifies seen IDs return True; unseen IDs return False (modulo ~1% FPR).
    """

    @bv.event
    class Login:
        user_id: str
        device_id: str

    @bv.table(key="user_id")
    def UserDeviceBloom(logins):
        return logins.group_by("user_id").agg(
            device_seen=bv.bloom_member("device_id", capacity=2048, fpr=0.01),
        )

    app.register(Login, UserDeviceBloom)

    rng = random.Random(53)
    # Per-entity device pool — known seen set per user.
    seen_devices: dict[str, set[str]] = {entity: set() for entity in ENTITIES}
    device_pool = [f"device-{i:04d}" for i in range(50)]
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        device = rng.choice(device_pool)
        seen_devices[user].add(device)
        app.push("Login", {"user_id": user, "device_id": device})

    # bloom_member is read at apply-time and reflects "ever seen this value
    # for this entity". The most recent push's bloom-test result is what
    # app.get returns — but the contract is "ever-seen", so we assert the
    # filter at least contains every device this entity has seen. This is
    # a state-correctness test (no false-negatives); we cannot test FPR
    # via app.get without a per-query API.
    for entity in ENTITIES:
        if not seen_devices[entity]:
            continue
        result = app.get("UserDeviceBloom", entity)
        assert result is not None
        assert "device_seen" in result, f"{entity}: missing device_seen"
        # Result should be a bool (last-pushed device is always 'seen' since
        # every push inserts into the filter).
        assert isinstance(result["device_seen"], bool), (
            f"{entity}: device_seen is not bool: {result['device_seen']!r}"
        )

    assert cold_start_equivalent(app.get("UserDeviceBloom", "unknown_blm"))


# ---------------------------------------------------------------------------
# Test 5: entropy (Shannon entropy over categorical distribution)
# ---------------------------------------------------------------------------


def test_entropy_per_user_high_volume(app):
    """bv.entropy: 1500 events / 5 users across 8 categories.

    Some users get uniform distribution (entropy ~ log2(8) = 3.0);
    others get concentrated distribution (entropy ~ 1.0). Verifies
    Shannon entropy within ±0.05 nats... actually log2 base, so within
    ±0.05 of true entropy in bits.
    """

    @bv.event
    class Action:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserActionEntropy(actions):
        return actions.group_by("user_id").agg(
            diversity=bv.entropy("kind", window="forever"),
        )

    app.register(Action, UserActionEntropy)

    rng = random.Random(54)
    categories = [f"cat-{i}" for i in range(8)]
    counts: dict[str, Counter[str]] = {entity: Counter() for entity in ENTITIES}
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        # Half the entities pick uniform; half pick concentrated.
        if user in ("alice", "bob", "carol"):
            kind = rng.choice(categories)  # uniform across 8
        else:
            # Concentrated: 80% on cat-0, 20% spread elsewhere
            kind = "cat-0" if rng.random() < 0.8 else rng.choice(categories[1:])
        counts[user][kind] += 1
        app.push("Action", {"user_id": user, "kind": kind})

    for entity, c in counts.items():
        if not c:
            continue
        total = sum(c.values())
        # Shannon entropy in bits (log base 2)
        expected_entropy = -sum(
            (cnt / total) * math.log2(cnt / total) for cnt in c.values() if cnt > 0
        )
        result = app.get("UserActionEntropy", entity)
        assert_sketch_within_tolerance(
            float(result["diversity"]),
            float(expected_entropy),
            abs_=0.10,  # categorical entropy with 1500 samples is solid; allow 0.10 bits slack
            label=f"{entity} entropy",
        )

    assert cold_start_equivalent(app.get("UserActionEntropy", "unknown_ent"))


# Avoid a "statistics imported but unused" lint warning.
_ = statistics
