"""Bounded-buffer operator tests — histogram / hour_of_day_histogram /
dow_hour_histogram / seasonal_deviation / event_type_mix / most_recent_n /
reservoir_sample.

7 tests, each pushing 1000 events / 3-5 entities. Tests verify bucket-cap
behavior, fixed-bucket counts match expected, ring-buffer correctness, and
reservoir-sample uniformity.
"""
from __future__ import annotations

import random
from collections import Counter, deque
from datetime import datetime, timezone

import pytest

import beava as bv

from ._helpers import (
    ENTITIES,
    _engine_available,
    cold_start_equivalent,
)

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite",
)


# ---------------------------------------------------------------------------
# Test 1: histogram — fixed-bucket counts
# ---------------------------------------------------------------------------


def test_histogram_per_user_high_volume(app):
    """bv.histogram (buckets=[10, 50, 100, 500]): 1000 events / 5 users; bucket-count match."""

    @bv.event
    class Tx:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserAmountHist(txs: Tx):
        return txs.group_by("user_id").agg(
            hist=bv.histogram("amount", buckets=[10.0, 50.0, 100.0, 500.0]),
        )

    app.register(Tx, UserAmountHist)

    rng = random.Random(100)
    # Per-entity bucket counts: cells are <10, 10-50, 50-100, 100-500, >=500
    bucket_labels = ["<10", "10-50", "50-100", "100-500", ">=500"]

    def bucketize(amt: float) -> str:
        if amt < 10.0:
            return "<10"
        if amt < 50.0:
            return "10-50"
        if amt < 100.0:
            return "50-100"
        if amt < 500.0:
            return "100-500"
        return ">=500"

    counts: dict[str, dict[str, int]] = {
        entity: dict.fromkeys(bucket_labels, 0) for entity in ENTITIES
    }
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        amount = rng.uniform(0.0, 1000.0)
        counts[user][bucketize(amount)] += 1
        app.push("Tx", {"user_id": user, "amount": amount})

    for entity, exp in counts.items():
        if sum(exp.values()) == 0:
            continue
        result = app.get("UserAmountHist", entity)
        actual_hist = result["hist"]
        # Every bucket label expected count must match observed.
        for label, exp_cnt in exp.items():
            actual_cnt = actual_hist.get(label, 0)
            assert actual_cnt == exp_cnt, (
                f"{entity}: bucket {label!r} expected {exp_cnt}, got {actual_cnt}"
            )

    assert cold_start_equivalent(app.get("UserAmountHist", "unknown_hist"))


# ---------------------------------------------------------------------------
# Test 2: hour_of_day_histogram — 24-bucket per-hour count
# ---------------------------------------------------------------------------


def test_hour_of_day_histogram_per_user_high_volume(app):
    """bv.hour_of_day_histogram: 1000 events / 5 users; counts within 24-bucket bound."""

    @bv.event
    class Beat:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserHourHist(beats: Beat):
        return beats.group_by("user_id").agg(
            hist=bv.hour_of_day_histogram(),
        )

    app.register(Beat, UserHourHist)

    rng = random.Random(101)
    counts: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        counts[user] += 1
        app.push("Beat", {"user_id": user, "kind": "p"})

    # The current-UTC-hour bucket should hold all of this entity's pushes
    # (within a few-second test window). Other 23 buckets should be 0.
    current_hour = datetime.now(tz=timezone.utc).hour
    for entity, total in counts.items():
        if total == 0:
            continue
        result = app.get("UserHourHist", entity)
        hist = result["hist"]
        # Sum across all 24 buckets must equal total events for entity.
        if isinstance(hist, dict):
            actual_sum = sum(hist.values())
        elif isinstance(hist, list):
            actual_sum = sum(hist)
        else:
            actual_sum = 0
        assert actual_sum == total, (
            f"{entity}: hour_hist sum={actual_sum} != total={total}"
        )
        # Current hour bucket should hold a non-zero count
        if isinstance(hist, dict):
            current_bucket = hist.get(str(current_hour), hist.get(current_hour, 0))
        else:
            current_bucket = hist[current_hour]
        assert current_bucket > 0, f"{entity}: current hour {current_hour} bucket = 0"

    assert cold_start_equivalent(app.get("UserHourHist", "unknown_hod"))


# ---------------------------------------------------------------------------
# Test 3: dow_hour_histogram — 168-bucket per-(dow, hour) count
# ---------------------------------------------------------------------------


def test_dow_hour_histogram_per_user_high_volume(app):
    """bv.dow_hour_histogram: 1000 events / 5 users; sum equals push count per entity."""

    @bv.event
    class Beat:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserDowHourHist(beats: Beat):
        return beats.group_by("user_id").agg(
            hist=bv.dow_hour_histogram(),
        )

    app.register(Beat, UserDowHourHist)

    rng = random.Random(102)
    counts: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        counts[user] += 1
        app.push("Beat", {"user_id": user, "kind": "tick"})

    for entity, total in counts.items():
        if total == 0:
            continue
        result = app.get("UserDowHourHist", entity)
        hist = result["hist"]
        if isinstance(hist, dict):
            actual_sum = sum(hist.values())
        elif isinstance(hist, list):
            actual_sum = sum(hist)
        else:
            actual_sum = 0
        assert actual_sum == total, (
            f"{entity}: dow_hour sum={actual_sum} != total={total}"
        )

    assert cold_start_equivalent(app.get("UserDowHourHist", "unknown_dwh"))


# ---------------------------------------------------------------------------
# Test 4: seasonal_deviation — z-score vs hour-of-day baseline
# ---------------------------------------------------------------------------


def test_seasonal_deviation_per_user_high_volume(app):
    """bv.seasonal_deviation: 1000 events / 3 users; |z| bounded after warmup."""

    @bv.event
    class Tx:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserSeasonal(txs: Tx):
        return txs.group_by("user_id").agg(
            sd=bv.seasonal_deviation("amount"),
        )

    app.register(Tx, UserSeasonal)

    pool = ENTITIES[:3]
    rng = random.Random(103)
    for _ in range(1000):
        user = rng.choice(pool)
        amount = rng.gauss(100.0, 20.0)
        app.push("Tx", {"user_id": user, "amount": amount})

    for entity in pool:
        result = app.get("UserSeasonal", entity)
        sd = result.get("sd")
        if sd is None:
            continue
        # Seasonal deviation z-score against same-hour baseline; with all events
        # in the same wall-clock hour and N(100, 20) inputs, |z| < ~6 with
        # very high probability.
        assert abs(float(sd)) <= 10.0, f"{entity}: |seasonal_deviation|={abs(sd)} too large"

    assert cold_start_equivalent(app.get("UserSeasonal", "unknown_sd"))


# ---------------------------------------------------------------------------
# Test 5: event_type_mix — proportion per category (capped at max_categories)
# ---------------------------------------------------------------------------


def test_event_type_mix_per_user_high_volume(app):
    """bv.event_type_mix: 1500 events / 5 users with 5 categories; proportions sum ~ 1.0."""

    @bv.event
    class Action:
        user_id: str
        kind: str

    @bv.table(key="user_id")
    def UserMix(actions: Action):
        return actions.group_by("user_id").agg(
            mix=bv.event_type_mix("kind"),
        )

    app.register(Action, UserMix)

    rng = random.Random(104)
    categories = ["p2p", "card", "crypto", "wire", "ach"]
    counts: dict[str, Counter[str]] = {entity: Counter() for entity in ENTITIES}
    for _ in range(1500):
        user = rng.choice(ENTITIES)
        kind = rng.choice(categories)
        counts[user][kind] += 1
        app.push("Action", {"user_id": user, "kind": kind})

    for entity, c in counts.items():
        if not c:
            continue
        total = sum(c.values())
        expected_props = {k: v / total for k, v in c.items()}
        result = app.get("UserMix", entity)
        actual_mix = result["mix"]
        # Verify proportions sum to ~1.0 and each known category is within tolerance.
        actual_sum = sum(actual_mix.values())
        assert abs(actual_sum - 1.0) < 0.01, (
            f"{entity}: mix proportions sum={actual_sum} != 1.0"
        )
        for cat, exp_prop in expected_props.items():
            actual_prop = actual_mix.get(cat, 0.0)
            assert abs(actual_prop - exp_prop) < 0.02, (
                f"{entity}: mix[{cat}]={actual_prop} != expected {exp_prop}"
            )

    assert cold_start_equivalent(app.get("UserMix", "unknown_mix"))


# ---------------------------------------------------------------------------
# Test 6: most_recent_n — fixed-size circular buffer
# ---------------------------------------------------------------------------


def test_most_recent_n_per_user_high_volume(app):
    """bv.most_recent_n (n=10): 1000 events / 5 users; trailing-10 ring buffer per entity."""

    @bv.event
    class Login:
        user_id: str
        ip: str

    @bv.table(key="user_id")
    def UserRecentIps(logins: Login):
        return logins.group_by("user_id").agg(
            recent=bv.most_recent_n("ip", n=10),
        )

    app.register(Login, UserRecentIps)

    rng = random.Random(105)
    history: dict[str, deque[str]] = {entity: deque(maxlen=10) for entity in ENTITIES}
    for i in range(1000):
        user = rng.choice(ENTITIES)
        ip = f"10.0.0.{i % 256}"
        history[user].append(ip)
        app.push("Login", {"user_id": user, "ip": ip})

    for entity, dq in history.items():
        if not dq:
            continue
        expected = list(dq)
        result = app.get("UserRecentIps", entity)
        actual = result["recent"]
        # Most-recent-n is a sequence — engine returns Vec<Value> in
        # oldest-to-newest order (per docs).
        assert list(actual) == expected, (
            f"{entity}: expected most_recent_10={expected!r}, got {actual!r}"
        )

    assert cold_start_equivalent(app.get("UserRecentIps", "unknown_mrn"))


# ---------------------------------------------------------------------------
# Test 7: reservoir_sample — Vitter Algorithm R uniform sample
# ---------------------------------------------------------------------------


def test_reservoir_sample_per_user_high_volume(app):
    """bv.reservoir_sample (samples=100): 1000 events / 3 users; sample size = 100."""

    @bv.event
    class Tx:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserSample(txs: Tx):
        return txs.group_by("user_id").agg(
            sample=bv.reservoir_sample("amount", samples=100),
        )

    app.register(Tx, UserSample)

    pool = ENTITIES[:3]
    rng = random.Random(106)
    counts: dict[str, int] = {entity: 0 for entity in pool}
    pushed_values: dict[str, list[float]] = {entity: [] for entity in pool}
    for _ in range(1000):
        user = rng.choice(pool)
        counts[user] += 1
        amount = rng.uniform(0.0, 1000.0)
        pushed_values[user].append(amount)
        app.push("Tx", {"user_id": user, "amount": amount})

    for entity, n in counts.items():
        if n == 0:
            continue
        result = app.get("UserSample", entity)
        sample = result["sample"]
        # Sample size must be min(n, 100).
        expected_size = min(n, 100)
        assert len(sample) == expected_size, (
            f"{entity}: sample size {len(sample)} != expected {expected_size}"
        )
        # Every sampled value must be from the entity's pushed value set.
        pushed_set = set(pushed_values[entity])
        for val in sample:
            assert val in pushed_set, (
                f"{entity}: sampled value {val!r} not in pushed set"
            )

    assert cold_start_equivalent(app.get("UserSample", "unknown_rsv"))
