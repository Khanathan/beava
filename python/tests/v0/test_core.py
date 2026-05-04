"""Core aggregation operator tests — count / sum / mean / min / max / var / std / ratio.

8 tests, each pushing >=500 events across >=3 entities, computing per-entity
expected via a Python loop over the same input data, then asserting
``app.get(table, entity)`` matches expected. Cold-start assertion at the end.

Per ADR-002 (Polars-rename) the public Python helpers for v0 are:
  avg -> mean / variance -> var / stddev -> std
The old names remain as deprecation aliases in v0.0.x; tests use the new names.
"""
from __future__ import annotations

import random
import statistics

import pytest

import beava as bv

from ._helpers import ENTITIES, _engine_available, cold_start_equivalent

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite",
)


# ---------------------------------------------------------------------------
# Test 1: count
# ---------------------------------------------------------------------------


def test_count_per_user_high_volume(app):
    """bv.count: 1000 events / 5 users; per-entity count + cold-start."""

    @bv.event
    class Click:
        user_id: str
        page: str

    @bv.table(key="user_id")
    def UserClicks(clicks):
        return clicks.group_by("user_id").agg(
            click_count=bv.count(window="forever"),
        )

    app.register(Click, UserClicks)

    rng = random.Random(42)
    expected: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        expected[user] += 1
        app.push("Click", {"user_id": user, "page": rng.choice(["/home", "/about", "/contact"])})

    assert sum(expected.values()) == 1000
    for entity, exp in expected.items():
        result = app.get("UserClicks", entity)
        assert result == {"click_count": exp}, f"{entity}: expected {exp}, got {result}"

    assert cold_start_equivalent(app.get("UserClicks", "unknown_xyz"))


# ---------------------------------------------------------------------------
# Test 2: sum
# ---------------------------------------------------------------------------


def test_sum_per_user_high_volume(app):
    """bv.sum: 800 purchase events / 4 users; per-entity sum + cold-start."""

    @bv.event
    class Purchase:
        user_id: str
        amount: float

    @bv.table(key="user_id")
    def UserSpend(purchases):
        return purchases.group_by("user_id").agg(
            total=bv.sum("amount", window="forever"),
        )

    app.register(Purchase, UserSpend)

    pool = ENTITIES[:4]  # alice/bob/carol/dave
    rng = random.Random(43)
    expected: dict[str, float] = {entity: 0.0 for entity in pool}
    for _ in range(800):
        user = rng.choice(pool)
        amount = rng.uniform(0.50, 500.00)
        expected[user] += amount
        app.push("Purchase", {"user_id": user, "amount": amount})

    for entity, exp in expected.items():
        result = app.get("UserSpend", entity)
        assert result is not None, f"{entity}: cold result"
        assert "total" in result, f"{entity}: missing total key in {result!r}"
        assert abs(result["total"] - exp) < 1e-6, f"{entity}: expected {exp}, got {result['total']}"

    assert cold_start_equivalent(app.get("UserSpend", "unknown_zz"))


# ---------------------------------------------------------------------------
# Test 3: mean (formerly avg)
# ---------------------------------------------------------------------------


def test_mean_per_user_high_volume(app):
    """bv.mean: 600 events / 3 users; per-entity arithmetic mean + cold-start."""

    @bv.event
    class Order:
        user_id: str
        value: float

    @bv.table(key="user_id")
    def UserMeanOrder(orders):
        return orders.group_by("user_id").agg(
            avg_value=bv.mean("value", window="forever"),
        )

    app.register(Order, UserMeanOrder)

    pool = ENTITIES[:3]
    rng = random.Random(44)
    accum: dict[str, list[float]] = {entity: [] for entity in pool}
    for _ in range(600):
        user = rng.choice(pool)
        value = rng.uniform(1.0, 1000.0)
        accum[user].append(value)
        app.push("Order", {"user_id": user, "value": value})

    expected = {entity: sum(values) / len(values) for entity, values in accum.items()}
    for entity, exp in expected.items():
        result = app.get("UserMeanOrder", entity)
        assert abs(result["avg_value"] - exp) < 1e-6, (
            f"{entity}: expected {exp}, got {result['avg_value']}"
        )

    assert cold_start_equivalent(app.get("UserMeanOrder", "unknown_y"))


# ---------------------------------------------------------------------------
# Test 4: min
# ---------------------------------------------------------------------------


def test_min_per_user_high_volume(app):
    """bv.min: 500 events / 5 users; per-entity minimum + cold-start."""

    @bv.event
    class Bid:
        user_id: str
        price: float

    @bv.table(key="user_id")
    def UserMinBid(bids):
        return bids.group_by("user_id").agg(
            lowest=bv.min("price", window="forever"),
        )

    app.register(Bid, UserMinBid)

    rng = random.Random(45)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        price = rng.uniform(0.10, 9999.99)
        accum[user].append(price)
        app.push("Bid", {"user_id": user, "price": price})

    expected = {entity: min(values) for entity, values in accum.items() if values}
    for entity, exp in expected.items():
        result = app.get("UserMinBid", entity)
        assert abs(result["lowest"] - exp) < 1e-9, (
            f"{entity}: expected {exp}, got {result['lowest']}"
        )

    assert cold_start_equivalent(app.get("UserMinBid", "unknown_minor"))


# ---------------------------------------------------------------------------
# Test 5: max
# ---------------------------------------------------------------------------


def test_max_per_user_high_volume(app):
    """bv.max: 500 events / 5 users; per-entity maximum + cold-start."""

    @bv.event
    class Score:
        user_id: str
        value: float

    @bv.table(key="user_id")
    def UserMaxScore(scores):
        return scores.group_by("user_id").agg(
            best=bv.max("value", window="forever"),
        )

    app.register(Score, UserMaxScore)

    rng = random.Random(46)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        value = rng.uniform(0.0, 100.0)
        accum[user].append(value)
        app.push("Score", {"user_id": user, "value": value})

    expected = {entity: max(values) for entity, values in accum.items() if values}
    for entity, exp in expected.items():
        result = app.get("UserMaxScore", entity)
        assert abs(result["best"] - exp) < 1e-9, (
            f"{entity}: expected {exp}, got {result['best']}"
        )

    assert cold_start_equivalent(app.get("UserMaxScore", "unknown_high"))


# ---------------------------------------------------------------------------
# Test 6: var (formerly variance — Welford / Bessel-corrected)
# ---------------------------------------------------------------------------


def test_var_per_user_high_volume(app):
    """bv.var: 750 events / 5 users; per-entity sample variance + cold-start."""

    @bv.event
    class Latency:
        user_id: str
        ms: float

    @bv.table(key="user_id")
    def UserLatencyVar(measurements):
        return measurements.group_by("user_id").agg(
            spread=bv.var("ms", window="forever"),
        )

    app.register(Latency, UserLatencyVar)

    rng = random.Random(47)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(750):
        user = rng.choice(ENTITIES)
        ms = rng.gauss(50.0, 15.0)  # latency-like distribution
        accum[user].append(ms)
        app.push("Latency", {"user_id": user, "ms": ms})

    # statistics.variance is sample variance (Bessel-corrected) — same as Welford.
    expected = {
        entity: statistics.variance(values)
        for entity, values in accum.items()
        if len(values) >= 2
    }
    for entity, exp in expected.items():
        result = app.get("UserLatencyVar", entity)
        # Floating-point Welford accumulators differ from naive two-pass by
        # accumulated rounding; tolerate 1e-6 relative error.
        assert abs(result["spread"] - exp) / exp < 1e-6, (
            f"{entity}: expected {exp}, got {result['spread']}"
        )

    assert cold_start_equivalent(app.get("UserLatencyVar", "unknown_var"))


# ---------------------------------------------------------------------------
# Test 7: std (formerly stddev — sqrt of sample variance)
# ---------------------------------------------------------------------------


def test_std_per_user_high_volume(app):
    """bv.std: 750 events / 5 users; per-entity sample stddev + cold-start."""

    @bv.event
    class Reading:
        user_id: str
        value: float

    @bv.table(key="user_id")
    def UserReadingStd(readings):
        return readings.group_by("user_id").agg(
            sigma=bv.std("value", window="forever"),
        )

    app.register(Reading, UserReadingStd)

    rng = random.Random(48)
    accum: dict[str, list[float]] = {entity: [] for entity in ENTITIES}
    for _ in range(750):
        user = rng.choice(ENTITIES)
        value = rng.gauss(0.0, 10.0)
        accum[user].append(value)
        app.push("Reading", {"user_id": user, "value": value})

    expected = {
        entity: statistics.stdev(values)
        for entity, values in accum.items()
        if len(values) >= 2
    }
    for entity, exp in expected.items():
        result = app.get("UserReadingStd", entity)
        assert abs(result["sigma"] - exp) / exp < 1e-6, (
            f"{entity}: expected {exp}, got {result['sigma']}"
        )

    assert cold_start_equivalent(app.get("UserReadingStd", "unknown_sigma"))


# ---------------------------------------------------------------------------
# Test 8: ratio
# ---------------------------------------------------------------------------


def test_ratio_per_user_high_volume(app):
    """bv.ratio: 600 login events / 4 users; per-entity failure ratio + cold-start.

    Pushes ~50% failed events; expected per-entity ratio is failed / total
    computed from the same input stream.
    """

    @bv.event
    class Login:
        user_id: str
        status: str

    @bv.table(key="user_id")
    def UserFailRatio(logins):
        return logins.group_by("user_id").agg(
            failure_ratio=bv.ratio(
                window="forever",
                where=bv.col("status") == "failed",
            ),
        )

    app.register(Login, UserFailRatio)

    pool = ENTITIES[:4]
    rng = random.Random(49)
    counts: dict[str, dict[str, int]] = {
        entity: {"failed": 0, "total": 0} for entity in pool
    }
    for _ in range(600):
        user = rng.choice(pool)
        status = "failed" if rng.random() < 0.45 else "ok"
        counts[user]["total"] += 1
        if status == "failed":
            counts[user]["failed"] += 1
        app.push("Login", {"user_id": user, "status": status})

    expected = {
        entity: c["failed"] / c["total"] for entity, c in counts.items() if c["total"] > 0
    }
    for entity, exp in expected.items():
        result = app.get("UserFailRatio", entity)
        assert abs(result["failure_ratio"] - exp) < 1e-9, (
            f"{entity}: expected {exp}, got {result['failure_ratio']}"
        )

    assert cold_start_equivalent(app.get("UserFailRatio", "unknown_user"))
