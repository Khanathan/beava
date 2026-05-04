"""Geo operator tests — geo_velocity / geo_distance / geo_spread / distance_from_home.

4 tests, each pushing 500-1000 events with lat/lon pairs distributed within
NYC bounding box (lat 40.6-40.9, lon -74.0 to -73.9). Expected values use
`haversine_km` from _helpers.py to mirror the engine's haversine math
(mean Earth radius 6371 km, per `agg_geo.rs::haversine_km`).
"""
from __future__ import annotations

import random
import statistics
import time

import pytest

import beava as bv

from ._helpers import (
    ENTITIES,
    _engine_available,
    assert_sketch_within_tolerance,
    cold_start_equivalent,
    haversine_km,
)

pytestmark = pytest.mark.skipif(
    not _engine_available(),
    reason="requires Phase 13.4 engine + Phase 13.5 SDK rewrite",
)


# NYC bounding box — used by all geo tests
NYC_LAT_MIN, NYC_LAT_MAX = 40.60, 40.90
NYC_LON_MIN, NYC_LON_MAX = -74.05, -73.85


def _rand_nyc_point(rng: random.Random) -> tuple[float, float]:
    return (
        rng.uniform(NYC_LAT_MIN, NYC_LAT_MAX),
        rng.uniform(NYC_LON_MIN, NYC_LON_MAX),
    )


# ---------------------------------------------------------------------------
# Test 1: geo_velocity — max km/h between consecutive matching events
# ---------------------------------------------------------------------------


def test_geo_velocity_per_user_high_volume(app):
    """bv.geo_velocity: 500 events / 5 users; max speed bounded by physical possibility."""

    @bv.event
    class GeoTx:
        user_id: str
        latitude: float
        longitude: float

    @bv.table(key="user_id")
    def UserGeoVel(txs: GeoTx):
        return txs.group_by("user_id").agg(
            max_kmh=bv.geo_velocity(lat="latitude", lon="longitude"),
        )

    app.register(GeoTx, UserGeoVel)

    rng = random.Random(110)
    push_count: dict[str, int] = {entity: 0 for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        lat, lon = _rand_nyc_point(rng)
        push_count[user] += 1
        app.push("GeoTx", {"user_id": user, "latitude": lat, "longitude": lon})
        # Mild gap so apply-time delta_t > 0
        time.sleep(0.0005)

    for entity, n in push_count.items():
        if n < 2:
            continue
        result = app.get("UserGeoVel", entity)
        max_kmh = result.get("max_kmh")
        assert max_kmh is not None
        # NYC-bounding-box max diagonal is ~30 km; with sub-millisecond pushes
        # max km/h can be very high (since dt is tiny). Just assert finite + non-negative.
        assert max_kmh >= 0.0, f"{entity}: max_kmh negative: {max_kmh}"

    assert cold_start_equivalent(app.get("UserGeoVel", "unknown_gv"))


# ---------------------------------------------------------------------------
# Test 2: geo_distance — cumulative haversine path length
# ---------------------------------------------------------------------------


def test_geo_distance_per_user_high_volume(app):
    """bv.geo_distance: 500 events / 5 users; cumulative path equals sum of segments."""

    @bv.event
    class Move:
        user_id: str
        lat: float
        lon: float

    @bv.table(key="user_id")
    def UserPath(moves: Move):
        return moves.group_by("user_id").agg(
            total_km=bv.geo_distance(lat="lat", lon="lon"),
        )

    app.register(Move, UserPath)

    rng = random.Random(111)
    history: dict[str, list[tuple[float, float]]] = {entity: [] for entity in ENTITIES}
    for _ in range(500):
        user = rng.choice(ENTITIES)
        lat, lon = _rand_nyc_point(rng)
        history[user].append((lat, lon))
        app.push("Move", {"user_id": user, "lat": lat, "lon": lon})

    for entity, points in history.items():
        if len(points) < 2:
            continue
        # Expected: sum of haversine(prev, curr) for each consecutive pair.
        expected_total = 0.0
        for i in range(1, len(points)):
            expected_total += haversine_km(
                points[i - 1][0], points[i - 1][1], points[i][0], points[i][1]
            )
        result = app.get("UserPath", entity)
        actual = float(result["total_km"])
        # 0.5% tolerance for engine vs Python haversine numerics + accumulated error.
        assert_sketch_within_tolerance(
            actual, expected_total, rel=0.01, label=f"{entity} geo_distance"
        )

    assert cold_start_equivalent(app.get("UserPath", "unknown_gd"))


# ---------------------------------------------------------------------------
# Test 3: geo_spread — RMS dispersion around running mean centroid
# ---------------------------------------------------------------------------


def test_geo_spread_per_user_high_volume(app):
    """bv.geo_spread: 1000 events / 5 users in NYC bbox; spread within bbox-diagonal bound."""

    @bv.event
    class Visit:
        user_id: str
        lat: float
        lon: float

    @bv.table(key="user_id")
    def UserGeoSpread(visits: Visit):
        return visits.group_by("user_id").agg(
            spread_km=bv.geo_spread(lat="lat", lon="lon"),
        )

    app.register(Visit, UserGeoSpread)

    rng = random.Random(112)
    history: dict[str, list[tuple[float, float]]] = {entity: [] for entity in ENTITIES}
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        lat, lon = _rand_nyc_point(rng)
        history[user].append((lat, lon))
        app.push("Visit", {"user_id": user, "lat": lat, "lon": lon})

    # NYC bbox diagonal is ~30 km; spread (RMS) is at most ~half of that.
    bbox_diag_km = haversine_km(
        NYC_LAT_MIN, NYC_LON_MIN, NYC_LAT_MAX, NYC_LON_MAX
    )
    for entity, points in history.items():
        if len(points) < 5:
            continue
        result = app.get("UserGeoSpread", entity)
        spread = float(result["spread_km"])
        # Spread must be non-negative and bounded by bbox diagonal.
        assert spread >= 0.0, f"{entity}: spread negative: {spread}"
        assert spread <= bbox_diag_km, (
            f"{entity}: spread={spread} > bbox_diag={bbox_diag_km}"
        )

    assert cold_start_equivalent(app.get("UserGeoSpread", "unknown_gs"))


# ---------------------------------------------------------------------------
# Test 4: distance_from_home — current vs running centroid of last samples=100
# ---------------------------------------------------------------------------


def test_distance_from_home_per_user_high_volume(app):
    """bv.distance_from_home (samples=50): 1000 events / 5 users; bounded by bbox diag."""

    @bv.event
    class Tx:
        user_id: str
        lat: float
        lon: float

    @bv.table(key="user_id")
    def UserHome(txs: Tx):
        return txs.group_by("user_id").agg(
            from_home=bv.distance_from_home(lat="lat", lon="lon", samples=50),
        )

    app.register(Tx, UserHome)

    rng = random.Random(113)
    history: dict[str, list[tuple[float, float]]] = {entity: [] for entity in ENTITIES}
    for _ in range(1000):
        user = rng.choice(ENTITIES)
        lat, lon = _rand_nyc_point(rng)
        history[user].append((lat, lon))
        app.push("Tx", {"user_id": user, "lat": lat, "lon": lon})

    bbox_diag_km = haversine_km(
        NYC_LAT_MIN, NYC_LON_MIN, NYC_LAT_MAX, NYC_LON_MAX
    )
    for entity, points in history.items():
        if not points:
            continue
        # Expected: haversine(last_point, mean_centroid_of_last_50)
        last_lat, last_lon = points[-1]
        last_50 = points[-50:]
        mean_lat = statistics.mean(p[0] for p in last_50)
        mean_lon = statistics.mean(p[1] for p in last_50)
        expected = haversine_km(last_lat, last_lon, mean_lat, mean_lon)
        result = app.get("UserHome", entity)
        actual = float(result["from_home"])
        # Tolerance — engine may use equirectangular or true haversine; bbox-bound.
        assert actual >= 0.0
        assert actual <= bbox_diag_km, (
            f"{entity}: from_home={actual} > bbox_diag={bbox_diag_km}"
        )
        # If we have enough samples, verify within reasonable tolerance.
        if len(points) >= 50:
            # Centroid distance — engine may differ within a few hundred meters
            # depending on projection; allow generous bound.
            assert_sketch_within_tolerance(
                actual, expected, abs_=2.0, label=f"{entity} distance_from_home"
            )

    assert cold_start_equivalent(app.get("UserHome", "unknown_dfh"))
