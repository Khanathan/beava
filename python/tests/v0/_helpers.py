"""Shared helpers for v0 user-facing operator integration tests.

Re-exports ``_engine_available`` from ``conftest`` so test modules can
construct their module-level ``pytestmark = pytest.mark.skipif(...)`` line
without importing the conftest module directly.

Provides:
  - ENTITIES — default 5-entity pool (alice/bob/carol/dave/eve)
  - gen_events — deterministic event-stream generator
  - compute_expected_per_entity — Python brute-force ground-truth computation
  - assert_sketch_within_tolerance — sketch-op tolerance helper
  - assert_top_k_order_preserved — top_k ranking helper
  - haversine_km — great-circle distance for geo tests (mean Earth radius 6371 km)
  - cold_start_equivalent — re-export from conftest for convenience
"""
from __future__ import annotations

import math
import random
from typing import Any, Callable, Iterable

from .conftest import _engine_available, cold_start_equivalent

__all__ = [
    "ENTITIES",
    "_engine_available",
    "cold_start_equivalent",
    "gen_events",
    "compute_expected_per_entity",
    "assert_sketch_within_tolerance",
    "assert_top_k_order_preserved",
    "haversine_km",
    "EARTH_RADIUS_KM",
]


# Default 5-entity pool for tests that don't override it.
ENTITIES: list[str] = ["alice", "bob", "carol", "dave", "eve"]

# Mean Earth radius (km) — matches `crates/beava-core/src/agg_geo.rs::haversine_km`
# (per CONTEXT D-02). Used by both the engine and the expected-value computation
# so tests don't drift from the server's radius.
EARTH_RADIUS_KM: float = 6371.0


def gen_events(
    rng: random.Random,
    n: int,
    entity_pool: list[str],
    payload_fn: Callable[[int, str], dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    """Generate a deterministic event stream of (entity, payload) pairs.

    Args:
        rng: Seeded ``random.Random`` instance — drives entity selection.
        n: Number of events to generate.
        entity_pool: List of entity IDs; each event gets one via rng.choice.
        payload_fn: Callable ``(event_index, entity_id) -> payload_dict``.

    Returns:
        List of ``(entity_id, payload)`` tuples of length n.
    """
    events: list[tuple[str, dict[str, Any]]] = []
    for i in range(n):
        entity = rng.choice(entity_pool)
        events.append((entity, payload_fn(i, entity)))
    return events


def compute_expected_per_entity(
    events: Iterable[tuple[str, dict[str, Any]]],
    accumulator_init: Callable[[], Any],
    update: Callable[[Any, dict[str, Any]], Any],
) -> dict[str, Any]:
    """Fold *events* into per-entity accumulators using *update* / *accumulator_init*.

    Args:
        events: Iterable of ``(entity_id, payload)`` tuples.
        accumulator_init: Zero-arg callable that returns a fresh accumulator
                          (e.g. ``lambda: 0``, ``lambda: []``, ``lambda: {}``).
        update: Callable ``(acc, payload) -> new_acc``. May mutate-and-return
                or return a new value.

    Returns:
        ``{entity_id: accumulator}`` map. Entities never seen in *events* are
        absent from the result (mirroring app.get returning {} for cold-start).
    """
    out: dict[str, Any] = {}
    for entity, payload in events:
        if entity not in out:
            out[entity] = accumulator_init()
        out[entity] = update(out[entity], payload)
    return out


def assert_sketch_within_tolerance(
    actual: float,
    expected: float,
    *,
    rel: float | None = None,
    abs_: float | None = None,
    label: str = "",
) -> None:
    """Assert *actual* is within *rel* fraction OR *abs_* absolute of *expected*.

    Args:
        actual: Observed sketch value.
        expected: Ground-truth value (computed in test).
        rel: Optional relative tolerance (e.g. 0.05 for 5%).
        abs_: Optional absolute tolerance (e.g. 2.0 for ±2 percentile points).
        label: Optional context label for the assertion message.

    Raises:
        AssertionError if neither tolerance bound is satisfied.
    """
    if rel is None and abs_ is None:
        raise ValueError("must supply at least one of rel= or abs_=")
    diff = abs(actual - expected)
    rel_ok = rel is not None and (
        expected == 0.0 or diff / abs(expected) <= rel
    )
    abs_ok = abs_ is not None and diff <= abs_
    if not (rel_ok or abs_ok):
        prefix = f"{label}: " if label else ""
        raise AssertionError(
            f"{prefix}expected={expected!r}, actual={actual!r}, diff={diff!r}, "
            f"rel-tol={rel!r}, abs-tol={abs_!r} — neither bound satisfied"
        )


def assert_top_k_order_preserved(
    actual_top_k: list[Any],
    expected_ranking: list[Any],
    *,
    label: str = "",
) -> None:
    """Assert *actual_top_k* preserves the top-of-ranking from *expected_ranking*.

    SpaceSaving sketches reliably preserve the very top members but may
    perturb the tail. This helper checks that the top-K members ARE present
    in *actual_top_k* (set inclusion) and that the #1 item is correctly
    ranked first.

    Args:
        actual_top_k: Top-K result list (most-frequent first), from app.get.
        expected_ranking: Ground-truth top-K list ranked by frequency, longest
                          to shortest tail (computed in test).
        label: Optional context label.

    Raises:
        AssertionError if the top item differs or any of the top members
        are missing from *actual_top_k*.
    """
    prefix = f"{label}: " if label else ""
    if not actual_top_k:
        raise AssertionError(f"{prefix}actual top-K is empty; expected {expected_ranking!r}")
    if not expected_ranking:
        return  # Nothing to check.
    if actual_top_k[0] != expected_ranking[0]:
        raise AssertionError(
            f"{prefix}top item differs: actual[0]={actual_top_k[0]!r}, "
            f"expected[0]={expected_ranking[0]!r}"
        )
    actual_set = set(actual_top_k)
    expected_set = set(expected_ranking)
    missing = expected_set - actual_set
    if missing:
        raise AssertionError(
            f"{prefix}expected top-K members missing from actual: {missing!r} "
            f"(actual={actual_top_k!r}, expected={expected_ranking!r})"
        )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two ``(lat, lon)`` points.

    Uses the same formula as the engine's ``crates/beava-core/src/agg_geo.rs::
    haversine_km`` (mean Earth radius 6371 km, spherical-Earth approximation,
    accurate to ~0.5%). Lat/lon in decimal degrees.
    """
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2.0) ** 2
    )
    c = 2.0 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_KM * c
