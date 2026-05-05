"""Plan 13.5.1-07a — operator-catalogue signature reconciliation.

Per-family unit-test contract for the 16 helpers in `python/beava/_agg.py`
that drift from the v0 acceptance test call shapes (Plan 05 deficit
Category 1) and from `docs/operators/<family>/<op>.md`. This suite is the
RED→GREEN gate per CLAUDE.md §Conventions §TDD Discipline item #1; the
v0 acceptance integration smoke remains the additive engine-dependent gate
per item #4.

Family layout mirrors the per-family table in 13.5.1-07a-PLAN.md:

- ``test_buffer_family``        — histogram, hour_of_day_histogram,
                                  dow_hour_histogram, seasonal_deviation,
                                  event_type_mix, reservoir_sample
- ``test_geo_family``           — geo_velocity, geo_distance, geo_spread,
                                  distance_from_home
- ``test_point_ordinal_family`` — first_n, last_n (where= parity)
- ``test_recency_family``       — streak, max_streak, negative_streak,
                                  has_seen, first_seen_in_window,
                                  time_since, time_since_last_n
- ``test_velocity_family``      — outlier_count (sigma rename),
                                  value_change_count (regression guard)
"""
from __future__ import annotations

import beava as bv


# ---------------------------------------------------------------------------
# Family A — Buffer
# ---------------------------------------------------------------------------


def test_buffer_family() -> None:
    """6 buffer ops match docs/operators/buffer-geo/<op>.md signature blocks."""

    # bv.histogram — buckets is list[float], no window=, lifetime-only
    h = bv.histogram("amount", buckets=[10.0, 50.0, 100.0, 500.0])
    d = h.to_dict()
    assert d["op"] == "histogram"
    assert d["field"] == "amount"
    assert d["buckets"] == [10.0, 50.0, 100.0, 500.0]
    assert "window" not in d, f"histogram is lifetime-only; got window={d.get('window')!r}"

    # bv.hour_of_day_histogram — no window=
    h = bv.hour_of_day_histogram()
    d = h.to_dict()
    assert d["op"] == "hour_of_day_histogram"
    assert "window" not in d

    # bv.dow_hour_histogram — no window=
    h = bv.dow_hour_histogram()
    d = h.to_dict()
    assert d["op"] == "dow_hour_histogram"
    assert "window" not in d

    # bv.seasonal_deviation — no window=
    h = bv.seasonal_deviation("amount")
    d = h.to_dict()
    assert d["op"] == "seasonal_deviation"
    assert d["field"] == "amount"
    assert "window" not in d

    # bv.event_type_mix — no window=, accepts categories + max_categories
    h = bv.event_type_mix("kind")
    d = h.to_dict()
    assert d["op"] == "event_type_mix"
    assert d["field"] == "kind"
    assert "window" not in d

    h = bv.event_type_mix("kind", categories=["p2p", "card"], max_categories=10)
    d = h.to_dict()
    assert d["categories"] == ["p2p", "card"]
    assert d["max_categories"] == 10

    # bv.reservoir_sample — samples= (NOT k=)
    h = bv.reservoir_sample("amount", samples=100)
    d = h.to_dict()
    assert d["op"] == "reservoir_sample"
    assert d["field"] == "amount"
    assert d["samples"] == 100, f"reservoir_sample expects samples=, got d={d!r}"
    assert "k" not in d

    # bv.reservoir_sample — accepts where= per docs parity
    h = bv.reservoir_sample("amount", samples=100, where=bv.col("kind") == "p2p")
    assert h.where is not None
