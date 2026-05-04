"""Beava Python SDK — Phase 13.5 Plan 01 minimal foundation.

The five-module surface (`__init__`, `_wire`, `_transport`, `_errors`, `_embed`)
is intentionally bare after Plan 01 deletes the stale pre-Phase-13.0 surface.

Plans 02-07 re-populate the public namespace:
  - Plan 02: bv.App core + URL-scheme dispatch + test_mode kwarg
  - Plan 03: pipeline DSL — bv.col, bv.lit, @bv.event, @bv.table
  - Plan 04: 53 op helpers + ADR-002 deprecation aliases
  - Plan 05: PEP 563 fix + bv.demo loader + beava.test/cli submodules
  - Plan 06: in-package demo datasets
  - Plan 07: beava.test fixtures + replay + MockApp

v0 ships events-only per `project_v0_events_only_scope` (locked 2026-04-30,
ADR-001 partial overturn 2026-05-03 revives @bv.table for aggregation-output).
"""

from __future__ import annotations

# Re-exports from kept modules only:
from beava._agg import (  # noqa: F401
    age,
    avg,
    bloom_member,
    burst_count,
    count,
    count_distinct,
    decayed_count,
    decayed_sum,
    delta_from_prev,
    distance_from_home,
    dow_hour_histogram,
    ema,
    entropy,
    event_type_mix,
    ew_zscore,
    ewma,
    ewvar,
    first,
    first_n,
    first_seen,
    first_seen_in_window,
    geo_distance,
    geo_spread,
    geo_velocity,
    has_seen,
    histogram,
    hour_of_day_histogram,
    inter_arrival_stats,
    lag,
    last,
    last_n,
    last_seen,
    max,
    max_streak,
    mean,
    min,
    most_recent_n,
    n_unique,
    negative_streak,
    outlier_count,
    percentile,
    quantile,
    rate_of_change,
    ratio,
    reservoir_sample,
    seasonal_deviation,
    std,
    stddev,
    streak,
    sum,
    time_since,
    time_since_last_n,
    top_k,
    trend,
    trend_residual,
    twa,
    value_change_count,
    var,
    variance,
    z_score,
)
from beava._app import App  # noqa: F401
from beava._col import col, lit  # noqa: F401
from beava._errors import (  # noqa: F401
    BinaryNotFoundError,
    RegistrationError,
    ValidationError,
)
from beava._events import event  # noqa: F401
from beava._table import table  # noqa: F401

__all__ = [
    "App",
    "RegistrationError",
    "BinaryNotFoundError",
    "ValidationError",
    "col",
    "lit",
    "event",
    "table",
    # core (8)
    "count",
    "sum",
    "mean",
    "min",
    "max",
    "var",
    "std",
    "ratio",
    # sketch (5)
    "n_unique",
    "quantile",
    "top_k",
    "bloom_member",
    "entropy",
    # point/ordinal (5)
    "first",
    "last",
    "first_n",
    "last_n",
    "lag",
    # recency (10)
    "first_seen",
    "last_seen",
    "age",
    "has_seen",
    "time_since",
    "time_since_last_n",
    "streak",
    "max_streak",
    "negative_streak",
    "first_seen_in_window",
    # decay (6 + ema alias)
    "ewma",
    "ema",
    "ewvar",
    "ew_zscore",
    "decayed_sum",
    "decayed_count",
    "twa",
    # velocity (9)
    "rate_of_change",
    "inter_arrival_stats",
    "burst_count",
    "delta_from_prev",
    "trend",
    "trend_residual",
    "outlier_count",
    "value_change_count",
    "z_score",
    # bounded buffers (7)
    "histogram",
    "hour_of_day_histogram",
    "dow_hour_histogram",
    "seasonal_deviation",
    "event_type_mix",
    "most_recent_n",
    "reservoir_sample",
    # geo (4)
    "geo_velocity",
    "geo_distance",
    "geo_spread",
    "distance_from_home",
    # ADR-002 deprecation aliases (5)
    "avg",
    "variance",
    "stddev",
    "count_distinct",
    "percentile",
]
