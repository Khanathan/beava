"""Phase 13.5 Plan 05: module-structure regression tests per D-04."""
from __future__ import annotations

import importlib


def test_beava_top_level_imports() -> None:
    import beava

    assert hasattr(beava, "App")
    assert hasattr(beava, "event")
    assert hasattr(beava, "table")
    assert hasattr(beava, "col")
    assert hasattr(beava, "lit")
    assert hasattr(beava, "demo")  # Plan 05


def test_beava_test_submodule() -> None:
    """``beava.test`` must be importable per D-04."""
    importlib.import_module("beava.test")


def test_beava_cli_submodule() -> None:
    """``beava.cli`` must be importable per D-04."""
    importlib.import_module("beava.cli")


def test_53_op_helpers_in_namespace() -> None:
    import beava

    expected_53 = {
        # core
        "count", "sum", "mean", "min", "max", "var", "std", "ratio",
        # sketch
        "n_unique", "quantile", "top_k", "bloom_member", "entropy",
        # point/ordinal
        "first", "last", "first_n", "last_n", "lag",
        # recency
        "first_seen", "last_seen", "age", "has_seen", "time_since",
        "time_since_last_n", "streak", "max_streak", "negative_streak",
        "first_seen_in_window",
        # decay (+ ema alias)
        "ewma", "ema", "ewvar", "ew_zscore", "decayed_sum", "decayed_count", "twa",
        # velocity
        "rate_of_change", "inter_arrival_stats", "burst_count", "delta_from_prev",
        "trend", "trend_residual", "outlier_count", "value_change_count", "z_score",
        # bounded buffers
        "histogram", "hour_of_day_histogram", "dow_hour_histogram",
        "seasonal_deviation", "event_type_mix", "most_recent_n", "reservoir_sample",
        # geo
        "geo_velocity", "geo_distance", "geo_spread", "distance_from_home",
    }
    missing = expected_53 - set(dir(beava))
    assert not missing, f"Missing helpers in bv namespace: {missing}"
