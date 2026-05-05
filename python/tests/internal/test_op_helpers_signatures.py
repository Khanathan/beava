"""Phase 13.5 Plan 04 red tests: 53 op helpers + ema alias signature regression.

Each helper returns an AggDescriptor with to_dict() rendering wire-shape JSON.
Family-by-family coverage. Failures here mean either the op helper is missing
or its signature drifted from docs/sdk-api/python.md § Operator catalog.
"""
from __future__ import annotations

import pytest

import beava as bv


# ── Core (8) ───────────────────────────────────────────────────────────────
def test_count_no_field() -> None:
    d = bv.count(window="1h")
    assert d.to_dict()["op"] == "count"


def test_sum_field_str_required() -> None:
    d = bv.sum("amount", window="1h")
    assert d.to_dict()["op"] == "sum"


def test_sum_field_expr_raises_RegistrationError() -> None:
    """Q1 Path B: bv.sum with _Expr arg is FORBIDDEN."""
    from beava._errors import RegistrationError

    with pytest.raises(RegistrationError, match="schema_mismatch|field"):
        bv.sum(bv.col("amount") * 2, window="1h")  # type: ignore[arg-type]


def test_mean_polars_name() -> None:
    d = bv.mean("amount", window="1h")
    assert d.to_dict()["op"] == "mean"


def test_min_op() -> None:
    assert bv.min("x", window="1h").to_dict()["op"] == "min"


def test_max_op() -> None:
    assert bv.max("x", window="1h").to_dict()["op"] == "max"


def test_var_op() -> None:
    assert bv.var("x", window="1h").to_dict()["op"] == "var"


def test_std_op() -> None:
    assert bv.std("x", window="1h").to_dict()["op"] == "std"


def test_ratio_op() -> None:
    assert bv.ratio(window="1h").to_dict()["op"] == "ratio"


# ── Sketch (5) ─────────────────────────────────────────────────────────────
def test_n_unique() -> None:
    assert bv.n_unique("user_id", window="1h").to_dict()["op"] == "n_unique"


def test_quantile() -> None:
    d = bv.quantile("amount", q=0.99, window="1h")
    out = d.to_dict()
    assert out["op"] == "quantile"
    assert out["q"] == 0.99


def test_top_k() -> None:
    d = bv.top_k("page", k=10, window="1h")
    assert d.to_dict()["op"] == "top_k"
    assert d.to_dict()["k"] == 10


def test_bloom_member() -> None:
    assert bv.bloom_member("ip", window="1h").to_dict()["op"] == "bloom_member"


def test_entropy() -> None:
    assert bv.entropy("page", window="1h").to_dict()["op"] == "entropy"


# ── Point/ordinal (5) ──────────────────────────────────────────────────────
def test_first() -> None:
    assert bv.first("amount").to_dict()["op"] == "first"


def test_last() -> None:
    assert bv.last("amount").to_dict()["op"] == "last"


def test_first_n() -> None:
    assert bv.first_n("amount", n=10).to_dict()["op"] == "first_n"


def test_last_n() -> None:
    assert bv.last_n("amount", n=10).to_dict()["op"] == "last_n"


def test_lag() -> None:
    assert bv.lag("amount", n=1).to_dict()["op"] == "lag"


# ── Recency (10) ───────────────────────────────────────────────────────────
def test_first_seen() -> None:
    assert bv.first_seen().to_dict()["op"] == "first_seen"


def test_last_seen() -> None:
    assert bv.last_seen().to_dict()["op"] == "last_seen"


def test_age() -> None:
    assert bv.age().to_dict()["op"] == "age"


def test_has_seen() -> None:
    assert bv.has_seen("ip").to_dict()["op"] == "has_seen"


def test_time_since() -> None:
    assert bv.time_since().to_dict()["op"] == "time_since"


def test_time_since_last_n() -> None:
    assert bv.time_since_last_n(n=5).to_dict()["op"] == "time_since_last_n"


def test_streak() -> None:
    assert bv.streak("flag").to_dict()["op"] == "streak"


def test_max_streak() -> None:
    assert bv.max_streak("flag").to_dict()["op"] == "max_streak"


def test_negative_streak() -> None:
    assert bv.negative_streak("flag").to_dict()["op"] == "negative_streak"


def test_first_seen_in_window() -> None:
    assert (
        bv.first_seen_in_window("ip", window="1h").to_dict()["op"]
        == "first_seen_in_window"
    )


# ── Decay (6, plus ema alias of ewma) ──────────────────────────────────────
def test_ewma() -> None:
    d = bv.ewma("amount", half_life="5m")
    assert d.to_dict()["op"] == "ewma"
    assert d.to_dict()["half_life"] == "5m"


def test_ema_alias() -> None:
    """ema is the alias of ewma per docs/sdk-api/python.md § Operator catalog."""
    d = bv.ema("amount", half_life="5m")
    assert d.to_dict()["op"] == "ewma"


def test_ewvar() -> None:
    assert bv.ewvar("amount", half_life="5m").to_dict()["op"] == "ewvar"


def test_ew_zscore() -> None:
    assert bv.ew_zscore("amount", half_life="5m").to_dict()["op"] == "ew_zscore"


def test_decayed_sum() -> None:
    assert bv.decayed_sum("amount", half_life="5m").to_dict()["op"] == "decayed_sum"


def test_decayed_count() -> None:
    assert bv.decayed_count(half_life="5m").to_dict()["op"] == "decayed_count"


def test_twa() -> None:
    assert bv.twa("amount", window="1h").to_dict()["op"] == "twa"


# ── Velocity (9) ───────────────────────────────────────────────────────────
def test_rate_of_change() -> None:
    assert (
        bv.rate_of_change("amount", window="1h").to_dict()["op"] == "rate_of_change"
    )


def test_inter_arrival_stats() -> None:
    assert (
        bv.inter_arrival_stats(window="1h").to_dict()["op"] == "inter_arrival_stats"
    )


def test_burst_count() -> None:
    assert (
        bv.burst_count(window="1h", sub_window="1m").to_dict()["op"] == "burst_count"
    )


def test_delta_from_prev() -> None:
    assert bv.delta_from_prev("amount").to_dict()["op"] == "delta_from_prev"


def test_trend() -> None:
    assert bv.trend("amount", window="1h").to_dict()["op"] == "trend"


def test_trend_residual() -> None:
    assert (
        bv.trend_residual("amount", window="1h").to_dict()["op"] == "trend_residual"
    )


def test_outlier_count() -> None:
    assert bv.outlier_count("amount", window="1h").to_dict()["op"] == "outlier_count"


def test_value_change_count() -> None:
    assert (
        bv.value_change_count("status", window="1h").to_dict()["op"]
        == "value_change_count"
    )


def test_z_score() -> None:
    assert bv.z_score("amount", baseline_window="1h").to_dict()["op"] == "z_score"


# ── Bounded buffers (7) ────────────────────────────────────────────────────
def test_histogram() -> None:
    # Plan 13.5.1-07a: histogram is lifetime-only with buckets: list[float]
    assert (
        bv.histogram("amount", buckets=[10.0, 50.0, 100.0]).to_dict()["op"]
        == "histogram"
    )


def test_hour_of_day_histogram() -> None:
    # Plan 13.5.1-07a: lifetime-only — no window kwarg
    assert (
        bv.hour_of_day_histogram().to_dict()["op"] == "hour_of_day_histogram"
    )


def test_dow_hour_histogram() -> None:
    # Plan 13.5.1-07a: lifetime-only — no window kwarg
    assert bv.dow_hour_histogram().to_dict()["op"] == "dow_hour_histogram"


def test_seasonal_deviation() -> None:
    # Plan 13.5.1-07a: lifetime-only — no window kwarg
    assert (
        bv.seasonal_deviation("amount").to_dict()["op"] == "seasonal_deviation"
    )


def test_event_type_mix() -> None:
    # Plan 13.5.1-07a: lifetime-only — no window kwarg; categories/max_categories soft-defaulted
    assert bv.event_type_mix("type").to_dict()["op"] == "event_type_mix"


def test_most_recent_n() -> None:
    assert bv.most_recent_n("amount", n=10).to_dict()["op"] == "most_recent_n"


def test_reservoir_sample() -> None:
    # Plan 13.5.1-07a: kwarg renamed k → samples per docs/operators/buffer-geo/reservoir_sample.md
    assert (
        bv.reservoir_sample("amount", samples=100).to_dict()["op"]
        == "reservoir_sample"
    )


# ── Geo (4) ────────────────────────────────────────────────────────────────
def test_geo_velocity() -> None:
    assert (
        bv.geo_velocity("lat", "lon", window="1h").to_dict()["op"] == "geo_velocity"
    )


def test_geo_distance() -> None:
    assert bv.geo_distance("lat", "lon").to_dict()["op"] == "geo_distance"


def test_geo_spread() -> None:
    assert bv.geo_spread("lat", "lon", window="1h").to_dict()["op"] == "geo_spread"


def test_distance_from_home() -> None:
    assert (
        bv.distance_from_home("lat", "lon", window="30d").to_dict()["op"]
        == "distance_from_home"
    )
