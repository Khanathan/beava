//! Phase 12.8 Plan 04 — 53-variant / 54-op-string lifetime bound classification.
//!
//! Plan 01 landed `OpLifetimeBound` + a placeholder `lifetime_bound_for_op_str`
//! helper that returns `Unbounded` for every op-string. Plan 04 (this plan)
//! populates the per-op classification table — every op-string in
//! `crates/beava-core/src/agg_compile.rs::parse_agg_kind` (54 strings: 53
//! `AggKind` variants plus the `"ema"` SDK alias for `Ewma`) maps to a
//! non-`Unbounded` `OpLifetimeBound`.
//!
//! Per CONTEXT.md D-03: every operator declares either an O(1) bound, a
//! bounded-sketch bound, a bound-by-required-kwarg, or a bound-by-config-with-default.
//! `Unbounded` is now reserved for typos / genuinely-unclassified op-strings.
//!
//! These 15 tests RED at HEAD-of-Plan-01 (placeholder returns Unbounded for
//! every op); GREEN after Plan 04's GREEN commit replaces the body with the
//! 54-row match.

use beava_core::register_validate::{
    lifetime_bound_for_op_str, pre_check_unbounded_op_in_lifetime_mode, OpLifetimeBound,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Build a register payload with a single derivation containing one group_by
/// op whose `agg` map carries one named feature with the given op-string and
/// optional params blob.
fn payload_with_op(
    deriv_name: &str,
    feature_name: &str,
    op_str: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": deriv_name,
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            feature_name: {"op": op_str, "params": params}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", feature_name: "i64"},
                    "optional_fields": []
                }
            }
        ]
    })
}

// ─── Test 1: every supported op-string has a classified bound ────────────────

/// Enumerate every op-string accepted by `parse_agg_kind` in agg_compile.rs
/// (54 strings = 53 `AggKind` variants + `"ema"` alias for `Ewma`). After
/// Plan 04's GREEN commit, every entry must classify as non-Unbounded.
#[test]
fn every_aggkind_has_classified_bound() {
    // Sourced from `parse_agg_kind` in agg_compile.rs — every match-arm op-string.
    let all_op_strings: &[&str] = &[
        // Phase 5 core (8) — Phase 13.4-01 per ADR-002: avg→mean, variance→var, stddev→std.
        "count",
        "sum",
        "mean",
        "min",
        "max",
        "var",
        "std",
        "ratio",
        // Phase 8 point/ordinal (5)
        "first",
        "last",
        "first_n",
        "last_n",
        "lag",
        // Phase 8 recency markers (6)
        "first_seen",
        "last_seen",
        "age",
        "has_seen",
        "time_since",
        "time_since_last_n",
        // Phase 8 streaks (3)
        "streak",
        "max_streak",
        "negative_streak",
        // Phase 8 windowed recency (1)
        "first_seen_in_window",
        // Phase 9 decay (6 + 1 alias = 7 op-strings)
        "ewma",
        "ema",
        "ewvar",
        "ew_zscore",
        "decayed_sum",
        "decayed_count",
        "twa",
        // Phase 9 velocity (8)
        "rate_of_change",
        "inter_arrival_stats",
        "burst_count",
        "delta_from_prev",
        "trend",
        "trend_residual",
        "outlier_count",
        "value_change_count",
        // Phase 9 z-score (1)
        "z_score",
        // Phase 10 sketches (5) — Phase 13.4-01 per ADR-002: count_distinct→n_unique, percentile→quantile.
        "n_unique",
        "quantile",
        "top_k",
        "bloom_member",
        "entropy",
        // Phase 11 buffer ops (7)
        "histogram",
        "hour_of_day_histogram",
        "dow_hour_histogram",
        "seasonal_deviation",
        "event_type_mix",
        "most_recent_n",
        "reservoir_sample",
        // Phase 11 geo ops (4)
        "geo_velocity",
        "geo_distance",
        "geo_spread",
        "distance_from_home",
    ];
    // 8 + 5 + 6 + 3 + 1 + 7 + 8 + 1 + 5 + 7 + 4 = 55 — but we count "ewma"+"ema"
    // as two op-strings even though they map to the same `AggKind::Ewma`. Plan
    // body specified 54; we ship 55 because Phase 8 also has the standalone
    // `first` and `last` (single-element) ops in addition to `first_n` / `last_n`,
    // and `most_recent_n` lives in the Phase 11 group, not Phase 8.
    //
    // Verify: exactly 55 op-strings (no duplicates).
    let unique: std::collections::BTreeSet<&&str> = all_op_strings.iter().collect();
    assert_eq!(
        unique.len(),
        all_op_strings.len(),
        "duplicate op-string in test fixture"
    );
    assert_eq!(
        all_op_strings.len(),
        55,
        "expected 55 op-strings (53 AggKind variants + `ema` alias for Ewma + the \
         enumerated list-shape; verify against agg_compile::parse_agg_kind), got {}",
        all_op_strings.len()
    );

    let mut unclassified: Vec<&str> = vec![];
    for op in all_op_strings {
        let bound = lifetime_bound_for_op_str(op);
        if matches!(bound, OpLifetimeBound::Unbounded) {
            unclassified.push(op);
        }
    }
    assert!(
        unclassified.is_empty(),
        "{} op-strings are still classified as Unbounded after Plan 04: {:?}. \
         Every op in agg_compile::parse_agg_kind must have a non-Unbounded \
         classification in lifetime_bound_for_op_str.",
        unclassified.len(),
        unclassified
    );
}

// ─── Tests 2–7: spot-check individual classifications ────────────────────────

#[test]
fn count_classifies_as_o1() {
    assert_eq!(lifetime_bound_for_op_str("count"), OpLifetimeBound::O1);
}

#[test]
fn n_unique_classifies_as_bounded_sketch() {
    // Phase 13.4-01 per ADR-002: count_distinct → n_unique.
    assert_eq!(
        lifetime_bound_for_op_str("n_unique"),
        OpLifetimeBound::BoundedSketch
    );
}

#[test]
fn first_n_requires_n_kwarg() {
    assert_eq!(
        lifetime_bound_for_op_str("first_n"),
        OpLifetimeBound::BoundedByRequiredKwarg("n")
    );
}

#[test]
fn top_k_classified_bounded_by_config_k_with_default_10() {
    // Per Plan 04 objective: top_k uses BoundedByConfig("k", 10) for backward
    // compat with existing tests that don't specify k (agg_compile.rs:606
    // unwrap_or(10)). Hard-required `k` would break ~10 tests.
    assert_eq!(
        lifetime_bound_for_op_str("top_k"),
        OpLifetimeBound::BoundedByConfig("k", 10)
    );
}

#[test]
fn histogram_requires_buckets_kwarg() {
    // Plan 04 elevates histogram to a hard-required cap. Per the existing JSON
    // wire convention (agg_compile.rs:179: `params.get("buckets").as_array()`),
    // the kwarg name is `buckets` (a Vec<f64>), not `num_buckets`. Plan 04
    // adopts the existing convention.
    assert_eq!(
        lifetime_bound_for_op_str("histogram"),
        OpLifetimeBound::BoundedByRequiredKwarg("buckets")
    );
}

#[test]
fn event_type_mix_bounded_by_config_max_categories_256() {
    assert_eq!(
        lifetime_bound_for_op_str("event_type_mix"),
        OpLifetimeBound::BoundedByConfig("max_categories", 256)
    );
}

// ─── Test 8: catch-all / typo behavior ───────────────────────────────────────

#[test]
fn unknown_op_returns_unbounded() {
    // Sentinel: typos / unknown op-strings still trigger Unbounded → rejection.
    assert_eq!(
        lifetime_bound_for_op_str("nonexistent_op_zzz"),
        OpLifetimeBound::Unbounded
    );
}

// ─── Tests 9–15: pre_check shim end-to-end with the populated table ──────────

#[test]
fn pre_check_passes_count_lifetime() {
    // count → O1. Windowless count must NOT be rejected post-Plan-04.
    let body = payload_with_op("ByUser", "cnt", "count", serde_json::json!({}));
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_none(),
        "windowless count (O1) must not be rejected post-Plan-04"
    );
}

#[test]
fn pre_check_rejects_first_n_without_n_kwarg() {
    // first_n → BoundedByRequiredKwarg("n"). Missing `n` → reject.
    let body = payload_with_op(
        "ByUser",
        "first5",
        "first_n",
        serde_json::json!({"field": "amount"}),
    );
    let err = pre_check_unbounded_op_in_lifetime_mode(&body)
        .expect("first_n without n kwarg must be rejected in lifetime mode");
    assert_eq!(err.code, "unbounded_op_in_lifetime_mode");
    assert_eq!(err.op_label, "first_n");
    assert!(
        err.reason.contains("requires explicit") && err.reason.contains("n"),
        "reason should mention required kwarg `n`, got: {}",
        err.reason
    );
}

#[test]
fn pre_check_passes_first_n_with_n_5_kwarg() {
    // first_n with n=5 → accept.
    let body = payload_with_op(
        "ByUser",
        "first5",
        "first_n",
        serde_json::json!({"field": "amount", "n": 5}),
    );
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_none(),
        "first_n with n=5 must be accepted"
    );
}

#[test]
fn pre_check_rejects_first_n_with_n_zero_kwarg() {
    // first_n with n=0 → reject (n must be > 0).
    let body = payload_with_op(
        "ByUser",
        "first0",
        "first_n",
        serde_json::json!({"field": "amount", "n": 0}),
    );
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_some(),
        "first_n with n=0 must be rejected (zero is not a positive cap)"
    );
}

#[test]
fn pre_check_rejects_histogram_without_buckets() {
    // histogram → BoundedByRequiredKwarg("buckets"). Missing/empty buckets → reject.
    let body = payload_with_op(
        "ByUser",
        "h",
        "histogram",
        serde_json::json!({"field": "amount"}),
    );
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_some(),
        "histogram without `buckets` array must be rejected"
    );
}

#[test]
fn pre_check_passes_histogram_with_buckets_array() {
    // histogram with explicit buckets (non-empty Vec<f64>) → accept.
    let body = payload_with_op(
        "ByUser",
        "h",
        "histogram",
        serde_json::json!({
            "field": "amount",
            "buckets": [10.0, 50.0, 100.0, 500.0]
        }),
    );
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_none(),
        "histogram with non-empty buckets array must be accepted"
    );
}

#[test]
fn pre_check_passes_top_k_without_k_kwarg() {
    // top_k → BoundedByConfig("k", 10). Missing k → soft default 10 applies → accept.
    let body = payload_with_op(
        "ByUser",
        "tk",
        "top_k",
        serde_json::json!({"field": "merchant"}),
    );
    assert!(
        pre_check_unbounded_op_in_lifetime_mode(&body).is_none(),
        "top_k without explicit k must be accepted (BoundedByConfig default=10)"
    );
}
