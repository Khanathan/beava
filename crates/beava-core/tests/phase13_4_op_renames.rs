//! Phase 13.4 Plan 01 — ADR-002 op-rename verification.
//!
//! Asserts that the five Polars-style names parse to the expected `AggKind`
//! variants AND that the old SQL-prose names are rejected at the wire boundary.
//!
//! RED at HEAD-of-Plan-13.4-01 — `parse_agg_kind` still maps the OLD names
//! (`avg`, `variance`, `stddev`, `count_distinct`, `percentile`); GREEN after
//! Task 1.b's GREEN commit replaces them with the Polars names
//! (`mean`, `var`, `std`, `n_unique`, `quantile`).
//!
//! The Rust `AggKind::*` enum variants stay unchanged — only the JSON-side
//! op-string mapping flips. Per ADR-002 the old names are rejected at the
//! Rust wire boundary; the Python SDK keeps deprecation aliases through v0
//! per Phase 13.5.

use beava_core::agg_compile::parse_agg_kind;
use beava_core::agg_op::AggKind;
use beava_core::register_validate::lifetime_bound_for_op_str;

#[test]
fn new_names_parse_to_expected_aggkind() {
    assert_eq!(parse_agg_kind("mean"), Some(AggKind::Avg));
    assert_eq!(parse_agg_kind("var"), Some(AggKind::Variance));
    assert_eq!(parse_agg_kind("std"), Some(AggKind::StdDev));
    assert_eq!(parse_agg_kind("n_unique"), Some(AggKind::CountDistinct));
    assert_eq!(parse_agg_kind("quantile"), Some(AggKind::Percentile));
}

#[test]
fn old_names_are_rejected() {
    // Per ADR-002: Rust server is strict; old SQL-prose names are rejected.
    // Python SDK keeps deprecation aliases (Phase 13.5), but the wire boundary
    // here in `parse_agg_kind` does NOT.
    assert_eq!(parse_agg_kind("avg"), None);
    assert_eq!(parse_agg_kind("variance"), None);
    assert_eq!(parse_agg_kind("stddev"), None);
    assert_eq!(parse_agg_kind("count_distinct"), None);
    assert_eq!(parse_agg_kind("percentile"), None);
}

#[test]
fn non_renamed_ops_still_parse() {
    // Regression guard — the rename only touched five entries; the rest of
    // `parse_agg_kind` (count, sum, min, max, ewma, …) must still resolve.
    assert!(parse_agg_kind("count").is_some());
    assert!(parse_agg_kind("sum").is_some());
    assert!(parse_agg_kind("min").is_some());
    assert!(parse_agg_kind("max").is_some());
    assert!(parse_agg_kind("ewma").is_some());
    // bv.ema stays as alias per ADR-002 — not renamed.
    assert!(parse_agg_kind("ema").is_some());
    // ratio is in the same Phase 5 core block; ensure the lockstep edit didn't
    // accidentally remove or rename it.
    assert!(parse_agg_kind("ratio").is_some());
}

#[test]
fn lifetime_bound_agrees_on_new_names() {
    // V0-MEM-GOV-02 invariant: `parse_agg_kind` (agg_compile.rs) and
    // `lifetime_bound_for_op_str` (register_validate.rs) must agree on the
    // public op-string set. The architectural test
    // `crates/beava-core/tests/op_lifetime_bounds.rs` walks the catalogue;
    // this test additionally pins behaviour to the renamed names.
    //
    // `mean`, `var`, `std` are O1 (same bound as `count`).
    let count_bound = lifetime_bound_for_op_str("count");
    assert_eq!(lifetime_bound_for_op_str("mean"), count_bound);
    assert_eq!(lifetime_bound_for_op_str("var"), count_bound);
    assert_eq!(lifetime_bound_for_op_str("std"), count_bound);
    // `n_unique` is sketch-bounded (HLL); `quantile` is sketch-bounded
    // (DDSketch) — both are NOT the same variant as `count` (O1).
    assert_ne!(lifetime_bound_for_op_str("n_unique"), count_bound);
    assert_ne!(lifetime_bound_for_op_str("quantile"), count_bound);
}

#[test]
fn lifetime_bound_rejects_old_names() {
    // Old SQL-prose names should miss the lifetime-bound table after the
    // rename. The existing default for unrecognized ops in
    // `register_validate.rs::lifetime_bound_for_op_str` is
    // `OpLifetimeBound::Unbounded` (line 488 catch-all). Use a
    // definitely-unknown sentinel string to capture the default variant
    // without hard-coding the enum spelling here.
    let unknown = lifetime_bound_for_op_str("definitely_not_an_op_zzz");
    assert_eq!(lifetime_bound_for_op_str("avg"), unknown);
    assert_eq!(lifetime_bound_for_op_str("variance"), unknown);
    assert_eq!(lifetime_bound_for_op_str("stddev"), unknown);
    assert_eq!(lifetime_bound_for_op_str("count_distinct"), unknown);
    assert_eq!(lifetime_bound_for_op_str("percentile"), unknown);
}
