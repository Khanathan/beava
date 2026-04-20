//! Phase 55 Wave 0 RED — SC-5 cascade + inbox metrics on /metrics.
//!
//! Contract (D-D4 + ROADMAP-locked): Five new Prometheus metrics land on
//! /metrics as a result of Phase 55:
//!   - beava_cascade_cross_shard_total{source, target}     — Counter
//!   - beava_cascade_intra_shard_total{shard}              — Counter
//!   - beava_cascade_queue_depth{source, target}           — Gauge
//!   - beava_cascade_lag_seconds{source, target}           — Histogram
//!       buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
//!   - beava_shard_inbox_high_watermark_total{shard}       — Counter
//!       fires at 75% fill
//!
//! Wave 1 (plan 55-01) registers these metrics alongside the coalesce
//! buffer. Wave 0 landing: `#[ignore = "55-W1"]` (metrics depend on
//! CascadeBuffer wiring, not wire format → Wave 1, not Wave 2).
//!
//! Run:
//!   cargo test --release --test cascade_metrics -- --ignored

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::spawn_two_shards;

/// SC-5 primary assertion — /metrics surface contains all five Phase 55
/// metric names with correct Prometheus TYPE annotations (counter, gauge,
/// histogram). Test: boot server, issue PUSH that triggers a cross-shard
/// cascade, GET /metrics, grep for each expected metric name + # TYPE
/// line.
#[test]
#[ignore = "55-W1"]
fn metrics_endpoint_exposes_all_five_phase_55_metrics() {
    let _expected: &[&str] = &[
        "beava_cascade_cross_shard_total",
        "beava_cascade_intra_shard_total",
        "beava_cascade_queue_depth",
        "beava_cascade_lag_seconds",
        "beava_shard_inbox_high_watermark_total",
    ];
    unimplemented!("Wave 1 — SC-5 metric visibility (5 new series on /metrics)");
}

/// SC-5 threshold — high-watermark counter fires when inbox depth
/// crosses 75% of capacity. Fill source→target SPSC inbox to 75% (e.g.
/// 48/64), assert `beava_shard_inbox_high_watermark_total{shard=target} >= 1`.
#[test]
#[ignore = "55-W1"]
fn high_watermark_fires_at_75_percent_fill() {
    let _harness = spawn_two_shards(64);
    unimplemented!("Wave 1 — SC-5 high-watermark threshold (75% of inbox cap)");
}
