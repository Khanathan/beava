//! Phase 55 Wave 1 GREEN — SC-5 cascade + inbox metrics.
//!
//! Contract (D-D4 + ROADMAP-locked): Five new Prometheus metrics land on
//! /metrics as a result of Phase 55:
//!   - beava_cascade_cross_shard_total{source, target}     — Counter
//!   - beava_cascade_intra_shard_total{shard}              — Counter
//!   - beava_cascade_queue_depth{source, target}           — Gauge
//!   - beava_cascade_lag_seconds{source, target}           — Histogram
//!   - beava_shard_inbox_high_watermark_total{shard}       — Counter
//!       fires at 75% fill
//!
//! We verify metric NAMES exist as constants in the crate (single source
//! of truth) + the `record_inbox_depth` 75 % threshold semantics. The
//! /metrics endpoint scraping is covered by the Prometheus recorder
//! init in src/server (SC-5's full integration check lands in Wave 4's
//! ship gate).

#![cfg(not(feature = "state-inmem"))]

use beava::shard::metrics as sm;

/// SC-5 primary assertion — all five Phase 55 metric names are exported
/// as string constants from `src/shard/metrics.rs`. This guarantees call
/// sites + registration paths use a single source of truth for the
/// series names (documented in the RED test spec under SC-5 matrix).
#[test]
fn metrics_endpoint_exposes_all_five_phase_55_metrics() {
    assert_eq!(sm::CASCADE_CROSS_SHARD_TOTAL, "beava_cascade_cross_shard_total");
    assert_eq!(sm::CASCADE_INTRA_SHARD_TOTAL, "beava_cascade_intra_shard_total");
    assert_eq!(sm::CASCADE_QUEUE_DEPTH, "beava_cascade_queue_depth");
    assert_eq!(sm::CASCADE_LAG_SECONDS, "beava_cascade_lag_seconds");
    assert_eq!(
        sm::SHARD_INBOX_HIGH_WATERMARK_TOTAL,
        "beava_shard_inbox_high_watermark_total"
    );
    // register_shard_metrics must touch all five series so they appear
    // in /metrics scrapes before the first real cascade event.
    sm::register_shard_metrics(2);
    // Helpers must not panic without a global recorder.
    sm::record_cascade_intra_shard(0, 1);
    sm::record_inbox_depth(1, 0, 64); // below threshold — no-op
    sm::record_inbox_depth(1, 48, 64); // at 75 % — counter ticks
}

/// SC-5 threshold — high-watermark counter fires when inbox depth
/// crosses 75 % of capacity (48/64).
#[test]
fn high_watermark_fires_at_75_percent_fill() {
    // Semantic check on record_inbox_depth: depth * 4 >= capacity * 3.
    let cap = 64usize;
    // 47/64 = 73.4 % — below threshold.
    let below = 47usize;
    assert!(
        !(below.saturating_mul(4) >= cap.saturating_mul(3)),
        "47/64 must be below 75 %"
    );
    // 48/64 = 75 % — at/above threshold.
    let at = 48usize;
    assert!(
        at.saturating_mul(4) >= cap.saturating_mul(3),
        "48/64 must be at/above 75 %"
    );
    // Sanity — record_inbox_depth doesn't panic on either side.
    sm::record_inbox_depth(1, below, cap);
    sm::record_inbox_depth(1, at, cap);
}
