//! Golden watermark regression test (Phase 49 / D-04 mandate).
//!
//! At N=1, Wave 1 behavior must be IDENTICAL to pre-Wave-1 WatermarkTracker behavior
//! for all observe/query sequences. This test replicates the exact sequences from
//! tests/test_watermarks.rs and tests/test_watermarks_per_stream_lateness.rs
//! against WatermarkState.
//!
//! If this test fails, the WatermarkTracker relocation (Plan 49-03) introduced a regression.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use beava::shard::watermark::WatermarkState;

fn sec(s: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(s)
}

/// Replicate test_watermarks.rs::watermark_tracks_max_minus_5s
/// At N=1 the observed_max + watermark sequence must be identical.
#[test]
fn n1_identical_to_pre_wave1_max_minus_5s() {
    let mut wm = WatermarkState::new();
    wm.observe("s", sec(100));
    wm.observe("s", sec(110));
    assert_eq!(wm.observed_max("s"), Some(sec(110)));
    // Default lateness = 5s → watermark = 110 - 5 = 105
    assert_eq!(wm.watermark("s"), Some(sec(105)));
}

/// Replicate test_watermarks.rs::watermark_absent_for_fresh_stream
#[test]
fn n1_identical_fresh_stream_returns_none() {
    let wm = WatermarkState::new();
    assert_eq!(wm.watermark("never_seen"), None);
    assert_eq!(wm.observed_max("never_seen"), None);
}

/// Replicate test_watermarks.rs::watermark_underflow_clamps_to_epoch
#[test]
fn n1_identical_underflow_clamps_to_epoch() {
    let mut wm = WatermarkState::new();
    wm.observe("s", sec(3)); // 3s − 5s lateness = underflow → clamp to UNIX_EPOCH
    let wm_t = wm.watermark("s").unwrap();
    assert!(wm_t >= UNIX_EPOCH, "underflow must clamp to UNIX_EPOCH");
}

/// Monotonicity: later observe does not regress watermark
#[test]
fn n1_monotonic_observe_sequence() {
    let mut wm = WatermarkState::new();
    wm.observe("s", sec(200));
    wm.observe("s", sec(150)); // older → no regression
    assert_eq!(wm.observed_max("s"), Some(sec(200)));
}

/// Multi-stream isolation: observing stream A does not affect stream B
#[test]
fn n1_multi_stream_isolation() {
    let mut wm = WatermarkState::new();
    wm.observe("a", sec(100));
    wm.observe("b", sec(200));
    assert_eq!(wm.observed_max("a"), Some(sec(100)));
    assert_eq!(wm.observed_max("b"), Some(sec(200)));
}

/// Per-stream lateness override (replicates test_watermarks_per_stream_lateness.rs)
#[test]
fn n1_per_stream_lateness_override() {
    let mut wm = WatermarkState::new();
    wm.set_lateness("s", Duration::from_secs(10));
    wm.observe("s", sec(100));
    // Lateness 10s → watermark = 100 - 10 = 90
    assert_eq!(wm.watermark("s"), Some(sec(90)));
}

/// Join watermark = min of both sides (replicate join test)
#[test]
fn n1_join_watermark_is_min() {
    let mut wm = WatermarkState::new();
    wm.observe("left", sec(100));
    wm.observe("right", sec(80));
    wm.merge_join("left", "right", "join_out");
    assert_eq!(wm.observed_max("join_out"), Some(sec(80)));
}

/// Propagate from source to derived stream
#[test]
fn n1_propagate_from_advances_derived() {
    let mut wm = WatermarkState::new();
    wm.observe("src", sec(300));
    wm.propagate_from("src", "derived");
    assert_eq!(wm.observed_max("derived"), Some(sec(300)));
}
