//! Plan 22-03 integration tests for the hybrid PercentileOp.
//!
//! Covers: exact-mode correctness, transition at the 257th event, sketch
//! mode accuracy, telemetry reporting, ring-buffer retraction on bucket
//! expiry, decrement-saturation on underflow.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tally::engine::operators::{Operator, PercentileOp, PERCENTILE_EXACT_THRESHOLD};
use tally::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

#[test]
fn exact_mode_exact_quantile_up_to_threshold() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 1..=200 {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "exact");
    // p50 of 1..=200 (linear interpolation): index = 0.5*199 = 99.5
    // = values[99]*0.5 + values[100]*0.5 = 100*0.5 + 101*0.5 = 100.5
    match op.read(t) {
        FeatureValue::Float(f) => assert!((f - 100.5).abs() < 1e-9, "got {}", f),
        other => panic!("{:?}", other),
    }
}

#[test]
fn transition_fires_on_event_257_not_256() {
    let mut op = PercentileOp::new(
        "v",
        0.9,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 1..=PERCENTILE_EXACT_THRESHOLD {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "exact", "at 256 events, still exact");
    op.push(
        &json!({ "v": (PERCENTILE_EXACT_THRESHOLD + 1) as f64 }),
        None,
        t,
    )
    .unwrap();
    assert_eq!(op.mode_name(), "sketch", "event 257 crosses threshold");
}

#[test]
fn sketch_mode_quantile_within_alpha() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 1..=10_000 {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let q50 = match op.read(t) {
        FeatureValue::Float(f) => f,
        other => panic!("{:?}", other),
    };
    // Ground truth (integer-median of 1..=10_000 with numpy indexing): ~5000.5
    let err = (q50 - 5000.5).abs() / 5000.5;
    assert!(err < 0.05, "quantile error {} too large for α=0.01", err);
}

#[test]
fn hybrid_telemetry_reports_mode_and_alpha() {
    let mut op = PercentileOp::new(
        "v",
        0.9,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Exact telemetry before transition.
    op.push(&json!({ "v": 1.0 }), None, t).unwrap();
    let t1 = op.hybrid_telemetry().expect("telemetry");
    assert_eq!(t1.mode, "exact");
    assert_eq!(t1.op, "percentile");
    assert_eq!(t1.transition_at, PERCENTILE_EXACT_THRESHOLD);
    assert!(t1.sketch_alpha_current.is_none());
    assert!(t1.memory_bytes > 0);
    // Force transition.
    for i in 0..500 {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    let t2 = op.hybrid_telemetry().expect("telemetry after transition");
    assert_eq!(t2.mode, "sketch");
    assert!(t2.sketch_alpha_current.unwrap() >= 0.01);
}

#[test]
fn bucket_retraction_drops_expired_values_in_sketch_mode() {
    // Window = 2 buckets of 60s each = 120s window.
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(120),
        Duration::from_secs(60),
        false,
    );
    let t0 = ts(1_000_000);
    // Burn through 300 events to force sketch mode — all in the first bucket.
    for i in 0..300 {
        op.push(&json!({ "v": i as f64 }), None, t0).unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    // Advance past the window → every retention bucket drains into
    // sketch.decrement(). Total count should drop to 0.
    let t_far = t0 + Duration::from_secs(10_000);
    let result = op.read(t_far);
    assert_eq!(
        result,
        FeatureValue::Missing,
        "fully expired sketch should report Missing (NaN quantile)"
    );
}

#[test]
fn decrement_saturates_on_retract_before_insert() {
    // A pathological stream: push ⟨v=5⟩ 300 times to transition, then push
    // a bunch more different values, then advance ~30 buckets to force
    // retraction far past any live value. Sketch should empty cleanly
    // without panicking (tests T-22-02 threat mitigation).
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(60),
        Duration::from_secs(10),
        false,
    );
    let t0 = ts(1_000_000);
    for i in 0..300 {
        op.push(&json!({ "v": i as f64 }), None, t0).unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    // Jump forward 10 full windows.
    let t1 = t0 + Duration::from_secs(60 * 10);
    let _ = op.read(t1);
    assert_eq!(op.read(t1), FeatureValue::Missing);
}

#[test]
fn empty_operator_returns_missing() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(60),
        Duration::from_secs(10),
        false,
    );
    assert_eq!(op.read(ts(1_000)), FeatureValue::Missing);
    let t = op.hybrid_telemetry().expect("telemetry");
    assert_eq!(t.mode, "exact");
    assert_eq!(t.exact_count, 0);
}

#[test]
fn transition_preserves_distribution_within_alpha() {
    // Push 300 known values; the quantile right before transition should
    // match the quantile right after, within sketch alpha.
    let mut op = PercentileOp::new(
        "v",
        0.9,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 0..PERCENTILE_EXACT_THRESHOLD {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    let exact_q = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    assert_eq!(op.mode_name(), "exact");
    // One more push → sketch mode.
    op.push(&json!({ "v": 256.0 }), None, t).unwrap();
    assert_eq!(op.mode_name(), "sketch");
    let sketch_q = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    let err = (exact_q - sketch_q).abs() / exact_q.max(1.0);
    assert!(
        err < 0.05,
        "transition shifted p90 by {}% (exact={}, sketch={})",
        err * 100.0,
        exact_q,
        sketch_q
    );
}
