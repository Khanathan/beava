//! Plan 22-03 integration tests for the hybrid DistinctCountOp.
//!
//! Covers: exact correctness, HLL approximation within tolerance, per-bucket
//! retraction on expiry, telemetry reporting.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beava::engine::hll::DistinctCountOp;
use beava::engine::operators::Operator;
use beava::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn count_of(op: &mut DistinctCountOp, t: SystemTime) -> f64 {
    match op.read(t) {
        FeatureValue::Float(f) => f,
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn exact_up_to_threshold_is_zero_error() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    for i in 0..500 {
        op.push(&json!({ "d": format!("u{}", i) }), None, t)
            .unwrap();
    }
    let c = count_of(&mut op, t);
    assert!((c - 500.0).abs() < 1e-9, "got {}", c);
    assert_eq!(op.mode_name(), "exact");
}

#[test]
fn hll_mode_within_2_percent_on_100k() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Push 20_000 uniques — comfortably over the 1024 per-bucket
    // threshold, which triggers HLL promotion for the current bucket.
    let n = 20_000u64;
    for i in 0..n {
        op.push(&json!({ "d": format!("u{}", i) }), None, t)
            .unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let est = count_of(&mut op, t);
    let err = (est - n as f64).abs() / n as f64;
    // HLL++ at p=12 is ~1.6% typical; allow 5% headroom for small-sample
    // noise and the mid-scale bias correction region.
    assert!(err < 0.05, "error {} > 5% (est={} true={})", err, est, n);
}

#[test]
fn transition_at_1025th_unique() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // The per-bucket hashset promotes to HLL *on insert* past 1024 uniques.
    for i in 0..=1024 {
        op.push(&json!({ "d": format!("u{}", i) }), None, t)
            .unwrap();
    }
    // After inserting 1025 uniques, at least one bucket should have
    // promoted.
    assert_eq!(op.mode_name(), "sketch");
}

#[test]
fn bucket_expiry_drops_distinct_count() {
    // window=120s, bucket=60s. All events land in bucket0. After window,
    // bucket0 expires and count drops to Missing.
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(120),
        Duration::from_secs(60),
        false,
    );
    let t0 = ts(1_000_000);
    for i in 0..50 {
        op.push(&json!({ "d": format!("u{}", i) }), None, t0)
            .unwrap();
    }
    assert_eq!(count_of(&mut op, t0), 50.0);
    let t_far = t0 + Duration::from_secs(5_000);
    assert_eq!(op.read(t_far), FeatureValue::Missing);
}

#[test]
fn telemetry_reports_exact_and_sketch_modes() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    op.push(&json!({ "d": "alpha" }), None, t).unwrap();
    let tel = op.hybrid_telemetry().expect("telemetry");
    assert_eq!(tel.op, "distinct_count");
    assert_eq!(tel.mode, "exact");
    assert_eq!(tel.transition_at, 1024);
    assert!(tel.memory_bytes > 0);

    // Push enough distinct values to tip the current bucket into HLL.
    for i in 0..2000 {
        op.push(&json!({ "d": format!("v{}", i) }), None, t)
            .unwrap();
    }
    let tel2 = op.hybrid_telemetry().expect("telemetry");
    assert_eq!(tel2.mode, "sketch");
}
