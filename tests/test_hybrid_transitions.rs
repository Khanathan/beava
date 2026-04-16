//! Plan 22-03 cross-operator transition tests.
//!
//! Verifies that for all three hybrid operators, values pushed *before* the
//! exact→sketch transition remain correctly accounted for in the sketch
//! state *after* transition (nothing is "lost" in the copy step), and that
//! the reported memory increases once the sketch lands.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beava::engine::hll::DistinctCountOp;
use beava::engine::operators::{
    Operator, PercentileOp, TopKOp, PERCENTILE_EXACT_THRESHOLD,
};
use beava::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

#[test]
fn percentile_transition_preserves_prior_values() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Push exactly PERCENTILE_EXACT_THRESHOLD events.
    for i in 0..PERCENTILE_EXACT_THRESHOLD {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    let pre = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    assert_eq!(op.mode_name(), "exact");
    // One more event tips to Sketch.
    op.push(&json!({ "v": PERCENTILE_EXACT_THRESHOLD as f64 }), None, t).unwrap();
    assert_eq!(op.mode_name(), "sketch");
    let post = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    // Values are preserved: sketch p50 stays near exact p50 (within α).
    let err = (pre - post).abs() / pre.max(1.0);
    assert!(err < 0.05, "pre={} post={} err={}", pre, post, err);
}

#[test]
fn top_k_transition_preserves_ranking() {
    let mut op = TopKOp::new(
        "m",
        3,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        1024,
        2048,
        4,
        false,
    );
    let t = ts(1_000_000);
    // Build a heavy hitter set before transition.
    for (name, n) in &[("alpha", 500u32), ("beta", 300), ("gamma", 200)] {
        for _ in 0..*n {
            op.push(&json!({ "m": name }), None, t).unwrap();
        }
    }
    // Add 1030 uniques each with count 1 → push past threshold.
    for i in 0..1030 {
        op.push(&json!({ "m": format!("x{}", i) }), None, t)
            .unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let out = match op.read(t) {
        FeatureValue::String(s) => s,
        _ => panic!(),
    };
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    let arr = parsed.as_array().unwrap();
    // Top 3 must still be alpha/beta/gamma.
    assert_eq!(arr[0]["value"], json!("alpha"));
    assert_eq!(arr[1]["value"], json!("beta"));
    assert_eq!(arr[2]["value"], json!("gamma"));
}

#[test]
fn distinct_count_transition_preserves_cardinality_within_tolerance() {
    let mut op = DistinctCountOp::new(
        "d",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    // Just under threshold → exact.
    for i in 0..1000 {
        op.push(&json!({ "d": format!("u{}", i) }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "exact");
    let pre = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    assert!((pre - 1000.0).abs() < 1e-9);
    // Cross threshold.
    for i in 1000..1500 {
        op.push(&json!({ "d": format!("u{}", i) }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let post = match op.read(t) {
        FeatureValue::Float(f) => f,
        _ => panic!(),
    };
    let err = (post - 1500.0).abs() / 1500.0;
    assert!(err < 0.05, "post-transition error {} > 5%", err);
}

#[test]
fn memory_bytes_increase_after_transition() {
    let mut op = PercentileOp::new(
        "v",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    );
    let t = ts(1_000_000);
    op.push(&json!({ "v": 1.0 }), None, t).unwrap();
    let mem_exact = op.hybrid_telemetry().unwrap().memory_bytes;
    for i in 0..400 {
        op.push(&json!({ "v": i as f64 }), None, t).unwrap();
    }
    let mem_sketch = op.hybrid_telemetry().unwrap().memory_bytes;
    assert!(
        mem_sketch > mem_exact,
        "sketch memory ({}) should exceed lone-exact memory ({})",
        mem_sketch,
        mem_exact
    );
}
