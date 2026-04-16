//! Plan 22-03 integration tests for the hybrid TopKOp.
//!
//! Covers: exact heavy-hitters, transition, sketch top-k recall on Zipfian
//! data, bucket-granular retraction, telemetry.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beava::engine::operators::{Operator, TopKOp};
use beava::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn read_top_names(op: &mut TopKOp, t: SystemTime) -> Vec<(serde_json::Value, u64)> {
    match op.read(t) {
        FeatureValue::String(s) => {
            let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
            parsed
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    let v = e.get("value").unwrap().clone();
                    let c = e.get("count").unwrap().as_u64().unwrap();
                    (v, c)
                })
                .collect()
        }
        FeatureValue::Missing => vec![],
        other => panic!("unexpected {:?}", other),
    }
}

fn make_op(k: usize, window_secs: u64, bucket_secs: u64) -> TopKOp {
    TopKOp::new(
        "m",
        k,
        Duration::from_secs(window_secs),
        Duration::from_secs(bucket_secs),
        1024,
        2048,
        4,
        false,
    )
}

#[test]
fn exact_heavy_hitters_identified() {
    let mut op = make_op(3, 3600, 60);
    let t = ts(1_000_000);
    for (m, n) in &[("a", 50u32), ("b", 30), ("c", 10), ("d", 5)] {
        for _ in 0..*n {
            op.push(&json!({ "m": m }), None, t).unwrap();
        }
    }
    assert_eq!(op.mode_name(), "exact");
    let top = read_top_names(&mut op, t);
    assert_eq!(top.len(), 3);
    assert_eq!(top[0].0, json!("a"));
    assert_eq!(top[0].1, 50);
    assert_eq!(top[1].0, json!("b"));
    assert_eq!(top[2].0, json!("c"));
}

#[test]
fn transition_past_1024_unique_values() {
    let mut op = make_op(5, 3600, 60);
    let t = ts(1_000_000);
    for i in 0..1025 {
        op.push(&json!({ "m": i }), None, t).unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
}

#[test]
fn sketch_mode_top_k_recall_on_zipf() {
    // Zipf-ish: value 0 gets 5000 hits, value 1 gets 2500, ..., plus 2000
    // cold "noise" singletons to push unique-cardinality past 1024.
    let mut op = make_op(5, 3600, 60);
    let t = ts(1_000_000);
    let hot_counts = [5000u32, 2500, 1250, 625, 300, 150];
    for (i, &c) in hot_counts.iter().enumerate() {
        for _ in 0..c {
            op.push(&json!({ "m": format!("hot_{}", i) }), None, t)
                .unwrap();
        }
    }
    for i in 0..2000 {
        op.push(&json!({ "m": format!("cold_{}", i) }), None, t)
            .unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let top = read_top_names(&mut op, t);
    assert_eq!(top.len(), 5);
    // Top-3 should be hot_0, hot_1, hot_2 in order.
    assert_eq!(top[0].0, json!("hot_0"));
    assert_eq!(top[1].0, json!("hot_1"));
    assert_eq!(top[2].0, json!("hot_2"));
    // hot_0's count estimate should be within 10% of 5000.
    let err = ((top[0].1 as f64) - 5000.0).abs() / 5000.0;
    assert!(err < 0.1, "hot_0 count estimate error {}", err);
}

#[test]
fn bucket_expiry_drops_counts() {
    let mut op = make_op(3, 120, 60);
    let t0 = ts(1_000_000);
    for _ in 0..10 {
        op.push(&json!({ "m": "a" }), None, t0).unwrap();
    }
    let top = read_top_names(&mut op, t0);
    assert_eq!(top[0], (json!("a"), 10));
    // Jump past the window → retention bucket drains, exact map empties.
    let t_far = t0 + Duration::from_secs(5_000);
    assert_eq!(op.read(t_far), FeatureValue::Missing);
    assert_eq!(op.mode_name(), "exact");
    assert_eq!(op.exact_count(), Some(0));
}

#[test]
fn sketch_mode_bucket_retraction_preserves_survivors() {
    // 3-bucket window (180s / 60s). Singletons land in bucket 0,
    // hotty lands in bucket 2. After bucket 0 is retracted out of the
    // window, hotty is still in-range and must appear in top-k.
    let mut op = make_op(3, 180, 60);
    let t0 = ts(1_000_000);
    for i in 0..1500 {
        op.push(&json!({ "m": format!("x{}", i) }), None, t0)
            .unwrap();
    }
    assert_eq!(op.mode_name(), "sketch");
    let t_hot = t0 + Duration::from_secs(120); // bucket 2
    for _ in 0..50 {
        op.push(&json!({ "m": "hotty" }), None, t_hot).unwrap();
    }
    // Advance 1 further bucket so bucket 0 gets retracted (3 buckets past
    // start advances head by 3 which wraps and evicts bucket 0).
    let t_read = t0 + Duration::from_secs(200);
    let top = read_top_names(&mut op, t_read);
    assert!(
        top.iter().any(|(v, _)| v == &json!("hotty")),
        "hotty should survive; got {:?}",
        top
    );
}

#[test]
fn hybrid_telemetry_for_top_k() {
    let mut op = make_op(3, 3600, 60);
    let t = ts(1_000_000);
    op.push(&json!({ "m": "a" }), None, t).unwrap();
    let tel = op.hybrid_telemetry().expect("telemetry");
    assert_eq!(tel.op, "top_k");
    assert_eq!(tel.mode, "exact");
    assert_eq!(tel.transition_at, 1024);

    for i in 0..1100 {
        op.push(&json!({ "m": i }), None, t).unwrap();
    }
    let tel2 = op.hybrid_telemetry().expect("telemetry after transition");
    assert_eq!(tel2.mode, "sketch");
}
