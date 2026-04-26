//! Plan 22-03 snapshot round-trip for the three hybrid operators.
//!
//! Confirms that `OperatorState::{Percentile, TopK, DistinctCount}` survive
//! a postcard serde round-trip in *both* the exact and sketch modes. The
//! internal enum variants (`PercentileMode::{Exact, Sketch}`, same for TopK)
//! carry explicit `#[serde(rename = "v0_percentile_hybrid")]` etc. tags so
//! that 22-02's concurrent snapshot work (new variants at the bottom of the
//! top-level `OperatorState` enum) stays conflict-free.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tally::engine::hll::DistinctCountOp;
use tally::engine::operators::{PercentileOp, TopKOp};
use tally::state::snapshot::OperatorState;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn round_trip(op: &OperatorState) -> OperatorState {
    let bytes = postcard::to_stdvec(op).expect("postcard serialize");
    postcard::from_bytes::<OperatorState>(&bytes).expect("postcard deserialize")
}

#[test]
fn round_trip_percentile_exact() {
    let mut op = OperatorState::Percentile(PercentileOp::new(
        "amount",
        0.5,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    ));
    let t = ts(1_000_000);
    for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
        op.push(&json!({ "amount": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_percentile_sketch() {
    // Push 300 values — crosses the 256-threshold → Sketch mode.
    let mut op = OperatorState::Percentile(PercentileOp::new(
        "amount",
        0.95,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    ));
    let t = ts(1_000_000);
    for i in 0..300 {
        op.push(&json!({ "amount": i as f64 }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    // Sketch quantile is approximate — check they agree on the same sketch
    // after round-trip (not against a ground truth).
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_top_k_exact() {
    let mut op = OperatorState::TopK(TopKOp::new(
        "merchant",
        3,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        1024,
        2048,
        4,
        false,
    ));
    let t = ts(1_000_000);
    for (m, n) in &[("a", 5u32), ("b", 3), ("c", 1)] {
        for _ in 0..*n {
            op.push(&json!({ "merchant": m }), None, t).unwrap();
        }
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_top_k_sketch() {
    // Push > 1024 distinct keys → Sketch mode.
    let mut op = OperatorState::TopK(TopKOp::new(
        "id",
        10,
        Duration::from_secs(3600),
        Duration::from_secs(60),
        1024,
        2048,
        4,
        false,
    ));
    let t = ts(1_000_000);
    for i in 0..1500 {
        op.push(&json!({ "id": i }), None, t).unwrap();
    }
    // And punch in a heavy hitter.
    for _ in 0..500 {
        op.push(&json!({ "id": 999_999 }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_distinct_count_exact() {
    let mut op = OperatorState::DistinctCount(DistinctCountOp::new(
        "device",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    ));
    let t = ts(1_000_000);
    for i in 0..50 {
        op.push(&json!({ "device": format!("d{}", i) }), None, t)
            .unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_distinct_count_sketch() {
    // Push enough uniques (>1024) to promote at least one bucket into HLL.
    let mut op = OperatorState::DistinctCount(DistinctCountOp::new(
        "device",
        Duration::from_secs(3600),
        Duration::from_secs(60),
        false,
    ));
    let t = ts(1_000_000);
    for i in 0..2000 {
        op.push(&json!({ "device": format!("d{}", i) }), None, t)
            .unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}
