//! Phase 22-02 snapshot round-trip tests for the 11 linear + order-sensitive
//! operator states.
//!
//! Every OperatorState variant the plan owns (count, sum, avg, min, max,
//! variance, stddev, first, last, first_n, last_n, ema, lag) is serialized
//! via postcard and deserialized back. We then feed the same event stream
//! into both the original and the round-tripped operator and assert the
//! `read()` outputs are bit-identical.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tally::engine::operators::{
    AvgOp, CountOp, EmaOp, FirstNOp, FirstOp, LagOp, LastNOp, LastOp, MaxOp, MinOp, StddevOp,
    SumOp, VarianceOp,
};
use tally::state::snapshot::OperatorState;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// Serialize → deserialize an OperatorState via postcard. Returns the
/// reconstructed value; panics if either step fails.
fn round_trip(op: &OperatorState) -> OperatorState {
    let bytes = postcard::to_stdvec(op).expect("postcard serialize");
    postcard::from_bytes::<OperatorState>(&bytes).expect("postcard deserialize")
}

fn hour_window() -> (Duration, Duration) {
    (Duration::from_secs(3600), Duration::from_secs(60))
}

// ---------------------------------------------------------------------------
// Count / Sum / Avg
// ---------------------------------------------------------------------------

#[test]
fn round_trip_count_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Count(CountOp::new(w, b));
    let t = ts(1_000_000);
    for _ in 0..5 {
        op.push(&json!({}), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_sum_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Sum(SumOp::new("amount", w, b, false));
    let t = ts(1_000_000);
    for v in [10.0, 20.0, 30.0] {
        op.push(&json!({ "amount": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_avg_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Avg(AvgOp::new("amount", w, b, false));
    let t = ts(1_000_000);
    for v in [1.0, 2.0, 3.0, 4.0] {
        op.push(&json!({ "amount": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

// ---------------------------------------------------------------------------
// Min / Max
// ---------------------------------------------------------------------------

#[test]
fn round_trip_min_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Min(MinOp::new("v", w, b, false));
    let t = ts(1_000_000);
    for v in [10.0, 5.0, 20.0, 3.0, 15.0] {
        op.push(&json!({ "v": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_max_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Max(MaxOp::new("v", w, b, false));
    let t = ts(1_000_000);
    for v in [10.0, 5.0, 20.0, 3.0, 15.0] {
        op.push(&json!({ "v": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

// ---------------------------------------------------------------------------
// Variance / Stddev
// ---------------------------------------------------------------------------

#[test]
fn round_trip_variance_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Variance(VarianceOp::new("x", w, b, false));
    let t = ts(1_000_000);
    for v in [1.0, 2.0, 3.0, 4.0, 5.0_f64] {
        op.push(&json!({ "x": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_stddev_preserves_state() {
    let (w, b) = hour_window();
    let mut op = OperatorState::Stddev(StddevOp::new("x", w, b, false));
    let t = ts(1_000_000);
    for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0_f64] {
        op.push(&json!({ "x": v }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

// ---------------------------------------------------------------------------
// First / Last (event-time aware)
// ---------------------------------------------------------------------------

#[test]
fn round_trip_first_preserves_state() {
    let mut op = OperatorState::First(FirstOp::new("country", false));
    let t = ts(1_000_000);
    op.push(
        &json!({"country": "UK", "_event_time": 500}),
        None,
        t,
    )
    .unwrap();
    op.push(
        &json!({"country": "US", "_event_time": 100}),
        None,
        t,
    )
    .unwrap();
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_last_preserves_state() {
    let mut op = OperatorState::Last(LastOp::new("country", false));
    let t = ts(1_000_000);
    op.push(&json!({"country": "A"}), None, t).unwrap();
    op.push(
        &json!({"country": "B"}),
        None,
        t + Duration::from_secs(5),
    )
    .unwrap();
    let mut restored = round_trip(&op);
    assert_eq!(
        op.read(t + Duration::from_secs(5)),
        restored.read(t + Duration::from_secs(5))
    );
}

// ---------------------------------------------------------------------------
// FirstN / LastN
// ---------------------------------------------------------------------------

#[test]
fn round_trip_first_n_preserves_state() {
    let mut op = OperatorState::FirstN(FirstNOp::new("v", 5, false));
    let t = ts(1_000_000);
    for i in 0..20_i64 {
        op.push(
            &json!({"v": format!("e{}", i), "_event_time": i}),
            None,
            t,
        )
        .unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

#[test]
fn round_trip_last_n_preserves_state() {
    let mut op = OperatorState::LastN(LastNOp::new("v", 5, false));
    let t = ts(1_000_000);
    for i in 0..20 {
        op.push(&json!({"v": format!("e{}", i)}), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

// ---------------------------------------------------------------------------
// Ema / Lag
// ---------------------------------------------------------------------------

#[test]
fn round_trip_ema_preserves_state() {
    let mut op = OperatorState::Ema(EmaOp::new("x", 60.0, false));
    let t = ts(1_000_000);
    op.push(&json!({"x": 100.0}), None, t).unwrap();
    op.push(
        &json!({"x": 0.0}),
        None,
        t + Duration::from_secs(60),
    )
    .unwrap();
    let mut restored = round_trip(&op);
    assert_eq!(
        op.read(t + Duration::from_secs(60)),
        restored.read(t + Duration::from_secs(60))
    );
}

#[test]
fn round_trip_lag_preserves_state() {
    let mut op = OperatorState::Lag(LagOp::new("amount", 3, false));
    let t = ts(1_000_000);
    for i in 1..=10 {
        op.push(&json!({ "amount": i as f64 }), None, t).unwrap();
    }
    let mut restored = round_trip(&op);
    assert_eq!(op.read(t), restored.read(t));
}

// ---------------------------------------------------------------------------
// Composite: a Vec of all 13 operators round-trips as a single blob
// ---------------------------------------------------------------------------

#[test]
fn round_trip_all_operator_states_in_one_blob() {
    let (w, b) = hour_window();
    let t = ts(1_000_000);

    let mut states: Vec<OperatorState> = vec![
        OperatorState::Count(CountOp::new(w, b)),
        OperatorState::Sum(SumOp::new("a", w, b, false)),
        OperatorState::Avg(AvgOp::new("a", w, b, false)),
        OperatorState::Min(MinOp::new("a", w, b, false)),
        OperatorState::Max(MaxOp::new("a", w, b, false)),
        OperatorState::Variance(VarianceOp::new("a", w, b, false)),
        OperatorState::Stddev(StddevOp::new("a", w, b, false)),
        OperatorState::First(FirstOp::new("a", false)),
        OperatorState::Last(LastOp::new("a", false)),
        OperatorState::FirstN(FirstNOp::new("a", 3, false)),
        OperatorState::LastN(LastNOp::new("a", 3, false)),
        OperatorState::Ema(EmaOp::new("a", 60.0, false)),
        OperatorState::Lag(LagOp::new("a", 2, false)),
    ];

    // Drive identical events into each operator.
    for i in 1..=6_i64 {
        for op in states.iter_mut() {
            op.push(
                &json!({"a": i as f64, "_event_time": i}),
                None,
                t + Duration::from_secs(i as u64),
            )
            .unwrap();
        }
    }

    let bytes = postcard::to_stdvec(&states).expect("serialize Vec");
    let mut restored: Vec<OperatorState> =
        postcard::from_bytes(&bytes).expect("deserialize Vec");

    assert_eq!(states.len(), restored.len());
    let read_at = t + Duration::from_secs(6);
    for (orig, rest) in states.iter_mut().zip(restored.iter_mut()) {
        assert_eq!(
            orig.read(read_at),
            rest.read(read_at),
            "{} mismatch after round-trip",
            orig.operator_type_name()
        );
    }
}
