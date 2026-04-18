//! Phase 22-02 operator correctness tests.
//!
//! One positive test per operator, plus edge-case tests per the plan:
//! empty window, single event, bucket expiry, event-time handling.
//!
//! Covered operators (plan 22-02):
//!   count, sum, avg, min, max, variance, stddev,
//!   first, last, first_n, last_n, ema, lag.
//!
//! Percentile / count_distinct / top_k are owned by plan 22-03 and
//! intentionally not touched here.

use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beava::engine::operators::{
    AvgOp, CountOp, EmaOp, FirstNOp, FirstOp, LagOp, LastNOp, LastOp, MaxOp, MinOp, Operator,
    StddevOp, SumOp, VarianceOp, FIRST_N_CAP,
};
use beava::types::FeatureValue;

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn hour_window() -> (Duration, Duration) {
    (Duration::from_secs(3600), Duration::from_secs(60))
}

// ---------------------------------------------------------------------------
// count
// ---------------------------------------------------------------------------

#[test]
fn test_count_correctness() {
    let (w, b) = hour_window();
    let mut op = CountOp::new(w, b);
    let t = ts(1_000_000);
    for _ in 0..7 {
        op.push(&json!({}), None, t).unwrap();
    }
    assert_eq!(op.read(t), FeatureValue::Int(7));
}

#[test]
fn test_count_bucket_expiry() {
    // 5m window, 1m bucket — advance past window → 0 / Missing.
    let mut op = CountOp::new(Duration::from_secs(5 * 60), Duration::from_secs(60));
    let t0 = ts(1_000_000);
    for _ in 0..10 {
        op.push(&json!({}), None, t0).unwrap();
    }
    assert_eq!(op.read(t0), FeatureValue::Int(10));
    let t_future = t0 + Duration::from_secs(20 * 60);
    assert_eq!(op.read(t_future), FeatureValue::Missing);
}

// ---------------------------------------------------------------------------
// sum
// ---------------------------------------------------------------------------

#[test]
fn test_sum_correctness() {
    let (w, b) = hour_window();
    let mut op = SumOp::new("amount", w, b, false);
    let t = ts(1_000_000);
    for v in [1.0, 2.0, 3.0, 4.0, 5.0_f64] {
        op.push(&json!({ "amount": v }), None, t).unwrap();
    }
    assert_eq!(op.read(t), FeatureValue::Float(15.0));
}

#[test]
fn test_sum_empty_window() {
    let (w, b) = hour_window();
    let mut op = SumOp::new("amount", w, b, false);
    assert_eq!(op.read(ts(1_000_000)), FeatureValue::Missing);
}

// ---------------------------------------------------------------------------
// avg
// ---------------------------------------------------------------------------

#[test]
fn test_avg_correctness() {
    let (w, b) = hour_window();
    let mut op = AvgOp::new("amount", w, b, false);
    let t = ts(1_000_000);
    for v in [10.0, 20.0, 30.0] {
        op.push(&json!({ "amount": v }), None, t).unwrap();
    }
    assert_eq!(op.read(t), FeatureValue::Float(20.0));
}

#[test]
fn test_avg_empty_window_returns_missing() {
    let (w, b) = hour_window();
    let mut op = AvgOp::new("amount", w, b, false);
    assert_eq!(op.read(ts(1_000_000)), FeatureValue::Missing);
}

// ---------------------------------------------------------------------------
// min / max — bucket-granular
// ---------------------------------------------------------------------------

#[test]
fn test_min_bucket_granular() {
    let mut op = MinOp::new(
        "v",
        Duration::from_secs(3 * 60),
        Duration::from_secs(60),
        false,
    );
    let t0 = ts(1_000_000);
    // bucket 0: [10, 5] -> bucket-min = 5
    op.push(&json!({"v": 10.0}), None, t0).unwrap();
    op.push(&json!({"v": 5.0}), None, t0).unwrap();
    // bucket 1: [3]
    op.push(&json!({"v": 3.0}), None, t0 + Duration::from_secs(60))
        .unwrap();
    assert_eq!(
        op.read(t0 + Duration::from_secs(60)),
        FeatureValue::Float(3.0)
    );
    // After expiry of bucket 0, min should become 3.0 (only remaining bucket).
    let _later = t0 + Duration::from_secs(3 * 60);
    // Advance past the whole window: all buckets expire → Missing.
    let far = t0 + Duration::from_secs(10 * 60);
    assert_eq!(op.read(far), FeatureValue::Missing);
    // Push one more into the now-current bucket.
    op.push(&json!({"v": 7.0}), None, far).unwrap();
    assert_eq!(op.read(far), FeatureValue::Float(7.0));
}

#[test]
fn test_max_tie_returns_max() {
    let (w, b) = hour_window();
    let mut op = MaxOp::new("v", w, b, false);
    let t = ts(1_000_000);
    op.push(&json!({"v": 42.0}), None, t).unwrap();
    op.push(&json!({"v": 42.0}), None, t).unwrap();
    op.push(&json!({"v": 41.0}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::Float(42.0));
}

// ---------------------------------------------------------------------------
// variance (Welford per-bucket, Chan merge)
// ---------------------------------------------------------------------------

#[test]
fn test_variance_welford_known_answer() {
    let (w, b) = hour_window();
    let mut op = VarianceOp::new("x", w, b, false);
    let t = ts(1_000_000);
    // [1,2,3,4,5]: mean=3, ss=(4+1+0+1+4)=10, sample variance=10/4=2.5.
    for v in [1.0, 2.0, 3.0, 4.0, 5.0_f64] {
        op.push(&json!({ "x": v }), None, t).unwrap();
    }
    match op.read(t) {
        FeatureValue::Float(f) => assert!((f - 2.5).abs() < 1e-9, "got {}", f),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn test_variance_bucket_merge_matches_unbucketed() {
    // Split [1..=10] across 3 buckets (different timestamps) and assert that
    // the Chan-merged variance matches the single-bucket reference.
    let window = Duration::from_secs(10 * 60);
    let bucket = Duration::from_secs(60);
    let mut op = VarianceOp::new("x", window, bucket, false);

    let t0 = ts(1_000_000);
    let groups = [
        (t0, &[1.0, 2.0, 3.0][..]),
        (t0 + Duration::from_secs(60), &[4.0, 5.0, 6.0][..]),
        (t0 + Duration::from_secs(120), &[7.0, 8.0, 9.0, 10.0][..]),
    ];
    for (t, vals) in groups {
        for &v in vals {
            op.push(&json!({"x": v}), None, t).unwrap();
        }
    }
    let read_at = t0 + Duration::from_secs(120);
    let got = match op.read(read_at) {
        FeatureValue::Float(f) => f,
        other => panic!("expected Float, got {:?}", other),
    };
    // Reference: sample variance of 1..=10.
    let mean = 5.5_f64;
    let ss: f64 = (1..=10).map(|i| (i as f64 - mean).powi(2)).sum();
    let reference = ss / 9.0;
    assert!(
        (got - reference).abs() < 1e-9,
        "bucket-merged={} reference={}",
        got,
        reference
    );
}

#[test]
fn test_variance_empty_window_missing() {
    let (w, b) = hour_window();
    let mut op = VarianceOp::new("x", w, b, false);
    assert_eq!(op.read(ts(1_000_000)), FeatureValue::Missing);
}

#[test]
fn test_variance_single_event_zero() {
    let (w, b) = hour_window();
    let mut op = VarianceOp::new("x", w, b, false);
    let t = ts(1_000_000);
    op.push(&json!({"x": 42.0}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::Float(0.0));
}

// ---------------------------------------------------------------------------
// stddev
// ---------------------------------------------------------------------------

#[test]
fn test_stddev_sqrt_of_variance_relation() {
    // Use the existing StddevOp (population variance via sum/sum_sq).
    // Assert stddev^2 matches the numeric variance for a small series.
    let (w, b) = hour_window();
    let mut sd = StddevOp::new("x", w, b, false);
    let t = ts(1_000_000);
    for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0_f64] {
        sd.push(&json!({"x": v}), None, t).unwrap();
    }
    let got = match sd.read(t) {
        FeatureValue::Float(f) => f,
        other => panic!("expected Float, got {:?}", other),
    };
    // Population variance of this series is 4.0, stddev = 2.0.
    assert!((got - 2.0).abs() < 1e-12, "got {}", got);
    assert!((got * got - 4.0).abs() < 1e-12);
}

// ---------------------------------------------------------------------------
// first / last — event-time
// ---------------------------------------------------------------------------

#[test]
fn test_first_by_event_time_out_of_order() {
    let mut op = FirstOp::new("country", false);
    let t_now = ts(1_000_000);
    // Arriving order: later event first, then earlier event.
    op.push(&json!({"country": "UK", "_event_time": 500}), None, t_now)
        .unwrap();
    op.push(&json!({"country": "US", "_event_time": 100}), None, t_now)
        .unwrap();
    // Earlier _event_time wins, regardless of arrival order.
    assert_eq!(op.read(t_now), FeatureValue::String("US".into()));
}

#[test]
fn test_last_wall_clock_fallback() {
    // No _event_time in the payload — should use `now`, i.e. arrival order.
    let mut op = LastOp::new("country", false);
    let t0 = ts(1_000_000);
    let t1 = t0 + Duration::from_secs(10);
    op.push(&json!({"country": "A"}), None, t0).unwrap();
    op.push(&json!({"country": "B"}), None, t1).unwrap();
    assert_eq!(op.read(t1), FeatureValue::String("B".into()));
}

#[test]
fn test_first_single_event() {
    let mut op = FirstOp::new("country", false);
    let t = ts(1_000_000);
    op.push(&json!({"country": "US"}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::String("US".into()));
}

#[test]
fn test_last_single_event() {
    let mut op = LastOp::new("country", false);
    let t = ts(1_000_000);
    op.push(&json!({"country": "DE"}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::String("DE".into()));
}

// ---------------------------------------------------------------------------
// first_n / last_n
// ---------------------------------------------------------------------------

#[test]
fn test_first_n_bounded_to_n_earliest() {
    let mut op = FirstNOp::new("country", 5, false);
    let t_now = ts(1_000_000);
    // Push 100 events with ascending event_times 1..=100.
    for i in 1..=100_i64 {
        op.push(
            &json!({"country": format!("c{}", i), "_event_time": i}),
            None,
            t_now,
        )
        .unwrap();
    }
    let out = match op.read(t_now) {
        FeatureValue::String(s) => s,
        other => panic!("expected String, got {:?}", other),
    };
    let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
    assert_eq!(arr.len(), 5);
    // Ensure it's the 5 earliest: c1..c5.
    for (i, entry) in arr.iter().enumerate() {
        assert_eq!(entry.as_str().unwrap(), format!("c{}", i + 1));
    }
}

#[test]
fn test_first_n_out_of_order_inserts_correctly() {
    let mut op = FirstNOp::new("v", 3, false);
    let t = ts(1_000_000);
    // Later events arrive first with later _event_time.
    op.push(&json!({"v": "x", "_event_time": 500}), None, t)
        .unwrap();
    op.push(&json!({"v": "y", "_event_time": 700}), None, t)
        .unwrap();
    op.push(&json!({"v": "z", "_event_time": 100}), None, t)
        .unwrap();
    op.push(&json!({"v": "w", "_event_time": 50}), None, t)
        .unwrap();
    let out = match op.read(t) {
        FeatureValue::String(s) => s,
        _ => panic!(),
    };
    let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
    assert_eq!(arr.len(), 3);
    // Expected earliest three: w(50), z(100), x(500)
    assert_eq!(arr[0].as_str().unwrap(), "w");
    assert_eq!(arr[1].as_str().unwrap(), "z");
    assert_eq!(arr[2].as_str().unwrap(), "x");
}

#[test]
fn test_first_n_cap_enforced() {
    // Constructing FirstNOp with n > FIRST_N_CAP clamps internally.
    let op = FirstNOp::new("v", FIRST_N_CAP + 1000, false);
    // Can only verify via observable side-effect: pushing more than FIRST_N_CAP
    // results in at most FIRST_N_CAP stored entries.
    let mut op = op;
    let t = ts(1_000_000);
    for i in 0..(FIRST_N_CAP + 500) as i64 {
        op.push(&json!({"v": format!("e{}", i), "_event_time": i}), None, t)
            .unwrap();
    }
    let out = match op.read(t) {
        FeatureValue::String(s) => s,
        _ => panic!(),
    };
    let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
    assert_eq!(arr.len(), FIRST_N_CAP);
}

#[test]
fn test_last_n_deque_bounded() {
    let mut op = LastNOp::new("country", 5, false);
    let t = ts(1_000_000);
    // Push 100 events; LastN retains the last 5 (by insertion order).
    for i in 0..100_i64 {
        op.push(&json!({"country": format!("c{}", i)}), None, t)
            .unwrap();
    }
    let out = match op.read(t) {
        FeatureValue::String(s) => s,
        _ => panic!(),
    };
    let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
    assert_eq!(arr.len(), 5);
    for (i, entry) in arr.iter().enumerate() {
        assert_eq!(entry.as_str().unwrap(), format!("c{}", 95 + i));
    }
}

// ---------------------------------------------------------------------------
// ema
// ---------------------------------------------------------------------------

#[test]
fn test_ema_half_life_decay() {
    // half_life=60s: after exactly one half-life, alpha = 2^(-1) = 0.5.
    // Seed = 100, then push 0 sixty seconds later.
    // Expected: alpha*current + (1-alpha)*value = 0.5*100 + 0.5*0 = 50.
    let mut op = EmaOp::new("x", 60.0, false);
    let t0 = ts(1_000_000);
    op.push(&json!({"x": 100.0}), None, t0).unwrap();
    let t1 = t0 + Duration::from_secs(60);
    op.push(&json!({"x": 0.0}), None, t1).unwrap();
    match op.read(t1) {
        FeatureValue::Float(f) => assert!((f - 50.0).abs() < 1e-9, "got {}", f),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn test_ema_first_event_initializes() {
    let mut op = EmaOp::new("x", 60.0, false);
    let t = ts(1_000_000);
    op.push(&json!({"x": 42.0}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::Float(42.0));
}

#[test]
fn test_ema_no_events_missing() {
    let mut op = EmaOp::new("x", 60.0, false);
    assert_eq!(op.read(ts(1_000_000)), FeatureValue::Missing);
}

// ---------------------------------------------------------------------------
// lag
// ---------------------------------------------------------------------------

#[test]
fn test_lag_returns_n_events_ago() {
    // Push 10 values; lag(n=2) returns the value from 2 events ago:
    // when 10th event lands, lag(2) should be the 8th event (value 8).
    let mut op = LagOp::new("amount", 2, false);
    let t = ts(1_000_000);
    for i in 1..=10 {
        op.push(&json!({"amount": i as f64}), None, t).unwrap();
    }
    assert_eq!(op.read(t), FeatureValue::Float(8.0));
}

#[test]
fn test_lag_insufficient_history_returns_missing() {
    // Only 2 events pushed, n=5 → not enough history.
    let mut op = LagOp::new("amount", 5, false);
    let t = ts(1_000_000);
    op.push(&json!({"amount": 1.0}), None, t).unwrap();
    op.push(&json!({"amount": 2.0}), None, t).unwrap();
    assert_eq!(op.read(t), FeatureValue::Missing);
}
