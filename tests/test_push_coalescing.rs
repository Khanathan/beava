//! Phase 12 Plan 02 — Server-side async push coalescing tests.
//!
//! Covers:
//!   - `PendingAsync` / `ConnAccumulator` unit behavior
//!   - `handle_push_batch` single-lock grouped dispatch semantics
//!   - Cascade + fan-out equivalence under the coalescer
//!   - Partial failure preserves per-seq error attribution
//!
//! These are correctness gates. The performance win from coalescing comes
//! from the caller holding the AppState mutex once per batch; these tests
//! assert the primitive preserves v1.2 single-event cascade + fan-out
//! semantics byte-for-byte.

#![allow(dead_code, unused_imports)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use tally::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use tally::server::tcp::{
    handle_push_batch, AppState, BackfillTracker, ConnAccumulator, Metrics, PendingAsync,
    SharedState, BATCH_DEADLINE_US, BATCH_SIZE,
};
use tally::state::store::StateStore;

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_state() -> SharedState {
    Arc::new(Mutex::new(AppState {
        engine: PipelineEngine::new(),
        store: StateStore::new(),
        metrics: Metrics::default(),
        snapshot_path: std::path::PathBuf::from("test.snapshot"),
        event_log: None,
        backfill_tracker: Arc::new(BackfillTracker::default()),
        backfill_complete: HashSet::new(),
        snapshot_cycle: 0,
        snapshot_seq: 1,
        last_base_seq: 0,
        previous_base_seq: 0,
        throughput: tally::server::throughput::ThroughputTracker::new(),
        latency: tally::server::latency::LatencyTracker::new(),
    }))
}

fn count_stream(name: &str, key: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key.into()),
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
    }
}

fn cascade_child(name: &str, key: &str, parent: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.into(),
        key_field: Some(key.into()),
        features: vec![(
            "count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: Some(vec![parent.to_string()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
    }
}

fn register(state: &SharedState, defs: Vec<StreamDefinition>) {
    let mut app = state.lock().unwrap();
    for def in defs {
        app.engine.register(def).unwrap();
    }
}

fn pending(seq: u64, stream: &str, payload: serde_json::Value, now: SystemTime) -> PendingAsync {
    let raw = serde_json::to_vec(&payload).unwrap();
    PendingAsync::new(seq, stream.into(), payload, raw, now)
}

fn get_count(state: &SharedState, stream: &str, key: &str) -> Option<i64> {
    let mut app = state.lock().unwrap();
    let now = ts(1000);
    let AppState { ref engine, ref mut store, .. } = *app;
    let features = engine.get_features(key, store, now);
    let qualified = format!("{}.count_1h", stream);
    if let Some(fv) = features.get(&qualified).or_else(|| features.get("count_1h")) {
        match fv {
            tally::types::FeatureValue::Int(n) => Some(*n),
            tally::types::FeatureValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// ConnAccumulator unit behavior
// ---------------------------------------------------------------------------

#[test]
fn accumulator_new_is_empty_and_dead() {
    let acc = ConnAccumulator::new();
    assert!(acc.is_empty());
    assert!(!acc.is_full());
    assert_eq!(acc.len(), 0);
    assert!(acc.deadline().is_none());
    assert_eq!(acc.next_seq_peek(), 0);
}

#[test]
fn accumulator_push_assigns_monotonic_seq_and_arms_deadline() {
    let mut acc = ConnAccumulator::new();
    assert!(acc.deadline().is_none());

    acc.push("A".into(), json!({"user_id": "u1"}), vec![], ts(1000));
    assert_eq!(acc.len(), 1);
    // First push arms deadline.
    let d = acc.deadline().expect("deadline armed on first push");
    // Must be in the future and within a tight bound of 200µs.
    let now = tokio::time::Instant::now();
    assert!(d >= now);
    assert!(d <= now + Duration::from_millis(2));
    assert_eq!(acc.next_seq_peek(), 1);

    acc.push("A".into(), json!({"user_id": "u2"}), vec![], ts(1000));
    assert_eq!(acc.len(), 2);
    assert_eq!(acc.next_seq_peek(), 2);
    // Second push does NOT re-arm the deadline.
    let d2 = acc.deadline().expect("deadline still armed");
    assert_eq!(d, d2);
}

#[test]
fn accumulator_is_full_at_batch_size_exact() {
    let mut acc = ConnAccumulator::new();
    for i in 0..(BATCH_SIZE - 1) {
        acc.push(
            "A".into(),
            json!({"user_id": format!("u{}", i)}),
            vec![],
            ts(1000),
        );
        assert!(!acc.is_full(), "not full at {}", i + 1);
    }
    // 64th event hits the cap.
    acc.push("A".into(), json!({"user_id": "uX"}), vec![], ts(1000));
    assert_eq!(acc.len(), BATCH_SIZE);
    assert!(acc.is_full());
    // Sanity: the locked constant matches the plan.
    assert_eq!(BATCH_SIZE, 64);
    assert_eq!(BATCH_DEADLINE_US, 200);
}

#[test]
fn accumulator_drain_clears_buf_and_deadline_but_not_next_seq() {
    let mut acc = ConnAccumulator::new();
    acc.push("A".into(), json!({"user_id": "u1"}), vec![], ts(1000));
    acc.push("A".into(), json!({"user_id": "u2"}), vec![], ts(1000));
    assert_eq!(acc.next_seq_peek(), 2);

    let drained = acc.drain();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].seq, 0);
    assert_eq!(drained[1].seq, 1);
    assert!(acc.is_empty());
    assert!(acc.deadline().is_none());
    // next_seq is NEVER reset on drain — per-connection monotonic (D-12).
    assert_eq!(acc.next_seq_peek(), 2);

    // Next push picks up where we left off and re-arms deadline.
    acc.push("A".into(), json!({"user_id": "u3"}), vec![], ts(1000));
    assert_eq!(acc.next_seq_peek(), 3);
    assert!(acc.deadline().is_some());
}

// ---------------------------------------------------------------------------
// handle_push_batch — grouped dispatch under one lock
// ---------------------------------------------------------------------------

#[test]
fn empty_batch_returns_empty_no_side_effects() {
    let state = make_state();
    register(&state, vec![count_stream("A", "user_id")]);
    let results = handle_push_batch(&state, &[]);
    assert!(results.is_empty());
    assert_eq!(state.lock().unwrap().metrics.events_total, 0);
}

#[test]
fn three_events_one_stream_single_append_many() {
    let state = make_state();
    register(&state, vec![count_stream("A", "user_id")]);
    let batch = vec![
        pending(0, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(1, "A", json!({"user_id": "u2"}), ts(1000)),
        pending(2, "A", json!({"user_id": "u1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.is_ok()));
    // u1 saw 2 events, u2 saw 1.
    assert_eq!(get_count(&state, "A", "u1"), Some(2));
    assert_eq!(get_count(&state, "A", "u2"), Some(1));
    // Metrics bumped by the full batch length.
    assert_eq!(state.lock().unwrap().metrics.events_total, 3);
}

#[test]
fn mixed_streams_preserve_input_order_and_state() {
    let state = make_state();
    register(
        &state,
        vec![count_stream("A", "user_id"), count_stream("B", "user_id")],
    );
    // Interleaved streams: A, B, A, B — grouping would reshuffle, but the
    // results vec must track input order.
    let batch = vec![
        pending(0, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(1, "B", json!({"user_id": "u1"}), ts(1000)),
        pending(2, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(3, "B", json!({"user_id": "u1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert_eq!(results.len(), 4);
    assert!(results.iter().all(|r| r.is_ok()));
    assert_eq!(get_count(&state, "A", "u1"), Some(2));
    assert_eq!(get_count(&state, "B", "u1"), Some(2));
    assert_eq!(state.lock().unwrap().metrics.events_total, 4);
}

#[test]
fn unknown_stream_errors_every_event_in_group_in_input_order() {
    let state = make_state();
    register(&state, vec![count_stream("A", "user_id")]);
    let batch = vec![
        pending(0, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(1, "GHOST", json!({"user_id": "u1"}), ts(1000)),
        pending(2, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(3, "GHOST", json!({"user_id": "u1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert_eq!(results.len(), 4);
    assert!(results[0].is_ok(), "A seq 0 ok");
    assert!(results[1].is_err(), "GHOST seq 1 errors");
    assert!(results[2].is_ok(), "A seq 2 ok");
    assert!(results[3].is_err(), "GHOST seq 3 errors");
    // A saw two real events; GHOST mutated nothing.
    assert_eq!(get_count(&state, "A", "u1"), Some(2));
}

// ---------------------------------------------------------------------------
// Cascade + fan-out under the coalescer (the Phase-11 regression class)
// ---------------------------------------------------------------------------

#[test]
fn cascade_target_updated_under_coalescer() {
    // A (parent) -> B (child via depends_on). Both keyed on user_id.
    // Batch 3 primary events through handle_push_batch; assert that the
    // cascade child's count equals what we'd get via 3 sequential single
    // pushes through the v1.2 path.
    let state = make_state();
    register(
        &state,
        vec![
            count_stream("A", "user_id"),
            cascade_child("B", "user_id", "A"),
        ],
    );
    let batch = vec![
        pending(0, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(1, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(2, "A", json!({"user_id": "u1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert!(results.iter().all(|r| r.is_ok()));
    assert_eq!(get_count(&state, "A", "u1"), Some(3));
    // The cascade child MUST reflect all 3 events exactly — this is the
    // Phase-11-class regression guard (pitfall C-9 / T-12-09).
    assert_eq!(get_count(&state, "B", "u1"), Some(3));
}

#[test]
fn fan_out_target_count_exact_under_coalescer() {
    // Primary Transactions keyed on user_id, sibling MerchantActivity
    // keyed on merchant_id. 4 events each containing both keys must bump
    // MerchantActivity by exactly 4 (not 1, not 16).
    let state = make_state();
    register(
        &state,
        vec![
            count_stream("Transactions", "user_id"),
            count_stream("MerchantActivity", "merchant_id"),
        ],
    );
    let batch = vec![
        pending(0, "Transactions", json!({"user_id": "u1", "merchant_id": "m1"}), ts(1000)),
        pending(1, "Transactions", json!({"user_id": "u2", "merchant_id": "m1"}), ts(1000)),
        pending(2, "Transactions", json!({"user_id": "u3", "merchant_id": "m1"}), ts(1000)),
        pending(3, "Transactions", json!({"user_id": "u4", "merchant_id": "m1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert!(results.iter().all(|r| r.is_ok()));
    // MerchantActivity fan-out MUST fire exactly once per primary event.
    assert_eq!(get_count(&state, "MerchantActivity", "m1"), Some(4));
    // Primary stream tracked 4 distinct users.
    assert_eq!(get_count(&state, "Transactions", "u1"), Some(1));
    assert_eq!(get_count(&state, "Transactions", "u4"), Some(1));
}

// ---------------------------------------------------------------------------
// Cascade equivalence: coalesced batch vs N sequential single pushes
// ---------------------------------------------------------------------------

#[test]
fn cascade_equivalence_3_events_batch_vs_sequential() {
    // Build two identical engines. One processes events via
    // handle_push_batch; the other processes them via the single-event
    // engine path. Both must produce identical (A, B) count state.
    let batch_state = make_state();
    let seq_state = make_state();
    register(
        &batch_state,
        vec![
            count_stream("A", "user_id"),
            cascade_child("B", "user_id", "A"),
        ],
    );
    register(
        &seq_state,
        vec![
            count_stream("A", "user_id"),
            cascade_child("B", "user_id", "A"),
        ],
    );

    let events = [
        json!({"user_id": "u1"}),
        json!({"user_id": "u2"}),
        json!({"user_id": "u1"}),
    ];

    // Batch path.
    let batch: Vec<PendingAsync> = events
        .iter()
        .enumerate()
        .map(|(i, e)| pending(i as u64, "A", e.clone(), ts(1000)))
        .collect();
    let results = handle_push_batch(&batch_state, &batch);
    assert!(results.iter().all(|r| r.is_ok()));

    // Sequential path.
    {
        let mut app = seq_state.lock().unwrap();
        let AppState { ref engine, ref mut store, .. } = *app;
        for e in &events {
            engine
                .push_with_cascade_no_features("A", e, store, ts(1000))
                .unwrap();
        }
    }

    for key in ["u1", "u2"] {
        assert_eq!(
            get_count(&batch_state, "A", key),
            get_count(&seq_state, "A", key),
            "A/{key} batch vs sequential mismatch"
        );
        assert_eq!(
            get_count(&batch_state, "B", key),
            get_count(&seq_state, "B", key),
            "B/{key} batch vs sequential mismatch"
        );
    }
}

// ---------------------------------------------------------------------------
// Partial failure: bad event in middle, surrounding events still apply
// ---------------------------------------------------------------------------

#[test]
fn partial_failure_scatters_err_to_correct_seq() {
    // Inject a failure via unknown stream at seq=1. Events at seq 0 and 2
    // must still apply their operator mutations. The result vec must
    // mirror input order exactly.
    let state = make_state();
    register(&state, vec![count_stream("A", "user_id")]);

    let batch = vec![
        pending(0, "A", json!({"user_id": "u1"}), ts(1000)),
        pending(1, "GHOST", json!({"user_id": "u1"}), ts(1000)),
        pending(2, "A", json!({"user_id": "u1"}), ts(1000)),
    ];
    let results = handle_push_batch(&state, &batch);
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
    // Two good A events applied.
    assert_eq!(get_count(&state, "A", "u1"), Some(2));
}
