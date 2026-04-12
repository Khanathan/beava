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

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use tally::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use tally::server::tcp::{
    handle_push_batch, BackfillTracker, ConnAccumulator, PendingAsync,
    SharedState, BATCH_DEADLINE_US, BATCH_SIZE, make_concurrent_state,
};
use tally::state::store::StateStore;

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_state() -> SharedState {
    make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    )
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
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
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
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
    }
}

fn register(state: &SharedState, defs: Vec<StreamDefinition>) {
    let mut engine = state.engine.write();
    for def in defs {
        engine.register(def).unwrap();
    }
}

fn pending(seq: u64, stream: &str, payload: serde_json::Value, now: SystemTime) -> PendingAsync {
    let raw = serde_json::to_vec(&payload).unwrap();
    PendingAsync::new(seq, stream.into(), payload, raw, now)
}

fn get_count(state: &SharedState, stream: &str, key: &str) -> Option<i64> {
    let now = ts(1000);
    let engine = state.engine.read();
    let store = &state.store;
    let features = engine.get_features(key, &state.store, now);
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
    assert_eq!(state.metrics.lock().events_total, 0);
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
    assert_eq!(state.metrics.lock().events_total, 3);
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
    assert_eq!(state.metrics.lock().events_total, 4);
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
        let engine = seq_state.engine.read();
        let store = &seq_state.store;
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

// ===========================================================================
// Task 2: end-to-end select! loop + sync force-flush + drain isolation
// ===========================================================================
//
// These tests spin up a real `handle_connection_public` on a random
// 127.0.0.1 port and drive it via raw TCP frames. They exercise the
// deadline-armed select! loop, the 64-frame auto-flush, sync force-flush
// (pitfall H-2), seq-ordered drain (pitfall C-2), and per-connection
// isolation of the drain queue.

mod e2e {
    use super::*;
    use tally::server::protocol::{
        self as proto, OP_GET, OP_PUSH_ASYNC, TYPE_I64, TYPE_STR, STATUS_OK, STATUS_ERROR,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Build an OP_PUSH_ASYNC binary payload:
    ///   [u16 name_len][name][u16 field_count][field...]
    /// with TYPE_STR fields only (tests use user_id-style strings).
    fn build_async_payload(stream_name: &str, fields: &[(&str, &str)]) -> Vec<u8> {
        let mut buf = proto::write_string(stream_name);
        buf.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for (k, v) in fields {
            buf.extend_from_slice(&proto::write_string(k));
            buf.push(TYPE_STR);
            buf.extend_from_slice(&proto::write_string(v));
        }
        buf
    }

    /// Build a GET payload: [u16 key_len][key].
    fn build_get_payload(key: &str) -> Vec<u8> {
        proto::write_string(key)
    }

    /// Send a framed command (no response read).
    async fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
        let len = (1 + payload.len()) as u32;
        stream.write_u32(len).await.unwrap();
        stream.write_u8(opcode).await.unwrap();
        if !payload.is_empty() {
            stream.write_all(payload).await.unwrap();
        }
        stream.flush().await.unwrap();
    }

    /// Read exactly one response frame: [u32 len][u8 status][payload].
    async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let len = stream.read_u32().await.unwrap() as usize;
        let status = stream.read_u8().await.unwrap();
        let mut body = vec![0u8; len - 1];
        if !body.is_empty() {
            stream.read_exact(&mut body).await.unwrap();
        }
        (status, body)
    }

    /// Spawn a test server using the Phase 12 coalescing handler and return
    /// (addr, state).
    async fn spawn_server() -> (std::net::SocketAddr, SharedState) {
        let state = make_state();
        register(&state, vec![count_stream("A", "user_id")]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_state = state.clone();
        tokio::spawn(async move {
            let _ = tally::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        (addr, state)
    }

    #[tokio::test]
    async fn sixty_four_frames_dispatch_and_count_matches() {
        // 64 back-to-back OP_PUSH_ASYNC frames trigger the full-accumulator
        // auto-flush path. Assert the primary stream saw exactly 64 events.
        let (addr, state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        for _ in 0..64 {
            let payload = build_async_payload("A", &[("user_id", "u1")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }

        // Force a sync GET so any in-flight buffered events flush first.
        let get = build_get_payload("u1");
        send_frame(&mut client, OP_GET, &get).await;
        let (status, _) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);

        assert_eq!(get_count(&state, "A", "u1"), Some(64));
    }

    #[tokio::test]
    async fn five_frames_deadline_flush_then_get_reflects_mutations() {
        // Five async frames followed by a sleep > BATCH_DEADLINE_US. The
        // deadline branch of the select! loop must fire and flush the
        // accumulator even though there is no sync command following.
        let (addr, state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        for _ in 0..5 {
            let payload = build_async_payload("A", &[("user_id", "u2")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }
        // Wait comfortably longer than the 200µs deadline and also long
        // enough to avoid the tokio test runtime wheel floor.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Now confirm the state has been mutated WITHOUT sending any
        // additional async frames that would sync-flush.
        assert_eq!(get_count(&state, "A", "u2"), Some(5));
    }

    #[tokio::test]
    async fn sync_force_flush_before_dispatch() {
        // Three async frames followed by a sync GET with no delay. The GET
        // must observe all three async mutations because the sync arm
        // force-flushes the accumulator first (H-2).
        let (addr, _state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        for _ in 0..3 {
            let payload = build_async_payload("A", &[("user_id", "u3")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }

        let get = build_get_payload("u3");
        send_frame(&mut client, OP_GET, &get).await;
        let (status, body) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count_1h"], 3);
    }

    #[tokio::test]
    async fn mixed_sync_async_interleaved_no_hangs() {
        // 10 async + 1 sync + 10 async + 1 sync. All data consistent; no
        // timeouts; the second sync observes all 20 async mutations.
        let (addr, _state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        for _ in 0..10 {
            let payload = build_async_payload("A", &[("user_id", "u4")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }
        // First sync GET — force-flush path.
        let get = build_get_payload("u4");
        send_frame(&mut client, OP_GET, &get).await;
        let (status, body) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count_1h"], 10);

        for _ in 0..10 {
            let payload = build_async_payload("A", &[("user_id", "u4")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }
        send_frame(&mut client, OP_GET, &get).await;
        let (status, body) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count_1h"], 20);
    }

    #[tokio::test]
    async fn bad_async_event_drains_before_next_sync_response() {
        // An async push to an unknown stream becomes a pending drain error.
        // On the next sync GET the server must write a STATUS_ERROR frame
        // BEFORE the sync response frame, and the good events must still
        // have been applied.
        let (addr, _state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        // Good event 1.
        send_frame(
            &mut client,
            OP_PUSH_ASYNC,
            &build_async_payload("A", &[("user_id", "u5")]),
        )
        .await;
        // Bad event (unknown stream).
        send_frame(
            &mut client,
            OP_PUSH_ASYNC,
            &build_async_payload("GHOST", &[("user_id", "u5")]),
        )
        .await;
        // Good event 2.
        send_frame(
            &mut client,
            OP_PUSH_ASYNC,
            &build_async_payload("A", &[("user_id", "u5")]),
        )
        .await;

        // Sync GET triggers: force-flush -> drain errors -> sync response.
        // First read must be the drain error frame; second read the sync OK.
        let get = build_get_payload("u5");
        send_frame(&mut client, OP_GET, &get).await;

        let (s1, _) = read_frame(&mut client).await;
        assert_eq!(s1, STATUS_ERROR, "first frame is drained async error");
        let (s2, body2) = read_frame(&mut client).await;
        assert_eq!(s2, STATUS_OK, "second frame is the GET response");
        let json: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json["count_1h"], 2, "two good events still applied");
    }

    // ------------------------------------------------------------------
    // Phase 12 Plan 03: mixed workload sync p99 shape-based sanity test.
    //
    // Spawns two concurrent TCP clients against one server:
    //   - Task A (saturator): pushes OP_PUSH_ASYNC frames as fast as it
    //     can, then FLUSH at the end.
    //   - Task B (sampler):  concurrently sends sync OP_PUSH frames with
    //     ~500µs spacing and records wall-clock response latency.
    //
    // We do NOT assert the tight 91.4µs bench gate here — in-process
    // `cargo test` (debug build, single-threaded tokio) shares the
    // runtime with both clients AND both server handler tasks, which
    // inflates absolute numbers 500-1000× vs a dedicated release bench
    // because the sampler is scheduled cooperatively against a saturator
    // that never yields mid-batch. The purpose of this test is structural:
    //   1. sync_p99 < 100ms (pathological deadlock / starvation catch —
    //      in a correctly wired coalescer even the debug-runtime sampler
    //      completes within 100ms per sample; a hang or cross-connection
    //      drain leak would blow past this)
    //   2. sync_p50 < sync_p99 (distribution sanity)
    //   3. sync_p99 < 3.0 * sync_p50 (tail-shape guard: p99 no more than
    //      3× p50 — catches "async saturation explodes sync tail"
    //      pathological regressions REGARDLESS of the in-test noise
    //      floor; this is the primary defense in cargo-test mode)
    // The tight ±5% bench gate — sync p99 in [82.6, 91.4]µs under
    // release-build multi-core saturation — lives in
    // benchmark/tally-throughput/bench.py and is evaluated by `--mode mixed`.
    #[tokio::test]
    async fn mixed_workload_sync_p99() {
        let (addr, _state) = spawn_server().await;

        // Pre-connect both clients so socket setup is not part of the
        // measurement and both are registered with the server before the
        // saturator starts hogging the runtime.
        let mut sat_sock = TcpStream::connect(addr).await.unwrap();
        let mut smp_sock = TcpStream::connect(addr).await.unwrap();

        // Sampler warmup OUTSIDE the concurrent section — no cold-cache
        // samples enter p99.
        for _ in 0..20u32 {
            let payload = build_async_payload("A", &[("user_id", "smp")]);
            send_frame(&mut smp_sock, proto::OP_PUSH, &payload).await;
            let _ = read_frame(&mut smp_sock).await;
        }

        // Sampler task: fixed 60 samples @ 500µs pacing = ~30ms of work.
        // Spawned first so the single-threaded runtime starts polling it
        // before the saturator hogs the socket.
        let sampler = tokio::spawn(async move {
            let mut latencies_us: Vec<f64> = Vec::with_capacity(80);
            for _ in 0..60u32 {
                let payload = build_async_payload("A", &[("user_id", "smp")]);
                let t0 = std::time::Instant::now();
                send_frame(&mut smp_sock, proto::OP_PUSH, &payload).await;
                let (_s, _body) = read_frame(&mut smp_sock).await;
                let dt = t0.elapsed();
                latencies_us.push(dt.as_nanos() as f64 / 1000.0);
                tokio::time::sleep(std::time::Duration::from_micros(500)).await;
            }
            latencies_us
        });

        // Saturator task: OP_PUSH_ASYNC frames in bursts of 64 (one full
        // accumulator) with a short sleep between bursts so the single-
        // threaded test runtime has a chance to service the sampler's
        // sync OP_PUSH between dispatches. In a release multi-core bench
        // this pacing is unnecessary; in the debug cargo-test runtime it
        // is what makes the shape-based sanity test meaningful (without
        // it, the sampler just sits in the server's accept queue until
        // the saturator is 100% done). See plan H-2 rationale.
        let saturator = tokio::spawn(async move {
            for burst in 0..20u32 {
                for _ in 0..64u32 {
                    let payload = build_async_payload("A", &[("user_id", "sat")]);
                    send_frame(&mut sat_sock, OP_PUSH_ASYNC, &payload).await;
                }
                // Yield the runtime between bursts so the sampler gets serviced.
                tokio::time::sleep(std::time::Duration::from_micros(500)).await;
                let _ = burst;
            }
            send_frame(&mut sat_sock, proto::OP_FLUSH, &[]).await;
            let _ = read_frame(&mut sat_sock).await;
        });

        let (sat_res, smp_res) = tokio::join!(saturator, sampler);
        sat_res.unwrap();
        let mut latencies = smp_res.unwrap();
        assert!(
            latencies.len() >= 20,
            "mixed workload sampler collected too few samples: {}",
            latencies.len()
        );

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50_idx = latencies.len() / 2;
        let p99_idx = (latencies.len() as f64 * 0.99) as usize;
        let p99_idx = p99_idx.min(latencies.len() - 1);
        let sync_p50_us = latencies[p50_idx];
        let sync_p99_us = latencies[p99_idx];

        eprintln!(
            "mixed_workload_sync_p99: samples={} p50={:.2}µs p99={:.2}µs",
            latencies.len(),
            sync_p50_us,
            sync_p99_us
        );

        // 1. Absolute pathological ceiling — catches deadlock /
        //    cross-connection drain leak / indefinite starvation. See
        //    doc comment above for why this is 100ms not 200µs in the
        //    debug cargo-test runtime (the tight 91.4µs gate lives in
        //    the release bench).
        assert!(
            sync_p99_us < 100_000.0,
            "sync p99 under async saturation exceeded 100ms pathological ceiling: {:.2}µs",
            sync_p99_us
        );
        // 2. Sanity: p50 strictly less than p99 (non-degenerate distribution).
        assert!(
            sync_p50_us < sync_p99_us,
            "sync p50 >= p99 ({} >= {}) — sampler likely too sparse",
            sync_p50_us,
            sync_p99_us
        );
        // 3. Shape: p99 no more than 3× p50 — tail-blowup guard.
        assert!(
            sync_p99_us < 3.0 * sync_p50_us,
            "sync p99 tail blew up under async saturation: p50={:.2}µs p99={:.2}µs (ratio={:.2}× > 3×)",
            sync_p50_us,
            sync_p99_us,
            sync_p99_us / sync_p50_us
        );
    }

    #[tokio::test]
    async fn two_connections_drain_isolation() {
        // Open two concurrent connections against the same server. A bad
        // async event on conn A must NOT surface on conn B's drain.
        let (addr, _state) = spawn_server().await;

        let mut conn_a = TcpStream::connect(addr).await.unwrap();
        let mut conn_b = TcpStream::connect(addr).await.unwrap();

        // conn A: push one bad event.
        send_frame(
            &mut conn_a,
            OP_PUSH_ASYNC,
            &build_async_payload("GHOST", &[("user_id", "uA")]),
        )
        .await;

        // conn B: push one good event, then sync GET. conn B must see only
        // STATUS_OK — no drain error from conn A.
        send_frame(
            &mut conn_b,
            OP_PUSH_ASYNC,
            &build_async_payload("A", &[("user_id", "uB")]),
        )
        .await;
        let get = build_get_payload("uB");
        send_frame(&mut conn_b, OP_GET, &get).await;
        let (status, body) = read_frame(&mut conn_b).await;
        assert_eq!(
            status, STATUS_OK,
            "conn B must not inherit conn A's drain error"
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["count_1h"], 1);

        // Sanity: conn A's next sync would have surfaced the error, but we
        // don't need to prove it — isolation is the target assertion.
        let _ = TYPE_I64;
    }
}
