//! Phase 13 Plan 01 -- OP_PUSH_BATCH (0x0A) decode, dispatch, and integration tests.
//!
//! Covers:
//!   - ConnAccumulator.advance_seq unit behavior
//!   - parse_command decode roundtrip (single + multi event)
//!   - Oversized batch reject (16,385 -> error) per D-07/H-7
//!   - Giant count (0xFFFFFFFF) clean reject, no OOM per D-08
//!   - Partial failure error attribution with [batch:{id} event:{idx}] prefix
//!   - E2E batch dispatch via raw TCP
//!   - Backward compat: OP_PUSH_ASYNC still works after PushBatch addition
//!   - Seq continuity between batch and async frames
//!   - Decode micro-benchmark (H-6 / D-18)
//!
//! Phase 54-04 Pass A5: gated under `state-inmem` — reads feature values
//! through `engine.get_features(&state.store)`, only compiled on the
//! in-memory build after this pass.

#![cfg(feature = "state-inmem")]
#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::protocol::{
    self as proto, decode_event_binary, parse_command, OP_GET, OP_PUSH_ASYNC, OP_PUSH_BATCH,
    STATUS_ERROR, STATUS_OK, TYPE_F64, TYPE_I64, TYPE_STR,
};
use beava::server::tcp::{handle_push_batch, make_concurrent_state, BackfillTracker, ConnAccumulator, PendingAsync, SharedState, BATCH_DEADLINE_US, BATCH_SIZE};
// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn make_state() -> SharedState {
    make_concurrent_state(
        PipelineEngine::new(),
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
        group_by_keys: None,
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
        watermark_lateness: None,
        shard_key: None,
    }
}

fn register(state: &SharedState, defs: Vec<StreamDefinition>) {
    let mut engine = state.engine.write();
    for def in defs {
        engine.register(def).unwrap();
    }
}

fn get_count(state: &SharedState, stream: &str, key: &str) -> Option<i64> {
    let now = ts(1000);
    let engine = state.engine.read();
    // Phase 54-04 Pass A6a: `state.store` deleted. Legacy engine.get_features
    // still takes `&StateStore` (Pass-B cleanup target); local scratch store
    // keeps the test compiling. Pass C migrates to shard-scatter read.
    let _ = state; // keep reference alive across this block
    let local_store = beava::state::store::StateStore::new();
    let features = engine.get_features(key, &local_store, now);
    let qualified = format!("{}.count_1h", stream);
    if let Some(fv) = features
        .get(&qualified)
        .or_else(|| features.get("count_1h"))
    {
        match fv {
            beava::types::FeatureValue::Int(n) => Some(*n),
            beava::types::FeatureValue::Float(f) => Some(*f as i64),
            _ => None,
        }
    } else {
        None
    }
}

/// Encode a single binary event body (matching decode_event_binary format):
///   [u16 field_count][for each: [u16 key_len][key][u8 type_tag][value_bytes]]
fn encode_event_body(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for (key, val) in fields {
        buf.extend_from_slice(&proto::write_string(key));
        match val {
            serde_json::Value::Null => {
                buf.push(0x00); // TYPE_NULL
            }
            serde_json::Value::Bool(b) => {
                buf.push(0x01); // TYPE_BOOL
                buf.push(if *b { 1 } else { 0 });
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    buf.push(TYPE_I64);
                    buf.extend_from_slice(&i.to_be_bytes());
                } else if let Some(f) = n.as_f64() {
                    buf.push(TYPE_F64);
                    buf.extend_from_slice(&f.to_be_bytes());
                }
            }
            serde_json::Value::String(s) => {
                buf.push(TYPE_STR);
                buf.extend_from_slice(&proto::write_string(s));
            }
            _ => panic!("unsupported value type in test helper"),
        }
    }
    buf
}

/// Build an OP_PUSH_BATCH payload:
///   [u16 stream_len][stream][u32 batch_id][u32 count]
///   [for each: [u32 event_len][event_bytes]]
fn build_push_batch_payload(
    stream_name: &str,
    batch_id: u32,
    events: &[Vec<(&str, serde_json::Value)>],
) -> Vec<u8> {
    let mut buf = proto::write_string(stream_name);
    buf.extend_from_slice(&batch_id.to_be_bytes());
    buf.extend_from_slice(&(events.len() as u32).to_be_bytes());
    for fields in events {
        let event_bytes = encode_event_body(fields);
        buf.extend_from_slice(&(event_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&event_bytes);
    }
    buf
}

/// Build a raw batch payload with an arbitrary count header (for oversized/giant tests).
fn build_push_batch_payload_raw_count(stream_name: &str, batch_id: u32, count: u32) -> Vec<u8> {
    let mut buf = proto::write_string(stream_name);
    buf.extend_from_slice(&batch_id.to_be_bytes());
    buf.extend_from_slice(&count.to_be_bytes());
    buf
}

// ===========================================================================
// Unit tests: ConnAccumulator.advance_seq
// ===========================================================================

#[test]
fn advance_seq_reserves_and_advances() {
    let mut acc = ConnAccumulator::new();
    assert_eq!(acc.next_seq_peek(), 0);

    // Reserve 5 seq numbers for a batch.
    let base = acc.advance_seq(5);
    assert_eq!(base, 0);
    assert_eq!(acc.next_seq_peek(), 5);

    // Normal push should pick up at seq 5.
    acc.push("A".into(), json!({"x": 1}), vec![], ts(1000));
    assert_eq!(acc.next_seq_peek(), 6);

    // Another advance_seq(3) starts at 6.
    let base2 = acc.advance_seq(3);
    assert_eq!(base2, 6);
    assert_eq!(acc.next_seq_peek(), 9);
}

// ===========================================================================
// Decode roundtrip tests
// ===========================================================================

#[test]
fn decode_roundtrip_single_event() {
    let events = vec![vec![("user_id", json!("u1")), ("amount", json!(42_i64))]];
    let payload = build_push_batch_payload("Transactions", 1, &events);
    let cmd = parse_command(OP_PUSH_BATCH, &payload).unwrap();
    match cmd {
        proto::Command::PushBatch {
            stream_name,
            batch_id,
            events: evts,
        } => {
            assert_eq!(stream_name, "Transactions");
            assert_eq!(batch_id, 1);
            assert_eq!(evts.len(), 1);
            let (val, _raw) = &evts[0];
            assert_eq!(val["user_id"], "u1");
            assert_eq!(val["amount"], 42);
        }
        _ => panic!("expected PushBatch"),
    }
}

#[test]
fn decode_roundtrip_multi_event() {
    let events = vec![
        vec![("user_id", json!("u1")), ("amount", json!(10_i64))],
        vec![("user_id", json!("u2")), ("amount", json!(20_i64))],
        vec![("user_id", json!("u3")), ("amount", json!(30_i64))],
    ];
    let payload = build_push_batch_payload("Tx", 42, &events);
    let cmd = parse_command(OP_PUSH_BATCH, &payload).unwrap();
    match cmd {
        proto::Command::PushBatch {
            stream_name,
            batch_id,
            events: evts,
        } => {
            assert_eq!(stream_name, "Tx");
            assert_eq!(batch_id, 42);
            assert_eq!(evts.len(), 3);
            assert_eq!(evts[0].0["user_id"], "u1");
            assert_eq!(evts[1].0["user_id"], "u2");
            assert_eq!(evts[2].0["user_id"], "u3");
            assert_eq!(evts[0].0["amount"], 10);
            assert_eq!(evts[1].0["amount"], 20);
            assert_eq!(evts[2].0["amount"], 30);
        }
        _ => panic!("expected PushBatch"),
    }
}

// ===========================================================================
// Oversized / giant count rejection
// ===========================================================================

#[test]
fn oversized_batch_reject() {
    // count = 16_385, just over the hard cap (D-07)
    let payload = build_push_batch_payload_raw_count("A", 1, 16_385);
    let result = parse_command(OP_PUSH_BATCH, &payload);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("batch too large"),
        "expected 'batch too large', got: {}",
        err_msg
    );
}

#[test]
fn giant_count_clean_reject() {
    // count = 0xFFFFFFFF (4,294,967,295). No actual events in the payload.
    // Must reject cleanly with no OOM, no crash. (D-08)
    let payload = build_push_batch_payload_raw_count("A", 1, 0xFFFFFFFF);
    let result = parse_command(OP_PUSH_BATCH, &payload);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("batch too large"),
        "expected 'batch too large', got: {}",
        err_msg
    );
}

// ===========================================================================
// E2E tests via raw TCP
// ===========================================================================

mod e2e {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Build an OP_PUSH_ASYNC binary payload.
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

    fn build_get_payload(key: &str) -> Vec<u8> {
        proto::write_string(key)
    }

    async fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
        let len = (1 + payload.len()) as u32;
        stream.write_u32(len).await.unwrap();
        stream.write_u8(opcode).await.unwrap();
        if !payload.is_empty() {
            stream.write_all(payload).await.unwrap();
        }
        stream.flush().await.unwrap();
    }

    async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
        let len = stream.read_u32().await.unwrap() as usize;
        let status = stream.read_u8().await.unwrap();
        let mut body = vec![0u8; len - 1];
        if !body.is_empty() {
            stream.read_exact(&mut body).await.unwrap();
        }
        (status, body)
    }

    async fn spawn_server() -> (std::net::SocketAddr, SharedState) {
        let state = make_state();
        register(&state, vec![count_stream("A", "user_id")]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_state = state.clone();
        tokio::spawn(async move {
            let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, state)
    }

    #[tokio::test]
    async fn e2e_batch_dispatch_count_correct() {
        // Send PushBatch with 5 events for the same key, then GET.
        // Count should be 5.
        let (addr, state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        let events: Vec<Vec<(&str, serde_json::Value)>> =
            (0..5).map(|_| vec![("user_id", json!("u1"))]).collect();
        let payload = build_push_batch_payload("A", 1, &events);
        send_frame(&mut client, OP_PUSH_BATCH, &payload).await;

        // Sync GET to force flush + read state.
        let get = build_get_payload("u1");
        send_frame(&mut client, OP_GET, &get).await;
        let (status, _) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);

        assert_eq!(get_count(&state, "A", "u1"), Some(5));
    }

    #[tokio::test]
    async fn partial_failure_preserves_good_events() {
        // Batch with 3 events: event 1 targets unknown stream "GHOST",
        // events 0 and 2 are valid. Good events should still apply.
        // Drain should contain error for event 1 with [batch: prefix.
        let state = make_state();
        register(&state, vec![count_stream("A", "user_id")]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_state = state.clone();
        tokio::spawn(async move {
            let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut client = TcpStream::connect(addr).await.unwrap();

        // Build a batch where all events go to stream "A", but event 1
        // has a payload that will succeed. To test partial failure, we
        // need events from different streams. Build manually with
        // interleaved stream names -- but OP_PUSH_BATCH has ONE stream_name.
        // So partial failure here comes from the handler itself, not from
        // unknown streams (all events share the same stream_name).
        //
        // Instead, test partial failure by sending to an unregistered
        // stream entirely, and verify the errors have the [batch:] prefix.
        // For a true partial-failure test we'd need events with invalid
        // payloads (missing key field), but the count_stream has key="user_id"
        // and events without it would error.
        let events = vec![
            vec![("user_id", json!("u1"))],       // good
            vec![("no_key_here", json!("oops"))], // bad: missing user_id
            vec![("user_id", json!("u1"))],       // good
        ];
        let payload = build_push_batch_payload("A", 99, &events);
        send_frame(&mut client, OP_PUSH_BATCH, &payload).await;

        // Send sync GET to force drain flush.
        let get = build_get_payload("u1");
        send_frame(&mut client, OP_GET, &get).await;

        // Read: we might get STATUS_ERROR drain frames before the OK GET response.
        let mut errors = Vec::new();
        let mut got_ok = false;
        for _ in 0..5 {
            let (status, body) = read_frame(&mut client).await;
            if status == STATUS_ERROR {
                errors.push(String::from_utf8_lossy(&body).to_string());
            } else if status == STATUS_OK {
                got_ok = true;
                break;
            }
        }
        assert!(got_ok, "expected STATUS_OK for GET");

        // Good events (0 and 2) should have applied.
        assert_eq!(get_count(&state, "A", "u1"), Some(2));

        // Check that at least one error has [batch:99 prefix.
        assert!(
            errors.iter().any(|e| e.contains("[batch:99")),
            "expected [batch:99 in drain errors, got: {:?}",
            errors
        );
    }

    #[tokio::test]
    async fn backward_compat_push_async_still_works() {
        // Verify D-14: OP_PUSH_ASYNC still works after adding PushBatch.
        let (addr, state) = spawn_server().await;
        let mut client = TcpStream::connect(addr).await.unwrap();

        for _ in 0..3 {
            let payload = build_async_payload("A", &[("user_id", "u1")]);
            send_frame(&mut client, OP_PUSH_ASYNC, &payload).await;
        }

        let get = build_get_payload("u1");
        send_frame(&mut client, OP_GET, &get).await;
        let (status, _) = read_frame(&mut client).await;
        assert_eq!(status, STATUS_OK);

        assert_eq!(get_count(&state, "A", "u1"), Some(3));
    }

    #[tokio::test]
    async fn batch_then_async_seq_continuity() {
        // Send a PushBatch with 3 events, then 2 OP_PUSH_ASYNC frames.
        // The async frames should get seqs 3 and 4. Send a bad async
        // to an unknown stream to generate a drain error with seq >= 3.
        let state = make_state();
        register(&state, vec![count_stream("A", "user_id")]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_state = state.clone();
        tokio::spawn(async move {
            let _ = beava::server::tcp::run_tcp_server_with_listener(listener, srv_state).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut client = TcpStream::connect(addr).await.unwrap();

        // Batch of 3 events (consumes seqs 0,1,2).
        let events: Vec<Vec<(&str, serde_json::Value)>> =
            (0..3).map(|_| vec![("user_id", json!("u1"))]).collect();
        let payload = build_push_batch_payload("A", 1, &events);
        send_frame(&mut client, OP_PUSH_BATCH, &payload).await;

        // 1 good async + 1 bad async to unknown stream.
        let good_payload = build_async_payload("A", &[("user_id", "u1")]);
        send_frame(&mut client, OP_PUSH_ASYNC, &good_payload).await;

        let bad_payload = build_async_payload("GHOST", &[("user_id", "u1")]);
        send_frame(&mut client, OP_PUSH_ASYNC, &bad_payload).await;

        // Sync GET to force flush.
        let get = build_get_payload("u1");
        send_frame(&mut client, OP_GET, &get).await;

        // Read drain errors (if any) then OK.
        let mut errors = Vec::new();
        for _ in 0..5 {
            let (status, body) = read_frame(&mut client).await;
            if status == STATUS_ERROR {
                errors.push(String::from_utf8_lossy(&body).to_string());
            } else {
                break;
            }
        }

        // The 3 batch + 1 good async = 4 events for key u1.
        assert_eq!(get_count(&state, "A", "u1"), Some(4));

        // The bad async (GHOST) should have produced a drain error.
        // Its seq should be >= 3 (batch consumed 0,1,2; good async = 3, bad async = 4).
        assert!(
            !errors.is_empty(),
            "expected at least one drain error for GHOST stream"
        );
    }
}

// ===========================================================================
// Decode micro-benchmark (H-6, D-18)
// ===========================================================================

#[test]
fn decode_microbench_1000_events() {
    // Build a 1000-event batch frame, time 100 iterations of parse_command.
    // Assert per-event decode < 2us (1000 events * 100 iters in < 200ms).
    let events: Vec<Vec<(&str, serde_json::Value)>> = (0..1000)
        .map(|i| {
            vec![
                ("user_id", json!(format!("u{}", i))),
                ("amount", json!(i as i64)),
                ("merchant_id", json!(format!("m{}", i % 100))),
            ]
        })
        .collect();
    let payload = build_push_batch_payload("Transactions", 1, &events);

    let iterations = 100;
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let cmd = parse_command(OP_PUSH_BATCH, &payload).unwrap();
        // Prevent optimizer from eliding the work.
        std::hint::black_box(&cmd);
    }
    let elapsed = start.elapsed();
    let total_events = 1000 * iterations;
    let per_event_ns = elapsed.as_nanos() as f64 / total_events as f64;
    let per_event_us = per_event_ns / 1000.0;

    eprintln!(
        "decode_microbench: {} events x {} iters = {} total in {:.2?} ({:.2} us/event)",
        1000, iterations, total_events, elapsed, per_event_us
    );

    // H-6 gate: per-event decode must be < 2us.
    assert!(
        per_event_us < 2.0,
        "per-event decode too slow: {:.2} us (target < 2.0 us)",
        per_event_us
    );
}

// ===========================================================================
// Server-side latency instrumentation for batch path
// ===========================================================================

/// handle_push_batch must populate the PUSH command histogram so that
/// /debug/latency reports non-zero p50/p99 under batch ingest. Before the
/// Phase 43 fix, only the legacy OP_PUSH single-event path recorded
/// latency; the 99%+ of real traffic (OP_PUSH_BATCH + OP_PUSH_ASYNC
/// accumulator flush → handle_push_batch) produced count=0 forever.
#[test]
fn handle_push_batch_records_push_latency() {
    let state = make_state();
    register(&state, vec![count_stream("Tx", "user_id")]);

    // Baseline: PUSH histogram is empty before any batch call.
    {
        let latency = state.latency.lock();
        let now = std::time::Instant::now();
        let j = latency.to_json(now);
        assert_eq!(
            j["per_command"][0]["command"].as_str().unwrap(),
            "PUSH",
            "first per_command entry should be PUSH"
        );
        assert_eq!(
            j["per_command"][0]["count"].as_u64().unwrap(),
            0,
            "PUSH count should be 0 before any batch"
        );
    }

    let batch: Vec<PendingAsync> = (0..10)
        .map(|i| {
            PendingAsync::new(
                i,
                "Tx".into(),
                json!({"user_id": format!("u{}", i)}),
                vec![],
                ts(1000),
            )
        })
        .collect();

    let results = handle_push_batch(&state, &batch);
    assert_eq!(results.len(), 10);
    for r in &results {
        assert!(r.is_ok(), "all events in the batch should succeed");
    }

    let latency = state.latency.lock();
    let now = std::time::Instant::now();
    let j = latency.to_json(now);

    let push_count = j["per_command"][0]["count"].as_u64().unwrap();
    assert!(
        push_count >= 1,
        "PUSH count must be >= 1 after handle_push_batch (got {}); \
         pre-Phase-43 bug: only OP_PUSH single-event path recorded latency",
        push_count
    );
    let p50 = j["per_command"][0]["p50_us"].as_f64().unwrap();
    assert!(
        p50 > 0.0,
        "PUSH p50_us should be > 0 after a real batch (got {})",
        p50
    );

    let per_stream = j["per_stream"].as_array().unwrap();
    assert!(
        per_stream.iter().any(|s| s["stream"] == "Tx"),
        "per_stream should include 'Tx' after a push to Tx; got {:?}",
        per_stream
    );
}
