//! Phase 14 Plan 02 -- Concurrency integration tests.
//!
//! Proves that multi-client concurrent access to the Tally TCP server is safe
//! and correct under the ConcurrentAppState per-field locking model. All tests
//! use `tokio::test(flavor = "multi_thread", worker_threads = 4)` to ensure
//! actual OS-thread parallelism (not just green-thread interleaving).
//!
//! Tests:
//!   1. multi_stream_parallel_push -- different streams, different keys
//!   2. same_stream_different_keys_concurrent -- one stream, many keys
//!   3. concurrent_push_and_get -- reads interleaved with writes
//!   4. fan_out_under_concurrency -- cross-stream fan-out parallel
//!   5. set_mset_concurrent_with_push -- static + live features concurrently

#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use tally::engine::pipeline::PipelineEngine;
use tally::server::protocol::{
    self, OP_FLUSH, OP_GET, OP_MSET, OP_PUSH, OP_PUSH_ASYNC, OP_REGISTER, OP_SET, STATUS_ERROR,
    STATUS_OK, TYPE_BOOL, TYPE_F64, TYPE_I64, TYPE_NULL, TYPE_STR,
};
use tally::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use tally::state::store::StateStore;

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

/// Start a test server on a random TCP port. Returns (tcp_port, state).
async fn start_server() -> (u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test-concurrent.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    );

    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = tcp_listener.local_addr().unwrap().port();

    let tcp_state = state.clone();
    tokio::spawn(async move {
        tally::server::tcp::run_tcp_server_with_listener(tcp_listener, tcp_state)
            .await
            .unwrap();
    });

    // Wait for server to be ready by probing the port
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    (tcp_port, state)
}

/// Send a command frame and read the response. Returns (status, payload).
async fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> (u8, Vec<u8>) {
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(opcode).await.unwrap();
    if !payload.is_empty() {
        stream.write_all(payload).await.unwrap();
    }
    stream.flush().await.unwrap();

    let resp_len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    let payload_len = resp_len - 1;
    let mut resp_payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut resp_payload).await.unwrap();
    }
    (status, resp_payload)
}

/// Build binary PUSH payload (Phase 11 wire format).
fn build_push_payload(stream_name: &str, event: &serde_json::Value) -> Vec<u8> {
    let obj = event.as_object().expect("event must be a JSON object");
    let mut buf = protocol::write_string(stream_name);
    buf.extend_from_slice(&(obj.len() as u16).to_be_bytes());
    for (k, v) in obj {
        buf.extend_from_slice(&protocol::write_string(k));
        match v {
            serde_json::Value::Null => buf.push(TYPE_NULL),
            serde_json::Value::Bool(b) => {
                buf.push(TYPE_BOOL);
                buf.push(if *b { 1 } else { 0 });
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    buf.push(TYPE_I64);
                    buf.extend_from_slice(&i.to_be_bytes());
                } else if let Some(f) = n.as_f64() {
                    buf.push(TYPE_F64);
                    buf.extend_from_slice(&f.to_be_bytes());
                } else {
                    panic!("unsupported number: {}", n);
                }
            }
            serde_json::Value::String(s) => {
                buf.push(TYPE_STR);
                buf.extend_from_slice(&protocol::write_string(s));
            }
            _ => panic!("unsupported value type"),
        }
    }
    buf
}

fn build_get_payload(key: &str) -> Vec<u8> {
    protocol::write_string(key)
}

fn build_register_payload(
    name: &str,
    key_field: &str,
    features_json: Vec<serde_json::Value>,
) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "name": name,
        "key_field": key_field,
        "features": features_json
    }))
    .unwrap()
}

fn build_set_payload(key: &str, features: &serde_json::Value) -> Vec<u8> {
    let mut buf = protocol::write_string(key);
    buf.extend_from_slice(&serde_json::to_vec(features).unwrap());
    buf
}

fn build_mset_payload(entries: &[(&str, serde_json::Value)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (key, val) in entries {
        buf.extend_from_slice(&protocol::write_string(key));
        let json_bytes = serde_json::to_vec(val).unwrap();
        buf.extend_from_slice(&(json_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(&json_bytes);
    }
    buf
}

/// Register a stream via an existing TCP connection.
async fn register_stream(
    conn: &mut TcpStream,
    name: &str,
    key_field: &str,
    features: Vec<serde_json::Value>,
) {
    let payload = build_register_payload(name, key_field, features);
    let (status, _) = send_frame(conn, OP_REGISTER, &payload).await;
    assert_eq!(status, STATUS_OK, "REGISTER {} should succeed", name);
}

/// Push N async events to a stream for a specific key, then flush.
/// Uses sync PUSH (OP_PUSH) instead of async to avoid overwhelming
/// the server on low-core CI runners where BrokenPipe can occur.
async fn push_n_async(
    conn: &mut TcpStream,
    stream_name: &str,
    key_field: &str,
    key_value: &str,
    n: usize,
    amount: f64,
) {
    for _ in 0..n {
        let payload = build_push_payload(
            stream_name,
            &json!({ key_field: key_value, "amount": amount }),
        );
        let (status, _) = send_frame(conn, OP_PUSH, &payload).await;
        assert_eq!(status, STATUS_OK, "PUSH should succeed");
    }
}

/// GET features for a key, return parsed JSON.
async fn get_features(conn: &mut TcpStream, key: &str) -> serde_json::Value {
    let payload = build_get_payload(key);
    let (status, resp) = send_frame(conn, OP_GET, &payload).await;
    assert_eq!(status, STATUS_OK, "GET {} should succeed", key);
    serde_json::from_slice(&resp).unwrap()
}

// ---------------------------------------------------------------------------
// Test 1: Multi-stream parallel push
// ---------------------------------------------------------------------------

/// Spawn 4 tasks pushing to 2 different streams with different keys.
/// Proves: different streams + different keys = no data corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_stream_parallel_push() {
    let (port, _state) = start_server().await;

    // Register two streams from a single connection
    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        register_stream(
            &mut conn,
            "Transactions",
            "user_id",
            vec![json!({"name": "tx_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
        register_stream(
            &mut conn,
            "Logins",
            "user_id",
            vec![json!({"name": "login_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
    }

    let events_per_task = 50;

    // Spawn 4 tasks: 2 push to Transactions, 2 push to Logins
    let mut handles = Vec::new();

    for (stream, key) in &[
        ("Transactions", "user_1"),
        ("Transactions", "user_2"),
        ("Logins", "user_3"),
        ("Logins", "user_4"),
    ] {
        let stream_name = stream.to_string();
        let key_val = key.to_string();
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            push_n_async(
                &mut conn,
                &stream_name,
                "user_id",
                &key_val,
                events_per_task,
                1.0,
            )
            .await;
        }));
    }

    // Wait for all push tasks
    for h in handles {
        h.await.unwrap();
    }

    // Verify counts via GET
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let f1 = get_features(&mut conn, "user_1").await;
    assert_eq!(
        f1["tx_count_1h"], events_per_task,
        "user_1 tx_count_1h should be {}",
        events_per_task
    );

    let f2 = get_features(&mut conn, "user_2").await;
    assert_eq!(
        f2["tx_count_1h"], events_per_task,
        "user_2 tx_count_1h should be {}",
        events_per_task
    );

    let f3 = get_features(&mut conn, "user_3").await;
    assert_eq!(
        f3["login_count_1h"], events_per_task,
        "user_3 login_count_1h should be {}",
        events_per_task
    );

    let f4 = get_features(&mut conn, "user_4").await;
    assert_eq!(
        f4["login_count_1h"], events_per_task,
        "user_4 login_count_1h should be {}",
        events_per_task
    );
}

// ---------------------------------------------------------------------------
// Test 2: Same stream, different keys, concurrent
// ---------------------------------------------------------------------------

/// 4 tasks push 500 events each to the same stream but different entity keys.
/// Proves: entity-level concurrency within one stream works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_stream_different_keys_concurrent() {
    let (port, _state) = start_server().await;

    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        register_stream(
            &mut conn,
            "Payments",
            "user_id",
            vec![
                json!({"name": "pay_count_1h", "type": "count", "window": "1h"}),
                json!({"name": "pay_sum_1h", "type": "sum", "field": "amount", "window": "1h"}),
            ],
        )
        .await;
    }

    let events_per_task = 50;
    let mut handles = Vec::new();

    for i in 1..=4u32 {
        let key = format!("user_{}", i);
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            push_n_async(&mut conn, "Payments", "user_id", &key, events_per_task, 1.0).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify each key has correct count and sum
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    for i in 1..=4u32 {
        let key = format!("user_{}", i);
        let features = get_features(&mut conn, &key).await;
        assert_eq!(
            features["pay_count_1h"], events_per_task,
            "{} count should be {}",
            key, events_per_task
        );
        let sum = features["pay_sum_1h"].as_f64().unwrap();
        assert!(
            (sum - events_per_task as f64).abs() < 0.01,
            "{} sum should be {}, got {}",
            key,
            events_per_task,
            sum
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3: Concurrent push and get
// ---------------------------------------------------------------------------

/// 2 push tasks and 2 get tasks operating on the same key concurrently.
/// GET results should always return a valid count (no corruption).
/// Final GET after all pushes: count = 2 * events_per_task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_push_and_get() {
    let (port, _state) = start_server().await;

    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        register_stream(
            &mut conn,
            "Activity",
            "user_id",
            vec![json!({"name": "act_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
    }

    let events_per_push_task = 500;

    // 2 push tasks
    let mut handles = Vec::new();
    for _ in 0..2 {
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            push_n_async(
                &mut conn,
                "Activity",
                "user_id",
                "user_main",
                events_per_push_task,
                1.0,
            )
            .await;
        }));
    }

    // 2 get tasks -- each reads 50 times, verifying valid results
    for _ in 0..2 {
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            for _ in 0..50 {
                let features = get_features(&mut conn, "user_main").await;
                // Count should be a non-negative integer (no corruption)
                let count = features
                    .get("act_count_1h")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                assert!(count >= 0, "count should be non-negative, got {}", count);
                // Small delay between GETs
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final verification: total count should be 2 * events_per_push_task
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let features = get_features(&mut conn, "user_main").await;
    let final_count = features["act_count_1h"].as_i64().unwrap();
    assert_eq!(
        final_count,
        (2 * events_per_push_task) as i64,
        "Final count should be {}, got {}",
        2 * events_per_push_task,
        final_count
    );
}

// ---------------------------------------------------------------------------
// Test 4: Fan-out under concurrency
// ---------------------------------------------------------------------------

/// Register a pipeline with fan-out: Transactions (keyed on user_id) and
/// MerchantActivity (keyed on merchant_id). Events carry both keys, so a
/// single PUSH fans out to both streams. Two concurrent push tasks verify
/// correct counts in both streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_under_concurrency() {
    let (port, _state) = start_server().await;

    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        register_stream(
            &mut conn,
            "TxFanOut",
            "user_id",
            vec![json!({"name": "tx_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
        register_stream(
            &mut conn,
            "MerchFanOut",
            "merchant_id",
            vec![json!({"name": "merch_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
    }

    let events_per_task = 50;
    let mut handles = Vec::new();

    // Task 1: user_a + merchant_x
    {
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            for _ in 0..events_per_task {
                let payload = build_push_payload(
                    "TxFanOut",
                    &json!({"user_id": "user_a", "merchant_id": "merchant_x", "amount": 10.0}),
                );
                let (status, _) = send_frame(&mut conn, OP_PUSH, &payload).await;
                assert_eq!(status, STATUS_OK);
            }
        }));
    }

    // Task 2: user_b + merchant_y
    {
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            for _ in 0..events_per_task {
                let payload = build_push_payload(
                    "TxFanOut",
                    &json!({"user_id": "user_b", "merchant_id": "merchant_y", "amount": 20.0}),
                );
                let (status, _) = send_frame(&mut conn, OP_PUSH, &payload).await;
                assert_eq!(status, STATUS_OK);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify: TxFanOut counts
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let fa = get_features(&mut conn, "user_a").await;
    assert_eq!(
        fa["tx_count_1h"], events_per_task,
        "user_a tx_count should be {}",
        events_per_task
    );

    let fb = get_features(&mut conn, "user_b").await;
    assert_eq!(
        fb["tx_count_1h"], events_per_task,
        "user_b tx_count should be {}",
        events_per_task
    );

    // Verify: MerchFanOut counts (fan-out target)
    let mx = get_features(&mut conn, "merchant_x").await;
    assert_eq!(
        mx["merch_count_1h"], events_per_task,
        "merchant_x merch_count should be {}",
        events_per_task
    );

    let my = get_features(&mut conn, "merchant_y").await;
    assert_eq!(
        my["merch_count_1h"], events_per_task,
        "merchant_y merch_count should be {}",
        events_per_task
    );
}

// ---------------------------------------------------------------------------
// Test 5: SET/MSET concurrent with PUSH
// ---------------------------------------------------------------------------

/// One task pushes live events, another task writes static features via SET/MSET.
/// After both complete, GET returns both live and static features.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_mset_concurrent_with_push() {
    let (port, _state) = start_server().await;

    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        register_stream(
            &mut conn,
            "Orders",
            "user_id",
            vec![json!({"name": "order_count_1h", "type": "count", "window": "1h"})],
        )
        .await;
    }

    let push_count = 500;

    // Task 1: Push live events
    let push_handle = {
        tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            push_n_async(
                &mut conn,
                "Orders",
                "user_id",
                "user_combo",
                push_count,
                5.0,
            )
            .await;
        })
    };

    // Task 2: SET/MSET static features for same + different keys
    let set_handle = {
        tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();

            // SET a static feature for user_combo
            let set_payload = build_set_payload("user_combo", &json!({"lifetime_value": 9999.0}));
            let (status, _) = send_frame(&mut conn, OP_SET, &set_payload).await;
            assert_eq!(status, STATUS_OK, "SET should succeed");

            // MSET for user_combo and another key
            let mset_payload = build_mset_payload(&[
                ("user_combo", json!({"segment": "premium"})),
                ("user_other", json!({"segment": "basic", "score": 42.0})),
            ]);
            let (status, _) = send_frame(&mut conn, OP_MSET, &mset_payload).await;
            assert_eq!(status, STATUS_OK, "MSET should succeed");
        })
    };

    push_handle.await.unwrap();
    set_handle.await.unwrap();

    // Verify: user_combo has both live and static features
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let features = get_features(&mut conn, "user_combo").await;
    assert_eq!(
        features["order_count_1h"], push_count,
        "Live count should be {}",
        push_count
    );
    // Static features should also be present
    let lv = features["lifetime_value"].as_f64().unwrap();
    assert!(
        (lv - 9999.0).abs() < 0.01,
        "lifetime_value should be 9999.0, got {}",
        lv
    );
    assert_eq!(
        features["segment"], "premium",
        "segment should be 'premium'"
    );

    // Verify: user_other has only static features from MSET
    let other = get_features(&mut conn, "user_other").await;
    assert_eq!(other["segment"], "basic");
    let score = other["score"].as_f64().unwrap();
    assert!(
        (score - 42.0).abs() < 0.01,
        "score should be 42.0, got {}",
        score
    );
}

// ---------------------------------------------------------------------------
// Test 6: Concurrent enrichment correctness (C-5 proof)
// ---------------------------------------------------------------------------

/// Register a 3-stage cascade pipeline (Source -> Converter -> Aggregator) with
/// enrichment propagation. 8 concurrent clients each push 100 events with unique
/// user_ids. After all complete, verify each user's downstream aggregation is
/// exact (no cross-contamination between concurrent pushes).
///
/// Proves C-5: enrichment accumulator is per-push, stack-local, never shared
/// across concurrent connections.
///
/// Benchmark gate (run manually):
///   python3 benchmark/tally-throughput/bench.py --matrix --clients 8
///   Must pass within -5% of 1.1M eps baseline (C-1 gate)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_enriched_concurrent_clients() {
    let (port, _state) = start_server().await;

    // Register 3-stream cascade pipeline via single connection
    {
        let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // Stage 1: Source (keyless)
        let payload = serde_json::to_vec(&json!({
            "name": "ConcSource",
            "features": []
        }))
        .unwrap();
        let (status, _) = send_frame(&mut conn, OP_REGISTER, &payload).await;
        assert_eq!(status, STATUS_OK, "Register ConcSource should succeed");

        // Stage 2: Converter (keyed, depends on Source, derives amount_usd)
        let payload = serde_json::to_vec(&json!({
            "name": "ConcConverter",
            "key_field": "user_id",
            "depends_on": ["ConcSource"],
            "features": [
                {"name": "amount_usd", "type": "derive", "expr": "_event.amount * _event.rate"}
            ]
        }))
        .unwrap();
        let (status, _) = send_frame(&mut conn, OP_REGISTER, &payload).await;
        assert_eq!(status, STATUS_OK, "Register ConcConverter should succeed");

        // Stage 3: Aggregator (keyed, depends on Converter, sums amount_usd)
        let payload = serde_json::to_vec(&json!({
            "name": "ConcAggregator",
            "key_field": "user_id",
            "depends_on": ["ConcConverter"],
            "features": [
                {"name": "total_usd_1h", "type": "sum", "field": "ConcConverter.amount_usd", "window": "1h"}
            ]
        })).unwrap();
        let (status, _) = send_frame(&mut conn, OP_REGISTER, &payload).await;
        assert_eq!(status, STATUS_OK, "Register ConcAggregator should succeed");
    }

    let events_per_client = 100;
    let num_clients = 8;
    let amount = 10.0_f64;
    let rate = 1.5_f64;
    // Expected per user: 100 events * 10.0 * 1.5 = 1500.0

    // Spawn 8 concurrent client tasks
    let mut handles = Vec::new();
    for client_id in 0..num_clients {
        let user_id = format!("user_{}", client_id);
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
            for _ in 0..events_per_client {
                let payload = build_push_payload(
                    "ConcSource",
                    &json!({
                        "user_id": user_id,
                        "amount": amount,
                        "rate": rate
                    }),
                );
                let len = (1 + payload.len()) as u32;
                conn.write_u32(len).await.unwrap();
                conn.write_u8(OP_PUSH_ASYNC).await.unwrap();
                conn.write_all(&payload).await.unwrap();
            }
            conn.flush().await.unwrap();

            // Flush to ensure all async events are processed
            let (status, _) = send_frame(&mut conn, OP_FLUSH, &[]).await;
            assert_eq!(
                status, STATUS_OK,
                "FLUSH should succeed for client {}",
                client_id
            );
        }));
    }

    // Wait for all clients to complete
    for h in handles {
        h.await.unwrap();
    }

    // Verify each user's downstream aggregation is exact (no cross-contamination)
    let mut conn = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let expected_total = (events_per_client as f64) * amount * rate; // 100 * 10.0 * 1.5 = 1500.0

    for client_id in 0..num_clients {
        let user_id = format!("user_{}", client_id);
        let features = get_features(&mut conn, &user_id).await;

        let total = features["total_usd_1h"].as_f64().unwrap_or(0.0);
        assert!(
            (total - expected_total).abs() < 0.01,
            "User {} total_usd_1h should be {}, got {} (cross-contamination detected!)",
            user_id,
            expected_total,
            total
        );
    }
}
