//! Integration tests for the Beava TCP + HTTP server.
//!
//! Tests all SRV-* requirements by starting a real server on random ports
//! and sending binary protocol frames over TCP connections.
//!
//! SRV-01: TCP connection and persistent connections
//! SRV-02: Frame roundtrip and malformed frame handling
//! SRV-03: REGISTER + PUSH, push to unregistered stream
//! SRV-04: GET features after push, GET unknown key
//! SRV-05: SET static features
//! SRV-06: MSET bulk write
//! SRV-07: REGISTER with multiple feature types, PUSH returns computed features
//! SRV-08: HTTP /health endpoint

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::protocol::{
    self, OP_FLUSH, OP_GET, OP_MSET, OP_PUSH, OP_PUSH_ASYNC, OP_REGISTER, OP_SET, STATUS_ERROR,
    STATUS_OK, TYPE_BOOL, TYPE_F64, TYPE_I64, TYPE_NULL, TYPE_STR,
};
use beava::server::tcp::{make_concurrent_state, BackfillTracker, SharedState};
use beava::state::store::StateStore;

// ---------------------------------------------------------------------------
// Test server helper
// ---------------------------------------------------------------------------

/// Start a test server on random ports. Returns (tcp_port, http_port, state).
async fn start_test_server() -> (u16, u16, SharedState) {
    let state: SharedState = make_concurrent_state(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("test.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    );

    // Bind to port 0 for random assignment
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_port = tcp_listener.local_addr().unwrap().port();

    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    // Spawn TCP accept loop using pre-bound listener
    let tcp_state = state.clone();
    tokio::spawn(async move {
        beava::server::tcp::run_tcp_server_with_listener(tcp_listener, tcp_state)
            .await
            .unwrap();
    });

    // Spawn HTTP server using pre-bound listener
    let http_state = state.clone();
    tokio::spawn(async move {
        beava::server::http::run_http_server_with_listener(http_listener, http_state)
            .await
            .unwrap();
    });

    // Small delay for servers to start accepting
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    (tcp_port, http_port, state)
}

// ---------------------------------------------------------------------------
// Frame send/receive helpers
// ---------------------------------------------------------------------------

/// Send a command frame and read the response. Returns (status, payload).
async fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> (u8, Vec<u8>) {
    // Write frame: [4-byte length (opcode+payload)][opcode][payload]
    let len = (1 + payload.len()) as u32;
    stream.write_u32(len).await.unwrap();
    stream.write_u8(opcode).await.unwrap();
    if !payload.is_empty() {
        stream.write_all(payload).await.unwrap();
    }
    stream.flush().await.unwrap();

    // Read response: [4-byte length][status][payload]
    let resp_len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    let payload_len = resp_len - 1;
    let mut resp_payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut resp_payload).await.unwrap();
    }
    (status, resp_payload)
}

/// Build PUSH payload matching Phase 11's binary wire format.
///
/// Layout: `[u16 name_len][name utf-8][u16 field_count][field...]`
/// where each field is `[u16 key_len][key utf-8][u8 type_tag][value bytes]`.
///
/// Accepts a `serde_json::Value::Object` and maps each value to the
/// appropriate binary type tag. Nested arrays/objects panic — tests must
/// use flat field dicts.
fn build_push_payload(stream_name: &str, event: &serde_json::Value) -> Vec<u8> {
    let obj = event
        .as_object()
        .expect("test fixture: event must be a JSON object");
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
                    panic!("unsupported JSON number: {}", n);
                }
            }
            serde_json::Value::String(s) => {
                buf.push(TYPE_STR);
                buf.extend_from_slice(&protocol::write_string(s));
            }
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                panic!("binary PUSH test fixture does not support nested arrays/objects");
            }
        }
    }
    buf
}

/// Build GET payload: [u16 key]
fn build_get_payload(key: &str) -> Vec<u8> {
    protocol::write_string(key)
}

/// Build SET payload: [u16 key][JSON bytes]
fn build_set_payload(key: &str, features: &serde_json::Value) -> Vec<u8> {
    let mut buf = protocol::write_string(key);
    buf.extend_from_slice(&serde_json::to_vec(features).unwrap());
    buf
}

/// Build REGISTER payload: raw JSON bytes of stream definition.
fn build_register_payload(
    name: &str,
    key_field: &str,
    features_json: Vec<serde_json::Value>,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "name": name,
        "key_field": key_field,
        "features": features_json
    }))
    .unwrap()
}

/// Build MSET payload: [u32 count][for each: u16 key string + u32 json_len + json_bytes]
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

/// Helper: register a simple Transactions stream with count feature.
async fn register_tx_stream(stream: &mut TcpStream) {
    let payload = build_register_payload(
        "Transactions",
        "user_id",
        vec![serde_json::json!({
            "name": "tx_count_1h",
            "type": "count",
            "window": "1h"
        })],
    );
    let (status, _) = send_frame(stream, OP_REGISTER, &payload).await;
    assert_eq!(status, STATUS_OK, "REGISTER should succeed");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// SRV-01: TCP connection succeeds.
#[tokio::test(flavor = "current_thread")]
async fn test_tcp_connect() {
    let (tcp_port, _, _state) = start_test_server().await;
    let stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port)).await;
    assert!(stream.is_ok(), "Should connect to TCP server");
}

/// SRV-02: Send a GET frame, receive a well-formed response with status byte.
#[tokio::test(flavor = "current_thread")]
async fn test_frame_roundtrip() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let payload = build_get_payload("nonexistent");
    let (status, resp) = send_frame(&mut stream, OP_GET, &payload).await;
    assert_eq!(status, STATUS_OK);

    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json, serde_json::json!({}));
}

/// SRV-03, SRV-07: REGISTER a stream, PUSH an event, verify features returned.
#[tokio::test(flavor = "current_thread")]
async fn test_register_and_push() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Register
    register_tx_stream(&mut stream).await;

    // Push event — Phase 11 sync push returns an empty ack
    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 50.0}),
    );
    let (status, resp) = send_frame(&mut stream, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_OK);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json, serde_json::json!({}));

    // State WAS updated — verify via GET
    let get_payload = build_get_payload("u1");
    let (get_status, get_resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(get_status, STATUS_OK);
    let get_json: serde_json::Value = serde_json::from_slice(&get_resp).unwrap();
    assert_eq!(
        get_json["tx_count_1h"], 1,
        "Count should be 1 after first push"
    );
}

/// SRV-03: PUSH to unregistered stream returns error.
#[tokio::test(flavor = "current_thread")]
async fn test_push_unregistered_stream() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let push_payload = build_push_payload("NonExistent", &serde_json::json!({"user_id": "u1"}));
    let (status, resp) = send_frame(&mut stream, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_ERROR);
    let msg = String::from_utf8_lossy(&resp);
    assert!(
        msg.contains("unknown stream"),
        "Error should mention 'unknown stream', got: {}",
        msg
    );
}

/// SRV-04: GET features after PUSH returns them.
#[tokio::test(flavor = "current_thread")]
async fn test_get_features_after_push() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    register_tx_stream(&mut stream).await;

    // Push
    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 50.0}),
    );
    send_frame(&mut stream, OP_PUSH, &push_payload).await;

    // GET
    let get_payload = build_get_payload("u1");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);

    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json["tx_count_1h"], 1, "GET should return pushed features");
}

/// SRV-04: GET for unknown key returns empty JSON {}.
#[tokio::test(flavor = "current_thread")]
async fn test_get_unknown_key() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let get_payload = build_get_payload("nobody");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);

    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json, serde_json::json!({}));
}

/// SRV-05: SET writes static features, readable via GET.
#[tokio::test(flavor = "current_thread")]
async fn test_set_static_features() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // SET
    let set_payload = build_set_payload(
        "u1",
        &serde_json::json!({"segment": "premium", "score": 0.95}),
    );
    let (status, _) = send_frame(&mut stream, OP_SET, &set_payload).await;
    assert_eq!(status, STATUS_OK);

    // GET
    let get_payload = build_get_payload("u1");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);

    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json["segment"], "premium");
    assert_eq!(json["score"], 0.95);
}

/// SRV-06: MSET with 2048+ entries completes, values readable via GET.
#[tokio::test(flavor = "current_thread")]
async fn test_mset_bulk_write() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Build 2048 entries
    let entries: Vec<(&str, serde_json::Value)> = (0..2048)
        .map(|i| {
            // We need owned strings but the slice expects &str.
            // Use a trick: allocate and leak for test scope.
            let key: &str = Box::leak(format!("k{}", i).into_boxed_str());
            (key, serde_json::json!({"score": i}))
        })
        .collect();

    let mset_payload = build_mset_payload(&entries);
    let (status, _) = send_frame(&mut stream, OP_MSET, &mset_payload).await;
    assert_eq!(status, STATUS_OK, "MSET should succeed");

    // Verify first and last entries
    let get_payload = build_get_payload("k0");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json["score"], 0);

    let get_payload = build_get_payload("k2047");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json["score"], 2047);
}

/// SRV-07: REGISTER with count + sum + derive, PUSH returns all computed features.
#[tokio::test(flavor = "current_thread")]
async fn test_register_with_derive() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let reg_payload = build_register_payload(
        "Transactions",
        "user_id",
        vec![
            serde_json::json!({"name": "tx_count", "type": "count", "window": "1h"}),
            serde_json::json!({"name": "tx_sum", "type": "sum", "field": "amount", "window": "1h"}),
            serde_json::json!({"name": "avg_amount", "type": "avg", "field": "amount", "window": "1h"}),
            serde_json::json!({"name": "rate", "type": "derive", "expr": "tx_count / 1"}),
        ],
    );
    let (status, _) = send_frame(&mut stream, OP_REGISTER, &reg_payload).await;
    assert_eq!(status, STATUS_OK, "REGISTER with derive should succeed");

    // Push event — Phase 11: sync push returns empty ack. Use GET to read features.
    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 100.0}),
    );
    let (status, resp) = send_frame(&mut stream, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_OK);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json, serde_json::json!({}));

    // GET: verifies the operators AND the derive expression evaluate correctly.
    let get_payload = build_get_payload("u1");
    let (gstatus, gresp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(gstatus, STATUS_OK);
    let gjson: serde_json::Value = serde_json::from_slice(&gresp).unwrap();
    assert_eq!(gjson["tx_count"], 1);
    assert_eq!(gjson["tx_sum"], 100.0);
    assert_eq!(gjson["avg_amount"], 100.0);
    assert_eq!(gjson["rate"], 1.0);
}

/// SRV-08: HTTP GET /health returns 200 with {"status": "ok"}.
#[tokio::test(flavor = "current_thread")]
async fn test_health_endpoint() {
    let (_tcp_port, http_port, _state) = start_test_server().await;

    // Send raw HTTP/1.1 request over TcpStream (no reqwest dependency)
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", http_port))
        .await
        .unwrap();

    let request = format!(
        "GET /health HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
        http_port
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    // Read full response
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_str = String::from_utf8_lossy(&response);

    // Verify HTTP 200
    assert!(
        response_str.starts_with("HTTP/1.1 200"),
        "Expected HTTP 200, got: {}",
        response_str.lines().next().unwrap_or("")
    );

    // Verify body contains {"status":"ok"}
    // Body is after the empty line in HTTP response
    let body = response_str.split("\r\n\r\n").nth(1).unwrap_or("");
    // Body may be chunked-encoded; extract JSON
    assert!(
        body.contains(r#""status":"ok"#) || body.contains(r#""status": "ok"#),
        "Body should contain status:ok, got: {}",
        body
    );
}

/// SRV-01: Multiple commands on same TCP connection work correctly.
#[tokio::test(flavor = "current_thread")]
async fn test_persistent_connection() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Register
    register_tx_stream(&mut stream).await;

    // Push 3 events on the same connection — Phase 11 sync returns empty ack.
    // After each push, use GET to verify the count increments correctly,
    // exercising the persistent-connection path for PUSH+GET interleaving.
    for i in 1..=3 {
        let push_payload = build_push_payload(
            "Transactions",
            &serde_json::json!({"user_id": "u1", "amount": 10.0}),
        );
        let (status, resp) = send_frame(&mut stream, OP_PUSH, &push_payload).await;
        assert_eq!(status, STATUS_OK);
        let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        assert_eq!(json, serde_json::json!({}));

        let get_payload = build_get_payload("u1");
        let (gstatus, gresp) = send_frame(&mut stream, OP_GET, &get_payload).await;
        assert_eq!(gstatus, STATUS_OK);
        let gjson: serde_json::Value = serde_json::from_slice(&gresp).unwrap();
        assert_eq!(
            gjson["tx_count_1h"], i,
            "Count should increment to {} on push {}",
            i, i
        );
    }
}

/// SRV-02: Malformed frame (zero-length) produces error response.
#[tokio::test(flavor = "current_thread")]
async fn test_malformed_frame() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Send a zero-length frame: [0, 0, 0, 0]
    stream.write_all(&[0u8, 0, 0, 0]).await.unwrap();
    stream.flush().await.unwrap();

    // Should receive an error response
    let resp_len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    assert_eq!(
        status, STATUS_ERROR,
        "Zero-length frame should return error"
    );

    if resp_len > 1 {
        let mut payload = vec![0u8; resp_len - 1];
        stream.read_exact(&mut payload).await.unwrap();
        let msg = String::from_utf8_lossy(&payload);
        assert!(
            msg.contains("invalid frame length"),
            "Error should mention invalid frame, got: {}",
            msg
        );
    }
}

/// G-01: Frame length > 64MB is rejected with error and connection close.
#[tokio::test(flavor = "current_thread")]
async fn test_frame_oversized_rejected() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Send a frame header with length = 64MB + 1 (exceeds the 64 * 1024 * 1024 limit)
    let oversized_len: u32 = 64 * 1024 * 1024 + 1;
    stream.write_u32(oversized_len).await.unwrap();
    stream.flush().await.unwrap();

    // Should receive an error response
    let resp_len = stream.read_u32().await.unwrap() as usize;
    let status = stream.read_u8().await.unwrap();
    assert_eq!(status, STATUS_ERROR, "Oversized frame should return error");

    if resp_len > 1 {
        let mut payload = vec![0u8; resp_len - 1];
        stream.read_exact(&mut payload).await.unwrap();
        let msg = String::from_utf8_lossy(&payload);
        assert!(
            msg.contains("invalid frame length"),
            "Error should mention invalid frame length, got: {}",
            msg
        );
    }

    // Connection should be closed -- next read should return EOF
    let result = stream.read_u32().await;
    assert!(
        result.is_err() || result.unwrap() == 0,
        "Connection should be closed after oversized frame"
    );
}

/// G-03: Client disconnects after sending length header but before payload.
/// Server should handle gracefully without panic.
#[tokio::test(flavor = "current_thread")]
async fn test_client_disconnect_mid_frame() {
    let (tcp_port, _, _state) = start_test_server().await;

    // Connect and send only the length header, then drop
    {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
            .await
            .unwrap();
        // Send a length header indicating 100 bytes to follow
        stream.write_u32(100).await.unwrap();
        stream.flush().await.unwrap();
        // Drop stream -- server will get UnexpectedEof on read_u8 or read_exact
    }

    // Small delay for server to process the disconnection
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify server is still alive by making a successful connection
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    let get_payload = build_get_payload("test");
    let (status, _) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(
        status, STATUS_OK,
        "Server should still be alive after client disconnect"
    );
}

/// G-11: MSET with 0 entries returns OK.
#[tokio::test(flavor = "current_thread")]
async fn test_mset_empty() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let empty_entries: &[(&str, serde_json::Value)] = &[];
    let mset_payload = build_mset_payload(empty_entries);
    let (status, _) = send_frame(&mut stream, OP_MSET, &mset_payload).await;
    assert_eq!(status, STATUS_OK, "Empty MSET should succeed");
}

/// G-12: Re-registering a stream with the same name overwrites the definition.
#[tokio::test(flavor = "current_thread")]
async fn test_register_duplicate_overwrites() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    // Register "Transactions" with count feature
    let reg1 = build_register_payload(
        "Transactions",
        "user_id",
        vec![serde_json::json!({"name": "tx_count", "type": "count", "window": "1h"})],
    );
    let (status, _) = send_frame(&mut stream, OP_REGISTER, &reg1).await;
    assert_eq!(status, STATUS_OK);

    // Re-register "Transactions" with sum feature instead
    let reg2 = build_register_payload(
        "Transactions",
        "user_id",
        vec![
            serde_json::json!({"name": "tx_sum", "type": "sum", "field": "amount", "window": "1h"}),
        ],
    );
    let (status, _) = send_frame(&mut stream, OP_REGISTER, &reg2).await;
    assert_eq!(status, STATUS_OK);

    // Push event — Phase 11: sync push returns empty ack. Use GET to verify
    // the second definition is the one being applied.
    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 100.0}),
    );
    let (status, resp) = send_frame(&mut stream, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_OK);
    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(json, serde_json::json!({}));

    let get_payload = build_get_payload("u1");
    let (gstatus, gresp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(gstatus, STATUS_OK);
    let gjson: serde_json::Value = serde_json::from_slice(&gresp).unwrap();
    // Should have tx_sum from the second registration, not tx_count from the first
    assert_eq!(
        gjson["tx_sum"], 100.0,
        "Should use overwritten stream definition"
    );
    assert_eq!(
        gjson.get("tx_count"),
        None,
        "Old feature should not exist after overwrite"
    );
}

// ---------------------------------------------------------------------------
// HTTP request helpers
// ---------------------------------------------------------------------------

/// Parse an HTTP response body, handling both chunked and non-chunked encoding.
/// Returns the JSON body as a string.
fn extract_http_body(response: &str) -> String {
    let body_start = response.find("\r\n\r\n").unwrap_or(response.len()) + 4;
    let raw_body = &response[body_start..];
    // If Transfer-Encoding: chunked, parse chunk format
    if response.contains("transfer-encoding: chunked")
        || response.contains("Transfer-Encoding: chunked")
    {
        // Chunked format: [hex-length]\r\n[data]\r\n ... 0\r\n\r\n
        let mut result = String::new();
        let mut remaining = raw_body;
        while let Some(size_end) = remaining.find("\r\n") {
            let size_str = remaining[..size_end].trim();
            let chunk_size = match usize::from_str_radix(size_str, 16) {
                Ok(s) => s,
                Err(_) => break,
            };
            if chunk_size == 0 {
                break;
            }
            let data_start = size_end + 2;
            let data_end = data_start + chunk_size;
            if data_end <= remaining.len() {
                result.push_str(&remaining[data_start..data_end]);
            }
            remaining = if data_end + 2 <= remaining.len() {
                &remaining[data_end + 2..]
            } else {
                ""
            };
        }
        result
    } else {
        raw_body.to_string()
    }
}

/// Extract the HTTP status code from a response string.
fn extract_http_status(response: &str) -> u16 {
    let status_line = response.lines().next().unwrap_or("");
    status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0)
}

async fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_str = String::from_utf8_lossy(&response).to_string();
    let status = extract_http_status(&response_str);
    let body = extract_http_body(&response_str);
    (status, body)
}

async fn http_post(port: u16, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        body.len(),
        body
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_str = String::from_utf8_lossy(&response).to_string();
    let status = extract_http_status(&response_str);
    let resp_body = extract_http_body(&response_str);
    (status, resp_body)
}

async fn http_delete(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let request = format!(
        "DELETE {} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        path
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_str = String::from_utf8_lossy(&response).to_string();
    let status = extract_http_status(&response_str);
    let resp_body = extract_http_body(&response_str);
    (status, resp_body)
}

// ---------------------------------------------------------------------------
// HTTP Management API Tests (SRV-08)
// ---------------------------------------------------------------------------

/// Test: GET /pipelines with no registered pipelines returns empty list.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_list_empty() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_get(http_port, "/pipelines").await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["pipelines"], serde_json::json!([]));
}

/// Test: POST /pipelines registers a pipeline, GET /pipelines lists it.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_register_and_list() {
    let (_tcp_port, http_port, _state) = start_test_server().await;

    // Register via HTTP
    let pipeline_json = serde_json::json!({
        "name": "Transactions",
        "key_field": "user_id",
        "features": [
            {"name": "tx_count_1h", "type": "count", "window": "1h"}
        ]
    });
    let (status, _body) = http_post(http_port, "/pipelines", &pipeline_json.to_string()).await;
    assert_eq!(status, 200);

    // List pipelines
    let (status, body) = http_get(http_port, "/pipelines").await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let pipelines = json["pipelines"].as_array().unwrap();
    assert!(
        pipelines.iter().any(|p| p == "Transactions"),
        "Pipeline list should contain 'Transactions', got: {:?}",
        pipelines
    );
}

/// Test: GET /pipelines/:name returns pipeline definition with features.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_get_by_name() {
    let (_tcp_port, http_port, _state) = start_test_server().await;

    // Register via HTTP
    let pipeline_json = serde_json::json!({
        "name": "Transactions",
        "key_field": "user_id",
        "features": [
            {"name": "tx_count_1h", "type": "count", "window": "1h"}
        ]
    });
    http_post(http_port, "/pipelines", &pipeline_json.to_string()).await;

    // Get by name
    let (status, body) = http_get(http_port, "/pipelines/Transactions").await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["name"], "Transactions");
    assert_eq!(json["key_field"], "user_id");
    let features = json["features"].as_array().unwrap();
    assert!(!features.is_empty(), "Should have at least one feature");
    assert_eq!(features[0]["type"], "count");
}

/// Test: GET /pipelines/:name for nonexistent pipeline returns 404.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_get_nonexistent() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_get(http_port, "/pipelines/NotReal").await;
    assert_eq!(status, 404);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

/// Test: DELETE /pipelines/:name removes pipeline, subsequent GET returns 404.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_delete() {
    let (_tcp_port, http_port, _state) = start_test_server().await;

    // Register
    let pipeline_json = serde_json::json!({
        "name": "Transactions",
        "key_field": "user_id",
        "features": [
            {"name": "tx_count_1h", "type": "count", "window": "1h"}
        ]
    });
    http_post(http_port, "/pipelines", &pipeline_json.to_string()).await;

    // Delete
    let (status, _body) = http_delete(http_port, "/pipelines/Transactions").await;
    assert_eq!(status, 200);

    // Verify it's gone
    let (status, _body) = http_get(http_port, "/pipelines/Transactions").await;
    assert_eq!(status, 404);
}

/// Test: DELETE /pipelines/:name for nonexistent pipeline returns 404.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_delete_nonexistent() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_delete(http_port, "/pipelines/NotReal").await;
    assert_eq!(status, 404);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

/// Test: GET /metrics returns Prometheus text format with all 5 metrics.
#[tokio::test(flavor = "current_thread")]
async fn test_metrics_endpoint() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_get(http_port, "/metrics").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("beava_keys_total"),
        "Metrics should contain beava_keys_total, got: {}",
        body
    );
    assert!(
        body.contains("beava_events_total"),
        "Metrics should contain beava_events_total"
    );
    assert!(
        body.contains("beava_push_latency_seconds"),
        "Metrics should contain beava_push_latency_seconds"
    );
    assert!(
        body.contains("beava_snapshot_duration_seconds"),
        "Metrics should contain beava_snapshot_duration_seconds"
    );
    assert!(
        body.contains("beava_memory_bytes"),
        "Metrics should contain beava_memory_bytes"
    );
}

/// Test: GET /debug/key/:key after pushing events returns operator state.
#[tokio::test(flavor = "current_thread")]
async fn test_debug_key_after_push() {
    let (tcp_port, http_port, _state) = start_test_server().await;

    // Register and push via TCP
    let mut tcp_stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    register_tx_stream(&mut tcp_stream).await;

    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u123", "amount": 50.0}),
    );
    let (status, _) = send_frame(&mut tcp_stream, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_OK);

    // Debug key via HTTP
    let (status, body) = http_get(http_port, "/debug/key/u123").await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["key"], "u123");
    assert!(
        json["live_operators"].is_array(),
        "Should have live_operators array"
    );
    assert!(
        json["computed_features"].is_object(),
        "Should have computed_features object"
    );
}

/// Test: GET /debug/key/:key for nonexistent key returns 404.
#[tokio::test(flavor = "current_thread")]
async fn test_debug_key_nonexistent() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_get(http_port, "/debug/key/nobody").await;
    assert_eq!(status, 404);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

/// Test: GET /debug/memory returns entity_count and stream_count.
#[tokio::test(flavor = "current_thread")]
async fn test_debug_memory() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    let (status, body) = http_get(http_port, "/debug/memory").await;
    assert_eq!(status, 200);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["entity_count"].is_number(), "Should have entity_count");
    assert!(json["stream_count"].is_number(), "Should have stream_count");
    assert!(
        json["estimated_bytes"].is_number(),
        "Should have estimated_bytes"
    );
}

/// Test: POST /pipelines with invalid JSON returns 400.
#[tokio::test(flavor = "current_thread")]
async fn test_pipelines_register_invalid_json() {
    let (_tcp_port, http_port, _state) = start_test_server().await;
    // Missing required fields
    let (status, body) = http_post(http_port, "/pipelines", r#"{"invalid": true}"#).await;
    assert_eq!(status, 400);
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(json["error"].is_string(), "Should have error message");
}

// ---------------------------------------------------------------------------
// Pre-existing integration tests
// ---------------------------------------------------------------------------

/// G-13: State is visible across separate TCP connections (shared state).
#[tokio::test(flavor = "current_thread")]
async fn test_cross_connection_state_visibility() {
    let (tcp_port, _, _state) = start_test_server().await;

    // Connection A: register and push
    let mut conn_a = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    register_tx_stream(&mut conn_a).await;

    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 50.0}),
    );
    let (status, _) = send_frame(&mut conn_a, OP_PUSH, &push_payload).await;
    assert_eq!(status, STATUS_OK);

    // Connection B: GET the same key
    let mut conn_b = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    let get_payload = build_get_payload("u1");
    let (status, resp) = send_frame(&mut conn_b, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);

    let json: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(
        json["tx_count_1h"], 1,
        "Connection B should see features written by Connection A"
    );
}

// ---------------------------------------------------------------------------
// Phase 11: OP_PUSH_ASYNC, OP_FLUSH, malformed async push
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_push_async_roundtrip_then_get() {
    use tokio::io::AsyncWriteExt;
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    register_tx_stream(&mut stream).await;

    // Fire-and-forget async push. Server processes it and sends NO response frame.
    let push_payload = build_push_payload(
        "Transactions",
        &serde_json::json!({"user_id": "u-async-1", "amount": 25.0}),
    );
    let frame = protocol::encode_frame(OP_PUSH_ASYNC, &push_payload);
    stream.write_all(&frame).await.expect("send async push");

    // Follow-up GET. TCP in-order delivery + sequential handle_connection
    // dispatch guarantees the async push has been processed before the GET
    // hits the state lock.
    let get_payload = build_get_payload("u-async-1");
    let (status, resp) = send_frame(&mut stream, OP_GET, &get_payload).await;
    assert_eq!(status, STATUS_OK);

    let features: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(
        features["tx_count_1h"], 1,
        "async push was not processed: features={features}"
    );
}

#[tokio::test]
async fn test_flush_roundtrip() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();

    let (status, payload) = send_frame(&mut stream, OP_FLUSH, &[]).await;
    assert_eq!(status, STATUS_OK, "FLUSH should return STATUS_OK");
    assert!(
        payload.is_empty(),
        "FLUSH response body should be empty, got {} bytes",
        payload.len()
    );
}

#[tokio::test]
async fn test_push_async_malformed_returns_error() {
    let (tcp_port, _, _state) = start_test_server().await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .unwrap();
    register_tx_stream(&mut stream).await;

    // Build a malformed payload: stream_name ok, field_count=1, key ok, type_tag=0xFF.
    let mut buf = protocol::write_string("Transactions");
    buf.extend_from_slice(&1u16.to_be_bytes()); // field_count
    buf.extend_from_slice(&protocol::write_string("user_id"));
    buf.push(0xFF); // unknown type tag

    // Async frame. Server MUST reply with STATUS_ERROR even though it's async.
    let (status, payload) = send_frame(&mut stream, OP_PUSH_ASYNC, &buf).await;
    assert_eq!(
        status, STATUS_ERROR,
        "malformed async push must produce STATUS_ERROR; got {status}"
    );
    let msg = std::str::from_utf8(&payload).unwrap_or("");
    assert!(
        msg.contains("type tag") || msg.contains("Protocol") || msg.contains("protocol"),
        "error message should indicate protocol issue, got: {msg}"
    );
}
