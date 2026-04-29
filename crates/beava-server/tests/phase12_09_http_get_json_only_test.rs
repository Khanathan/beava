//! Plan 12-09 Wave 5/6 — HTTP /get JSON-only regression coverage.
//!
//! Asserts that HTTP /get keeps its JSON-only contract regardless of the
//! Plan 12-09 msgpack-on-TCP work. Locked decision D-D: HTTP /get response
//! is always `Content-Type: application/json`, payload starts with `b'{'`,
//! and is parseable as JSON.
//!
//! Two tests:
//! 1. `test_http_post_get_returns_json_response` — POST /get with JSON body;
//!    response Content-Type and shape are JSON.
//! 2. `test_http_get_single_returns_json_response` — GET /get/{feature}/{key}
//!    response is JSON (the simpler URL-routed shape).
//!
//! These should pass GREEN today (informational regression), proving that
//! Plan 12-09's apply_shard plumbing (which now passes body_format byte
//! through dispatch helpers) did NOT change the HTTP path semantics.

#![cfg(feature = "testing")]

use beava_server::server::ServerV18;
use std::net::SocketAddr;
use std::time::Duration;

static SERVER_SERIALIZER_12_09_HTTP_JSON: std::sync::Mutex<()> = std::sync::Mutex::new(());

async fn boot_v18() -> (
    SocketAddr,
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = ServerV18::bind(any, any, any).await.expect("bind");
    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();
    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    std::mem::forget(wal_dir);
    std::mem::forget(snap_dir);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async move {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    poll_until_listening(http_addr, Duration::from_secs(10)).await;
    poll_until_listening(tcp_addr, Duration::from_secs(10)).await;
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if let Ok(r) = client
            .get(format!("http://{}/health", http_addr))
            .send()
            .await
        {
            if r.status().as_u16() == 200 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    (http_addr, tcp_addr, shutdown_tx, serve_task)
}

async fn poll_until_listening(addr: SocketAddr, deadline: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {addr} never opened within {deadline:?}");
}

fn register_payload() -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {"event_time": "i64", "user_id": "str", "amount": "f64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "cnt": {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

async fn register_and_push_for_alice(http_addr: SocketAddr) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/register", http_addr))
        .json(&register_payload())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed");

    let body = serde_json::json!({"event_time": 1000, "user_id": "alice", "amount": 42.0});
    let resp = client
        .post(format!("http://{}/push/Txn", http_addr))
        .json(&body)
        .send()
        .await
        .expect("push");
    assert!(resp.status().is_success(), "push failed");
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// HTTP POST /get returns Content-Type: application/json and the body parses as JSON.
/// Plan 12-09 D-D regression guard — HTTP /get is JSON-only regardless of any
/// internal format-byte plumbing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_http_post_get_returns_json_response() {
    {
        let _g = SERVER_SERIALIZER_12_09_HTTP_JSON
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let client = reqwest::Client::new();
    let req_body = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let resp = client
        .post(format!("http://{}/get", http_addr))
        .json(&req_body)
        .send()
        .await
        .expect("post /get");
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("application/json"),
        "expected Content-Type: application/json on HTTP /get, got: {ct:?}"
    );
    let raw = resp.bytes().await.expect("body bytes").to_vec();
    assert_eq!(
        raw.first().copied(),
        Some(b'{'),
        "HTTP /get payload must start with '{{' (JSON), got first byte 0x{:02x?}",
        raw.first()
    );
    let v: serde_json::Value = serde_json::from_slice(&raw).expect("body decodes as JSON");
    assert_eq!(
        v["result"]["alice"]["cnt"], 1,
        "expected alice.cnt=1, got {v:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

/// HTTP GET /get/{feature}/{key} returns Content-Type: application/json and a JSON body.
/// Plan 12-09 D-D regression guard for the URL-routed shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_http_get_single_returns_json_response() {
    {
        let _g = SERVER_SERIALIZER_12_09_HTTP_JSON
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/get/cnt/alice", http_addr))
        .send()
        .await
        .expect("get cnt/alice");
    assert_eq!(resp.status().as_u16(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        ct.contains("application/json"),
        "expected Content-Type: application/json on HTTP /get/{{feature}}/{{key}}, got: {ct:?}"
    );
    let raw = resp.bytes().await.expect("body bytes").to_vec();
    assert_eq!(
        raw.first().copied(),
        Some(b'{'),
        "URL-routed HTTP /get payload must start with '{{', got first byte 0x{:02x?}",
        raw.first()
    );
    let v: serde_json::Value = serde_json::from_slice(&raw).expect("body decodes as JSON");
    assert_eq!(v["value"], 1, "expected value=1, got {v:#}");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}
