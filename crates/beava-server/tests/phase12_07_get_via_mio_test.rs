//! Plan 12-07 Wave 7 — integration tests proving HTTP /get + TCP /get
//! work end-to-end through the full mio data plane (ServerV18).
//!
//! The lower-layer tests (apply_shard, dispatch_get_batch, encoder) live
//! elsewhere in this plan; this file just plugs them together via real
//! sockets and asserts the full round-trip works. All tests here pass
//! post Waves 1-5 — they're the integration sanity checkpoint.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Serializer pattern: `{ let _g = MUTEX.lock(); }` — drop before awaits.
/// Mirrors phase18_04_6_integration_test.rs:23 etc.
static SERVER_SERIALIZER_12_07_GET: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Boot ServerV18 + return (sv18, http_addr, tcp_addr, shutdown_tx, serve_task).
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
    // Leak the tempdirs so they live for the duration of the server.
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

    // Wait for the data-plane HTTP listener to accept connections (kernel-level).
    poll_until_listening(http_addr, Duration::from_secs(10)).await;
    poll_until_listening(tcp_addr, Duration::from_secs(10)).await;
    // Also wait for /health = 200 to confirm the apply thread is consuming.
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
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64"
                    },
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

// ─── 7.a — HTTP /get integration ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_http_get_single_via_mio_returns_value() {
    {
        let _g = SERVER_SERIALIZER_12_07_GET
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/get/cnt/alice", http_addr))
        .send()
        .await
        .expect("get cnt/alice");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["value"], 1, "expected value=1, got {body:#}");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_http_get_batch_via_mio_returns_result_map() {
    {
        let _g = SERVER_SERIALIZER_12_07_GET
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let client = reqwest::Client::new();
    let req = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let resp = client
        .post(format!("http://{}/get", http_addr))
        .json(&req)
        .send()
        .await
        .expect("post /get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["result"]["alice"]["cnt"], 1,
        "expected result.alice.cnt=1, got {body:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

// ─── 7.b — TCP /get integration ──────────────────────────────────────────────

/// Send one framed TCP request and read back exactly one framed response.
async fn tcp_send_and_recv(tcp_addr: SocketAddr, op: u16, ct: u8, payload: &[u8]) -> Frame {
    let mut sock = tokio::net::TcpStream::connect(tcp_addr)
        .await
        .expect("tcp connect");
    let mut tx_buf = BytesMut::new();
    encode_frame(
        &Frame::new(op, ct, Bytes::copy_from_slice(payload)),
        &mut tx_buf,
    );
    sock.write_all(&tx_buf).await.expect("write");
    let mut rx_buf = BytesMut::with_capacity(64 * 1024);
    let mut tmp = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(frame)) = decode_frame(&mut rx_buf, 4 * 1024 * 1024) {
            return frame;
        }
        tokio::select! {
            r = sock.read(&mut tmp) => {
                let n = r.expect("read");
                if n == 0 {
                    panic!("connection closed before frame received");
                }
                rx_buf.extend_from_slice(&tmp[..n]);
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!("no TCP frame received within deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_op_get_single_returns_op_get_response() {
    use beava_core::wire::{OP_GET, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_07_GET
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let frame = tcp_send_and_recv(
        tcp_addr,
        OP_GET,
        CT_JSON,
        br#"{"feature":"cnt","key":"alice"}"#,
    )
    .await;
    assert_eq!(
        frame.op, OP_GET_RESPONSE,
        "expected OP_GET_RESPONSE for OP_GET"
    );
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(v["value"], 1, "expected value=1, got {v:#}");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_op_mget_returns_op_get_response() {
    use beava_core::wire::{OP_GET_RESPONSE, OP_MGET};
    {
        let _g = SERVER_SERIALIZER_12_07_GET
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let frame = tcp_send_and_recv(
        tcp_addr,
        OP_MGET,
        CT_JSON,
        br#"{"feature":"cnt","keys":["alice","bob"]}"#,
    )
    .await;
    assert_eq!(frame.op, OP_GET_RESPONSE);
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(
        v["result"]["alice"]["cnt"], 1,
        "expected result.alice.cnt=1, got {v:#}"
    );
    assert!(v["result"].get("bob").is_none(), "bob should be omitted");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_op_get_multi_returns_op_get_response() {
    use beava_core::wire::{OP_GET_MULTI, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_07_GET
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let frame = tcp_send_and_recv(
        tcp_addr,
        OP_GET_MULTI,
        CT_JSON,
        br#"{"keys":["alice"],"features":["cnt"]}"#,
    )
    .await;
    assert_eq!(frame.op, OP_GET_RESPONSE);
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(
        v["result"]["alice"]["cnt"], 1,
        "expected result.alice.cnt=1, got {v:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}
