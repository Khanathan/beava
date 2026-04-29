//! Plan 12-09 Wave 6 — JSON TCP /get regression coverage.
//!
//! Wave 4 changed apply_shard.rs's TCP /get arms to plumb body_format through
//! the dispatch helpers. The old behavior (CT_JSON body → CT_JSON response,
//! same shape as Plan 12-07) MUST keep working bit-for-bit.
//!
//! Three tests covering OP_GET / OP_MGET / OP_GET_MULTI with `content_type=
//! CT_JSON` and JSON bodies. All assert the response is (OP_GET_RESPONSE,
//! CT_JSON, JSON payload) — the locked Plan 12-07 contract.
//!
//! These should pass GREEN today (post-Wave-4); if they fail, Wave 4 has a
//! regression bug that needs fixing.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SERVER_SERIALIZER_12_09_TCP_JSON: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

// ─── Tests ────────────────────────────────────────────────────────────────────

/// OP_GET + CT_JSON: response frame is (OP_GET_RESPONSE, CT_JSON, JSON
/// `{"value":1}`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_get_single_json_unchanged() {
    use beava_core::wire::{OP_GET, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_09_TCP_JSON
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let frame = tcp_send_and_recv(
        tcp_addr,
        OP_GET,
        CT_JSON,
        br#"{"feature":"cnt","key":"alice"}"#,
    )
    .await;
    assert_eq!(frame.op, OP_GET_RESPONSE);
    assert_eq!(
        frame.content_type, CT_JSON,
        "JSON request must yield CT_JSON response (Wave 4 regression guard)"
    );
    assert_eq!(
        frame.payload.first().copied(),
        Some(b'{'),
        "JSON payload must start with '{{'"
    );
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(v["value"], 1);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

/// OP_MGET + CT_JSON: response is (OP_GET_RESPONSE, CT_JSON, JSON
/// `{"result":{"alice":{"cnt":1}}}`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_mget_json_unchanged() {
    use beava_core::wire::{OP_GET_RESPONSE, OP_MGET};
    {
        let _g = SERVER_SERIALIZER_12_09_TCP_JSON
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
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
    assert_eq!(frame.content_type, CT_JSON);
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(v["result"]["alice"]["cnt"], 1);
    assert!(v["result"].get("bob").is_none());

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

/// OP_GET_MULTI + CT_JSON: response is (OP_GET_RESPONSE, CT_JSON, JSON
/// `{"result":{...}}`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_get_multi_json_unchanged() {
    use beava_core::wire::{OP_GET_MULTI, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_09_TCP_JSON
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
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
    assert_eq!(frame.content_type, CT_JSON);
    let v: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json");
    assert_eq!(v["result"]["alice"]["cnt"], 1);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}
