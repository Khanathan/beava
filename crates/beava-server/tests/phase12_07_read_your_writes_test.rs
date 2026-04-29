//! Plan 12-07 Wave 7 Task 7.c — read-your-writes via the apply thread.
//!
//! Asserts that a single client connection that POSTs /push then immediately
//! issues /get sees the pushed event in the response. The apply thread is the
//! single writer; both push and get serialise on the same RingItem channel,
//! so by construction the get observes the post-push state.
//!
//! Two tests cover both wire transports through the same memory model:
//! - HTTP keep-alive (single reqwest::Client; HTTP/1.1 keep-alive on)
//! - TCP single socket (one OP_PUSH then one OP_GET on the same connection)

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Serializer pattern: `{ let _g = MUTEX.lock(); }` — drop before awaits.
/// Mirrors phase18_04_6_integration_test.rs:23 etc.
static SERVER_SERIALIZER_12_07_RYW: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    let wal_dir = tempfile::tempdir().expect("wal");
    let snap_dir = tempfile::tempdir().expect("snap");
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
    // Wait for /health = 200 to confirm the apply thread is consuming.
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

// ─── HTTP read-your-writes ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_http_push_then_get_on_same_connection_sees_pushed_event() {
    {
        let _g = SERVER_SERIALIZER_12_07_RYW
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_v18().await;

    // Single keep-alive client (reqwest::Client reuses HTTP/1.1 connections).
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .build()
        .expect("client");
    // Register.
    let resp = client
        .post(format!("http://{}/register", http_addr))
        .json(&register_payload())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success());

    // PUSH then immediately GET on the same client. The apply thread serialises
    // both commands on the same RingItem channel, so the get must see the push.
    let body = serde_json::json!({"event_time": 1000, "user_id": "alice", "amount": 1.0});
    let resp = client
        .post(format!("http://{}/push/Txn", http_addr))
        .json(&body)
        .send()
        .await
        .expect("push");
    assert!(resp.status().is_success(), "push failed");

    let resp = client
        .get(format!("http://{}/get/cnt/alice", http_addr))
        .send()
        .await
        .expect("get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["value"], 1,
        "read-your-writes failed: expected value=1 immediately after push, got {body:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

// ─── TCP read-your-writes (single socket, OP_PUSH then OP_GET) ───────────────

async fn tcp_send_one_frame_and_read_one(
    sock: &mut tokio::net::TcpStream,
    op: u16,
    ct: u8,
    payload: &[u8],
    rx_buf: &mut BytesMut,
) -> Frame {
    let mut tx = BytesMut::new();
    encode_frame(
        &Frame::new(op, ct, Bytes::copy_from_slice(payload)),
        &mut tx,
    );
    sock.write_all(&tx).await.expect("write");
    let mut tmp = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(frame)) = decode_frame(rx_buf, 4 * 1024 * 1024) {
            return frame;
        }
        tokio::select! {
            r = sock.read(&mut tmp) => {
                let n = r.expect("read");
                if n == 0 { panic!("conn closed"); }
                rx_buf.extend_from_slice(&tmp[..n]);
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    panic!("no TCP frame received within deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_op_push_then_op_get_on_same_connection_sees_pushed_event() {
    use beava_core::wire::{OP_GET, OP_GET_RESPONSE, OP_PUSH};
    {
        let _g = SERVER_SERIALIZER_12_07_RYW
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;

    // Register over HTTP (the simpler path; TCP register works too but isn't
    // the test scope).
    let client = reqwest::Client::new();
    client
        .post(format!("http://{}/register", http_addr))
        .json(&register_payload())
        .send()
        .await
        .expect("register");

    // Single TCP socket — OP_PUSH then OP_GET, both reads on the same buffer.
    let mut sock = tokio::net::TcpStream::connect(tcp_addr).await.expect("tcp");
    let mut rx_buf = BytesMut::with_capacity(64 * 1024);

    // OP_PUSH.
    let push_envelope =
        br#"{"event":"Txn","body":{"event_time":1000,"user_id":"alice","amount":1.0}}"#;
    let ack =
        tcp_send_one_frame_and_read_one(&mut sock, OP_PUSH, CT_JSON, push_envelope, &mut rx_buf)
            .await;
    // OP_PUSH ack reflects back as OP_PUSH echo per encode_glue_response_tcp.
    assert_eq!(ack.op, OP_PUSH, "expected OP_PUSH ack, got {:#06x}", ack.op);

    // Immediately OP_GET on the same socket — ApplyShard FIFO serialises
    // the get behind the push, so we must see cnt=1.
    let resp_frame = tcp_send_one_frame_and_read_one(
        &mut sock,
        OP_GET,
        CT_JSON,
        br#"{"feature":"cnt","key":"alice"}"#,
        &mut rx_buf,
    )
    .await;
    assert_eq!(
        resp_frame.op, OP_GET_RESPONSE,
        "expected OP_GET_RESPONSE, got {:#06x}",
        resp_frame.op
    );
    let v: serde_json::Value = serde_json::from_slice(&resp_frame.payload).expect("json");
    assert_eq!(
        v["value"], 1,
        "TCP read-your-writes failed: expected value=1, got {v:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}
