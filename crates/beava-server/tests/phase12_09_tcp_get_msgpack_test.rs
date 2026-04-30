//! Plan 12-09 Wave 3 — TCP `/get` msgpack round-trip via the full mio data plane.
//!
//! Boots `ServerV18`, registers Txn -> TxnAgg(cnt) + pushes 10 events, then
//! sends OP_GET_MULTI / OP_MGET / OP_GET frames with `content_type=CT_MSGPACK`
//! and msgpack-encoded bodies. Asserts:
//! 1. Response frame `op == OP_GET_RESPONSE`
//! 2. Response frame `content_type == CT_MSGPACK` (★ this is the Wave 3 RED
//!    assertion — Wave 1 still hardcodes CT_JSON in encode_glue_response_tcp)
//! 3. `rmp_serde::from_slice::<serde_json::Value>(&payload)` round-trips back to
//!    the expected `{result: {alice: {cnt: 10}}}` shape (or `{value: 10}` for
//!    OP_GET single)
//! 4. Payload first byte is NOT `b'{'` (msgpack-tagged maps start 0x80-0x8f
//!    or 0xde/0xdf — definitively non-JSON)
//!
//! RED until Wave 3 Task 3.b changes encode_glue_response_tcp to emit
//! `*format` instead of constant CT_JSON, and Wave 4 Task 4.b plumbs
//! body_format through apply_shard.rs's TCP /get arms.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_MSGPACK};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Serializer pattern: `{ let _g = MUTEX.lock(); }` — drop before awaits.
/// Mirrors phase12_07_get_via_mio_test.rs:20.
static SERVER_SERIALIZER_12_09_MSGPACK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Boot ServerV18 + return (http_addr, tcp_addr, shutdown_tx, serve_task).
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
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
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

    for i in 0..10 {
        let body = serde_json::json!({"event_time": 1000 + i, "user_id": "alice", "amount": 42.0});
        let resp = client
            .post(format!("http://{}/push/Txn", http_addr))
            .json(&body)
            .send()
            .await
            .expect("push");
        assert!(resp.status().is_success(), "push failed");
    }
}

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

// ─── Tests ────────────────────────────────────────────────────────────────────

/// OP_GET_MULTI + CT_MSGPACK request body → response frame must be
/// (OP_GET_RESPONSE, CT_MSGPACK, msgpack payload). RED today because
/// `encode_glue_response_tcp` hardcodes CT_JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_get_multi_msgpack_round_trip() {
    use beava_core::wire::{OP_GET_MULTI, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_09_MSGPACK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let req = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let mp_body = rmp_serde::to_vec_named(&req).expect("msgpack encode");
    let frame = tcp_send_and_recv(tcp_addr, OP_GET_MULTI, CT_MSGPACK, &mp_body).await;

    assert_eq!(
        frame.op, OP_GET_RESPONSE,
        "expected OP_GET_RESPONSE for OP_GET_MULTI"
    );
    assert_eq!(
        frame.content_type, CT_MSGPACK,
        "expected CT_MSGPACK content_type byte (0x02), got 0x{:02x}",
        frame.content_type
    );
    let first_byte = frame.payload.first().copied();
    assert_ne!(
        first_byte,
        Some(b'{'),
        "msgpack payload must NOT start with '{{' (that would be JSON), got first byte 0x{:02x?}",
        first_byte
    );
    let v: serde_json::Value =
        rmp_serde::from_slice(&frame.payload).expect("payload decodes as msgpack");
    assert_eq!(
        v["result"]["alice"]["cnt"], 10,
        "expected result.alice.cnt=10, got {v:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

/// OP_MGET + CT_MSGPACK with `{feature, keys}` body → response is
/// (OP_GET_RESPONSE, CT_MSGPACK, msgpack `{result: {alice: {cnt: 10}}}`).
/// RED today because apply_shard.rs:262 has `body_format: _`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_mget_msgpack_round_trip() {
    use beava_core::wire::{OP_GET_RESPONSE, OP_MGET};
    {
        let _g = SERVER_SERIALIZER_12_09_MSGPACK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let req = serde_json::json!({"feature": "cnt", "keys": ["alice", "bob"]});
    let mp_body = rmp_serde::to_vec_named(&req).expect("msgpack encode");
    let frame = tcp_send_and_recv(tcp_addr, OP_MGET, CT_MSGPACK, &mp_body).await;

    assert_eq!(frame.op, OP_GET_RESPONSE);
    assert_eq!(frame.content_type, CT_MSGPACK);
    let v: serde_json::Value =
        rmp_serde::from_slice(&frame.payload).expect("payload decodes as msgpack");
    assert_eq!(v["result"]["alice"]["cnt"], 10);
    assert!(
        v["result"].get("bob").is_none(),
        "bob should be omitted (no state), got {v:#}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}

/// OP_GET (single feature/key) + CT_MSGPACK with `{feature, key}` body →
/// response is (OP_GET_RESPONSE, CT_MSGPACK, msgpack `{value: 10}`).
/// RED today because apply_shard.rs:235 has `body_format: _`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_get_single_msgpack_round_trip() {
    use beava_core::wire::{OP_GET, OP_GET_RESPONSE};
    {
        let _g = SERVER_SERIALIZER_12_09_MSGPACK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    }
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18().await;
    register_and_push_for_alice(http_addr).await;

    let req = serde_json::json!({"feature": "cnt", "key": "alice"});
    let mp_body = rmp_serde::to_vec_named(&req).expect("msgpack encode");
    let frame = tcp_send_and_recv(tcp_addr, OP_GET, CT_MSGPACK, &mp_body).await;

    assert_eq!(frame.op, OP_GET_RESPONSE);
    assert_eq!(frame.content_type, CT_MSGPACK);
    let v: serde_json::Value =
        rmp_serde::from_slice(&frame.payload).expect("payload decodes as msgpack");
    assert_eq!(v["value"], 10, "expected value=10, got {v:#}");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
}
