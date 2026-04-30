//! Plan 12-08 Wave 2 — drain-until-empty (D-D).
//!
//! Verifies the apply thread drains the entire `read_rx` channel in a single
//! loop iteration (no DRAIN_CAP=1024 ceiling). Pre-Wave-2.b apply re-entered
//! the listener-poll cadence after every 1024 items; post-Wave-2.b it drains
//! until `try_recv()` returns `Err(Empty)`.
//!
//! Test shape: push 4096 events through a single TCP connection in one
//! `write_all` and assert that the highest observed drain count
//! (`APPLY_MAX_DRAIN_PER_ITER`) is > 1024.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PUSH};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SERVER_SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Boot ServerV18 with `BEAVA_IO_THREADS=8` so 4096 events spread across many
/// parser workers and arrive at apply quickly.
async fn boot_v18_with_workers(
    n_workers: usize,
) -> (
    SocketAddr,
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    std::env::set_var("BEAVA_IO_THREADS", n_workers.to_string());
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

async fn register_via_http(http_addr: SocketAddr) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/register", http_addr))
        .json(&register_payload())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed");
}

/// Build N OP_PUSH frames (JSON envelope, distinct user_ids) into one BytesMut.
fn build_push_burst(n: usize) -> Bytes {
    let mut buf = BytesMut::new();
    for i in 0..n {
        let envelope = serde_json::json!({
            "event": "Txn",
            "body": {"event_time": 1000 + i as i64, "user_id": format!("user_{i}"), "amount": 1.0}
        });
        let body = serde_json::to_vec(&envelope).expect("json envelope");
        let frame = Frame {
            op: OP_PUSH,
            content_type: CT_JSON,
            payload: Bytes::from(body),
        };
        encode_frame(&frame, &mut buf);
    }
    buf.freeze()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_apply_drains_more_than_1024_items_per_iteration() {
    {
        let _g = SERVER_SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
    } // drop before awaits

    // 8 IO worker threads — enough to surge events into read_rx faster than
    // apply can drain in a single try_recv pass with the old DRAIN_CAP=1024.
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18_with_workers(8).await;
    register_via_http(http_addr).await;

    // Send 4096 frames in a single write_all over one connection. The OS
    // will fragment the 4096 events across many TCP segments; the parser
    // worker picks them up in chunks and the apply thread drains the
    // resulting RingItems. Pre-Wave-2.b: apply drains in chunks of 1024
    // (re-entering the outer loop after each 1024). Post-Wave-2.b: apply
    // drains all queued items until try_recv returns Err(Empty).
    const N_PUSHES: usize = 4096;
    let burst = build_push_burst(N_PUSHES);

    let sock = tokio::net::TcpStream::connect(tcp_addr)
        .await
        .expect("connect");
    sock.set_nodelay(true).ok();

    // Spawn a reader task to drain ack frames concurrently.
    let (mut read_half, mut write_half) = sock.into_split();
    let reader = tokio::spawn(async move {
        let mut rx = BytesMut::with_capacity(64 * 1024);
        let mut count = 0usize;
        let mut tmp = [0u8; 32 * 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while count < N_PUSHES && tokio::time::Instant::now() < deadline {
            // Drain any complete frames already buffered.
            while let Ok(Some(_frame)) = decode_frame(&mut rx, 4 * 1024 * 1024) {
                count += 1;
                if count >= N_PUSHES {
                    return count;
                }
            }
            tokio::select! {
                r = read_half.read(&mut tmp) => {
                    match r {
                        Ok(0) => return count, // EOF
                        Ok(n) => rx.extend_from_slice(&tmp[..n]),
                        Err(_) => return count,
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        count
    });

    write_half
        .write_all(&burst)
        .await
        .expect("write_all 4096-push burst");
    // No close — keep the connection alive so reader keeps reading until
    // it has all 4096 acks.

    let acked = tokio::time::timeout(Duration::from_secs(60), reader)
        .await
        .expect("reader timeout")
        .expect("reader panic");

    // Allow stragglers + idle drain pass to update the max_drain counter.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let max_drain = beava_server::server::apply_max_drain_per_iter();

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
    std::env::remove_var("BEAVA_IO_THREADS");

    assert_eq!(acked, N_PUSHES, "expected {N_PUSHES} acks, got {acked}");
    assert!(
        max_drain > 1024,
        "expected apply_max_drain_per_iter > 1024 (D-D drain-until-empty), got {max_drain} \
         (DRAIN_CAP=1024 still capping the drain)"
    );
}
