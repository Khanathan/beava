//! Plan 12-08 Wave 4 — per-IO-worker BytesMutPool (D-C).
//!
//! Verifies that response encoders use a pooled BytesMut instead of
//! allocating a fresh BytesMut per response. Pre-Wave-4.b: the encoder
//! closures take `(WorkerProto, &mut BytesMut)` and extend the per-client
//! `write_buf` directly (whose internal `BufMut::put_slice` may grow via
//! reallocation). Post-Wave-4.b: the encoder takes
//! `(WorkerProto, &BytesMutPool, &mut BytesMut)` — acquires a pre-sized
//! buffer from the pool, writes into it, extends client_buf, releases.
//!
//! Test: push 1000 events; assert pool_acquire_count >= 1000 and
//! pool_alloc_count < 256 (pool size cap means after 256 acquires the
//! pool warms up and subsequent acquires recycle).

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PUSH};
use beava_runtime_core::bytes_pool::{pool_acquire_count, pool_alloc_count};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static SERVER_SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
async fn test_response_pool_used_by_encoder() {
    {
        let _g = SERVER_SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
    } // drop before awaits

    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18_with_workers(2).await;
    register_via_http(http_addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    const N_PUSHES: usize = 1000;
    let burst = build_push_burst(N_PUSHES);

    let allocs_before = pool_alloc_count();
    let acquires_before = pool_acquire_count();

    let sock = tokio::net::TcpStream::connect(tcp_addr)
        .await
        .expect("connect");
    sock.set_nodelay(true).ok();
    let (mut read_half, mut write_half) = sock.into_split();

    write_half
        .write_all(&burst)
        .await
        .expect("write_all 1000-push burst");

    let reader = tokio::spawn(async move {
        let mut rx = BytesMut::with_capacity(64 * 1024);
        let mut count = 0usize;
        let mut tmp = [0u8; 32 * 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while count < N_PUSHES && tokio::time::Instant::now() < deadline {
            while let Ok(Some(_)) = decode_frame(&mut rx, 4 * 1024 * 1024) {
                count += 1;
                if count >= N_PUSHES {
                    return count;
                }
            }
            tokio::select! {
                r = read_half.read(&mut tmp) => {
                    match r {
                        Ok(0) => return count,
                        Ok(n) => rx.extend_from_slice(&tmp[..n]),
                        Err(_) => return count,
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        count
    });

    let acked = tokio::time::timeout(Duration::from_secs(30), reader)
        .await
        .expect("reader timeout")
        .expect("reader panic");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let allocs_after = pool_alloc_count();
    let acquires_after = pool_acquire_count();
    let new_allocs = allocs_after - allocs_before;
    let new_acquires = acquires_after - acquires_before;

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
    std::env::remove_var("BEAVA_IO_THREADS");

    assert_eq!(acked, N_PUSHES, "expected {N_PUSHES} acks, got {acked}");
    assert!(
        new_acquires >= N_PUSHES as u64,
        "expected ≥ {N_PUSHES} pool acquires for {N_PUSHES} responses, got {new_acquires}"
    );
    // 2 IO workers × pool cap 256 = 512 total capacity. Real allocs are ≤ 512
    // initial fills; subsequent encoders recycle. Allow a bit of headroom for
    // any concurrent register-resp / health-shim acks and inter-worker drift.
    assert!(
        new_allocs < 600,
        "expected < 600 new BytesMut allocs (pool size cap × 2 workers + headroom), got {new_allocs}"
    );
}
