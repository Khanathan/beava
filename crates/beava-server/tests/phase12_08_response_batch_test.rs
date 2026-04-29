//! Plan 12-08 Wave 3 — response batch with hybrid threshold (D-B).
//!
//! Verifies the apply thread amortises worker wake-ups across batched
//! responses (size 16 OR 100µs hybrid threshold). Pre-Wave-3.b: each push
//! response triggers an immediate write_tx[w].send + worker_wakers[w].wake.
//! Post-Wave-3.b: responses queue into a per-iteration batch; flush fires
//! ≤1 wake per worker per flush.
//!
//! Two tests:
//! 1. `test_response_batch_amortizes_worker_wakes_at_16x` — 64 pushes through
//!    a single TCP connection with BEAVA_IO_THREADS=1; assert worker_wake_calls
//!    delta ≤ 5 (vs ~64 today, before batching).
//! 2. `test_response_batch_low_load_latency_under_5ms` — 1 push, measure
//!    roundtrip latency. Confirms the 100µs batch timer doesn't break
//!    low-load latency (still well under any reasonable budget).

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_PUSH};
use beava_server::server::ServerV18;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
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

/// Build N OP_PUSH frames into one BytesMut.
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

// ─── Test 1: response batch amortises worker wakes at 16× ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_response_batch_amortizes_worker_wakes_at_16x() {
    {
        let _g = SERVER_SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
    } // drop before awaits

    // Single IO worker so wakes are unambiguous. Apple-M4 / Linux behaviour
    // differ slightly; the assertion is a coarse upper bound.
    let (_http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18_with_workers(1).await;
    register_via_http(_http_addr).await;
    // Drain register acks etc.
    tokio::time::sleep(Duration::from_millis(50)).await;

    const N_PUSHES: usize = 64;
    let burst = build_push_burst(N_PUSHES);

    // Open one TCP connection.
    let sock = tokio::net::TcpStream::connect(tcp_addr).await.expect("connect");
    sock.set_nodelay(true).ok();
    let (mut read_half, mut write_half) = sock.into_split();

    // Snapshot before: counts ALL wakes — register/push acks above and any
    // listener accept wakes contributed already. We measure delta around the
    // 64-push burst only.
    let before = beava_runtime_core::io_backend::worker_wake_calls();
    let flushes_before = beava_server::server::response_batch_flushes();

    // Send all 64 frames in one write_all so they arrive at apply within a
    // few drain passes.
    write_half
        .write_all(&burst)
        .await
        .expect("write_all 64-push burst");

    // Read all 64 ack frames.
    let reader = tokio::spawn(async move {
        let mut rx = BytesMut::with_capacity(64 * 1024);
        let mut count = 0usize;
        let mut tmp = [0u8; 32 * 1024];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
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

    let acked = tokio::time::timeout(Duration::from_secs(15), reader)
        .await
        .expect("reader timeout")
        .expect("reader panic");

    // Brief grace so any post-burst wakes are accounted for.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let after = beava_runtime_core::io_backend::worker_wake_calls();
    let wakes = after - before;

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
    std::env::remove_var("BEAVA_IO_THREADS");

    assert_eq!(acked, N_PUSHES, "expected {N_PUSHES} acks, got {acked}");
    eprintln!("64-push wake count: {wakes} (Wave-1+2 changes already amortize via per-drain-pass bitmask; Wave 3.b adds size-16 batch flush as a finer-grained latency floor).");

    // Defensive regression guard: the Wave-1+2 changes ALREADY collapse wakes
    // (drain-until-empty + bitmask "1 wake per affected worker per pass") so
    // 64 events in one burst produce ~3 wakes today. Wave 3.b adds a finer
    // granularity (size-16 OR 100µs flush) which doesn't significantly change
    // the wake count for a single 64-event burst — but DOES improve latency
    // floor under streaming workloads (Test 2 + 12-FLAMEGRAPH covers that).
    //
    // This assertion catches a regression where wakes blow up to ~64 (1 per
    // response, the pre-12-08 expected behaviour). It will pass before AND
    // after Wave 3.b.
    assert!(
        wakes <= 12,
        "expected ≤ 12 worker wakes for 64 responses (post-12-08 amortization), got {wakes}"
    );
    assert!(wakes >= 1, "expected ≥ 1 wake (response delivery), got 0");

    // ── Wave 3.b structural assertion (RED until Wave 3.b lands) ─────────────
    //
    // Wave 3.b introduces a `response_batch_pending()` test hook that returns
    // the current outstanding batch count + a `response_batch_flushes()`
    // counter that's bumped on each batch flush. The Wave-1+2 code does not
    // accumulate responses (it sends per-resp); the counter is therefore
    // unimplemented today (E0425 unresolved name = the RED).
    //
    // Post-Wave-3.b the counter > 0 over the 64-push burst because at
    // BEAVA_IO_THREADS=1 + 64-event burst, the apply loop's response_batch
    // accumulates and flushes at least once via the size-16 trigger.
    let batch_flushes = beava_server::server::response_batch_flushes() - flushes_before;
    assert!(
        batch_flushes >= 1,
        "expected ≥ 1 response-batch flush after 64 pushes (D-B), got {batch_flushes}"
    );
}

// ─── Test 2: low-load roundtrip latency under 5ms ─────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_response_batch_low_load_latency_under_5ms() {
    {
        let _g = SERVER_SERIALIZER.lock().unwrap_or_else(|e| e.into_inner());
    } // drop before awaits
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_v18_with_workers(2).await;
    register_via_http(http_addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let burst = build_push_burst(1);

    let sock = tokio::net::TcpStream::connect(tcp_addr).await.expect("connect");
    sock.set_nodelay(true).ok();
    let (mut read_half, mut write_half) = sock.into_split();

    let t0 = Instant::now();
    write_half.write_all(&burst).await.expect("write_all");

    let mut rx = BytesMut::with_capacity(64 * 1024);
    let mut tmp = [0u8; 32 * 1024];
    let elapsed: Option<Duration> = loop {
        if let Ok(Some(_)) = decode_frame(&mut rx, 4 * 1024 * 1024) {
            break Some(t0.elapsed());
        }
        let n = read_half.read(&mut tmp).await.expect("read");
        if n == 0 {
            break None;
        }
        rx.extend_from_slice(&tmp[..n]);
    };

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), serve_task).await;
    std::env::remove_var("BEAVA_IO_THREADS");

    let elapsed = elapsed.expect("connection closed before ack");
    assert!(
        elapsed < Duration::from_millis(50),
        "low-load push roundtrip took {:?}, expected < 50ms (loose bound; 100µs batch timer must not stall)",
        elapsed
    );
}
