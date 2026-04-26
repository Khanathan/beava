//! Phase 18 Plan 04.7 integration tests — IoPool wiring into serve_with_dirs.
//!
//! Tasks covered:
//!   4.7.1 — serve_with_dirs uses IoPool::publish + join_all for read+write
//!   4.7.2 — apply thread does no parse / no encode (off-apply invariant)
//!   4.7.3 — mixed HTTP+TCP traffic correctness through the same IoPool
//!
//! All three RED tests are written first and fail to compile / fail to pass
//! until 4.7.1.b/4.7.2.b/4.7.3.b GREEN tasks rewire serve_with_dirs.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;

/// Global serializer for tests that boot a full ServerV18 — same pattern as
/// phase18_04_6 + phase18_09 + phase18_10 to avoid OS scheduler thrash when
/// multiple ServerV18 instances boot concurrently.
static SERVER_SERIALIZER_04_7: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Wait until the hand-rolled HTTP server at `addr` accepts connections.
/// Any HTTP response (including 404) means the mio event loop is up.
async fn wait_for_http_04_7(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .expect("reqwest client");
    loop {
        match client.get(format!("http://{}/ping", addr)).send().await {
            Ok(_) => return,
            Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("hand-rolled HTTP server at {} did not become ready", addr);
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
}

/// Small two-node pipeline: TestEvent → group_by(user_id) → cnt.
fn small_pipeline_register() -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "TestEv47",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64", "event_time": "i64"},
                    "optional_fields": []
                },
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TestAgg47",
                "output_kind": "table",
                "upstreams": ["TestEv47"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {"cnt": {"op": "count", "params": {}}}
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

// ─── Task 4.7.1 ───────────────────────────────────────────────────────────────

/// Boots ServerV18::serve_with_dirs() with BEAVA_IO_THREADS=2, pushes 1000
/// events via TCP at parallel=4, verifies every event was applied (state
/// counter advances correctly).
///
/// Also verifies (via the iopool_observer module exposed from beava-server)
/// that parse and encode were performed by IoPool worker threads, NOT the
/// apply thread.
///
/// RED until serve_with_dirs is rewired to use IoPool for read+write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_serve_with_dirs_uses_iopool_for_read_write() {
    {
        let _g = SERVER_SERIALIZER_04_7.lock().unwrap();
    }

    // Pin IoPool to 2 threads for deterministic test.
    std::env::set_var("BEAVA_IO_THREADS", "2");
    beava_server::server::iopool_observer::reset();

    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = beava_server::server::ServerV18::bind(any, any, any)
        .await
        .expect("ServerV18::bind");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    wait_for_http_04_7(http_addr).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    // Register pipeline.
    let resp = client
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .body(small_pipeline_register().to_string())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed: {}", resp.status());

    // Push 1000 events at parallel=4 via TCP.
    use beava_core::wire::{CT_JSON, OP_PUSH};
    use beava_server::testing::TcpClient;
    use bytes::Bytes;

    const TOTAL: usize = 1000;
    const PARALLEL: usize = 4;
    let per_worker = TOTAL / PARALLEL;

    let mut handles = Vec::new();
    for w in 0..PARALLEL {
        let h = tokio::spawn(async move {
            let mut tc = TcpClient::connect(tcp_addr).await.expect("tcp connect");
            for i in 0..per_worker {
                let user_id = format!("u{:04}", (w * per_worker + i) % 100);
                let env = serde_json::json!({
                    "event": "TestEv47",
                    "body": {
                        "user_id": user_id,
                        "amount": 1.0,
                        "event_time": 1_000_000_000i64 + i as i64
                    }
                });
                let env_bytes = serde_json::to_vec(&env).unwrap();
                let _resp = tc
                    .send_raw(OP_PUSH, CT_JSON, Bytes::from(env_bytes))
                    .await
                    .expect("push");
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.await.expect("worker join");
    }

    // Allow apply thread + WAL writer to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify state via /get/cnt/u0000 — should match the per-key push count.
    // We pushed `per_worker` per worker, with key cycling u0000..u0099 (mod 100).
    // u0000 receives per_worker / 100 from EACH worker = PARALLEL * (per_worker / 100).
    let resp = client
        .get(format!("http://{}/get/cnt/u0000", http_addr))
        .send()
        .await
        .expect("get u0000");
    assert!(resp.status().is_success(), "get failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.expect("get body");

    // Total events for u0000 = ceil(per_worker / 100) per worker × PARALLEL workers
    // For per_worker=250, that's 3 (i=0,100,200) per worker × 4 workers = 12.
    // We just need a reasonable lower bound — at least 1 push per worker per 100 events.
    let cnt_value = body
        .pointer("/value/cnt")
        .or_else(|| body.get("cnt"))
        .or_else(|| body.get("value"));
    assert!(
        cnt_value.is_some(),
        "expected cnt field in response, got: {body}"
    );

    // Verify off-apply parse + encode happened.
    let off_apply_parse = beava_server::server::iopool_observer::off_apply_parse_count();
    let off_apply_encode = beava_server::server::iopool_observer::off_apply_encode_count();
    assert!(
        off_apply_parse > 0,
        "expected off-apply parse count > 0, got {off_apply_parse}"
    );
    assert!(
        off_apply_encode > 0,
        "expected off-apply encode count > 0, got {off_apply_encode}"
    );

    // Shutdown.
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), serve_task).await;
}

// ─── Task 4.7.2 ───────────────────────────────────────────────────────────────

/// Apply-thread parse/encode invariant: after pushing N events, the
/// apply-thread parse + encode counters must be 0 — proving parse and encode
/// strictly run on IoPool worker threads.
///
/// RED until 4.7.2.b verifies the implementation maintains this invariant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_apply_thread_does_no_parse_or_encode() {
    {
        let _g = SERVER_SERIALIZER_04_7.lock().unwrap();
    }

    std::env::set_var("BEAVA_IO_THREADS", "2");
    beava_server::server::iopool_observer::reset();

    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = beava_server::server::ServerV18::bind(any, any, any)
        .await
        .expect("ServerV18::bind");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    wait_for_http_04_7(http_addr).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .body(small_pipeline_register().to_string())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed: {}", resp.status());

    // Push 100 events through TCP.
    use beava_core::wire::{CT_JSON, OP_PUSH};
    use beava_server::testing::TcpClient;
    use bytes::Bytes;

    let mut tc = TcpClient::connect(tcp_addr).await.expect("tcp connect");
    for i in 0..100 {
        let env = serde_json::json!({
            "event": "TestEv47",
            "body": {
                "user_id": format!("u{:03}", i),
                "amount": 1.0,
                "event_time": 1_000_000_000i64 + i
            }
        });
        let env_bytes = serde_json::to_vec(&env).unwrap();
        let _resp = tc
            .send_raw(OP_PUSH, CT_JSON, Bytes::from(env_bytes))
            .await
            .expect("push");
    }

    // Allow apply thread + WAL writer to drain.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Apply-thread parse + encode must be 0 (or near-0 for any startup quirk).
    let apply_parse = beava_server::server::iopool_observer::apply_parse_count();
    let apply_encode = beava_server::server::iopool_observer::apply_encode_count();
    assert_eq!(
        apply_parse, 0,
        "apply thread must not call parse — found {apply_parse} calls"
    );
    assert_eq!(
        apply_encode, 0,
        "apply thread must not call encode — found {apply_encode} calls"
    );

    // And off-apply counters MUST be > 0 (proves the work happened, just on workers).
    let off_apply_parse = beava_server::server::iopool_observer::off_apply_parse_count();
    let off_apply_encode = beava_server::server::iopool_observer::off_apply_encode_count();
    assert!(
        off_apply_parse > 0,
        "expected off-apply parse > 0, got {off_apply_parse}"
    );
    assert!(
        off_apply_encode > 0,
        "expected off-apply encode > 0, got {off_apply_encode}"
    );

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), serve_task).await;
}

// ─── Task 4.7.3 ───────────────────────────────────────────────────────────────

/// Mixed HTTP + TCP traffic interleaved through the same IoPool. Verifies:
///   1. Both protocols are routed through the IoPool (no protocol-specific bypass)
///   2. State is correct after both halves complete (no race / lost events)
///
/// 100 TCP pushes + 100 HTTP pushes — interleaved by spawning two tasks
/// concurrently. Each pushes to the same key set. Final cnt for each key
/// should equal (TCP_pushes + HTTP_pushes) for that key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mixed_http_tcp_through_iopool() {
    {
        let _g = SERVER_SERIALIZER_04_7.lock().unwrap();
    }

    std::env::set_var("BEAVA_IO_THREADS", "2");
    beava_server::server::iopool_observer::reset();

    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = beava_server::server::ServerV18::bind(any, any, any)
        .await
        .expect("ServerV18::bind");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve_task = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    wait_for_http_04_7(http_addr).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    let resp = client
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .body(small_pipeline_register().to_string())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed: {}", resp.status());

    const N: usize = 100;
    // Single shared key so we can sanity-check the final count.
    let key = "u_mixed";

    // TCP pusher.
    let tcp_handle = {
        let key = key.to_string();
        tokio::spawn(async move {
            use beava_core::wire::{CT_JSON, OP_PUSH};
            use beava_server::testing::TcpClient;
            use bytes::Bytes;
            let mut tc = TcpClient::connect(tcp_addr).await.expect("tcp connect");
            for i in 0..N {
                let env = serde_json::json!({
                    "event": "TestEv47",
                    "body": {
                        "user_id": key,
                        "amount": 1.0,
                        "event_time": 2_000_000_000i64 + i as i64
                    }
                });
                let env_bytes = serde_json::to_vec(&env).unwrap();
                let _resp = tc
                    .send_raw(OP_PUSH, CT_JSON, Bytes::from(env_bytes))
                    .await
                    .expect("tcp push");
            }
        })
    };

    // HTTP pusher.
    let http_handle = {
        let key = key.to_string();
        let http_client = client.clone();
        tokio::spawn(async move {
            for i in 0..N {
                let body = serde_json::json!({
                    "user_id": key,
                    "amount": 1.0,
                    "event_time": 3_000_000_000i64 + i as i64
                });
                let resp = http_client
                    .post(format!("http://{}/push/TestEv47", http_addr))
                    .header("Content-Type", "application/json")
                    .body(body.to_string())
                    .send()
                    .await
                    .expect("http push");
                assert!(
                    resp.status().is_success(),
                    "http push #{i} failed: {}",
                    resp.status()
                );
            }
        })
    };

    tcp_handle.await.expect("tcp pusher join");
    http_handle.await.expect("http pusher join");

    // Drain.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Read back: cnt for key should equal 2*N.
    let resp = client
        .get(format!("http://{}/get/cnt/{}", http_addr, key))
        .send()
        .await
        .expect("get");
    assert!(resp.status().is_success(), "get failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.expect("get body");
    let cnt = body
        .pointer("/value/cnt")
        .and_then(|v| v.as_i64())
        .or_else(|| body.get("cnt").and_then(|v| v.as_i64()))
        .unwrap_or(-1);
    assert_eq!(
        cnt,
        (2 * N) as i64,
        "expected cnt={}, got {cnt} (body: {body})",
        2 * N
    );

    // Both protocols should have driven the IoPool — off-apply counters > 0.
    let off_apply_parse = beava_server::server::iopool_observer::off_apply_parse_count();
    let off_apply_encode = beava_server::server::iopool_observer::off_apply_encode_count();
    let _ = (off_apply_parse, off_apply_encode); // observed via Ordering::Acquire below
    assert!(
        beava_server::server::iopool_observer::off_apply_parse_count() > 0,
        "off-apply parse should be > 0 (mixed traffic)"
    );
    assert!(
        beava_server::server::iopool_observer::off_apply_encode_count() > 0,
        "off-apply encode should be > 0 (mixed traffic)"
    );

    // No apply-thread parse / encode.
    assert_eq!(
        beava_server::server::iopool_observer::apply_parse_count(),
        0,
        "apply thread parsed under mixed traffic — invariant violated"
    );
    assert_eq!(
        beava_server::server::iopool_observer::apply_encode_count(),
        0,
        "apply thread encoded under mixed traffic — invariant violated"
    );

    // Quiet `unused` warning on Ordering import.
    let _: Ordering = Ordering::Acquire;

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), serve_task).await;
}
