//! Plan 12-07 Wave 5.5 — /health must return 200 on the mio data-plane port.
//!
//! `python/benches/read_bench.py:203` polls `/health` on the data-plane HTTP
//! port at startup and treats anything other than 200 as "server not ready".
//! For the bench to drive `target/release/beava` post-Wave-6 migration,
//! `/health` MUST live on the mio listener.
//!
//! RED until Task 5.5.b adds a `Route::Health` shim into `router.rs` and
//! plumbs it through `http_listener` → `apply_shard` → `encode_glue_response_http`.

#![cfg(feature = "testing")]

use beava_server::server::ServerV18;
use std::net::SocketAddr;
use std::time::Duration;

/// Global serializer for tests that boot a full ServerV18 — same pattern as
/// phase18_04_6 / phase18_04_7 to avoid OS scheduler thrash when multiple
/// ServerV18 instances boot concurrently.
static SERVER_SERIALIZER_12_07_HEALTH: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Poll until a TCP listener is up at `addr` (kernel-accept level).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_endpoint_returns_200_on_mio_data_plane_port() {
    {
        let _g = SERVER_SERIALIZER_12_07_HEALTH.lock().unwrap();
    }

    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let wal_dir = tempfile::tempdir().expect("wal dir");
    let snap_dir = tempfile::tempdir().expect("snap dir");
    let sv18 = ServerV18::bind(any, any, any).await.expect("bind");
    let http_addr = sv18.http_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let wp = wal_dir.path().to_path_buf();
    let sp = snap_dir.path().to_path_buf();
    let serve = tokio::spawn(async move {
        sv18.serve_with_dirs(
            async move {
                let _ = shutdown_rx.await;
            },
            wp,
            sp,
        )
        .await
    });

    poll_until_listening(http_addr, Duration::from_secs(5)).await;

    let url = format!("http://{}/health", http_addr);
    let resp = reqwest::get(&url).await.expect("GET /health");
    let status = resp.status();
    let body_bytes = resp.bytes().await.expect("read body");
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert_eq!(
        status.as_u16(),
        200,
        "GET {} expected 200 (read_bench.py contract); got {} body={}",
        url,
        status,
        body_str
    );

    let _ = shutdown_tx.send(());
    let _ = serve.await;
}
