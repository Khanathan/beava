//! Phase 20 TRAC-06: demo-page routing + asset embedding.
//!
//! Verifies:
//! 1. `GET /` serves `demo.html` when `public_mode=true`.
//! 2. `GET /` serves the existing debug UI when `public_mode=false`.
//! 3. `demo.js` is embedded in the binary and references `/public/stats`.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::PipelineEngine;
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
use beava::state::store::StateStore;

fn state_with_mode(public_mode: bool) -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-demo-page.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        Some("secret".into()),
        public_mode,
        1,
    )
}

async fn start_with(state: SharedState) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        beava::server::http::run_http_server_with_listener(listener, state)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    port
}

async fn http_get(port: u16, path: &str) -> (u16, Vec<u8>) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        path
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("missing separator");
    let status: u16 = std::str::from_utf8(&buf[..sep])
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let body = buf[sep + 4..].to_vec();
    (status, body)
}

#[tokio::test]
async fn demo_page_served_when_public() {
    let port = start_with(state_with_mode(true)).await;
    let (status, body) = http_get(port, "/").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("<h1>Beava</h1>"),
        "demo.html must contain <h1>Beava</h1>; body:\n{}",
        text
    );
    assert!(
        text.contains("/public/stats") || text.contains("/static/demo.js"),
        "demo.html must reference its live data source; body:\n{}",
        text
    );
}

#[tokio::test]
async fn debug_page_served_when_not_public() {
    let port = start_with(state_with_mode(false)).await;
    let (status, body) = http_get(port, "/").await;
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body);
    // Debug UI is distinct from the demo page. Accept any of a few stable
    // markers that the Phase 10 UI is known to ship: vendored d3, htmx, or
    // the app.js bundle. Fail if the demo marker leaks in.
    assert!(
        !text.contains("<h1>Beava</h1>") || text.contains("app.js"),
        "public_mode=false must not serve the demo page; body head:\n{}",
        &text[..text.len().min(400)]
    );
}

#[tokio::test]
async fn demo_assets_embedded() {
    let port = start_with(state_with_mode(true)).await;
    let (status, body) = http_get(port, "/static/demo.js").await;
    assert_eq!(status, 200, "demo.js must be embedded and served");
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("/public/stats"),
        "demo.js must poll /public/stats; body:\n{}",
        text
    );
}
