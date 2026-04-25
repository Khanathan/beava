//! Phase 18 Plan 01 — Task 1.5 integration test.
//!
//! Tests that `Server::bind_v18(http_addr, tcp_addr, admin_addr)` stands up
//! all three listeners:
//!   - HTTP event-plane (hand-rolled loop)
//!   - TCP event-plane (hand-rolled loop)
//!   - Admin plane (tokio/axum on a separate port)
//!
//! Plan 18-07: feature flag removed; test runs unconditionally.

use beava_server::server::ServerV18;
use std::net::SocketAddr;

// ─── Helper ───────────────────────────────────────────────────────────────────

fn any_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

// ─── Task 1.5 RED test ────────────────────────────────────────────────────────

/// Verify that Server::bind_v18 successfully binds all three listeners and
/// reports non-zero OS-allocated ports.
#[tokio::test]
async fn bind_v18_all_three_listeners_come_up() {
    let sv18 = ServerV18::bind(
        any_addr(), // HTTP event-plane
        any_addr(), // TCP event-plane
        any_addr(), // Admin (tokio/axum)
    )
    .await
    .expect("bind_v18 should succeed");

    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();
    let admin_addr = sv18.admin_addr();

    // All three addresses must be on localhost with non-zero OS-assigned ports.
    assert_eq!(http_addr.ip().to_string(), "127.0.0.1");
    assert_ne!(
        http_addr.port(),
        0,
        "HTTP port must be OS-assigned non-zero"
    );

    assert_eq!(tcp_addr.ip().to_string(), "127.0.0.1");
    assert_ne!(tcp_addr.port(), 0, "TCP port must be OS-assigned non-zero");

    assert_eq!(admin_addr.ip().to_string(), "127.0.0.1");
    assert_ne!(
        admin_addr.port(),
        0,
        "Admin port must be OS-assigned non-zero"
    );

    // All three ports must be distinct.
    assert_ne!(
        http_addr.port(),
        tcp_addr.port(),
        "HTTP and TCP ports must differ"
    );
    assert_ne!(
        http_addr.port(),
        admin_addr.port(),
        "HTTP and Admin ports must differ"
    );
    assert_ne!(
        tcp_addr.port(),
        admin_addr.port(),
        "TCP and Admin ports must differ"
    );

    sv18.shutdown().await;
}

/// Admin /health endpoint responds 200 after bind_v18.
#[tokio::test]
async fn bind_v18_admin_health_responds_200() {
    let sv18 = ServerV18::bind(any_addr(), any_addr(), any_addr())
        .await
        .expect("bind_v18");

    let admin_addr = sv18.admin_addr();
    let url = format!("http://{}/health", admin_addr);

    // Give the admin server a moment to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = reqwest::get(&url).await.expect("admin /health request");
    assert_eq!(resp.status().as_u16(), 200, "admin /health must return 200");

    sv18.shutdown().await;
}
