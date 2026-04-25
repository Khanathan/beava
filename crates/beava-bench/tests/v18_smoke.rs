//! Smoke test for the Phase 18 v18 bench harness scaffold.
//!
//! Boots ServerV18, verifies admin /health is up, then attempts to register
//! a tiny pipeline and push 10 events through the legacy Server path.
//!
//! Architecture note (2026-04-24):
//! ServerV18::bind() opens three ports — HTTP event-plane, TCP event-plane,
//! and admin (tokio/axum). However, Plans 18-05 and 18-06 (EventLoop wiring
//! into ServerV18::serve()) were not yet executed, so the event-plane listeners
//! are bound but never dispatch. This smoke test:
//!   - PASSES: admin /health on the ServerV18 admin port
//!   - DEFERRED: event-plane push/register until ServerV18::serve() is wired

use beava_server::server::ServerV18;
use std::net::SocketAddr;

fn any_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Boot ServerV18, verify admin /health returns 200, shut down cleanly.
///
/// This is the RED test: it only compiles once ServerV18 is importable and
/// has the expected API (bind, admin_addr, shutdown).
#[tokio::test]
async fn v18_admin_health_up_after_bind() {
    let sv18 = ServerV18::bind(any_addr(), any_addr(), any_addr())
        .await
        .expect("ServerV18::bind should succeed on localhost:0");

    let admin_addr = sv18.admin_addr();

    // Give the axum admin server a beat to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let resp = reqwest::get(format!("http://{}/health", admin_addr))
        .await
        .expect("admin /health request must succeed");

    assert_eq!(
        resp.status().as_u16(),
        200,
        "admin /health must return 200 after ServerV18::bind"
    );

    sv18.shutdown().await;
}

/// Verify that ServerV18 binds three distinct OS-assigned ports.
#[tokio::test]
async fn v18_all_three_ports_distinct_non_zero() {
    let sv18 = ServerV18::bind(any_addr(), any_addr(), any_addr())
        .await
        .expect("bind");

    let http_p = sv18.http_addr().port();
    let tcp_p = sv18.tcp_addr().port();
    let admin_p = sv18.admin_addr().port();

    assert_ne!(http_p, 0, "HTTP event-plane port must be non-zero");
    assert_ne!(tcp_p, 0, "TCP event-plane port must be non-zero");
    assert_ne!(admin_p, 0, "Admin port must be non-zero");
    assert_ne!(http_p, tcp_p, "HTTP and TCP ports must differ");
    assert_ne!(http_p, admin_p, "HTTP and admin ports must differ");
    assert_ne!(tcp_p, admin_p, "TCP and admin ports must differ");

    sv18.shutdown().await;
}
