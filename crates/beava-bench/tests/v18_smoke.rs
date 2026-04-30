//! Smoke test for the Phase 18 v18 bench harness scaffold.
//!
//! Boots ServerV18, verifies admin /health is up, then (after Plan 18-05.1)
//! performs a real HTTP register + push through the data-plane event loop.

use beava_server::server::ServerV18;
use std::net::SocketAddr;

fn any_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Boot ServerV18, verify admin /health returns 200, shut down cleanly.
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

/// GREEN test (Plan 18-05.1): after serve() is wired, the HTTP event-plane
/// must respond to /push with 200 after a prior /register.
///
/// This test calls serve() on a tokio task, pushes one HTTP event, asserts 200.
#[tokio::test]
async fn v18_serve_data_plane_http_push_returns_200() {
    let sv18 = ServerV18::bind(any_addr(), any_addr(), any_addr())
        .await
        .expect("bind");

    let http_addr = sv18.http_addr();

    // Wrap serve() so we can shut it down after the assertions.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        sv18.serve(async move {
            let _ = shutdown_rx.await;
        })
        .await
    });

    // Give the event-plane a beat to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    // Register a minimal pipeline on the HTTP data-plane.
    // Format: `{ "nodes": [ {kind:event,...}, {kind:derivation,...} ] }`
    let register_payload = serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "SmokeEvent",
                "schema": {
                    "fields": { "event_time": "i64", "user_id": "str" },
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "SmokeAgg",
                "output_kind": "table",
                "upstreams": ["SmokeEvent"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": { "cnt": { "op": "count", "params": {} } }
                    }
                ],
                "schema": {
                    "fields": { "user_id": "str", "cnt": "i64" },
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let reg_resp = client
        .post(format!("http://{}/register", http_addr))
        .header("Content-Type", "application/json")
        .body(register_payload.to_string())
        .send()
        .await
        .expect("register request");
    assert!(
        reg_resp.status().is_success(),
        "register must succeed, got {}",
        reg_resp.status()
    );

    // Push one event.
    let event_payload = serde_json::json!({
        "user_id": "test-user-1",
        "event_time": 1_000_001
    });
    let push_resp = client
        .post(format!("http://{}/push/SmokeEvent", http_addr))
        .header("Content-Type", "application/json")
        .body(event_payload.to_string())
        .send()
        .await
        .expect("push request");
    assert_eq!(
        push_resp.status().as_u16(),
        200,
        "push must return 200, got {}",
        push_resp.status()
    );

    // Shut down.
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), serve_task).await;
}
