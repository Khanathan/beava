//! Phase 13.4 Plan 08 (D-03 USER-LOCKED): integration tests for the
//! `OP_RESET` test_mode gate via the programmatic
//! `Config { test_mode: true }` path.
//!
//! D-03 says reset is gated on `test_mode`, computed at boot as the OR of
//! `cfg.test_mode || env::BEAVA_TEST_MODE=="1"`. This file covers the
//! programmatic Rust path (the in-process integration-test enabling route).
//!
//! TDD RED — Task 8.b — these tests assume the surface that Task 8.d
//! lands. Until Task 8.d these tests are RED (compile errors or 501s).
//!
//! Coverage:
//! - Test 1 — `reset_with_config_test_mode_true_succeeds_and_clears_state`:
//!   programmatic `ServerV18Config { test_mode: true, persistence:
//!   Persistence::Memory }` enables reset. Push, reset, push-without-register
//!   fails.
//! - Test 2 — `reset_with_config_test_mode_false_and_no_env_var_returns_403`:
//!   default config + env-var unset → reset returns 403 +
//!   `reset_disabled_in_production`.
//! - Test 3 — `reset_with_config_test_mode_true_overrides_env_var_unset`:
//!   programmatic `test_mode=true` alone (no env var) is enough — proves OR
//!   semantic from the programmatic side.
//!
//! All tests serialize via `serial_test` because Tests 2 and 3 mutate the
//! process-global env var via `std::env::remove_var` to ensure no other test
//! leaks `BEAVA_TEST_MODE=1` in.

#![cfg(feature = "testing")]

use beava_persistence::Persistence;
use beava_server::server::{ServerV18, ServerV18Config};
use std::net::SocketAddr;
use std::time::Duration;

// ─── Helpers (same shape as the env-var sibling test file) ────────────────

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

async fn wait_health_ok(http_addr: SocketAddr, deadline: Duration) {
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if let Ok(r) = client
            .get(format!("http://{}/health", http_addr))
            .send()
            .await
        {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("/health never returned 200 within {deadline:?}");
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
                }
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

async fn register(http_addr: SocketAddr) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/register", http_addr))
        .json(&register_payload())
        .send()
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register failed: {}", resp.status());
}

async fn push_one(http_addr: SocketAddr, user_id: &str, event_time_ms: i64) -> reqwest::StatusCode {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "event_time": event_time_ms,
        "user_id": user_id,
        "amount": 1.0,
    });
    let resp = client
        .post(format!("http://{}/push/Txn", http_addr))
        .json(&body)
        .send()
        .await
        .expect("push");
    resp.status()
}

async fn boot_with_config(
    cfg: ServerV18Config,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sv18 = ServerV18::bind_with_config(any, Some(any), any, cfg)
        .await
        .expect("bind_with_config");
    let http_addr = sv18.http_addr();
    let tcp_addr = sv18.tcp_addr();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let serve_task = tokio::spawn(async move {
        sv18.serve(async move {
            let _ = shutdown_rx.await;
        })
        .await
    });

    poll_until_listening(http_addr, Duration::from_secs(10)).await;
    poll_until_listening(tcp_addr, Duration::from_secs(10)).await;
    wait_health_ok(http_addr, Duration::from_secs(10)).await;

    (http_addr, shutdown_tx, serve_task)
}

async fn shutdown_and_wait(
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    serve_task: tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), serve_task).await;
}

// ─── Test 1 — programmatic test_mode=true enables reset ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn reset_with_config_test_mode_true_succeeds_and_clears_state() {
    // Defensive: clear env var so we know the programmatic flag is the
    // ONLY source of test_mode.
    std::env::remove_var("BEAVA_TEST_MODE");

    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: true, // programmatic gate
    };
    let (http_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

    register(http_addr).await;
    let s = push_one(http_addr, "alice", 1000).await;
    assert!(s.is_success(), "push failed: {s}");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/reset", http_addr))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("post /reset");
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        status, 200,
        "POST /reset must return 200 with Config{{ test_mode: true }}, got {status} body={body}"
    );
    assert_eq!(
        body["reset"], true,
        "reset body must contain reset=true, got {body}"
    );

    // After reset, push without re-register MUST fail.
    let s2 = push_one(http_addr, "bob", 2000).await;
    assert!(
        !s2.is_success(),
        "post-reset push without re-register MUST fail (registry empty), got {s2}"
    );

    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 2 — Config{test_mode:false} + no env var → 403 ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn reset_with_config_test_mode_false_and_no_env_var_returns_403() {
    std::env::remove_var("BEAVA_TEST_MODE");

    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: false, // production-by-default
    };
    let (http_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/reset", http_addr))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("post /reset");
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        status, 403,
        "default config (no env, no programmatic flag) MUST return 403, got {status} body={body}"
    );
    assert_eq!(
        body["error"]["code"], "reset_disabled_in_production",
        "expected error.code = reset_disabled_in_production, got {body}"
    );

    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 3 — programmatic alone is enough (env var unset) ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn reset_with_config_test_mode_true_overrides_env_var_unset() {
    std::env::remove_var("BEAVA_TEST_MODE");

    // Programmatic test_mode=true with env var explicitly unset — proves the
    // OR semantic from the programmatic side. cfg.test_mode || false → true.
    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: true,
    };
    let (http_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{}/reset", http_addr))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("post /reset");
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        status, 200,
        "Config{{ test_mode: true }} alone MUST enable reset (env var not required), \
         got {status} body={body}"
    );
    assert_eq!(body["reset"], true, "reset body must contain reset=true");

    shutdown_and_wait(shutdown_tx, serve_task).await;
}
