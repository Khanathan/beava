//! Phase 13.4 Plan 08 (D-03 USER-LOCKED): integration tests for the
//! `OP_RESET` test_mode gate via the `BEAVA_TEST_MODE=1` shell env var.
//!
//! D-03 says reset is gated on `test_mode`, computed at boot as the OR of
//! `cfg.test_mode || env::BEAVA_TEST_MODE=="1"`. This file covers the
//! env-var path (the "ops/CI" enabling route).
//!
//! TDD RED — Task 8.a — these tests assume the following surface that
//! Task 8.d will land:
//! - `beava_core::wire::OP_RESET = 0x0040`
//! - `WireRequest::TcpReset` + `WireRequest::HttpReset` variants
//! - `Route::Reset` (POST /reset only; GET → 405)
//! - `dispatch_reset_sync` arm in `apply_shard.rs`
//! - boot-time `effective_test_mode` resolution on AppState
//!
//! Until Task 8.d lands all three tests are RED (compile errors or 501s).
//!
//! Coverage:
//! - Test 1 — `reset_with_env_var_enabled_succeeds_and_clears_state`: HTTP
//!   POST /reset with `BEAVA_TEST_MODE=1` returns 200; subsequent state is
//!   gone; subsequent push without re-register fails.
//! - Test 2 — `tcp_reset_with_env_var_enabled_succeeds`: TCP `OP_RESET`
//!   (0x0040) frame succeeds with the env var set; response frame is
//!   `OP_GET_RESPONSE` (0x0023, the generic JSON success frame) carrying
//!   `{"reset":true,...}`.
//! - Test 3 — `env_var_value_other_than_1_does_not_enable`: BEAVA_TEST_MODE=true
//!   (NOT `=1`) does NOT enable test_mode; POST /reset → 403 +
//!   `reset_disabled_in_production`. Per D-03, the check is exactly `== "1"`.
//!
//! **Env-var test isolation:** `std::env::set_var` is process-global, so all
//! tests in this file (and in `phase13_4_reset_gated_programmatic.rs` /
//! `phase13_4_reset_default_rejected.rs`) carry `#[serial_test::serial]` to
//! serialise execution.

#![cfg(feature = "testing")]

use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_GET_RESPONSE, OP_RESET};
use beava_persistence::Persistence;
use beava_server::server::{ServerV18, ServerV18Config};
use bytes::Bytes;
use std::net::SocketAddr;
use std::time::Duration;

// ─── Helpers ──────────────────────────────────────────────────────────────

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
    assert!(
        resp.status().is_success(),
        "register failed: {}",
        resp.status()
    );
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

    (http_addr, tcp_addr, shutdown_tx, serve_task)
}

async fn shutdown_and_wait(
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    serve_task: tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), serve_task).await;
}

/// RAII guard that removes BEAVA_TEST_MODE on drop, even on panic.
struct EnvGuard {
    var: &'static str,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var(self.var);
    }
}

// ─── Test 1 — env-var enables; POST /reset clears state ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn reset_with_env_var_enabled_succeeds_and_clears_state() {
    std::env::set_var("BEAVA_TEST_MODE", "1");
    let _g = EnvGuard {
        var: "BEAVA_TEST_MODE",
    };

    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: false, // env var alone enables — proves OR semantic
    };
    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

    register(http_addr).await;
    let s1 = push_one(http_addr, "alice", 1000).await;
    let s2 = push_one(http_addr, "alice", 1001).await;
    let s3 = push_one(http_addr, "alice", 1002).await;
    assert!(s1.is_success(), "push 1 failed: {s1}");
    assert!(s2.is_success(), "push 2 failed: {s2}");
    assert!(s3.is_success(), "push 3 failed: {s3}");

    // POST /reset with empty body
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
        "POST /reset must return 200 with env var set, got {status} body={body}"
    );
    assert_eq!(
        body["reset"], true,
        "reset response body must contain `reset: true`, got {body}"
    );

    // After reset, push without re-register MUST fail (unknown event).
    let s4 = push_one(http_addr, "bob", 2000).await;
    assert!(
        !s4.is_success(),
        "after reset, push without re-register MUST fail (descriptors deregistered); got {s4}"
    );

    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 2 — TCP OP_RESET succeeds with env var set ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn tcp_reset_with_env_var_enabled_succeeds() {
    std::env::set_var("BEAVA_TEST_MODE", "1");
    let _g = EnvGuard {
        var: "BEAVA_TEST_MODE",
    };

    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: false,
    };
    let (_http_addr, tcp_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

    // Use the TestServer-flavoured TcpClient harness to talk framed wire.
    let mut client = beava_server::testing::TcpClient::connect(tcp_addr)
        .await
        .expect("tcp connect");

    let resp = client
        .send_raw(OP_RESET, CT_JSON, Bytes::from_static(b"{}"))
        .await
        .expect("send OP_RESET");
    assert_eq!(
        resp.op, OP_GET_RESPONSE,
        "expected OP_GET_RESPONSE (success frame) for TCP reset, got {:#06x}",
        resp.op
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resp.payload).expect("response body must be JSON");
    assert_eq!(body["reset"], true, "TCP reset body must have reset=true");

    let _ = client.close().await;
    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 3 — env var value other than "1" does NOT enable ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn env_var_value_other_than_1_does_not_enable() {
    // D-03 says the check is exactly `== "1"`. Set a different truthy value.
    std::env::set_var("BEAVA_TEST_MODE", "true");
    let _g = EnvGuard {
        var: "BEAVA_TEST_MODE",
    };

    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: false,
    };
    let (http_addr, tcp_addr, shutdown_tx, serve_task) = boot_with_config(cfg).await;

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
        "BEAVA_TEST_MODE=true (NOT `=1`) MUST be rejected; got {status} body={body}"
    );
    assert_eq!(
        body["error"]["code"], "reset_disabled_in_production",
        "expected error.code = reset_disabled_in_production, got {body}"
    );

    // TCP path should also reject (0xFFFF error frame).
    let mut tcp_client = beava_server::testing::TcpClient::connect(tcp_addr)
        .await
        .expect("tcp connect");
    let resp = tcp_client
        .send_raw(OP_RESET, CT_JSON, Bytes::from_static(b"{}"))
        .await
        .expect("send OP_RESET");
    assert_eq!(
        resp.op, OP_ERROR_RESPONSE,
        "expected OP_ERROR_RESPONSE (0xFFFF) when env var != \"1\", got {:#06x}",
        resp.op
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resp.payload).expect("error body must be JSON");
    assert_eq!(body["error"]["code"], "reset_disabled_in_production");

    let _ = tcp_client.close().await;
    shutdown_and_wait(shutdown_tx, serve_task).await;
}
