//! Phase 13.4 Plan 08 (D-03 USER-LOCKED): integration tests for the
//! `OP_RESET` test_mode gate.
//!
//! D-03 says reset is gated on `test_mode`. This file covers the
//! ops/CI-enabling route (originally tested via `BEAVA_TEST_MODE=1`
//! env-set; Phase 13.5.3 rewrite uses the per-server
//! `ServerV18Config.test_mode` field set programmatically). The
//! complementary programmatic-API path (`Server::new(Config { test_mode:
//! true })`) is tested in `phase13_4_reset_gated_programmatic.rs`.
//!
//! Phase 13.5.3 rewrite (workspace test determinism):
//! - The two "env var enables" tests now construct
//!   `ServerV18Config { test_mode: true, ..default() }` directly. The
//!   bind path no longer ORs in `env::BEAVA_TEST_MODE == "1"` —
//!   production env-reading happens once in `from_env()` at boot.
//! - The third test (`env_var_value_other_than_1_does_not_enable`)
//!   tested env-var-string parsing semantics; that's now covered by
//!   `crates/beava-server/src/server.rs::env_var_plumbing_tests::test_from_env_test_mode_strict_eq_one`
//!   (unit test inside src/, where the architectural tripwire
//!   `phase13_5_3_no_env_var_pokes_in_tests.rs` does not walk). It is
//!   removed from this integration test file as a consequence.
//!
//! Coverage:
//! - Test 1 — `reset_with_test_mode_enabled_succeeds_and_clears_state`:
//!   HTTP POST /reset with `cfg.test_mode = true` returns 200;
//!   subsequent state is gone; subsequent push without re-register fails.
//! - Test 2 — `tcp_reset_with_test_mode_enabled_succeeds`: TCP `OP_RESET`
//!   (0x0040) frame succeeds with the cfg flag set; response frame is
//!   `OP_GET_RESPONSE` (0x0023, the generic JSON success frame) carrying
//!   `{"reset":true,...}`.

#![cfg(feature = "testing")]

use beava_core::wire::{CT_JSON, OP_GET_RESPONSE, OP_RESET};
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

// Phase 13.5.3: EnvGuard struct removed — no env mutation needed.

// ─── Test 1 — cfg.test_mode enables; POST /reset clears state ──────────────
//
// Phase 13.5.3 rewrite: env-var path replaced by direct cfg.test_mode = true.
// The serial_test attribute is also removed — Phase 13.5.3 closed the
// process-global env mutation that required serialization.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_with_test_mode_enabled_succeeds_and_clears_state() {
    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: true,
        ..ServerV18Config::default()
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

// ─── Test 2 — TCP OP_RESET succeeds with cfg.test_mode set ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_reset_with_test_mode_enabled_succeeds() {
    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: true,
        ..ServerV18Config::default()
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

// ─── Test 3 (deleted Phase 13.5.3) — env-var-string-parsing semantics ─────
//
// The original `env_var_value_other_than_1_does_not_enable` test asserted
// that `BEAVA_TEST_MODE=true` (NOT `=1`) does NOT enable test_mode per
// Phase 13.4 D-03 USER-LOCKED's strict `== "1"` check. That contract is
// still enforced; the test now lives at
// `crates/beava-server/src/server.rs::env_var_plumbing_tests::test_from_env_test_mode_strict_eq_one`
// where the env-var read site lives, instead of in this integration test
// file (which would force a `set_var` call and re-introduce the
// cross-test pollution Phase 13.5.3 closed).
