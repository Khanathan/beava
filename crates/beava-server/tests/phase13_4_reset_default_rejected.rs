//! Phase 13.4 Plan 08 (D-03 USER-LOCKED): integration tests for the
//! production-default-rejected path of `OP_RESET`.
//!
//! D-03 says reset is gated on `test_mode`. With NEITHER `BEAVA_TEST_MODE=1`
//! env var NOR `Config { test_mode: true }`, every reset call MUST return
//! 403 (HTTP) / 0xFFFF (TCP) with structured `reset_disabled_in_production`.
//!
//! This file isolates the production-default surface — covered loosely by
//! `phase13_4_reset_gated_env_var::env_var_value_other_than_1_does_not_enable`
//! and by `phase13_4_reset_gated_programmatic::reset_with_config_test_mode_false_and_no_env_var_returns_403`,
//! but the dedicated negative-path file makes the contract loud-and-clear:
//! v0 ships production-safe by default.
//!
//! TDD RED — Task 8.c — these tests assume the surface that Task 8.d lands.
//!
//! Coverage:
//! - Test 1 — `default_config_no_env_var_post_reset_returns_403_structured`:
//!   POST /reset returns 403 with the structured error body shape and the
//!   reason text mentions actionable opt-ins (`BEAVA_TEST_MODE` and
//!   `test_mode`).
//! - Test 2 — `tcp_default_config_op_reset_returns_0xFFFF_error_frame`: TCP
//!   `OP_RESET` (0x0040) returns the OP_ERROR_RESPONSE (0xFFFF) frame
//!   carrying `reset_disabled_in_production`.
//! - Test 3 — `default_config_get_reset_returns_405_method_not_allowed`:
//!   GET /reset returns 405 (router-level method check; reset is POST-only).
//!
//! All tests `#[serial_test::serial]` so no other parallel test mutates the
//! env var while this file runs.

#![cfg(feature = "testing")]

use beava_core::wire::{CT_JSON, OP_ERROR_RESPONSE, OP_RESET};
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

async fn boot_default() -> (
    SocketAddr,
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), beava_server::ServerError>>,
) {
    // Default config: production-safe (no env var, no programmatic flag).
    // We use `Persistence::Memory` to keep tests cheap; the gate logic does
    // not depend on persistence.
    let cfg = ServerV18Config {
        persistence: Persistence::Memory,
        test_mode: false,
        ..ServerV18Config::default()
    };
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

// ─── Test 1 — POST /reset returns 403 + structured body ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn default_config_no_env_var_post_reset_returns_403_structured() {
    // Defensive: ensure env var is not set (sibling tests should have
    // cleaned it up, but make this file resilient to runner ordering).
    std::env::remove_var("BEAVA_TEST_MODE");

    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_default().await;

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
        "default boot must return 403 for /reset; got {status} body={body}"
    );
    assert_eq!(
        body["error"]["code"], "reset_disabled_in_production",
        "expected error.code = reset_disabled_in_production, got {body}"
    );
    // Reason text MUST mention both opt-in paths so users see actionable
    // error text. This guards against future refactors stripping the
    // explanation.
    let reason = body["error"]["reason"].as_str().unwrap_or("").to_string();
    assert!(
        reason.contains("BEAVA_TEST_MODE"),
        "reason MUST mention BEAVA_TEST_MODE env var, got: {reason}"
    );
    assert!(
        reason.contains("test_mode"),
        "reason MUST mention test_mode kwarg, got: {reason}"
    );

    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 2 — TCP OP_RESET returns OP_ERROR_RESPONSE frame ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn tcp_default_config_op_reset_returns_0xffff_error_frame() {
    std::env::remove_var("BEAVA_TEST_MODE");

    let (_http_addr, tcp_addr, shutdown_tx, serve_task) = boot_default().await;

    let mut client = beava_server::testing::TcpClient::connect(tcp_addr)
        .await
        .expect("tcp connect");
    let resp = client
        .send_raw(OP_RESET, CT_JSON, Bytes::from_static(b"{}"))
        .await
        .expect("send OP_RESET");

    assert_eq!(
        resp.op, OP_ERROR_RESPONSE,
        "default boot TCP OP_RESET MUST return OP_ERROR_RESPONSE (0xFFFF), got {:#06x}",
        resp.op
    );
    let body: serde_json::Value =
        serde_json::from_slice(&resp.payload).expect("error body must be JSON");
    assert_eq!(
        body["error"]["code"], "reset_disabled_in_production",
        "TCP error body must have error.code = reset_disabled_in_production, got {body}"
    );

    let _ = client.close().await;
    shutdown_and_wait(shutdown_tx, serve_task).await;
}

// ─── Test 3 — GET /reset returns 405 Method Not Allowed ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn default_config_get_reset_returns_405_method_not_allowed() {
    std::env::remove_var("BEAVA_TEST_MODE");

    let (http_addr, _tcp_addr, shutdown_tx, serve_task) = boot_default().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/reset", http_addr))
        .send()
        .await
        .expect("get /reset");

    assert_eq!(
        resp.status().as_u16(),
        405,
        "GET /reset must return 405 (POST-only route); got {}",
        resp.status()
    );

    shutdown_and_wait(shutdown_tx, serve_task).await;
}
