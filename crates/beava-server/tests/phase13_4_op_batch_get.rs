//! Phase 13.4 Plan 03 — OP_BATCH_GET (0x0024) heterogeneous batched read.
//!
//! Tests the read-side counterpart to OP_PUSH for batched fan-out: clients
//! send heterogeneous (table, entity_id) tuples in a single frame; the server
//! returns a single OP_GET_RESPONSE (0x0023) frame whose JSON body holds
//! per-tuple results in request order.
//!
//! Per the plan must_haves:
//! - Heterogeneous batches mix tables (`UserSpend` + `MerchantSpend`) in one frame.
//! - Per-entry partial-failure shape: `{"results":[..., {table, entity_id,
//!   error: {code: "unknown_table", ...}}, ...]}` — the rest of the batch
//!   completes (no whole-frame 4xx).
//! - Empty batch (`{"requests": []}`) returns `{"results": []}` 200.
//! - HTTP `POST /batch_get` and TCP `OP_BATCH_GET (0x0024)` produce identical
//!   response bodies.
//!
//! TDD: RED until Task 3.d implements `dispatch_batch_get_sync`. Tests 1-4
//! fail because the Task 3.b stub returns `InternalError "not_yet_implemented"`.
//! Test 5 is `#[ignore]`d because it depends on Plan 13.4-09's `key_cols: []`
//! register-time acceptance (global-table sentinel routing per ADR-003);
//! Plan 13.4-09 removes the ignore.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_GET_RESPONSE};
use beava_server::testing::TestServer;
use bytes::{Bytes, BytesMut};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const OP_BATCH_GET: u16 = 0x0024;

// ─── Shared registration + push helpers ─────────────────────────────────────

/// A two-table pipeline:
///   - `UserSpend(user_id) → cnt, total` driven by `Tx`.
///   - `MerchantSpend(merchant_id) → cnt` driven by `Tx`.
fn register_payload() -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "merchant_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "UserSpend",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt": {"op": "count", "params": {}},
                        "total": {"op": "sum", "params": {"field": "amount"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64", "total": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            },
            {
                "kind": "derivation",
                "name": "MerchantSpend",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["merchant_id"],
                    "agg": {
                        "merchant_cnt": {"op": "count", "params": {}}
                    }
                }],
                "schema": {
                    "fields": {"merchant_id": "str", "merchant_cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["merchant_id"]
            }
        ]
    })
}

async fn register(ts: &TestServer) {
    let resp = ts
        .post_json("/register", &register_payload())
        .await
        .expect("register");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "register failed: status={status} body={body_text}"
    );
}

/// Push two events: alice spends 10 + 32.5 at acme; bob spends 5 at acme.
async fn push_seed_events(ts: &TestServer) {
    let events = [
        json!({"event_time": 1000, "user_id": "alice", "merchant_id": "acme", "amount": 10.0}),
        json!({"event_time": 1001, "user_id": "alice", "merchant_id": "acme", "amount": 32.5}),
        json!({"event_time": 1002, "user_id": "bob",   "merchant_id": "acme", "amount": 5.0}),
    ];
    for body in events {
        let resp = reqwest::Client::new()
            .post(format!("{}/push/Tx", ts.base_url()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("push");
        assert!(
            resp.status().is_success(),
            "push failed: status={}, body={body:?}",
            resp.status()
        );
    }
}

// ─── Test 1 — heterogeneous HTTP batch returns per-tuple results ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_batch_get_returns_per_tuple_results() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    let req = json!({
        "requests": [
            {"table": "UserSpend",     "entity_id": "alice"},
            {"table": "MerchantSpend", "entity_id": "acme"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "batch_get must return 200 on heterogeneous success"
    );
    let body: serde_json::Value = resp.json().await.expect("json body");

    let results = body
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(
        results.len(),
        2,
        "results must mirror request length, got: {body:#}"
    );

    // Order MUST mirror request order (per wire-spec contract).
    assert_eq!(results[0]["table"], "UserSpend");
    assert_eq!(results[0]["entity_id"], "alice");
    assert_eq!(
        results[0]["features"]["cnt"], 2,
        "alice cnt=2, got: {body:#}"
    );
    let alice_total = results[0]["features"]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    assert_eq!(results[1]["table"], "MerchantSpend");
    assert_eq!(results[1]["entity_id"], "acme");
    assert_eq!(
        results[1]["features"]["merchant_cnt"], 3,
        "acme merchant_cnt=3 (alice×2 + bob×1), got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — partial failure: unknown table → per-tuple error entry ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_batch_get_unknown_table_returns_partial_error() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    let req = json!({
        "requests": [
            {"table": "UserSpend",   "entity_id": "alice"},
            {"table": "DoesNotExist", "entity_id": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "partial-failure batch must still return 200 (rest of batch completes)"
    );
    let body: serde_json::Value = resp.json().await.expect("json body");

    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "got: {body:#}");

    // Index 0: UserSpend, alice — success
    assert_eq!(results[0]["table"], "UserSpend");
    assert_eq!(results[0]["entity_id"], "alice");
    assert_eq!(results[0]["features"]["cnt"], 2);

    // Index 1: DoesNotExist — per-tuple error (NOT whole-frame 4xx)
    assert_eq!(results[1]["table"], "DoesNotExist");
    assert_eq!(results[1]["entity_id"], "alice");
    assert_eq!(
        results[1]["error"]["code"], "unknown_table",
        "expected error.code=unknown_table, got: {body:#}"
    );
    assert!(
        results[1].get("features").is_none(),
        "error tuple must not carry a `features` field, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — empty batch returns 200 + empty results ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_batch_get_empty_returns_empty_results() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    // Don't push any events.

    let req = json!({ "requests": [] });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "empty batch must return 200, NOT 400"
    );
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        body,
        json!({ "results": [] }),
        "empty batch must return {{\"results\":[]}}, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — TCP OP_BATCH_GET produces same response shape as HTTP ─────────

/// Encode and write a single frame to a tokio TcpStream, then read exactly
/// one response frame via `decode_frame`.
async fn tcp_send_recv_frame(addr: std::net::SocketAddr, op: u16, payload: Bytes) -> Frame {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("tcp connect");
    let _ = stream.set_nodelay(true);

    let frame = Frame::new(op, CT_JSON, payload);
    let mut wire = BytesMut::new();
    encode_frame(&frame, &mut wire);
    stream.write_all(&wire).await.expect("write frame");

    // Read until we have at least one full frame.
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        if let Some(f) = decode_frame(&mut buf, 4 * 1024 * 1024).expect("decode") {
            return f;
        }
        let n = stream.read_buf(&mut buf).await.expect("read");
        if n == 0 {
            panic!(
                "tcp connection closed before full response frame; partial buf len = {}",
                buf.len()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_batch_get_returns_same_response_shape() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Issue HTTP /batch_get to capture the canonical body.
    let req_body = json!({
        "requests": [
            {"table": "UserSpend",     "entity_id": "alice"},
            {"table": "MerchantSpend", "entity_id": "acme"}
        ]
    });
    let http_resp = ts
        .post_json("/batch_get", &req_body)
        .await
        .expect("POST /batch_get");
    assert_eq!(http_resp.status().as_u16(), 200);
    let http_body: serde_json::Value = http_resp.json().await.expect("http json body");

    // Now hit the TCP listener with the same payload and OP_BATCH_GET (0x0024).
    let tcp_addr = ts.tcp_addr().expect("tcp listener bound");
    let payload_bytes = serde_json::to_vec(&req_body).expect("serialise");
    let resp_frame = tcp_send_recv_frame(tcp_addr, OP_BATCH_GET, Bytes::from(payload_bytes)).await;

    assert_eq!(
        resp_frame.op, OP_GET_RESPONSE,
        "TCP response frame op must be OP_GET_RESPONSE (0x0023), got {:#06x}",
        resp_frame.op
    );
    assert_eq!(resp_frame.content_type, CT_JSON);
    let tcp_body: serde_json::Value =
        serde_json::from_slice(&resp_frame.payload).expect("tcp body json");

    assert_eq!(
        tcp_body, http_body,
        "TCP and HTTP /batch_get bodies must be byte-equivalent;\nhttp:\n{http_body:#}\ntcp:\n{tcp_body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 5 — global table (entity_id = "" sentinel) — IGNORED until Plan 09 ──

/// Plan 13.4-09 (global-table sentinel routing per ADR-003) lands the
/// `key_cols: []` register-time acceptance. Until that plan ships, this test
/// fails at registration with `unsupported_node_kind` or `key_cols_required`.
/// Plan 13.4-09's closing commit removes the `#[ignore]` attribute below.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_batch_get_with_global_table_entity_id_empty() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Global aggregation: GroupBy with empty `keys` per ADR-003.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"event_time": "i64", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "GlobalCounter",
                "output_kind": "table",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": [],
                    "agg": {"events_total": {"op": "count", "params": {}}}
                }],
                "schema": {
                    "fields": {"events_total": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": []
            }
        ]
    });
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(
        resp.status().is_success(),
        "global-table register must succeed once Plan 13.4-09 lands key_cols:[]; got status={}",
        resp.status()
    );

    // Push 3 events so the global counter increments.
    for i in 0..3 {
        let body = json!({"event_time": 1000 + i, "amount": 1.0});
        let r = reqwest::Client::new()
            .post(format!("{}/push/Tx", ts.base_url()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("push");
        assert!(r.status().is_success());
    }

    // Batch-get the global table with the empty-string sentinel.
    let req = json!({
        "requests": [
            {"table": "GlobalCounter", "entity_id": ""}
        ]
    });
    let r = ts.post_json("/batch_get", &req).await.expect("batch_get");
    assert_eq!(r.status().as_u16(), 200);
    let body: serde_json::Value = r.json().await.expect("json");
    assert_eq!(body["results"][0]["table"], "GlobalCounter");
    assert_eq!(body["results"][0]["entity_id"], "");
    assert_eq!(
        body["results"][0]["features"]["events_total"], 3,
        "global counter must reflect 3 pushed events, got: {body:#}"
    );

    ts.shutdown().await.ok();
}
