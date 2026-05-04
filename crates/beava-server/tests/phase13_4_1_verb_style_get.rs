//! Phase 13.4.1 Plan 01 — RED integration tests for verb-style `POST /get` +
//! `OP_GET (0x0020)` (D-01) and structured-400 rejection of legacy request
//! shapes (D-05).
//!
//! Six failing integration tests asserting the locked Phase 13.0 wire-spec
//! contract:
//!
//! - **D-01** — `POST /get` body is `{"table": "...", "key": "...", "features"?: [...]}`;
//!   response is a FLAT feature dict (no `{table, entity_id, features:{...}}`
//!   envelope). When `features` is present, response is narrowed to those
//!   keys only.
//! - **D-01** — `OP_GET (0x0020)` frame body is identical to the HTTP body;
//!   response frame is `OP_GET_RESPONSE (0x0023)` with body in the same
//!   content-type as the request (CT_JSON / CT_MSGPACK; carry-forward of
//!   Plan 12-09 D-A/D-B codec discipline).
//! - **D-05** — Receiving the legacy `{keys, features}` 2D-cell shape on
//!   `POST /get` returns `400 Bad Request` with body
//!   `{"error":{"code":"unsupported_request_shape","message":"<doc-hint>"}}`.
//! - **D-05** — Receiving the legacy `{feature, key}` single-feature shape on
//!   `POST /get` returns the same structured 400.
//!
//! ## TDD discipline (CLAUDE.md §Conventions)
//!
//! All 6 tests are RED at the time this file lands. The matching GREEN
//! commits live in Plan 13.4.1-04, which will:
//!   * Migrate `WireRequest::HttpGet` body parser from `{keys, features}` to
//!     `{table, key, features?}` (D-01).
//!   * Migrate `WireRequest::TcpGet` body parser from `{feature, key}` to
//!     `{table, key, features?}` (D-01).
//!   * Add `GlueResponse::UnsupportedRequestShape` variant + the legacy-shape
//!     detection ladder (D-05).
//!   * Flatten the response constructor to drop the `{table, entity_id,
//!     features:{...}}` envelope.
//!
//! ## Helpers
//!
//! `register_payload`, `register`, `push_seed_events`, and `tcp_send_recv_frame`
//! are copied verbatim from `phase13_4_op_batch_get.rs` (the closest analog).
//! After the seed pushes, `UserSpend(user_id="alice")` has `cnt=2, total=42.5`
//! and `MerchantSpend(merchant_id="acme")` has `merchant_cnt=3` — Plan 04
//! GREEN tests reuse the same fixture.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, CT_MSGPACK, OP_GET_RESPONSE};
use beava_server::testing::TestServer;
use bytes::{Bytes, BytesMut};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const OP_GET: u16 = 0x0020;

// ─── Shared helpers (copied from phase13_4_op_batch_get.rs) ────────────────

/// A two-table pipeline:
///   - `UserSpend(user_id) → cnt, total` driven by `Tx`.
///   - `MerchantSpend(merchant_id) → merchant_cnt` driven by `Tx`.
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

/// Push two events for alice (10 + 32.5) and one for bob (5) — all at acme.
/// After this, `UserSpend("alice") = {cnt: 2, total: 42.5}` and
/// `MerchantSpend("acme") = {merchant_cnt: 3}`.
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

/// Encode a single frame, write it to the TCP listener, and read exactly one
/// response frame back. Copied verbatim from `phase13_4_op_batch_get.rs:270-297`.
async fn tcp_send_recv_frame(
    addr: std::net::SocketAddr,
    op: u16,
    content_type: u8,
    payload: Bytes,
) -> Frame {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("tcp connect");
    let _ = stream.set_nodelay(true);

    let frame = Frame::new(op, content_type, payload);
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

// ─── Test 1 — D-01 verb-style POST /get returns FLAT feature dict ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_verb_style_get_returns_flat_dict() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // D-01 verb-style body: {table, key} (no `features` ⇒ all features for
    // the entity).
    let req = json!({"table": "UserSpend", "key": "alice"});
    let resp = ts.post_json("/get", &req).await.expect("POST /get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "verb-style POST /get must return 200; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");

    // FLAT row — feature dict IS the response, no envelope.
    assert_eq!(
        body["cnt"], 2,
        "alice cnt=2; FLAT dict shape (no envelope), got: {body:#}"
    );
    let alice_total = body["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    // No envelope wrapping — D-03 flat-row contract.
    assert!(
        body.get("table").is_none(),
        "FLAT response — no `table` envelope key; got: {body:#}"
    );
    assert!(
        body.get("entity_id").is_none(),
        "FLAT response — no `entity_id` envelope key; got: {body:#}"
    );
    assert!(
        body.get("features").is_none(),
        "FLAT response — no `features` envelope key; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — D-01 + D-06 features filter narrows response dict ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_verb_style_get_with_features_filter_narrows() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // D-01 + D-06: features filter narrows the response to only the
    // requested keys. Asking for ["cnt"] must NOT include `total`.
    let req = json!({
        "table": "UserSpend",
        "key": "alice",
        "features": ["cnt"]
    });
    let resp = ts.post_json("/get", &req).await.expect("POST /get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "narrowed POST /get must return 200; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        body,
        json!({"cnt": 2}),
        "narrowed response must be exactly {{\"cnt\": 2}}; got: {body:#}"
    );
    assert!(
        body.get("total").is_none(),
        "features filter narrows — `total` must be omitted; got: {body:#}"
    );
}

// ─── Test 3 — D-05 legacy {keys, features} shape rejected with 400 ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_legacy_keys_features_shape_rejected() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Legacy 2D-cell shape (Phase 12.x) must be REJECTED with 400 +
    // structured error. NEVER silently auto-translate (D-05 LOCKED).
    let req = json!({
        "keys": ["alice"],
        "features": ["cnt"]
    });
    let resp = ts.post_json("/get", &req).await.expect("POST /get");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "legacy {{keys, features}} shape must be rejected with 400; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        body["error"]["code"], "unsupported_request_shape",
        "expected error.code=unsupported_request_shape; got: {body:#}"
    );
    let message = body["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("error.message must be a string; got: {body:#}"));
    assert!(
        message.contains("POST /get expects {table, key, features?}"),
        "error.message must point at the verb-style schema; got: {message}"
    );
    assert!(
        message.contains("docs/http-api.md#post-get"),
        "error.message must contain the doc hint; got: {message}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — D-05 legacy {feature, key} shape rejected with 400 ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_legacy_feature_key_shape_rejected() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Legacy single-feature TCP shape sent over HTTP — also rejected.
    let req = json!({
        "feature": "cnt",
        "key": "alice"
    });
    let resp = ts.post_json("/get", &req).await.expect("POST /get");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "legacy {{feature, key}} shape must be rejected with 400; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(
        body["error"]["code"], "unsupported_request_shape",
        "expected error.code=unsupported_request_shape; got: {body:#}"
    );
    let message = body["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("error.message must be a string; got: {body:#}"));
    assert!(
        message.contains("docs/http-api.md#post-get"),
        "error.message must contain the doc hint; got: {message}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 5 — D-01 OP_GET TCP CT_JSON returns FLAT feature dict ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_op_get_returns_flat_dict_json() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    let tcp_addr = ts.tcp_addr().expect("tcp listener bound");
    let payload =
        serde_json::to_vec(&json!({"table": "UserSpend", "key": "alice"})).expect("serialise json");

    let resp_frame = tcp_send_recv_frame(tcp_addr, OP_GET, CT_JSON, Bytes::from(payload)).await;

    assert_eq!(
        resp_frame.op, OP_GET_RESPONSE,
        "TCP OP_GET response frame must be OP_GET_RESPONSE (0x0023); got {:#06x}",
        resp_frame.op
    );
    assert_eq!(
        resp_frame.content_type, CT_JSON,
        "json-in must produce json-out (D-A/D-B codec discipline); got 0x{:02x}",
        resp_frame.content_type
    );

    let body: serde_json::Value =
        serde_json::from_slice(&resp_frame.payload).expect("parse json body");

    // FLAT dict — same shape as the HTTP route per D-01.
    assert_eq!(body["cnt"], 2, "alice cnt=2; FLAT dict, got: {body:#}");
    let alice_total = body["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    // No envelope wrapping.
    assert!(
        body.get("table").is_none(),
        "FLAT response — no `table` envelope; got: {body:#}"
    );
    assert!(
        body.get("entity_id").is_none(),
        "FLAT response — no `entity_id` envelope; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 6 — D-01 OP_GET TCP CT_MSGPACK returns FLAT msgpack feature dict ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_op_get_returns_flat_dict_msgpack() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    let tcp_addr = ts.tcp_addr().expect("tcp listener bound");
    // CT_MSGPACK request body — msgpack-in must produce msgpack-out per
    // Plan 12-09 D-A/D-B codec discipline carry-forward.
    let payload = rmp_serde::to_vec_named(&json!({"table": "UserSpend", "key": "alice"}))
        .expect("serialise msgpack");

    let resp_frame = tcp_send_recv_frame(tcp_addr, OP_GET, CT_MSGPACK, Bytes::from(payload)).await;

    assert_eq!(
        resp_frame.op, OP_GET_RESPONSE,
        "TCP OP_GET response frame must be OP_GET_RESPONSE (0x0023); got {:#06x}",
        resp_frame.op
    );
    assert_eq!(
        resp_frame.content_type, CT_MSGPACK,
        "msgpack-in must produce msgpack-out (Plan 12-09 D-B); got 0x{:02x}",
        resp_frame.content_type
    );

    let body: serde_json::Value =
        rmp_serde::from_slice(&resp_frame.payload).expect("parse msgpack body");

    // FLAT dict — same shape as the JSON route per D-01.
    assert_eq!(
        body["cnt"], 2,
        "alice cnt=2; FLAT msgpack dict, got: {body:#}"
    );
    let alice_total = body["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );

    // No envelope wrapping.
    assert!(
        body.get("table").is_none(),
        "FLAT response — no `table` envelope; got: {body:#}"
    );
    assert!(
        body.get("entity_id").is_none(),
        "FLAT response — no `entity_id` envelope; got: {body:#}"
    );

    ts.shutdown().await.ok();
}
