//! Phase 13.4 Plan 02 — GET response payload flat-dict / drop the envelope.
//!
//! Per Phase 13.0-15 wire-spec, the multi-feature batch read flattens the per-entity
//! response so the JSON tree no longer carries an outer envelope key. The previous
//! shape was `{"result": {entity_id: {feature: value}}}`; the new shape is
//! `{entity_id: {feature: value}}`.
//!
//! Single-feature `GET /get/:feature/:key` (which returns `{"value": <val>}`)
//! is UNCHANGED — only the multi-feature batched read flips. Cold-start
//! (no events for the entity yet) returns `{}` per the wire-spec contract
//! (omitting absent keys, NOT a 404).
//!
//! TDD: this is the RED gate — current encoder still wraps in `{"result": ...}`,
//! so the populated, cold-start, and TCP tests must all fail before Plan 02
//! Task 2.b drops the envelope in `runtime_core_glue::dispatch_get_batch`.
//!
//! Plan-doc note: the plan said "feature_query.rs::format_get_response" but the
//! actual envelope construction lives in
//! `crates/beava-server/src/runtime_core_glue.rs::dispatch_get_batch` (line ~441
//! at the time of writing). The intent is unambiguous; see EXECUTOR-DEVIATION-02.md.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_GET_MULTI, OP_GET_RESPONSE};
use beava_server::testing::TestServer;
use bytes::{Bytes, BytesMut};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ─── Shared registration + push helpers ─────────────────────────────────────

/// A tiny pipeline with two features (`cnt = count()`, `total = sum(amount)`)
/// keyed by `user_id`. Used by every test in this file.
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
            }
        ]
    })
}

async fn register(ts: &TestServer) {
    let resp = ts
        .post_json("/register", &register_payload())
        .await
        .expect("register");
    assert!(
        resp.status().is_success(),
        "register failed: status={}",
        resp.status()
    );
}

async fn push_alice_two_events(ts: &TestServer) {
    for (i, amount) in [(1i64, 10.0f64), (2, 32.5)] {
        let body = json!({"event_time": 1000 + i, "user_id": "alice", "amount": amount});
        let resp = reqwest::Client::new()
            .post(format!("{}/push/Tx", ts.base_url()))
            .json(&body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("push");
        assert!(
            resp.status().is_success(),
            "push {i} failed: status={}",
            resp.status()
        );
    }
}

// ─── Test 1 — populated entity returns flat dict (no `result` envelope) ─────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_get_returns_flat_dict_no_row_envelope() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_alice_two_events(&ts).await;

    let req = json!({"keys": ["alice"], "features": ["cnt", "total"]});
    let resp = ts.post_json("/get", &req).await.expect("post /get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json body");

    // The load-bearing assertion: NO `result` envelope key.
    assert!(
        body.get("result").is_none(),
        "result envelope must be absent (Phase 13.0-15 wire-spec), got: {body:#}"
    );
    // The historic `row` envelope should also stay absent — per the plan's
    // wording, we want a flat dict end-to-end.
    assert!(
        body.get("row").is_none(),
        "row envelope must be absent, got: {body:#}"
    );

    // Flat-dict body: alice's row is at the top level.
    assert_eq!(body["alice"]["cnt"], 2, "expected cnt=2, got: {body:#}");
    let total = body["alice"]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("total must be a number, got: {body:#}"));
    assert!(
        (total - 42.5).abs() < 1e-9,
        "expected total=42.5, got total={total}, body={body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — cold-start returns `{}` (flat dict; absent entity omitted) ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_get_cold_start_returns_flat_dict_with_defaults() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    // No events pushed.

    let req = json!({"keys": ["nobody"], "features": ["cnt", "total"]});
    let resp = ts.post_json("/get", &req).await.expect("post /get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json body");

    // No envelope.
    assert!(
        body.get("result").is_none(),
        "result envelope must be absent on cold-start, got: {body:#}"
    );
    assert!(
        body.get("row").is_none(),
        "row envelope must be absent on cold-start, got: {body:#}"
    );

    // Per wire-spec ("Cold-start returns `{}`") the absent entity is omitted
    // from the response; the response body itself is the empty flat dict.
    // Note: the plan's example wording suggested cold-start defaults like
    // `{cnt: 0, total: 0}`, but the existing dispatch path silently omits
    // empty entities (matches the wire-spec). See EXECUTOR-DEVIATION-02.md.
    assert!(
        body.is_object(),
        "cold-start body must be a JSON object, got: {body:#}"
    );
    assert!(
        body.get("nobody").is_none(),
        "absent entity must be omitted from the flat-dict body, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — unknown feature returns a structured error ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_get_unknown_feature_returns_structured_error() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;

    let req = json!({"keys": ["alice"], "features": ["does_not_exist"]});
    let resp = ts.post_json("/get", &req).await.expect("post /get");

    // The plan's "must_have" wanted a 404 + `{"error":{"code":"unknown_table"}}`.
    // The current /get path takes feature names (not table names) and reports
    // `feature_not_found` via the existing `internal_error` 500 path. Aligning
    // the response code to a structured 404 + `unknown_table` is Plan 04's
    // remit (verb-style routes change the request shape from {keys, features}
    // to {table, key}). For Plan 02 we keep the existing semantic and only
    // assert the structured-error contract: status >= 400 with a JSON body
    // containing `error.code`. See EXECUTOR-DEVIATION-02.md.
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert!(
        !(200..300).contains(&status),
        "unknown feature must NOT be a 2xx success, got status={status}, body={body_text}"
    );
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("error body must be JSON");
    let code = body["error"]["code"]
        .as_str()
        .unwrap_or_else(|| panic!("structured error.code missing, body={body:#}"));
    let reason = body["error"]["reason"].as_str().unwrap_or_default();
    assert!(
        code == "unknown_table"
            || code == "internal_error" && reason.contains("feature_not_found"),
        "expected unknown_table (Plan 04+) or internal_error/feature_not_found (Plan 02 baseline), \
         got code={code}, reason={reason}, body={body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — TCP GET multi returns same flat-dict shape ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_get_returns_same_flat_dict_shape() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_alice_two_events(&ts).await;
    let tcp_addr = ts.tcp_addr().expect("tcp listener");

    // OP_GET_MULTI (0x0022) is the multi-feature batched TCP read whose body
    // mirrors the HTTP POST /get shape — `{keys, features}`.
    let payload = serde_json::to_vec(&json!({
        "keys": ["alice"],
        "features": ["cnt", "total"]
    }))
    .expect("serialize tcp body");

    let mut sock = tokio::net::TcpStream::connect(tcp_addr)
        .await
        .expect("tcp connect");
    let mut tx_buf = BytesMut::new();
    encode_frame(
        &Frame::new(OP_GET_MULTI, CT_JSON, Bytes::from(payload)),
        &mut tx_buf,
    );
    sock.write_all(&tx_buf).await.expect("tcp write");

    let mut rx_buf = BytesMut::with_capacity(64 * 1024);
    let mut tmp = [0u8; 8192];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let frame = loop {
        if let Some(f) = decode_frame(&mut rx_buf, 4 * 1024 * 1024).expect("decode") {
            break f;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("tcp read deadline; rx_buf so far: {} bytes", rx_buf.len());
        }
        tokio::select! {
            r = sock.read(&mut tmp) => {
                let n = r.expect("tcp read");
                if n == 0 {
                    panic!("connection closed before complete frame");
                }
                rx_buf.extend_from_slice(&tmp[..n]);
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    };

    assert_eq!(
        frame.op, OP_GET_RESPONSE,
        "expected OP_GET_RESPONSE for OP_GET_MULTI"
    );
    let body: serde_json::Value = serde_json::from_slice(&frame.payload).expect("json body");

    // Both transports MUST emit the same wire shape (no envelope).
    assert!(
        body.get("result").is_none(),
        "TCP response must NOT carry the result envelope, got: {body:#}"
    );
    assert!(
        body.get("row").is_none(),
        "TCP response must NOT carry the row envelope, got: {body:#}"
    );
    assert_eq!(
        body["alice"]["cnt"], 2,
        "expected alice.cnt=2 over TCP, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 5 — single-feature HTTP GET still uses {"value": ...} envelope ───

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_feature_http_get_still_returns_value_envelope() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_alice_two_events(&ts).await;

    let url = format!("{}/get/cnt/alice", ts.base_url());
    let resp = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("get single");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json body");

    // Single-feature path UNCHANGED — `{"value": <val>}` stays per the
    // wire-spec. Only the multi-feature batch path flips.
    assert_eq!(
        body["value"], 2,
        "single-feature GET MUST still return {{\"value\": ...}}, got: {body:#}"
    );
    // The batch envelope keys must NOT appear on the single-feature path.
    assert!(
        body.get("result").is_none(),
        "single-feature GET must not carry result envelope, got: {body:#}"
    );
    assert!(
        body.get("row").is_none(),
        "single-feature GET must not carry row envelope, got: {body:#}"
    );

    ts.shutdown().await.ok();
}
