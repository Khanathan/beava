//! Phase 13.4.1 Plan 02 — RED integration tests for verb-style
//! `POST /batch_get` + `OP_BATCH_GET (0x0024)` (D-02), FLAT row response shape
//! (D-03), and per-entry `features` filter with omit-on-absent semantics
//! (D-06).
//!
//! Six failing integration tests asserting the locked Phase 13.0 wire-spec
//! contract:
//!
//! - **D-02** — `POST /batch_get` body is
//!   `{"requests": [{"table": "...", "key": "...", "features"?: [...]}, ...]}`.
//!   `OP_BATCH_GET` frame body is identical. Per-entry `features` filter is
//!   independent — different entries can request different feature subsets.
//! - **D-03** — `/batch_get` response is
//!   `{"results": [<flat_dict_for_request_1>, <flat_dict_for_request_2>, ...]}`
//!   where each entry is the raw feature-name-to-value dict — NO
//!   `{table, entity_id, features:{...}}` envelope. Error entries are
//!   `{"error": {"code": "...", "message": "..."}}` with no envelope.
//! - **D-06** — When the request entry's `features` field is present, the
//!   response dict is narrowed to those keys — features that aren't present
//!   on the entity are OMITTED (NOT set to `null`, NOT errored).
//! - **D-06 distinction** — A feature name that is NOT present in the registry
//!   at all (typo) triggers a WHOLE-BATCH reject upfront with
//!   `feature_not_found` (4xx, no `results` array in body). Per-entity
//!   sparsity (registry-known feature absent on this entity) omits silently
//!   per row inside a 200 `results` array.
//!
//! ## TDD discipline (CLAUDE.md §Conventions)
//!
//! All 6 tests are RED at the time this file lands. The matching GREEN
//! commits live in Plan 13.4.1-04, which will:
//!   * Migrate `BatchGetReqEntry` from `{table, entity_id}` to
//!     `{table, key, features?: Vec<String>}` (D-02).
//!   * Flatten the per-row response constructor in `dispatch_batch_get_sync`
//!     to drop the `{table, entity_id, features:{...}}` envelope (D-03).
//!   * Add the per-entry features-filter narrowing pass at the
//!     `feature_map.insert` site (D-06).
//!   * Wire the upfront `feature_not_found` whole-batch reject for
//!     registry-typo features (D-06 distinction).
//!
//! ## Helpers
//!
//! `register_payload`, `register`, `push_seed_events`, and `tcp_send_recv_frame`
//! mirror the analog in `phase13_4_op_batch_get.rs`. After the seed pushes,
//! `UserSpend("alice") = {cnt: 2, total: 42.5}`,
//! `UserSpend("bob") = {cnt: 1, total: 5.0}`, and
//! `MerchantSpend("acme") = {merchant_cnt: 3}`. Plan 04 GREEN tests reuse the
//! same fixture.

#![cfg(feature = "testing")]

use beava_core::wire::{decode_frame, encode_frame, Frame, CT_JSON, OP_GET_RESPONSE};
use beava_server::testing::TestServer;
use bytes::{Bytes, BytesMut};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const OP_BATCH_GET: u16 = 0x0024;

// ─── Shared helpers (mirrors phase13_4_op_batch_get.rs) ────────────────────

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

/// A pipeline with a SECOND aggregation `UserHits(user_id) → hit_count` driven
/// by a NON-PUSHED event `Hit`. Combined with `register_payload` events,
/// `UserHits("alice")` will exist in the registry/descriptor but have NO data
/// (alice has not been touched by any `Hit` event), so D-06 omit-on-absent
/// behaviour can be observed.
fn register_payload_with_user_hits() -> serde_json::Value {
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
                "kind": "event",
                "name": "Hit",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str"
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
                "name": "UserHits",
                "output_kind": "table",
                "upstreams": ["Hit"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "hit_count": {"op": "count", "params": {}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "hit_count": "i64"},
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
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "register failed: status={status} body={body_text}"
    );
}

async fn register_with_user_hits(ts: &TestServer) {
    let resp = ts
        .post_json("/register", &register_payload_with_user_hits())
        .await
        .expect("register");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "register-with-user-hits failed: status={status} body={body_text}"
    );
}

/// Push two events for alice (10 + 32.5) and one for bob (5) — all at acme.
/// After this:
///   `UserSpend("alice") = {cnt: 2, total: 42.5}`
///   `UserSpend("bob")   = {cnt: 1, total: 5.0}`
///   `MerchantSpend("acme") = {merchant_cnt: 3}`
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
/// response frame back. Mirrors the helper in `phase13_4_op_batch_get.rs:270-297`.
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

// ─── Test 1 — D-02 + D-03 verb-style batch_get returns FLAT rows ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_batch_get_returns_flat_rows() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // D-02 verb-style body: per-entry {table, key} (no features filter →
    // return all features for the entity).
    let req = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice"},
            {"table": "MerchantSpend", "key": "acme"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "verb-style POST /batch_get must return 200; got status={}",
        resp.status()
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

    // FLAT row 0 — feature dict IS the row, no `{table, entity_id, features}`
    // envelope.
    assert_eq!(
        results[0]["cnt"], 2,
        "alice cnt=2; FLAT dict shape (no envelope), got: {body:#}"
    );
    let alice_total = results[0]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("alice total must be number, got: {body:#}"));
    assert!(
        (alice_total - 42.5).abs() < 1e-9,
        "alice total=42.5, got total={alice_total}"
    );
    assert!(
        results[0].get("table").is_none(),
        "FLAT row — no `table` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("entity_id").is_none(),
        "FLAT row — no `entity_id` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("features").is_none(),
        "FLAT row — no `features` envelope key; got: {body:#}"
    );

    // FLAT row 1.
    assert_eq!(
        results[1]["merchant_cnt"], 3,
        "acme merchant_cnt=3 (alice×2 + bob×1); FLAT dict, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — D-02 + D-06 per-entry features filter narrows independently ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_batch_get_per_entry_features_filter_narrows() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Per-entry filters DIFFER — alice gets ["cnt"], bob gets ["total"].
    // Each row narrows independently (D-02 + D-06).
    let req = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice", "features": ["cnt"]},
            {"table": "UserSpend", "key": "bob",   "features": ["total"]}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "narrowed POST /batch_get must return 200; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "got: {body:#}");

    // Row 0 — alice, narrowed to ["cnt"]: only `cnt` present.
    assert_eq!(
        results[0],
        json!({"cnt": 2}),
        "alice narrowed to cnt only; got: {body:#}"
    );

    // Row 1 — bob, narrowed to ["total"]: only `total` present.
    assert!(
        results[1].get("cnt").is_none(),
        "bob narrowed to total only — `cnt` must be omitted; got: {body:#}"
    );
    let bob_total = results[1]["total"]
        .as_f64()
        .unwrap_or_else(|| panic!("bob total must be number, got: {body:#}"));
    assert!(
        (bob_total - 5.0).abs() < 1e-9,
        "bob total=5.0, got total={bob_total}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — D-06 omit-on-absent: feature in registry but absent on entity ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_batch_get_omits_absent_feature_from_row() {
    let ts = TestServer::spawn().await.expect("spawn");
    register_with_user_hits(&ts).await;
    push_seed_events(&ts).await; // pushes Tx events ONLY, never Hit.

    // alice has not been touched by any `Hit` event, so `hit_count` is in
    // the `UserHits` descriptor BUT absent on the entity. Per D-06: OMIT
    // from the row dict (NOT `null`, NOT an error).
    let req = json!({
        "requests": [
            {"table": "UserHits", "key": "alice", "features": ["hit_count"]}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "omit-on-absent POST /batch_get must return 200; got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 1, "got: {body:#}");

    // Per D-06: feature in registry but absent on entity → row dict is
    // empty (NOT `{"hit_count": null}`, NOT an error tuple).
    assert_eq!(
        results[0],
        json!({}),
        "absent feature must be OMITTED from row dict (not null, not error); got: {body:#}"
    );
    assert!(
        !results[0]
            .as_object()
            .unwrap_or_else(|| panic!("results[0] must be a JSON object; got: {body:#}"))
            .contains_key("hit_count"),
        "row dict must NOT contain `hit_count` key when entity has no Hit events; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — D-06 distinction: feature-not-in-registry → whole-batch reject ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_batch_get_unknown_feature_in_registry_errors_upfront() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // `does_not_exist` is NOT a feature in the `UserSpend` descriptor (typo).
    // Per CONTEXT.md D-06 verbatim ("Feature not in registry... → reject the
    // whole request with `feature_not_found` error") and per PATTERNS.md §2's
    // `runtime_core_glue.rs:382-394` precedent, this MUST be a WHOLE-BATCH
    // reject upfront — NOT a 200 with a per-tuple error row. Three structural
    // assertions: (a) 4xx status, (b) `feature_not_found` mention in body,
    // (c) NO `results` key in body (the request never reached the per-entry
    // dispatch loop).
    let req = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice", "features": ["does_not_exist"]}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");

    let resp_status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json");
    let body_text = body.to_string();
    assert!(
        resp_status >= 400,
        "expected 4xx for feature-not-in-registry per D-06 (whole-batch reject); got status {resp_status}; body: {body_text}"
    );
    assert!(
        body_text.contains("feature_not_found"),
        "body should mention feature_not_found per D-06; body: {body_text}"
    );
    // Whole-batch rejection per D-06; NOT per-tuple error rows in a 200
    // response. The structural difference between the two dispositions is
    // that whole-batch reject has NO `results` key in the body (the request
    // never reached the per-entry dispatch loop).
    assert!(
        !body_text.contains("\"results\""),
        "expected whole-batch rejection, not per-tuple error rows in 200 response; body: {body_text}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 5 — D-02 + D-03 OP_BATCH_GET TCP returns FLAT rows in OP_GET_RESPONSE ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_op_batch_get_returns_flat_rows() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    let tcp_addr = ts.tcp_addr().expect("tcp listener bound");
    let req_body = json!({
        "requests": [
            {"table": "UserSpend", "key": "alice"},
            {"table": "MerchantSpend", "key": "acme"}
        ]
    });
    let payload_bytes = serde_json::to_vec(&req_body).expect("serialise");
    let resp_frame = tcp_send_recv_frame(tcp_addr, OP_BATCH_GET, Bytes::from(payload_bytes)).await;

    assert_eq!(
        resp_frame.op, OP_GET_RESPONSE,
        "TCP OP_BATCH_GET response frame must be OP_GET_RESPONSE (0x0023); got {:#06x}",
        resp_frame.op
    );
    assert_eq!(
        resp_frame.content_type, CT_JSON,
        "json-in must produce json-out (D-A/D-B codec discipline); got 0x{:02x}",
        resp_frame.content_type
    );

    let body: serde_json::Value =
        serde_json::from_slice(&resp_frame.payload).expect("parse json body");

    let results = body["results"]
        .as_array()
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(results.len(), 2, "got: {body:#}");

    // FLAT rows over TCP — same shape as the HTTP route per D-03.
    assert_eq!(
        results[0]["cnt"], 2,
        "TCP alice cnt=2; FLAT dict, got: {body:#}"
    );
    assert_eq!(
        results[1]["merchant_cnt"], 3,
        "TCP acme merchant_cnt=3; FLAT dict, got: {body:#}"
    );
    assert!(
        results[0].get("table").is_none(),
        "TCP FLAT row — no `table` envelope key; got: {body:#}"
    );
    assert!(
        results[0].get("features").is_none(),
        "TCP FLAT row — no `features` envelope key; got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 6 — D-03 unknown_table per-tuple error row is FLAT ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase13_4_1_batch_get_unknown_table_returns_flat_error_row() {
    let ts = TestServer::spawn().await.expect("spawn");
    register(&ts).await;
    push_seed_events(&ts).await;

    // Per-tuple partial-failure semantics preserved from Phase 13.4 — the
    // unknown table is reported in-line as a flat error tuple
    // (`{"error": {"code": "...", ...}}`) with NO envelope fields.
    let req = json!({
        "requests": [
            {"table": "UserSpend",    "key": "alice"},
            {"table": "DoesNotExist", "key": "alice"}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("POST /batch_get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "partial-failure batch_get must still return 200 (rest of batch completes); got status={}",
        resp.status()
    );

    let body: serde_json::Value = resp.json().await.expect("json body");
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "got: {body:#}");

    // Index 0 — UserSpend/alice success row, FLAT.
    assert_eq!(
        results[0]["cnt"], 2,
        "success row must be flat dict; got: {body:#}"
    );

    // Index 1 — DoesNotExist per-tuple error row, FLAT.
    assert_eq!(
        results[1]["error"]["code"], "unknown_table",
        "expected error.code=unknown_table; got: {body:#}"
    );
    assert!(
        results[1].get("table").is_none(),
        "FLAT error row — no `table` envelope key; got: {body:#}"
    );
    assert!(
        results[1].get("entity_id").is_none(),
        "FLAT error row — no `entity_id` envelope key; got: {body:#}"
    );
    assert!(
        results[1].get("features").is_none(),
        "FLAT error row — no `features` envelope key; got: {body:#}"
    );

    ts.shutdown().await.ok();
}
