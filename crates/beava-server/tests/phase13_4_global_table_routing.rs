//! Phase 13.4 Plan 09 — Global-table sentinel routing per ADR-003 and
//! ADR-001 §"Deferred for Phase 13.4 implementation".
//!
//! Acceptance gate for ADR-003 ("First-class global aggregation"):
//!
//! - Register payloads with `output_kind: "table"` derivations whose
//!   `group_by.keys: []` and `table_primary_key: []` are accepted (signal:
//!   global table — single sentinel state slot).
//! - Push events for the upstream source advance the sentinel slot.
//! - GET requests with `entity_id: ""` retrieve the sentinel state via
//!   the existing per-entity hashmap key path (per ADR-003: "the existing
//!   `&str` key path handles `\"\"` natively. No new code path inside
//!   `apply_shard.rs::dispatch_*_sync` — just the absence of a special-case
//!   rejection.").
//!
//! Per the architectural commitment `project_v0_events_only_scope` (with
//! ADR-001 partial overturn 2026-05-03): `output_kind: "table"` derivations
//! are revived for aggregation-output ONLY. Top-level `kind: "table"`
//! register nodes STAY rejected by Plan 12.7-01's JSON-prelude shim
//! (`pre_check_unsupported_node_kind`) — Test 4 below pins this.
//!
//! TDD: this is the RED gate for Plan 13.4-09. Tests 1, 2, 5, 6 fail
//! against the current engine because `parse_entity_key("", &[])` returns
//! `None` (segments-vs-arity mismatch), which surfaces as `key_parse_failure`
//! at GET / batch_get time. Tests 3, 4 pass against the existing engine
//! (already enforced by serde + the JSON-prelude shim respectively).

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;
use std::time::Duration;

// ─── Shared helpers ─────────────────────────────────────────────────────────

/// Minimal register payload for a global table:
///   - Event source `Tx` carries `event_time` + `amount`.
///   - Derivation `GlobalCounter` aggregates `count` GLOBALLY (key_cols=[]).
///
/// The wire shape uses `ops[0].keys: []` and `table_primary_key: []` —
/// both empty arrays are the global-table signal per ADR-003.
fn global_payload() -> serde_json::Value {
    json!({
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
    })
}

/// Mixed registry: per-entity `UserSpend(user_id)` + global `GlobalCounter`.
/// Pushes one shape of `Tx`; both aggregations track it.
fn mixed_payload() -> serde_json::Value {
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
                    "agg": {"spend_count": {"op": "count", "params": {}}}
                }],
                "schema": {
                    "fields": {"user_id": "str", "spend_count": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
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
    })
}

async fn push_event(ts: &TestServer, body: &serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/push/Tx", ts.base_url()))
        .json(body)
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

// ─── Test 1 — register accepts empty keys + table_primary_key (global table) ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_global_table_with_empty_key_cols_succeeds() {
    let ts = TestServer::spawn().await.expect("spawn");

    let resp = ts
        .post_json("/register", &global_payload())
        .await
        .expect("post /register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        (200..300).contains(&status),
        "register with key_cols=[] (group_by.keys=[] + table_primary_key=[]) \
         must succeed per ADR-003, got status={status} body={body_text}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 2 — push + GET with sentinel entity_id="" returns aggregated state ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_then_get_global_table_returns_aggregated_features() {
    let ts = TestServer::spawn().await.expect("spawn");

    let resp = ts
        .post_json("/register", &global_payload())
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register must succeed");

    // Push 5 events with various event_time / amount values. None of these
    // carry a `user_id` field — the global counter aggregates ALL events
    // regardless of identity.
    for i in 0..5i64 {
        push_event(
            &ts,
            &json!({"event_time": 1000 + i, "amount": (i as f64) + 0.5}),
        )
        .await;
    }

    // Batch-get the global slot via the empty-string sentinel.
    let req = json!({
        "requests": [
            {"table": "GlobalCounter", "entity_id": ""}
        ]
    });
    let resp = ts
        .post_json("/batch_get", &req)
        .await
        .expect("post /batch_get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");

    let results = body
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(results.len(), 1, "single result expected, got: {body:#}");
    assert_eq!(results[0]["table"], "GlobalCounter");
    assert_eq!(results[0]["entity_id"], "");
    assert_eq!(
        results[0]["features"]["events_total"], 5,
        "global events_total must be 5, got: {body:#}"
    );
    assert!(
        results[0].get("error").is_none(),
        "no per-tuple error expected, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — register with kind=event missing schema is rejected ────────────
//
// Defensive: confirms the existing register-validation pipeline still
// rejects malformed payloads. The plan's `must_haves.truths[2]` says
// "Register payload with `key_cols: null` (not an empty array) returns a
// structured 400". The engine doesn't have a `key_cols` field at the wire
// level — it has `group_by.keys` (Vec<String>, defaults to empty if absent)
// and `table_primary_key` (Option<Vec<String>>). Neither serde-deserializes
// from `null` into a Vec; serde rejects with "invalid type: null". This
// test pins that the existing serde-strict path holds.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_with_keys_null_returns_400() {
    let ts = TestServer::spawn().await.expect("spawn");

    let mut payload = global_payload();
    // Force `keys: null` on the group_by op — should be rejected by serde
    // (Vec<String> is non-nullable).
    payload["nodes"][1]["ops"][0]["keys"] = serde_json::Value::Null;

    let resp = ts
        .post_json("/register", &payload)
        .await
        .expect("post /register");
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json");
    assert!(
        (400..500).contains(&status),
        "keys=null must be rejected, got status={status} body={body:#}"
    );
    assert!(
        body.get("error").is_some(),
        "expected error envelope, got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 4 — top-level kind=table STAYS rejected by 12.7-01 shim ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_top_level_kind_table_still_rejected() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Plan 13.4-05 surgical permit (D-04) only widened the architectural
    // test allowlist (FORBIDDEN_PATTERNS minus bare `OpNode::Table`). The
    // JSON-prelude shim `pre_check_unsupported_node_kind` (Plan 12.7-01)
    // is UNCHANGED and still rejects top-level `{kind: "table"}` register
    // nodes with `unsupported_node_kind`. ADR-001 partial overturn revives
    // ONLY derivation-output `output_kind=table`, NOT top-level table
    // nodes.
    let payload = json!({
        "nodes": [
            {
                "kind": "table",
                "name": "ShouldNotRegister",
                "primary_key": [],
                "schema": {
                    "fields": {"x": "i64"},
                    "optional_fields": []
                },
                "mode": "upsert"
            }
        ]
    });

    let resp = ts
        .post_json("/register", &payload)
        .await
        .expect("post /register");
    let status = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(status, 400, "top-level kind=table must return 400, got: {body:#}");
    let code = body["error"]["code"].as_str().unwrap_or_default();
    assert_eq!(
        code, "unsupported_node_kind",
        "expected error.code='unsupported_node_kind' (Plan 12.7-01 shim, \
         unchanged by Plan 13.4-05's surgical permit), got body={body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 5 — mixed per-entity + global tables coexist in one registry ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_global_and_per_entity_in_same_registry_works() {
    let ts = TestServer::spawn().await.expect("spawn");

    let resp = ts
        .post_json("/register", &mixed_payload())
        .await
        .expect("register");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "mixed-registry register must succeed: status={status} body={body_text}"
    );

    // Push 3 alice events + 2 bob events.
    for ts_ms in [1000_i64, 1001, 1002] {
        push_event(
            &ts,
            &json!({"event_time": ts_ms, "user_id": "alice", "amount": 1.0}),
        )
        .await;
    }
    for ts_ms in [2000_i64, 2001] {
        push_event(
            &ts,
            &json!({"event_time": ts_ms, "user_id": "bob", "amount": 2.0}),
        )
        .await;
    }

    // Per-entity GET on alice.
    let req = json!({
        "requests": [{"table": "UserSpend", "entity_id": "alice"}]
    });
    let resp = ts.post_json("/batch_get", &req).await.expect("batch_get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["results"][0]["features"]["spend_count"], 3,
        "alice spend_count=3, got: {body:#}"
    );

    // Global GET on sentinel "".
    let req = json!({
        "requests": [{"table": "GlobalCounter", "entity_id": ""}]
    });
    let resp = ts.post_json("/batch_get", &req).await.expect("batch_get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(
        body["results"][0]["features"]["events_total"], 5,
        "global events_total=5 (3 alice + 2 bob), got: {body:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 6 — heterogeneous batch mixes per-entity + global in one frame ────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn op_batch_get_heterogeneous_per_entity_and_global_works() {
    let ts = TestServer::spawn().await.expect("spawn");

    let resp = ts
        .post_json("/register", &mixed_payload())
        .await
        .expect("register");
    assert!(resp.status().is_success(), "register must succeed");

    push_event(
        &ts,
        &json!({"event_time": 1000, "user_id": "alice", "amount": 1.0}),
    )
    .await;
    push_event(
        &ts,
        &json!({"event_time": 1001, "user_id": "alice", "amount": 1.0}),
    )
    .await;
    push_event(
        &ts,
        &json!({"event_time": 1002, "user_id": "bob", "amount": 1.0}),
    )
    .await;

    // Heterogeneous batch — per-entity (alice) + global (sentinel "") in
    // one frame. ADR-003: "composes natively with OP_BATCH_GET (a
    // heterogeneous batch can mix per-entity and global lookups by
    // entity_id alone)".
    let req = json!({
        "requests": [
            {"table": "UserSpend",     "entity_id": "alice"},
            {"table": "GlobalCounter", "entity_id": ""}
        ]
    });
    let resp = ts.post_json("/batch_get", &req).await.expect("batch_get");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");

    let results = body
        .get("results")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("expected results array, got: {body:#}"));
    assert_eq!(results.len(), 2);

    // Order preserved per Plan 03 wire contract.
    assert_eq!(results[0]["table"], "UserSpend");
    assert_eq!(results[0]["entity_id"], "alice");
    assert_eq!(results[0]["features"]["spend_count"], 2);
    assert!(
        results[0].get("error").is_none(),
        "alice tuple should not error, got: {body:#}"
    );

    assert_eq!(results[1]["table"], "GlobalCounter");
    assert_eq!(results[1]["entity_id"], "");
    assert_eq!(results[1]["features"]["events_total"], 3);
    assert!(
        results[1].get("error").is_none(),
        "global tuple should not error, got: {body:#}"
    );

    ts.shutdown().await.ok();
}
