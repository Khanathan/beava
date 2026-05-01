//! Phase 12.8 Plan 03 — RED tests for cold-entity TTL eviction on the apply hot path.
//!
//! Per CONTEXT.md D-01 (`@bv.event(cold_after='7d')` per-source decorator) +
//! D-04 (FRESH state on resurrect, Redis TTL pattern, locked permanent), Plan 03
//! wires `EventDescriptor.cold_after_ms` (added by Plan 02) into
//! `apply_event_to_aggregations` such that:
//!
//! - When `cold_after_ms = Some(N)` and `now_ms - last_seen_ms > N` for the
//!   entity being touched, the entity's `Vec<AggOp>` is CLEARED before the new
//!   event is applied (FRESH state — no partial-state preservation).
//! - When `cold_after_ms = None`, no eviction logic runs (zero behavior change
//!   for sources that don't opt in).
//! - Only the entity TOUCHED by the current event is checked — idle entities
//!   are NOT swept (preserves `project_no_sharded_apply` single-thread invariant
//!   per CONTEXT D-04 "no background scan thread").
//!
//! These 7 tests pin the contract end-to-end through TestServer. They drive
//! /register → /push → /get over HTTP with `tokio::time::sleep` between pushes
//! to age the `last_seen_ms` past the 100ms TTL.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;
use std::time::Duration;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Register payload with a configurable `cold_after_ms`. When `cold_after_ms`
/// is `None`, the field is emitted as JSON `null` (matches the Plan 02
/// `#[serde(default)]` shape — deserialised to `None` server-side).
///
/// Pipeline shape: a `Txn` event source + a single `TxnAgg` derivation that
/// counts events grouped by `user_id`. Mirrors the simplest fraud pipeline
/// shape used elsewhere in the test suite (phase12_07_*).
fn register_payload_count(cold_after_ms: Option<u64>) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
                "cold_after_ms": cold_after_ms,
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

/// Register payload with `count` + windowed `sum(amount)` on the same source.
/// Used by `test_cold_eviction_clears_all_aggregations_for_source` to assert
/// that eviction sweeps every aggregation derived from the cold source.
fn register_payload_count_and_sum(cold_after_ms: Option<u64>) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
                "cold_after_ms": cold_after_ms,
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "cnt": {"op": "count", "params": {}},
                    "total": {"op": "sum", "params": {"field": "amount"}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64", "total": "f64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    })
}

/// Register payload with a compound key `(user_id, merchant)` and a count agg.
fn register_payload_compound_key(cold_after_ms: Option<u64>) -> serde_json::Value {
    json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "user_id": "str",
                        "merchant": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
                "cold_after_ms": cold_after_ms,
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id", "merchant"], "agg": {
                    "cnt": {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "merchant": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id", "merchant"]
            }
        ]
    })
}

/// Push one event with `user_id`, `amount`. Returns `()` on 2xx; panics
/// otherwise (test should fail fast on push errors).
async fn push_user(ts: &TestServer, user_id: &str, amount: f64) {
    let body = json!({"user_id": user_id, "amount": amount});
    let resp = ts.post_json("/push/Txn", &body).await.expect("push");
    let status = resp.status();
    assert!(
        status.is_success(),
        "push for user_id={user_id} returned {status}, body={}",
        resp.text().await.unwrap_or_default()
    );
}

/// Push an event with compound key (user_id, merchant).
async fn push_user_merchant(ts: &TestServer, user_id: &str, merchant: &str, amount: f64) {
    let body = json!({"user_id": user_id, "merchant": merchant, "amount": amount});
    let resp = ts.post_json("/push/Txn", &body).await.expect("push");
    let status = resp.status();
    assert!(
        status.is_success(),
        "push for user_id={user_id} merchant={merchant} returned {status}, body={}",
        resp.text().await.unwrap_or_default()
    );
}

/// Query feature `feature` for entity `key` via /get. Returns the raw
/// `value` field (JSON Value).
async fn get_feature(ts: &TestServer, feature: &str, key: &str) -> serde_json::Value {
    let path = format!("/get/{feature}/{key}");
    let body = ts.get_json(&path).await;
    body["value"].clone()
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// 1. First event for new entity with TTL — no prior state, no eviction
/// triggered. Confirms TTL-set sources don't break the first-event-for-entity
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_first_event_for_new_entity_with_ttl_works() {
    let ts = TestServer::spawn().await.expect("spawn");

    // 1d TTL (large, never expires within the test).
    let payload = register_payload_count(Some(86_400_000));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(
        resp.status().is_success(),
        "register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    push_user(&ts, "alice", 10.0).await;

    let cnt = get_feature(&ts, "cnt", "alice").await;
    assert_eq!(
        cnt, 1,
        "first event for new entity must yield cnt=1 (no eviction); got {cnt}"
    );

    ts.shutdown().await.ok();
}

/// 2. Warm entity with TTL accumulates correctly across rapid pushes (no
/// sleep). Confirms warm-path skips eviction (now_ms - last_seen_ms <=
/// cold_after_ms).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_warm_entity_with_ttl_accumulates() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_count(Some(86_400_000)); // 1d
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    for _ in 0..3 {
        push_user(&ts, "alice", 10.0).await;
    }

    let cnt = get_feature(&ts, "cnt", "alice").await;
    assert_eq!(
        cnt, 3,
        "warm entity must accumulate to cnt=3 across 3 events; got {cnt}"
    );

    ts.shutdown().await.ok();
}

/// 3. Cold entity resets on resurrect — Redis TTL pattern (FRESH state, D-04
/// locked permanent). Push, sleep past TTL, push again — second push sees a
/// fresh entity with cnt=1 (NOT cnt=2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_entity_resets_on_resurrect() {
    let ts = TestServer::spawn().await.expect("spawn");

    // 100ms TTL — short enough to age within the test.
    let payload = register_payload_count(Some(100));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    // First push at T=0; entity born with cnt=1.
    push_user(&ts, "alice", 10.0).await;
    let cnt_warm = get_feature(&ts, "cnt", "alice").await;
    assert_eq!(cnt_warm, 1, "first push: cnt must be 1");

    // Wait 200ms — past the 100ms TTL with margin.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Second push at T=200ms+; eviction fires; entity reset to fresh; cnt=1.
    push_user(&ts, "alice", 10.0).await;
    let cnt_resurrect = get_feature(&ts, "cnt", "alice").await;
    assert_eq!(
        cnt_resurrect, 1,
        "FRESH state on resurrect (D-04): cnt must be 1 (not 2). Got cnt={cnt_resurrect}"
    );

    ts.shutdown().await.ok();
}

/// 4. Other entities not affected by per-event eviction.
///
/// Push alice + bob (warm). Sleep past TTL. Push alice ONLY. Verify alice's
/// state was reset (cnt=1, fresh) AND bob's state is preserved (cnt=1, his
/// pre-sleep value — eviction is per-entity-on-arrival, not a global sweep).
///
/// This confirms `project_no_sharded_apply` invariant: eviction logic only
/// touches the entity being apply()'d on the current event. Idle bob holds
/// his memory until either his own event arrives OR an explicit
/// `app.delete()` call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_other_entities_not_affected() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_count(Some(100));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    // T=0: push alice + bob; both warm with cnt=1.
    push_user(&ts, "alice", 10.0).await;
    push_user(&ts, "bob", 20.0).await;

    // Wait 200ms — past the 100ms TTL.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Push alice ONLY. Eviction fires for alice; bob is untouched.
    push_user(&ts, "alice", 30.0).await;

    let cnt_alice = get_feature(&ts, "cnt", "alice").await;
    let cnt_bob = get_feature(&ts, "cnt", "bob").await;
    assert_eq!(
        cnt_alice, 1,
        "alice must be evicted + resurrected: cnt=1 (not 2). Got cnt={cnt_alice}"
    );
    assert_eq!(
        cnt_bob, 1,
        "bob is idle — not evicted (per-entity-on-arrival eviction, not global sweep). \
         Got cnt={cnt_bob}"
    );

    ts.shutdown().await.ok();
}

/// 5. Null-TTL (cold_after_ms = None) — no eviction, ever.
///
/// Confirms the zero-cost path: sources that don't set cold_after preserve
/// pre-Plan-03 behavior (warm forever, accumulate without eviction).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_null_ttl_no_eviction() {
    let ts = TestServer::spawn().await.expect("spawn");

    // No cold_after — null/None — eviction logic must not fire.
    let payload = register_payload_count(None);
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    push_user(&ts, "alice", 10.0).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    push_user(&ts, "alice", 10.0).await;

    let cnt = get_feature(&ts, "cnt", "alice").await;
    assert_eq!(
        cnt, 2,
        "null-TTL: no eviction; cnt must accumulate to 2 across 1s gap. Got cnt={cnt}"
    );

    ts.shutdown().await.ok();
}

/// 6. Cold-after with compound key — `multi` HashMap shape correctly evicts.
///
/// The eviction logic must work for `EntityKeyShape::Multi(EntityKey)` —
/// compound keys go through a separate sub-map than single-key shapes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_after_with_compound_key() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_compound_key(Some(100));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(resp.status().is_success(), "register failed");

    // T=0: push (alice, mer1).
    push_user_merchant(&ts, "alice", "mer1", 10.0).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // T=200ms+: push (alice, mer1) again; eviction fires.
    push_user_merchant(&ts, "alice", "mer1", 20.0).await;

    // Compound-key GET path: keys are pipe-separated (alice|mer1) per
    // existing query path conventions. Verify compound entity also resets.
    let cnt = get_feature(&ts, "cnt", "alice|mer1").await;
    assert_eq!(
        cnt, 1,
        "compound entity must also reset on resurrect. Got cnt={cnt}"
    );

    ts.shutdown().await.ok();
}

/// 7. Cold eviction sweeps every aggregation sourced from the cold source.
///
/// A single `Txn` source feeds two derivations: `count` and `sum(amount)`.
/// When the entity goes cold, BOTH features must reset on the next event —
/// not just one. Confirms the eviction loop iterates ALL aggregations for
/// the source, not just the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cold_eviction_clears_all_aggregations_for_source() {
    let ts = TestServer::spawn().await.expect("spawn");

    let payload = register_payload_count_and_sum(Some(100));
    let resp = ts.post_json("/register", &payload).await.expect("register");
    assert!(
        resp.status().is_success(),
        "register failed: {}",
        resp.text().await.unwrap_or_default()
    );

    // T=0: push alice with amount=10.0; cnt=1, total=10.0.
    push_user(&ts, "alice", 10.0).await;
    let cnt_warm = get_feature(&ts, "cnt", "alice").await;
    let total_warm = get_feature(&ts, "total", "alice").await;
    assert_eq!(cnt_warm, 1, "warm cnt must be 1");
    // sum returns numeric (F64 or I64 depending on input/rounding). Use as_f64
    // which accepts both.
    let total_warm_f = total_warm
        .as_f64()
        .unwrap_or_else(|| panic!("warm total expected numeric, got: {total_warm:?}"));
    assert!(
        (total_warm_f - 10.0).abs() < 1e-9,
        "warm total must be 10.0, got {total_warm_f}"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // T=200ms+: push alice with amount=20.0. BOTH features reset.
    push_user(&ts, "alice", 20.0).await;
    let cnt_resurrect = get_feature(&ts, "cnt", "alice").await;
    let total_resurrect = get_feature(&ts, "total", "alice").await;
    assert_eq!(
        cnt_resurrect, 1,
        "FRESH state: cnt must be 1 (not 2). Got cnt={cnt_resurrect}"
    );
    let total_resurrect_f = total_resurrect
        .as_f64()
        .unwrap_or_else(|| panic!("resurrect total expected numeric, got: {total_resurrect:?}"));
    assert!(
        (total_resurrect_f - 20.0).abs() < 1e-9,
        "FRESH state: total must be 20.0 (not 30.0). Got total={total_resurrect_f}"
    );

    ts.shutdown().await.ok();
}
