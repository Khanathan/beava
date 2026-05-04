//! Phase 13.4 Plan 06 — dry_run=true register flag (CONTEXT scope item #6).
//!
//! `dry_run=true` runs the same diff classifier as the D-01 force gate
//! (Task 6.a / 6.b) but **does not mutate** the registry or in-memory state.
//! Wire shape on success:
//!
//! ```json
//! HTTP/1.1 200 OK
//! {
//!   "diff": {"additive": [...], "destructive": [...]},
//!   "would_apply": false
//! }
//! ```
//!
//! `dry_run` wins over `force` — when both flags are set, the dry_run branch
//! fires first (per CONTEXT scope item #6: "returns JSON without applying").
//! The diff classifier is pure + sorted, so two dry_run calls with the same
//! payload produce byte-identical responses (idempotency).
//!
//! TDD: this is the RED gate — at the time of writing the dispatch chain
//! recognizes `force` (Task 6.b GREEN) but NOT `dry_run`, so:
//!   - Test 1 fails: dry_run on destructive should return 200+diff, current
//!     handler returns 409 force_required.
//!   - Test 2 fails: dry_run on additive should return 200+diff envelope
//!     (with would_apply=false); current handler returns the legacy success
//!     envelope (no `would_apply` key, no `diff` key).
//!   - Test 3 fails: same reason as Test 1.
//!   - Test 4 fails: same reason as Test 1.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::{json, Value};

// ─── Shared register-payload helpers ────────────────────────────────────────

fn baseline_payload() -> Value {
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
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {
                        "cnt":   {"op": "count", "params": {"window": "1h"}},
                        "total": {"op": "sum",   "params": {"field": "amount", "window": "1h"}}
                    }
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64", "total": "f64"},
                    "optional_fields": []
                }
            }
        ]
    })
}

async fn post_register(ts: &TestServer, body: &Value) -> (u16, Value) {
    let resp = ts
        .post_json("/register", body)
        .await
        .expect("post /register");
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.expect("json body");
    (status, body)
}

/// Asserts the dry_run wire shape: HTTP 200 + {diff: {...}, would_apply: false}.
fn assert_dry_run_response(status: u16, body: &Value) {
    assert_eq!(
        status, 200,
        "expected dry_run HTTP 200, got status={status}, body={body:#}"
    );
    assert_eq!(
        body["would_apply"], false,
        "expected would_apply=false, got body={body:#}"
    );
    assert!(
        body["diff"].is_object(),
        "expected diff to be an object, got body={body:#}"
    );
    assert!(
        body["diff"]["additive"].is_array(),
        "expected diff.additive to be an array, got body={body:#}"
    );
    assert!(
        body["diff"]["destructive"].is_array(),
        "expected diff.destructive to be an array, got body={body:#}"
    );
}

// ─── Test 1 — dry_run on destructive returns diff WITHOUT applying ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_on_destructive_returns_diff_without_applying() {
    let ts = TestServer::spawn().await.expect("spawn");
    let (status_a, _) = post_register(&ts, &baseline_payload()).await;
    assert!((200..300).contains(&status_a));

    // Destructive (window-change) payload with dry_run=true — should return
    // 200 + diff envelope.
    let mut payload_dry = baseline_payload();
    payload_dry["nodes"][1]["ops"][0]["agg"]["cnt"]["params"]["window"] = json!("30m");
    payload_dry["nodes"][1]["ops"][0]["agg"]["total"]["params"]["window"] = json!("30m");
    payload_dry["dry_run"] = json!(true);

    let (status_dry, body_dry) = post_register(&ts, &payload_dry).await;
    assert_dry_run_response(status_dry, &body_dry);
    let destructive = body_dry["diff"]["destructive"].as_array().unwrap();
    assert!(
        !destructive.is_empty(),
        "destructive list must be non-empty for window-change preview, body={body_dry:#}"
    );
    let has_window_change = destructive
        .iter()
        .any(|e| e.get("kind").and_then(|k| k.as_str()) == Some("window_change"));
    assert!(
        has_window_change,
        "expected at least one window_change entry in destructive list, body={body_dry:#}"
    );

    // Now register the SAME destructive payload WITHOUT dry_run AND without
    // force — should hit 409, proving dry_run did NOT apply.
    let mut payload_real = baseline_payload();
    payload_real["nodes"][1]["ops"][0]["agg"]["cnt"]["params"]["window"] = json!("30m");
    payload_real["nodes"][1]["ops"][0]["agg"]["total"]["params"]["window"] = json!("30m");
    let (status_real, body_real) = post_register(&ts, &payload_real).await;
    assert_eq!(
        status_real, 409,
        "destructive register after dry_run must still 409 — proves dry_run is non-applying. body={body_real:#}"
    );
    assert_eq!(body_real["error"]["code"], "force_required");

    ts.shutdown().await.ok();
}

// ─── Test 2 — dry_run on additive returns empty destructive list ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_on_additive_returns_empty_destructive_list() {
    let ts = TestServer::spawn().await.expect("spawn");
    let (status_a, _) = post_register(&ts, &baseline_payload()).await;
    assert!((200..300).contains(&status_a));

    // Add a NEW event source + a NEW derivation that depends on it. Pure additive.
    let mut payload_dry = baseline_payload();
    payload_dry["nodes"].as_array_mut().unwrap().push(json!({
        "kind": "event",
        "name": "Login",
        "schema": {
            "fields": {"event_time": "i64", "user_id": "str"},
            "optional_fields": []
        }
    }));
    payload_dry["nodes"].as_array_mut().unwrap().push(json!({
        "kind": "derivation",
        "name": "LoginCnt",
        "output_kind": "event",
        "upstreams": ["Login"],
        "ops": [{
            "op": "group_by",
            "keys": ["user_id"],
            "agg": {
                "logins": {"op": "count", "params": {"window": "1h"}}
            }
        }],
        "schema": {
            "fields": {"user_id": "str", "logins": "i64"},
            "optional_fields": []
        }
    }));
    payload_dry["dry_run"] = json!(true);

    let (status_dry, body_dry) = post_register(&ts, &payload_dry).await;
    assert_dry_run_response(status_dry, &body_dry);
    let destructive = body_dry["diff"]["destructive"].as_array().unwrap();
    assert!(
        destructive.is_empty(),
        "destructive list must be empty for pure-additive preview, body={body_dry:#}"
    );
    let additive = body_dry["diff"]["additive"].as_array().unwrap();
    assert!(
        !additive.is_empty(),
        "additive list must be non-empty (new descriptors), body={body_dry:#}"
    );
    let has_new_descriptor = additive
        .iter()
        .any(|e| e.get("kind").and_then(|k| k.as_str()) == Some("new_descriptor"));
    assert!(
        has_new_descriptor,
        "expected at least one new_descriptor entry in additive list, body={body_dry:#}"
    );

    // Re-register the SAME additive payload WITHOUT dry_run — should succeed
    // (additive register doesn't need force). Confirms dry_run did NOT apply
    // (otherwise the second call would be a no-op with empty diff envelope).
    let mut payload_real = payload_dry.clone();
    payload_real.as_object_mut().unwrap().remove("dry_run");
    let (status_real, body_real) = post_register(&ts, &payload_real).await;
    assert!(
        (200..300).contains(&status_real),
        "additive re-register without dry_run must succeed, got status={status_real}, body={body_real:#}"
    );

    ts.shutdown().await.ok();
}

// ─── Test 3 — dry_run + force=true is treated as dry_run (dry_run wins) ─────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_with_force_true_treats_as_dry_run() {
    let ts = TestServer::spawn().await.expect("spawn");
    let (status_a, _) = post_register(&ts, &baseline_payload()).await;
    assert!((200..300).contains(&status_a));

    // Destructive payload with BOTH flags — dry_run should win.
    let mut payload_both = baseline_payload();
    payload_both["nodes"][1]["ops"][0]["agg"]["cnt"]["params"]["window"] = json!("30m");
    payload_both["nodes"][1]["ops"][0]["agg"]["total"]["params"]["window"] = json!("30m");
    payload_both["force"] = json!(true);
    payload_both["dry_run"] = json!(true);

    let (status_both, body_both) = post_register(&ts, &payload_both).await;
    assert_dry_run_response(status_both, &body_both);
    let destructive = body_both["diff"]["destructive"].as_array().unwrap();
    assert!(
        !destructive.is_empty(),
        "destructive list must be reported for dry_run+force preview, body={body_both:#}"
    );

    // Subsequent register with NEITHER flag must STILL hit 409 — proves
    // force=true did NOT apply (dry_run won).
    let mut payload_real = baseline_payload();
    payload_real["nodes"][1]["ops"][0]["agg"]["cnt"]["params"]["window"] = json!("30m");
    payload_real["nodes"][1]["ops"][0]["agg"]["total"]["params"]["window"] = json!("30m");
    let (status_real, body_real) = post_register(&ts, &payload_real).await;
    assert_eq!(
        status_real, 409,
        "destructive register after dry_run+force must still 409 — force did NOT win. body={body_real:#}"
    );
    assert_eq!(body_real["error"]["code"], "force_required");

    ts.shutdown().await.ok();
}

// ─── Test 4 — dry_run is idempotent (same payload → same diff bytes) ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_idempotent_diffs() {
    let ts = TestServer::spawn().await.expect("spawn");
    let (status_a, _) = post_register(&ts, &baseline_payload()).await;
    assert!((200..300).contains(&status_a));

    let mut payload_dry = baseline_payload();
    payload_dry["nodes"][1]["ops"][0]["agg"]["cnt"]["params"]["window"] = json!("30m");
    payload_dry["nodes"][1]["ops"][0]["agg"]["total"]["params"]["window"] = json!("30m");
    payload_dry["dry_run"] = json!(true);

    let (status_1, body_1) = post_register(&ts, &payload_dry).await;
    assert_dry_run_response(status_1, &body_1);

    let (status_2, body_2) = post_register(&ts, &payload_dry).await;
    assert_dry_run_response(status_2, &body_2);

    // Same `diff` — both envelopes must be byte-identical (modulo
    // serde_json's stable BTreeMap ordering of object keys).
    assert_eq!(
        body_1["diff"], body_2["diff"],
        "two dry_run calls with the same payload must produce identical diffs.\nfirst:  {body_1:#}\nsecond: {body_2:#}"
    );

    ts.shutdown().await.ok();
}
