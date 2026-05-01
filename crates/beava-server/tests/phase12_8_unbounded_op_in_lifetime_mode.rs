//! Phase 12.8 Plan 01 — Register-time rejection of unbounded ops in lifetime mode.
//!
//! Per CONTEXT.md D-03 ("Hard reject at register-time"), Beava v0 commits that
//! every lifetime aggregation declares a finite per-entity memory ceiling. The
//! 4th JSON-prelude shim `pre_check_unbounded_op_in_lifetime_mode` walks the
//! register payload's derivation nodes and rejects any windowless op whose
//! lifetime memory bound is `Unbounded` per `lifetime_bound_for_op_str`.
//!
//! **Wave-1 contract (this plan):** the bound classifier helper is a stub that
//! returns `Unbounded` for every op-string. Plan 12.8-04 (Wave 2) populates the
//! per-op classification table; Plan 12.8-06 (Wave 3) flips the env-gate
//! `BEAVA_MEMORY_GOV_ENFORCE` default from OFF to ON. Wave 1 keeps the workspace
//! green by gating the shim entirely behind the env-var: if unset (the
//! `cargo test --workspace` default), the shim is a no-op and existing
//! lifetime-op registrations succeed exactly as they did pre-12.8.
//!
//! Per CONTEXT.md D-02 framing: error code is `unbounded_op_in_lifetime_mode`
//! (forward-looking — "requires explicit memory bound in v0"), NOT a
//! retrospective `feature_removed_*` code. v0 is the FIRST public release.
//!
//! Test-process env model
//! ----------------------
//! Rust integration tests in a single `tests/foo.rs` file run in the SAME
//! process. `std::env::*` is process-global. `apply_shard.rs::memory_gov_enforce_enabled`
//! does a per-call `std::env::var` read on the cold register path (NOT a
//! OnceLock cache — that would memoize the first read and break per-test
//! flips). Each test below acquires `ENV_LOCK`, sets/clears the env var, runs
//! its TestServer through one /register call, and then clears the env var.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

const ENV_KEY: &str = "BEAVA_MEMORY_GOV_ENFORCE";

/// Process-global env mutex. Mirrors the pattern in
/// `tests/wal_env_var_tunables.rs` — in-test sequencing avoids race on
/// `std::env::set_var` / `std::env::remove_var` when the integration test
/// binary uses tokio's multi-thread runtime.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enforce_on() {
    std::env::set_var(ENV_KEY, "1");
}

fn enforce_off() {
    std::env::remove_var(ENV_KEY);
}

#[tokio::test]
async fn test_unbounded_op_rejected_when_enforcement_enabled() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // Tx event + a derivation that group_by user_id and runs a windowless count.
    // Plan 01's stub `lifetime_bound_for_op_str` returns Unbounded for every op
    // string — so this windowless count gets rejected by the 4th shim.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "TxByUser",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt": {"op": "count", "params": {}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    enforce_off();
    ts.shutdown().await.ok();

    assert_eq!(
        status, 400,
        "windowless lifetime op must be rejected at register time when \
         BEAVA_MEMORY_GOV_ENFORCE=1, got status={status}, body={body_text}"
    );
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "unbounded_op_in_lifetime_mode",
        "expected unbounded_op_in_lifetime_mode, got body={body}"
    );
    let reason = body["error"]["reason"]
        .as_str()
        .expect("reason should be a string");
    assert!(
        reason.contains("requires explicit memory bound in v0"),
        "reason should contain the v0 framing 'requires explicit memory bound \
         in v0', got: {reason}"
    );
    assert!(
        reason.contains("count"),
        "reason should name the rejected op `count`, got: {reason}"
    );
}

#[tokio::test]
async fn test_windowed_op_passes_when_enforcement_enabled() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // Same shape as above BUT the count carries `params.window = "60s"`.
    // Windowed path is naturally bounded by the 64-bucket cap; the shim must
    // skip these.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "TxByUser",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt": {"op": "count", "params": {"window": "60s"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    enforce_off();
    ts.shutdown().await.ok();

    assert!(
        (200..300).contains(&status),
        "windowed op should bypass the 4th shim (naturally bounded by 64-bucket \
         cap) even with BEAVA_MEMORY_GOV_ENFORCE=1; got status={status}, \
         body={body_text}"
    );
}

#[tokio::test]
async fn test_no_enforcement_when_env_unset() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    // SAME windowless-count payload as test 1. WITHOUT the env var, the 4th
    // shim must short-circuit and accept the registration. This is the
    // workspace-stays-green guarantee for Wave 1.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "TxByUser",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt": {"op": "count", "params": {}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    ts.shutdown().await.ok();

    assert!(
        (200..300).contains(&status),
        "without BEAVA_MEMORY_GOV_ENFORCE, the 4th shim must be a no-op (Wave 1 \
         default-OFF gate); got status={status}, body={body_text}"
    );
}

#[tokio::test]
async fn test_unbounded_op_path_includes_node_and_op_index() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // 2 derivations: derivation 1 is windowed (fine), derivation 2 is windowless
    // (rejected). Per first-occurrence semantics the shim returns the path of
    // the FIRST offender — which is the second derivation's count op.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "Windowed",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt_60s": {"op": "count", "params": {"window": "60s"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt_60s": "i64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "Lifetime",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt_total": {"op": "count", "params": {}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt_total": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    enforce_off();
    ts.shutdown().await.ok();

    assert_eq!(
        status, 400,
        "windowless count in derivation 2 must be rejected, got \
         status={status}, body={body_text}"
    );
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("body json");
    assert_eq!(body["error"]["code"], "unbounded_op_in_lifetime_mode");
    let path = body["error"]["path"].as_str().unwrap_or_default();
    // nodes[2] = "Lifetime" derivation, ops[0] = group_by, agg.cnt_total = the
    // windowless count feature whose op-string is "count" and whose lifetime
    // bound is Unbounded (Plan 01 stub).
    assert_eq!(
        path, "nodes[2].Lifetime.ops[0].agg.cnt_total",
        "error.path should be nodes[2].Lifetime.ops[0].agg.cnt_total \
         (first-offender semantics across derivations), got: {path}"
    );
}

#[tokio::test]
async fn test_message_suggests_windowed_kwarg() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // Same windowless-count payload as test 1 — the rejection reason text must
    // help the user migrate. Per CONTEXT D-03, the message names the cap kwarg
    // they should add.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Tx",
                "schema": {
                    "fields": {"user_id": "str", "amount": "f64"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "TxByUser",
                "output_kind": "event",
                "upstreams": ["Tx"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "cnt": {"op": "count", "params": {}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    enforce_off();
    ts.shutdown().await.ok();

    assert_eq!(status, 400, "windowless count rejected, got {body_text}");
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("body json");
    let reason = body["error"]["reason"]
        .as_str()
        .expect("reason should be a string");
    // The message should suggest the `windowed=` kwarg so the user has a
    // clear migration path.
    assert!(
        reason.contains("windowed=\"60s\"") || reason.contains("windowed=\"60s\""),
        "reason should suggest a `windowed=\"60s\"` migration, got: {reason}"
    );
    // Also must suggest at least one cap kwarg vocabulary fragment so users on
    // bounded-by-config ops (histogram etc.) see the cap-kwarg path even though
    // Plan 01's stub classifies everything as Unbounded.
    assert!(
        reason.contains("num_buckets")
            || reason.contains("k=N")
            || reason.contains("n=N"),
        "reason should mention at least one cap kwarg (num_buckets / k=N / n=N), \
         got: {reason}"
    );
}
