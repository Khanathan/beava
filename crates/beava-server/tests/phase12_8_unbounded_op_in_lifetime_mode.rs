//! Phase 12.8 Plan 01 + 04 — Register-time rejection of unbounded ops in lifetime mode.
//!
//! Per CONTEXT.md D-03 ("Hard reject at register-time"), Beava v0 commits that
//! every lifetime aggregation declares a finite per-entity memory ceiling. The
//! 4th JSON-prelude shim `pre_check_unbounded_op_in_lifetime_mode` walks the
//! register payload's derivation nodes and rejects any windowless op whose
//! lifetime memory bound is `Unbounded` (typo / unclassified) OR is
//! `BoundedByRequiredKwarg` with the kwarg missing per `lifetime_bound_for_op_str`.
//!
//! **History:**
//! - **Plan 01 (Wave 1)** shipped the shim with a placeholder helper that
//!   returned `Unbounded` for every op-string + an env-gate
//!   `BEAVA_MEMORY_GOV_ENFORCE` defaulting OFF. This file's tests originally
//!   used `count` as the rejection target (Plan-01-stub behavior).
//! - **Plan 04 (Wave 2)** populated the 53-variant / 54-op-string
//!   classification table. `count` is now O1 (accepted); `histogram` without
//!   `buckets` is rejected via `BoundedByRequiredKwarg`. The rejection-tests
//!   below were updated to use `histogram` (no buckets) as the canonical
//!   rejected example.
//! - **Plan 06 (Wave 3)** flips `BEAVA_MEMORY_GOV_ENFORCE` default OFF→ON.
//!   Test `test_no_enforcement_when_env_unset` (which asserted "unset env →
//!   no enforcement") was renamed `test_default_enforcement_on_when_env_unset`
//!   and inverted to assert "unset env → enforcement ON" — the complementary
//!   escape-hatch test (`BEAVA_MEMORY_GOV_ENFORCE=0` → enforcement OFF) lives
//!   in `phase12_8_metrics_endpoint.rs::test_env_var_zero_disables_enforcement`
//!   per the Plan 06 wave-3 file-ownership shift.
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
/// `tests/wal_env_var_tunables.rs` (which uses `std::sync::Mutex` because its
/// tests are `#[test]` — sync). This file uses `#[tokio::test]` and the lock
/// MUST be held across `.await` points (TestServer spawn + post_json), so we
/// use `tokio::sync::Mutex` to satisfy clippy's `await_holding_lock` lint.
/// Tokio's mutex is async-aware and designed for this pattern.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn enforce_on() {
    std::env::set_var(ENV_KEY, "1");
}

fn enforce_off() {
    std::env::remove_var(ENV_KEY);
}

#[tokio::test]
async fn test_unbounded_op_rejected_when_enforcement_enabled() {
    let _guard = ENV_LOCK.lock().await;
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // Tx event + a derivation that group_by user_id and runs a windowless
    // `histogram` op WITHOUT the required `buckets` cap kwarg. Per Plan 04
    // classification, histogram is `BoundedByRequiredKwarg("buckets")` —
    // missing buckets in lifetime mode → reject. (Pre-Plan-04 this test used
    // `count`, but count is now O1 / accepted; histogram-without-buckets is
    // the canonical post-Plan-04 rejection example.)
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
                            "h": {"op": "histogram", "params": {"field": "amount"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "h": "f64"},
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
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
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
        reason.contains("histogram"),
        "reason should name the rejected op `histogram`, got: {reason}"
    );
}

#[tokio::test]
async fn test_windowed_op_passes_when_enforcement_enabled() {
    let _guard = ENV_LOCK.lock().await;
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

/// Plan 12.8-06 (Wave 3): the env-gate default flipped OFF → ON. This test
/// was originally written for the Wave-1 default-OFF posture: "no env var
/// set → enforcement OFF → unbounded ops accepted." Plan 06's `must_haves.truths[0]`
/// flips that semantics: now "no env var set → enforcement ON → unbounded ops
/// REJECTED." The complementary `test_env_var_zero_disables_enforcement` test
/// (lives in `phase12_8_metrics_endpoint.rs` per Plan 06's wave-3 ownership
/// shift) covers the explicit `BEAVA_MEMORY_GOV_ENFORCE=0` escape hatch.
#[tokio::test]
async fn test_default_enforcement_on_when_env_unset() {
    let _guard = ENV_LOCK.lock().await;
    enforce_off();

    let ts = TestServer::spawn().await.expect("spawn");

    // SAME windowless-histogram-without-buckets payload as test 1.
    // Plan 06 default-ON: this MUST now reject (post-flip) — the symmetric
    // assertion to test 1's enforce_on() variant.
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
                            "h": {"op": "histogram", "params": {"field": "amount"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "h": "f64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");

    ts.shutdown().await.ok();

    assert_eq!(
        status, 400,
        "Plan 12.8-06: env-gate default flipped to ON — unset BEAVA_MEMORY_GOV_ENFORCE \
         must now REJECT histogram-without-buckets at register time; got \
         status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "unbounded_op_in_lifetime_mode",
        "expected unbounded_op_in_lifetime_mode rejection, got body={body}"
    );
}

#[tokio::test]
async fn test_unbounded_op_path_includes_node_and_op_index() {
    let _guard = ENV_LOCK.lock().await;
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // 2 derivations: derivation 1 has a windowed count (fine), derivation 2 has
    // a windowless histogram WITHOUT `buckets` (rejected post-Plan-04). Per
    // first-occurrence semantics the shim returns the path of the FIRST
    // offender — which is the second derivation's histogram op.
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
                            "h_total": {"op": "histogram", "params": {"field": "amount"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "h_total": "f64"},
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
        "windowless histogram in derivation 2 must be rejected, got \
         status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(body["error"]["code"], "unbounded_op_in_lifetime_mode");
    let path = body["error"]["path"].as_str().unwrap_or_default();
    // nodes[2] = "Lifetime" derivation, ops[0] = group_by, agg.h_total = the
    // windowless histogram feature whose op-string is "histogram" and which
    // is BoundedByRequiredKwarg("buckets") with the kwarg missing.
    assert_eq!(
        path, "nodes[2].Lifetime.ops[0].agg.h_total",
        "error.path should be nodes[2].Lifetime.ops[0].agg.h_total \
         (first-offender semantics across derivations), got: {path}"
    );
}

#[tokio::test]
async fn test_message_suggests_windowed_kwarg() {
    let _guard = ENV_LOCK.lock().await;
    enforce_on();

    let ts = TestServer::spawn().await.expect("spawn");

    // Windowless histogram WITHOUT `buckets` — the rejection reason text must
    // help the user migrate. Per CONTEXT D-03 + Plan 04 BoundedByRequiredKwarg
    // path, the message names the missing cap kwarg AND offers the `windowed=`
    // alternative.
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
                            "h": {"op": "histogram", "params": {"field": "amount"}}
                        }
                    }
                ],
                "schema": {
                    "fields": {"user_id": "str", "h": "f64"},
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
        "windowless histogram rejected, got {body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    let reason = body["error"]["reason"]
        .as_str()
        .expect("reason should be a string");
    // The message should suggest the `windowed=` kwarg so the user has a
    // clear migration path.
    assert!(
        reason.contains("windowed=\"60s\""),
        "reason should suggest a `windowed=\"60s\"` migration, got: {reason}"
    );
    // Also must name the missing cap kwarg vocabulary fragment so the user
    // knows what specific param to add for their op (`buckets=` for histogram).
    assert!(
        reason.contains("buckets"),
        "reason should mention the missing kwarg `buckets`, got: {reason}"
    );
}
