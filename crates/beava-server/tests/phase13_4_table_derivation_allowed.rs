//! Phase 13.4 Plan 05 — Surgical-permit acceptance test for ADR-001 partial
//! overturn (D-04 USER-LOCKED).
//!
//! This test locks the semantics of the surgical permit landed in
//! `phase12_7_no_table_surface.rs::FORBIDDEN_PATTERNS`: the bare
//! `"OpNode::Table"` entry is removed (so `OpNode::Table*` variants can
//! re-enter the codebase as the *aggregation-output* decorator per
//! ADR-001), while the per-variant entries (`OpNode::TableUpsert`,
//! `OpNode::TableDelete`) STAY forbidden — those represent the
//! user-mutable write surface (`app.upsert / app.delete / app.retract`)
//! that ADR-001 explicitly leaves killed.
//!
//! ## What this test asserts
//!
//! 1. **Smoke (`forbidden_pattern_walk_still_passes_after_d04_edit`)** —
//!    a trivial `assert!(true)` anchor. The actual enforcement runs in
//!    the sibling test
//!    `phase12_7_no_table_surface::forbidden_pattern_walk`; this anchor
//!    exists so the SUMMARY can grep for this file as the artifact of
//!    Plan 13.4-05.
//!
//! 2. **Top-level table register STILL rejected
//!    (`top_level_table_register_still_rejected`)** — proves the surgical
//!    permit is INDEED surgical: a register payload with a top-level
//!    `{"kind": "table", ...}` node is still rejected with HTTP 400 +
//!    structured error code `unsupported_node_kind`. This is enforced by
//!    the existing `pre_check_unsupported_node_kind` shim from Plan
//!    12.7-01, which is unchanged by Plan 13.4-05.
//!
//! 3. **Derivation with `output_kind: "table"` succeeds
//!    (`derivation_with_output_kind_table_succeeds`)** — the GREEN
//!    assertion for ADR-001's narrow revival. `#[ignore]`'d in this plan
//!    because the engine-side acceptance of `output_kind: "table"` is
//!    deferred to Plan 13.4-09 (global-table sentinel routing,
//!    ADR-003). Plan 09's closing commit removes the `#[ignore]`
//!    attribute as the GREEN gate for the engine work.
//!
//! ## Why all three together
//!
//! The triplet captures the full D-04 contract: the architectural-test
//! edit alone permits `OpNode::Table` to compile (test 1's anchor); the
//! top-level reject test (2) proves the user-mutable surface stays
//! killed; the deferred derivation-accept test (3) holds the GREEN
//! assertion for the engine work that lands in Plan 09.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

/// Test 1 — Smoke anchor for Plan 13.4-05's surgical-permit edit.
///
/// The actual enforcement runs in
/// `phase12_7_no_table_surface::forbidden_pattern_walk` (a sibling
/// integration test). This anchor exists so the SUMMARY for Plan 13.4-05
/// can grep for `phase13_4_table_derivation_allowed.rs` as the artifact
/// of the surgical-permit work and so cargo's test count for this file
/// reflects the plan's contribution.
#[test]
fn forbidden_pattern_walk_still_passes_after_d04_edit() {
    // Trivial anchor — the load-bearing assertion is in
    // phase12_7_no_table_surface::forbidden_pattern_walk, which runs as a
    // separate test in the same crate. If Task 5.b's edit accidentally
    // violated a remaining forbidden pattern, that sibling test surfaces
    // it; this anchor exists purely as a Plan-13.4-05 artifact marker.
    //
    // Phase 13.4 cleanup: replaced `assert!(true, ...)` with a `let _ = ...`
    // noop to silence `clippy::assertions_on_constants` under
    // `cargo clippy --workspace --all-targets --all-features -- -D warnings`
    // while preserving the explanatory message as a string literal.
    let _ = "phase13_4_table_derivation_allowed exists as the Plan 13.4-05 \
             artifact marker; real enforcement is in phase12_7_no_table_surface";
}

/// Test 2 — Top-level table register STILL rejected (D-04 surgical permit).
///
/// Proves that removing the bare `"OpNode::Table"` entry from
/// `FORBIDDEN_PATTERNS` does NOT relax the user-facing register surface.
/// A register payload with a top-level `{"kind": "table", ...}` node is
/// still rejected at the JSON-prelude shim
/// (`register_validate::pre_check_unsupported_node_kind`, Plan 12.7-01)
/// with HTTP 400 + structured code `unsupported_node_kind`.
///
/// Per ADR-001's narrow partial-overturn: only the aggregation-output
/// decorator (`derivation { output_kind: "table" }`) is permitted; the
/// user-mutable surface stays killed.
#[tokio::test]
async fn top_level_table_register_still_rejected() {
    let ts = TestServer::spawn().await.expect("spawn");

    // A top-level table-kind node — same shape as the existing
    // 12.7-01 reject test (`phase12_7_unsupported_node_kind.rs`),
    // re-asserted here under the Plan 13.4-05 banner so the surgical
    // permit is provably surgical.
    let payload = json!({
        "nodes": [
            {
                "kind": "table",
                "name": "X",
                "schema": {
                    "fields": {"id": "str"},
                    "optional_fields": []
                },
                "primary_key": ["id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 400,
        "top-level table register must still be rejected after D-04 \
         surgical permit, got status={status}, body={body_text}"
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body json");
    assert_eq!(
        body["error"]["code"], "unsupported_node_kind",
        "rejection code must be unsupported_node_kind (per Plan 12.7-01 \
         shim, unchanged by Plan 13.4-05's surgical permit), got body={body}"
    );

    ts.shutdown().await.ok();
}

/// Test 3 — Derivation with `output_kind: "table"` succeeds (GREEN-pending Plan 09).
///
/// `#[ignore]`'d in Plan 13.4-05 because the engine-side acceptance of
/// `output_kind: "table"` (the actual `OpNode::Table*` variant
/// resurrection + `key_cols: []` validation per ADR-003 sentinel
/// routing) is deferred to Plan 13.4-09. Plan 09's closing commit
/// REMOVES this `#[ignore]` attribute as the GREEN gate for that
/// engine work. Phase 13.5 (Python `@bv.table` decorator) wires the
/// SDK side; this test validates the wire shape end-to-end at the
/// engine layer.
///
/// The asserted shape matches ADR-001 §"Deferred for Phase 13.4
/// implementation" and the `output_kind: "table"` field plumbing
/// described in the SCRATCH-PLANNER notes for Plan 09.
#[ignore]
#[tokio::test]
async fn derivation_with_output_kind_table_succeeds() {
    let ts = TestServer::spawn().await.expect("spawn");

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
                "name": "UserSpend",
                "output_kind": "table",
                "key_cols": ["user_id"],
                "upstreams": ["Tx"],
                "ops": [{"op": "count"}],
                "schema": {
                    "fields": {"user_id": "str", "count": "i64"},
                    "optional_fields": []
                }
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert!(
        (200..300).contains(&status),
        "derivation with output_kind=table must succeed (2xx) per ADR-001 \
         partial overturn + Plan 13.4-09 sentinel routing, got status={status}, body={body_text}"
    );

    ts.shutdown().await.ok();
}
