//! Phase 13.5.1 Plan 07b — `DerivationDescriptor.schema` is `serde(default)`-able.
//!
//! Plan 05 in Phase 13.5.1 added a Python-side `_FIXED_OP_OUTPUT_TYPE` mirror
//! to populate the derivation `schema.fields` map because the server's
//! `DerivationDescriptor` deserializer rejected payloads with `missing field
//! schema`. Plan 05 marked this as Deviation 3 — FORMALIZE-V0.
//!
//! This regression test asserts the server now accepts a register payload
//! with no `schema` field on a `kind: derivation` node. The server's
//! `validate_expressions` runs schema-propagation from upstream + chain
//! and writes the result back to the registry; the deserializer's
//! `serde(default)` lets the wire-format schema-field be omitted entirely.
//!
//! The Python SDK no longer needs to mirror `output_type_for` — the server
//! is the single source of truth.

#![cfg(feature = "testing")]

use beava_server::testing::TestServer;
use serde_json::json;

#[tokio::test]
async fn register_succeeds_when_derivation_omits_schema_field() {
    let ts = TestServer::spawn().await.expect("spawn");

    // Register an event + a derivation that omits `schema` entirely.
    // Server should infer schema.fields from the chain via
    // `validate_expressions` → `OpChain::compile` → `propagated_schemas`.
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Click",
                "schema": {
                    "fields": {"user_id": "str", "page": "str"},
                    "optional_fields": []
                }
            },
            {
                "kind": "derivation",
                "name": "ClickCounts",
                "output_kind": "table",
                "upstreams": ["Click"],
                "ops": [
                    {
                        "op": "group_by",
                        "keys": ["user_id"],
                        "agg": {
                            "n": {
                                "op": "count",
                                "params": {"window": "forever"}
                            }
                        }
                    }
                ],
                // NB: NO `schema` field — server must infer.
                "table_primary_key": ["user_id"]
            }
        ]
    });

    let resp = ts.post_json("/register", &payload).await.expect("register");
    let status = resp.status().as_u16();
    let body_text = resp.text().await.expect("body text");
    assert_eq!(
        status, 200,
        "register without explicit schema field must succeed (serde default + chain inference), got status={status}, body={body_text}"
    );
}
