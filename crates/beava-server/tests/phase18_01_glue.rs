//! Phase 18 Plan 01 — Task 1.4 integration test.
//!
//! Tests that the `runtime_core_glue` layer bridges hand-rolled `WireRequest`
//! parsed frames to the existing `AppState::apply_event` / `AppState::query`
//! path. Uses the feature-gated `testing` harness for server setup.
//!
//! TDD: this file is the RED commit for Task 1.4. It fails until
//! `crates/beava-server/src/runtime_core_glue.rs` is implemented.

#![cfg(feature = "testing")]

use beava_runtime_core::wire_request::WireRequest;
use beava_server::runtime_core_glue::{dispatch_wire_request, GlueResponse};
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn register_payload_bytes() -> Bytes {
    let payload = json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
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
    });
    Bytes::from(serde_json::to_vec(&payload).unwrap())
}

// ─── Task 1.4 RED test ────────────────────────────────────────────────────────

/// Verify that pushing an HTTP WireRequest through dispatch_wire_request
/// routes to AppState and produces a non-error GlueResponse.
#[tokio::test]
async fn http_push_through_glue_applies_event_and_returns_ok() {
    use beava_server::testing::TestServerBuilder;

    let ts = TestServerBuilder::new()
        .dev_endpoints(false)
        .spawn()
        .await
        .expect("spawn test server");

    let app = Arc::clone(&ts.app_state());

    // 1. Register descriptors through the glue layer.
    let reg_req = WireRequest::Register {
        payload: register_payload_bytes(),
    };
    let reg_resp = dispatch_wire_request(&app, reg_req).await;
    // Plan 12.6-01: GlueResponse::RegisterOk + RegisterError were collapsed
    // into the unified Register { http_status, body, tcp_op } variant.
    // Success is indicated by `http_status == 200`.
    match reg_resp {
        GlueResponse::Register {
            http_status: 200, ..
        } => {}
        other => panic!("expected Register {{ http_status: 200, .. }}, got {other:?}"),
    }

    // 2. Push an event through the HTTP glue path.
    let event_body = Bytes::from(
        serde_json::to_vec(&json!({"event_time": 1000, "user_id": "alice", "amount": 42.0}))
            .unwrap(),
    );
    let push_req = WireRequest::HttpPush {
        event_name: "Txn".to_owned(),
        body: event_body,
        body_format: beava_core::wire::CT_JSON,
    };
    let push_resp = dispatch_wire_request(&app, push_req).await;
    match push_resp {
        GlueResponse::PushAck { .. } => {}
        other => panic!("expected PushAck, got {other:?}"),
    }

    // 3. Query the derived table to confirm apply happened.
    let get_req = WireRequest::HttpGetSingle {
        feature: "TxnAgg".to_owned(),
        key: "alice".to_owned(),
    };
    let get_resp = dispatch_wire_request(&app, get_req).await;
    match get_resp {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["cnt"], 1, "expected cnt=1 after one push; got: {v:#}");
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }

    ts.shutdown().await.expect("shutdown");
}
