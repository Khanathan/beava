//! Plan 12-09 Wave 1 — `dispatch_get_batch_sync` body_format branching.
//!
//! Drives `dispatch_get_batch_sync` directly with `body_format` byte and asserts:
//! 1. `CT_MSGPACK` body parses + response is msgpack-shaped + `format == CT_MSGPACK`
//! 2. `CT_JSON` body parses + response is JSON-shaped + `format == CT_JSON`  (regression guard)
//! 3. Unsupported byte (0xFF) returns `InternalError` carrying `"unsupported content_type"`
//! 4. msgpack response shape parses to the same `serde_json::Value` as JSON response (Wave 2)
//!
//! RED until Wave 1 Task 1.b adds `body_format: u8` param + branches.

#![cfg(feature = "testing")]

use beava_core::registry::Registry;
use beava_core::wire::{CT_JSON, CT_MSGPACK};
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::ApplyShard;
use beava_server::idem_cache::IdemCache;
use beava_server::runtime_core_glue::{dispatch_get_batch_sync, GlueResponse};
use beava_server::AppState;
use bytes::Bytes;
use std::sync::Arc;

// ─── Test harness ─────────────────────────────────────────────────────────────

struct AppFixture {
    app_state: Arc<AppState>,
    _wal_dir: tempfile::TempDir,
    _rt: tokio::runtime::Runtime,
}

/// Boot AppState, register Txn -> TxnAgg(cnt by user_id), push 10 Txns for alice.
fn setup_app_state_with_count_pipeline() -> AppFixture {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("test rt");
    let _guard = rt.enter();

    let wal_dir = tempfile::tempdir().expect("wal tempdir");
    let (wal_sink, _wal_worker) = WalSink::spawn(WalSinkConfig {
        dir: wal_dir.path().to_path_buf(),
        initial_start_lsn: 1,
        initial_registry_version: 1,
        fsync_interval_ms: 100,
        fsync_bytes: 0,
        segment_bytes: 64 * 1024 * 1024,
        sync_mode: beava_persistence::SyncMode::Periodic,
    })
    .expect("wal spawn");

    let registry = Arc::new(Registry::new());
    let dev_agg = beava_server::registry_debug::DevAggState::new(Arc::clone(&registry));
    let idem_cache = Arc::new(IdemCache::new());
    let app_state = Arc::new(AppState::new(dev_agg, wal_sink, idem_cache));

    let wal_lsn = Arc::new(WalLsn::new());
    let wal_ring = Arc::new(WalBufferRing::new(3, 64 * 1024, Arc::clone(&wal_lsn)));

    let shard = ApplyShard::new(Arc::clone(&app_state), wal_ring, wal_lsn);

    let reg_payload = serde_json::json!({
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
                "event_time_field": "event_time"
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{
                    "op": "group_by",
                    "keys": ["user_id"],
                    "agg": {"cnt": {"op": "count", "params": {}}}
                }],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let reg_bytes = serde_json::to_vec(&reg_payload).unwrap();
    let _ = shard.dispatch_wire_request_with_row(
        WireRequest::Register {
            payload: Bytes::from(reg_bytes),
        },
        None,
    );

    // Push 10 events for alice.
    for i in 0..10 {
        let event_body = serde_json::json!({
            "event_time": 1000 + i,
            "user_id": "alice",
            "amount": 42.0
        });
        let push_bytes = serde_json::to_vec(&event_body).unwrap();
        let _ = shard.dispatch_wire_request_with_row(
            WireRequest::HttpPush {
                event_name: "Txn".to_string(),
                body: Bytes::from(push_bytes),
                body_format: CT_JSON,
            },
            None,
        );
    }

    drop(_guard);
    AppFixture {
        app_state,
        _wal_dir: wal_dir,
        _rt: rt,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Test 1 — CT_MSGPACK body parses, response is framed with format=CT_MSGPACK, payload
/// is msgpack-encoded and round-trips back to the expected `{result: {alice: {cnt: 10}}}` shape.
#[test]
fn test_dispatch_get_batch_msgpack_body_returns_msgpack_format() {
    let fx = setup_app_state_with_count_pipeline();
    let req = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let mp_body = rmp_serde::to_vec_named(&req).expect("msgpack encode req body");
    let resp = dispatch_get_batch_sync(&fx.app_state, &Bytes::from(mp_body), CT_MSGPACK);
    match resp {
        GlueResponse::QueryResult { body, format } => {
            assert_eq!(
                format, CT_MSGPACK,
                "expected response format=CT_MSGPACK, got 0x{format:02x}"
            );
            let v: serde_json::Value =
                rmp_serde::from_slice(&body).expect("response body decodes as msgpack");
            assert_eq!(
                v["result"]["alice"]["cnt"], 10,
                "expected alice.cnt=10, got {v:#}"
            );
        }
        other => panic!("expected QueryResult{{..,format:CT_MSGPACK}}, got {other:?}"),
    }
}

/// Test 2 — CT_JSON body unchanged: response framed with format=CT_JSON, payload
/// is JSON-shaped (regression guard for Wave 4 etc.).
#[test]
fn test_dispatch_get_batch_json_body_unchanged() {
    let fx = setup_app_state_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, CT_JSON);
    match resp {
        GlueResponse::QueryResult { body, format } => {
            assert_eq!(
                format, CT_JSON,
                "expected response format=CT_JSON, got 0x{format:02x}"
            );
            let v: serde_json::Value =
                serde_json::from_slice(&body).expect("response body decodes as JSON");
            assert_eq!(
                v["result"]["alice"]["cnt"], 10,
                "expected alice.cnt=10, got {v:#}"
            );
        }
        other => panic!("expected QueryResult{{..,format:CT_JSON}}, got {other:?}"),
    }
}

/// Test 3 — Unsupported content_type byte returns InternalError carrying
/// "unsupported content_type" + the byte rendered as "0xff".
#[test]
fn test_dispatch_get_batch_unsupported_format_returns_internal_error() {
    let fx = setup_app_state_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, 0xFF);
    match resp {
        GlueResponse::InternalError { reason } => {
            assert!(
                reason.contains("unsupported content_type"),
                "expected 'unsupported content_type' in reason, got: {reason}"
            );
            assert!(
                reason.contains("0xff"),
                "expected '0xff' in reason, got: {reason}"
            );
        }
        other => panic!("expected InternalError, got {other:?}"),
    }
}

/// Test 4 (Wave 2) — msgpack response and JSON response decode to the SAME
/// `serde_json::Value`. Confirms `to_vec_named` (not plain `to_vec`) is the
/// right encoder choice — `to_vec` would emit sequential-integer keys for
/// the inner Map, breaking shape parity with JSON's string-keyed objects.
#[test]
fn test_msgpack_and_json_responses_are_shape_equivalent() {
    let fx = setup_app_state_with_count_pipeline();

    // JSON request body.
    let json_body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
    let json_resp = dispatch_get_batch_sync(&fx.app_state, &json_body, CT_JSON);

    // msgpack request body — equivalent shape.
    let req = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let mp_body = rmp_serde::to_vec_named(&req).expect("msgpack encode req body");
    let mp_resp = dispatch_get_batch_sync(&fx.app_state, &Bytes::from(mp_body), CT_MSGPACK);

    let (json_value, mp_value) = match (json_resp, mp_resp) {
        (
            GlueResponse::QueryResult {
                body: jb,
                format: jf,
            },
            GlueResponse::QueryResult {
                body: mb,
                format: mf,
            },
        ) => {
            assert_eq!(jf, CT_JSON);
            assert_eq!(mf, CT_MSGPACK);
            let jv: serde_json::Value = serde_json::from_slice(&jb).expect("json body");
            let mv: serde_json::Value = rmp_serde::from_slice(&mb).expect("msgpack body");
            (jv, mv)
        }
        other => panic!("expected two QueryResult variants, got {other:?}"),
    };

    assert_eq!(
        json_value, mp_value,
        "json and msgpack responses must decode to equivalent serde_json::Value"
    );
}
