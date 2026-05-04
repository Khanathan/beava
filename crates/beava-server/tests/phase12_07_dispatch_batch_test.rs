//! Plan 12-07 Wave 4 — real `dispatch_get_batch` impl tests.
//!
//! Drives `dispatch_get_batch_sync` directly and asserts:
//! 1. Real result map after register + push (vs the Plan 18-01 stub `{"result":{}}`)
//! 2. Missing keys are OMITTED from the result map (no null entries)
//! 3. Unknown features yield InternalError carrying "feature_not_found"
//! 4. Cell-cap > 10_000 yields InternalError carrying "batch_too_large"
//!
//! RED until Wave 4 Task 4.b replaces the stub.

#![cfg(feature = "testing")]

use beava_core::registry::Registry;
use beava_core::wire::CT_JSON;
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

/// Boot AppState, register Txn -> TxnAgg(cnt by user_id), push 1 Txn for alice.
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

    let event_body = serde_json::json!({
        "event_time": 1000,
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

    drop(_guard);
    AppFixture {
        app_state,
        _wal_dir: wal_dir,
        _rt: rt,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// dispatch_get_batch returns real per-key/per-feature values for known keys.
/// Plan 13.4-02: response is now flat-dict per Phase 13.0-15 wire-spec
/// (no historic `{"result": ...}` envelope).
#[test]
fn test_dispatch_get_batch_returns_real_results() {
    let fx = setup_app_state_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, CT_JSON);
    match resp {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(
                v.get("result").is_none(),
                "result envelope must be absent (Plan 13.4-02), got {v:#}"
            );
            assert_eq!(v["alice"]["cnt"], 1, "expected alice.cnt=1, got {v:#}");
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }
}

/// dispatch_get_batch OMITS keys with no matching state (no null, no empty obj).
/// Mirrors the axum-side post_get_batch_handler behavior.
#[test]
fn test_dispatch_get_batch_omits_missing_keys() {
    let fx = setup_app_state_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice","ghost"],"features":["cnt"]}"#);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, CT_JSON);
    match resp {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            // Plan 13.4-02: dropped `{"result": ...}` envelope per Phase 13.0-15.
            assert!(
                v.get("result").is_none(),
                "result envelope must be absent (Plan 13.4-02), got {v:#}"
            );
            assert!(v["alice"].is_object(), "alice should be present, got {v:#}");
            assert!(
                v.get("ghost").is_none(),
                "ghost should be omitted (not null), got {v:#}"
            );
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }
}

/// dispatch_get_batch returns InternalError carrying "feature_not_found" when
/// any requested feature is unknown.
#[test]
fn test_dispatch_get_batch_returns_error_for_unknown_feature() {
    let fx = setup_app_state_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice"],"features":["unknown_feat"]}"#);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, CT_JSON);
    match resp {
        GlueResponse::InternalError { reason } => {
            assert!(
                reason.contains("feature_not_found"),
                "expected feature_not_found in reason, got: {reason}"
            );
            assert!(
                reason.contains("unknown_feat"),
                "expected unknown_feat name in reason, got: {reason}"
            );
        }
        other => panic!("expected InternalError feature_not_found, got {other:?}"),
    }
}

/// Cell-cap (10_000): keys × features > 10_000 -> InternalError "batch_too_large".
#[test]
fn test_dispatch_get_batch_enforces_cell_cap_10000() {
    let fx = setup_app_state_with_count_pipeline();
    // 10_001 keys × 1 feature = 10_001 cells > BATCH_CAP=10_000.
    let keys: Vec<String> = (0..10_001).map(|i| format!("k{i}")).collect();
    let payload = serde_json::json!({"keys": keys, "features": ["cnt"]});
    let body_bytes = serde_json::to_vec(&payload).unwrap();
    let body = Bytes::from(body_bytes);
    let resp = dispatch_get_batch_sync(&fx.app_state, &body, CT_JSON);
    match resp {
        GlueResponse::InternalError { reason } => {
            assert!(
                reason.contains("batch_too_large"),
                "expected batch_too_large in reason, got: {reason}"
            );
        }
        other => panic!("expected InternalError batch_too_large, got {other:?}"),
    }
}
