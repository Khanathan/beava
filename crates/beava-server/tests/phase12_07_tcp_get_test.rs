//! Plan 12-07 Wave 3 — apply_shard dispatch arms for TcpGet/TcpMGet/TcpGetMulti.
//!
//! Drives `ApplyShard::dispatch_one` directly (no full server boot required)
//! after registering a Txn -> TxnAgg(cnt) pipeline and pushing one event for
//! alice. Asserts that the new TCP /get variants route to
//! `dispatch_get_single_sync` / `dispatch_get_batch_sync` and produce
//! `GlueResponse::QueryResult` (or `QueryNotFound` for unknown features).
//!
//! RED until Wave 3 Task 3.b adds the dedicated arms in apply_shard.rs —
//! today the variants fall through to the catch-all `WireRequest::Unknown |
//! WireRequest::ParseError | WireRequest::TcpGet { .. } | ... =>
//! GlueResponse::Unsupported` arm.

#![cfg(feature = "testing")]

use beava_core::registry::Registry;
use beava_core::wire::CT_JSON;
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::ApplyShard;
use beava_server::idem_cache::IdemCache;
use beava_server::runtime_core_glue::GlueResponse;
use beava_server::AppState;
use bytes::Bytes;
use std::sync::Arc;

// ─── Test harness ─────────────────────────────────────────────────────────────

struct ShardFixture {
    shard: ApplyShard,
    _wal_dir: tempfile::TempDir,
    _rt: tokio::runtime::Runtime,
}

/// Boot an ApplyShard, register a Txn pipeline with a TxnAgg(cnt) aggregation,
/// and push one event for alice. Returns the shard ready for /get dispatch tests.
fn setup_apply_shard_with_count_pipeline() -> ShardFixture {
    // WalSink::spawn requires a tokio runtime to spawn its background worker.
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

    // Register Txn -> TxnAgg(cnt by user_id).
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
    let reg_resps = shard.dispatch_wire_request_with_row(
        WireRequest::Register {
            payload: Bytes::from(reg_bytes),
        },
        None,
    );
    // Plan 12.6-01: success path is `Register { http_status: 200, .. }`.
    match &reg_resps[0] {
        GlueResponse::Register {
            http_status: 200, ..
        } => {}
        other => panic!("registration failed: {other:?}"),
    }

    // Push one Txn event for alice.
    let event_body = serde_json::json!({
        "event_time": 1000,
        "user_id": "alice",
        "amount": 42.0
    });
    let push_bytes = serde_json::to_vec(&event_body).unwrap();
    let push_resps = shard.dispatch_wire_request_with_row(
        WireRequest::HttpPush {
            event_name: "Txn".to_string(),
            body: Bytes::from(push_bytes),
            body_format: CT_JSON,
        },
        None,
    );
    match &push_resps[0] {
        GlueResponse::PushAck { .. } => {}
        other => panic!("push failed: {other:?}"),
    }

    drop(_guard);
    ShardFixture {
        shard,
        _wal_dir: wal_dir,
        _rt: rt,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// TcpGet (verb-style single-row) routes to
/// dispatch_get_single_verb_style_sync and returns QueryResult { body: FLAT
/// feature dict } for a known table/key.
///
/// Plan 13.4.1-04 (D-01): TCP OP_GET body is verb-style
/// `{table, key, features?}`; legacy `{feature, key}` is rejected with
/// `unsupported_request_shape` (D-05). Migrated by Plan 13.4.1-05 closure.
/// Plan 13.4.1-04 (D-03): response is the FLAT feature dict (no `value`
/// envelope, no `{table, entity_id, features}` wrapper).
#[test]
fn test_apply_shard_dispatches_tcp_get_single() {
    let fx = setup_apply_shard_with_count_pipeline();
    let body = Bytes::from_static(br#"{"table":"TxnAgg","key":"alice"}"#);
    let resps = fx.shard.dispatch_wire_request_with_row(
        WireRequest::TcpGet {
            body,
            body_format: CT_JSON,
        },
        None,
    );
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(v["cnt"], 1, "expected cnt=1, got {v:#}");
            assert!(
                v.get("table").is_none(),
                "FLAT row must NOT carry table envelope key (D-03), got {v:#}"
            );
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }
}

/// TcpMGet (single feature, multi key) returns batch result map containing
/// alice (pushed) but omitting bob (no events).
#[test]
fn test_apply_shard_dispatches_tcp_mget() {
    let fx = setup_apply_shard_with_count_pipeline();
    let body = Bytes::from_static(br#"{"feature":"cnt","keys":["alice","bob"]}"#);
    let resps = fx.shard.dispatch_wire_request_with_row(
        WireRequest::TcpMGet {
            body,
            body_format: CT_JSON,
        },
        None,
    );
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(body).unwrap();
            // Plan 13.4-02: dropped `{"result": ...}` envelope per Phase 13.0-15.
            assert!(
                v.get("result").is_none(),
                "result envelope must be absent (Plan 13.4-02), got {v:#}"
            );
            assert_eq!(v["alice"]["cnt"], 1, "expected alice.cnt=1, got {v:#}");
            assert!(
                v.get("bob").is_none(),
                "bob should be omitted (no events), got {v:#}"
            );
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }
}

/// TcpGetMulti (multi feature, multi key) mirrors HTTP /get shape and routes
/// to dispatch_get_batch_sync.
#[test]
fn test_apply_shard_dispatches_tcp_get_multi() {
    let fx = setup_apply_shard_with_count_pipeline();
    let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
    let resps = fx.shard.dispatch_wire_request_with_row(
        WireRequest::TcpGetMulti {
            body,
            body_format: CT_JSON,
        },
        None,
    );
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::QueryResult { body, format: _ } => {
            let v: serde_json::Value = serde_json::from_slice(body).unwrap();
            // Plan 13.4-02: dropped `{"result": ...}` envelope per Phase 13.0-15.
            assert!(
                v.get("result").is_none(),
                "result envelope must be absent (Plan 13.4-02), got {v:#}"
            );
            assert_eq!(
                v["alice"]["cnt"], 1,
                "expected alice.cnt=1 for TcpGetMulti, got {v:#}"
            );
        }
        other => panic!("expected QueryResult, got {other:?}"),
    }
}

/// TcpGet for an unknown table returns QueryNotFound { code: "unknown_table" }.
///
/// Plan 13.4.1-04 (D-01): verb-style `OP_GET` resolves `table` (not feature)
/// against the registry; an unregistered table name maps to `unknown_table`
/// per `dispatch_get_single_verb_style_sync`'s `compiled_aggregation` miss
/// path. Pre-13.4.1 this test asserted `feature_not_found` against the
/// legacy `{feature, key}` shape — migrated by Plan 13.4.1-05 closure.
#[test]
fn test_apply_shard_tcp_get_unknown_feature_returns_query_not_found() {
    let fx = setup_apply_shard_with_count_pipeline();
    let body = Bytes::from_static(br#"{"table":"DoesNotExist","key":"alice"}"#);
    let resps = fx.shard.dispatch_wire_request_with_row(
        WireRequest::TcpGet {
            body,
            body_format: CT_JSON,
        },
        None,
    );
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::QueryNotFound { code } => {
            assert_eq!(*code, "unknown_table", "expected unknown_table");
        }
        other => panic!("expected QueryNotFound unknown_table, got {other:?}"),
    }
}
