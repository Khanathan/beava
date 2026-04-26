//! Plan 18-04.8 — body→Row migration to IoPool worker thread.
//!
//! Contract:
//! - `ApplyShard::dispatch_wire_request_with_row(req, Some(pre_parsed))` skips
//!   the body→Row deserialization step inside `dispatch_push_sync` (the
//!   apply-thread `parse` stage drops to function-call overhead).
//! - When `pre_parsed_row = None`, `dispatch_push_sync` falls back to body→Row
//!   parsing (backward-compat for IoPool pre-parse failure).
//! - Malformed body with `pre_parsed_row = None` still yields `invalid_event`.

use beava_core::registry::Registry;
use beava_core::row::{Row, Value};
use beava_core::wire::{CT_JSON, CT_MSGPACK};
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

/// Build a minimal ApplyShard with one registered event "tx" containing
/// `amount: f64` and `account_id: str`.
async fn make_shard() -> ApplyShard {
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
    let dev_agg = beava_server::registry_debug::DevAggState::new(registry);
    let idem_cache = Arc::new(IdemCache::new());
    let app_state = Arc::new(AppState::new(dev_agg, wal_sink, idem_cache));

    let wal_lsn = Arc::new(WalLsn::new());
    let wal_ring = Arc::new(WalBufferRing::new(3, 64 * 1024, Arc::clone(&wal_lsn)));

    let shard = ApplyShard::new(Arc::clone(&app_state), wal_ring, wal_lsn);

    // Register a minimal "tx" event so push paths don't bail with event_not_found.
    let payload = serde_json::json!({
        "events": {
            "tx": {
                "schema": {
                    "fields": {
                        "amount": "f64",
                        "account_id": "str"
                    }
                }
            }
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let req = WireRequest::Register {
        payload: Bytes::from(payload_bytes),
    };
    let _ = shard.dispatch_wire_request_with_row(req, None);
    shard
}

/// Task 8.3.b — `dispatch_wire_request_with_row(req, Some(row))` accepts a
/// pre-parsed Row and uses it. Body bytes can be malformed (we don't parse
/// them inside the apply path when Some is provided).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pre_parsed_row_skips_body_parse() {
    let shard = make_shard().await;

    // Build a Row matching the registered schema.
    let row = Row::new()
        .with_field("amount", Value::F64(99.95))
        .with_field("account_id", Value::Str("acc_123".into()));

    // Provide INVALID body bytes — if they were parsed, push would fail
    // with invalid_event. Since pre_parsed_row is Some, body bytes are
    // ignored on the apply path (they only flow through to WAL).
    let req = WireRequest::TcpPush {
        event_name: "tx".into(),
        body: Bytes::from_static(b"not-valid-json-or-msgpack"),
        body_format: CT_JSON,
    };

    let resps = shard.dispatch_wire_request_with_row(req, Some(row));
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::PushAck { .. } => {} // success
        other => panic!(
            "expected PushAck (pre-parsed Row should bypass body→Row), got {other:?}"
        ),
    }
}

/// Task 8.3.b fallback — `dispatch_wire_request_with_row(req, None)` falls back
/// to body→Row parsing inside `dispatch_push_sync`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_pre_parsed_row_falls_back_to_body_parse() {
    let shard = make_shard().await;

    // Valid msgpack body matching the registered schema.
    use serde::Serialize;
    #[derive(Serialize)]
    struct Body<'a> {
        amount: f64,
        account_id: &'a str,
    }
    let mp = rmp_serde::to_vec_named(&Body {
        amount: 12.34,
        account_id: "acc_b",
    })
    .unwrap();

    let req = WireRequest::TcpPush {
        event_name: "tx".into(),
        body: Bytes::from(mp),
        body_format: CT_MSGPACK,
    };

    let resps = shard.dispatch_wire_request_with_row(req, None);
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::PushAck { .. } => {} // success — fallback parse worked
        other => panic!("expected PushAck via fallback parse, got {other:?}"),
    }
}

/// Malformed body with no pre-parsed Row → invalid_event (existing error path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_malformed_body_no_pre_parsed_yields_invalid_event() {
    let shard = make_shard().await;

    let req = WireRequest::TcpPush {
        event_name: "tx".into(),
        body: Bytes::from_static(b"\xff\xff\xff\xff"),
        body_format: CT_MSGPACK,
    };

    let resps = shard.dispatch_wire_request_with_row(req, None);
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::PushError { code, .. } => {
            assert_eq!(code, &"invalid_event");
        }
        other => panic!("expected PushError(invalid_event), got {other:?}"),
    }
}

/// Backward compat: existing `dispatch_wire_request_sync(req)` still works,
/// equivalent to `dispatch_wire_request_with_row(req, None)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_legacy_dispatch_wire_request_sync_still_works() {
    let shard = make_shard().await;

    use serde::Serialize;
    #[derive(Serialize)]
    struct Body<'a> {
        amount: f64,
        account_id: &'a str,
    }
    let mp = rmp_serde::to_vec_named(&Body {
        amount: 7.0,
        account_id: "acc_legacy",
    })
    .unwrap();

    let req = WireRequest::TcpPush {
        event_name: "tx".into(),
        body: Bytes::from(mp),
        body_format: CT_MSGPACK,
    };

    let resps = shard.dispatch_wire_request_sync(req);
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        GlueResponse::PushAck { .. } => {}
        other => panic!("expected PushAck, got {other:?}"),
    }
}
