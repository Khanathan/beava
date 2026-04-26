//! Plan 18-12 RED — bookkeeping clones the descriptor's `name_arc`, no alloc.
//!
//! Contract:
//! - After a successful push through `dispatch_push_sync`, the
//!   `event_id_index` carries an `EventIdEntry::Stream` whose `event_name`
//!   field is the SAME `Arc<str>` allocation as the registered descriptor's
//!   `name_arc` (proven via `Arc::ptr_eq`).
//! - Refcount semantics: a successful push bumps the descriptor's
//!   `name_arc` strong_count by 1 (the bookkeeping entry holds it).
//!
//! Under the prior `event_name.to_string()` shape, every push allocated a
//! fresh `String` — `Arc::ptr_eq` would never hold. Under the
//! `Arc::from(event_name)` interim shape (Task 12.2.b), the bookkeeping
//! entry's Arc points at a NEW allocation independent of the descriptor's
//! Arc — `Arc::ptr_eq` still does NOT hold. Only the Task 12.3.b shape
//! (`descriptor.name_arc.clone()`) makes both invariants pass.

use beava_core::registry::Registry;
use beava_core::row::{Row, Value};
use beava_core::wire::CT_JSON;
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::ApplyShard;
use beava_server::idem_cache::IdemCache;
use beava_server::registry_debug::EventIdEntry;
use beava_server::runtime_core_glue::GlueResponse;
use beava_server::AppState;
use bytes::Bytes;
use std::sync::Arc;

struct ShardFixture {
    shard: ApplyShard,
    app_state: Arc<AppState>,
    registry: Arc<Registry>,
    _wal_dir: tempfile::TempDir,
    // Keeping the runtime alive for any background WAL workers spawned during
    // shard construction.
    _rt: tokio::runtime::Runtime,
}

fn make_shard() -> ShardFixture {
    // WalSink::spawn requires a tokio runtime to spawn its background worker
    // into. Multi-thread runtime so the apply path can spin its own
    // current-thread runtime for register without nesting.
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

    // Register a minimal "tx" event — no event_time_field so the server
    // stamps wall-clock at push time, no dedupe to keep the path simple.
    let payload = serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "tx",
                "schema": {
                    "fields": { "amount": "f64", "account_id": "str" },
                    "optional_fields": []
                }
            }
        ]
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let req = WireRequest::Register {
        payload: Bytes::from(payload_bytes),
    };
    let _ = shard.dispatch_wire_request_with_row(req, None);

    drop(_guard);
    ShardFixture {
        shard,
        app_state,
        registry,
        _wal_dir: wal_dir,
        _rt: rt,
    }
}

/// Plan 18-12 RED — Task 12.3.a — bookkeeping reuses the descriptor's
/// `name_arc` allocation. After a successful push:
///
///   1. The `event_id_index` contains an `EventIdEntry::Stream` keyed by
///      the ack_lsn from the push.
///   2. That entry's `event_name` Arc<str> is `Arc::ptr_eq` to the
///      registered descriptor's `name_arc` — same allocation, refcount-
///      bumped, no per-push String/Arc alloc.
///
/// The interim Task 12.2.b shape (`Arc::from(event_name)`) constructs a
/// FRESH Arc<str> per push, so `Arc::ptr_eq` does NOT hold under that
/// shape. Only the Task 12.3.b shape (`descriptor.name_arc.clone()`)
/// makes the assertion pass — this is the strongest form of RED for the
/// alloc-elimination contract.
#[test]
fn dispatch_push_sync_bookkeeping_clones_descriptor_name_arc() {
    let fx = make_shard();

    // Pre-push baseline: capture the descriptor's name_arc allocation.
    let descriptor = fx
        .registry
        .get_event_descriptor("tx")
        .expect("tx must be registered");
    assert_eq!(
        descriptor.name_arc.as_ref(),
        "tx",
        "pre-push: descriptor.name_arc must be populated to event name"
    );

    // Push one event — pre-parsed Row to keep the path deterministic.
    let row = Row::new()
        .with_field("amount", Value::F64(42.0))
        .with_field("account_id", Value::Str("acc_zero".into()));
    let req = WireRequest::TcpPush {
        event_name: "tx".into(),
        body: Bytes::from_static(b"{}"), // body bytes ignored when pre-parsed_row is Some
        body_format: CT_JSON,
    };
    let resps = fx.shard.dispatch_wire_request_with_row(req, Some(row));
    assert_eq!(resps.len(), 1);
    let ack_lsn = match &resps[0] {
        GlueResponse::PushAck { ack_lsn, .. } => *ack_lsn,
        other => panic!("expected PushAck, got {other:?}"),
    };

    // Read back the bookkeeping entry for that LSN.
    let idx = fx.app_state.dev_agg.event_id_index.lock();
    let entry = idx
        .get(&ack_lsn)
        .expect("event_id_index must contain a Stream entry for the just-pushed ack_lsn");
    let entry_name_arc: Arc<str> = match entry {
        EventIdEntry::Stream { event_name } => Arc::clone(event_name),
        EventIdEntry::TableWrite { .. } => panic!("expected EventIdEntry::Stream variant"),
    };
    drop(idx);

    // Sub-assertion 1: content matches.
    assert_eq!(
        entry_name_arc.as_ref(),
        "tx",
        "Stream.event_name must round-trip the event name"
    );

    // Sub-assertion 2: SAME allocation as the registered descriptor's name_arc.
    // This is what proves no per-push alloc — the bookkeeping site refcount-
    // bumped the registry-resident Arc rather than constructing a new one.
    assert!(
        Arc::ptr_eq(&entry_name_arc, &descriptor.name_arc),
        "Plan 18-12 contract violated: bookkeeping must clone the registered \
         descriptor's name_arc (refcount bump, no alloc), but `Arc::ptr_eq` \
         shows a fresh allocation. This means the bookkeeping site is still \
         calling `Arc::from(event_name)` or `event_name.to_string()` instead \
         of `descriptor.name_arc.clone()`."
    );
}
