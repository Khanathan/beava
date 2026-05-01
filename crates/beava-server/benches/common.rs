//! Shared bench-bootstrap helper module — Phase 12.6 Plan 11.
//!
//! Builds an `ApplyShard` wired to a fully-initialized `AppState` (Registry,
//! state-tables, idem-cache) and a hand-rolled WAL ring. Used by Phase 12.6+
//! apply-path benches to avoid duplicating the bootstrap across cells.
//!
//! The shape mirrors `crates/beava-server/benches/phase12_07_read_path.rs`
//! `setup_warm_app_state` (lines 32-127) — the canonical server-side bench
//! bootstrap pattern. That pattern was itself derived from the WalGlue +
//! WalBufferRing + WalLsn setup in
//! `crates/beava-server/tests/phase18_02_inline_wal_test.rs` lines 150-174
//! (`dispatch_push_periodic_returns_committed_lsn`), extended to wire the
//! Registry + apply tables so `dispatch_push_sync` resolves event names and
//! updates agg state.
//!
//! ## Why not just import phase12_07_read_path's helper?
//!
//! Per criterion's bench-target convention, each `[[bench]]` is its own
//! integration target with no cross-bench imports. Extracting the helper
//! here lets new Phase 12.6+ benches reuse the bootstrap via `mod common;`
//! without copy-paste.
//!
//! ## API
//!
//! - [`BenchHarness`] — bundle of `ApplyShard`, `Arc<AppState>`, `Arc<Registry>`,
//!   the tempdir/runtime keep-alive, and an `event_name` extracted from the
//!   register payload (so callers don't need to repeat it).
//! - [`build_apply_shard_with_pipeline`] — single entry point. Pass a JSON
//!   register payload; get back a ready-to-push harness.
//!
//! Allow dead_code because each bench target compiles this file independently
//! and may use a subset of the surface.

#![allow(dead_code)]

use beava_core::registry::Registry;
use beava_persistence::{WalSink, WalSinkConfig};
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wal_writer::WalWriter;
use beava_runtime_core::wire_request::WireRequest;
use beava_server::apply_shard::ApplyShard;
use beava_server::idem_cache::IdemCache;
use beava_server::AppState;
use bytes::Bytes;
use std::sync::Arc;
use std::thread::JoinHandle;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// A fully-bootstrapped apply-path harness: shard + state + registry, plus
/// the supporting WAL tempdir, WAL writer thread, and tokio runtime that
/// must outlive the bench closure.
///
/// The `_wal_writer_handle` keeps the WAL writer thread alive for the
/// duration of the harness — without it, `WalBufferRing::append` will
/// eventually block forever waiting for a sealed buffer to be returned to
/// FREE. Even though benches are short-lived, criterion runs the iter loop
/// for many iterations during warmup + measurement, easily exceeding the
/// 3 × 16 MiB ring capacity over multiple cells.
pub struct BenchHarness {
    pub shard: ApplyShard,
    pub state: Arc<AppState>,
    pub registry: Arc<Registry>,
    /// Event name parsed out of the register payload. Convenience so cells
    /// don't repeat the literal in two places.
    pub event_name: String,
    // Keep-alive — these MUST live for the duration of the bench:
    _wal_dir: TempDir,
    _wal_writer_handle: Option<JoinHandle<()>>,
    _rt: Runtime,
}

/// Build an `ApplyShard` wired to a registered pipeline.
///
/// `register_payload` is the JSON value passed to /register (matches the
/// canonical `RegisterPayload` shape — `{nodes: [...]}` with at least one
/// `kind: "event"` node).
///
/// Returns a `BenchHarness` whose `dispatch_wire_request_with_row` is ready
/// for an event-push loop. Caller pushes events via:
///
/// ```ignore
/// harness.shard.dispatch_wire_request_with_row(
///     WireRequest::HttpPush {
///         event_name: harness.event_name.clone(),
///         body: Bytes::from(serde_json::to_vec(&row_value).unwrap()),
///         body_format: beava_core::wire::CT_JSON,
///     },
///     None,
/// );
/// ```
pub fn build_apply_shard_with_pipeline(register_payload: serde_json::Value) -> BenchHarness {
    // 1. Tokio runtime — needed because the Register code path internally
    //    spawns a `current_thread` runtime via apply_shard.rs:180. The push
    //    hot path itself is fully sync (no .await).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("rt");
    let _guard = rt.enter();

    // 2. WAL sink — Register dispatches into the legacy async WAL sink, so
    //    we need a real one. Periodic mode with a fast fsync interval keeps
    //    the bench loop free of long blocking waits.
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

    // 3. Registry + DevAggState (the registry-mediated apply table holder).
    let registry = Arc::new(Registry::new());
    let dev_agg = beava_server::registry_debug::DevAggState::new(Arc::clone(&registry));
    let idem_cache = Arc::new(IdemCache::new());
    let app_state = Arc::new(AppState::new(dev_agg, wal_sink, idem_cache));

    // 4. Hand-rolled WAL ring (the *new* mio-data-plane WAL, parallel to the
    //    legacy WalSink). dispatch_push_sync writes the event record into
    //    this ring; the legacy WalSink is the cold-path register surface.
    //
    //    A WalWriter thread MUST be spawned alongside the ring — without it,
    //    sealed buffers never return to FREE state, and `append` blocks on
    //    `free_condvar` once the ring's 3 × 16 MiB capacity is exhausted.
    //    Tick interval is 5 ms (matches production default in
    //    `beava-server/src/server.rs::ServerV18`).
    let wal_lsn = Arc::new(WalLsn::new());
    let wal_ring = Arc::new(WalBufferRing::new(3, 1 << 24, Arc::clone(&wal_lsn)));
    let writer = WalWriter::new(
        wal_dir.path(),
        Arc::clone(&wal_ring),
        Arc::clone(&wal_lsn),
        5, // tick_ms — production default
    )
    .expect("WalWriter::new on local tempdir");
    let writer_handle = writer.spawn();

    let shard = ApplyShard::new(Arc::clone(&app_state), Arc::clone(&wal_ring), wal_lsn);

    // 5. Send Register through the apply path — this populates the registry
    //    AND grows the state tables in lock-step (apply_shard.rs:189-196).
    let payload_bytes = serde_json::to_vec(&register_payload).expect("register payload to_vec");
    let _ = shard.dispatch_wire_request_with_row(
        WireRequest::Register {
            payload: Bytes::from(payload_bytes),
        },
        None,
    );

    // 6. Extract the event name from the payload — the first "kind": "event"
    //    node is the one cells push to. Panics if absent (caller bug).
    let event_name: String = register_payload
        .get("nodes")
        .and_then(|n| n.as_array())
        .and_then(|nodes| {
            nodes
                .iter()
                .find(|node| node.get("kind").and_then(|k| k.as_str()) == Some("event"))
        })
        .and_then(|node| node.get("name").and_then(|n| n.as_str()))
        .map(|s| s.to_string())
        .expect("register_payload must contain at least one node with kind=\"event\"");

    drop(_guard);
    BenchHarness {
        shard,
        state: app_state,
        registry,
        event_name,
        _wal_dir: wal_dir,
        _wal_writer_handle: Some(writer_handle),
        _rt: rt,
    }
}
