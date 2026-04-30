//! Dev-state types — `DevAggState`, `EventIdEntry`, `RegistryDump`, and
//! `build_registry_dump`.
//!
//! **Plan 12.6-10 (single hot-path entry):** the legacy in-source
//! `dev_apply_ops_router`, `dev_apply_events_router`, `registry_debug_router`,
//! `RegistryDebugState`, `ApplyOpsRequest`, `ApplyEventsRequest`,
//! `ApplyEventsResponse`, `get_registry`, `post_dev_apply_ops`,
//! `post_dev_apply_events`, `json_to_value`, and `value_to_json` —
//! all axum-router orphans preserved by Plan 12.6-07 — are deleted here.
//! Their consumer (`crate::http::router`) was deleted in Plan 12.6-07; the
//! remaining surface had zero call-sites in the workspace and contained the
//! only third caller of `apply_event_to_aggregations` outside the legitimate
//! `apply_shard.rs::dispatch_push_sync` (mio data plane) +
//! `recovery.rs::replay_*` (WAL replay) callers. Removing the orphans makes
//! `crates/beava-server/tests/phase12_6_mio_only_dataplane.rs` pass green.
//!
//! What survives in this module is the pure data-state surface consumed by
//! the live mio data plane:
//!
//! - **`DevAggState`** — single-writer `state_tables` + `registry` + atomic
//!   counters + `temporal_stores` + `event_id_index`, owned by `AppState`,
//!   updated on the apply thread by `apply_shard.rs::dispatch_push_sync`,
//!   read by GET handlers + recovery.
//! - **`EventIdEntry`** — `Stream { event_name: Arc<str> } | TableWrite { ... }`
//!   side-table consumed by `apply_shard.rs::dispatch_push_sync` and
//!   `temporal_http::*_via_mio`.
//! - **`RegistryDump`** + **`build_registry_dump`** — built once per mio
//!   `/registry` request from `apply_shard.rs::dispatch_one`'s
//!   `WireRequest::HttpRegistry` arm; gated on `AppState.dev_endpoints`.
//!
//! Production data-plane `/registry` returns 404 when `dev_endpoints == false`;
//! the tokio admin sidecar (`http_admin.rs::BoundAdminServer` at
//! `cfg.admin_addr`) is the canonical observability surface (per
//! `project_phase18_no_dual_runtime`).

use beava_core::agg_state_table::StateTables;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use beava_core::temporal::TemporalStore;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// Phase 11.5 D-10/D-12 — runtime side-table that maps an event_id (LSN) to
/// the kind of WAL record at that LSN. The retract handler uses this to
/// route stream events to 501, table writes to MVCC retraction, and unknown
/// IDs to 404 — without re-walking the WAL on the hot path.
///
/// Plan 18-12: `Stream.event_name` is `Arc<str>` (was `String`) so the
/// dispatch_push_sync bookkeeping site can clone the Arc — refcount bump,
/// no heap alloc per push — instead of calling `event_name.to_string()`.
/// `Arc<str>` derefs to `&str` and serializes/displays the same way; consumers
/// reading the event_name as a string slice work unchanged.
#[derive(Debug, Clone)]
pub enum EventIdEntry {
    /// A pushed stream event. Retraction is unimplemented in v0; the
    /// handler returns 501 with `stream_retraction_unimplemented`.
    Stream { event_name: Arc<str> },
    /// A table write — temporal or non-temporal. The retract handler
    /// inspects the descriptor at retract-time to decide between 400
    /// (table_not_temporal) and the actual MVCC retraction path.
    TableWrite {
        table_name: String,
        entity_key: Vec<u8>,
        retracted: bool,
    },
}

/// Full registry dump. `_dev_only: true` is a permanent wire sentinel so SDK
/// authors know this endpoint is unstable.
#[derive(Debug, Serialize)]
pub struct RegistryDump {
    pub version: u64,
    pub events: BTreeMap<String, EventDescriptor>,
    pub tables: BTreeMap<String, TableDescriptor>,
    pub derivations: BTreeMap<String, DerivationDescriptor>,
    pub _dev_only: bool, // always true
}

/// Plan 12.6-01: build a `RegistryDump` from a live `Arc<Registry>`.
/// Re-used by the mio data-plane `/registry` route via
/// `apply_shard.rs::dispatch_one`'s `WireRequest::HttpRegistry` arm so
/// the response body matches the legacy axum `get_registry` handler exactly.
pub fn build_registry_dump(registry: &Registry) -> RegistryDump {
    let inner = registry.snapshot();
    let events = inner
        .events
        .into_iter()
        .map(|(k, v)| (k, (*v).clone()))
        .collect();
    RegistryDump {
        version: inner.version,
        events,
        tables: inner.tables,
        derivations: inner.derivations,
        _dev_only: true,
    }
}

/// Shared state for the dev apply-events endpoint and (in Plan 05-06) the dev
/// query endpoint.  Both endpoints share the same `state_tables` so events
/// pushed via `/dev/apply_events` are immediately visible via `/dev/query`.
///
/// Single-writer invariant is preserved at the HTTP layer by the `Mutex` —
/// only the apply handler takes the lock; query handlers use a read-only
/// snapshot.
///
/// # SDK-AGG-02, AGG-CORE-09
#[derive(Clone)]
pub struct DevAggState {
    /// Per-aggregation, per-entity state.  `Mutex` wraps the outer map only;
    /// per-entity `AggOp` state is updated under this single lock (single-writer
    /// invariant per D-06 + project_stateful_architecture.md).
    pub state_tables: Arc<Mutex<StateTables>>,
    /// Registry shared with the main router (read-only from this endpoint).
    pub registry: Arc<Registry>,
    /// Monotonic event-id counter. Feeds `apply_event_to_aggregations`'s
    /// `event_id` parameter; value is ignored in Phase 5 but keeps the
    /// signature stable for Phase 6 WAL (D-08).
    pub next_event_id: Arc<AtomicU64>,
    /// **Plan 12.6-06 D-03 hard rip — renamed from `max_event_time_ms`.**
    ///
    /// Stores the latest server-side wall-clock the apply path saw. Used by
    /// the `/get` query handlers as the query time for windowed-op bucketing
    /// (post-pivot the time-source is server `now_ms` exclusively per
    /// `project_redis_shaped_no_event_time_ever`; the field is fed by the
    /// `apply_shard.rs::dispatch_push_sync` `now_ms` write site rather than
    /// any body-derived event timestamp). Value is 0 until the first event is
    /// applied; readers fall back to wall-clock in that case.
    ///
    /// AtomicU64 (cast from i64) for lock-free reads from the GET hot path.
    pub query_time_ms: Arc<AtomicU64>,

    /// Phase 11.5 D-01 — per-table MVCC stores. Key = table name. Created
    /// lazily on first push-table for a temporal table.
    pub temporal_stores: Arc<Mutex<HashMap<String, TemporalStore>>>,

    /// Phase 11.5 D-10 — event_id → entry index (see EventIdEntry).
    /// Populated at apply-time by /push/{event_name} and
    /// /push-table/{table_name}; consumed at retract-time.
    ///
    /// Plan 18-06 follow-up: swapped from `std::collections::HashMap` (which
    /// hashes `u64` keys with SipHash, ~150–250 ns/insert) to
    /// `hashbrown::HashMap` with `FxBuildHasher` (~50–80 ns/insert). On the
    /// per-push hot path the bookkeeping `bk_evid` substage drops by ~150 ns.
    pub event_id_index: Arc<Mutex<hashbrown::HashMap<u64, EventIdEntry, fxhash::FxBuildHasher>>>,
}

impl DevAggState {
    pub fn new(registry: Arc<Registry>) -> Self {
        DevAggState {
            state_tables: Arc::new(Mutex::new(StateTables::new())),
            registry,
            next_event_id: Arc::new(AtomicU64::new(0)),
            query_time_ms: Arc::new(AtomicU64::new(0)),
            temporal_stores: Arc::new(Mutex::new(HashMap::new())),
            event_id_index: Arc::new(Mutex::new(hashbrown::HashMap::with_hasher(
                fxhash::FxBuildHasher::default(),
            ))),
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Phase 12.6 Plan 06 (Task 2.a / RED) — guards the D-03 hard-rip surface
    /// at the AppState level. Reads the source via `include_str!` and asserts
    /// the post-rip tokens are absent. RED today because DevAggState still
    /// carries the legacy field. Flips GREEN once Task 2.b renames the field
    /// to `query_time_ms` and propagates the rename.
    ///
    /// **Forbidden token is reconstructed at runtime via chunked `concat`** so
    /// the test source itself does not contain the literal it forbids — same
    /// pattern as Plan 05's agg_windowed RED test. Function name is also
    /// chunk-friendly (avoids `max_event_time_ms` as a substring).
    #[test]
    fn dev_agg_state_post_d03_has_no_legacy_max_field() {
        let src = include_str!("registry_debug.rs");
        let stripped: String = src
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .filter(|l| !l.trim_start().starts_with("///"))
            .filter(|l| !l.trim_start().starts_with("//!"))
            .collect::<Vec<_>>()
            .join("\n");
        let forbidden_field = ["max", "_event_time_ms"].concat();
        assert!(
            !stripped.contains(&forbidden_field),
            "Phase 12.6 Plan 06 D-03: DevAggState must not carry a `{forbidden_field}` field/atomic after the hard rip. Found in registry_debug.rs source."
        );
    }

    #[test]
    fn event_id_entry_stream_takes_arc_str() {
        let arc_name: Arc<str> = Arc::from("Txn");
        let entry = EventIdEntry::Stream {
            event_name: arc_name.clone(),
        };

        match entry {
            EventIdEntry::Stream { event_name } => {
                assert_eq!(
                    event_name.as_ref(),
                    "Txn",
                    "Stream.event_name must round-trip the input Arc<str> content"
                );
                assert!(
                    Arc::ptr_eq(&event_name, &arc_name),
                    "Stream.event_name must hold the SAME Arc allocation, not a re-derive"
                );
            }
            EventIdEntry::TableWrite { .. } => panic!("expected EventIdEntry::Stream variant"),
        }
    }
}
