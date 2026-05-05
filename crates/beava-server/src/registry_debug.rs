//! Dev-state types: `DevAggState`, `RegistryDump`, `build_registry_dump`.
//!
//! - **`DevAggState`** — single-writer `state_tables` + `registry` + atomic
//!   counters, owned by `AppState`, updated on the apply thread by
//!   `apply_shard::dispatch_push_sync`, read by GET handlers and recovery.
//! - **`RegistryDump`** + **`build_registry_dump`** — built once per mio
//!   `/registry` request from `apply_shard::dispatch_one`'s
//!   `WireRequest::HttpRegistry` arm; gated on `AppState.dev_endpoints`.
//!
//! Production data-plane `/registry` returns 404 when `dev_endpoints` is
//! false; the canonical observability surface is the tokio admin sidecar
//! (`http_admin::BoundAdminServer` on `cfg.admin_addr`), per the mio-only
//! invariant.

use beava_core::agg_state_table::StateTables;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

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

/// Build a `RegistryDump` snapshot from a live `Arc<Registry>`. Used by the
/// mio data-plane `/registry` route.
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

/// Shared state for the dev apply-events endpoint and the dev query
/// endpoint. Both endpoints share `state_tables` so events pushed via
/// `/dev/apply_events` are immediately visible via `/dev/query`.
///
/// Single-writer invariant: only the apply handler takes the `Mutex`; query
/// handlers use a read-only snapshot.
#[derive(Clone)]
pub struct DevAggState {
    /// Per-aggregation, per-entity state. The `Mutex` wraps only the outer
    /// map; per-entity `AggOp` state is updated under this single lock.
    pub state_tables: Arc<Mutex<StateTables>>,
    /// Registry shared with the main router (read-only from this endpoint).
    pub registry: Arc<Registry>,
    /// Monotonic event-id counter. Feeds `apply_event_to_aggregations`'s
    /// `event_id` parameter and the WAL.
    pub next_event_id: Arc<AtomicU64>,
    /// Latest server-side `now_ms` observed on the apply path. Used by `/get`
    /// handlers as the query time for windowed-op bucketing — server time is
    /// the only time source per `project_redis_shaped_no_event_time_ever`,
    /// fed from the `apply_shard::dispatch_push_sync` write site. Value is
    /// 0 until the first event applies; readers fall back to wall-clock in
    /// that case.
    ///
    /// `AtomicU64` (cast from i64) for lock-free reads on the GET hot path.
    pub query_time_ms: Arc<AtomicU64>,
}

impl DevAggState {
    pub fn new(registry: Arc<Registry>) -> Self {
        DevAggState {
            state_tables: Arc::new(Mutex::new(StateTables::new())),
            registry,
            next_event_id: Arc::new(AtomicU64::new(0)),
            query_time_ms: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    /// Tripwire: `DevAggState` must never carry a `max_event_time_ms` field
    /// again. The forbidden literal is rebuilt via chunked `concat` so this
    /// test file itself does not contain the substring it forbids.
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
            "DevAggState must not carry a `{forbidden_field}` field/atomic. Found in registry_debug.rs source."
        );
    }
}
