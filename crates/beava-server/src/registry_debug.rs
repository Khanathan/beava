//! Dev-only endpoints: GET /registry + POST /dev/apply_ops.
//!
//! Plan 12.6-07: legacy axum gating env-var deleted. The mio data-plane
//! `/registry` shim is gated via `AppState.dev_endpoints` which TestServer
//! flips via the `.dev_endpoints(true)` builder. Production data-plane
//! `/registry` is permanently 404; the tokio admin sidecar (cfg.admin_addr)
//! is the canonical surface.
//!
//! # POST /dev/apply_ops
//!
//! Applies a registered derivation's compiled op-chain to a synthetic row and
//! returns the result.  Useful for acceptance tests and interactive debugging.
//!
//! Request body: `{"derivation": "<name>", "row": {<field>: <json value>, ...}}`
//!
//! Response variants:
//! - `{"kept": true, "row": {...}}` — filter kept the row (with transforms applied)
//! - `{"kept": false}` — a Filter op dropped the row
//! - HTTP 404 `{"error": "derivation_not_found"}` — derivation name unknown
//! - HTTP 400 `{"error": "no_compiled_chain"}` — derivation has no compiled ops
//!   (should not happen for derivations registered with ops; defensive guard)
//!
//! **JSON ↔ Value conversion rules (doc-inline in handler):**
//! - `bool`             → `Value::Bool`
//! - integer-fitting Number → `Value::I64`
//! - other Number       → `Value::F64`
//! - string             → `Value::Str`
//! - null               → `Value::Null`
//! - array / object     → `Value::Null` (no nested support in v0)
//!
//! **Row → JSON rules:**
//! - `Value::Null`     → `serde_json::Value::Null`
//! - `Value::I64(n)`   → JSON Number (i64)
//! - `Value::F64(f)`   → JSON Number (f64)
//! - `Value::Bool(b)`  → JSON Bool
//! - `Value::Str(s)`   → JSON String
//! - `Value::Bytes(_)` → JSON Null (binary not representable in JSON v0)
//! - `Value::Datetime(ms)` → JSON Number (i64 ms since epoch)

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use beava_core::agg_apply::apply_event_to_aggregations;
use beava_core::agg_state_table::StateTables;
use beava_core::registry::{DerivationDescriptor, EventDescriptor, Registry, TableDescriptor};
use beava_core::row::{Row, Value};
use beava_core::temporal::TemporalStore;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Axum state for the debug router.
#[derive(Clone)]
pub struct RegistryDebugState {
    pub registry: Arc<Registry>,
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

/// Build the GET /registry sub-router.  Caller merges this into the main
/// router conditionally based on `dev_endpoints_enabled`.
pub fn registry_debug_router(state: RegistryDebugState) -> Router {
    Router::new()
        .route("/registry", get(get_registry))
        .with_state(state)
}

async fn get_registry(State(state): State<RegistryDebugState>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(build_registry_dump(&state.registry)).unwrap())
}

// ─── POST /dev/apply_ops ──────────────────────────────────────────────────────

/// Request body for POST /dev/apply_ops.
#[derive(Debug, Deserialize)]
pub struct ApplyOpsRequest {
    pub derivation: String,
    pub row: BTreeMap<String, serde_json::Value>,
}

/// Build a sub-router for POST /dev/apply_ops. Plan 12.6-07: kept for any
/// remaining legacy axum-router callers (none in production); the mio data
/// plane has no `/dev/apply_ops` route.
pub fn dev_apply_ops_router(registry: Arc<Registry>) -> axum::Router {
    Router::new()
        .route("/dev/apply_ops", post(post_dev_apply_ops))
        .with_state(registry)
}

/// POST /dev/apply_ops handler.
///
/// Looks up the compiled OpChain for `body.derivation`, converts the JSON row
/// to a `Row<Value>`, applies the chain, and returns the result.
///
/// # JSON → Value conversion (per module doc):
/// - bool             → Value::Bool
/// - integer-fitting Number → Value::I64
/// - other Number     → Value::F64
/// - string           → Value::Str
/// - null             → Value::Null
/// - array / object   → Value::Null (no nested support in v0)
///
/// # Row → JSON conversion (per module doc):
/// - Value::Null      → serde_json::Value::Null
/// - Value::I64(n)    → JSON Number (i64)
/// - Value::F64(f)    → JSON Number (f64)
/// - Value::Bool(b)   → JSON Bool
/// - Value::Str(s)    → JSON String
/// - Value::Bytes(_)  → JSON Null  (binary not JSON-representable in v0)
/// - Value::Datetime(ms) → JSON Number (i64 ms since epoch)
async fn post_dev_apply_ops(
    State(registry): State<Arc<Registry>>,
    Json(body): Json<ApplyOpsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Step 1: look up the compiled chain.
    let chain = match registry.compiled_chain(&body.derivation) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "derivation_not_found"})),
            );
        }
    };

    // Step 2: convert JSON row → Row<Value>.
    let mut row = Row::new();
    for (field, jv) in body.row {
        let v = json_to_value(&jv);
        row = row.with_field(&field, v);
    }

    // Step 3: apply the chain.
    match chain.apply(row) {
        None => (StatusCode::OK, Json(serde_json::json!({"kept": false}))),
        Some(updated_row) => {
            let mut obj = serde_json::Map::new();
            for (field, v) in updated_row.0 {
                obj.insert(field.into_string(), value_to_json(v));
            }
            (
                StatusCode::OK,
                Json(serde_json::json!({"kept": true, "row": serde_json::Value::Object(obj)})),
            )
        }
    }
}

// ─── Conversion helpers ───────────────────────────────────────────────────────

/// Convert a `serde_json::Value` to a `beava_core::row::Value`.
/// See module-level doc for the conversion table.
fn json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(f) = n.as_f64() {
                Value::F64(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone().into()),
        // Array / Object → Null (no nested support in v0)
        _ => Value::Null,
    }
}

/// Convert a `beava_core::row::Value` to a `serde_json::Value`.
/// See module-level doc for the conversion table.
///
/// Phase 11 (D-01): `Value::List` → JSON array; `Value::Map` → JSON object.
fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::I64(n) => serde_json::Value::Number(n.into()),
        Value::F64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.into_string()),
        // Bytes not JSON-representable in v0 → Null
        Value::Bytes(_) => serde_json::Value::Null,
        // Datetime: emit as i64 ms since epoch
        Value::Datetime(ms) => serde_json::Value::Number(ms.into()),
        Value::Json(j) => j,
        Value::List(items) => {
            serde_json::Value::Array(items.into_iter().map(value_to_json).collect())
        }
        Value::Map(m) => {
            serde_json::Value::Object(m.into_iter().map(|(k, v)| (k, value_to_json(v))).collect())
        }
    }
}

// ─── POST /dev/apply_events ───────────────────────────────────────────────────

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

/// Request body for `POST /dev/apply_events`.
///
/// **Plan 12.6-06 (D-03 hard rip):** the legacy `event_time_ms` field has been
/// removed; the apply path uses server-side wall-clock at dispatch
/// (`SystemTime::now()`) per `project_redis_shaped_no_event_time_ems`. Stale
/// fixtures sending `event_time_ms` get rejected by serde's strict
/// `deny_unknown_fields` (added below).
///
/// ```json
/// {
///   "source": "Transaction",
///   "row": { "user_id": "alice", "amount": 100.0 }
/// }
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyEventsRequest {
    pub source: String,
    pub row: BTreeMap<String, serde_json::Value>,
}

/// Response body for `POST /dev/apply_events`.
///
/// ```json
/// { "applied_to": ["AggTable1", "AggTable2"] }
/// ```
#[derive(Debug, Serialize)]
pub struct ApplyEventsResponse {
    pub applied_to: Vec<String>,
}

/// Build the sub-router for `POST /dev/apply_events`. Plan 12.6-07: kept
/// for any remaining legacy axum-router callers (none in production); the
/// mio data plane has no `/dev/apply_events` route.
pub fn dev_apply_events_router(state: DevAggState) -> axum::Router {
    Router::new()
        .route("/dev/apply_events", post(post_dev_apply_events))
        .with_state(state)
}

/// `POST /dev/apply_events` handler.
///
/// Converts the JSON row to `Row<Value>`, looks up the event source in the
/// registry (404 if not found), pulls the next monotonic `event_id`, then
/// calls `apply_event_to_aggregations`. Returns the list of aggregation
/// node_names whose state was touched.
///
/// Plan 12.6-07: not mounted in production (the legacy axum router that
/// gated this is gone). Tests may call directly when a registered axum
/// router is constructed in-process.
async fn post_dev_apply_events(
    State(dev_state): State<DevAggState>,
    Json(body): Json<ApplyEventsRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Step 1: validate that source exists in the registry.
    {
        let inner = dev_state.registry.read();
        if !inner.events.contains_key(&body.source) {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "source_not_found"})),
            );
        }
    }

    // Step 2: convert JSON row → Row<Value>.
    let mut row = Row::new();
    for (field, jv) in &body.row {
        let v = json_to_value(jv);
        row = row.with_field(field.as_str(), v);
    }

    // Step 3: pull monotonic event_id (ignored in Phase 5; stable for Phase 6).
    let event_id = dev_state.next_event_id.fetch_add(1, Ordering::SeqCst);

    // Step 4: snapshot which agg node_names exist BEFORE and AFTER to build
    // the `applied_to` list.  We report the aggregations that were touched
    // (their source matches the pushed event's source).
    let matching_aggs: Vec<String> = dev_state
        .registry
        .compiled_aggregations_for_source(&body.source)
        .into_iter()
        .map(|d| d.node_name.clone())
        .collect();

    // Plan 12.6-06: server-side wall-clock at dispatch is the single time
    // source per `project_redis_shaped_no_event_time_ever`. The apply path
    // uses this `now_ms` value instead of any body-derived event-time read.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let now_ms_i64: i64 = now_ms as i64;

    // Step 5: apply under the single-writer lock.
    {
        let mut tables = dev_state.state_tables.lock();
        apply_event_to_aggregations(
            &body.source,
            &row,
            now_ms_i64,
            event_id,
            &dev_state.registry,
            &mut tables,
        );
    }

    // Step 6: bump query_time_ms (Plan 12.6-06: post-Path-X this is fed by
    // server now_ms — see DevAggState.query_time_ms doc-comment).
    if now_ms > 0 {
        dev_state.query_time_ms.fetch_max(now_ms, Ordering::Relaxed);
    }

    (
        StatusCode::OK,
        Json(
            serde_json::to_value(ApplyEventsResponse {
                applied_to: matching_aggs,
            })
            .unwrap(),
        ),
    )
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
