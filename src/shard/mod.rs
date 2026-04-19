//! Per-shard state module (v1.2 TPC Wave 1 — TPC-PERF-01).
//!
//! `Shard` is the sole data-path unit at N=1. Each Shard owns:
//! - `state` — entity-state storage. Default build: `fjall::PartitionHandle`
//!   (Phase 53-03, TPC-PERSIST-01). Under `--features state-inmem` (D-03):
//!   `AHashMap<EntityKey, EntityState>` — the Phase 49 legacy path kept for
//!   dev-mode A/B benchmarks.
//! - `dirty_set: HashSet<EntityKey>` — plain; single writer (shard thread), no arc-swap
//! - `watermark: WatermarkState` — per-shard; replaces WatermarkTracker (Plan 49-03)
//! - `event_log: Option<EventLog>` — points at data/logs/{stream}.bin in Wave 1 (D-03)
//!
//! ## Single-writer invariant (default / fjall build)
//!
//! `fjall::PartitionHandle` is `Clone + Send + Sync` and all of its mutating
//! ops take `&self`. The type system does NOT enforce the single-writer
//! invariant; it is a **convention**: only the shard thread that owns the
//! `Shard` may mutate its partition via `StoreView::Sharded`. Do NOT clone
//! the handle into another thread for writes. Concurrent readers (e.g.
//! snapshot fan-out) may hold clones for reads only.

/// fjall 2.11 keyspace + partition lifecycle (Phase 53 Plan 02, D-01
/// one-keyspace layout). Plan 03 wires `Shard.state` in here.
pub mod fjall_backend;
pub mod global_watermark;
/// Per-shard Prometheus metrics (Phase 50-02, D-07).
pub mod metrics;
/// Phase 53-03 (D-03): legacy `ShardedStateStoreV1` is gated behind the
/// dev-mode `state-inmem` feature. The default (fjall) build does NOT compile
/// this module — Plan 03B introduces `ShardedStateStoreFjall` as its
/// production-build sibling.
#[cfg(feature = "state-inmem")]
pub mod store;
/// Phase 53-03B: fjall-backed `ShardedStateStore` — default (non-state-inmem) build.
#[cfg(not(feature = "state-inmem"))]
pub mod store_fjall;
/// Shard thread lifecycle: spawn, ready-barrier, pinning, quarantine (Phase 50-03).
pub mod thread;
pub mod traits;
pub mod watermark;

#[cfg(feature = "state-inmem")]
use ahash::AHashMap;
use std::collections::HashSet;

use crate::state::event_log::EventLog;
use crate::state::store::EntityState;
use watermark::WatermarkState;

/// Entity key type alias (mirrors crate::types::EntityKey = String).
pub type EntityKey = String;

// ---------------------------------------------------------------------------
// Shard struct — two `#[cfg]`-guarded variants (Phase 53-03 TPC-PERSIST-01).
// ---------------------------------------------------------------------------

/// Per-shard state container (default, fjall build). Single writer — no lock.
///
/// `state` is a `fjall::PartitionHandle`, a cheap-to-clone, ref-counted handle
/// to the shard's partition within the single keyspace at `data/fjall/`. See
/// the module-level "single-writer invariant" note.
#[cfg(not(feature = "state-inmem"))]
pub struct Shard {
    /// Entity state: postcard(`SerializableEntityState`) values keyed by
    /// `entity_key.as_bytes()` inside a per-shard fjall partition.
    pub state: fjall::PartitionHandle,
    /// Dirty-set for snapshot delta: keys modified since last snapshot.
    /// Plain HashSet — no arc-swap needed because this shard is single-writer.
    pub dirty_set: HashSet<EntityKey>,
    /// Per-shard event log handle (Wave 1: same path as today — D-03).
    pub event_log: Option<EventLog>,
    /// Per-shard watermark state (replaces WatermarkTracker on PipelineEngine — Plan 49-03).
    pub watermark: WatermarkState,
    /// Phase 53-05 (W-4): accumulated postcard byte count written into
    /// `state` since the last `take_write_bytes()` sample. The shard event
    /// loop drains this counter every gauge tick and emits
    /// `beava_fjall_write_bytes_total{shard=N}`. Non-atomic because the
    /// shard is single-writer (thread owns it exclusively).
    pub write_bytes_since_sample: u64,
}

/// Per-shard state container (dev-only `state-inmem` build). Single writer.
///
/// Wave 1: N=1, so exactly one Shard exists. Event log path is
/// `data/logs/{stream}.bin` (existing layout, D-03 — Wave 1 keeps current path).
#[cfg(feature = "state-inmem")]
pub struct Shard {
    /// Entity state: key → EntityState. AHashMap (not DashMap) — single-threaded owner.
    pub state: AHashMap<EntityKey, EntityState>,
    /// Dirty-set for snapshot delta: keys modified since last snapshot.
    /// Plain HashSet — no arc-swap needed because this shard is single-writer.
    pub dirty_set: HashSet<EntityKey>,
    /// Per-shard event log handle (Wave 1: same path as today — D-03).
    pub event_log: Option<EventLog>,
    /// Per-shard watermark state (replaces WatermarkTracker on PipelineEngine — Plan 49-03).
    pub watermark: WatermarkState,
}

impl Shard {
    /// Create a Shard backed by a fjall partition (Phase 53-03 default build).
    ///
    /// The caller (boot path or Plan 03B's `ShardedStateStoreFjall`) is
    /// responsible for opening the partition via
    /// `shard::fjall_backend::open_shard_partition`.
    #[cfg(not(feature = "state-inmem"))]
    pub fn with_partition(state: fjall::PartitionHandle) -> Self {
        Shard {
            state,
            dirty_set: HashSet::new(),
            event_log: None,
            watermark: WatermarkState::new(),
            write_bytes_since_sample: 0,
        }
    }

    /// Phase 53-05 (W-4): drain the accumulated write-bytes counter and
    /// return its prior value. Called once per gauge-sample tick from the
    /// shard event loop to feed `beava_fjall_write_bytes_total{shard=N}`.
    #[cfg(not(feature = "state-inmem"))]
    pub fn take_write_bytes(&mut self) -> u64 {
        std::mem::replace(&mut self.write_bytes_since_sample, 0)
    }

    /// Create a new empty Shard (state-inmem only — AHashMap backend).
    #[cfg(feature = "state-inmem")]
    pub fn new() -> Self {
        Shard {
            state: AHashMap::new(),
            dirty_set: HashSet::new(),
            event_log: None,
            watermark: WatermarkState::new(),
        }
    }

    /// Create a Shard with an attached event log (state-inmem only).
    #[cfg(feature = "state-inmem")]
    pub fn with_event_log(event_log: EventLog) -> Self {
        Shard {
            state: AHashMap::new(),
            dirty_set: HashSet::new(),
            event_log: Some(event_log),
            watermark: WatermarkState::new(),
        }
    }
}

#[cfg(feature = "state-inmem")]
impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// EntityState <-> bytes conversion helpers (default / fjall build only).
//
// `EntityState` itself is NOT Serialize/Deserialize (it carries an
// `AtomicU64` and `AHashMap`s), but `SerializableEntityState` is — that's
// the same wire format used by snapshot v8. The Plan 01 spike measured
// postcard(SerializableEntityState) p95 = 64 B on our workload, well under
// the fjall 4 KiB block size.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "state-inmem"))]
fn entity_to_bytes(entity: &EntityState) -> Vec<u8> {
    use crate::state::snapshot::{SerializableEntityState, SerializableStreamEntityState};
    use crate::state::store::SerializableTableRow;

    let ser = SerializableEntityState {
        streams: entity
            .streams
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    SerializableStreamEntityState {
                        operators: s.operators.clone(),
                        last_event_at: s.last_event_at,
                    },
                )
            })
            .collect(),
        static_features: entity
            .static_features
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        table_rows: entity
            .table_rows
            .iter()
            .map(|(k, v)| (k.clone(), SerializableTableRow::from(v)))
            .collect(),
    };
    postcard::to_stdvec(&ser).expect("postcard serialize SerializableEntityState")
}

#[cfg(not(feature = "state-inmem"))]
fn entity_from_bytes(bytes: &[u8]) -> Option<EntityState> {
    use crate::state::snapshot::SerializableEntityState;
    use crate::state::store::{StreamEntityState, TableRow};

    let ser: SerializableEntityState = postcard::from_bytes(bytes).ok()?;
    let mut streams: ahash::AHashMap<String, StreamEntityState> = ahash::AHashMap::new();
    for (name, s) in ser.streams {
        streams.insert(
            name,
            StreamEntityState {
                operators: s.operators,
                last_event_at: s.last_event_at,
            },
        );
    }
    Some(EntityState {
        streams,
        static_features: ser.static_features.into_iter().collect(),
        table_rows: ser
            .table_rows
            .into_iter()
            .map(|(k, v)| (k, TableRow::from(v)))
            .collect(),
        dirty_gen: std::sync::atomic::AtomicU64::new(0),
    })
}

// ---------------------------------------------------------------------------
// Phase 50.5-01: StoreView enum — cascade-shape shim (Wave 0 chose enum <5 sites)
// Phase 53-03: Sharded arm reworked to round-trip through postcard + fjall.
// ---------------------------------------------------------------------------

/// Storage view abstraction for `push_with_cascade_internal`.
///
/// Chosen shape: enum (CASCADE-SHAPE.md: 4 call sites, 2 distinct methods → enum).
///
/// `Legacy` delegates to the DashMap-backed `StateStore` (N=1 path).
/// `Sharded` delegates to the per-shard `Shard`. In the default (fjall) build
/// the Sharded arm round-trips through `postcard` + `fjall::PartitionHandle`;
/// in the dev-mode `state-inmem` build it uses the legacy AHashMap path.
pub enum StoreView<'a> {
    /// N=1 legacy path — DashMap-backed state store.
    Legacy(&'a crate::state::store::StateStore),
    /// N>1 per-shard path.
    Sharded(&'a mut Shard),
}

impl<'a> StoreView<'a> {
    /// Get or create an entity for the given key, then run `f` with mutable
    /// access to the `EntityState`. Closure-based to avoid returning a guard
    /// whose lifetime differs between the two arms.
    pub fn with_entity_mut<F, R>(&mut self, key: &str, f: F) -> R
    where
        F: FnOnce(&mut crate::state::store::EntityState) -> R,
    {
        match self {
            StoreView::Legacy(store) => {
                let mut guard = store.get_or_create_entity(key);
                f(&mut *guard)
            }
            #[cfg(not(feature = "state-inmem"))]
            StoreView::Sharded(shard) => {
                // Read-modify-write on the fjall partition. Missing key =>
                // default entity. Corrupt bytes (postcard deserialize Err)
                // => treat as missing + overwrite (T-53-03-01 mitigation).
                let mut entity = shard
                    .state
                    .get(key.as_bytes())
                    .ok()
                    .flatten()
                    .and_then(|bytes| entity_from_bytes(&bytes))
                    .unwrap_or_default();
                let r = f(&mut entity);
                let bytes = entity_to_bytes(&entity);
                let byte_count = bytes.len() as u64;
                shard
                    .state
                    .insert(key.as_bytes(), bytes)
                    .expect("fjall partition insert");
                // Phase 53-05 (W-4 revision): accumulate write-bytes in the
                // shard's per-thread counter. The shard event loop reads
                // this via `take_write_bytes()` at the next gauge-sample
                // tick and emits `beava_fjall_write_bytes_total{shard=N}`.
                shard.write_bytes_since_sample = shard
                    .write_bytes_since_sample
                    .saturating_add(byte_count);
                r
            }
            #[cfg(feature = "state-inmem")]
            StoreView::Sharded(shard) => {
                let entity = shard.state.entry(key.to_string()).or_default();
                f(entity)
            }
        }
    }

    /// Read-only entity lookup. Returns `None` if the key is absent.
    pub fn get_entity_ref<F, R>(&self, key: &str, f: F) -> Option<R>
    where
        F: FnOnce(&crate::state::store::EntityState) -> R,
    {
        match self {
            StoreView::Legacy(store) => store.get_entity(key).map(|guard| f(&*guard)),
            #[cfg(not(feature = "state-inmem"))]
            StoreView::Sharded(shard) => shard
                .state
                .get(key.as_bytes())
                .ok()
                .flatten()
                .and_then(|bytes| entity_from_bytes(&bytes))
                .map(|entity| f(&entity)),
            #[cfg(feature = "state-inmem")]
            StoreView::Sharded(shard) => shard.state.get(key).map(|entity| f(entity)),
        }
    }
}

// ---------------------------------------------------------------------------
// W-6 revision: `read_entity_from_shard` — read-only helper that takes `&Shard`
// (not `&mut StoreView`). Plan 03B's `src/shard/thread.rs::get_table_row_on_shard`
// and friends use this for read paths so they don't need to widen StoreView or
// borrow mutably where a shared reference suffices. The helper does NOT
// write back, in contrast to `StoreView::Sharded::with_entity_mut` which
// always re-serializes.
// ---------------------------------------------------------------------------

/// Read-only lookup against a Shard. Returns `None` if the key is absent or
/// the stored bytes fail to deserialize (treated as missing — Plan 03's
/// `T-53-03-01` corrupt-row mitigation).
#[cfg(not(feature = "state-inmem"))]
pub fn read_entity_from_shard<F, R>(shard: &Shard, key: &str, f: F) -> Option<R>
where
    F: FnOnce(&EntityState) -> R,
{
    let bytes = shard.state.get(key.as_bytes()).ok().flatten()?;
    let entity = entity_from_bytes(&bytes)?;
    Some(f(&entity))
}

/// Read-only lookup against a Shard (state-inmem build — plain AHashMap).
#[cfg(feature = "state-inmem")]
pub fn read_entity_from_shard<F, R>(shard: &Shard, key: &str, f: F) -> Option<R>
where
    F: FnOnce(&EntityState) -> R,
{
    shard.state.get(key).map(f)
}

// ---------------------------------------------------------------------------
// Phase 53-03 — Plan 03 tests (Test 4: approximate_len; Test 5: state-inmem)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn shard_state_approximate_len_returns_usize_not_result() {
        // Pitfall 4: metrics must use `approximate_len()` (O(1), usize) instead
        // of `len()` (expensive Result<usize>). This test asserts the cheap API
        // exists and returns a plain usize — Plan 03B wires it into the
        // per-shard event-loop gauges.
        use crate::shard::fjall_backend::{
            fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
        };
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
        std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let cfg = fjall_config_from_env(1);
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");

        let shard = super::Shard::with_partition(partition);
        for i in 0..10 {
            shard
                .state
                .insert(format!("k{}", i).as_bytes(), b"v".as_slice())
                .expect("insert");
        }
        let approx = shard.state.approximate_len();
        // `approximate_len()` returns `usize`, not `Result<usize>`; this line
        // will fail to compile if the backing type is `AHashMap` (which has no
        // such method) — RED signal for Task 1.
        let _check: usize = approx;
        assert!(approx <= 10, "approximate_len returns usize <= insert count");
        std::env::remove_var("BEAVA_FJALL_FSYNC_DISABLE");
        std::env::remove_var("BEAVA_FJALL_CACHE_MB");
    }

    #[cfg(feature = "state-inmem")]
    #[test]
    fn inmem_build_compiles_and_uses_ahashmap() {
        // D-03: when compiled with `--features state-inmem`, Shard.state remains
        // the legacy AHashMap path. This test exists to guarantee the dev-mode
        // fallback still compiles + behaves as before.
        let s = super::Shard::new();
        assert_eq!(s.state.len(), 0);
    }
}
