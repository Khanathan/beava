//! Per-shard state module (v1.2 TPC Wave 1 — TPC-PERF-01).
//!
//! `Shard` is the sole data-path unit at N=1. Each Shard owns:
//! - `state: AHashMap<EntityKey, EntityState>` — hot-path reads/writes, zero shared lock
//! - `dirty_set: HashSet<EntityKey>` — plain; single writer (shard thread), no arc-swap
//! - `watermark: WatermarkState` — per-shard; replaces WatermarkTracker (Plan 49-03)
//! - `event_log: Option<EventLog>` — points at data/logs/{stream}.bin in Wave 1 (D-03)

/// fjall 2.11 keyspace + partition lifecycle (Phase 53 Plan 02, D-01
/// one-keyspace layout). New surface: Plan 03 will wire `Shard.state` in.
pub mod fjall_backend;
pub mod global_watermark;
/// Per-shard Prometheus metrics (Phase 50-02, D-07).
pub mod metrics;
pub mod store;
/// Shard thread lifecycle: spawn, ready-barrier, pinning, quarantine (Phase 50-03).
pub mod thread;
pub mod traits;
pub mod watermark;

use ahash::AHashMap;
use std::collections::HashSet;

use crate::state::event_log::EventLog;
use crate::state::store::EntityState;
use watermark::WatermarkState;

/// Entity key type alias (mirrors crate::types::EntityKey = String).
pub type EntityKey = String;

/// Per-shard state container. Single writer — no lock needed.
///
/// Wave 1: N=1, so exactly one Shard exists. Event log path is
/// `data/logs/{stream}.bin` (existing layout, D-03 — Wave 1 keeps current path).
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
    /// Create a new empty Shard with no event log.
    pub fn new() -> Self {
        Shard {
            state: AHashMap::new(),
            dirty_set: HashSet::new(),
            event_log: None,
            watermark: WatermarkState::new(),
        }
    }

    /// Create a Shard with an attached event log.
    pub fn with_event_log(event_log: EventLog) -> Self {
        Shard {
            state: AHashMap::new(),
            dirty_set: HashSet::new(),
            event_log: Some(event_log),
            watermark: WatermarkState::new(),
        }
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Phase 50.5-01: StoreView enum — cascade-shape shim (Wave 0 chose enum <5 sites)
// ---------------------------------------------------------------------------

/// Storage view abstraction for `push_with_cascade_internal`.
///
/// Chosen shape: enum (CASCADE-SHAPE.md: 4 call sites, 2 distinct methods → enum).
///
/// `Legacy` delegates to the DashMap-backed `StateStore` (N=1 path).
/// `Sharded` delegates to the `AHashMap`-backed per-shard `Shard` (N>1 path).
///
/// Bodies are `unimplemented!()` stubs; Task 3 implements each arm.
pub enum StoreView<'a> {
    /// N=1 legacy path — DashMap-backed state store.
    Legacy(&'a crate::state::store::StateStore),
    /// N>1 per-shard path — single-writer AHashMap partition.
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
            StoreView::Legacy(store) => {
                store.get_entity(key).map(|guard| f(&*guard))
            }
            StoreView::Sharded(shard) => {
                shard.state.get(key).map(|entity| f(entity))
            }
        }
    }
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
