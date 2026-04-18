//! Per-shard state module (v1.2 TPC Wave 1 — TPC-PERF-01).
//!
//! `Shard` is the sole data-path unit at N=1. Each Shard owns:
//! - `state: AHashMap<EntityKey, EntityState>` — hot-path reads/writes, zero shared lock
//! - `dirty_set: HashSet<EntityKey>` — plain; single writer (shard thread), no arc-swap
//! - `watermark: WatermarkState` — per-shard; replaces WatermarkTracker (Plan 49-03)
//! - `event_log: Option<EventLog>` — points at data/logs/{stream}.bin in Wave 1 (D-03)

pub mod store;
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
