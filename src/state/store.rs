//! Co-located state types: `EntityState`, `StaticFeature`, `TableRow`, `StreamEntityState`,
//! plus the `check_shard_count_guard` / `read_beava_shards` boot helpers.
//!
//! Phase 54-04 Pass A6b: the `StateStore` struct (DashMap-backed global entity map),
//! its `Default`/`Debug`/`impl StateStore` blocks, its `to_concurrent`/`from_concurrent`
//! converters, and its in-file unit tests were deleted. Production callers migrated to
//! shard-owned state during Waves 1/2/3 + Pass A1-A6a; Pass B closed the last test-only
//! paths that still took `&StateStore`.
//!
//! Phase 54-04 closeout (2026-04-19): the `StreamStore` DashMap-backed per-stream
//! entity map was deleted — it was the last in-tree user of `dashmap::DashMap`.
//! The `dashmap` + `arc-swap` Cargo deps are removed; the `verify-no-dashmap.sh`
//! grep gate is now GREEN.
//!
//! This module now exists only to host the *data shapes* that the shard + snapshot
//! + pipeline paths still reference (`EntityState`, `TableRow`, `SerializableTableRow`,
//! `StaticFeature`, `StreamEntityState`). Pass C may relocate these to
//! `src/state/entity.rs` and retire this filename; the name is preserved here purely to
//! avoid an import-churn commit spanning 8 files.

use crate::state::snapshot::OperatorState;
use crate::types::FeatureValue;
use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

// ============================================================
// Phase 52-01: Shard-count boot guard (TPC-CORR-02)
// ============================================================

/// Hard-fail boot guard: compare the shard_count stored in a loaded snapshot
/// against the current `BEAVA_SHARDS` environment variable value.
///
/// This is a pure function (not a method) so tests can call it without
/// constructing a store or loading a snapshot from disk.
///
/// Returns `Ok(())` when the counts match or `Err(msg)` with the exact
/// TPC-CORR-02 error string when they differ. The caller (boot path) must
/// propagate the error to `main` and exit the process — no silent empty boot.
///
/// Env parsing: non-numeric `BEAVA_SHARDS` values default to 1 (T-52-01-03:
/// malformed env var → no panic, treat as N=1).
///
/// # Arguments
/// - `snapshot_shard_count` — the `shard_count` field from the loaded snapshot
///   (1 for v7-promoted snapshots, explicit for v8 snapshots).
/// - `beava_shards` — the current operator-configured shard count, already
///   parsed by the caller (use `read_beava_shards()` in the boot path).
pub fn check_shard_count_guard(snapshot_shard_count: u16, beava_shards: u16) -> Result<(), String> {
    if snapshot_shard_count != beava_shards {
        return Err(format!(
            "snapshot shard_count={} but BEAVA_SHARDS={} \u{2014} run 'tally reshard --from {} --to {}' then restart",
            snapshot_shard_count, beava_shards, snapshot_shard_count, beava_shards
        ));
    }
    Ok(())
}

/// Read the current `BEAVA_SHARDS` env var value as a `u16`.
///
/// Defaults to 1 if the variable is absent or contains a non-numeric value.
/// (T-52-01-03: internal env var; malformed → default to 1, no panic.)
pub fn read_beava_shards() -> u16 {
    std::env::var("BEAVA_SHARDS")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(1)
}

/// A directly-written feature value (from SET/MSET commands).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticFeature {
    pub value: FeatureValue,
    pub updated_at: SystemTime,
}

/// Phase 24: Tombstone grace window for table rows. Tombstoned rows remain in
/// the `table_rows` map for this duration so that out-of-order late events and
/// downstream cascade consumers can still observe the tombstone. After the
/// grace window, `gc_tombstones` removes them. Locked to 7 days per
/// `@tl.table(tombstone_grace="7d")` default (v0-restructure-spec §3.1).
pub const TOMBSTONE_GRACE: Duration = Duration::from_secs(7 * 86400);

/// Phase 24: Lifecycle state for a table row. A row is either `Live` (carrying
/// its fields) or `Tombstoned` with the timestamp at which it was removed.
/// Tombstoned rows are retained until `TOMBSTONE_GRACE` has elapsed so late
/// events and cascade consumers can observe the deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TableRowState {
    /// Row is live. Its fields live on `TableRow.fields`.
    Live,
    /// Row was deleted at `since`. `TableRow.fields` is typically empty but
    /// callers must not rely on that — filter by this variant instead.
    Tombstoned { since: SystemTime },
}

/// Phase 24: First-class row in a Table source. Each entity key maps to zero
/// or more named tables via `EntityState.table_rows`; each `TableRow` owns
/// its own field map and lifecycle state.
///
/// The row is the unit of identity referenced by `(table_name, key)` — the
/// `OP_PUSH_TABLE` / `OP_DELETE_TABLE` opcodes and the Table↔Table join
/// cascade address rows through this type.
///
/// **Tombstone contract:** `get_table_row` returns `Some(&TableRow)` for
/// both `Live` and `Tombstoned` rows. Consumers that want only live data
/// must match on `state`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRow {
    /// Row fields keyed by column name. For `Tombstoned` rows this is
    /// typically empty but the type does not enforce that.
    pub fields: AHashMap<String, FeatureValue>,
    /// Live or Tombstoned + since-timestamp.
    pub state: TableRowState,
    /// Wall-clock-ish timestamp of the last mutation (upsert or tombstone).
    pub updated_at: SystemTime,
}

/// Phase 24: Serialization shape of a `TableRow`. `AHashMap` does not derive
/// `Serialize`/`Deserialize` so the field map flattens to a `Vec<(k, v)>` on
/// disk — mirrors how `SerializableEntityState.static_features` handles the
/// same constraint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializableTableRow {
    pub fields: Vec<(String, FeatureValue)>,
    pub state: TableRowState,
    pub updated_at: SystemTime,
}

impl From<&TableRow> for SerializableTableRow {
    fn from(row: &TableRow) -> Self {
        SerializableTableRow {
            fields: row
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            state: row.state.clone(),
            updated_at: row.updated_at,
        }
    }
}

impl From<SerializableTableRow> for TableRow {
    fn from(s: SerializableTableRow) -> Self {
        TableRow {
            fields: s.fields.into_iter().collect(),
            state: s.state,
            updated_at: s.updated_at,
        }
    }
}

/// Per-stream state within an entity. Isolates operators and last_event_at
/// per stream for independent TTL management (OPS-02).
#[derive(Debug, Clone, Default)]
pub struct StreamEntityState {
    /// Operators belonging to this stream only.
    pub operators: Vec<(String, OperatorState)>,
    /// Last event timestamp for this stream (per-stream TTL).
    pub last_event_at: Option<SystemTime>,
}

/// Per-entity state. Holds live features grouped by stream name (from streaming
/// operators) and static features (from direct SET/MSET writes).
///
/// Phase 24: also holds `table_rows` — first-class Table row storage keyed
/// by table name. This map is independent of `static_features`; upserting a
/// table row named "X" does not populate `static_features["X"]`, and vice
/// versa. The legacy `static_features` path is preserved for backward
/// compatibility with existing SET/MSET callers.
///
/// `dirty_gen` is a per-entity generation watermark used by the legacy
/// (now-removed) DashMap-backed store to short-circuit hot-key dirty-set
/// writes. The default (fjall) build does not read this field; it is kept
/// for binary compatibility with the `state-inmem` feature build and for
/// the serialization shape.
#[derive(Debug)]
pub struct EntityState {
    /// Live features grouped by stream name. Each stream has its own operators
    /// and last_event_at for independent TTL management.
    pub streams: AHashMap<String, StreamEntityState>,
    /// Features from direct writes (SET/MSET). Bypass pipeline engine.
    pub static_features: AHashMap<String, StaticFeature>,
    /// Phase 24: Table rows keyed by table name. Each entry is a first-class
    /// row (Live or Tombstoned) addressed by `(entity_key, table_name)`. See
    /// `TableRow` for the lifecycle contract.
    pub table_rows: AHashMap<String, TableRow>,
    /// Per-entity generation watermark; see struct-level doc. Preserved for
    /// binary compatibility with the legacy DashMap store semantics.
    pub dirty_gen: std::sync::atomic::AtomicU64,
}

impl Clone for EntityState {
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            static_features: self.static_features.clone(),
            table_rows: self.table_rows.clone(),
            dirty_gen: std::sync::atomic::AtomicU64::new(
                self.dirty_gen.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl Default for EntityState {
    fn default() -> Self {
        Self {
            streams: AHashMap::new(),
            static_features: AHashMap::new(),
            table_rows: AHashMap::new(),
            dirty_gen: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl EntityState {
    /// Create a new empty EntityState.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create a StreamEntityState for the given stream name.
    /// Returns a mutable reference to the stream's state.
    pub fn get_or_create_stream(&mut self, stream_name: &str) -> &mut StreamEntityState {
        self.streams.entry(stream_name.to_string()).or_default()
    }

    /// Returns true when this entity has no streams, no static features, and
    /// no table rows (Live or Tombstoned).
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.static_features.is_empty() && self.table_rows.is_empty()
    }
}

// Phase 54-04 2026-04-19: `StreamStore` (DashMap-backed per-stream entity
// map) deleted. It was the last in-tree user of `dashmap::DashMap`. Entity
// state lives in per-shard fjall partitions via `shard::Shard::state` on
// the default build; the `state-inmem` flag-gated `ShardedStateStoreV1` in
// `src/shard/store.rs` uses `ahash::AHashMap`, not DashMap.
