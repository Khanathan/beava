//! In-memory state store: EntityState + StateStore + StreamStore.
//!
//! EntityState stores per-key features from streaming operators (live) and
//! direct writes (static). StateStore maps entity keys to EntityState using
//! AHashMap (not std HashMap) per locked decision.
//!
//! Phase 14: StreamStore provides per-stream `DashMap<EntityKey, StreamEntityState>`
//! for entity-level concurrency. ConcurrentAppState (in tcp.rs) uses StreamStore
//! per registered stream so events for different streams and different entity keys
//! proceed concurrently.
//!
//! v1.1: EntityState groups live operators by stream name using
//! AHashMap<String, StreamEntityState>. Each stream has its own operators
//! and last_event_at for independent TTL management (OPS-02).

use crate::state::snapshot::{
    OperatorState, SerializableEntityState, SerializableStreamEntityState,
};
use crate::types::{EntityKey, FeatureMap, FeatureValue};
use ahash::{AHashMap, AHashSet};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

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
/// events and cascade consumers can observe the deletion; see
/// `StateStore::gc_tombstones`.
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
/// upcoming `OP_PUSH_TABLE` / `OP_DELETE_TABLE` opcodes (plan 02) and the
/// Table↔Table join cascade rework (plan 03) both address rows through this
/// type rather than the legacy `static_features` map.
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
#[derive(Debug, Clone)]
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
}

impl Default for EntityState {
    fn default() -> Self {
        Self {
            streams: AHashMap::new(),
            static_features: AHashMap::new(),
            table_rows: AHashMap::new(),
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

/// The top-level state store. Maps entity keys to their state.
///
/// v1.3: `entities` is a `DashMap` for per-key concurrency — two events
/// targeting different entity keys never contend on the same lock.
/// `dirty_keys` and `deleted_keys` are wrapped in fine-grained `PLMutex`
/// so snapshot tracking does not require a global store lock.
///
/// v1.1 Phase 9: tracks dirty and deleted keys since the last snapshot clear
/// for incremental snapshot serialization.
pub struct StateStore {
    entities: DashMap<EntityKey, EntityState>,
    /// Keys modified since last snapshot clear (mutation-touched).
    dirty_keys: parking_lot::Mutex<AHashSet<EntityKey>>,
    /// Keys evicted/deleted since last snapshot clear. Populated by eviction
    /// and explicit deletes; consumed by delta snapshot serialization.
    deleted_keys: parking_lot::Mutex<AHashSet<EntityKey>>,
}

// DashMap does not implement Debug, so implement manually.
impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStore")
            .field("entity_count", &self.entities.len())
            .field("dirty_count", &self.dirty_keys.lock().len())
            .field("deleted_count", &self.deleted_keys.lock().len())
            .finish()
    }
}

/// Phase 43+: DashMap shard count tuned for 8-worker-thread server deployments.
/// Default DashMap picks `num_cpus * 4` rounded to power of 2 (=256 on a 48-CPU
/// host), which is more shards than necessary and bloats memory + cache footprint
/// when only 8 worker threads are active. 16 shards = 2 shards per worker → low
/// contention probability under 8-way concurrency, better cache locality.
pub const STATE_SHARD_AMOUNT: usize = 16;

impl Default for StateStore {
    fn default() -> Self {
        Self {
            entities: DashMap::with_shard_amount(STATE_SHARD_AMOUNT),
            dirty_keys: parking_lot::Mutex::new(AHashSet::new()),
            deleted_keys: parking_lot::Mutex::new(AHashSet::new()),
        }
    }
}

impl StateStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create an EntityState for the given key.
    /// Returns a DashMap RefMut guard that derefs to `&mut EntityState`.
    /// The guard must be dropped before accessing a different key in the
    /// same DashMap to avoid potential deadlock on the same shard.
    pub fn get_or_create_entity(
        &self,
        key: &str,
    ) -> dashmap::mapref::one::RefMut<'_, String, EntityState> {
        self.entities.entry(key.to_string()).or_default()
    }

    /// Read-only access to an entity's state. Returns None if key not found.
    /// Returns a DashMap Ref guard that derefs to `&EntityState`.
    pub fn get_entity(
        &self,
        key: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, String, EntityState>> {
        self.entities.get(key)
    }

    /// Mutable access to an entity's state. Returns None if key not found.
    /// Returns a DashMap RefMut guard that derefs to `&mut EntityState`.
    pub fn get_entity_mut(
        &self,
        key: &str,
    ) -> Option<dashmap::mapref::one::RefMut<'_, String, EntityState>> {
        self.entities.get_mut(key)
    }

    /// Write a static feature for an entity. Creates the entity if absent.
    /// Accepts an explicit `now` timestamp for determinism and testability (WR-05).
    pub fn set_static(&self, key: &str, feature_name: &str, value: FeatureValue, now: SystemTime) {
        let mut entity = self.get_or_create_entity(key);
        entity.static_features.insert(
            feature_name.to_string(),
            StaticFeature {
                value,
                updated_at: now,
            },
        );
    }

    /// Phase 23-03: Delete all static features for an entity key. Alias for
    /// `tombstone_static` used by test harnesses and Rust callers that
    /// model a "delete this row" primitive.
    pub fn delete_entity(&self, key: &str) -> bool {
        self.tombstone_static(key)
    }

    /// Phase 23-03: Tombstone a Table row — remove all static_features for
    /// the key. Returns `true` if the key existed and had static features
    /// before the call. Used by Table↔Table join cascade and is the Rust-
    /// side primitive that higher layers (TCP SET with empty body, a future
    /// OP_DELETE, or engine-driven tombstone propagation) route through.
    ///
    /// NOTE: This does NOT delete live operator state under the key — only
    /// the Table's directly-written rows. Live aggregations continue to
    /// exist because they are fed by stream events, not direct writes.
    /// For a full entity removal see `remove_entity_complete`.
    pub fn tombstone_static(&self, key: &str) -> bool {
        let existed = self
            .entities
            .get_mut(key)
            .map(|mut e| {
                let had_rows = !e.static_features.is_empty();
                e.static_features.clear();
                had_rows
            })
            .unwrap_or(false);
        if existed {
            self.mark_dirty(key);
        }
        existed
    }

    /// Phase 24: Upsert a Table row for `(key, table_name)`. Replaces any
    /// prior row (Live or Tombstoned) under the same identity with a fresh
    /// `Live { fields }` row. Marks the key dirty. Accepts an explicit `now`
    /// timestamp for determinism (same convention as `set_static`).
    pub fn upsert_table_row(
        &self,
        key: &str,
        table_name: &str,
        fields: AHashMap<String, FeatureValue>,
        now: SystemTime,
    ) {
        {
            let mut entity = self.get_or_create_entity(key);
            entity.table_rows.insert(
                table_name.to_string(),
                TableRow {
                    fields,
                    state: TableRowState::Live,
                    updated_at: now,
                },
            );
        }
        self.mark_dirty(key);
    }

    /// Phase 24: Tombstone a Table row for `(key, table_name)`. Flips an
    /// existing row to `Tombstoned { since: now }`, or creates a new
    /// tombstone-only row with empty fields if none exists. Returns `true`
    /// if a prior **Live** row existed under this identity — used by callers
    /// that need to distinguish "deleted a real row" from "deleted absent".
    /// Marks the key dirty.
    pub fn tombstone_table_row(&self, key: &str, table_name: &str, now: SystemTime) -> bool {
        let had_live = {
            let mut entity = self.get_or_create_entity(key);
            let prior_live = entity
                .table_rows
                .get(table_name)
                .map(|r| matches!(r.state, TableRowState::Live))
                .unwrap_or(false);
            entity.table_rows.insert(
                table_name.to_string(),
                TableRow {
                    fields: AHashMap::new(),
                    state: TableRowState::Tombstoned { since: now },
                    updated_at: now,
                },
            );
            prior_live
        };
        self.mark_dirty(key);
        had_live
    }

    /// Phase 24: Read a Table row for `(key, table_name)`. Returns `None`
    /// if the entity or the row is absent. Returns `Some(row)` for BOTH
    /// Live and Tombstoned rows — callers who want live-only data must
    /// match on `row.state`. Because `DashMap` returns a `Ref` guard, this
    /// call clones the row to decouple the caller from the shard lock.
    pub fn get_table_row(&self, key: &str, table_name: &str) -> Option<TableRow> {
        let entity = self.entities.get(key)?;
        entity.table_rows.get(table_name).cloned()
    }

    /// Phase 25-01: Per-Table row projection for `OP_GET_MULTI`. Returns
    /// `Some(serde_json::Value::Object)` iff the entity exists AND carries
    /// a `Live` row for `table_name`. Tombstoned rows, absent entities,
    /// and absent table rows all collapse to `None` — callers cannot
    /// distinguish them from the return type (T-25-01-02 information-
    /// disclosure mitigation: tombstones never leak field data through
    /// this path).
    ///
    /// The returned JSON shape matches the flat object a single-table
    /// row view would produce: field name → value (not prefixed with
    /// `TableName.`).
    ///
    /// `_now` is accepted for API symmetry with `collect_merged_features`;
    /// tombstone-grace GC is the sole ageing mechanism for table rows and
    /// runs in `gc_tombstones`, so this read path is time-independent.
    pub fn collect_table_row_view(
        &self,
        key: &str,
        table_name: &str,
        _now: SystemTime,
    ) -> Option<serde_json::Value> {
        let entity = self.entities.get(key)?;
        let row = entity.table_rows.get(table_name)?;
        match row.state {
            TableRowState::Live => {
                let mut map = serde_json::Map::with_capacity(row.fields.len());
                for (name, val) in row.fields.iter() {
                    map.insert(name.clone(), val.to_json_value());
                }
                Some(serde_json::Value::Object(map))
            }
            TableRowState::Tombstoned { .. } => None,
        }
    }

    /// Phase 24: Sweep every entity's `table_rows`, removing Tombstoned
    /// rows whose `since` is older than `TOMBSTONE_GRACE` relative to `now`.
    /// Live rows, static_features, and streams are untouched. Returns the
    /// count of rows removed. Uses `DashMap::iter_mut` so per-shard locking
    /// bounds the critical section (T-24-01-02).
    pub fn gc_tombstones(&self, now: SystemTime) -> usize {
        let mut removed: usize = 0;
        for mut entry in self.entities.iter_mut() {
            let before = entry.value().table_rows.len();
            entry.value_mut().table_rows.retain(|_, row| match row.state {
                TableRowState::Live => true,
                TableRowState::Tombstoned { since } => match now.duration_since(since) {
                    Ok(age) => age <= TOMBSTONE_GRACE,
                    // `now` is before `since` (clock skew) — keep row.
                    Err(_) => true,
                },
            });
            removed += before - entry.value().table_rows.len();
        }
        removed
    }

    /// Collect all feature values for an entity.
    /// Iterates all streams' operators calling read(now) (which advances time
    /// to expire stale buckets), then overlays static_features. Static features
    /// with the same name override live features (direct writes take precedence).
    /// DashMap get_mut provides interior mutability for operator read().
    pub fn get_all_features(&self, key: &str, now: SystemTime) -> FeatureMap {
        let mut entity = match self.entities.get_mut(key) {
            Some(e) => e,
            None => return FeatureMap::default(),
        };

        let mut features = FeatureMap::new();

        // Collect live features from all streams' operators
        for (_stream_name, stream_state) in entity.streams.iter_mut() {
            for (name, op) in stream_state.operators.iter_mut() {
                features.insert(name.clone(), op.read(now));
            }
        }

        // Overlay static features (static takes precedence)
        for (name, sf) in &entity.static_features {
            features.insert(name.clone(), sf.value.clone());
        }

        features
    }

    /// Phase 24-02: Collect a merged feature view for GET — identical to
    /// `get_all_features` but ALSO flattens Live `table_rows` into the result
    /// as `format!("{table_name}.{field_name}")`. Tombstoned rows are filtered
    /// entirely (information-disclosure mitigation T-24-02-03).
    ///
    /// Overlay order (last writer wins on collision):
    ///   1. Stream live operator features (per-stream operators)
    ///   2. Flattened Live Table rows as `TableName.field`
    ///   3. `static_features`
    ///
    /// Collisions between (2) and (3) should not occur in v0 because Table
    /// rows emit prefixed names (`TableName.col`) while `static_features`
    /// use raw names; the overlay rule above is documented so callers can
    /// reason about any future collision.
    pub fn collect_merged_features(&self, key: &str, now: SystemTime) -> FeatureMap {
        let mut entity = match self.entities.get_mut(key) {
            Some(e) => e,
            None => return FeatureMap::default(),
        };

        let mut features = FeatureMap::new();

        // 1. Live stream operator features.
        for (_stream_name, stream_state) in entity.streams.iter_mut() {
            for (name, op) in stream_state.operators.iter_mut() {
                features.insert(name.clone(), op.read(now));
            }
        }

        // 2. Flattened Live table rows. Tombstoned rows are skipped.
        for (table_name, row) in entity.table_rows.iter() {
            if matches!(row.state, TableRowState::Live) {
                for (field_name, value) in row.fields.iter() {
                    features.insert(format!("{}.{}", table_name, field_name), value.clone());
                }
            }
        }

        // 3. Static features (overlay).
        for (name, sf) in &entity.static_features {
            features.insert(name.clone(), sf.value.clone());
        }

        features
    }

    /// Read a single feature value for an entity. Used by cross-key lookups.
    /// Returns Missing if entity or feature not found.
    /// DashMap get_mut provides interior mutability for operator read().
    pub fn get_feature_value(
        &self,
        key: &str,
        feature_name: &str,
        now: SystemTime,
    ) -> FeatureValue {
        let mut entity = match self.entities.get_mut(key) {
            Some(e) => e,
            None => return FeatureValue::Missing,
        };
        // Check live operators across all streams
        for (_stream_name, stream_state) in entity.streams.iter_mut() {
            for (name, op) in stream_state.operators.iter_mut() {
                if name == feature_name {
                    return op.read(now);
                }
            }
        }
        // Check static features
        if let Some(sf) = entity.static_features.get(feature_name) {
            return sf.value.clone();
        }
        FeatureValue::Missing
    }

    /// Number of tracked entities.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    // ======================== Dirty / Deleted Tracking (Phase 9) ========================

    /// Mark an entity key as dirty (mutated since the last snapshot clear).
    /// Idempotent -- repeated calls leave the dirty set unchanged.
    pub fn mark_dirty(&self, key: &str) {
        self.dirty_keys.lock().insert(key.to_string());
    }

    /// Batch-mark entity keys as dirty. Idempotent. O(n) inserts into the
    /// `dirty_keys` HashSet using a single `extend` call. Mirrors `mark_dirty`
    /// semantics — does **not** touch `deleted_keys` (a key already in
    /// `deleted_keys` remains there; this matches the single-key `mark_dirty`
    /// contract which also does not cross-mutate the delete set).
    ///
    /// Used by Phase 12's `handle_push_batch` to amortize the per-event
    /// dirty-mark cost: one call per stream group instead of one per event.
    pub fn mark_dirty_many<I, S>(&self, keys: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.dirty_keys
            .lock()
            .extend(keys.into_iter().map(Into::into));
    }

    /// Mark an entity key as deleted since the last snapshot clear. A deleted
    /// key is automatically removed from the dirty set so it does not appear
    /// in the next delta's `changed_entities` (avoids ambiguity).
    pub fn mark_deleted(&self, key: &str) {
        self.deleted_keys.lock().insert(key.to_string());
        self.dirty_keys.lock().remove(key);
    }

    /// Clear the dirty set. Called after a successful snapshot write.
    pub fn clear_dirty(&self) {
        self.dirty_keys.lock().clear();
    }

    /// Drain the deleted key set into a Vec. Leaves the set empty.
    pub fn take_deleted(&self) -> Vec<String> {
        self.deleted_keys.lock().drain().collect()
    }

    /// Number of keys currently marked dirty.
    pub fn dirty_count(&self) -> usize {
        self.dirty_keys.lock().len()
    }

    /// Read-only view of the dirty key set (clone). Primarily for tests and debug APIs.
    #[cfg(test)]
    pub(crate) fn dirty_keys(&self) -> AHashSet<EntityKey> {
        self.dirty_keys.lock().clone()
    }

    /// Clone only dirty entities for a delta snapshot, applying the same lazy
    /// GC pattern as `clone_for_snapshot_with_gc`. Entities whose key is not
    /// in `dirty_keys` are skipped entirely. If a stream is not present in
    /// `valid_features`, all of its operators are included (defensive, matching
    /// the non-delta path).
    pub fn clone_dirty_for_snapshot_with_gc(
        &self,
        valid_features: &AHashMap<String, Vec<String>>,
    ) -> Vec<(String, SerializableEntityState)> {
        let dirty = self.dirty_keys.lock();
        self.entities
            .iter()
            .filter(|entry| dirty.contains(entry.key().as_str()))
            .map(|entry| {
                let key = entry.key();
                let entity = entry.value();
                let streams: Vec<(String, SerializableStreamEntityState)> = entity
                    .streams
                    .iter()
                    .map(|(stream_name, stream_state)| {
                        let operators = if let Some(valid) = valid_features.get(stream_name) {
                            stream_state
                                .operators
                                .iter()
                                .filter(|(name, _)| valid.contains(name))
                                .cloned()
                                .collect()
                        } else {
                            stream_state.operators.clone()
                        };
                        (
                            stream_name.clone(),
                            SerializableStreamEntityState {
                                operators,
                                last_event_at: stream_state.last_event_at,
                            },
                        )
                    })
                    .collect();
                (
                    key.clone(),
                    SerializableEntityState {
                        streams,
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
                    },
                )
            })
            .collect()
    }

    /// Collect all entity keys into a Vec. DashMap iteration returns
    /// guards, so we collect to owned Strings to avoid lifetime issues.
    pub fn entity_keys(&self) -> Vec<String> {
        self.entities
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Clone full state for snapshot serialization with garbage collection of
    /// removed operators. Filters out operators whose feature name is no longer
    /// in the current stream definition (lazy GC on snapshot).
    /// If a stream_name is not in valid_features (stream was unregistered entirely),
    /// include all operators (defensive).
    pub fn clone_for_snapshot_with_gc(
        &self,
        valid_features: &AHashMap<String, Vec<String>>,
    ) -> Vec<(String, SerializableEntityState)> {
        self.entities
            .iter()
            .map(|entry| {
                let key = entry.key();
                let entity = entry.value();
                let streams: Vec<(String, SerializableStreamEntityState)> = entity
                    .streams
                    .iter()
                    .map(|(stream_name, stream_state)| {
                        let operators = if let Some(valid) = valid_features.get(stream_name) {
                            // Filter to only operators whose name is in the valid set
                            stream_state
                                .operators
                                .iter()
                                .filter(|(name, _)| valid.contains(name))
                                .cloned()
                                .collect()
                        } else {
                            // Stream not in valid_features -- include all (defensive)
                            stream_state.operators.clone()
                        };
                        (
                            stream_name.clone(),
                            SerializableStreamEntityState {
                                operators,
                                last_event_at: stream_state.last_event_at,
                            },
                        )
                    })
                    .collect();
                (
                    key.clone(),
                    SerializableEntityState {
                        streams,
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
                    },
                )
            })
            .collect()
    }

    /// Clone full state for snapshot serialization (v4 format).
    /// DashMap is not directly serializable by postcard -- convert to Vec<(K, V)>.
    pub fn clone_for_snapshot(&self) -> Vec<(String, SerializableEntityState)> {
        self.entities
            .iter()
            .map(|entry| {
                let key = entry.key();
                let entity = entry.value();
                let streams: Vec<(String, SerializableStreamEntityState)> = entity
                    .streams
                    .iter()
                    .map(|(stream_name, stream_state)| {
                        (
                            stream_name.clone(),
                            SerializableStreamEntityState {
                                operators: stream_state.operators.clone(),
                                last_event_at: stream_state.last_event_at,
                            },
                        )
                    })
                    .collect();
                (
                    key.clone(),
                    SerializableEntityState {
                        streams,
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
                    },
                )
            })
            .collect()
    }

    /// Bulk-insert aggregated entity state from a remote snapshot.
    ///
    /// Used by the replica-mode server boot (Phase 36,
    /// `src/server/replica_client.rs`) after fetching a `BaseSnapshotState`
    /// over the wire. Does **NOT** run events through `apply_event` — the
    /// input is aggregated state already; replaying would double-count.
    /// (Originally introduced in Phase 28-04 for the Option K embedded
    /// `FrozenClient`, which was mothballed in Phase 38-01.)
    ///
    /// Unlike `restore_from_snapshot`, this does NOT clear existing entities.
    /// Overlapping keys are overwritten (last write wins), matching the
    /// single-shot historical-clone use case where the store starts empty.
    ///
    /// No side effects: no dirty-tracking, no listener notify, no metric bump.
    pub fn bulk_load(&self, entities: Vec<(String, SerializableEntityState)>) {
        for (key, state) in entities {
            let mut streams = AHashMap::new();
            for (stream_name, stream_state) in state.streams {
                streams.insert(
                    stream_name,
                    StreamEntityState {
                        operators: stream_state.operators,
                        last_event_at: stream_state.last_event_at,
                    },
                );
            }
            let entity = EntityState {
                streams,
                static_features: state.static_features.into_iter().collect(),
                table_rows: state
                    .table_rows
                    .into_iter()
                    .map(|(k, v)| (k, TableRow::from(v)))
                    .collect(),
            };
            self.entities.insert(key, entity);
        }
    }

    /// Restore state from a snapshot (v4 format). Clears existing state first.
    pub fn restore_from_snapshot(&self, entities: Vec<(String, SerializableEntityState)>) {
        self.entities.clear();
        for (key, state) in entities {
            let mut streams = AHashMap::new();
            for (stream_name, stream_state) in state.streams {
                streams.insert(
                    stream_name,
                    StreamEntityState {
                        operators: stream_state.operators,
                        last_event_at: stream_state.last_event_at,
                    },
                );
            }
            let entity = EntityState {
                streams,
                static_features: state.static_features.into_iter().collect(),
                table_rows: state
                    .table_rows
                    .into_iter()
                    .map(|(k, v)| (k, TableRow::from(v)))
                    .collect(),
            };
            self.entities.insert(key, entity);
        }
    }

    /// Apply a delta snapshot on top of the current state (Phase 9, OPS-04).
    ///
    /// For each entity in `deleted_keys`: remove from the store entirely.
    /// For each entity in `changed_entities`: replace the existing entity
    /// (same conversion as `restore_from_snapshot`). The dirty/deleted
    /// tracking sets are NOT modified -- applying a delta during recovery
    /// should not produce new dirty tracking.
    pub fn apply_delta(
        &self,
        changed_entities: Vec<(String, SerializableEntityState)>,
        deleted_keys: Vec<String>,
    ) {
        // Deletes first, so that an entity which is deleted AND re-inserted
        // in the same delta ends up inserted (matches common delta semantics).
        for key in deleted_keys {
            self.entities.remove(&key);
        }
        for (key, state) in changed_entities {
            let mut streams = AHashMap::new();
            for (stream_name, stream_state) in state.streams {
                streams.insert(
                    stream_name,
                    StreamEntityState {
                        operators: stream_state.operators,
                        last_event_at: stream_state.last_event_at,
                    },
                );
            }
            let entity = EntityState {
                streams,
                static_features: state.static_features.into_iter().collect(),
                table_rows: state
                    .table_rows
                    .into_iter()
                    .map(|(k, v)| (k, TableRow::from(v)))
                    .collect(),
            };
            self.entities.insert(key, entity);
        }
    }

    /// Remove entities whose last_event_at (across all streams) is strictly
    /// older than `ttl` from `now`. For per-stream grouping, we use the most
    /// recent last_event_at across all streams. Entities with no streams that
    /// have a last_event_at are not evicted (never received an event).
    /// Entities exactly at TTL age are kept (evicted only after TTL has fully elapsed).
    /// Returns the count of evicted entities.
    pub fn remove_expired_entities(&self, now: SystemTime, ttl: std::time::Duration) -> usize {
        let before = self.entities.len();
        self.entities.retain(|_key, entity| {
            // Find the most recent last_event_at across all streams
            let most_recent = entity
                .streams
                .values()
                .filter_map(|s| s.last_event_at)
                .max();
            match most_recent {
                None => true, // No streams with events -- don't evict
                Some(last) => {
                    now.duration_since(last)
                        .unwrap_or(std::time::Duration::ZERO)
                        <= ttl
                }
            }
        });
        before - self.entities.len()
    }

    /// Remove entities where `is_empty()` returns true.
    ///
    /// **Contract (Phase 9 IN-04):** Callers that remove empty entities must
    /// first call `mark_deleted` for each entity they expect to end up
    /// removed. Failure to do so produces an incremental delta that misses
    /// the deletion and lets recovery resurrect the entity from the base
    /// snapshot. The eviction path obeys this contract; any new caller
    /// should do the same or accept the resurrection risk.
    pub fn remove_empty_entities(&self) {
        self.entities.retain(|_key, entity| !entity.is_empty());
    }

    /// Phase 9 WR-05: in-place GC pass that drops operators whose stream is
    /// no longer registered or whose feature name has been removed from its
    /// stream definition. Intended to run once at startup, after base+deltas
    /// have been loaded and all pipelines re-registered, to clean up zombie
    /// operators that survived because no event arrived for the affected
    /// entity between an unregister and the next base write.
    ///
    /// Streams not present in `valid_features` are removed entirely (defensive).
    pub fn gc_invalid_operators(&self, valid_features: &AHashMap<String, Vec<String>>) {
        for mut entry in self.entities.iter_mut() {
            entry
                .value_mut()
                .streams
                .retain(|stream_name, stream_state| {
                    if let Some(valid) = valid_features.get(stream_name) {
                        stream_state
                            .operators
                            .retain(|(name, _)| valid.contains(name));
                        !stream_state.operators.is_empty()
                    } else {
                        // Stream not registered anymore -- drop it wholesale.
                        false
                    }
                });
        }
    }
}

// ============================================================
// Phase 14: StreamStore — per-stream DashMap for entity-level concurrency
// ============================================================

/// Per-stream entity storage backed by `DashMap` (D-02, D-03).
///
/// Each registered stream gets its own `StreamStore`. Events targeting
/// different entity keys within the same stream proceed concurrently
/// via DashMap's internal sharded locking. Events targeting different
/// streams use entirely different `StreamStore` instances.
pub struct StreamStore {
    /// Entity-level concurrent map: key -> stream entity state.
    pub entities: DashMap<String, StreamEntityState>,
    /// Keys modified since last snapshot clear. Protected by parking_lot::Mutex.
    pub dirty_keys: parking_lot::Mutex<AHashSet<String>>,
    /// Keys deleted since last snapshot clear. Protected by parking_lot::Mutex.
    pub deleted_keys: parking_lot::Mutex<AHashSet<String>>,
}

impl StreamStore {
    /// Create an empty StreamStore.
    pub fn new() -> Self {
        Self {
            entities: DashMap::with_shard_amount(STATE_SHARD_AMOUNT),
            dirty_keys: parking_lot::Mutex::new(AHashSet::new()),
            deleted_keys: parking_lot::Mutex::new(AHashSet::new()),
        }
    }

    /// Number of entity keys in this stream store.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }
}

impl Default for StreamStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore {
    /// Convert flat entity state into per-stream `StreamStore` DashMaps and a
    /// static feature `DashMap`. Used during startup to populate
    /// `ConcurrentAppState` from a recovered snapshot.
    ///
    /// Each entity's `streams` map is distributed: for each `(stream_name,
    /// StreamEntityState)`, the entry is inserted into the corresponding
    /// `StreamStore`'s DashMap keyed by entity key. Static features are
    /// collected into a separate DashMap.
    pub fn to_concurrent(
        &self,
    ) -> (
        DashMap<String, StreamStore>,
        DashMap<String, AHashMap<String, StaticFeature>>,
    ) {
        let stream_stores: DashMap<String, StreamStore> = DashMap::new();
        let static_store: DashMap<String, AHashMap<String, StaticFeature>> = DashMap::new();

        for entry in self.entities.iter() {
            let entity_key = entry.key();
            let entity_state = entry.value();
            // Distribute stream entries into per-stream StreamStores
            for (stream_name, stream_entity_state) in &entity_state.streams {
                let store = stream_stores.entry(stream_name.clone()).or_default();
                store
                    .entities
                    .insert(entity_key.clone(), stream_entity_state.clone());
            }

            // Collect static features
            if !entity_state.static_features.is_empty() {
                static_store.insert(entity_key.clone(), entity_state.static_features.clone());
            }
        }

        // Distribute dirty/deleted keys to per-stream stores
        let dirty = self.dirty_keys.lock();
        for key in dirty.iter() {
            for entry in stream_stores.iter() {
                if entry.value().entities.contains_key(key) {
                    entry.value().dirty_keys.lock().insert(key.clone());
                }
            }
        }

        (stream_stores, static_store)
    }

    /// Reconstitute a `StateStore` from per-stream DashMaps and a static
    /// feature DashMap. Used for snapshot serialization (temporary approach;
    /// Plan 02 will optimize to iterate DashMaps directly).
    pub fn from_concurrent(
        stream_stores: &DashMap<String, StreamStore>,
        static_store: &DashMap<String, AHashMap<String, StaticFeature>>,
    ) -> Self {
        let entities: DashMap<EntityKey, EntityState> = DashMap::new();
        let dirty_keys = AHashSet::new();
        let deleted_keys = AHashSet::new();

        // Collect stream entity state from each StreamStore
        for entry in stream_stores.iter() {
            let stream_name = entry.key();
            let store = entry.value();

            // Collect dirty/deleted from this stream
            // (dirty_keys/deleted_keys collected into local AHashSets, then wrapped)

            // Distribute entities
            for entity_entry in store.entities.iter() {
                let entity_key = entity_entry.key();
                let stream_entity_state = entity_entry.value();

                let mut entity = entities.entry(entity_key.clone()).or_default();
                entity
                    .streams
                    .insert(stream_name.clone(), stream_entity_state.clone());
            }
        }

        // Collect dirty/deleted in a second pass (avoid holding DashMap guards simultaneously)
        let mut dirty_keys = dirty_keys;
        let mut deleted_keys = deleted_keys;
        for entry in stream_stores.iter() {
            let store = entry.value();
            for k in store.dirty_keys.lock().iter() {
                dirty_keys.insert(k.clone());
            }
            for k in store.deleted_keys.lock().iter() {
                deleted_keys.insert(k.clone());
            }
        }

        // Overlay static features
        for entry in static_store.iter() {
            let entity_key = entry.key();
            let static_features = entry.value();
            let mut entity = entities.entry(entity_key.clone()).or_default();
            entity.static_features = static_features.clone();
        }

        StateStore {
            entities,
            dirty_keys: parking_lot::Mutex::new(dirty_keys),
            deleted_keys: parking_lot::Mutex::new(deleted_keys),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::operators::{CountOp, SumOp};
    use crate::state::snapshot::OperatorState;
    use std::time::{Duration, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_new_store_is_empty() {
        let store = StateStore::new();
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn test_get_or_create_entity_creates_new() {
        let store = StateStore::new();
        {
            let entity = store.get_or_create_entity("u123");
            assert!(entity.streams.is_empty());
            assert!(entity.static_features.is_empty());
            assert!(entity.is_empty());
        }
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_get_or_create_entity_returns_existing() {
        let store = StateStore::new();
        // First call creates
        store.get_or_create_entity("u123");
        // Mutate the entity so we can verify it's the same one
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream_state = entity.get_or_create_stream("TestStream");
            stream_state.last_event_at = Some(ts(1000));
        }
        assert_eq!(store.entity_count(), 1); // Still only 1 entity
        let entity = store.get_entity("u123").unwrap();
        assert_eq!(
            entity.streams.get("TestStream").unwrap().last_event_at,
            Some(ts(1000))
        );
    }

    #[test]
    fn test_stream_entity_state_holds_operators_with_independent_last_event_at() {
        let mut entity = EntityState::new();
        let op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        let stream = entity.get_or_create_stream("Transactions");
        stream.operators.push(("tx_count_1h".to_string(), op));
        stream.last_event_at = Some(ts(1000));

        let stream2 = entity.get_or_create_stream("Logins");
        stream2.last_event_at = Some(ts(2000));

        // Each stream has independent last_event_at
        assert_eq!(
            entity.streams.get("Transactions").unwrap().last_event_at,
            Some(ts(1000))
        );
        assert_eq!(
            entity.streams.get("Logins").unwrap().last_event_at,
            Some(ts(2000))
        );
        assert_eq!(
            entity.streams.get("Transactions").unwrap().operators.len(),
            1
        );
        assert_eq!(
            entity.streams.get("Transactions").unwrap().operators[0].0,
            "tx_count_1h"
        );
    }

    #[test]
    fn test_entity_state_stores_static_features() {
        let store = StateStore::new();
        store.set_static(
            "u123",
            "lifetime_value",
            FeatureValue::Float(4500.0),
            ts(1000),
        );
        let entity = store.get_entity("u123").unwrap();
        assert_eq!(entity.static_features.len(), 1);
        assert_eq!(
            entity.static_features.get("lifetime_value").unwrap().value,
            FeatureValue::Float(4500.0)
        );
    }

    #[test]
    fn test_get_all_features_merges_all_streams_and_static() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Add a live operator in a named stream
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("Transactions");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("tx_count".to_string(), op));
        }

        // Add a static feature
        store.set_static(
            "u123",
            "segment",
            FeatureValue::String("high_value".into()),
            ts(1000),
        );

        let features = store.get_all_features("u123", now);
        assert_eq!(features.get("tx_count"), Some(&FeatureValue::Int(1)));
        assert_eq!(
            features.get("segment"),
            Some(&FeatureValue::String("high_value".into()))
        );
    }

    #[test]
    fn test_static_feature_overrides_live_feature_same_name() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Add a live operator named "score" in a stream
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("Transactions");
            let mut op = OperatorState::Sum(SumOp::new(
                "amount",
                Duration::from_secs(3600),
                Duration::from_secs(60),
                false,
            ));
            op.push(&serde_json::json!({"amount": 100.0}), None, now)
                .unwrap();
            stream.operators.push(("score".to_string(), op));
        }

        // Write a static feature with the same name
        store.set_static("u123", "score", FeatureValue::Float(999.0), ts(1000));

        let features = store.get_all_features("u123", now);
        // Static takes precedence
        assert_eq!(features.get("score"), Some(&FeatureValue::Float(999.0)));
    }

    #[test]
    fn test_get_feature_value_searches_across_all_streams() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Add operators in two different streams
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream1 = entity.get_or_create_stream("Transactions");
            let mut op1 = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op1.push(&serde_json::json!({}), None, now).unwrap();
            stream1.operators.push(("tx_count".to_string(), op1));

            let stream2 = entity.get_or_create_stream("Logins");
            let mut op2 = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op2.push(&serde_json::json!({}), None, now).unwrap();
            op2.push(&serde_json::json!({}), None, now).unwrap();
            stream2.operators.push(("login_count".to_string(), op2));
        }

        let val = store.get_feature_value("u123", "tx_count", now);
        assert_eq!(val, FeatureValue::Int(1));

        let val = store.get_feature_value("u123", "login_count", now);
        assert_eq!(val, FeatureValue::Int(2));
    }

    #[test]
    fn test_get_all_features_unknown_key_returns_empty() {
        let store = StateStore::new();
        let features = store.get_all_features("nonexistent", ts(1000));
        assert!(features.is_empty());
    }

    #[test]
    fn test_entity_is_empty() {
        let entity = EntityState::new();
        assert!(entity.is_empty());

        let mut entity2 = EntityState::new();
        entity2.get_or_create_stream("Transactions");
        assert!(!entity2.is_empty());

        let mut entity3 = EntityState::new();
        entity3.static_features.insert(
            "key".to_string(),
            StaticFeature {
                value: FeatureValue::Int(1),
                updated_at: ts(1000),
            },
        );
        assert!(!entity3.is_empty());
    }

    // ======================== clone_for_snapshot / restore_from_snapshot Tests ========================

    #[test]
    fn test_clone_for_snapshot_preserves_per_stream_state() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Add an entity with a live operator in a named stream and static feature
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("Transactions");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("tx_count".to_string(), op));
            stream.last_event_at = Some(now);
        }
        store.set_static(
            "u123",
            "segment",
            FeatureValue::String("premium".into()),
            now,
        );

        let snapshot = store.clone_for_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].0, "u123");
        assert_eq!(snapshot[0].1.streams.len(), 1);
        assert_eq!(snapshot[0].1.static_features.len(), 1);

        // Verify stream state preserved
        let stream_snap = &snapshot[0].1.streams[0];
        assert_eq!(stream_snap.0, "Transactions");
        assert_eq!(stream_snap.1.operators.len(), 1);
        assert_eq!(stream_snap.1.last_event_at, Some(now));

        // Verify operator state preserved
        let mut op = stream_snap.1.operators[0].1.clone();
        assert_eq!(op.read(now), FeatureValue::Int(2));
    }

    #[test]
    fn test_restore_from_snapshot_v4() {
        let store = StateStore::new();
        let now = ts(60_000);

        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        op.push(&serde_json::json!({}), None, now).unwrap();

        let snapshot_entities = vec![(
            "u456".to_string(),
            crate::state::snapshot::SerializableEntityState {
                streams: vec![(
                    "TestStream".to_string(),
                    SerializableStreamEntityState {
                        operators: vec![("count".to_string(), op)],
                        last_event_at: Some(now),
                    },
                )],
                static_features: vec![(
                    "tier".to_string(),
                    StaticFeature {
                        value: FeatureValue::String("gold".into()),
                        updated_at: now,
                    },
                )],
                table_rows: vec![],
            },
        )];

        store.restore_from_snapshot(snapshot_entities);
        assert_eq!(store.entity_count(), 1);
        let entity = store.get_entity("u456").unwrap();
        assert_eq!(entity.streams.len(), 1);
        let stream = entity.streams.get("TestStream").unwrap();
        assert_eq!(stream.operators.len(), 1);
        assert_eq!(stream.last_event_at, Some(now));
        assert_eq!(entity.static_features.len(), 1);
    }

    // ======================== get_feature_value Tests ========================

    #[test]
    fn test_get_feature_value_returns_live_operator_value() {
        let store = StateStore::new();
        let now = ts(60_000);

        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("TestStream");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("tx_count".to_string(), op));
        }

        let val = store.get_feature_value("u123", "tx_count", now);
        assert_eq!(val, FeatureValue::Int(2));
    }

    #[test]
    fn test_get_feature_value_returns_static_feature() {
        let store = StateStore::new();
        let now = ts(60_000);
        store.set_static(
            "u123",
            "segment",
            FeatureValue::String("premium".into()),
            now,
        );

        let val = store.get_feature_value("u123", "segment", now);
        assert_eq!(val, FeatureValue::String("premium".into()));
    }

    #[test]
    fn test_get_feature_value_returns_missing_for_unknown_entity() {
        let store = StateStore::new();
        let val = store.get_feature_value("nonexistent", "anything", ts(60_000));
        assert_eq!(val, FeatureValue::Missing);
    }

    #[test]
    fn test_get_feature_value_returns_missing_for_unknown_feature() {
        let store = StateStore::new();
        store.get_or_create_entity("u123");
        let val = store.get_feature_value("u123", "nonexistent_feature", ts(60_000));
        assert_eq!(val, FeatureValue::Missing);
    }

    // ======================== remove_expired_entities Tests ========================

    #[test]
    fn test_remove_expired_entities() {
        let store = StateStore::new();
        let now = ts(100_000);
        let ttl = Duration::from_secs(3600); // 1 hour TTL

        // Entity with old last_event_at (should be evicted)
        {
            let mut entity = store.get_or_create_entity("old_user");
            let stream = entity.get_or_create_stream("TestStream");
            stream.last_event_at = Some(ts(1000)); // Very old
        }

        // Entity with recent last_event_at (should be kept)
        {
            let mut entity = store.get_or_create_entity("recent_user");
            let stream = entity.get_or_create_stream("TestStream");
            stream.last_event_at = Some(ts(99_000)); // Recent
        }

        // Entity with no streams (should be kept -- never pushed)
        store.get_or_create_entity("no_event_user");

        assert_eq!(store.entity_count(), 3);
        let evicted = store.remove_expired_entities(now, ttl);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("old_user").is_none());
        assert!(store.get_entity("recent_user").is_some());
        assert!(store.get_entity("no_event_user").is_some());
    }

    // ======================== remove_empty_entities Tests ========================

    // ======================== clone_for_snapshot_with_gc Tests ========================

    #[test]
    fn test_clone_for_snapshot_with_gc() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Create entity with operators a, b, c in stream "Transactions"
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("Transactions");
            let mut op_a = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op_a.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("a".to_string(), op_a));

            let mut op_b = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op_b.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("b".to_string(), op_b));

            let mut op_c = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op_c.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("c".to_string(), op_c));
        }

        // Valid features: only a and c (b was removed from definition)
        let mut valid_features = ahash::AHashMap::new();
        valid_features.insert(
            "Transactions".to_string(),
            vec!["a".to_string(), "c".to_string()],
        );

        let snapshot = store.clone_for_snapshot_with_gc(&valid_features);
        assert_eq!(snapshot.len(), 1);
        let stream_snap = &snapshot[0].1.streams[0];
        assert_eq!(stream_snap.0, "Transactions");
        // Only a and c should be present, b filtered out
        let op_names: Vec<&String> = stream_snap.1.operators.iter().map(|(n, _)| n).collect();
        assert_eq!(op_names.len(), 2);
        assert!(op_names.contains(&&"a".to_string()));
        assert!(op_names.contains(&&"c".to_string()));
        assert!(!op_names.contains(&&"b".to_string()));
    }

    #[test]
    fn test_clone_for_snapshot_with_gc_unknown_stream_includes_all() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Create entity with operators in stream "OldStream" (not in valid_features)
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("OldStream");
            let mut op_a = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op_a.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("x".to_string(), op_a));
        }

        // Valid features map does not contain "OldStream"
        let valid_features = ahash::AHashMap::new();

        let snapshot = store.clone_for_snapshot_with_gc(&valid_features);
        assert_eq!(snapshot.len(), 1);
        let stream_snap = &snapshot[0].1.streams[0];
        assert_eq!(stream_snap.0, "OldStream");
        // All operators included (defensive behavior)
        assert_eq!(stream_snap.1.operators.len(), 1);
    }

    #[test]
    fn test_remove_empty_entities() {
        let store = StateStore::new();

        // Empty entity (should be removed)
        store.get_or_create_entity("empty");

        // Entity with a stream (should be kept)
        {
            let mut entity = store.get_or_create_entity("has_stream");
            entity.get_or_create_stream("TestStream");
        }

        // Entity with static features (should be kept)
        store.set_static("has_static", "key", FeatureValue::Int(1), ts(1000));

        assert_eq!(store.entity_count(), 3);
        store.remove_empty_entities();
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("empty").is_none());
        assert!(store.get_entity("has_stream").is_some());
        assert!(store.get_entity("has_static").is_some());
    }

    // ======================== Phase 9: Dirty / Deleted Tracking Tests ========================

    #[test]
    fn test_mark_dirty_inserts_key() {
        let store = StateStore::new();
        store.mark_dirty("u123");
        assert_eq!(store.dirty_count(), 1);
        assert!(store.dirty_keys().contains("u123"));
    }

    #[test]
    fn test_mark_dirty_is_idempotent() {
        let store = StateStore::new();
        store.mark_dirty("u123");
        store.mark_dirty("u123");
        store.mark_dirty("u123");
        assert_eq!(store.dirty_count(), 1);
    }

    #[test]
    fn test_mark_dirty_multiple_keys() {
        let store = StateStore::new();
        store.mark_dirty("u1");
        store.mark_dirty("u2");
        store.mark_dirty("u3");
        assert_eq!(store.dirty_count(), 3);
    }

    #[test]
    fn test_clear_dirty_empties_the_set() {
        let store = StateStore::new();
        store.mark_dirty("u1");
        store.mark_dirty("u2");
        assert_eq!(store.dirty_count(), 2);
        store.clear_dirty();
        assert_eq!(store.dirty_count(), 0);
        assert!(store.dirty_keys().is_empty());
    }

    #[test]
    fn test_mark_deleted_records_key() {
        let store = StateStore::new();
        store.mark_deleted("u456");
        let deleted = store.take_deleted();
        assert_eq!(deleted.len(), 1);
        assert!(deleted.contains(&"u456".to_string()));
    }

    #[test]
    fn test_mark_deleted_removes_from_dirty() {
        // A key that was marked dirty and then deleted should NOT appear in
        // the dirty set -- it must only appear in the deleted list.
        let store = StateStore::new();
        store.mark_dirty("u789");
        assert_eq!(store.dirty_count(), 1);

        store.mark_deleted("u789");
        assert_eq!(
            store.dirty_count(),
            0,
            "Deleted key must be removed from dirty set"
        );

        let deleted = store.take_deleted();
        assert_eq!(deleted, vec!["u789".to_string()]);
    }

    #[test]
    fn test_take_deleted_clears_the_set() {
        let store = StateStore::new();
        store.mark_deleted("a");
        store.mark_deleted("b");

        let first = store.take_deleted();
        assert_eq!(first.len(), 2);

        // Second call returns empty
        let second = store.take_deleted();
        assert!(second.is_empty(), "take_deleted should clear the set");
    }

    #[test]
    fn test_dirty_count_returns_zero_when_empty() {
        let store = StateStore::new();
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn test_clone_dirty_for_snapshot_returns_only_dirty_entities() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Create three entities with live operators
        for key in &["u1", "u2", "u3"] {
            let mut entity = store.get_or_create_entity(key);
            let stream = entity.get_or_create_stream("Transactions");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("tx_count".to_string(), op));
        }

        // Only mark u1 and u3 as dirty
        store.mark_dirty("u1");
        store.mark_dirty("u3");

        let valid_features = ahash::AHashMap::new();
        let snapshot = store.clone_dirty_for_snapshot_with_gc(&valid_features);

        assert_eq!(snapshot.len(), 2);
        let keys: Vec<&String> = snapshot.iter().map(|(k, _)| k).collect();
        assert!(keys.contains(&&"u1".to_string()));
        assert!(keys.contains(&&"u3".to_string()));
        assert!(!keys.contains(&&"u2".to_string()));
    }

    #[test]
    fn test_clone_dirty_for_snapshot_empty_when_no_dirty() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Create an entity but do NOT mark it dirty
        {
            let mut entity = store.get_or_create_entity("u1");
            let stream = entity.get_or_create_stream("Transactions");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("tx_count".to_string(), op));
        }

        let valid_features = ahash::AHashMap::new();
        let snapshot = store.clone_dirty_for_snapshot_with_gc(&valid_features);
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_clone_dirty_for_snapshot_applies_gc_filtering() {
        let store = StateStore::new();
        let now = ts(60_000);

        // Create entity with operators a, b, c in stream "Transactions"
        {
            let mut entity = store.get_or_create_entity("u123");
            let stream = entity.get_or_create_stream("Transactions");
            for name in &["a", "b", "c"] {
                let mut op = OperatorState::Count(CountOp::new(
                    Duration::from_secs(3600),
                    Duration::from_secs(60),
                ));
                op.push(&serde_json::json!({}), None, now).unwrap();
                stream.operators.push((name.to_string(), op));
            }
        }
        store.mark_dirty("u123");

        // Valid features: only a and c (b was removed from definition)
        let mut valid_features = ahash::AHashMap::new();
        valid_features.insert(
            "Transactions".to_string(),
            vec!["a".to_string(), "c".to_string()],
        );

        let snapshot = store.clone_dirty_for_snapshot_with_gc(&valid_features);
        assert_eq!(snapshot.len(), 1);
        let stream_snap = &snapshot[0].1.streams[0];
        assert_eq!(stream_snap.0, "Transactions");
        let op_names: Vec<&String> = stream_snap.1.operators.iter().map(|(n, _)| n).collect();
        assert_eq!(op_names.len(), 2);
        assert!(op_names.contains(&&"a".to_string()));
        assert!(op_names.contains(&&"c".to_string()));
        assert!(!op_names.contains(&&"b".to_string()));
    }

    #[test]
    fn test_clone_dirty_for_snapshot_unknown_stream_includes_all() {
        let store = StateStore::new();
        let now = ts(60_000);

        {
            let mut entity = store.get_or_create_entity("u1");
            let stream = entity.get_or_create_stream("OldStream");
            let mut op = OperatorState::Count(CountOp::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
            ));
            op.push(&serde_json::json!({}), None, now).unwrap();
            stream.operators.push(("x".to_string(), op));
        }
        store.mark_dirty("u1");

        // valid_features does NOT contain OldStream
        let valid_features = ahash::AHashMap::new();
        let snapshot = store.clone_dirty_for_snapshot_with_gc(&valid_features);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].1.streams[0].1.operators.len(), 1);
    }

    #[test]
    fn test_clone_dirty_skips_keys_that_are_dirty_but_not_in_entities() {
        // Edge case: a key was marked dirty but the underlying entity was removed
        // (e.g., via remove_empty_entities). clone_dirty should simply skip it.
        let store = StateStore::new();
        store.mark_dirty("ghost");
        // No entity for "ghost" was ever created
        let valid_features = ahash::AHashMap::new();
        let snapshot = store.clone_dirty_for_snapshot_with_gc(&valid_features);
        assert!(snapshot.is_empty());
    }

    // ======================== bulk_load Tests (Phase 28-04) ========================

    fn _entity_with_stream(stream_name: &str, now: SystemTime) -> crate::state::snapshot::SerializableEntityState {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        op.push(&serde_json::json!({}), None, now).unwrap();
        crate::state::snapshot::SerializableEntityState {
            streams: vec![(
                stream_name.to_string(),
                SerializableStreamEntityState {
                    operators: vec![("count".to_string(), op)],
                    last_event_at: Some(now),
                },
            )],
            static_features: vec![],
            table_rows: vec![],
        }
    }

    #[test]
    fn bulk_load_empty_input_is_noop() {
        let store = StateStore::new();
        store.bulk_load(vec![]);
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn bulk_load_single_entity_inserts_without_clearing() {
        let store = StateStore::new();
        let now = ts(1000);
        // Pre-populate so we can verify bulk_load does NOT clear.
        store.get_or_create_entity("preexisting");
        assert_eq!(store.entity_count(), 1);
        store.bulk_load(vec![("u_a".to_string(), _entity_with_stream("Txn", now))]);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("u_a").is_some());
        assert!(store.get_entity("preexisting").is_some());
        let e = store.get_entity("u_a").unwrap();
        assert_eq!(e.streams.get("Txn").unwrap().last_event_at, Some(now));
    }

    #[test]
    fn bulk_load_overlapping_keys_overwrite() {
        let store = StateStore::new();
        let earlier = ts(1000);
        let later = ts(2000);
        store.bulk_load(vec![("u_a".to_string(), _entity_with_stream("Txn", earlier))]);
        store.bulk_load(vec![("u_a".to_string(), _entity_with_stream("Txn", later))]);
        assert_eq!(store.entity_count(), 1);
        let e = store.get_entity("u_a").unwrap();
        assert_eq!(e.streams.get("Txn").unwrap().last_event_at, Some(later));
    }

    #[test]
    fn bulk_load_multi_stream_entity() {
        let store = StateStore::new();
        let now = ts(1000);
        let mut e = _entity_with_stream("StreamA", now);
        // Add a second stream to the same entity.
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        op.push(&serde_json::json!({}), None, now).unwrap();
        e.streams.push((
            "StreamB".to_string(),
            SerializableStreamEntityState {
                operators: vec![("count".to_string(), op)],
                last_event_at: Some(now),
            },
        ));
        store.bulk_load(vec![("u_multi".to_string(), e)]);
        let got = store.get_entity("u_multi").unwrap();
        assert_eq!(got.streams.len(), 2);
        assert!(got.streams.contains_key("StreamA"));
        assert!(got.streams.contains_key("StreamB"));
    }

    #[test]
    fn bulk_load_does_not_mark_dirty() {
        // Regression guard: bulk_load must NOT record dirty keys — a clone
        // population is not a local mutation event.
        let store = StateStore::new();
        let now = ts(1000);
        store.bulk_load(vec![("u_a".to_string(), _entity_with_stream("Txn", now))]);
        assert_eq!(store.dirty_count(), 0);
    }

    // ======================== collect_table_row_view Tests (Phase 25-01) ========================

    mod collect_table_row_view {
        use super::*;

        #[test]
        fn never_seen_entity_returns_none() {
            let store = StateStore::new();
            let now = ts(1000);
            assert!(store.collect_table_row_view("nobody", "UserProfile", now).is_none());
        }

        #[test]
        fn live_row_returns_some_object_with_fields() {
            let store = StateStore::new();
            let now = ts(1000);
            let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
            fields.insert("country".into(), FeatureValue::String("US".into()));
            fields.insert("score".into(), FeatureValue::Int(42));
            store.upsert_table_row("u1", "UserProfile", fields, now);

            let v = store
                .collect_table_row_view("u1", "UserProfile", now)
                .expect("live row must project to Some");
            let obj = v.as_object().expect("row view must be a JSON object");
            assert_eq!(obj.get("country").and_then(|v| v.as_str()), Some("US"));
            assert_eq!(obj.get("score").and_then(|v| v.as_i64()), Some(42));
            assert_eq!(obj.len(), 2);
        }

        #[test]
        fn tombstoned_row_collapses_to_none() {
            let store = StateStore::new();
            let now = ts(1000);
            let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
            fields.insert("x".into(), FeatureValue::Int(1));
            store.upsert_table_row("u1", "T", fields, now);
            store.tombstone_table_row("u1", "T", now);

            assert!(
                store.collect_table_row_view("u1", "T", now).is_none(),
                "tombstoned rows must never leak fields (T-25-01-02)"
            );
        }

        #[test]
        fn still_none_after_gc_tombstones() {
            let store = StateStore::new();
            let now = ts(1000);
            let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
            fields.insert("x".into(), FeatureValue::Int(1));
            store.upsert_table_row("u1", "T", fields, now);
            store.tombstone_table_row("u1", "T", now);

            // Advance past TOMBSTONE_GRACE and GC.
            let later = now + TOMBSTONE_GRACE + Duration::from_secs(1);
            let removed = store.gc_tombstones(later);
            assert_eq!(removed, 1);
            assert!(store.collect_table_row_view("u1", "T", later).is_none());
        }

        #[test]
        fn unknown_table_on_existing_entity_returns_none() {
            let store = StateStore::new();
            let now = ts(1000);
            let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
            fields.insert("x".into(), FeatureValue::Int(1));
            store.upsert_table_row("u1", "Known", fields, now);
            assert!(store.collect_table_row_view("u1", "Unknown", now).is_none());
        }
    }
}
