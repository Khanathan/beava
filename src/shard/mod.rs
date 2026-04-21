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
/// Phase 55-01 D-A1+D-A2: per-batch source-side coalesce buffer for
/// cross-shard TT cascade (see `src/shard/cascade_buffer.rs`).
pub mod cascade_buffer;
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
use std::time::SystemTime;

use crate::state::event_log::EventLog;
use crate::state::store::{EntityState, TableRow, TableRowState};
use crate::types::FeatureValue;
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
// Phase 54-02 Task 1 (Pass A): widened Shard surface for legacy-StateStore
// parity. Mirrors `StateStore::{delete_entity, tombstone_static,
// upsert_table_row, tombstone_table_row}` against per-shard state. Adds
// `take_dirty` + `iter_entities` so the snapshot cycle can run per-shard
// (legacy DashMap.iter() has no direct equivalent).
//
// Semantics contract (preserved from StateStore EXCEPT where noted):
// - `delete_entity`: **SEMANTIC DIVERGENCE** from legacy. Legacy aliases
//   `tombstone_static` (keeps entity, clears static_features). The Shard
//   variant REMOVES the entity from storage entirely — the plan's unit test
//   spec requires `read_entity_from_shard` to return `None` after
//   `delete_entity`. Wave 4 deletes the legacy path, unifying semantics.
// - `tombstone_static`: clears static_features (preserves streams +
//   table_rows); returns `true` iff the entity had prior static_features.
//   Marks the key dirty on success.
// - `upsert_table_row`: RMW — writes `TableRow { fields, Live, now }` at
//   `(key, table_name)`, replacing any prior row (live or tombstoned).
//   Marks dirty.
// - `tombstone_table_row`: RMW — flips the row at `(key, table_name)` to
//   `Tombstoned { since: now }` (creates an empty-fields tombstone if
//   absent). Returns `true` iff a prior **Live** row existed under this
//   identity. Marks dirty.
//
// Single-writer invariant: all of these take `&mut Shard` — caller is the
// shard thread that owns the `fjall::PartitionHandle` (default build) or
// the `AHashMap` (state-inmem build).
// ---------------------------------------------------------------------------

impl Shard {
    /// Phase 54-02: Remove the entity for `key` from this shard's state.
    ///
    /// Returns `true` iff the entity was present before the call. On the
    /// default (fjall) build this issues `PartitionHandle::remove`; on
    /// state-inmem it removes from the `AHashMap`. Also removes the key
    /// from the shard's `dirty_set` because a deleted entity cannot be
    /// part of an incremental snapshot delta (matches
    /// `StateStore::mark_deleted`'s dirty-removal semantics).
    ///
    /// NOTE: this diverges from `StateStore::delete_entity` which is an
    /// alias for `tombstone_static` and KEEPS the entity. Phase 54-04 Pass
    /// A6a deleted the `StoreView::Legacy` arm, unifying on full-removal
    /// semantics.
    #[cfg(not(feature = "state-inmem"))]
    pub fn delete_entity(&mut self, key: &str) -> bool {
        let existed = matches!(self.state.get(key.as_bytes()), Ok(Some(_)));
        if existed {
            self.state
                .remove(key.as_bytes())
                .expect("fjall partition remove");
            self.dirty_set.remove(key);
        }
        existed
    }

    #[cfg(feature = "state-inmem")]
    pub fn delete_entity(&mut self, key: &str) -> bool {
        let existed = self.state.remove(key).is_some();
        if existed {
            self.dirty_set.remove(key);
        }
        existed
    }

    /// Phase 54-02: Clear all static_features for `key` (legacy
    /// `StateStore::tombstone_static` parity). Preserves streams +
    /// table_rows. Returns `true` iff the entity had static features
    /// before the call.
    pub fn tombstone_static(&mut self, key: &str) -> bool {
        let had_rows = {
            let view = StoreView::Sharded(self);
            view.get_entity_ref(key, |e| !e.static_features.is_empty())
                .unwrap_or(false)
        };
        if !had_rows {
            return false;
        }
        {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(key, |e| e.static_features.clear());
        }
        self.dirty_set.insert(key.to_string());
        true
    }

    /// Phase 54-02: Upsert a Table row for `(key, table_name)`. Mirrors
    /// `StateStore::upsert_table_row` — same field-map signature, same
    /// "fresh Live row replaces prior state" semantics. Marks dirty.
    pub fn upsert_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        fields: ahash::AHashMap<String, FeatureValue>,
        now: SystemTime,
    ) {
        {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(key, |entity| {
                entity.table_rows.insert(
                    table_name.to_string(),
                    TableRow {
                        fields,
                        state: TableRowState::Live,
                        updated_at: now,
                    },
                );
            });
        }
        self.dirty_set.insert(key.to_string());
    }

    /// Phase 55-02 D-B5 (TPC-SOURCE-01): full-replace upsert for a
    /// source-table row. Stores the row under `(key, table_name)` as a
    /// fresh `Live` row with fields.
    ///
    /// # `source_lsn` handling (Phase 55 MED-5 clarification)
    ///
    /// The D-B3 locked decision called for `source_lsn` to be "echoed on
    /// ack; stored per row". Phase 55 delivers the ack-echo half: TCP
    /// opcodes and HTTP handlers echo the input `source_lsn` back to the
    /// client in the response body, which is sufficient for Debezium-
    /// style "I know what I sent" resume semantics.
    ///
    /// The per-row storage half is DEFERRED to Phase 56/57, because
    /// landing it here would require adding `source_lsn: Option<u64>` to
    /// the `TableRow` struct — a postcard schema_version v10 bump not
    /// otherwise justified by Phase 55 correctness. Full-replace
    /// idempotence under CDC retry is guaranteed by identical fields
    /// content (D-B5), and Phase 57 consumes the PendingRetraction
    /// marker from the event log — neither path requires per-row LSN
    /// storage today.
    ///
    /// The parameter is therefore accepted for API stability (callers
    /// pass it; future implementations will store it) but discarded in
    /// this wave. External CDC connectors that plan to query per-row LSN
    /// must wait for Phase 56 or a dedicated follow-up.
    ///
    /// Mirrors `Shard::upsert_table_row` but is kept as a distinct entry
    /// point so the cascade-suppression invariant (D-B6 — no cascade on
    /// source-table writes) is localised at the dispatch arm that calls
    /// this method, not here.
    pub fn upsert_source_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        fields: ahash::AHashMap<String, FeatureValue>,
        _source_lsn: u64,
        now: SystemTime,
    ) {
        {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(key, |entity| {
                entity.table_rows.insert(
                    table_name.to_string(),
                    TableRow {
                        fields,
                        state: TableRowState::Live,
                        updated_at: now,
                    },
                );
            });
        }
        self.dirty_set.insert(key.to_string());
    }

    /// Phase 55-02 D-B5: hard-delete a source-table row. Unlike
    /// `tombstone_table_row` (which keeps an empty-fields Tombstoned entry),
    /// this removes the `(key, table_name)` entry from the entity's
    /// `table_rows` map entirely. Subsequent reads return `None`. Caller is
    /// responsible for writing the `PendingRetraction` marker to the event
    /// log (Phase 57 consumer).
    pub fn delete_source_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        _now: SystemTime,
    ) -> bool {
        let had_row = {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(key, |entity| entity.table_rows.remove(table_name).is_some())
        };
        if had_row {
            self.dirty_set.insert(key.to_string());
        }
        had_row
    }

    /// Phase 54-02: Tombstone a Table row for `(key, table_name)`. Mirrors
    /// `StateStore::tombstone_table_row` — flips an existing Live row to
    /// `Tombstoned { since: now }` or creates an empty-fields tombstone.
    /// Returns `true` iff a prior **Live** row existed under this
    /// identity. Marks dirty.
    pub fn tombstone_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        now: SystemTime,
    ) -> bool {
        let had_live = {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(key, |entity| {
                let prior_live = entity
                    .table_rows
                    .get(table_name)
                    .map(|r| matches!(r.state, TableRowState::Live))
                    .unwrap_or(false);
                entity.table_rows.insert(
                    table_name.to_string(),
                    TableRow {
                        fields: ahash::AHashMap::new(),
                        state: TableRowState::Tombstoned { since: now },
                        updated_at: now,
                    },
                );
                prior_live
            })
        };
        self.dirty_set.insert(key.to_string());
        had_live
    }

    /// Phase 54-02: Consume the dirty-set, returning its prior contents.
    /// The shard's `dirty_set` is left empty. Used by the per-shard
    /// snapshot cycle (Wave 2+) where each shard thread flushes its own
    /// delta without contending on a shared `ArcSwap<DashSet>` —
    /// replaces `StateStore::take_dirty_and_advance_gen` for the shard
    /// path.
    pub fn take_dirty(&mut self) -> HashSet<EntityKey> {
        std::mem::take(&mut self.dirty_set)
    }

    /// Phase 57-02 (TPC-CORR-10): snapshot the set of entity keys on this
    /// shard whose row carries a populated `streams[stream_name]` slot.
    /// Used by `PipelineEngine::fan_out_retraction_for_primary` to find
    /// candidate downstream rows matching a tombstoned primary event.
    ///
    /// Implementation: iterates the shard's `dirty_set` and filters by
    /// "entity has a non-empty stream slot for `stream_name`". This is
    /// O(dirty_count) not O(all_rows) — the per-batch mark-dirty
    /// discipline in `push_with_cascade_on_shard` bounds the walk to
    /// rows touched by the current event batch.
    ///
    /// Wave 2 trade-off: we filter by `dirty_set` rather than scanning
    /// the full partition because (a) the tombstone fan-out is a hot path
    /// on the write side and (b) a full-scan would dominate Phase 57's
    /// perf gate. If Wave 3+ needs broader coverage (cascade rows NOT
    /// dirty in the current batch — e.g. cross-batch retractions), a
    /// secondary reverse index on `contributing_inputs.primary_event_id`
    /// becomes justified.
    ///
    /// Returns an owning `Vec<String>` so the caller can iterate without
    /// holding a borrow on the shard (enabling mutable re-entry for the
    /// inline `apply_retraction` fast path).
    pub fn dirty_set_for_stream_snapshot(&self, stream_name: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(self.dirty_set.len().min(64));
        for key in self.dirty_set.iter() {
            let has_stream: bool = read_entity_from_shard(self, key, |entity| {
                entity.streams.contains_key(stream_name)
            })
            .unwrap_or(false);
            if has_stream {
                out.push(key.clone());
            }
        }
        out
    }

    /// Phase 54-02: Iterate all entities held by this shard as
    /// `(key, EntityState)` pairs. On the default (fjall) build each row
    /// is deserialized via postcard on-demand; on state-inmem it's a
    /// cloning iteration over the `AHashMap`.
    ///
    /// Corrupt rows (postcard deserialize Err) are silently skipped —
    /// matches the `T-53-03-01` mitigation used by `with_entity_mut` /
    /// `read_entity_from_shard`. Callers that need a faithful error-
    /// surfacing iterator should route through `shard.state.iter()`
    /// directly on the default build.
    ///
    /// Returns an owning `Vec` rather than a borrowed iterator because:
    /// (a) the fjall branch must materialize deserialized entities anyway,
    /// and (b) borrowing from the partition handle across the yield point
    /// is awkward (KvPair is `Result` with Slice types). For the snapshot
    /// cycle this is fine — the whole point is to produce an owned delta.
    #[cfg(not(feature = "state-inmem"))]
    pub fn iter_entities(&self) -> Vec<(String, EntityState)> {
        self.state
            .iter()
            .filter_map(|kv| {
                let (k, v) = kv.ok()?;
                let key = std::str::from_utf8(&k).ok()?.to_string();
                let entity = entity_from_bytes(&v)?;
                Some((key, entity))
            })
            .collect()
    }

    #[cfg(feature = "state-inmem")]
    pub fn iter_entities(&self) -> Vec<(String, EntityState)> {
        self.state
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Phase 56 D-A1: pure entity lookup used by EnrichFromTable +
    /// StreamStreamJoin buffer read-side. Backend-agnostic: wraps the
    /// existing `read_entity_from_shard` helper and clones the
    /// `EntityState` out. Returns `None` when the key is absent on this
    /// shard's partition (caller increments enrich_missing_total).
    ///
    /// The `table_name` parameter is currently unused at this level —
    /// the entity and its table rows live inside a single EntityState on
    /// this shard, and the caller (operator eval) selects which
    /// `table_rows[table_name]` field to pull from the returned entity.
    /// The parameter is threaded through to match the D-A1 dispatch
    /// shape so metric labels + future per-table indexing work without
    /// a signature change.
    ///
    /// No mutation: takes `&self`. Safe to call from any reader holding
    /// a `&Shard`.
    pub fn read_entity_at(
        &self,
        _table_name: &str,
        key: &str,
    ) -> Option<EntityState> {
        read_entity_from_shard(self, key, |e| e.clone())
    }

    /// Phase 56 D-B1: cross-shard StreamStreamJoin buffer insert on the
    /// join-key-owning shard. Mirrors the Phase 23 StreamStreamJoin
    /// block in `pipeline.rs::push_with_cascade_on_shard` but runs on
    /// the target shard (this shard) for the relocated
    /// `hash(join_key) % N` ownership.
    ///
    /// Semantics:
    /// 1. Look up / create the `EntityState` at `join_key` on this shard.
    /// 2. Find the `StreamJoinBuffer` operator for `join_id` (feat_name
    ///    IS the join_id — confirmed in pipeline.rs:1813); create a
    ///    fresh one with `within_ms` if absent.
    /// 3. Probe the OPPOSITE side for matches in the symmetric interval
    ///    window (`|arriving_ts - buffered_ts| <= within_ms`).
    /// 4. Insert the arriving event on `side`; evict old entries.
    /// 5. Return the matched counterparty event maps (possibly empty).
    ///
    /// The caller (source shard on the Wave 3 dispatch path) consumes
    /// the Vec to emit joined outputs via its existing downstream
    /// cascade. Wave 1 does not yet wire this into the operator eval
    /// path — `apply_ssj_insert` is a primitive that Wave 3 calls.
    ///
    /// T-56-01-02 mitigation: if `event` is not a JSON object (e.g. a
    /// bare string/number from a malformed source), returns an empty
    /// matches Vec WITHOUT inserting anything — matches the silent-skip
    /// behaviour the existing StreamStreamJoin eval uses.
    pub fn apply_ssj_insert(
        &mut self,
        join_id: &str,
        side: crate::engine::operators::JoinSide,
        join_key: &str,
        event: serde_json::Value,
        within_ms: u64,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        // T-56-01-02: reject non-object events silently.
        let arriving_map: serde_json::Map<String, serde_json::Value> = match event {
            serde_json::Value::Object(m) => m,
            _ => return Vec::new(),
        };

        // Derive event_time_ms from the event, falling back to 0 on
        // parse failure (matches the Phase 23 behaviour in pipeline.rs
        // where `parse_event_time().unwrap_or(now)` is used — but here
        // the join-owning shard has no `now` parameter; 0 is the safe
        // default because the evict floor is `max_seen - within_ms`
        // and a zero timestamp will simply be evicted on the next
        // insert with a later timestamp).
        let event_time_ms: u64 = crate::engine::operators::parse_event_time(
            &serde_json::Value::Object(arriving_map.clone()),
        )
        .and_then(|st| {
            st.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_millis() as u64)
        })
        .unwrap_or(0);

        // The SSJ buffer lives under a synthetic stream name on the
        // EntityState — for the relocated (cross-shard) path the
        // stream-scope doesn't matter, only the (join_id, join_key)
        // pair identifies the buffer. Use a dedicated reserved stream
        // slot "__ssj__" so the buffer cannot collide with any real
        // stream's operator list.
        let stream_slot: &str = "__ssj__";
        let join_id_owned = join_id.to_string();
        let within_ms_copy = within_ms;

        let matches: Vec<serde_json::Map<String, serde_json::Value>> = {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(join_key, |entity| {
                entity.get_or_create_stream(stream_slot);
                let stream_state = entity.streams.get_mut(stream_slot).unwrap();
                if !stream_state.operators.iter().any(|(n, _)| *n == join_id_owned) {
                    stream_state.operators.push((
                        join_id_owned.clone(),
                        crate::state::snapshot::OperatorState::StreamJoinBuffer(
                            crate::engine::operators::StreamJoinBuffer::new(within_ms_copy),
                        ),
                    ));
                }
                let buf = stream_state
                    .operators
                    .iter_mut()
                    .find_map(|(n, op)| {
                        if *n != join_id_owned {
                            return None;
                        }
                        match op {
                            crate::state::snapshot::OperatorState::StreamJoinBuffer(b) => {
                                Some(b)
                            }
                            _ => None,
                        }
                    })
                    .expect("StreamJoinBuffer present after get-or-insert");
                let probed = buf.probe(side, event_time_ms);
                buf.insert(side, event_time_ms, arriving_map.clone());
                buf.evict();
                probed
            })
        };

        self.dirty_set.insert(join_key.to_string());
        matches
    }

    /// Phase 57 D-B4 + D-C1 + D-B5 (TPC-CORR-10): apply a retraction to a
    /// downstream row on this shard. Idempotent, depth-guarded, history-ttl-aware.
    ///
    /// Semantics (mirrors the locked design in
    /// `.planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md`):
    ///
    /// 1. **Depth guard (D-B5):** If `depth >= MAX_RETRACTION_DEPTH` return
    ///    `RetractOutcome::DepthExceeded` without touching state. The dispatch
    ///    arm in `thread.rs` also enforces this cap before calling this
    ///    method, so the method-level guard is defence-in-depth for direct
    ///    (non-dispatch) callers such as the same-shard fast path in
    ///    `PipelineEngine::retract_downstream_at_shard`.
    ///
    /// 2. **Idempotency probe (D-B4):** If the entity at `row_key` does not
    ///    exist on this shard, OR the entity has no stream slot named
    ///    `stream_name`, OR the stream slot is already empty of operator
    ///    state (stream was previously tombstoned), return
    ///    `RetractOutcome::NoOp`. No mutation. This covers:
    ///    - Source-shard retries on `ShardOverload` (duplicate dispatch).
    ///    - Fan-out collisions where multiple tombstones affect the same
    ///      downstream row.
    ///    - Retractions against never-emitted rows (race between the
    ///      retraction cascade and a slow primary push).
    ///
    /// 3. **History-ttl probe (D-C1):** If the stream has a
    ///    `last_event_at` AND the observed watermark indicates the event is
    ///    older than `history_ttl` before the watermark, return
    ///    `RetractOutcome::BeyondHistory`. Wave 1 treats this as a soft
    ///    skip — the actual `history_ttl` lookup against the
    ///    `StreamDefinition` lives on `PipelineEngine`, not `Shard`; the
    ///    Wave 4 (plan 57-04) wiring will add a `history_ttl: Duration`
    ///    parameter to this method to make the check live. Today the
    ///    method never returns `BeyondHistory` from the "never-populated"
    ///    path; Wave 4's SC-3 test will exercise the flip.
    ///
    /// 4. **Happy path:** tombstone the row's stream slot by clearing its
    ///    operators (the `StreamEntityState.operators` Vec is the only
    ///    carrier for per-stream retract-visible state in Wave 1 —
    ///    retraction-capable operators land in Waves 2/3) AND clearing
    ///    `contributing_inputs` so a re-retraction probe reads `NoOp`.
    ///    Returns `RetractOutcome::Retracted`. Marks dirty.
    ///
    /// The `reason` parameter is accepted for observability (the dispatch
    /// arm uses it to label metric counters) and for future Wave 2/3
    /// operator-specific retraction logic (e.g. a SourceTableDelete
    /// retraction might clear a source-table-row-backed cache entry; an
    /// EntityTombstone retraction might also clear `static_features`).
    /// Wave 1 treats all three reasons identically — the primitive is what
    /// Wave 2/3 extends.
    pub fn apply_retraction(
        &mut self,
        stream_name: &str,
        row_key: &str,
        _reason: &crate::shard::thread::RetractReason,
        depth: u8,
    ) -> crate::shard::thread::RetractOutcome {
        use crate::shard::thread::{RetractOutcome, MAX_RETRACTION_DEPTH};

        // 1. Depth guard — matches the dispatch-arm check; defence-in-depth.
        if depth >= MAX_RETRACTION_DEPTH {
            return RetractOutcome::DepthExceeded;
        }

        // 2. Idempotency probe: pull the entity's current stream-slot state
        //    WITHOUT mutating. Read through the shared `read_entity_from_shard`
        //    helper so fjall + state-inmem both route through the same code.
        let probe: Option<(bool, bool)> = read_entity_from_shard(self, row_key, |entity| {
            let stream_present = entity.streams.get(stream_name).is_some();
            let stream_has_state = entity
                .streams
                .get(stream_name)
                .map(|s| !s.operators.is_empty())
                .unwrap_or(false);
            (stream_present, stream_has_state)
        });

        let (stream_present, stream_has_state) = match probe {
            Some(t) => t,
            None => return RetractOutcome::NoOp,
        };
        if !stream_present || !stream_has_state {
            // Stream slot absent OR already emptied by a prior retraction —
            // D-B4 idempotent no-op.
            return RetractOutcome::NoOp;
        }

        // 3. History-ttl probe — Wave 1 does NOT have `history_ttl` wired on
        //    the Shard surface (belongs to `StreamDefinition` on PipelineEngine);
        //    Wave 4 lands the live check. This branch is reachable today only
        //    through a future caller that passes a concrete cut-off; keeping
        //    the match-arm existence in the code avoids a signature change
        //    when Wave 4 extends the method.
        //
        //    (No-op in Wave 1; left for Wave 4.)

        // 4. Happy path: tombstone the stream slot.
        {
            let mut view = StoreView::Sharded(self);
            view.with_entity_mut(row_key, |entity| {
                if let Some(s) = entity.streams.get_mut(stream_name) {
                    s.operators.clear();
                    s.last_event_at = None;
                }
                // Clear contributing_inputs so a re-retraction probe reads
                // NoOp. The field is in-memory-only today (Wave 1); Wave 2/3
                // wires the persistence path once operators produce it.
                entity.contributing_inputs = None;
            });
        }
        self.dirty_set.insert(row_key.to_string());
        RetractOutcome::Retracted
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
    use crate::state::snapshot::{SerializableEntityStateV10, SerializableStreamEntityState};
    use crate::state::store::SerializableTableRow;

    // Phase 57-02: write V10 per-entity wire format (adds `contributing_inputs`
    // for TPC-CORR-10 retraction tracking). Readers try V10 first then fall
    // back to V9 (`SerializableEntityState`) on postcard decode failure so
    // Phase 55/56 on-disk bytes continue to load.
    let ser = SerializableEntityStateV10 {
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
        contributing_inputs: entity.contributing_inputs.clone(),
    };
    postcard::to_stdvec(&ser).expect("postcard serialize SerializableEntityStateV10")
}

#[cfg(not(feature = "state-inmem"))]
fn entity_from_bytes(bytes: &[u8]) -> Option<EntityState> {
    use crate::state::snapshot::{SerializableEntityState, SerializableEntityStateV10};
    use crate::state::store::{StreamEntityState, TableRow};

    // Phase 57-02 wire-shim: try V10 first (includes `contributing_inputs`
    // primary_event_id for TPC-CORR-10), fall back to V9
    // (`SerializableEntityState` — Phase 55/56 wire layout without the field).
    // postcard does NOT support `#[serde(default)]` for missing trailing
    // fields so V9-era bytes fail V10 decode. The fallback preserves the
    // pre-Phase-57 "cannot-retract" semantic (D-A5) by mapping to None.
    let (streams_raw, static_features, table_rows_raw, contributing_inputs) =
        match postcard::from_bytes::<SerializableEntityStateV10>(bytes) {
            Ok(v10) => (
                v10.streams,
                v10.static_features,
                v10.table_rows,
                v10.contributing_inputs,
            ),
            Err(_) => {
                let v9: SerializableEntityState = postcard::from_bytes(bytes).ok()?;
                (v9.streams, v9.static_features, v9.table_rows, None)
            }
        };
    let mut streams: ahash::AHashMap<String, StreamEntityState> = ahash::AHashMap::new();
    for (name, s) in streams_raw {
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
        static_features: static_features.into_iter().collect(),
        table_rows: table_rows_raw
            .into_iter()
            .map(|(k, v)| (k, TableRow::from(v)))
            .collect(),
        // Phase 57-02: restore `contributing_inputs` from V10 wire format; V9
        // reads short-circuit to `None` (D-A5 cannot-retract semantic
        // preserved for pre-Phase-57 rows).
        contributing_inputs,
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
/// Phase 54-04 Pass A6a: the `Legacy` variant (DashMap-backed `StateStore`) has
/// been deleted. `Sharded` is the only remaining variant — kept as a
/// single-variant enum to avoid a mechanical rewrite of the ~15 call sites;
/// Pass A6b / Pass B may collapse to a tuple struct when the adjacent
/// `StateStore` struct itself is deleted.
pub enum StoreView<'a> {
    /// N>1 per-shard path. In the default (fjall) build the arm round-trips
    /// through `postcard` + `fjall::PartitionHandle`; in the dev-mode
    /// `state-inmem` build it uses the per-shard AHashMap path.
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

    // -----------------------------------------------------------------
    // Phase 54-02 Task 1: widened surface — the 5 methods StateStore
    // exposed for TT-cascade, SET/MSET static-feature, and dirty-set
    // operations. Phase 54-04 Pass A6a deleted the Legacy arm; all arms
    // now delegate to `Shard` methods above (Sharded).
    // -----------------------------------------------------------------

    /// Phase 54-02: Delete an entity. See the `Shard::delete_entity`
    /// docstring — Sharded uses full-removal semantics. The historical
    /// Legacy arm (alias for `tombstone_static` — keeps entity) was deleted
    /// in Phase 54-04 Pass A6a.
    pub fn delete_entity(&mut self, key: &str) -> bool {
        match self {
            StoreView::Sharded(shard) => shard.delete_entity(key),
        }
    }

    /// Phase 54-02: Clear the entity's static_features. Preserves streams
    /// and table_rows. Returns `true` iff there were static features
    /// before the call.
    pub fn tombstone_static(&mut self, key: &str) -> bool {
        match self {
            StoreView::Sharded(shard) => shard.tombstone_static(key),
        }
    }

    /// Phase 54-02: Upsert a Table row `(key, table_name)` with `fields`
    /// as a fresh Live row at `now`. Marks the key dirty.
    ///
    /// Signature mirrors `StateStore::upsert_table_row` (takes a field
    /// map rather than a prebuilt `TableRow`) so the Task 3 migration of
    /// operators.rs is a textual replacement.
    pub fn upsert_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        fields: ahash::AHashMap<String, crate::types::FeatureValue>,
        now: SystemTime,
    ) {
        match self {
            StoreView::Sharded(shard) => shard.upsert_table_row(key, table_name, fields, now),
        }
    }

    /// Phase 54-02: Tombstone a Table row `(key, table_name)`. Returns
    /// `true` iff a prior Live row existed under this identity.
    pub fn tombstone_table_row(
        &mut self,
        key: &str,
        table_name: &str,
        now: SystemTime,
    ) -> bool {
        match self {
            StoreView::Sharded(shard) => shard.tombstone_table_row(key, table_name, now),
        }
    }

    /// Phase 54-02: Mark the key dirty for the next snapshot cycle. On
    /// the shard path this is a plain `HashSet.insert` — no generation
    /// counter dance because the shard is single-writer (the `dirty_gen`
    /// short-circuit in `StateStore::mark_dirty` existed only to avoid
    /// hot-key contention on the shared `DashSet`).
    pub fn mark_dirty(&mut self, key: &str) {
        match self {
            StoreView::Sharded(shard) => {
                shard.dirty_set.insert(key.to_string());
            }
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

    // ---- Phase 56 Wave 1: read_entity_at + apply_ssj_insert unit tests ----

    /// Helper: build an empty Shard in the default (fjall) build using a
    /// temp partition. Mirrors the shard_state_approximate_len pattern.
    #[cfg(not(feature = "state-inmem"))]
    fn build_empty_shard_fjall() -> (super::Shard, tempfile::TempDir) {
        use crate::shard::fjall_backend::{
            fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
        };
        std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
        std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let cfg = fjall_config_from_env(1);
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");
        (super::Shard::with_partition(partition), tmp)
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn read_entity_at_returns_none_on_missing() {
        // Phase 56 D-A1: Shard::read_entity_at returns None for an
        // absent key — caller bumps enrich_missing_total.
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (shard, _tmp) = build_empty_shard_fjall();
        assert!(shard.read_entity_at("Countries", "XX").is_none());
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn read_entity_at_returns_some_after_upsert() {
        // Phase 56 D-A1: after upsert_source_table_row the row is
        // readable via read_entity_at (the entity exists and contains
        // the table row; caller pulls fields from returned EntityState).
        use crate::types::FeatureValue;
        use ahash::AHashMap;
        use std::sync::{Mutex, OnceLock};
        use std::time::SystemTime;
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (mut shard, _tmp) = build_empty_shard_fjall();
        let mut f: AHashMap<String, FeatureValue> = AHashMap::new();
        f.insert("gdp_usd".into(), FeatureValue::Int(800_000));
        shard.upsert_source_table_row("CH", "Countries", f, 1, SystemTime::now());
        let got = shard.read_entity_at("Countries", "CH");
        assert!(got.is_some());
        let got = got.unwrap();
        let row = got.table_rows.get("Countries").expect("Countries row present");
        assert_eq!(
            row.fields.get("gdp_usd"),
            Some(&FeatureValue::Int(800_000))
        );
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_ssj_insert_first_side_returns_empty_matches() {
        // Phase 56 D-B1: first insert on an empty buffer — no matches.
        use serde_json::json;
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (mut shard, _tmp) = build_empty_shard_fjall();
        let ev = json!({"user_id": "u1", "payload": "L", "_event_time": 1_000_000_u64});
        let matches = shard.apply_ssj_insert(
            "j1",
            crate::engine::operators::JoinSide::Left,
            "u1",
            ev,
            60_000,
        );
        assert!(matches.is_empty());
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_ssj_insert_second_side_returns_prior_counterparty() {
        // Phase 56 D-B1: after inserting a Left event, a Right insert
        // at the same join_key within the window returns the Left
        // event as a match.
        use serde_json::json;
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (mut shard, _tmp) = build_empty_shard_fjall();
        // Identical event_time (`_event_time` in milliseconds since epoch)
        // — well within a 60_000 ms window.
        let left_ev = json!({"user_id": "u1", "payload": "L", "_event_time": 1_700_000_000_000_u64});
        let right_ev = json!({"user_id": "u1", "payload": "R", "_event_time": 1_700_000_000_000_u64});
        let _ = shard.apply_ssj_insert(
            "j1",
            crate::engine::operators::JoinSide::Left,
            "u1",
            left_ev,
            60_000,
        );
        let matches = shard.apply_ssj_insert(
            "j1",
            crate::engine::operators::JoinSide::Right,
            "u1",
            right_ev,
            60_000,
        );
        assert_eq!(matches.len(), 1, "Right insert sees prior Left");
        assert_eq!(
            matches[0].get("payload").and_then(|v| v.as_str()),
            Some("L"),
            "Matched counterparty payload is from the Left side"
        );
    }

    // ---- Phase 57 Wave 1: apply_retraction unit tests ----

    /// Helper: seed a stream slot on `shard` at `row_key` with a dummy
    /// CountOp so subsequent apply_retraction calls see a non-empty
    /// operator list. Mirrors the minimal live-state shape Wave 2/3
    /// emitters will populate.
    #[cfg(not(feature = "state-inmem"))]
    fn seed_stream_row(shard: &mut super::Shard, stream: &str, row_key: &str) {
        use crate::engine::operators::CountOp;
        use crate::state::snapshot::OperatorState;
        use crate::state::store::StreamEntityState;
        use std::time::{Duration, SystemTime};

        let mut view = super::StoreView::Sharded(shard);
        view.with_entity_mut(row_key, |entity| {
            let stream_state = entity
                .streams
                .entry(stream.to_string())
                .or_insert_with(StreamEntityState::default);
            stream_state.operators.push((
                "cnt".to_string(),
                OperatorState::Count(CountOp::new(
                    Duration::from_secs(3600),
                    Duration::from_secs(60),
                )),
            ));
            stream_state.last_event_at = Some(SystemTime::now());
        });
        shard.dirty_set.insert(row_key.to_string());
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_retraction_noop_on_missing_row() {
        // Phase 57 D-B4: retracting a row that was never emitted on this
        // shard returns NoOp without touching state. Necessary for source
        // retries + fan-out collisions.
        use crate::shard::thread::{RetractOutcome, RetractReason};
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        let (mut shard, _tmp) = build_empty_shard_fjall();
        let out = shard.apply_retraction(
            "EnrichedSnap",
            "absent_key",
            &RetractReason::EntityTombstone {
                stream_name: "Primary".into(),
                entity_key: "u1".into(),
            },
            0,
        );
        assert_eq!(out, RetractOutcome::NoOp);
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_retraction_depth_guard_trips_at_cap() {
        // Phase 57 D-B5: depth >= MAX_RETRACTION_DEPTH returns DepthExceeded
        // without touching state. Defence-in-depth for direct (non-dispatch)
        // callers such as the same-shard fast path in
        // PipelineEngine::retract_downstream_at_shard.
        use crate::shard::thread::{RetractOutcome, RetractReason, MAX_RETRACTION_DEPTH};
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        let (mut shard, _tmp) = build_empty_shard_fjall();
        // Seed a live row so we can verify the guard trips BEFORE the
        // idempotency probe (state must remain untouched).
        seed_stream_row(&mut shard, "EnrichedSnap", "u1");

        let out = shard.apply_retraction(
            "EnrichedSnap",
            "u1",
            &RetractReason::EntityTombstone {
                stream_name: "Primary".into(),
                entity_key: "u1".into(),
            },
            MAX_RETRACTION_DEPTH,
        );
        assert_eq!(out, RetractOutcome::DepthExceeded);

        // Verify state unchanged — stream slot still has its operator.
        let still_live = super::read_entity_from_shard(&shard, "u1", |entity| {
            entity
                .streams
                .get("EnrichedSnap")
                .map(|s| !s.operators.is_empty())
                .unwrap_or(false)
        })
        .unwrap_or(false);
        assert!(still_live, "DepthExceeded must not mutate state");
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_retraction_happy_path_returns_retracted() {
        // Phase 57 happy path (D-B1): a live row is tombstoned — stream
        // slot is emptied, contributing_inputs cleared, returns Retracted.
        use crate::shard::thread::{RetractOutcome, RetractReason};
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        let (mut shard, _tmp) = build_empty_shard_fjall();
        seed_stream_row(&mut shard, "EnrichedSnap", "u1");

        let out = shard.apply_retraction(
            "EnrichedSnap",
            "u1",
            &RetractReason::SourceTableDelete {
                table_name: "Countries".into(),
                table_key: "US".into(),
                source_lsn: 42,
            },
            5,
        );
        assert_eq!(out, RetractOutcome::Retracted);

        // Verify: stream slot is now empty of operators → next retraction
        // against this same row is a NoOp (idempotent).
        let now_empty = super::read_entity_from_shard(&shard, "u1", |entity| {
            entity
                .streams
                .get("EnrichedSnap")
                .map(|s| s.operators.is_empty())
                .unwrap_or(true)
        })
        .unwrap_or(true);
        assert!(now_empty, "Retracted must empty the stream slot's operators");
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_retraction_is_idempotent_on_second_call() {
        // Phase 57 D-B4: re-retracting the same row after a successful
        // retraction is a NoOp. Source shards may retry on
        // ShardOverload, so this invariant is load-bearing.
        use crate::shard::thread::{RetractOutcome, RetractReason};
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        let (mut shard, _tmp) = build_empty_shard_fjall();
        seed_stream_row(&mut shard, "EnrichedSnap", "u1");

        let r1 = shard.apply_retraction(
            "EnrichedSnap",
            "u1",
            &RetractReason::PrimaryEventRetract {
                stream_name: "Primary".into(),
                event_id: 123,
            },
            0,
        );
        assert_eq!(r1, RetractOutcome::Retracted);

        let r2 = shard.apply_retraction(
            "EnrichedSnap",
            "u1",
            &RetractReason::PrimaryEventRetract {
                stream_name: "Primary".into(),
                event_id: 123,
            },
            0,
        );
        assert_eq!(
            r2,
            RetractOutcome::NoOp,
            "second retraction on same row must be NoOp"
        );
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_retraction_noop_on_unknown_stream_slot() {
        // Phase 57 D-B4: a retraction against a stream the row never
        // emitted into is a NoOp (cross-join fan-out collision case).
        use crate::shard::thread::{RetractOutcome, RetractReason};
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();

        let (mut shard, _tmp) = build_empty_shard_fjall();
        // Seed row with "StreamA" present; retract against "StreamB".
        seed_stream_row(&mut shard, "StreamA", "u1");

        let out = shard.apply_retraction(
            "StreamB",
            "u1",
            &RetractReason::EntityTombstone {
                stream_name: "StreamA".into(),
                entity_key: "u1".into(),
            },
            0,
        );
        assert_eq!(out, RetractOutcome::NoOp);
    }

    #[test]
    fn retract_reason_postcard_roundtrip() {
        // Phase 57: RetractReason must survive postcard round-trip — future
        // dispatch paths (cross-process / replica) rely on this wire format.
        use crate::shard::thread::RetractReason;

        let a = RetractReason::SourceTableDelete {
            table_name: "Countries".into(),
            table_key: "US".into(),
            source_lsn: 42,
        };
        let b = RetractReason::EntityTombstone {
            stream_name: "Primary".into(),
            entity_key: "u1".into(),
        };
        let c = RetractReason::PrimaryEventRetract {
            stream_name: "Primary".into(),
            event_id: 17,
        };

        for r in [&a, &b, &c] {
            let bytes = postcard::to_stdvec(r).expect("serialize");
            let restored: RetractReason =
                postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(&restored, r, "round-trip preserves variant");
        }
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn entity_state_v10_postcard_roundtrip_with_contributing_inputs() {
        // Phase 57-02 (TPC-CORR-10): EntityState with populated
        // `contributing_inputs` must survive the fjall per-entity wire round
        // trip so downstream rows retain their `primary_event_id` across
        // restart. V10 is the new wire format; V9 fallback is exercised by
        // `entity_state_v9_bytes_load_as_none_contributing_inputs` below.
        use crate::state::store::{ContribSet, EntityState};
        use std::sync::atomic::AtomicU64;

        let mut entity = EntityState {
            streams: ahash::AHashMap::new(),
            static_features: ahash::AHashMap::new(),
            table_rows: ahash::AHashMap::new(),
            contributing_inputs: Some(ContribSet {
                primary_event_id: Some(0x1234_5678_9abc_def0),
                source_table_keys: vec!["US".into(), "CA".into()],
                left_event_id: Some(0x1111_2222_3333_4444),
                right_event_id: Some(0x5555_6666_7777_8888),
            }),
            dirty_gen: AtomicU64::new(0),
        };
        // Add a stream so table_rows / streams aren't both empty (realistic
        // payload).
        entity.streams.insert(
            "Primary".to_string(),
            crate::state::store::StreamEntityState {
                operators: vec![],
                last_event_at: None,
            },
        );

        let bytes = super::entity_to_bytes(&entity);
        let restored =
            super::entity_from_bytes(&bytes).expect("v10 bytes must decode");
        let ci = restored
            .contributing_inputs
            .expect("contributing_inputs must survive round-trip");
        assert_eq!(ci.primary_event_id, Some(0x1234_5678_9abc_def0));
        assert_eq!(ci.source_table_keys, vec!["US".to_string(), "CA".to_string()]);
        assert_eq!(ci.left_event_id, Some(0x1111_2222_3333_4444));
        assert_eq!(ci.right_event_id, Some(0x5555_6666_7777_8888));
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn entity_state_v9_bytes_load_as_none_contributing_inputs() {
        // Phase 57-02: V9 on-disk bytes (emitted by Phase 55/56 binaries
        // without the `contributing_inputs` field) must load under the V10
        // decoder by falling through to the `SerializableEntityState` legacy
        // path, yielding `contributing_inputs = None` (D-A5 "cannot-retract"
        // semantic). This guards backward compat.
        use crate::state::snapshot::{SerializableEntityState, SerializableStreamEntityState};

        let v9 = SerializableEntityState {
            streams: vec![(
                "Primary".to_string(),
                SerializableStreamEntityState {
                    operators: vec![],
                    last_event_at: None,
                },
            )],
            static_features: vec![],
            table_rows: vec![],
        };
        let v9_bytes = postcard::to_stdvec(&v9).expect("v9 encode");
        let restored = super::entity_from_bytes(&v9_bytes)
            .expect("v9 bytes must decode under v10 reader via fallback");
        assert!(
            restored.contributing_inputs.is_none(),
            "pre-Phase-57 rows load with contributing_inputs = None (D-A5)"
        );
        assert!(
            restored.streams.contains_key("Primary"),
            "stream slot preserved via V9 fallback"
        );
    }

    #[cfg(not(feature = "state-inmem"))]
    #[test]
    fn apply_ssj_insert_rejects_non_object_event() {
        // Phase 56 T-56-01-02: non-object event (malformed source) is
        // silently skipped — returns empty matches, no insertion.
        use serde_json::json;
        use std::sync::{Mutex, OnceLock};
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _g = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let (mut shard, _tmp) = build_empty_shard_fjall();
        // bare string, not an object
        let ev = json!("not-an-object");
        let matches = shard.apply_ssj_insert(
            "j1",
            crate::engine::operators::JoinSide::Left,
            "u1",
            ev,
            60_000,
        );
        assert!(matches.is_empty());
        // Confirm nothing was inserted: a subsequent Right insert at
        // same join_key returns empty matches.
        let right_ev =
            json!({"user_id": "u1", "payload": "R", "_event_time": 1_700_000_000_000_u64});
        let matches2 = shard.apply_ssj_insert(
            "j1",
            crate::engine::operators::JoinSide::Right,
            "u1",
            right_ev,
            60_000,
        );
        assert!(matches2.is_empty(), "Nothing was buffered on the Left side");
    }
}
