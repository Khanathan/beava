# Phase 9: Incremental Snapshots - Research

**Researched:** 2026-04-09
**Domain:** Snapshot persistence, dirty tracking, delta serialization (Rust)
**Confidence:** HIGH

## Summary

Phase 9 converts the current full-state snapshot system into an incremental one where only changed entities are serialized per cycle. The current implementation (`save_snapshot` in `snapshot.rs`) serializes ALL entities into a single `SnapshotState` blob using postcard every 30 seconds. At scale (1M keys), this becomes the bottleneck -- the CLAUDE.md target is snapshot write < 1 second for 1M keys. Incremental snapshots make write time proportional to the change rate, not total state size.

The core design follows the Redis 7.0+ multi-part AOF pattern: a **base snapshot** (full state, written periodically) plus **delta snapshots** (only changed entities since last snapshot). Recovery loads the most recent base, then applies deltas in order. A periodic full snapshot (every Nth cycle) bounds recovery time and allows old delta files to be cleaned up.

**Primary recommendation:** Add a dirty-key tracking `HashSet<EntityKey>` to `StateStore`, mark keys dirty on every mutation (PUSH, SET, MSET, backfill), serialize only dirty keys as delta files. Bump snapshot format to v6. Full snapshots every 10th cycle (configurable). Recovery: load base + apply deltas in sequence order.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
None -- all implementation choices at Claude's discretion per CONTEXT.md.

### Claude's Discretion
All implementation choices are at Claude's discretion -- pure infrastructure phase. Use ROADMAP phase goal, success criteria, and codebase conventions to guide decisions.

### Deferred Ideas (OUT OF SCOPE)
None -- discuss phase skipped.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OPS-03 | Incremental snapshot serialization only writes changed entities since last snapshot | Dirty-key tracking in StateStore + delta snapshot file format (see Architecture Patterns) |
| OPS-04 | Snapshot restore handles incremental format (base + deltas) | Multi-file recovery protocol: load base, apply deltas in sequence (see Recovery Pattern) |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Serialization:** Postcard (not bincode) for all serialization -- locked decision [VERIFIED: Cargo.toml + snapshot.rs]
- **Hash maps:** AHashMap (not std HashMap) -- locked decision [VERIFIED: store.rs]
- **Threading:** Single-threaded tokio runtime (`current_thread`) [VERIFIED: main.rs]
- **Snapshot timing:** 30-second periodic interval [VERIFIED: main.rs line 179]
- **Atomic writes:** tmp file + rename pattern [VERIFIED: main.rs lines 228-229]
- **Cooperative yielding:** Snapshot serialization on `spawn_blocking` thread pool [VERIFIED: main.rs line 224]
- **TDD / Contract-First:** Define contracts and write tests before implementation [VERIFIED: memory MEMORY.md]
- **Format versioning:** Version byte prefix, current v5 [VERIFIED: snapshot.rs line 21]

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| postcard | 1.1.3 | Binary serialization for snapshots | Project locked decision, already in use [VERIFIED: Cargo.lock] |
| ahash | 0.8 | Fast HashMap for dirty-key tracking set | Already used throughout codebase [VERIFIED: Cargo.toml] |
| serde | 1.0 | Derive Serialize/Deserialize | Already used for all state types [VERIFIED: Cargo.toml] |
| tempfile | 3 | Temp directory for atomic writes in tests | Already in dev-dependencies [VERIFIED: Cargo.toml] |

### Supporting
No new dependencies required. This phase uses only existing crates.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Custom dirty HashSet | Bitmap per entity | More memory-efficient at >100K keys but added complexity; HashSet is simpler and matches existing patterns |
| Separate delta files per cycle | Append to single file | Append risks corruption propagation; separate files allow atomic write per delta and simple cleanup |
| LZ4 compression on deltas | No compression | Compression adds latency to hot path; postcard is already compact; defer compression to future optimization |

## Architecture Patterns

### Recommended Changes to Existing Structure
```
src/state/
  snapshot.rs       # Extended: DeltaSnapshotState, IncrementalSnapshotState enums, load/save delta functions
  store.rs          # Extended: dirty_keys HashSet + mark_dirty() + clear_dirty() + clone_dirty_for_snapshot()
```

No new files needed. All changes are extensions to existing modules.

### Pattern 1: Dirty-Key Tracking in StateStore
**What:** A `HashSet<EntityKey>` field on `StateStore` that tracks which entity keys have been modified since the last snapshot.
**When to use:** Every mutation to entity state (PUSH, SET, MSET, backfill replay) calls `mark_dirty(key)`.
**Why not at the caller level:** Centralizing in StateStore guarantees no mutation path forgets to mark dirty. Every path that calls `get_or_create_entity()`, `set_static()`, or modifies operators already goes through StateStore.

```rust
// Source: Design recommendation based on existing store.rs patterns
pub struct StateStore {
    entities: AHashMap<EntityKey, EntityState>,
    dirty_keys: AHashSet<EntityKey>,  // NEW: tracks changed entities
}

impl StateStore {
    /// Mark an entity key as dirty (changed since last snapshot).
    pub fn mark_dirty(&mut self, key: &str) {
        self.dirty_keys.insert(key.to_string());
    }

    /// Clear the dirty set (called after successful snapshot write).
    pub fn clear_dirty(&mut self) {
        self.dirty_keys.clear();
    }

    /// Clone only dirty entities for delta snapshot, with GC.
    pub fn clone_dirty_for_snapshot_with_gc(
        &self,
        valid_features: &AHashMap<String, Vec<String>>,
    ) -> Vec<(String, SerializableEntityState)> {
        self.entities.iter()
            .filter(|(key, _)| self.dirty_keys.contains(key.as_str()))
            .map(|(key, entity)| {
                // Same GC logic as clone_for_snapshot_with_gc
                // ...
            })
            .collect()
    }
}
```

### Pattern 2: Snapshot File Naming Convention
**What:** Base snapshots and delta snapshots use a naming convention with sequence numbers.
**Format:** `tally.snapshot.base.{seq}` for full snapshots, `tally.snapshot.delta.{seq}` for incremental deltas.
**Sequence number:** Monotonically increasing u64, stored in the snapshot header. Ensures deterministic ordering during recovery.

```rust
// Source: Design recommendation
// File naming examples:
// tally.snapshot.base.0000000010   <- full snapshot at cycle 10
// tally.snapshot.delta.0000000011  <- delta at cycle 11
// tally.snapshot.delta.0000000012  <- delta at cycle 12
// ...
// tally.snapshot.base.0000000020   <- full snapshot at cycle 20 (deltas 11-19 can be cleaned)
```

### Pattern 3: Delta Snapshot Format (v6)
**What:** A new snapshot format type that contains only changed entities since last snapshot, plus a reference to the base sequence number.
**Key design:** Use a type byte after the version byte to distinguish base vs delta.

```rust
// Source: Design recommendation extending existing snapshot.rs patterns
const SNAPSHOT_FORMAT_VERSION: u8 = 6;

/// Type discriminator: base (full) or delta (incremental)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotType {
    Base,
    Delta { base_seq: u64 },
}

/// Header present in all v6 snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub snapshot_type: SnapshotType,
    pub sequence: u64,
}

/// Full snapshot state (same as current SnapshotState but with header)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseSnapshotState {
    pub header: SnapshotHeader,
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
    pub backfill_complete: Vec<(String, String)>,
    pub deleted_keys: Vec<String>,  // Keys evicted since last base
}

/// Delta snapshot: only changed + deleted entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaSnapshotState {
    pub header: SnapshotHeader,
    pub changed_entities: Vec<(String, SerializableEntityState)>,
    pub deleted_keys: Vec<String>,
}
```

### Pattern 4: Recovery Protocol (OPS-04)
**What:** On startup, find the latest base snapshot, then apply all deltas with higher sequence numbers in order.
**Steps:**
1. Scan snapshot directory for all snapshot files
2. Find the latest base snapshot (highest sequence with `SnapshotType::Base`)
3. Load and restore the base snapshot (same as current `restore_from_snapshot`)
4. Find all delta snapshots with sequence > base sequence
5. Sort deltas by sequence number ascending
6. For each delta: merge `changed_entities` into store (overwrite existing), remove `deleted_keys`
7. Re-register pipelines from base snapshot (pipelines only stored in base)

```rust
// Source: Design recommendation
impl StateStore {
    /// Apply a delta snapshot on top of existing state.
    /// For each changed entity: replace the entire EntityState.
    /// For each deleted key: remove from store.
    pub fn apply_delta(&mut self, delta: DeltaSnapshotState) {
        for key in delta.deleted_keys {
            self.entities.remove(&key);
        }
        for (key, serializable_state) in delta.changed_entities {
            // Convert SerializableEntityState -> EntityState (same as restore_from_snapshot)
            let mut streams = AHashMap::new();
            for (stream_name, stream_state) in serializable_state.streams {
                streams.insert(stream_name, StreamEntityState {
                    operators: stream_state.operators,
                    last_event_at: stream_state.last_event_at,
                });
            }
            let entity = EntityState {
                streams,
                static_features: serializable_state.static_features.into_iter().collect(),
            };
            self.entities.insert(key, entity);
        }
    }
}
```

### Pattern 5: Periodic Full Snapshot Cycle
**What:** Every Nth snapshot cycle writes a full base instead of a delta.
**Default N:** 10 (configurable via `TALLY_FULL_SNAPSHOT_INTERVAL` env var).
**Rationale:** With 30s snapshot interval and N=10, a full base is written every 5 minutes. Recovery requires at most 9 deltas. Old base + deltas before the latest base are cleaned up.

### Pattern 6: Deleted Key Tracking
**What:** When keys are evicted (TTL expiry), they must appear in `deleted_keys` of the next delta so recovery can remove them from the base state.
**Implementation:** A separate `deleted_keys: AHashSet<EntityKey>` on StateStore, populated by eviction. Cleared after snapshot write alongside dirty_keys.

```rust
// Source: Design recommendation
pub struct StateStore {
    entities: AHashMap<EntityKey, EntityState>,
    dirty_keys: AHashSet<EntityKey>,
    deleted_keys: AHashSet<EntityKey>,  // Keys deleted since last snapshot
}
```

### Anti-Patterns to Avoid
- **Per-field deltas:** Tracking dirtiness at the individual operator level adds massive complexity. Entity-level granularity is sufficient -- entities are small (< 5KB each).
- **Appending to a single file:** Corruption in the middle of the file corrupts everything after it. Separate files with atomic rename are safer.
- **Skipping deleted key tracking:** If a key is evicted between snapshots but not recorded, recovery will resurrect it from the base. This is a correctness bug.
- **Clearing dirty set before confirming write:** If the snapshot write fails after clearing dirty, those changes are lost. Clear dirty only AFTER successful write + rename.
- **Storing pipelines in every delta:** Pipelines change rarely (registration events). Store them only in base snapshots. Deltas reference the base for pipeline state.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Serialization format | Custom binary encoding | Postcard + serde derive | Locked decision; postcard handles varint, versioning already proven in v1-v5 |
| Atomic file writes | Manual fsync/rename | Existing tmp + rename pattern | Already battle-tested in main.rs/http.rs |
| Hash sets for tracking | Custom bitmap | AHashSet | Matches existing patterns, O(1) insert/clear, bounded by entity count |

**Key insight:** The entire incremental snapshot system is a bookkeeping layer on top of the existing serialization infrastructure. No new serialization approaches needed -- just track what changed and serialize only those entities.

## Common Pitfalls

### Pitfall 1: Dirty Set Race with Snapshot Clone
**What goes wrong:** New events modify entities and mark them dirty while the snapshot is being cloned. The dirty set gets cleared but the snapshot doesn't contain those late-arriving changes.
**Why it happens:** The Mutex is released between cloning entities and clearing the dirty set, or new events arrive during the clone.
**How to avoid:** Clone dirty entities AND swap/clear the dirty set in a single lock acquisition. The pattern is: lock -> clone dirty entities -> move dirty_keys to empty set -> unlock -> serialize (on blocking thread) -> on success, no further action needed. On failure, the next cycle will re-include those keys since they'll be re-dirtied by events or still present.
**Warning signs:** Entities appear to "lose" recent state after restart.

### Pitfall 2: Deleted Keys Not Tracked Through Eviction
**What goes wrong:** Entity evicted by TTL between two snapshots. Delta doesn't record the deletion. On recovery, entity is resurrected from base snapshot.
**Why it happens:** Eviction code in `eviction.rs` removes from HashMap but doesn't notify dirty/deleted tracking.
**How to avoid:** Wire eviction to call `store.mark_deleted(key)` before removing. The deleted_keys set captures these for the next delta.
**Warning signs:** "Ghost" entities reappear after restart that should have been evicted.

### Pitfall 3: Sequence Number Gaps or Reuse
**What goes wrong:** If sequence numbers aren't monotonic (e.g., due to failed writes), recovery applies deltas in wrong order or skips deltas.
**Why it happens:** Sequence counter incremented before confirming successful write.
**How to avoid:** Store the sequence counter in AppState (not on disk). Increment after successful write. On startup, derive next sequence from the highest file sequence found + 1.
**Warning signs:** State inconsistencies after recovery, especially missing entities.

### Pitfall 4: Unbounded Delta Accumulation
**What goes wrong:** If full snapshots fail repeatedly, deltas accumulate without bound, making recovery slow and consuming disk.
**Why it happens:** Full snapshot write fails (disk full, serialization error) but deltas keep writing.
**How to avoid:** Track the number of deltas since last successful base. If it exceeds 2x the full_snapshot_interval, force a full snapshot attempt. Log warnings when deltas accumulate.
**Warning signs:** Recovery time increases over days; snapshot directory grows unbounded.

### Pitfall 5: Backward Compatibility with v5 Snapshots
**What goes wrong:** Upgrading from v5 to v6 format causes data loss if old single-file snapshots are not handled.
**Why it happens:** v6 format expects multi-file naming convention; old `tally.snapshot` file is ignored.
**How to avoid:** On startup, check for legacy `tally.snapshot` single file first. If found and no v6 base exists, treat it as the initial base snapshot (load it, then write a v6 base on first cycle). Bump to v6 format on first write.
**Warning signs:** Server starts fresh despite having a valid v5 snapshot on disk.

## Code Examples

### Snapshot Timer Integration (main.rs pattern)
```rust
// Source: Design extending existing main.rs snapshot timer pattern
// In the periodic snapshot task:
let snapshot_data = {
    let mut app = snap_state.lock().unwrap_or_else(|e| e.into_inner());
    let cycle = app.snapshot_cycle;
    app.snapshot_cycle += 1;
    let is_full = cycle % full_snapshot_interval == 0;

    let valid_features = app.engine.valid_features_map();

    if is_full {
        // Full base snapshot -- clone everything
        let entities = app.store.clone_for_snapshot_with_gc(&valid_features);
        let pipelines = /* same as current */;
        let backfill_complete = app.backfill_complete.iter().cloned().collect();
        app.store.clear_dirty();
        app.store.clear_deleted();
        SnapshotData::Base(BaseSnapshotState { /* ... */ })
    } else {
        // Delta -- clone only dirty entities
        let changed = app.store.clone_dirty_for_snapshot_with_gc(&valid_features);
        let deleted: Vec<String> = app.store.take_deleted();
        app.store.clear_dirty();
        SnapshotData::Delta(DeltaSnapshotState { changed_entities: changed, deleted_keys: deleted, /* ... */ })
    }
};
```

### Mark Dirty on PUSH (wiring example)
```rust
// Source: Design recommendation -- where to call mark_dirty
// In the engine.push() flow or in the TCP command handler after push:
// The cleanest place is in StateStore.get_or_create_entity():
pub fn get_or_create_entity(&mut self, key: &str) -> &mut EntityState {
    self.dirty_keys.insert(key.to_string());  // Always mark dirty on access-for-write
    self.entities
        .entry(key.to_string())
        .or_insert_with(EntityState::new)
}
```

### Recovery on Startup (load_incremental)
```rust
// Source: Design recommendation extending main.rs startup pattern
fn load_incremental_snapshot(snapshot_dir: &Path) -> Option<(SnapshotState, u64)> {
    // 1. Check for legacy v5 single-file snapshot
    let legacy_path = snapshot_dir.join("tally.snapshot");
    // 2. Scan for base files, find latest
    // 3. Load base
    // 4. Scan for delta files with seq > base_seq
    // 5. Sort by sequence, apply in order
    // 6. Return merged state + next_sequence
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Full snapshot every cycle (current v5) | Incremental: base + deltas (v6) | Phase 9 | Write time proportional to change rate, not total state size |
| Single snapshot file | Multi-file with naming convention | Phase 9 | Enables incremental + cleanup strategy |
| No dirty tracking | HashSet-based dirty key tracking | Phase 9 | Required for knowing which entities changed |

**Not deprecated:**
- The SnapshotState struct remains as the base format. It is extended, not replaced.
- Postcard serialization is unchanged. Only the data passed to it changes.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Full snapshot every 10th cycle is sufficient for bounding recovery time | Pattern 5 | If too infrequent, recovery time grows; if too frequent, negates incremental benefit. Configurable via env var mitigates this. |
| A2 | Entity-level dirty tracking (not field-level) provides sufficient granularity | Pattern 1 | If individual entities are very large, entity-level granularity wastes bytes. Per CLAUDE.md, entities are < 5KB, so this is fine. |
| A3 | Separate files (not appending to one) is the right approach | Pattern 2 | If thousands of delta files accumulate, filesystem overhead increases. Cleanup after each full snapshot mitigates this. |
| A4 | Pipelines need only be stored in base snapshots | Anti-Patterns | If a pipeline is registered between two base snapshots and server crashes before next base, the pipeline is lost from the snapshot. However, the event log still has events -- users would re-register the pipeline. LOW risk. |

## Open Questions

1. **Where exactly to call mark_dirty?**
   - What we know: Mutations happen via `get_or_create_entity()`, `set_static()`, and directly on `EntityState` references returned from the store.
   - What's unclear: Whether marking dirty inside `get_or_create_entity()` is sufficient (it would mark entities dirty even on read-then-no-write paths like GET).
   - Recommendation: Mark dirty at the call sites that actually mutate (PUSH handler, SET handler, MSET handler, backfill). This avoids false positives. The planner should enumerate all mutation call sites.

2. **Should pipelines also be stored in delta snapshots?**
   - What we know: Pipelines rarely change (only on REGISTER). Current code stores them in every full snapshot.
   - What's unclear: If a pipeline is registered between base snapshots and server crashes, it's lost from snapshot (though event log preserves events).
   - Recommendation: Store pipelines in base snapshots only. Pipeline registration is idempotent (SDK re-registers on connect). Document this as an accepted limitation.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test + cargo test |
| Config file | Cargo.toml (standard) |
| Quick run command | `cargo test --lib state::snapshot -- --nocapture` |
| Full suite command | `cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OPS-03 | Delta snapshot contains only changed entities | unit | `cargo test --lib state::snapshot::tests::test_delta_snapshot_contains_only_dirty -x` | Wave 0 |
| OPS-03 | Dirty keys tracked on PUSH/SET/MSET | unit | `cargo test --lib state::store::tests::test_dirty_tracking -x` | Wave 0 |
| OPS-03 | Dirty set cleared after successful snapshot | unit | `cargo test --lib state::store::tests::test_clear_dirty -x` | Wave 0 |
| OPS-04 | Recovery from base + deltas restores full state | unit | `cargo test --lib state::snapshot::tests::test_incremental_recovery -x` | Wave 0 |
| OPS-04 | Recovery handles deleted keys in deltas | unit | `cargo test --lib state::snapshot::tests::test_delta_deleted_keys_recovery -x` | Wave 0 |
| OPS-04 | Legacy v5 snapshot loaded as initial base | unit | `cargo test --lib state::snapshot::tests::test_v5_migration -x` | Wave 0 |
| OPS-03 | Full snapshot written every Nth cycle | integration | `cargo test --test test_snapshot::test_full_snapshot_cycle -x` | Wave 0 |
| OPS-04 | End-to-end: push events, snapshot, recover, verify features | integration | `cargo test --test test_snapshot::test_incremental_snapshot_e2e -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib state::snapshot state::store -- --nocapture`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] Delta snapshot serialization/deserialization tests (unit)
- [ ] Dirty key tracking tests in store.rs (unit)
- [ ] Incremental recovery tests (unit + integration)
- [ ] Legacy v5 migration test (unit)
- [ ] End-to-end incremental snapshot integration test

## Security Domain

Not applicable for this phase. Incremental snapshots are an internal persistence mechanism with no external attack surface. No new network endpoints, no new input parsing, no authentication changes.

## Sources

### Primary (HIGH confidence)
- `/Users/petrpan26/work/tally/src/state/snapshot.rs` -- Current snapshot format v5, SnapshotState, save/load functions
- `/Users/petrpan26/work/tally/src/state/store.rs` -- StateStore, EntityState, clone_for_snapshot methods
- `/Users/petrpan26/work/tally/src/main.rs` -- Periodic snapshot timer, startup recovery, atomic write pattern
- `/Users/petrpan26/work/tally/src/server/http.rs` -- Manual snapshot trigger endpoint
- `/Users/petrpan26/work/tally/Cargo.toml` -- postcard 1.1, ahash 0.8 versions confirmed
- `/Users/petrpan26/work/tally/Cargo.lock` -- postcard 1.1.3 exact version confirmed

### Secondary (MEDIUM confidence)
- [Redis persistence docs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) -- Multi-part AOF pattern (base + incremental files) as design inspiration
- [Memgraph data durability](https://memgraph.com/docs/fundamentals/data-durability) -- Delta snapshot pattern in graph databases

### Tertiary (LOW confidence)
- None -- all key design decisions grounded in codebase analysis and established patterns

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all crates already in use
- Architecture: HIGH -- straightforward extension of existing snapshot infrastructure with well-understood dirty-tracking pattern
- Pitfalls: HIGH -- identified from direct codebase analysis of mutation paths and recovery flow

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable domain, no external dependency changes expected)
