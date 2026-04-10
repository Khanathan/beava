# Phase 8: Backfill & Schema Evolution - Research

**Researched:** 2026-04-09
**Domain:** Schema evolution, cooperative backfill replay, streaming operator lifecycle
**Confidence:** HIGH

## Summary

This phase adds the ability to evolve stream definitions at runtime (adding and removing features without state reset) and to backfill new features by replaying historical events from the SSD event log. The core challenge is implementing schema diff on re-registration, lazy garbage collection of removed features, and cooperative backfill replay that uses event timestamps for determinism while not starving live traffic.

The codebase is well-prepared for this work. The `PipelineEngine::register()` currently replaces definitions wholesale on re-register. The `create_operator()` factory, the `EventLog::read_entries()` reader, and the MSET cooperative yielding pattern provide all the building blocks. The key architectural insight is that backfill operates on disjoint feature sets from live traffic (new features only), so there is no conflict -- the single-threaded model guarantees no races as long as we yield cooperatively.

**Primary recommendation:** Implement schema diff as a pure comparison of feature name sets in the existing `register()` method, lazy-mark removed features for snapshot GC, and run backfill as a tokio task that acquires the lock in 64-event chunks with `yield_now()` between chunks -- identical to the MSET pattern.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Diff old vs new FeatureDef lists by name -- compare registered stream's features against incoming definition, classify as added/removed/unchanged
- Removed features cleaned up lazily on next snapshot -- mark removed, stop computing, GC during snapshot serialization (no hot-path cost)
- Reject type changes -- return error on re-register if existing feature name has different operator type (user must remove+add with new name)
- Atomic swap on re-register -- build new definition, swap in single assignment; in-flight event completes with old definition (single-threaded, no race)
- Epoch boundary for live+backfill coexistence -- backfill replays events using historical timestamps; live events update operators normally for existing features; backfill only initializes the NEW feature's operator; no conflict because they operate on disjoint feature sets
- 64 events per yield cycle -- same cooperative pattern as MSET chunking
- Automatic on re-register -- when new feature has `backfill=True`, server starts background backfill task after registration returns OK; expose backfill status via `GET /debug/backfill` HTTP endpoint
- Idempotent restart on crash -- detect incomplete backfill (feature exists but no "backfill complete" marker), re-read event log from start; operators are deterministic so replay produces same result
- Per-feature backfill flag -- `st.count(window="1h", backfill=True)` on individual features; serialized in FeatureDef JSON, server reads during schema diff
- Event timestamps for bucketing during replay -- `operator.push(event, event_timestamp)` instead of wall clock; window expiry relative to event time for deterministic results
- Derives auto-resolve after backfill -- computed on read, no special handling; once backfilling operator has state, derives return computed values
- Re-registration returns schema diff summary -- `{"status": "ok", "added": ["feat"], "removed": ["feat"], "backfilling": ["feat"]}`

### Claude's Discretion
- Backfill task internal data structures (tracking progress, completion markers)
- Event log seek/iteration strategy for backfill replay
- HTTP backfill status endpoint response format details
- Snapshot v4 compatibility handling for lazy GC markers

### Deferred Ideas (OUT OF SCOPE)
None.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCHM-01 | User can add new features to an existing stream without resetting state | Schema diff in `register()` classifies features as added/removed/unchanged; existing operator state in `StreamEntityState.operators` is preserved for unchanged features; new operators created via `create_operator()` |
| SCHM-02 | User can remove features from a stream without resetting remaining features | Removed features marked with lazy GC flag; `StreamDefinition.features` list shrinks; existing operators for remaining features untouched; snapshot serialization filters out marked operators |
| SCHM-03 | User can register a new feature with `backfill=True` to auto-replay from event log | `backfill` field added to `FeatureDefRequest` and all Python operator classes; on re-register, added features with `backfill=True` trigger async backfill task; `EventLog::read_entries()` provides replay source |
| SCHM-04 | Backfill replay uses cooperative yielding to avoid starving live traffic | Backfill task acquires `SharedState` lock, processes 64 events, drops lock, calls `tokio::task::yield_now()`, repeats -- identical to MSET pattern |
| SCHM-05 | Backfill replays events using event timestamps (not wall clock) for deterministic results | `operator.push(event, event_timestamp)` already accepts `SystemTime` parameter; `LogEntry.timestamp` provides event timestamps; backfill passes `entry.timestamp` instead of `SystemTime::now()` |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.x (already in Cargo.toml) | Async runtime, `yield_now()` for cooperative scheduling | Already used; backfill task is a `tokio::spawn` [VERIFIED: codebase Cargo.toml] |
| postcard | 1.x (already in Cargo.toml) | Serialization for snapshot v4 format | Already used for snapshots and event log [VERIFIED: codebase] |
| serde_json | 1.x (already in Cargo.toml) | JSON parsing of event payloads during replay | Already used throughout [VERIFIED: codebase] |
| ahash | (already in Cargo.toml) | Fast hash maps per project convention | Locked v1.0 decision [VERIFIED: codebase] |
| axum | (already in Cargo.toml) | HTTP API for `/debug/backfill` endpoint | Already used for HTTP management API [VERIFIED: codebase] |

### Supporting
No new dependencies required. All building blocks exist in the codebase.

### Alternatives Considered
None -- this phase uses only existing dependencies.

**Installation:**
No new packages needed.

## Architecture Patterns

### Recommended Changes to Existing Files

```
src/
├── engine/
│   └── pipeline.rs        # Schema diff logic in register(), backfill flag on FeatureDef
├── server/
│   ├── protocol.rs         # backfill field on FeatureDefRequest, diff response format
│   ├── tcp.rs              # Register handler returns diff JSON, spawns backfill task
│   └── http.rs             # GET /debug/backfill endpoint
├── state/
│   ├── store.rs            # No structural changes (operators already per-stream)
│   ├── snapshot.rs         # Lazy GC during serialization (filter removed operators)
│   └── event_log.rs        # Possibly add streaming iterator for large logs
└── python/
    └── tally/
        ├── _operators.py   # backfill=False kwarg on all stateful operators
        └── _stream.py      # No structural changes (to_json passes through)
```

### Pattern 1: Schema Diff on Re-Registration
**What:** When `register()` receives a stream definition for an already-registered stream, compare old and new feature lists by name. Classify each feature as `Added`, `Removed`, or `Unchanged`. Reject features whose name exists but operator type changed.
**When to use:** Every `REGISTER` command for an existing stream.
**Example:**
```rust
// Source: [Designed for this phase based on codebase analysis]
struct SchemaDiff {
    added: Vec<String>,        // New features not in old definition
    removed: Vec<String>,      // Old features not in new definition
    unchanged: Vec<String>,    // Features present in both (same type)
    backfilling: Vec<String>,  // Subset of added where backfill=true
}

fn diff_features(
    old: &[(String, FeatureDef)],
    new: &[(String, FeatureDef)],
) -> Result<SchemaDiff, TallyError> {
    let old_names: AHashMap<&str, &FeatureDef> = old.iter()
        .map(|(n, d)| (n.as_str(), d)).collect();
    let new_names: AHashMap<&str, &FeatureDef> = new.iter()
        .map(|(n, d)| (n.as_str(), d)).collect();

    // Reject type changes
    for (name, new_def) in &new_names {
        if let Some(old_def) = old_names.get(name) {
            if !same_operator_type(old_def, new_def) {
                return Err(TallyError::Protocol(format!(
                    "feature '{}' type changed; remove and re-add with a new name", name
                )));
            }
        }
    }

    // Classify
    let added: Vec<String> = new_names.keys()
        .filter(|n| !old_names.contains_key(*n))
        .map(|n| n.to_string())
        .collect();
    let removed: Vec<String> = old_names.keys()
        .filter(|n| !new_names.contains_key(*n))
        .map(|n| n.to_string())
        .collect();
    let unchanged: Vec<String> = new_names.keys()
        .filter(|n| old_names.contains_key(*n))
        .map(|n| n.to_string())
        .collect();

    Ok(SchemaDiff { added, removed, unchanged, backfilling: vec![] })
}
```

### Pattern 2: Lazy GC of Removed Features
**What:** When a feature is removed, it is not immediately deleted from `StreamEntityState.operators`. Instead, the new `StreamDefinition` simply no longer includes it, so `push()` stops computing it. Operators for removed features are filtered out during the next snapshot serialization in `clone_for_snapshot()`.
**When to use:** Feature removal on re-registration.
**How it works:**
1. `register()` stores the new `StreamDefinition` (which excludes removed features).
2. `push()` iterates `stream.features` to determine which operators to push to -- removed features are naturally excluded because they are no longer in the definition.
3. `get_features()` / `get_all_features()` still reads orphan operators from `StreamEntityState` (they return stale values). This is acceptable: stale values decay via window expiry, or we add a name filter.
4. `clone_for_snapshot()` filters operators against the current `StreamDefinition` feature names, dropping orphans.
**Key insight:** The current `push()` code at line 316 of `pipeline.rs` already reconciles operators with the definition -- it only pushes to operators that exist in `stream.features`. It also creates missing operators for new features. This means lazy GC is nearly free: we just need the snapshot filter.

### Pattern 3: Cooperative Backfill Task
**What:** After re-registration identifies features with `backfill=True`, spawn a tokio task that reads the event log, acquires the state lock in 64-event chunks, pushes events to only the new operator(s), and yields between chunks.
**When to use:** When added features have `backfill=True`.
**Example:**
```rust
// Source: [Modeled after MSET handle_mset pattern in tcp.rs:312-333]
async fn run_backfill(
    state: SharedState,
    stream_name: String,
    feature_names: Vec<String>,  // Only the new features to backfill
    entries: Vec<LogEntry>,       // Read from event log
) {
    for chunk in entries.chunks(64) {
        {
            let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
            for entry in chunk {
                let event: serde_json::Value = match serde_json::from_slice(&entry.payload) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // Extract key, get entity, push to ONLY the backfill operators
                // using entry.timestamp (not SystemTime::now())
                // ... (simplified)
            }
        } // Lock released
        tokio::task::yield_now().await;
    }
    // Mark backfill complete
}
```

### Pattern 4: Backfill Status Tracking
**What:** A shared data structure tracking active and completed backfill tasks. Exposed via `GET /debug/backfill`.
**Design:**
```rust
struct BackfillStatus {
    stream: String,
    features: Vec<String>,
    total_events: usize,
    processed_events: usize,
    started_at: SystemTime,
    completed_at: Option<SystemTime>,
}
```
Store as `Vec<BackfillStatus>` or `AHashMap<String, BackfillStatus>` inside `AppState`. The backfill task updates `processed_events` each chunk. The HTTP endpoint reads it.

### Anti-Patterns to Avoid
- **Rebuilding all operator state on re-register:** The whole point is preserving existing state. Never clear `StreamEntityState` on re-registration.
- **Using wall clock for backfill replay:** Events must be replayed with `entry.timestamp` to produce deterministic results. Using `SystemTime::now()` would produce different window bucketing.
- **Blocking the event loop during backfill:** The backfill task must yield cooperatively. Never hold the lock for more than 64 events.
- **Immediate deletion of removed feature operators:** This would cause a hot-path penalty. Lazy GC during snapshot is the correct approach.
- **Running backfill as a synchronous operation:** Registration must return immediately with the diff summary. Backfill runs asynchronously.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cooperative scheduling | Custom thread pool or timer-based yielding | `tokio::task::yield_now()` with lock-chunk pattern | Already proven by MSET; single-threaded runtime makes this trivial |
| Event log iteration | Custom file cursor or memory-mapped reading | `EventLog::read_entries()` | Already implemented, tested, handles edge cases |
| Operator creation | Manual match on feature type strings | `create_operator(&FeatureDef)` | Already exists in `pipeline.rs:131`, handles all 7 operator types |
| JSON diff response | Custom binary protocol response | `serde_json::to_vec(&diff_summary)` | REGISTER response is already JSON bytes in the OK payload |

**Key insight:** Nearly all building blocks already exist. This phase is primarily about wiring existing components together with schema diff logic.

## Common Pitfalls

### Pitfall 1: Operator State Loss on Re-Registration
**What goes wrong:** Calling `self.streams.insert(name, stream)` in `register()` replaces the `StreamDefinition`, but the actual operator state lives in `StateStore`'s `EntityState.streams[stream_name].operators`. If `push()` re-creates operators because names don't match, state is lost.
**Why it happens:** The current `push()` code at line 316 checks `stream_state.operators.iter().any(|(n, _)| *n == **name)` and creates operators for missing features. For unchanged features, this is a no-op (they already exist). The risk is if we accidentally clear `StreamEntityState.operators` during re-registration.
**How to avoid:** Never touch `StateStore` during `register()`. Only update `PipelineEngine.streams`. The reconciliation in `push()` handles the rest naturally.
**Warning signs:** Tests show operator values reset to zero after re-registration.

### Pitfall 2: Backfill Reads Stale Event Log
**What goes wrong:** `EventLog::read_entries()` opens the file independently from the writer. If the writer has buffered but not flushed data, backfill misses recent events.
**Why it happens:** `BufWriter` does not flush automatically. `fsync_all()` runs on a 1-second timer.
**How to avoid:** Before starting backfill, call `event_log.fsync_all()` to flush pending writes. This ensures the reader sees all logged events.
**Warning signs:** Backfill results are slightly behind live state for recently pushed events.

### Pitfall 3: Large Event Logs OOM During Backfill
**What goes wrong:** `read_entries()` loads the entire log into a `Vec<LogEntry>`. For streams with millions of events and long `history_ttl`, this could exhaust memory.
**Why it happens:** `read_entries()` was designed for compaction (read-modify-write), not streaming iteration.
**How to avoid:** For Phase 8, the risk is bounded by `history_ttl` (default 72h). For a 100K events/sec stream, 72h = ~26B events -- too many. But realistic workloads are much lower. Add a streaming iterator if needed (Claude's discretion), or read in chunks. Monitor: if `read_entries()` returns > 1M entries, log a warning.
**Warning signs:** Memory spikes during backfill. OOM kills.

### Pitfall 4: Derive Features Accessing Backfilling Operators Mid-Replay
**What goes wrong:** A derive expression references a feature being backfilled. During backfill, the operator has partial state. A GET request reads the derive, which reads the partially-backfilled operator, returning an incorrect value.
**Why it happens:** Derives are computed on read and reference other features by name. There is no "backfill in progress" check.
**How to avoid:** Per CONTEXT.md: "Derives auto-resolve after backfill -- computed on read, no special handling." This means partial results during backfill are acceptable. The derive will return correct values once backfill completes. Document this behavior: during backfill, derived features referencing backfilling features may return partial/intermediate values.
**Warning signs:** Not a bug -- expected behavior per design decision.

### Pitfall 5: Snapshot Compatibility with Lazy GC Markers
**What goes wrong:** If we add a new field to `OperatorState` or `SerializableStreamEntityState` for GC markers, old snapshots may fail to deserialize.
**Why it happens:** Postcard is not self-describing -- adding fields breaks deserialization.
**How to avoid:** Do NOT add GC markers to the serialized state. Instead, implement lazy GC purely at serialization time: `clone_for_snapshot()` filters operators against the current `StreamDefinition.features` list. If a stream has operators not in the definition, they are simply omitted from the snapshot. No schema change needed.
**Warning signs:** Snapshot load fails after upgrading.

### Pitfall 6: Borrow Conflict in Backfill Task
**What goes wrong:** The backfill task needs to read the `StreamDefinition` (to know which operators to push to) AND mutate the `StateStore` (to push events). Both live under the same `Mutex<AppState>`.
**Why it happens:** Single lock for all state -- same issue solved multiple times in prior phases.
**How to avoid:** Inside each lock acquisition, extract needed data from `engine` (feature defs, key_field), then work with `store`. Use scoped borrows exactly as the existing `push()` method does (lines 298-388 of pipeline.rs).
**Warning signs:** Rust compiler borrow errors when accessing engine and store simultaneously.

## Code Examples

### Example 1: Schema Diff Helper (Type Comparison)
```rust
// Source: [Designed based on existing FeatureDef variants in pipeline.rs:20-69]
fn same_operator_type(a: &FeatureDef, b: &FeatureDef) -> bool {
    use std::mem::discriminant;
    discriminant(a) == discriminant(b)
}
```
Uses `std::mem::discriminant` to compare enum variants without comparing inner values. This is the idiomatic Rust way to check "same variant, possibly different parameters." [VERIFIED: Rust std library]

### Example 2: Backfill Flag on FeatureDef
```rust
// Source: [Extension of existing FeatureDef in pipeline.rs]
// Add backfill: bool to each variant that has operator state:
FeatureDef::Count {
    window: Duration,
    bucket: Duration,
    where_expr: Option<Expr>,
    backfill: bool,  // NEW
}
// ... same for Sum, Avg, Min, Max, Last, DistinctCount
// Derive does NOT get backfill (no state, computed on read)
```

### Example 3: Backfill Flag on Python Operator
```python
# Source: [Extension of existing Count in _operators.py]
class Count(OperatorBase):
    def __init__(self, *, window: str, where: str | None = None,
                 bucket: str | None = None, backfill: bool = False) -> None:
        self.window = window
        self.where_clause = where
        self.bucket = bucket
        self.backfill = backfill

    def to_json(self, name: str) -> dict:
        d: dict = {"name": name, "type": "count", "window": self.window}
        if self.where_clause is not None:
            d["where"] = self.where_clause
        if self.bucket is not None:
            d["bucket"] = self.bucket
        if self.backfill:
            d["backfill"] = True
        return d
```

### Example 4: Register Response with Diff Summary
```rust
// Source: [Extension of REGISTER handler in tcp.rs:242-263]
// After schema diff:
let diff_json = serde_json::json!({
    "status": "ok",
    "added": diff.added,
    "removed": diff.removed,
    "backfilling": diff.backfilling,
});
Ok(serde_json::to_vec(&diff_json).unwrap())
```

### Example 5: Snapshot Lazy GC Filter
```rust
// Source: [Extension of clone_for_snapshot() in store.rs:198-215]
// During clone_for_snapshot, filter operators against current engine definitions:
pub fn clone_for_snapshot_with_gc(
    &self,
    engine: &PipelineEngine,
) -> Vec<(String, SerializableEntityState)> {
    self.entities.iter().map(|(key, entity)| {
        let streams: Vec<(String, SerializableStreamEntityState)> = entity.streams.iter()
            .map(|(stream_name, stream_state)| {
                // Filter operators: keep only those in current definition
                let valid_names: AHashSet<&str> = engine.get_stream(stream_name)
                    .map(|def| def.features.iter()
                        .filter(|(_, d)| !matches!(d, FeatureDef::Derive { .. }))
                        .map(|(n, _)| n.as_str())
                        .collect())
                    .unwrap_or_default();
                let filtered_ops: Vec<_> = stream_state.operators.iter()
                    .filter(|(name, _)| valid_names.contains(name.as_str()))
                    .cloned()
                    .collect();
                (stream_name.clone(), SerializableStreamEntityState {
                    operators: filtered_ops,
                    last_event_at: stream_state.last_event_at,
                })
            })
            .collect();
        (key.clone(), SerializableEntityState {
            streams,
            static_features: entity.static_features.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }).collect()
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Replace definition on re-register (no diff) | Schema diff with preserve/add/remove | Phase 8 | Enables non-destructive schema evolution |
| No backfill support | Cooperative backfill from event log | Phase 8 | New features can be populated with historical data |
| Immediate operator cleanup | Lazy GC during snapshot serialization | Phase 8 | Zero hot-path cost for feature removal |

**Deprecated/outdated:**
- The current `register()` method in `PipelineEngine` does a simple `insert` which replaces the old definition. This will be enhanced with diff logic but must remain backward-compatible for first-time registration (no old definition = all features are "added").

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `std::mem::discriminant` works for comparing `FeatureDef` enum variants | Code Examples | LOW -- standard Rust, well-documented; fallback is manual match |
| A2 | Event log entries for a typical 72h window fit in memory for `read_entries()` | Common Pitfalls | MEDIUM -- for high-throughput streams, may need streaming iterator; bounded by history_ttl |
| A3 | Postcard deserialization is backward compatible when we DON'T change the schema (lazy GC via filtering, not new fields) | Common Pitfalls | LOW -- no schema change means no compatibility issue |

## Open Questions

1. **Should `GET` requests return partially-backfilled feature values or `Missing` during backfill?**
   - What we know: CONTEXT.md says "derives auto-resolve after backfill" which implies partial values are visible. Operators return their current state on `read()`.
   - What's unclear: Whether users expect to see `Missing` for a feature that is mid-backfill, or whether seeing a partial count (e.g., 47 out of eventual 1000) is acceptable.
   - Recommendation: Return partial values (current operator state). This is simpler, consistent with how operators work, and the backfill status endpoint lets users know when backfill is complete. [VERIFIED: consistent with CONTEXT.md "derives auto-resolve" decision]

2. **Memory budget for large event log reads**
   - What we know: `read_entries()` loads everything into memory. Default `history_ttl` is 72h.
   - What's unclear: What is the expected event volume? At 1K events/sec, 72h = 259M events = many GB.
   - Recommendation: Start with `read_entries()` as-is (works for reasonable volumes). Add a note to the plan that a streaming iterator is a Claude's discretion optimization if testing reveals memory concerns. For Phase 8, the feature is functional; optimization can follow.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust), pytest (Python) |
| Config file | Cargo.toml (Rust), python/pyproject.toml (Python) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test --lib && cd python && python -m pytest` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCHM-01 | Add new feature preserves existing operator state | unit | `cargo test schema_diff -- --exact` | No -- Wave 0 |
| SCHM-01 | Re-register with added feature, push events, verify old features retained | integration | `cargo test test_reregister_add_feature` | No -- Wave 0 |
| SCHM-02 | Remove feature, remaining features continue | unit | `cargo test schema_diff_remove -- --exact` | No -- Wave 0 |
| SCHM-02 | Removed feature lazy GC on snapshot | unit | `cargo test test_snapshot_lazy_gc` | No -- Wave 0 |
| SCHM-03 | Backfill flag parsed from JSON, triggers replay | unit | `cargo test test_backfill_flag_parsed` | No -- Wave 0 |
| SCHM-03 | Backfill replays events and produces correct operator state | integration | `cargo test test_backfill_replay` | No -- Wave 0 |
| SCHM-04 | Backfill yields cooperatively (64-event chunks) | integration | `cargo test test_backfill_cooperative_yield` | No -- Wave 0 |
| SCHM-05 | Backfill uses event timestamps, not wall clock | unit | `cargo test test_backfill_event_timestamps` | No -- Wave 0 |
| SCHM-03 | Python SDK backfill kwarg | unit | `cd python && python -m pytest tests/test_operators.py -k backfill` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test --lib && cd python && python -m pytest`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] Schema diff unit tests (register with added/removed features, type change rejection)
- [ ] Backfill replay integration tests (event log replay with timestamp determinism)
- [ ] Snapshot lazy GC tests (removed operators filtered during serialization)
- [ ] Python SDK backfill kwarg tests
- [ ] Cooperative yielding test (verify lock is not held for more than 64 events)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | N/A -- internal service, no auth |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A -- single-tenant |
| V5 Input Validation | Yes | Existing `RegisterRequest` validation; new `backfill` field is bool (no injection risk) |
| V6 Cryptography | No | N/A |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malicious re-registration with backfill flooding | Denial of Service | 64-event yield cycle rate-limits backfill; single backfill task per stream |
| Invalid `backfill` field type in JSON | Tampering | serde deserialization rejects non-bool; `#[serde(default)]` defaults to false |
| Path traversal in stream names during event log read | Tampering | Already mitigated by `sanitize_stream_name()` in event_log.rs |

## Sources

### Primary (HIGH confidence)
- Codebase analysis: `src/engine/pipeline.rs` (register, push, create_operator, FeatureDef) -- current registration and operator lifecycle
- Codebase analysis: `src/state/event_log.rs` (read_entries, append, LogEntry) -- event log reader/writer
- Codebase analysis: `src/server/tcp.rs` (handle_mset, handle_sync_command) -- cooperative yielding pattern and REGISTER handler
- Codebase analysis: `src/state/store.rs` (StreamEntityState, clone_for_snapshot) -- per-stream operator storage and snapshot serialization
- Codebase analysis: `src/state/snapshot.rs` (OperatorState, SnapshotState, save/load) -- snapshot format v4
- Codebase analysis: `src/server/protocol.rs` (RegisterRequest, FeatureDefRequest, convert_register_request) -- registration DTO schema
- Codebase analysis: `python/tally/_operators.py` -- Python SDK operator classes
- Codebase analysis: `src/main.rs` -- tokio background task patterns
- Phase 8 CONTEXT.md -- locked implementation decisions

### Secondary (MEDIUM confidence)
- None needed -- all claims verified against codebase

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all verified in Cargo.toml/codebase
- Architecture: HIGH -- patterns directly derived from existing code (MSET yielding, push reconciliation, snapshot serialization)
- Pitfalls: HIGH -- identified from reading actual code paths that will be modified

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable -- no external dependency changes)
