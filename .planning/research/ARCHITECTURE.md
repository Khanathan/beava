# Architecture Research: v2.0 New API & Engine Integration

**Domain:** Integration of function-based pipeline API (@tl.source, @tl.dataset, EventSet/FeatureSet) with existing Rust engine, plus enriched event propagation, feature projection, union node, and ephemeral pipeline lifecycle.
**Researched:** 2026-04-12
**Confidence:** HIGH for existing-shape analysis (code read directly); MEDIUM-HIGH for integration design (changes are well-scoped additions to proven architecture).

---

## Executive Summary

The v2.0 API change is primarily a **Python SDK rewrite** that compiles to a superset of the same JSON registration format the Rust engine already consumes. The Rust engine needs three surgical additions -- none of which alter the hot-path data structures:

1. **Enriched event propagation** (~50 LOC): when cascading through the DAG, merge upstream derive results into the event JSON before passing to downstream streams.
2. **Feature projection**: a new `projection` field on StreamDefinition that filters which features appear in PUSH responses.
3. **Union node**: a StreamDefinition that accepts events from multiple upstream streams (multiple entries in `depends_on` with union semantics instead of cascade).

The existing `PipelineEngine`, `StateStore`, `ConcurrentAppState`, petgraph DAG, and REGISTER command all survive unchanged in structure. The new Python types (`EventSet`, `FeatureSet`, `@tl.source`, `@tl.dataset`) are compile-time abstractions that produce the same `{"name", "key_field", "features", "depends_on", ...}` JSON the server already parses.

Ephemeral pipelines for on-demand compute require one new field (`ephemeral: bool`) on StreamDefinition and a lifecycle manager that skips snapshot persistence and enforces memory limits.

---

## 1. Current Architecture Shape (v1.3 Baseline)

### 1.1 Engine Core

```
ConcurrentAppState
  |-- engine: RwLock<PipelineEngine>     # read on PUSH/GET, write on REGISTER
  |-- store: StateStore (DashMap)         # per-entity concurrency
  |-- event_log: PLMutex<Option<EventLog>>
  |-- metrics, snapshot_*, backfill_*
```

`PipelineEngine` holds:
- `streams: AHashMap<String, StreamDefinition>` -- keyed stream definitions
- `views: AHashMap<String, ViewDefinition>` -- cross-stream views
- `raw_register_jsons: AHashMap<String, serde_json::Value>` -- raw JSON for snapshot persistence
- `dag: DiGraph<String, ()>` + `node_indices` + `topo_order` + `downstream_map` -- petgraph DAG for cascade

### 1.2 Push-Through Data Flow (Current)

```
PUSH "Transactions" { user_id: "u123", amount: 50.0, merchant_id: "m456" }
  |
  v
push_with_cascade_internal(stream_name, event, store, now)
  |-- push_internal("Transactions", event)   # primary stream
  |     |-- extract key from event["user_id"]
  |     |-- get_or_create entity state for "u123"
  |     |-- push event to each operator (count, sum, etc.)
  |     |-- read operator values + evaluate derives
  |     |-- return FeatureMap
  |
  |-- BFS over downstream_map to find cascade targets
  |-- for each downstream in topo_order:
  |     push_internal(downstream, event)     # <-- SAME event JSON
  |                                           # <-- THIS IS THE GAP
  |
  v (in tcp.rs, after cascade)
  fan_out: for each keyed stream whose key_field != primary key_field:
    if event contains that key_field -> push_no_features(target, event)
```

**Critical observation:** Line 906 of pipeline.rs passes the **original raw event** to every downstream stream. Derived columns computed by upstream streams (via `stream.map(amount_usd=raw["amount"] * raw["fx_rate"])`) are NOT available to downstream operators. This is the "enriched event propagation" gap.

### 1.3 Registration JSON Format (Current)

The server parses this from REGISTER commands:
```json
{
  "name": "transactions_raw__mapped",
  "key_field": null,
  "features": [
    {"name": "amount_usd", "type": "derive", "expr": "(_event.amount * _event.fx_rate)"}
  ],
  "depends_on": ["transactions_raw"],
  "filter": null
}
```

All v2.0 Python types must compile down to this format (with new optional fields).

---

## 2. Integration Points: New API -> Existing Engine

### 2.1 Python SDK: New Types Map to Existing JSON

| New Python Type | Compiles To | Server-Side Entity |
|---|---|---|
| `@tl.source("name")` | `{"name": "...", "key_field": null, "features": []}` | Keyless `StreamDefinition` (already supported) |
| `@tl.dataset(depends_on=[...])` returning `EventSet` | `{"name": "...", "key_field": null, "features": [...derives...], "depends_on": [...]}` | Keyless `StreamDefinition` with derives + depends_on (already supported) |
| `@tl.dataset(depends_on=[...])` returning `FeatureSet` | `{"name": "...", "key_field": "...", "features": [...], "depends_on": [...]}` | Keyed `StreamDefinition` with depends_on (already supported) |
| `EventSet` | Not a server-side type. Python-only type annotation indicating the function produces an event stream. | N/A |
| `FeatureSet` | Not a server-side type. Python-only type annotation indicating the function produces keyed features. | N/A |

**Key insight:** `EventSet` and `FeatureSet` are **Python-side compile-time abstractions only**. They inform the SDK's code generation (what JSON to emit) but have no representation in the Rust engine. The server already handles both keyless streams (EventSet equivalent) and keyed streams (FeatureSet equivalent).

### 2.2 What Changes in Registration JSON

New optional fields needed (backward compatible -- old clients omit them):

```json
{
  "name": "...",
  "key_field": "...",
  "features": [...],
  "depends_on": ["..."],
  "filter": "...",
  "entity_ttl": "...",
  "history_ttl": "...",
  "projection": ["feat_a", "feat_b"],   // NEW: feature projection
  "union_mode": false,                    // NEW: union node flag
  "ephemeral": false                      // NEW: on-demand lifecycle
}
```

### 2.3 Rust-Side Changes Summary

| Change | Scope | Lines (est.) | Risk |
|---|---|---|---|
| Enriched event propagation | `push_with_cascade_internal` | ~50 | Medium -- touches hot path |
| Feature projection field on StreamDefinition | `pipeline.rs` struct + REGISTER parser | ~20 | Low -- additive |
| Projection filtering on PUSH response | `push_internal` return path | ~10 | Low -- additive |
| Union node in DAG | `rebuild_dag` + `push_with_cascade_internal` | ~30 | Low -- additive |
| Ephemeral flag on StreamDefinition | struct + snapshot skip logic | ~15 | Low -- additive |
| Ephemeral lifecycle manager | New module or section in `tcp.rs` | ~80 | Medium -- new system |

**Total Rust changes: ~200 LOC.** No new crates. No data structure changes. No wire protocol changes.

---

## 3. Enriched Event Propagation (Deep Dive)

This is the most architecturally significant change. Today's cascade passes the raw event. The new API needs upstream derives (from `stream.map()`) to be visible to downstream operators.

### 3.1 Current Flow (Broken for Enrichment)

```
push_with_cascade_internal:
  primary_features = push_internal("raw", event)          # computes amount_usd derive
  for downstream in topo_order:
    push_internal(downstream, event)                       # event does NOT contain amount_usd
                                    ^--- downstream cannot access amount_usd
```

### 3.2 Proposed Flow (Enriched)

```
push_with_cascade_internal:
  primary_features = push_internal("raw", event)
  enriched_event = merge(event, primary_derives)           # <-- NEW: ~10 lines
  for downstream in topo_order:
    // Use enriched event from PREVIOUS stage, not original
    stage_features = push_internal(downstream, enriched_for_this_stage)
    enriched_event = merge(enriched_event, stage_derives)  # <-- accumulate
```

### 3.3 Implementation Detail

```rust
fn push_with_cascade_internal(
    &self,
    stream_name: &str,
    event: &serde_json::Value,
    store: &StateStore,
    now: SystemTime,
    read_features: bool,
) -> Result<FeatureMap, TallyError> {
    // Primary push -- always read features to get derives for enrichment
    let primary_features = self.push_internal(stream_name, event, store, now, true)?;

    // Build enriched event: original event + derive results from primary
    let mut enriched = event.clone();  // serde_json::Value::Object clone
    if let serde_json::Value::Object(ref mut map) = enriched {
        for (name, value) in &primary_features {
            // Only inject derive results (not operator reads) to avoid
            // leaking internal state names into the event namespace
            if self.is_derive_feature(stream_name, name) {
                map.insert(name.clone(), value.to_json_value());
            }
        }
    }

    // Cascade with enriched event
    for stream_in_order in &self.topo_order {
        if !reachable.contains(stream_in_order) { continue; }
        let stage_features = self.push_internal(
            stream_in_order, &enriched, store, now, true
        )?;
        // Accumulate: downstream derives become available to further downstream
        if let serde_json::Value::Object(ref mut map) = enriched {
            for (name, value) in &stage_features {
                if self.is_derive_feature(stream_in_order, name) {
                    map.insert(name.clone(), value.to_json_value());
                }
            }
        }
    }

    // Return primary features (or filtered by projection)
    if read_features { Ok(primary_features) } else { Ok(FeatureMap::new()) }
}
```

### 3.4 Performance Impact

**Concern:** `event.clone()` on every cascade push adds allocation.

**Mitigation:**
- Clone only happens when the stream HAS downstream dependents (check `downstream_map.contains_key(stream_name)` -- skip clone for leaf nodes).
- Most pipelines are 2-3 stages deep. One clone of a ~200-byte event JSON is ~50ns. Negligible vs. operator update cost (~1-5us).
- For `push_no_features` (async path): enrichment is still needed for correctness of downstream operators. But we can skip reading derives if no downstream needs them. Add a pre-computed `needs_enrichment: bool` flag per stream during `rebuild_dag()`.

**Concern:** `push_internal` currently skips derive evaluation when `read_features=false` (the async hot path). Enrichment requires derive evaluation for upstream stages even in async mode.

**Resolution:** In `push_with_cascade_internal`, call `push_internal(upstream, event, store, now, true)` for stages that have downstream dependents needing their derives, even in async mode. Only the **leaf stages** can skip derive evaluation. Pre-compute this at registration time:

```rust
// In rebuild_dag():
// A stream needs_derive_for_cascade if any downstream stream's operators
// reference fields that are derives of this stream.
// Simpler approximation: needs_derive_for_cascade = has_downstream && has_derive_features
```

### 3.5 Key Design Decision: Namespace Collision

When enriching, derive results from upstream merge into the event namespace. If upstream has a derive `amount_usd` and the raw event also has a field `amount_usd`, which wins?

**Recommendation: Upstream derive wins.** This matches the mental model -- `stream.map(amount_usd=...)` is an explicit override. If the user names a derive the same as a raw field, they intended the override. Document this clearly.

**Alternative considered:** Prefix derives with stream name (`raw__mapped.amount_usd`). Rejected -- it makes downstream expressions ugly and couples them to upstream naming.

---

## 4. Feature Projection

### 4.1 What It Does

When a PUSH response includes all features for a stream (operators + derives + statics), feature projection lets the definition specify a subset to return. This is useful when:
- A stream has 50 features but the caller only needs 5
- Internal intermediate features should not leak to the client

### 4.2 Integration Point

In `push_internal`, after collecting features into the `FeatureMap`, apply projection before return:

```rust
// After all features collected:
if let Some(ref projection) = stream.projection {
    features.retain(|k, _| projection.contains(k));
}
```

That is literally it -- ~5 lines of Rust.

### 4.3 StreamDefinition Change

```rust
pub struct StreamDefinition {
    // ... existing fields ...
    /// Optional projection: if set, PUSH responses include only these features.
    /// None means return all features (current behavior).
    pub projection: Option<Vec<String>>,
}
```

### 4.4 Interaction with Enriched Propagation

Projection applies to the **client-facing response only**, NOT to what gets enriched into the cascade event. Internally, all derives are still computed and passed downstream. Projection is a response filter, not a computation filter.

---

## 5. Union Node

### 5.1 What It Does

A union node accepts events from **multiple** upstream sources. Current `depends_on` means "cascade FROM these upstreams" -- but all upstream events must flow through a single chain. A union node says "events from stream A OR stream B both feed into me."

### 5.2 Current DAG Semantics vs. Union Semantics

**Current (`depends_on`):** Stream B `depends_on: ["A"]` means when A receives an event, it cascades to B. B never receives events directly from PUSH -- only through A.

**Union:** Stream C `depends_on: ["A", "B"], union_mode: true` means when A receives an event, it cascades to C. When B receives an event, it ALSO cascades to C. C sees events from both sources.

### 5.3 DAG Integration

The petgraph DAG already supports multiple incoming edges per node (that is how `depends_on: ["A", "B"]` works today for cascade). The difference is semantic:

- **Without `union_mode`:** `depends_on: ["A", "B"]` means "B must be registered before me AND I receive cascaded events from both A and B." This already works.
- **With `union_mode`:** Same DAG structure, same cascade behavior. The only difference is at the Python SDK level -- `union_mode` is a hint that the user explicitly wants events from multiple sources merged.

**Wait -- does this already work?**

Looking at `push_with_cascade_internal` (line 852-917): BFS starts from the pushed stream, finds all reachable downstream, and pushes the event to each in topo order. If stream C has `depends_on: ["A", "B"]`, then:
- Push to A: BFS finds C is downstream of A, pushes event to C. WORKS.
- Push to B: BFS finds C is downstream of B, pushes event to C. WORKS.

**The cascade DAG already supports union semantics.** The "union node" is not a new engine concept -- it is a Python SDK concept that generates a `StreamDefinition` with multiple `depends_on` entries. The engine already handles this.

### 5.4 What IS Needed

The Python SDK needs a `union()` function or method that:
1. Takes multiple `EventSet` or `Stream` inputs
2. Produces a new `EventSet`/`Stream` whose registration JSON has `depends_on: [all inputs]`
3. Validates all inputs have compatible schemas (or at least warns on mismatch)

```python
@tl.source("clicks")
def clicks() -> tl.EventSet: ...

@tl.source("purchases")
def purchases() -> tl.EventSet: ...

@tl.dataset(depends_on=[clicks, purchases])
def all_activity(events: tl.EventSet) -> tl.EventSet:
    return events  # union of clicks + purchases

@tl.dataset(depends_on=[all_activity])
def user_activity(events: tl.EventSet) -> tl.FeatureSet:
    return events.group_by("user_id").agg(
        activity_count_1h=tl.count(window="1h"),
    )
```

This compiles to:
```json
[
  {"name": "clicks", "key_field": null, "features": []},
  {"name": "purchases", "key_field": null, "features": []},
  {"name": "all_activity", "key_field": null, "features": [], "depends_on": ["clicks", "purchases"]},
  {"name": "user_activity", "key_field": "user_id", "features": [...], "depends_on": ["all_activity"]}
]
```

**No Rust engine changes needed.** The existing DAG cascade handles this.

---

## 6. Ephemeral Pipeline Lifecycle (On-Demand Compute)

### 6.1 Design Constraints (from Memory)

1. REGISTER must stay a runtime operation (sub-second pipeline creation)
2. Ephemeral pipelines should NOT be snapshotted (re-derive on restart)
3. Hard memory limits required (max pipelines, max keys per pipeline, memory budgets)
4. Same definition format for pre-registered and on-demand pipelines
5. Defer the product layer -- just architect the primitives in v2.0

### 6.2 Integration with Existing Systems

| System | Ephemeral Behavior | Implementation |
|---|---|---|
| **REGISTER** | Same command, `ephemeral: true` in JSON | Parse flag in `register()`, store on `StreamDefinition` |
| **PipelineEngine** | Same `streams` map, same DAG | No change to engine logic |
| **StateStore** | Same DashMap, same entity state | No change |
| **Snapshot** | Skip ephemeral streams' state during serialization | Filter in `clone_for_snapshot` |
| **Snapshot Recovery** | Ephemeral stream definitions NOT loaded from snapshot | Filter in `load_from_snapshot` |
| **Pipeline Persistence** | `raw_register_jsons` skips ephemeral entries | Filter in snapshot write |
| **Eviction** | Aggressive TTL (configurable, default short) | Use existing `entity_ttl` per-stream |
| **Memory Limits** | New: per-pipeline key count limit + global ephemeral memory budget | New enforcement in `push_internal` |

### 6.3 StreamDefinition Changes

```rust
pub struct StreamDefinition {
    // ... existing fields ...
    /// If true, this pipeline is ephemeral (on-demand). State is not
    /// persisted in snapshots and is lost on restart.
    pub ephemeral: bool,
}
```

### 6.4 Lifecycle Flow

```
1. Client sends REGISTER with ephemeral: true
   -> PipelineEngine.register() stores StreamDefinition (ephemeral=true)
   -> DAG rebuilds, topo_order updates
   -> State begins accumulating in DashMap

2. Events arrive via PUSH
   -> Normal push_with_cascade flow
   -> Ephemeral streams participate in cascade just like persistent ones

3. Snapshot fires
   -> clone_for_snapshot skips entities whose ONLY streams are ephemeral
   -> raw_register_jsons skips ephemeral entries

4. Client sends DELETE /pipelines/:name (or new UNREGISTER command)
   -> PipelineEngine removes stream
   -> DAG rebuilds
   -> State for that stream's entities can be lazily evicted (existing TTL) or eagerly purged

5. Server restart
   -> Snapshot loads, but ephemeral streams are absent
   -> Client must re-register ephemeral pipelines (this is the contract)
```

### 6.5 Memory Limit Enforcement

New fields on ConcurrentAppState (or a new `EphemeralManager`):

```rust
struct EphemeralLimits {
    max_ephemeral_pipelines: usize,         // default: 100
    max_keys_per_ephemeral_pipeline: usize, // default: 100_000
    max_ephemeral_memory_bytes: usize,      // default: 1GB
}
```

Enforcement points:
- `REGISTER` with `ephemeral: true`: reject if `max_ephemeral_pipelines` exceeded
- `push_internal` for ephemeral stream: reject (return error) if key count for that stream exceeds limit
- Periodic check (on eviction timer): if total ephemeral memory exceeds budget, evict oldest ephemeral keys first

### 6.6 Snapshot Interaction Detail

The existing snapshot path in `main.rs` calls `store.clone_for_snapshot_with_gc()`. This clones entities from the DashMap. The change:

```rust
// In clone_for_snapshot_with_gc:
// For each entity, skip stream entries where the stream is ephemeral.
// If ALL of an entity's streams are ephemeral and it has no static features,
// skip the entire entity.

// In raw_register_jsons serialization:
// Filter: only persist entries where !stream.ephemeral
```

This is ~15 lines of code across two sites.

---

## 7. Component Boundaries (New vs. Modified)

### 7.1 New Components

| Component | File (proposed) | Responsibility |
|---|---|---|
| `@tl.source` decorator | `python/tally/_source.py` | Declares a keyless event source |
| `@tl.dataset` decorator | `python/tally/_dataset.py` | Declares a pipeline stage (EventSet -> EventSet or FeatureSet) |
| `EventSet` type | `python/tally/_types.py` | Type annotation for event streams |
| `FeatureSet` type | `python/tally/_types.py` | Type annotation for keyed feature tables |
| `tl.union()` | `python/tally/_dataset.py` | Merge multiple EventSets |
| `EphemeralLimits` | `src/server/tcp.rs` or new `src/server/ephemeral.rs` | Memory/count limits for ephemeral pipelines |

### 7.2 Modified Components

| Component | File | Change |
|---|---|---|
| `StreamDefinition` | `src/engine/pipeline.rs` | Add `projection: Option<Vec<String>>`, `ephemeral: bool` |
| `push_with_cascade_internal` | `src/engine/pipeline.rs` | Enriched event propagation (~50 LOC) |
| `push_internal` return path | `src/engine/pipeline.rs` | Projection filtering (~5 LOC) |
| REGISTER JSON parser | `src/server/tcp.rs` (or `protocol.rs`) | Parse new optional fields |
| Snapshot serialization | `src/state/snapshot.rs` | Filter ephemeral entries |
| Snapshot recovery | `src/state/snapshot.rs` | Skip ephemeral definitions |
| `python/tally/__init__.py` | Python | New exports, eventually remove old API |
| `python/tally/_app.py` | Python | Support new registration flow |

### 7.3 Unchanged Components

| Component | Why Unchanged |
|---|---|
| `StateStore` / `DashMap` | Ephemeral streams use the same entity state structure |
| `OperatorState` enum | No new operators |
| Wire protocol (opcodes) | REGISTER already handles arbitrary JSON; new fields are additive |
| `petgraph` DAG | Union is already supported by multiple `depends_on` entries |
| Expression evaluator | No new syntax needed |
| Event log | Ephemeral streams can optionally skip event logging (but the mechanism exists) |
| Window / HLL / operators | No changes |

---

## 8. Data Flow: End-to-End Enriched Propagation

### 8.1 Example Pipeline

```python
@tl.source("raw_txns")
def raw_txns() -> tl.EventSet: ...

@tl.dataset(depends_on=[raw_txns])
def enriched_txns(events: tl.EventSet) -> tl.EventSet:
    return events.map(amount_usd=events["amount"] * events["fx_rate"])

@tl.dataset(depends_on=[enriched_txns])
def user_features(events: tl.EventSet) -> tl.FeatureSet:
    return events.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_usd_1h=tl.sum("amount_usd", window="1h"),  # uses enriched field
    )
```

### 8.2 Registration JSON

```json
[
  {"name": "raw_txns", "key_field": null, "features": []},
  {"name": "enriched_txns", "key_field": null, "features": [
    {"name": "amount_usd", "type": "derive", "expr": "(_event.amount * _event.fx_rate)"}
  ], "depends_on": ["raw_txns"]},
  {"name": "user_features", "key_field": "user_id", "features": [
    {"name": "tx_count_1h", "type": "count", "window": "1h"},
    {"name": "tx_sum_usd_1h", "type": "sum", "field": "amount_usd", "window": "1h"}
  ], "depends_on": ["enriched_txns"]}
]
```

### 8.3 Push-Through Trace

```
Client: PUSH "raw_txns" { user_id: "u123", amount: 50.0, fx_rate: 1.2 }

1. push_with_cascade_internal("raw_txns", event)
   |
   |-- push_internal("raw_txns", event)
   |   -> keyless, no operators, returns empty FeatureMap
   |   -> (no derives to enrich)
   |
   |-- enriched_event = event.clone()   // { user_id, amount, fx_rate }
   |
   |-- [topo order: enriched_txns, user_features]
   |
   |-- push_internal("enriched_txns", enriched_event)
   |   -> keyless, evaluates derive: amount_usd = 50.0 * 1.2 = 60.0
   |   -> returns { amount_usd: 60.0 }
   |   -> ENRICH: enriched_event = { user_id, amount, fx_rate, amount_usd: 60.0 }
   |
   |-- push_internal("user_features", enriched_event)
   |   -> keyed by "user_id", extract key "u123"
   |   -> count operator: increment tx_count_1h
   |   -> sum operator: reads "amount_usd" from enriched_event = 60.0, accumulate
   |   -> returns { tx_count_1h: 7, tx_sum_usd_1h: 420.0 }
   |
   v
   Response to client: features from primary stream (raw_txns) = {} (empty, keyless)
   // Client would GET "u123" to read user_features
```

### 8.4 Async Path Optimization

For `push_no_features` (OP_PUSH_ASYNC), the enrichment still happens for intermediate stages that have downstream dependents. But the FINAL stage can skip derive evaluation. Pre-computed at registration:

```
raw_txns: needs_derive_for_cascade = false (no derives)
enriched_txns: needs_derive_for_cascade = true (has downstream that reads amount_usd)
user_features: needs_derive_for_cascade = false (leaf node)
```

So even in async mode:
- `raw_txns`: push_internal(read_features=false) -- no derives to extract
- `enriched_txns`: push_internal(read_features=true) -- MUST evaluate derives for enrichment
- `user_features`: push_internal(read_features=false) -- leaf, skip derive eval

This preserves the async optimization for leaf streams while paying the derive cost only where enrichment is needed.

---

## 9. Interaction with Batch Path

### 9.1 push_batch_with_cascade_no_features

This is the hot-path batch primitive from Phase 12/13. It currently calls `push_with_cascade_no_features` per event. The enrichment change flows through automatically since `push_with_cascade_no_features` delegates to `push_with_cascade_internal`.

**One concern:** `push_with_cascade_internal` today skips `push_internal(..., read_features=false)` for cascade targets when in no-features mode. After enrichment, intermediate stages must switch to `read_features=true` when they have downstream dependents that need their derives.

The batch primitive does NOT need structural changes -- the per-event delegation handles this. But the **performance profile changes**: intermediate stages in the cascade now evaluate derives even in async mode. This is ~1-2us per stage per event for typical derive expressions. For a 3-stage pipeline at 1M eps, this adds ~2-3ms of aggregate CPU per second -- negligible.

---

## 10. Build Order Recommendation

### Phase 1: Python SDK -- New Types (No Rust Changes)

Build `@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet`, `tl.union()` as Python-only abstractions that compile to the SAME JSON format the server already accepts. This is 100% testable without any Rust changes -- the existing REGISTER handler parses the JSON.

**Test plan:** Python unit tests that verify JSON compilation matches expected format. Integration tests that register new-API pipelines on existing server.

**Why first:** De-risks the API design. If the JSON format needs changes, discover it before touching Rust.

### Phase 2: Enriched Event Propagation (Rust)

Modify `push_with_cascade_internal` to merge derive results into the cascaded event. Add `needs_derive_for_cascade` pre-computation to `rebuild_dag`.

**Test plan:** Integration tests with multi-stage pipelines where downstream operators reference upstream derives. Verify correctness in sync and async push modes. Benchmark to confirm <5% throughput regression.

**Why second:** This is the critical engine unlock. Without it, the new API's `stream.map()` produces derives that downstream stages cannot consume.

### Phase 3: Feature Projection (Rust)

Add `projection` field to `StreamDefinition`, parse from REGISTER JSON, apply in `push_internal` response.

**Test plan:** Unit test projection filtering. Integration test that projection does not affect enrichment (internal derives still propagate). Test empty projection (returns nothing). Test projection with nonexistent feature name (ignored, no error).

**Why third:** Builds on Phase 2 (needs enrichment to be correct before filtering).

### Phase 4: Ephemeral Pipeline Lifecycle (Rust)

Add `ephemeral` flag, snapshot filtering, memory limits. Add UNREGISTER command or `DELETE /pipelines/:name` support for cleanup.

**Test plan:** Ephemeral pipeline REGISTER + PUSH + verify state. Restart server, verify ephemeral state is gone. Verify memory limit enforcement. Verify persistent pipelines unaffected.

**Why fourth:** Independent of Phases 2-3 but logically belongs after the new API is stable.

### Phase 5: Old API Removal (Python)

Remove `@st.stream`, `@st.view`, legacy `Stream` class (now `DataStream`). Update `__init__.py`. Migration guide.

**Why last:** Only after the new API is proven in Phases 1-4.

### Dependency Graph

```
Phase 1 (Python SDK) --> Phase 2 (Enrichment) --> Phase 3 (Projection)
                     \                        /
                      -----> Phase 4 (Ephemeral) -- independent
                     
Phase 5 (Remove old API) depends on Phase 1 being stable
```

Phases 3 and 4 can run in parallel after Phase 2.

---

## 11. Open Questions

1. **Should PUSH response change for new API?** Currently, PUSH to a keyless stream returns an empty FeatureMap. With the new API, users push to a `@tl.source` (keyless) but want features from a downstream `@tl.dataset` (keyed). Options:
   - (a) Keep current behavior: PUSH returns primary stream features. Client uses GET for downstream features. (Simplest, no wire change.)
   - (b) New flag in PUSH command: "return features from stream X instead of primary." (More useful, but wire protocol change.)
   - **Recommendation: (a) for v2.0.** The async push path (fire-and-forget) is the hot path and returns nothing anyway. Sync push returning downstream features would require waiting for cascade completion, which already happens. This is a follow-up optimization.

2. **Union node schema validation.** When two streams feed into a union, should the engine validate that their events have compatible schemas? Currently no event schema validation exists. **Recommendation: No validation in v2.0.** Operators that reference missing fields already return `Missing` gracefully. Schema validation is a future DX improvement.

3. **Enrichment and event log.** Should the event log store the original event or the enriched event? **Recommendation: Original event.** Enrichment is deterministic from the pipeline definition + original event. Storing enriched events would waste disk and complicate backfill (which re-derives from raw events).

---

## 12. Confidence Assessment

| Area | Confidence | Notes |
|---|---|---|
| New API -> JSON mapping | HIGH | Existing DataFrame SDK already does this; new types are a refactor |
| Enriched event propagation | HIGH | Clear gap, clear fix, ~50 LOC, well-bounded scope |
| Feature projection | HIGH | Trivial filter on existing response path |
| Union node | HIGH | Already works in the DAG; "union" is a Python SDK concept |
| Ephemeral lifecycle | MEDIUM-HIGH | Snapshot filtering is simple; memory limits need careful enforcement design |
| Async path performance impact | MEDIUM | Need to benchmark derive evaluation cost on intermediate cascade stages |
| Old API removal | HIGH | Mechanical deletion, well-scoped |

---

## 13. Files Referenced

- `src/engine/pipeline.rs` lines 0-920 (StreamDefinition, PipelineEngine, push_internal, push_with_cascade_internal, rebuild_dag)
- `src/engine/mod.rs` (module structure)
- `src/types.rs` (FeatureValue, FeatureMap)
- `src/state/store.rs` lines 0-120 (StateStore, EntityState, StreamEntityState, DashMap)
- `src/server/tcp.rs` lines 0-100 (ConcurrentAppState, RwLock<PipelineEngine>)
- `python/tally/_dataframe.py` (existing DataFrame SDK -- Stream, Table, GroupBy, JoinedTable)
- `python/tally/__init__.py` (current public API exports)
- `.planning/PROJECT.md` (current state, v2.0 milestone definition)
- Memory files: `project_v2_api_redesign.md`, `project_on_demand_compute.md`
