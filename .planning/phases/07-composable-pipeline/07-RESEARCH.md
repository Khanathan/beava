# Phase 7: Composable Pipeline - Research

**Researched:** 2026-04-10
**Domain:** DAG-based pipeline cascade, keyless streams, topological execution, cycle detection
**Confidence:** HIGH

## Summary

Phase 7 transforms Tally from a flat push-through engine into a composable pipeline where events cascade through dependent streams in topological order. The core challenge is adding three capabilities to the existing `PipelineEngine`: (1) keyless streams that act as pure ingestion points with no aggregation and no entity key, (2) explicit `depends_on` relationships between streams that form a directed acyclic graph, and (3) a cascade execution engine that walks the DAG in topological order after each push, forwarding events to downstream dependents.

The existing fan-out logic in `tcp.rs` (lines 164-189) handles implicit multi-stream updates based on key_field presence in the event payload. The new cascade system is fundamentally different: it uses explicit `depends_on` declarations to build a DAG, executes streams in topological order via petgraph, and supports keyless-to-keyed transitions where a keyless stream has no key_field and its downstream keyed streams extract their own keys from the cascaded event. The fan-out mechanism will coexist with cascade -- fan-out handles cross-key implicit updates (same event, different keys), while cascade handles explicit pipeline dependency chains.

The implementation touches four layers: (1) Python SDK (`_stream.py`, `_operators.py`) for `depends_on`, optional `key`, and `filter` parameters; (2) Protocol (`protocol.rs`) for `RegisterRequest` schema changes; (3) Pipeline engine (`pipeline.rs`) for DAG construction, topological sort, cycle detection, and cascade execution; and (4) TCP handler (`tcp.rs`) for integrating cascade into the push flow.

**Primary recommendation:** Use petgraph 0.8.3 `DiGraph` for DAG construction with `toposort()` for topological ordering and cycle detection. Build the DAG at registration time (not per-push), cache the topological order, and invalidate/rebuild on new stream registration. Cascade execution reuses the existing `PipelineEngine::push()` method for each downstream stream.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Keyless streams defined via `@st.stream()` with no `key` parameter -- `key` becomes optional on the decorator
- Keyless streams have no windowed operators (count, sum, avg, etc.) -- they are pure ingest + cascade points
- Keyless streams CAN have derive expressions that compute from `_event.*` fields for enrichment/filtering before cascade
- Keyless streams are invisible to GET -- no entity state exists, GET returns features from keyed streams only
- Events pushed to keyless streams are persisted to the event log and cascaded to all downstream dependents
- Pushing to a keyless stream returns empty feature map `{}` -- no features to return
- If a downstream stream's key_field is missing from the cascaded event, that downstream stream is silently skipped (LEFT JOIN spirit)
- Keyed-to-keyed `depends_on` is supported -- downstream stream extracts its own key from the event (re-keying)
- Multi-level cascades supported (A->B->C, arbitrary depth) -- topological sort handles ordering via petgraph
- Events cascade through the entire DAG in a single push-through cycle (synchronous)
- `depends_on` expressed as class references: `depends_on=[RawEvents]` -- resolved to string names at serialization
- Stream-level `filter="expr"` parameter on `@st.stream()` -- applies before all operators, uses existing expression engine
- Multiple `depends_on` sources supported -- stream receives events from ALL upstream streams
- Type enforcement at REGISTER time: cycle detection, depends_on streams must exist, key_field presence validated
- Explicit LEFT JOIN semantics via `st.lookup()` -- when referencing features from another stream via foreign key, Missing returned if key not found (not error)

### Claude's Discretion
- petgraph graph type selection (DiGraph vs StableGraph)
- Internal cascade event representation (clone event vs reference)
- Registration order handling (does depends_on target need to be registered first, or deferred resolution?)

### Deferred Ideas (OUT OF SCOPE)
- Full Fennel-style `.filter(lambda df: ...)` with Python lambda support (Tally uses string expressions, not Python lambdas -- keeps Python out of hot path)
- Complex DAG transformations: map, flatMap on keyless streams (PIPE-F1, deferred to v1.2+)
- Schema migration for running operators (SCHM-F1, deferred to v1.2+)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PIPE-01 | User can define a keyless stream that ingests raw events without aggregation | Make `key_field` optional in `StreamDefinition` and `RegisterRequest`; keyless streams have `key_field: None`, can only have `Derive` features referencing `_event.*` fields; push returns `{}`, no entity state created |
| PIPE-02 | User can define a keyed stream with explicit `depends_on` declaring upstream dependencies | Add `depends_on: Vec<String>` to `StreamDefinition` and `RegisterRequest`; Python SDK `@st.stream(key="user_id", depends_on=[RawEvents])` serializes class refs to names; validated at REGISTER time |
| PIPE-03 | Events pushed to any stream automatically cascade through all dependent streams in topological order | Build `DiGraph` at registration time with petgraph; `toposort()` gives execution order; cascade engine walks order after primary push, calling `push()` on each downstream stream |
| PIPE-04 | Circular dependencies are detected and rejected at registration time | petgraph `toposort()` returns `Err(Cycle)` when graph has cycles; extract `node_id()` to identify the cycle participant and include it in the error message |
| PIPE-05 | Dependent streams receive null/missing for upstream values not yet available (LEFT JOIN semantics) | Already implemented: `EvalContext::resolve_field()` returns `FeatureValue::Missing` for unknown fields; `eval_binary()` propagates Missing with SQL NULL semantics; `st.lookup()` returns Missing when foreign key not found |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Language:** Rust (single binary, memory safety)
- **Threading:** Single-threaded v1 (tokio current_thread, no locks needed in engine)
- **State:** In-memory HashMap, periodic snapshots to disk
- **Protocol:** Custom binary TCP, persistent connections
- **Expression language:** String-based, parsed server-side (keeps Python out of hot path)
- **Hashing:** AHashMap everywhere (locked decision from v1.0)
- **Serialization:** postcard for snapshots/event log, serde_json for protocol payloads
- **Parser:** winnow for expression parsing
- **TDD:** Tests first, then structures, then implementation code
- **SDK name:** `tally` (Python package name is `tally`, not `streamlet`)

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| petgraph | 0.8.3 | DAG construction, topological sort, cycle detection | [VERIFIED: crates.io] Locked decision from v1.1 research. Latest release 2025-09-30. Most widely used Rust graph library. `toposort()` returns `Err(Cycle)` on cycles, exactly what we need for PIPE-04 |
| ahash | 0.8 | HashMap implementation | [VERIFIED: Cargo.toml] Already in use, locked decision |
| winnow | 1.0 | Expression parsing | [VERIFIED: Cargo.toml] Already in use for filter expressions |
| serde/serde_json | 1.0 | JSON serialization for RegisterRequest | [VERIFIED: Cargo.toml] Already in use |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| postcard | 1.1 | Event log serialization | [VERIFIED: Cargo.toml] Already in use. Keyless stream events logged via existing EventLog module |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| petgraph DiGraph | petgraph StableGraph | StableGraph maintains stable node indices across removals -- useful if streams are unregistered/re-registered frequently. DiGraph is simpler and sufficient since full DAG rebuild on registration change is fast |
| petgraph | Manual toposort | Manual DFS-based toposort is ~30 lines of code, avoids dependency. But petgraph is a locked decision and provides proven correctness + error types |

**Installation:**
```toml
# Add to Cargo.toml [dependencies]
petgraph = "0.8"
```

**Version verification:** petgraph 0.8.3 is the latest release, published 2025-09-30. [VERIFIED: crates.io API]

## Architecture Patterns

### Recommended Project Structure Changes

```
src/
├── engine/
│   ├── pipeline.rs       # StreamDefinition changes: optional key_field, depends_on, filter
│   │                     # New: DAG construction, topological sort cache, cascade execution
│   ├── expression.rs     # No changes needed (filter reuses existing parse/eval)
│   └── ...               # No other engine changes
├── server/
│   ├── protocol.rs       # RegisterRequest: optional key_field, depends_on, filter fields
│   └── tcp.rs            # Push handler: integrate cascade after primary push
├── state/
│   └── event_log.rs      # No changes (keyless streams already log via existing append())
python/
└── tally/
    ├── _stream.py        # @st.stream(): key becomes optional, add depends_on, filter params
    └── _operators.py     # No changes needed
```

### Pattern 1: DAG Construction at Registration Time

**What:** Build a petgraph `DiGraph<String, ()>` when streams are registered. Each node is a stream name, each edge represents a `depends_on` relationship (upstream -> downstream). Cache the topological order as a `Vec<String>`. Rebuild the graph on each new stream registration.

**When to use:** Every REGISTER command that registers a stream with `depends_on`.

**Example:**
```rust
// Source: petgraph docs (https://docs.rs/petgraph/latest/petgraph/algo/fn.toposort.html)
use petgraph::graph::DiGraph;
use petgraph::algo::toposort;

// In PipelineEngine:
struct PipelineEngine {
    streams: AHashMap<String, StreamDefinition>,
    views: AHashMap<String, ViewDefinition>,
    raw_register_jsons: AHashMap<String, serde_json::Value>,
    // NEW: DAG for cascade execution
    dag: DiGraph<String, ()>,
    node_indices: AHashMap<String, petgraph::graph::NodeIndex>,
    topo_order: Vec<String>,  // Cached topological order
}

fn rebuild_dag(&mut self) -> Result<(), TallyError> {
    let mut dag = DiGraph::new();
    let mut indices = AHashMap::new();

    // Add all streams as nodes
    for name in self.streams.keys() {
        let idx = dag.add_node(name.clone());
        indices.insert(name.clone(), idx);
    }

    // Add edges for depends_on relationships
    for stream in self.streams.values() {
        if let Some(deps) = &stream.depends_on {
            let downstream_idx = indices[&stream.name];
            for dep in deps {
                let upstream_idx = indices.get(dep).ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "stream '{}' depends_on '{}' which is not registered",
                        stream.name, dep
                    ))
                })?;
                // Edge: upstream -> downstream (data flows this direction)
                dag.add_edge(*upstream_idx, downstream_idx, ());
            }
        }
    }

    // Topological sort -- detects cycles
    let order = toposort(&dag, None).map_err(|cycle| {
        let node = &dag[cycle.node_id()];
        TallyError::Protocol(format!(
            "circular dependency detected involving stream '{}'", node
        ))
    })?;

    self.topo_order = order.iter().map(|idx| dag[*idx].clone()).collect();
    self.dag = dag;
    self.node_indices = indices;
    Ok(())
}
```
[VERIFIED: petgraph `toposort` returns `Result<Vec<NodeIndex>, Cycle<NodeIndex>>`, and `Cycle` has `node_id()` method]

### Pattern 2: Cascade Execution in Push Flow

**What:** After the primary push to a stream, walk the topological order and push the same event to all downstream streams reachable from the pushed stream. Skip downstream streams whose key_field is missing from the event (LEFT JOIN semantics). For keyless streams, push returns `{}`.

**When to use:** Every PUSH command.

**Example:**
```rust
// In PipelineEngine or tcp.rs push handler:
fn cascade_push(
    &self,
    origin_stream: &str,
    event: &serde_json::Value,
    store: &mut StateStore,
    now: SystemTime,
) -> Result<FeatureMap, TallyError> {
    let stream = self.streams.get(origin_stream).ok_or_else(|| {
        TallyError::Protocol(format!("unknown stream: {}", origin_stream))
    })?;

    // For keyless streams: log event, evaluate derives, return empty map
    if stream.key_field.is_none() {
        // Apply stream-level filter if present
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext { features: &AHashMap::new(), event: Some(event) };
            let result = eval(filter_expr, &ctx);
            if !result.is_truthy() {
                return Ok(FeatureMap::new()); // filtered out
            }
        }
        // No entity state to create -- return empty features
        // (Event log append happens in tcp.rs handler)
    } else {
        // Keyed stream: normal push
        // (handled by existing push() method)
    }

    // Walk topological order, push to downstream streams
    // that depend on origin_stream (directly or transitively)
    for downstream_name in &self.topo_order {
        if downstream_name == origin_stream { continue; }
        let downstream = &self.streams[downstream_name];
        // Check if this stream depends (directly) on a stream we already pushed to
        if let Some(ref deps) = downstream.depends_on {
            let should_cascade = /* ... check if origin is upstream ... */;
            if should_cascade {
                // Apply stream-level filter
                if let Some(ref filter_expr) = downstream.filter {
                    let ctx = EvalContext { features: &AHashMap::new(), event: Some(event) };
                    if !eval(filter_expr, &ctx).is_truthy() { continue; }
                }
                // Skip if key_field missing from event (LEFT JOIN semantics)
                if let Some(ref key_field) = downstream.key_field {
                    match event.get(key_field) {
                        Some(serde_json::Value::String(k)) if !k.is_empty() => {
                            let _ = self.push(downstream_name, event, store, now);
                        }
                        _ => continue, // silently skip
                    }
                }
                // If downstream is also keyless, cascade further (recursion via topo order)
            }
        }
    }

    Ok(features)
}
```
[ASSUMED: Exact cascade implementation detail -- will be refined during planning]

### Pattern 3: Keyless Stream as Pure Ingest Point

**What:** A keyless stream has `key_field: None`, no windowed operators (count, sum, etc.), only derive expressions using `_event.*` fields. It creates no entity state, returns `{}` on push, and is invisible to GET.

**When to use:** When the user wants a raw event ingestion point that fans out to downstream keyed streams.

**Example (Python SDK):**
```python
import tally as st

@st.stream()  # No key parameter
class RawEvents:
    # Only derive from _event fields -- no windowed operators allowed
    amount_usd = st.derive("_event.amount * _event.exchange_rate")

@st.stream(key="user_id", depends_on=[RawEvents])
class UserTransactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")

# Push to keyless stream -- cascades to UserTransactions
features = app.push(RawEvents, {
    "user_id": "u123",
    "amount": 50.0,
    "exchange_rate": 1.1
})
# features == {} (keyless streams return empty map)
```

### Pattern 4: Stream-Level Filter Expression

**What:** A filter expression evaluated before any operator processing. If the filter evaluates to falsy, the entire event is skipped for this stream (and its operators). Uses the existing expression engine.

**When to use:** When a downstream stream should only process a subset of cascaded events.

**Example (Python SDK):**
```python
@st.stream(key="user_id", depends_on=[RawEvents], filter="_event.status == 'failed'")
class FailedTransactions:
    failed_count_1h = st.count(window="1h")
```

**Rust implementation:**
```rust
// In StreamDefinition:
pub struct StreamDefinition {
    pub name: String,
    pub key_field: Option<String>,      // None for keyless streams
    pub features: Vec<(String, FeatureDef)>,
    pub entity_ttl: Option<Duration>,
    pub history_ttl: Option<Duration>,
    pub depends_on: Option<Vec<String>>, // NEW: upstream dependencies
    pub filter: Option<Expr>,            // NEW: stream-level filter expression
}
```

### Anti-Patterns to Avoid

- **Rebuilding DAG on every push:** DAG construction and toposort are O(V+E) but unnecessary per-push. Build once at registration time, cache the topological order, and only rebuild when a new stream is registered.

- **Recursive cascade implementation:** Using recursive function calls for cascade risks stack overflow with deep DAGs. Instead, iterate the pre-computed topological order linearly. The topological order guarantees upstream streams are processed before downstream ones.

- **Allowing windowed operators on keyless streams:** Windowed operators (count, sum, avg, etc.) need an entity key to store state. Keyless streams have no key, so they cannot have these operators. Enforce this at REGISTER time.

- **Modifying the event during cascade:** Do not mutate the event JSON as it flows through the DAG. Each stream reads from the same immutable event. If a keyless stream has derive expressions that enrich the event, those are computed features -- they do not modify the original event payload. Downstream streams still read from `_event.*` (the original event), not from upstream stream features.

- **Breaking existing fan-out behavior:** The current fan-out (lines 164-189 in tcp.rs) handles implicit cross-key updates. It must continue working for backward compatibility. Cascade (depends_on) is a separate mechanism. A stream can participate in both: it can be a cascade target (via depends_on) AND a fan-out target (via key_field match).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Topological sort | Custom DFS-based sort | `petgraph::algo::toposort()` | Handles cycle detection automatically, returns typed `Cycle` error with node info. Well-tested, O(V+E) |
| Cycle detection | Visited set tracking | `petgraph::algo::toposort()` | toposort detects cycles as a side effect -- no need for separate cycle detection |
| Graph data structure | `AHashMap<String, Vec<String>>` adjacency list | `petgraph::graph::DiGraph` | Provides NodeIndex-based O(1) lookups, iterator-based traversal, and compatibility with all petgraph algorithms |
| Filter expression evaluation | Custom filter logic | Existing `expression::parse_expr()` + `eval()` | Stream-level filter uses the exact same expression engine as `where` clauses. Already handles `_event.*` field access, string comparisons, boolean logic |

**Key insight:** The expression engine already supports everything needed for stream-level filters. The `EvalContext` with `event: Some(&event)` resolves `_event.field` references, and `eval()` returns `FeatureValue` which can be checked for truthiness. No new parsing or evaluation logic is needed.

## Common Pitfalls

### Pitfall 1: Cascade + Fan-out Interaction
**What goes wrong:** An event pushed to a keyless stream cascades to a keyed stream, which then also triggers fan-out to other streams with different key_fields. This could cause double-processing if the fan-out targets are also depends_on targets.
**Why it happens:** Fan-out and cascade are two independent mechanisms that can both fire for the same event.
**How to avoid:** During cascade execution, skip fan-out for streams that are already in the cascade DAG. Only apply fan-out for the primary push target (the stream the user explicitly pushed to), not for cascade-triggered pushes.
**Warning signs:** Duplicate feature counts, operators receiving the same event twice.

### Pitfall 2: Registration Order Dependencies
**What goes wrong:** If stream A depends_on stream B, but B is registered after A, the DAG build at A's registration time cannot validate that B exists.
**Why it happens:** Python SDK `app.register(A, B, C)` sends individual REGISTER commands sequentially. If A depends_on B, A arrives first.
**How to avoid:** Two approaches: (a) Deferred validation -- allow depends_on references to not-yet-registered streams, validate the full DAG only when all streams are registered (a "finalize" step); OR (b) Require registration in dependency order -- reject if target does not exist. Recommendation: accept all registrations first, rebuild and validate the DAG after each registration. If a stream's depends_on target is not yet registered, the DAG simply doesn't include that edge yet. The next registration that adds the missing target will rebuild and validate the full graph.
**Warning signs:** "unknown stream" errors during registration when streams are registered in arbitrary order.

### Pitfall 3: Keyless Stream Derive Features in Cascade Context
**What goes wrong:** A keyless stream has a derive expression like `_event.amount * _event.rate`. Downstream keyed streams expect to access these computed values, but they don't exist in the `_event` namespace.
**Why it happens:** Derive expressions on keyless streams are evaluated but their results are not injected back into the event payload.
**How to avoid:** Keyless stream derives are cosmetic/diagnostic only in v1.1. They are NOT passed to downstream streams. Downstream streams access the original event via `_event.*`. If enrichment (computed values from keyless streams flowing to downstream streams) is needed, it's a v1.2+ feature. Document this clearly.
**Warning signs:** Users expecting derived values from keyless streams to be available as `_event.*` in downstream streams.

### Pitfall 4: Borrow Conflicts in Cascade Push
**What goes wrong:** The cascade loop needs `&self` (to read stream definitions and topo order) while also needing `&mut store` (to push events). Since `PipelineEngine::push()` takes `&self` and `&mut store`, the same pattern as existing code works.
**Why it happens:** Rust borrow checker prevents aliased mutable references.
**How to avoid:** The existing `push()` method already demonstrates the correct pattern: `&self` for engine, `&mut store` passed separately. Cascade just calls `push()` in a loop. The key insight from Phase 6: collect what you need from `self` before mutating `store`, or use scoped borrows.
**Warning signs:** Compile errors about "cannot borrow `*self` as immutable because it is also borrowed as mutable."

### Pitfall 5: Topological Order Includes Non-Cascade Streams
**What goes wrong:** The topological order contains ALL registered streams, not just those reachable from the pushed stream. This means cascade iterates over irrelevant streams.
**Why it happens:** petgraph `toposort()` returns all nodes in the graph.
**How to avoid:** During cascade, only process streams that are downstream of the pushed stream. Either (a) pre-compute the reachable set for each stream (adjacency list traversal at registration time), or (b) during cascade iteration, skip streams that are not reachable from the origin. Option (a) is O(V+E) at registration time but O(1) at cascade time. Option (b) is simpler. Given typical DAG sizes (5-20 streams), option (b) with a simple check is sufficient.
**Warning signs:** Cascade processing streams that have no dependency relationship with the pushed stream.

## Code Examples

### Example 1: Python SDK -- @st.stream with depends_on and filter
```python
# Source: CONTEXT.md locked decisions + Fennel API mapping
import tally as st

@st.stream()  # Keyless stream -- no key parameter
class RawEvents:
    pass  # Pure ingest point, no operators

@st.stream(key="user_id", depends_on=[RawEvents])
class UserTransactions:
    tx_count_1h = st.count(window="1h")
    tx_sum_1h = st.sum("amount", window="1h")

@st.stream(
    key="user_id",
    depends_on=[RawEvents],
    filter="_event.status == 'failed'"
)
class FailedTransactions:
    failed_count_1h = st.count(window="1h")

@st.view(key="user_id")
class UserRisk:
    failure_rate = st.derive(
        "FailedTransactions.failed_count_1h / UserTransactions.tx_count_1h"
    )

app = st.App("localhost:6400")
app.register(RawEvents, UserTransactions, FailedTransactions, UserRisk)

# Push to keyless stream -- cascades to both keyed streams
features = app.push(RawEvents, {
    "user_id": "u123",
    "amount": 50.0,
    "status": "failed"
})
# features == {} (keyless stream returns empty)

# GET retrieves all keyed stream features
all = app.get("u123")
# all.tx_count_1h == 1
# all.failed_count_1h == 1
```

### Example 2: Python SDK -- _to_register_json with depends_on
```python
# Source: Existing _to_register_json pattern in _stream.py
# New fields in the JSON payload:
{
    "name": "UserTransactions",
    "key_field": "user_id",
    "depends_on": ["RawEvents"],
    "features": [
        {"name": "tx_count_1h", "type": "count", "window": "1h"}
    ]
}

# Keyless stream JSON:
{
    "name": "RawEvents",
    "key_field": null,  # or omitted
    "features": []
}

# Stream with filter:
{
    "name": "FailedTransactions",
    "key_field": "user_id",
    "depends_on": ["RawEvents"],
    "filter": "_event.status == 'failed'",
    "features": [
        {"name": "failed_count_1h", "type": "count", "window": "1h"}
    ]
}
```

### Example 3: Rust -- RegisterRequest Changes
```rust
// Source: Existing RegisterRequest in protocol.rs (line 244)
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    #[serde(default)]
    pub key_field: Option<String>,  // CHANGED: was required String, now optional
    #[serde(default, rename = "type")]
    pub definition_type: Option<String>,
    pub features: Vec<FeatureDefRequest>,
    #[serde(default)]
    pub entity_ttl: Option<String>,
    #[serde(default)]
    pub history_ttl: Option<String>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,  // NEW
    #[serde(default)]
    pub filter: Option<String>,            // NEW: expression string, parsed at registration
}
```
[VERIFIED: RegisterRequest currently has `key_field: String` (required). Making it `Option<String>` with `#[serde(default)]` is backward compatible since existing JSON always includes key_field]

### Example 4: Rust -- StreamDefinition Changes
```rust
// Source: Existing StreamDefinition in pipeline.rs (line 88)
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    pub name: String,
    pub key_field: Option<String>,       // CHANGED: was String, now Option<String>
    pub features: Vec<(String, FeatureDef)>,
    pub entity_ttl: Option<Duration>,
    pub history_ttl: Option<Duration>,
    pub depends_on: Option<Vec<String>>, // NEW
    pub filter: Option<Expr>,            // NEW: pre-parsed at registration
}
```

### Example 5: Rust -- Keyless Stream Push Handling
```rust
// In PipelineEngine::push(), handle keyless stream case:
pub fn push(
    &self,
    stream_name: &str,
    event: &serde_json::Value,
    store: &mut StateStore,
    now: SystemTime,
) -> Result<FeatureMap, TallyError> {
    let stream = self.streams.get(stream_name).ok_or_else(|| {
        TallyError::Protocol(format!("unknown stream: {}", stream_name))
    })?;

    // Apply stream-level filter before any processing
    if let Some(ref filter_expr) = stream.filter {
        let ctx = EvalContext {
            features: &AHashMap::new(),
            event: Some(event),
        };
        let result = eval(filter_expr, &ctx);
        match result {
            FeatureValue::Int(0) | FeatureValue::Missing => {
                return Ok(FeatureMap::new()); // filtered out
            }
            FeatureValue::Float(f) if f == 0.0 => {
                return Ok(FeatureMap::new()); // filtered out
            }
            _ => {} // truthy -- proceed
        }
    }

    // Keyless stream: no entity state, return empty feature map
    if stream.key_field.is_none() {
        return Ok(FeatureMap::new());
    }

    // ... existing keyed stream logic unchanged ...
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Implicit fan-out only (key_field match) | Explicit depends_on + implicit fan-out | Phase 7 | Users declare pipeline topology; events cascade through DAG |
| `key_field: String` (required) | `key_field: Option<String>` | Phase 7 | Enables keyless streams for raw event ingestion |
| Flat push-through (one stream per push) | DAG cascade (one push triggers multiple streams) | Phase 7 | Composable pipeline behavior |

**Deprecated/outdated:**
- Nothing deprecated. Fan-out continues to work alongside cascade.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Keyless stream derives are NOT injected into cascaded event (downstream sees original _event) | Pitfalls, Pattern 3 | If wrong, need event enrichment logic -- adds complexity to cascade. Clarify with user if enrichment is desired |
| A2 | Fan-out should NOT fire for cascade-triggered pushes (only for user-initiated primary push) | Pitfalls | If wrong, could cause duplicate processing. Needs explicit decision |
| A3 | Registration order can be arbitrary -- DAG validates on each registration, not requiring strict order | Pitfalls, Pattern 1 | If wrong, user must carefully order `app.register()` calls. Could be enforced but less ergonomic |
| A4 | DiGraph (not StableGraph) is sufficient since DAG is rebuilt on every registration, not incrementally modified | Standard Stack | If wrong, just swap type -- API is identical |
| A5 | Existing `push()` method can be reused for cascade pushes (each downstream stream gets the same event) | Pattern 2 | If wrong, cascade may need a separate code path. Current push() works as-is since it takes stream_name + event |

## Open Questions

1. **Should cascade-triggered pushes also trigger fan-out?**
   - What we know: Currently fan-out fires for every push in tcp.rs. If cascade calls push() for downstream streams, those pushes could also trigger fan-out.
   - What's unclear: Is this desired behavior? It could cause a cascade push to A (keyed, key=user_id) to also fan out to B (keyed, key=merchant_id) if B's key_field exists in the event.
   - Recommendation: Disable fan-out for cascade-triggered pushes. Only the user's explicit push triggers fan-out. This avoids surprising double-updates and keeps cascade behavior deterministic. The cascade itself handles the explicit dependency chain; fan-out is for implicit cross-key updates on the primary push only.

2. **Should deferred resolution be supported for depends_on targets?**
   - What we know: If A depends_on B, but B is registered after A, strict validation at A's registration would fail.
   - What's unclear: Whether Python SDK always registers in dependency order (it could sort topologically before sending).
   - Recommendation: Allow deferred resolution -- don't require depends_on targets to exist at registration time. Validate the full DAG after each registration. If a depends_on target is never registered, it simply means that stream never receives cascade events (benign). The cycle detection in toposort() validates the final graph shape.

3. **How should cascade interact with event log?**
   - What we know: The primary push logs the event to the origin stream's event log. Cascade pushes also need to log to downstream streams' event logs for backfill/replay to work correctly (Phase 8).
   - What's unclear: Should cascade-triggered event log entries be identical to the original event? Or should they include some metadata (e.g., "cascaded from RawEvents")?
   - Recommendation: Log the same raw event bytes to each downstream stream's event log. No metadata -- keeping it simple. Phase 8 (backfill) will replay events by re-pushing them, which will re-trigger cascade naturally.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rust toolchain | Build | Checked at build time | -- | -- |
| petgraph crate | DAG operations | Will be added to Cargo.toml | 0.8.3 | -- |
| Python 3.10+ | SDK tests | Checked at test time | -- | -- |
| pytest | Python SDK tests | Checked at test time | -- | -- |

No blocking dependencies. petgraph is a pure Rust crate with no system dependencies.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust) + pytest (Python) |
| Config file | Cargo.toml (Rust), python/pyproject.toml (Python) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test && cd python && python -m pytest tests/ -x` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PIPE-01 | Keyless stream with no key, no windowed ops, derives from _event | unit | `cargo test test_keyless -- --exact` | Wave 0 |
| PIPE-01 | Keyless stream returns empty feature map on push | unit | `cargo test test_keyless_push_returns_empty -- --exact` | Wave 0 |
| PIPE-01 | Keyless stream rejects windowed operators at registration | unit | `cargo test test_keyless_rejects_windowed_ops -- --exact` | Wave 0 |
| PIPE-02 | Keyed stream with depends_on registers successfully | unit | `cargo test test_depends_on_registration -- --exact` | Wave 0 |
| PIPE-02 | depends_on serialization from Python SDK | unit (Python) | `python -m pytest python/tests/test_stream.py::TestDependsOn -x` | Wave 0 |
| PIPE-03 | Push to upstream cascades to downstream in topo order | integration | `cargo test test_cascade_push -- --exact` | Wave 0 |
| PIPE-03 | Multi-level cascade (A->B->C) processes all levels | integration | `cargo test test_multi_level_cascade -- --exact` | Wave 0 |
| PIPE-03 | Keyless-to-keyed cascade creates entity state in downstream | integration | `cargo test test_keyless_to_keyed_cascade -- --exact` | Wave 0 |
| PIPE-04 | Circular dependency rejected at registration with error message | unit | `cargo test test_cycle_detection -- --exact` | Wave 0 |
| PIPE-04 | Self-dependency rejected | unit | `cargo test test_self_dependency -- --exact` | Wave 0 |
| PIPE-05 | Missing upstream values return Missing (not error) | unit | `cargo test test_left_join_missing -- --exact` | Wave 0 |
| PIPE-05 | Missing key_field in event skips downstream (silent) | unit | `cargo test test_missing_key_skips -- --exact` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test && cd python && python -m pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_pipeline.rs` -- new tests for keyless stream, depends_on, cascade, cycle detection, LEFT JOIN
- [ ] `src/engine/pipeline.rs` -- inline `#[cfg(test)] mod tests` for DAG construction unit tests
- [ ] `python/tests/test_stream.py` -- new tests for `@st.stream()` without key, depends_on, filter
- [ ] `python/tests/test_integration.py` -- E2E test for cascade push through live server

## Security Domain

Security enforcement is not explicitly disabled in config, but this phase has minimal security surface area:

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | N/A -- internal TCP protocol, no auth in v1 |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | RegisterRequest validation: reject cycles (petgraph toposort), validate depends_on targets exist, validate keyless streams have no windowed operators, validate filter expressions parse successfully (winnow) |
| V6 Cryptography | no | N/A |

### Known Threat Patterns for This Phase

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malicious DAG with deep cascade (DoS) | Denial of Service | Limit DAG depth / cascade chain length. In v1, typical DAGs are small (5-20 streams). No explicit limit needed yet, but topological order iteration is bounded by stream count |
| Circular dependency causing infinite loop | Denial of Service | petgraph `toposort()` detects cycles at registration time, prevents them from entering the system |
| Invalid filter expression causing panic | Denial of Service | winnow parser returns `Err` on invalid expressions; `convert_register_request` propagates error cleanly |

## Sources

### Primary (HIGH confidence)
- [petgraph crates.io](https://crates.io/api/v1/crates/petgraph) -- version 0.8.3, published 2025-09-30
- [petgraph toposort docs](https://docs.rs/petgraph/latest/petgraph/algo/fn.toposort.html) -- `toposort()` API, `Cycle` error type with `node_id()`
- [petgraph Cycle struct](https://docs.rs/petgraph/latest/petgraph/algo/struct.Cycle.html) -- `node_id()` method returns participating node
- Tally codebase -- `src/engine/pipeline.rs`, `src/server/protocol.rs`, `src/server/tcp.rs`, `python/tally/_stream.py`

### Secondary (MEDIUM confidence)
- [Fennel AI Pipeline docs](https://fennel.ai/docs/concepts/pipeline) -- `@inputs` decorator pattern, pipeline composition model
- [petgraph main docs](https://docs.rs/petgraph/latest/petgraph/) -- DiGraph vs StableGraph comparison

### Tertiary (LOW confidence)
- None -- all claims verified against codebase or official docs

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- petgraph is a locked decision, version verified against crates.io, API verified against docs.rs
- Architecture: HIGH -- cascade pattern follows directly from existing push-through architecture, all integration points identified in codebase
- Pitfalls: HIGH -- borrow patterns verified against existing Phase 6 solutions, fan-out interaction identified from codebase review
- Python SDK: HIGH -- `_stream.py` and `_operators.py` patterns are well-established, changes are additive

**Research date:** 2026-04-10
**Valid until:** 2026-05-10 (stable domain, no fast-moving dependencies)
