# Phase 17: Enriched Event Propagation - Research

**Researched:** 2026-04-12
**Domain:** Rust engine -- cascade enrichment, operator field resolution, DashMap concurrency
**Confidence:** HIGH

## Summary

Phase 17 solves the "enrichment gap" in cascade execution: when upstream datasets compute derived fields or aggregations, downstream datasets cannot currently reference those computed values because the cascade passes only the raw event `serde_json::Value` to each downstream `push_internal()`. This means a pipeline like `RawTxns -> CurrencyConverter(derive: amount_usd) -> UserStats(sum("amount_usd"))` fails because `amount_usd` does not exist in the raw event.

The solution is a **side-channel enrichment accumulator** (`AHashMap<String, serde_json::Value>`) that is threaded through the cascade. After each upstream push, its computed derives and aggregation results are inserted into this accumulator. Downstream operators check the accumulator before falling back to the raw event for field lookups. The accumulator is stack-local to `push_with_cascade_internal` -- it never enters DashMap (C-5) and involves zero clones of the raw event (C-1).

The implementation touches three layers: (1) `Operator::push` gains an enrichment parameter, (2) `push_internal` collects upstream results into the accumulator, (3) `push_with_cascade_internal` threads the accumulator through the topo-order loop.

**Primary recommendation:** Add an `enrichment: Option<&AHashMap<String, serde_json::Value>>` parameter to `Operator::push` and modify `push_with_cascade_internal` to build a stack-local accumulator populated by upstream stream results. This is the minimal-allocation approach that satisfies C-1 and C-5.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
None -- all implementation choices at Claude's discretion (infrastructure phase).

### Claude's Discretion
All implementation choices are at Claude's discretion. Key constraints from STATE.md critical pitfalls:
- C-1: Side-channel AHashMap accumulator, never clone serde_json::Value per hop. Gate: <5% regression from 1.1M eps.
- C-5: Enrichment values never re-enter DashMap during downstream push.

### Deferred Ideas (OUT OF SCOPE)
None.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ENG-01 | Enriched event propagation -- upstream derive results are visible to downstream datasets via a side-channel accumulator (not event clone), enabling multi-stage computed features | Covered by architecture pattern 1 (enrichment accumulator) + operator signature change + cascade threading |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ahash | already in Cargo.toml | AHashMap for enrichment accumulator | Zero new deps; matches existing FeatureMap type alias | [VERIFIED: Cargo.toml inspection]
| serde_json | already in Cargo.toml | Value type for enrichment overlay | Operators already use `&serde_json::Value` for field access | [VERIFIED: operators.rs]
| dashmap | already in Cargo.toml | Per-key entity concurrency | Unchanged; enrichment never enters DashMap | [VERIFIED: store.rs]
| parking_lot | already in Cargo.toml | RwLock on PipelineEngine | Unchanged; engine.read() guard held during cascade | [VERIFIED: tcp.rs]

### Supporting
No new dependencies required. All changes use existing crates.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| AHashMap enrichment | Clone serde_json::Value per hop | Violates C-1; allocation cliff at high throughput |
| Operator signature change | Trait object wrapping event + enrichment | Extra indirection, vtable cost on hot path |
| Inline enrichment into event JSON | Build merged serde_json::Object | serde_json::Value::clone() is deep-clone; ~500ns per hop per event at 10 fields |

**Installation:** No new dependencies.

## Architecture Patterns

### Current Cascade Flow (BEFORE)

```
push_with_cascade_internal(stream, event, store, now, read_features)
  1. primary_features = push_internal(stream, event, store, now, read_features)
  2. BFS to find all reachable downstream
  3. for each downstream in topo_order:
       push_internal(downstream, event, store, now, read_features)
                                   ^^^^^ same raw event -- no upstream results
```

Key observation: `push_internal` at line 506 of pipeline.rs processes operators by calling `op.push(event, now)` where `event` is the raw `serde_json::Value`. Operators extract fields via `event.get(&self.field)`. Derive expressions evaluate against an `EvalContext` containing the current stream's features + the raw event. **Neither mechanism can see upstream computed fields.** [VERIFIED: pipeline.rs:506-666, operators.rs:94-119]

### Recommended: Enrichment Accumulator Pattern

```
push_with_cascade_internal(stream, event, store, now, read_features)
  1. enrichment = AHashMap::new()        // stack-local, never in DashMap (C-5)
  2. primary_features = push_internal(stream, event, &enrichment, store, now, read_features)
  3. collect upstream derives/aggregation results into enrichment
  4. BFS downstream
  5. for each downstream in topo_order:
       push_internal(downstream, event, &enrichment, store, now, read_features)
       collect this stream's results into enrichment (for further downstream)
```

### Pattern 1: Operator Field Resolution with Enrichment

**What:** Every operator's `push()` method gains an optional enrichment overlay. Field lookup becomes: enrichment -> event -> error/skip.

**When to use:** During cascade execution when enrichment is non-empty.

**Implementation approach:**

```rust
// New Operator trait signature
pub trait Operator: std::fmt::Debug + Send {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError>;
    fn read(&mut self, now: SystemTime) -> FeatureValue;
}

// Helper for field resolution (used by all operators)
fn resolve_field<'a>(
    field: &str,
    event: &'a serde_json::Value,
    enrichment: Option<&'a AHashMap<String, serde_json::Value>>,
) -> Option<&'a serde_json::Value> {
    // Enrichment takes priority (upstream computed values)
    if let Some(enr) = enrichment {
        if let Some(val) = enr.get(field) {
            return Some(val);
        }
    }
    // Fall back to raw event
    event.get(field)
}
```
[ASSUMED -- design choice, not verified against external source]

**Why this approach over alternatives:**
- **Zero allocation:** AHashMap is stack-local, no heap per event. Only string keys + serde_json::Value (from derive eval, already computed).
- **C-1 compliance:** Never clones the raw event. The enrichment map grows incrementally as cascade progresses.
- **C-5 compliance:** The enrichment AHashMap is a local variable in `push_with_cascade_internal`. It never enters DashMap. Two concurrent connections each have their own stack-local enrichment.

### Pattern 2: Enrichment Collection After Each Cascade Step

**What:** After `push_internal` processes a stream, its computed features (derives + operator reads) are inserted into the enrichment accumulator for downstream use.

**Key design decision:** What gets enriched?

| Option | Fields in accumulator | Pros | Cons |
|--------|----------------------|------|------|
| **A: Derives only** | Only `FeatureDef::Derive` results | Minimal; derives are the "computed" fields | Can't aggregate an upstream aggregation (e.g., downstream sum of upstream count) |
| **B: All features** | All operator reads + derives | Full composability | Enrichment map grows larger per hop |
| **C: Explicitly marked** | New `enriched: bool` on FeatureDef | User controls what propagates | Requires schema change, more complexity |

**Recommendation: Option B (all features).** This matches the stated use case ("map -> group_by -> downstream sum") and requires no schema extension. The enrichment map size is bounded by the number of features per stream (typically 5-20), and the values are `serde_json::Value` scalars (numbers, strings), not large objects. [ASSUMED -- design choice]

### Pattern 3: Enrichment Key Namespacing

**What:** Enrichment keys must avoid collision between streams. Two upstream streams might both have a feature named `count_1h`.

**Approach:** Use qualified names `StreamName.feature_name` in the enrichment map. Downstream operators reference the field as-is from the event, but derive expressions can use `Upstream.field` syntax (already supported by `FieldRef::Qualified` in expression.rs). For operators (sum, count, etc.) that take a `field` parameter, the user writes `sum("Upstream.count_1h")` and the field resolver checks enrichment with the qualified key.

**Alternative:** Flat namespace (unqualified names). Simpler but risks collision. Given that the expression evaluator already supports qualified `StreamName.field` syntax, qualified namespacing is natural.

**Recommendation:** Support BOTH qualified and unqualified names in enrichment lookup. Unqualified names use last-write-wins (later topo-order stream overwrites earlier). Qualified names are always unambiguous. This matches how `EvalContext::resolve_field` already handles `FieldRef::Local` vs `FieldRef::Qualified`. [ASSUMED -- design choice]

### Pattern 4: EvalContext Enrichment for Derive Expressions

**What:** Derive expression evaluation in downstream streams should be able to reference upstream enriched values.

**Current code (pipeline.rs:644-661):**
```rust
let ctx = EvalContext {
    features: &features,  // current stream's operator results
    event: Some(event),   // raw event
};
```

**Modified:** Add enrichment source to EvalContext resolution chain:
```rust
pub struct EvalContext<'a> {
    pub features: &'a AHashMap<String, FeatureValue>,
    pub event: Option<&'a serde_json::Value>,
    pub enrichment: Option<&'a AHashMap<String, FeatureValue>>,  // NEW
}

// Resolution order: features -> enrichment -> event -> Missing
```

The enrichment map here is `AHashMap<String, FeatureValue>` (not `serde_json::Value`), since derive evaluation operates on `FeatureValue`. This is a different map from the operator-level enrichment (which uses `serde_json::Value` since operators work with JSON). The cascade function converts between them.

**Key insight:** Two enrichment channels are needed:
1. **For operators** (`AHashMap<String, serde_json::Value>`) -- operators call `event.get()` which returns `&serde_json::Value`
2. **For derives** (`AHashMap<String, FeatureValue>`) -- derive evaluation uses `FeatureValue`

Both are populated from the same upstream push results but with different value types. The conversion cost is minimal (scalar values only). [VERIFIED: expression.rs:304-342, operators.rs:94-119]

### Recommended Project Structure Changes

```
src/engine/
├── pipeline.rs    # MODIFIED: push_internal, push_with_cascade_internal gain enrichment param
├── operators.rs   # MODIFIED: Operator::push gains enrichment param, resolve_field helper
├── expression.rs  # MODIFIED: EvalContext gains enrichment field
└── (others unchanged)
src/server/
├── tcp.rs         # MINOR: push_batch paths pass None for enrichment (non-cascade)
└── (others unchanged)
```

### Anti-Patterns to Avoid
- **Cloning serde_json::Value per cascade hop (C-1 violation):** Every `Value::clone()` on a JSON object allocates. With 10 fields and 3 cascade hops, that's 30 allocations per event. At 1.1M eps, that's 33M allocations/sec -- instant performance cliff.
- **Storing enrichment in DashMap (C-5 violation):** Enrichment is per-push-request state, not per-entity state. Putting it in DashMap would serialize concurrent pushes and corrupt under concurrent access (one connection's enrichment visible to another).
- **Deep hierarchy of trait objects:** Adding a `Box<dyn EventLike>` wrapper around event + enrichment adds vtable indirection on every field lookup. Direct parameter passing avoids this.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Enrichment value type conversion | Custom serialize/deserialize for enrichment | `FeatureValue::to_json_value()` (already exists) | types.rs:41-48 already converts FeatureValue -> serde_json::Value [VERIFIED] |
| Qualified field parsing | New parser for "Stream.field" | Existing `FieldRef::Qualified` in expression.rs | Already parsed at registration time [VERIFIED: expression.rs:39-47] |
| Topological ordering | Re-implement ordering for enrichment | Existing `topo_order` in PipelineEngine | Already computed at DAG rebuild time [VERIFIED: pipeline.rs:273-274] |

## Common Pitfalls

### Pitfall 1: Allocation Cliff at Scale (C-1)
**What goes wrong:** Per-event allocation from cloning `serde_json::Value` or building new JSON objects causes GC-like pressure and throughput collapse.
**Why it happens:** `serde_json::Value::clone()` does a deep recursive clone. A JSON object with 10 fields means 10+ heap allocations.
**How to avoid:** The enrichment accumulator is created ONCE per `push_with_cascade_internal` call and populated incrementally. Values inserted are scalars (f64, i64, String) -- not nested objects. The accumulator itself is stack-allocated (AHashMap grows on heap but amortizes over reuse).
**Warning signs:** Benchmark regression > 5% from 1.1M eps baseline. Profile shows allocation hotspot in `push_with_cascade_internal`.
**Gate:** Full benchmark matrix must pass within -5% of 1.1M eps.

### Pitfall 2: Enrichment Leaking into DashMap (C-5)
**What goes wrong:** If enrichment values are stored in `EntityState` (inside DashMap), concurrent pushes for the same entity key see each other's enrichment, producing incorrect results.
**Why it happens:** Temptation to "cache" enrichment in entity state for future reads.
**How to avoid:** Enrichment accumulator is a local variable in `push_with_cascade_internal`. It is created, populated, consumed, and dropped within a single function call. It is never stored in `EntityState`, `StreamEntityState`, or any DashMap shard.
**Warning signs:** Test with 8 concurrent clients pushing different events shows cross-contamination of enriched values.

### Pitfall 3: Operator Signature Change Ripple
**What goes wrong:** Changing `Operator::push` signature breaks all 16 operator implementations + all test call sites.
**Why it happens:** The Operator trait is implemented by CountOp, SumOp, AvgOp, MinOp, MaxOp, LastOp, DistinctCountOp, StddevOp, PercentileOp, LagOp, EmaOp, LastNOp, FirstOp, ExactMinOp, ExactMaxOp (15 impls), plus OperatorState dispatch in snapshot.rs.
**How to avoid:** Use a helper function `resolve_field()` that all operators call, so the pattern change in each operator is mechanical (replace `event.get(&self.field)` with `resolve_field(&self.field, event, enrichment)`). Batch the change in one commit.
**Warning signs:** Compilation errors after trait change. Missed call sites.

### Pitfall 4: Non-Cascade Paths Must Still Work
**What goes wrong:** Direct `push()` (non-cascade), `push_no_features()`, `push_batch_no_features()` all call `push_internal` which calls operators. If the new signature requires enrichment, all callers must be updated.
**Why it happens:** The enrichment parameter is optional (None for non-cascade paths), but it still changes the function signature.
**How to avoid:** Use `Option<&AHashMap<...>>` so non-cascade callers pass `None`. The `resolve_field()` helper gracefully handles `None` enrichment.

### Pitfall 5: read_features=false Path Must Collect Enrichment
**What goes wrong:** The async push path (`read_features=false`) skips feature read + derive evaluation for performance. But if this stream is part of a cascade, its results are needed by downstream.
**Why it happens:** The no-features optimization was added in Phase 11 to skip the O(m) HLL read cost.
**How to avoid:** For cascade-aware paths, even in async mode, the CASCADE-INTERNAL caller must compute enrichment from upstream. The per-stream `push_internal` with `read_features=false` skips returning features to the CALLER but the cascade wrapper still needs upstream results to populate enrichment. This means: in `push_with_cascade_internal`, when cascade downstream exist, the primary push must always evaluate with `read_features=true` (or at least read the results needed for enrichment), regardless of whether the outer caller wants features. This is a subtle but critical design point.
**Warning signs:** Async batch push with cascade pipeline returns incorrect downstream values or skips enrichment entirely.

### Pitfall 6: where-Clause Evaluation Needs Enrichment Too
**What goes wrong:** A downstream operator has `where="Upstream.status == 'failed'"` -- the where-clause evaluates against an `EvalContext` that doesn't include enrichment.
**Why it happens:** Where-clause evaluation at pipeline.rs:601-612 builds an EvalContext with only `features: &ahash::AHashMap::new()` (empty!) and `event: Some(event)`.
**How to avoid:** Pass enrichment to where-clause EvalContext as well. The enrichment map should be available to all expression evaluation, not just derives.

## Code Examples

### Example 1: resolve_field helper for operators
```rust
// Source: design recommendation [ASSUMED]
// In operators.rs

/// Resolve a field value from enrichment overlay first, then raw event.
/// Used by all field-reading operators (sum, avg, min, max, last, etc.).
pub fn resolve_field<'a>(
    field: &str,
    event: &'a serde_json::Value,
    enrichment: Option<&'a ahash::AHashMap<String, serde_json::Value>>,
) -> Option<&'a serde_json::Value> {
    if let Some(enr) = enrichment {
        if let Some(val) = enr.get(field) {
            return Some(val);
        }
    }
    event.get(field)
}
```

### Example 2: Modified SumOp::push
```rust
// Source: design recommendation [ASSUMED]
impl Operator for SumOp {
    fn push(
        &mut self,
        event: &serde_json::Value,
        enrichment: Option<&ahash::AHashMap<String, serde_json::Value>>,
        now: SystemTime,
    ) -> Result<(), TallyError> {
        match resolve_field(&self.field, event, enrichment) {
            None => {
                if self.optional { Ok(()) }
                else { Err(TallyError::Type { field: self.field.clone(), expected: "numeric".into(), got: "absent".into() }) }
            }
            Some(val) => {
                if let Some(f) = val.as_f64() {
                    self.buffer.add_to_current(f, now);
                    self.event_count.add_to_current(1u64, now);
                    Ok(())
                } else {
                    Err(TallyError::Type { field: self.field.clone(), expected: "numeric".into(), got: format!("{}", val) })
                }
            }
        }
    }
}
```

### Example 3: Enrichment accumulation in cascade
```rust
// Source: design recommendation [ASSUMED]
// In pipeline.rs push_with_cascade_internal

fn push_with_cascade_internal(
    &self,
    stream_name: &str,
    event: &serde_json::Value,
    store: &StateStore,
    now: SystemTime,
    read_features: bool,
) -> Result<FeatureMap, TallyError> {
    // Stack-local enrichment accumulator (C-5: never enters DashMap)
    let mut enrichment_json: AHashMap<String, serde_json::Value> = AHashMap::new();
    let mut enrichment_fv: AHashMap<String, FeatureValue> = AHashMap::new();

    // Primary push -- always read features when downstream exists, for enrichment
    let has_downstream = self.downstream_map.contains_key(stream_name);
    let primary_read = read_features || has_downstream;
    let primary_features = self.push_internal_enriched(
        stream_name, event, None, store, now, primary_read,
    )?;

    // Populate enrichment from primary stream results
    if has_downstream {
        for (name, value) in &primary_features {
            let qualified = format!("{}.{}", stream_name, name);
            enrichment_json.insert(qualified.clone(), value.to_json_value());
            enrichment_json.insert(name.clone(), value.to_json_value()); // unqualified
            enrichment_fv.insert(qualified, value.clone());
            enrichment_fv.insert(name.clone(), value.clone()); // unqualified
        }
    }

    // ... BFS downstream ...

    for stream_in_order in &self.topo_order {
        // ... existing checks ...
        let ds_features = self.push_internal_enriched(
            stream_in_order, event,
            Some(&enrichment_json),
            store, now,
            read_features || has_further_downstream,
        )?;

        // Accumulate this stream's results for further downstream
        for (name, value) in &ds_features {
            let qualified = format!("{}.{}", stream_in_order, name);
            enrichment_json.insert(qualified.clone(), value.to_json_value());
            enrichment_json.insert(name.clone(), value.to_json_value());
            enrichment_fv.insert(qualified, value.clone());
            enrichment_fv.insert(name.clone(), value.clone());
        }
    }

    Ok(primary_features)
}
```

### Example 4: End-to-end test scenario
```python
# Python SDK test: upstream derive -> downstream aggregation
import tally as tl
from tally import source, dataset, group_by

@source
class RawTxns:
    pass

@dataset(depends_on=[RawTxns])
class CurrencyNorm:
    """Upstream: derive amount_usd from raw event fields."""
    features = group_by("user_id").agg(
        last_amount=tl.last("amount"),
    )
    amount_usd = tl.derive("_event.amount * _event.exchange_rate")

@dataset(depends_on=[CurrencyNorm])
class UserStats:
    """Downstream: aggregate the upstream-derived amount_usd."""
    features = group_by("user_id").agg(
        total_usd_1h=tl.sum("CurrencyNorm.amount_usd", window="1h"),
        tx_count_1h=tl.count(window="1h"),
    )

# Push: raw event has amount=100, exchange_rate=1.2
# CurrencyNorm computes amount_usd=120.0
# UserStats.total_usd_1h should sum 120.0
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Pass raw event to all cascade hops | Enrichment accumulator alongside raw event | Phase 17 (this phase) | Enables multi-stage computed features |
| Operator::push takes only event+now | Operator::push takes event+enrichment+now | Phase 17 (this phase) | All 15 operator impls updated |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Option B (all features in enrichment) is the right granularity | Architecture Pattern 2 | If too much data in enrichment map, slight perf regression; fixable by narrowing to derives-only |
| A2 | Both qualified and unqualified names in enrichment | Architecture Pattern 3 | Collision risk with unqualified names; could restrict to qualified-only but less ergonomic |
| A3 | Two enrichment maps needed (serde_json::Value + FeatureValue) | Architecture Pattern 4 | Could unify to one type with conversion at lookup time; minor perf tradeoff |
| A4 | Primary push must use read_features=true when downstream exists | Pitfall 5 | If skipped, async cascade would produce incorrect downstream results |

## Open Questions (RESOLVED)

1. **Should enrichment propagate in async/batch cascade paths?** — RESOLVED: YES. Primary push in cascade reads features when downstream exists, even in async mode. Outer caller still gets empty FeatureMap (async contract). Plan 17-02 implements this.

2. **Enrichment map key format for operators?** — RESOLVED: Support BOTH unqualified and qualified. Insert both `"amount_usd"` and `"CurrencyNorm.amount_usd"` into operator-level enrichment. Last-writer-wins for unqualified collisions. Plan 17-01 implements resolve_field().

3. **Should CountOp and other no-field operators receive enrichment?** — RESOLVED: YES for trait compliance, CountOp ignores the parameter. Zero performance cost. Plan 17-01 implements this.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust built-in) + pytest (Python integration) |
| Config file | Cargo.toml (test config), python/pyproject.toml |
| Quick run command | `cargo test --lib -- enrichment` |
| Full suite command | `cargo test && cd python && pytest` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ENG-01a | Upstream derive visible to downstream operator | integration | `cargo test test_enriched_derive_to_downstream_sum` | Wave 0 |
| ENG-01b | Multi-level cascade enrichment (3+ hops) | integration | `cargo test test_enriched_multi_hop_cascade` | Wave 0 |
| ENG-01c | Enrichment works with read_features=false (async path) | integration | `cargo test test_enriched_cascade_async_mode` | Wave 0 |
| ENG-01d | Benchmark gate: <5% regression from 1.1M eps (C-1) | benchmark | `python3 benchmark/tally-throughput/bench.py --matrix --clients 8` | Existing bench.py |
| ENG-01e | 8-concurrent-client enrichment correctness (C-5) | integration | `cargo test test_enriched_concurrent_clients` | Wave 0 |
| ENG-01f | where-clause with enriched field | unit | `cargo test test_enriched_where_clause` | Wave 0 |
| ENG-01g | Qualified and unqualified field resolution | unit | `cargo test test_enriched_field_resolution` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib`
- **Per wave merge:** `cargo test && cd python && pytest`
- **Phase gate:** Full suite + benchmark matrix green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] Enrichment unit tests in `src/engine/pipeline.rs` (inline tests)
- [ ] Integration tests in `tests/test_pipeline.rs` for cascade enrichment
- [ ] Concurrent enrichment test in `tests/test_concurrent.rs`
- [ ] No new framework install needed -- cargo test already works

## Security Domain

This phase involves no user-facing input changes, no authentication, no network protocol changes, and no new data persistence. The enrichment accumulator is internal to the engine and never exposed to clients.

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | n/a |
| V3 Session Management | no | n/a |
| V4 Access Control | no | n/a |
| V5 Input Validation | no | Event validation unchanged; enrichment is server-internal |
| V6 Cryptography | no | n/a |

No new threat patterns introduced. Enrichment values are computed from already-validated event data.

## Sources

### Primary (HIGH confidence)
- Codebase inspection: `src/engine/pipeline.rs` (push_internal, push_with_cascade_internal, topo_order)
- Codebase inspection: `src/engine/operators.rs` (Operator trait, SumOp::push field access pattern)
- Codebase inspection: `src/engine/expression.rs` (EvalContext, FieldRef, resolve_field)
- Codebase inspection: `src/state/store.rs` (DashMap entity store, EntityState structure)
- Codebase inspection: `src/server/tcp.rs` (handle_push_core_ex, cascade integration)
- Codebase inspection: `src/types.rs` (FeatureValue, FeatureMap type alias)

### Secondary (MEDIUM confidence)
- `.planning/research/SUMMARY.md` -- v2.0 research identified C-1 and C-5 pitfalls and recommended side-channel AHashMap approach

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all changes within existing crates
- Architecture: HIGH -- direct codebase analysis reveals exact change points
- Pitfalls: HIGH -- C-1/C-5 well-documented in project research; operator signature ripple verified by counting impls

**Research date:** 2026-04-12
**Valid until:** 2026-05-12 (stable Rust codebase, no external dependency changes)
