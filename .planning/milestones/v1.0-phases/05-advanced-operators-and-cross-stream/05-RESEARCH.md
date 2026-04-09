# Phase 5: Advanced Operators and Cross-Stream - Research

**Researched:** 2026-04-09
**Domain:** Rust streaming operators (min/max/last/distinct_count), where-clause filtering, cross-stream views, cross-key lookups, event fan-out
**Confidence:** HIGH

## Summary

Phase 5 completes the operator set and adds cross-stream capabilities to the Tally feature server. The phase decomposes into three distinct workstreams: (1) new operators (MinOp, MaxOp, LastOp, DistinctCountOp with windowed HLL), (2) where-clause filtering on all windowed operators, and (3) cross-stream features (ViewDefinition, qualified field resolution, cross-key lookups via StateStore point-reads, and event fan-out). All three workstreams share a common integration surface -- the OperatorState enum, FeatureDef enum, convert_register_request DTO, and snapshot serialization.

The codebase has well-established patterns from Phases 1-4 that Phase 5 follows directly. CountOp/SumOp/AvgOp provide the exact template for MinOp, MaxOp, and DistinctCountOp. The Python SDK already defines all Phase 5 operator classes (Min, Max, DistinctCount, Last, Lookup) and the @view decorator. The main implementation risk is the HyperLogLog windowed rotation, which requires a custom ring buffer approach because the existing `RingBuffer<T>` requires `T: Copy` and an HLL sketch is 16KB -- too large for Copy semantics. The `RingBuffer<T>` bound must either be relaxed to `Clone` or a specialized HLL ring buffer must be created.

**Primary recommendation:** Implement in three waves: (1) simple operators (min/max/last) + where-clause filtering + snapshot/protocol plumbing, (2) HLL with windowed rotation and DistinctCountOp, (3) cross-stream views, lookups, and fan-out. Each wave is independently testable.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- HLL uses RingBuffer<Hll> pattern -- same architecture as count (RingBuffer<u64>) and sum (RingBuffer<f64>), with HLL sketches as bucket values
- Merge all non-expired buckets via HLL union (bitwise max of registers) on read -- identical sliding window semantics to other operators
- Same window/bucket configuration as count/sum/avg (default: window/30 buckets)
- Memory: ~360KB per distinct_count feature per key (30 buckets x 12KB each) -- accepted tradeoff for accuracy
- Fixed 14-bit precision (2^14 = 16384 registers, ~12KB per sketch) per CLAUDE.md spec
- Epoch swap is event-driven via advance_to(now) -- same pattern as existing RingBuffer, no background timer
- Implement HyperLogLog from scratch in hll.rs per locked decision (external crates require nightly or are minimally maintained)
- Zero events in window returns Missing (consistent with all other operators)
- Unit test asserting serialized HLL size stays within expected bounds
- Filter at pipeline level: evaluate where expression before calling operator.push(); skip push if expression evaluates to false/Missing
- Where clauses can only reference event fields (_event.field) -- where runs before operators update, so current-cycle feature values aren't available
- Missing field in where expression treats as false (skip) -- not an error
- Optional where_expr: Option<Expr> field added to each windowed FeatureDef variant (Count, Sum, Avg, Min, Max, DistinctCount)
- Separate ViewDefinition type -- views have no key_field for push, only derive + lookup features (no windowed operators)
- Views recompute lazily on GET only -- industry consensus (Chalk, Fennel/Databricks, Flink Delta Join all use read-time evaluation)
- PUSH response returns features from the pushed stream only; GET response includes all features from all streams + views for that key
- Qualified field references (e.g. Transactions.tx_count_1h) resolved via stream-aware EvalContext that populates features from all registered streams sharing the entity key
- View registration uses separate REGISTER call with type: "view" -- matches Python SDK @st.view being distinct from @st.stream
- Lookup evaluates by reading target entity's feature from StateStore at eval time -- EvalContext gains &StateStore reference for point reads
- TTL-evicted target entity returns Missing -- per STATE.md blocker: "Missing propagation expected, not panic"
- Fan-out: server-level loop on PUSH -- iterate all registered streams, push event to all streams whose key_field exists in the event JSON
- Fan-out PUSH response: features from primary stream only (the one named in the PUSH command)
- Lookup foreign key extracted from the current event or from the entity's last known value

### Claude's Discretion
- MinOp/MaxOp per-bucket tracking strategy (per-bucket min/max or full scan)
- LastOp internal representation details
- Exact HLL hash function choice (MurmurHash3, xxHash, etc.)
- ViewDefinition struct layout and registration DTO format
- Fan-out iteration order across streams (order should not matter)
- Test fixture design for cross-stream integration tests

### Deferred Ideas (OUT OF SCOPE)
- PUSH response including view features for same-key views -- useful for fraud scoring but adds latency; defer to post-v1
- Configurable HLL precision (allow lower precision for memory savings) -- defer to post-v1
- All-streams-merged fan-out response -- defer to post-v1
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OPS-01 | min operator tracks minimum value of a field within a time window | MinOp using RingBuffer<MinBucket> with f64::INFINITY sentinel; same pattern as SumOp |
| OPS-02 | max operator tracks maximum value of a field within a time window | MaxOp using RingBuffer<MaxBucket> with f64::NEG_INFINITY sentinel; same pattern as SumOp |
| OPS-03 | last operator stores most recent value of a field with timestamp | LastOp with single (FeatureValue, SystemTime) state; no window needed |
| OPS-04 | distinct_count operator uses HyperLogLog with epoch-rotation for windowed approximate unique counts | DistinctCountOp with custom HllRingBuffer; Hll from-scratch in hll.rs; merge-on-read |
| OPS-05 | where-clause filtering supports conditional aggregation | Optional where_expr on FeatureDef windowed variants; eval before push; skip on false/Missing |
| XSTR-01 | @st.view computes derived features across multiple streams for same entity key | ViewDefinition type; lazy eval on GET; stream-aware EvalContext for qualified refs |
| XSTR-02 | st.lookup resolves cross-key feature references | EvalContext with &StateStore; point-read target entity; Missing on evicted |
| XSTR-03 | Single event fans out to update multiple streams | Server-level loop in PUSH handler; primary stream response only |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Language:** Rust, edition 2021 [VERIFIED: Cargo.toml]
- **Threading:** Single-threaded v1 (tokio single-threaded runtime, Arc<Mutex> for shared state) [VERIFIED: tcp.rs]
- **Serialization:** postcard (not bincode) for snapshots per RUSTSEC advisory [VERIFIED: Cargo.toml]
- **HashMap:** AHashMap (not std HashMap) everywhere [VERIFIED: codebase]
- **Expression parser:** winnow [VERIFIED: Cargo.toml]
- **TDD/Contract-first:** Define contracts and write tests before implementation [VERIFIED: MEMORY.md]
- **Operator trait:** `push(&mut self, event, now) -> Result + read(&mut self, now) -> FeatureValue` [VERIFIED: operators.rs]
- **OperatorState enum:** wraps concrete ops for serialization [VERIFIED: snapshot.rs]
- **Missing for empty state:** Zero events in window returns Missing [VERIFIED: all operators]
- **guard_float():** All f64 results checked for NaN/infinity -> Missing [VERIFIED: expression.rs]

## Standard Stack

### Core (already in Cargo.toml -- no new dependencies)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ahash | 0.8 | Fast HashMap | Already used, locked decision |
| winnow | 1.0 | Expression parser | Already used |
| serde | 1.0 | Serialization derives | Already used |
| serde_json | 1.0 | JSON parsing | Already used |
| postcard | 1.1 | Snapshot serialization | Locked decision, replaces bincode |
| tokio | 1.50 | Async runtime | Already used |
| axum | 0.8 | HTTP management API | Already used |
| thiserror | 2.0 | Error types | Already used |

[VERIFIED: Cargo.toml -- no new crate dependencies needed for Phase 5]

### No New Dependencies

Phase 5 requires NO new crate dependencies. The HyperLogLog implementation is from scratch (locked decision). The hash function for HLL can use Rust's standard library or the already-present `ahash` crate's hashing capabilities. Min/max/last operators use existing RingBuffer patterns. Cross-stream views and lookups use existing expression evaluator infrastructure.

**Installation:** No changes to Cargo.toml needed.

## Architecture Patterns

### Recommended File Changes
```
src/engine/
  operators.rs    # ADD: MinOp, MaxOp, LastOp, DistinctCountOp
  window.rs       # MODIFY: relax Copy bound to Clone, OR add bucket_ref/set_bucket methods
  hll.rs          # NEW: HyperLogLog implementation (struct Hll, new/insert/count/merge)
  pipeline.rs     # MODIFY: add FeatureDef variants, ViewDefinition, where-clause eval, fan-out support
  expression.rs   # MODIFY: extend EvalContext with &StateStore for lookup resolution
  view.rs         # NEW (optional): ViewDefinition type if separated from pipeline.rs
  mod.rs          # MODIFY: add hll module
src/state/
  snapshot.rs     # MODIFY: add OperatorState variants (Min, Max, Last, DistinctCount), bump version
  store.rs        # MODIFY: add method for point-read of single feature (for lookup)
src/server/
  protocol.rs     # MODIFY: add where/on fields to DTO, new type branches in convert_register_request
  tcp.rs          # MODIFY: fan-out loop in PUSH handler
  http.rs         # MODIFY: new FeatureDef match arms in pipeline detail endpoint
python/tally/
  _stream.py      # MODIFY: add "type": "view"/"stream" to register JSON
```

### Pattern 1: New Operator Implementation (MinOp/MaxOp/LastOp)
**What:** Follow exact same pattern as CountOp/SumOp/AvgOp -- struct with RingBuffer, implement Operator trait, add OperatorState variant, add FeatureDef variant, add convert_register_request branch.
**When to use:** Every new operator.
**Example (verified from codebase):**
```rust
// Source: src/engine/operators.rs existing pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinOp {
    field: String,
    buffer: RingBuffer<MinBucket>,    // bucket holds per-bucket minimum
    event_count: RingBuffer<u64>,      // track if any events (sentinel vs. actual 0)
    optional: bool,
}

impl Operator for MinOp {
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        // Same field extraction pattern as SumOp
        // On value: update bucket min if new value < current bucket value
    }
    fn read(&mut self, now: SystemTime) -> FeatureValue {
        // advance_to(now), then find minimum across all buckets
        // If event_count.sum_all() == 0, return Missing
    }
}
```

### Pattern 2: MinBucket / MaxBucket Wrapper Types
**What:** Newtype wrappers around f64 that implement Default with sentinel values, enabling use with existing RingBuffer<T>.
**When to use:** Min/Max operators need sentinel default values, not 0.0.
```rust
// MinBucket defaults to INFINITY (any real value is smaller)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MinBucket(pub f64);

impl Default for MinBucket {
    fn default() -> Self { MinBucket(f64::INFINITY) }
}

// MaxBucket defaults to NEG_INFINITY (any real value is larger)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MaxBucket(pub f64);

impl Default for MaxBucket {
    fn default() -> Self { MaxBucket(f64::NEG_INFINITY) }
}
```
**Note:** These wrappers need custom AddAssign (MinBucket should use `min(self, other)` semantics) or a new method on RingBuffer for set-if-less/set-if-greater. Alternatively, use `update_current` instead of `add_to_current`. [ASSUMED]

Actually, a cleaner approach: add a `set_min_current` and `set_max_current` method to RingBuffer, or a generic `update_current` method. The existing `add_to_current` uses `+=` which is wrong for min/max.

### Pattern 3: RingBuffer<Hll> -- Copy Constraint Resolution
**What:** The existing RingBuffer requires `T: Default + Copy`. HLL sketches are 16KB and should NOT be Copy (16KB memcpy per bucket rotation). Two approaches:
**Approach A (recommended): Relax RingBuffer bounds from Copy to Clone.**
- Change `RingBuffer<T: Default + Copy>` to `RingBuffer<T: Default + Clone>`
- Change all `T::default()` bucket zeroing to still work (Clone bound is sufficient)
- Existing operators (CountOp, SumOp, AvgOp) are unaffected because u64 and f64 implement Clone
- RingBuffer struct derives `#[derive(Clone, Serialize, Deserialize)]` already -- no issue
**Approach B: Fixed-array HLL registers.**
- `struct Hll { registers: [u8; 16384] }` -- this IS Copy
- But 16KB Copy is expensive on every bucket rotation
- Not recommended

[VERIFIED: window.rs line 17 -- `pub struct RingBuffer<T: Default + Copy>`]

**Recommendation:** Approach A. Relax Copy to Clone. This is a backward-compatible change (all Copy types are Clone). Add a `set_current` method for min/max that doesn't use AddAssign. [ASSUMED -- needs validation that serde still works]

### Pattern 4: Where-Clause Filtering at Pipeline Level
**What:** Evaluate where expression before calling operator.push(). If false/Missing, skip.
**When to use:** Any windowed FeatureDef with a where_expr.
```rust
// In PipelineEngine::push, for each operator:
if let Some(where_expr) = &feature_def.where_expr {
    let ctx = EvalContext { features: &FeatureMap::new(), event: Some(event) };
    match eval(where_expr, &ctx) {
        FeatureValue::Int(i) if i != 0 => { /* passes filter, proceed to push */ }
        _ => { /* false/Missing/zero: skip push */ continue; }
    }
}
op.push(event, now)?;
```
**Key constraint:** Where clauses can only reference `_event.field` because they run BEFORE operators update. The EvalContext passed to where-eval has empty features (or could just check event fields). [VERIFIED: CONTEXT.md locked decision]

### Pattern 5: Cross-Stream View Evaluation
**What:** ViewDefinition stores derive and lookup feature definitions. Evaluated lazily on GET.
```rust
pub struct ViewDefinition {
    pub name: String,
    pub key_field: String,         // The entity key this view is about
    pub features: Vec<(String, ViewFeatureDef)>,
}

pub enum ViewFeatureDef {
    Derive { expr: Expr },
    Lookup { target_stream: String, target_feature: String, on_field: String },
}
```
**GET path changes:** After collecting features from all streams, iterate views. For each view matching the entity key type, evaluate derives and lookups. Lookup reads from StateStore for the foreign key. [VERIFIED: CONTEXT.md -- views recompute lazily on GET only]

### Pattern 6: Fan-Out in PUSH Handler
**What:** On PUSH, iterate ALL registered streams. If the event contains a stream's key_field, push to that stream too.
```rust
// In tcp.rs handle_sync_command for PUSH:
let primary_features = engine.push(&stream_name, &payload, store, now)?;
// Fan-out: push to other streams whose key_field exists in the event
for stream in engine.list_streams() {
    if stream.name != stream_name {
        if let Some(serde_json::Value::String(key)) = payload.get(&stream.key_field) {
            if !key.is_empty() {
                // Push to secondary stream (ignore errors -- primary response already committed)
                let _ = engine.push(&stream.name, &payload, store, now);
            }
        }
    }
}
// Return primary_features only
```
[VERIFIED: CONTEXT.md -- fan-out PUSH response is primary stream only]

### Anti-Patterns to Avoid
- **Don't materialize views on PUSH:** Views evaluate lazily on GET only. Materializing on PUSH adds latency to the hot path. [VERIFIED: CONTEXT.md locked decision]
- **Don't use RingBuffer::add_to_current for MinOp/MaxOp:** AddAssign adds values; min/max need conditional replacement. A new method or wrapper is needed.
- **Don't use Copy for HLL buckets:** 16KB per-bucket Copy on rotation is expensive. Use Clone with explicit allocation control.
- **Don't evaluate where-clause with current features:** Where expressions run BEFORE operator push, so only event fields are available.
- **Don't panic on missing lookup target:** TTL-evicted targets return Missing, not panic. [VERIFIED: STATE.md blocker]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HyperLogLog | N/A (must hand-roll) | hll.rs from scratch | Locked decision -- external crates require nightly or are minimally maintained |
| Hash function for HLL | Custom hash | `ahash::RandomState` or SipHash | ahash already in deps; any well-distributed 64-bit hash works |
| Expression parsing | Custom parser | Existing winnow-based parser in expression.rs | Already handles all needed syntax including FieldRef::Qualified |
| Snapshot serialization | Custom binary | postcard (already in use) | Locked decision from Phase 4 |
| Ring buffer windowing | Custom for each op | Existing RingBuffer<T> (with Clone relaxation) | Proven correct with existing operators |

**Key insight:** The HLL is the only truly new data structure. Everything else reuses existing infrastructure with new variants.

## Common Pitfalls

### Pitfall 1: RingBuffer Copy Bound Blocks HLL
**What goes wrong:** `RingBuffer<Hll>` fails to compile because Hll (with Vec<u8> registers) doesn't implement Copy.
**Why it happens:** RingBuffer was designed for primitive types (u64, f64). HLL sketches are 16KB heap-allocated structures.
**How to avoid:** Relax the bound from `T: Default + Copy` to `T: Default + Clone` before implementing DistinctCountOp. Verify all existing operators still compile (they will -- Copy implies Clone).
**Warning signs:** Compilation error: "the trait `Copy` is not implemented for `Hll`"

### Pitfall 2: Min/Max Sentinel Confusion with Default
**What goes wrong:** MinOp initialized with f64 default (0.0) reports 0.0 as minimum even when no events have the field.
**Why it happens:** f64::default() is 0.0, which is a valid minimum. Need sentinel values.
**How to avoid:** Use MinBucket(f64::INFINITY) / MaxBucket(f64::NEG_INFINITY) wrapper types, OR use a parallel event_count RingBuffer<u64> (same pattern as SumOp).
**Warning signs:** Min/Max returns 0.0 or Float(0.0) instead of Missing when no events exist.

### Pitfall 3: Where-Clause Accessing Feature Values
**What goes wrong:** Where expression references a feature like `tx_count_1h` instead of an event field like `_event.status`, and silently evaluates to Missing (skipping all events).
**Why it happens:** Where expressions are evaluated BEFORE operators update. Feature values from the current cycle aren't available.
**How to avoid:** Document that where clauses should only use `_event.field` syntax. Consider validation at registration time (reject non-event fields in where expressions). [ASSUMED -- validation approach is discretionary]
**Warning signs:** Filtered aggregation always returns Missing.

### Pitfall 4: HLL Merge Overflow on Read
**What goes wrong:** Merging 30 HLL sketches on every read is expensive (~30 x 16KB = 480KB of register comparisons).
**Why it happens:** Merge-on-read is the correct design (matching Redis PFMERGE and Flink HOP windows), but naive implementation per-read could be slow.
**How to avoid:** Optimize merge loop: iterate once through all 16384 register positions, taking max across all non-expired bucket registers. Can be done in a single pass by iterating registers, not buckets.
**Warning signs:** p99 GET latency exceeds 50us target with distinct_count features.

### Pitfall 5: Snapshot Version Bump Breaks Backward Compatibility
**What goes wrong:** Adding new OperatorState variants (Min, Max, DistinctCount, Last) changes the postcard enum discriminant. Old snapshots fail to deserialize.
**Why it happens:** postcard uses sequential discriminants. Adding variants shifts existing ones if not appended.
**How to avoid:** Always APPEND new variants to the end of OperatorState enum. Bump SNAPSHOT_FORMAT_VERSION from 1 to 2. Old snapshots with version 1 return None (clean startup). [VERIFIED: snapshot.rs uses version byte check]
**Warning signs:** Server crashes on startup loading old snapshot.

### Pitfall 6: Fan-Out Infinite Loop
**What goes wrong:** If stream A and stream B both have the same key_field, fan-out could potentially create circular updates or double-count events.
**Why it happens:** Fan-out iterates ALL streams matching the event's key fields.
**How to avoid:** Fan-out should skip the primary stream (already handled by `stream.name != stream_name` check). Each stream is independent -- pushing the same event to two streams with the same key_field is correct (they maintain separate operator states). No circular risk because events are pushed once per stream.
**Warning signs:** Event counts doubled.

### Pitfall 7: EvalContext Lifetime Issues with &StateStore
**What goes wrong:** Adding `&StateStore` to EvalContext creates borrow conflicts when the same function holds `&mut StateStore` for operator reads and `&StateStore` for lookup resolution.
**Why it happens:** Rust borrow checker prevents simultaneous mutable and immutable references.
**How to avoid:** For GET path: read all operator features first (needs &mut), then evaluate views/lookups with &StateStore (immutable). For PUSH path: no lookups needed (views are GET-only). Use the destructured borrow pattern already established in tcp.rs.
**Warning signs:** Compilation error: "cannot borrow `*store` as immutable because it is also borrowed as mutable"

### Pitfall 8: Lookup Foreign Key Resolution
**What goes wrong:** Lookup needs to find the foreign entity key (e.g., merchant_id) but the event is being evaluated for a different entity key (e.g., user_id).
**Why it happens:** The lookup `on` field specifies which event field contains the foreign key. During GET (no event available), the foreign key might come from a `last` operator value.
**How to avoid:** During GET with no event context, lookup should try to resolve the foreign key from the entity's stored features (e.g., if `last_merchant_id` is a feature, use that). If no foreign key available, return Missing. [VERIFIED: CONTEXT.md -- "Lookup foreign key extracted from the current event or from the entity's last known value"]
**Warning signs:** Lookup always returns Missing on GET because no event is available.

## Code Examples

### HyperLogLog Core Implementation
```rust
// Source: Standard HLL algorithm, adapted for Tally
// Reference: https://www.arunma.com/2023/05/01/build-your-own-hyperloglog/

use serde::{Serialize, Deserialize};

const HLL_P: usize = 14;                    // Precision: 14 bits
const HLL_M: usize = 1 << HLL_P;            // 16384 registers
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_M as f64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hll {
    registers: Vec<u8>,  // Use Vec<u8> for Clone (not [u8; 16384] which is Copy but 16KB)
}

impl Default for Hll {
    fn default() -> Self {
        Self { registers: vec![0u8; HLL_M] }
    }
}

impl Hll {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&mut self, value: &str) {
        let hash = hash_value(value);  // 64-bit hash
        let index = (hash >> (64 - HLL_P)) as usize;  // Top 14 bits -> register index
        let remaining = (hash << HLL_P) | (1 << (HLL_P - 1));  // Remaining bits
        let leading_zeros = remaining.leading_zeros() as u8 + 1;
        self.registers[index] = self.registers[index].max(leading_zeros);
    }

    pub fn count(&self) -> f64 {
        let sum: f64 = self.registers.iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();
        let raw = HLL_ALPHA * (HLL_M as f64) * (HLL_M as f64) / sum;
        // Small range correction
        if raw <= 2.5 * HLL_M as f64 {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                return (HLL_M as f64) * (HLL_M as f64 / zeros as f64).ln();
            }
        }
        raw
    }

    /// Merge another HLL into this one (union: bitwise max of registers).
    pub fn merge(&mut self, other: &Hll) {
        for (a, &b) in self.registers.iter_mut().zip(other.registers.iter()) {
            *a = (*a).max(b);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|&r| r == 0)
    }
}

fn hash_value(value: &str) -> u64 {
    // Use ahash (already a dependency) for fast, well-distributed hashing
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}
```

### DistinctCountOp with HLL Ring Buffer
```rust
// Custom ring buffer for HLL (doesn't use add_to_current, uses insert-to-current instead)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistinctCountOp {
    field: String,
    buckets: Vec<Hll>,           // Ring buffer of HLL sketches
    head: usize,
    bucket_duration: Duration,
    window_duration: Duration,
    current_bucket_start: Option<SystemTime>,
    optional: bool,
}

impl Operator for DistinctCountOp {
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        self.advance_to(now);
        let value = extract_string_field(event, &self.field)?;
        self.buckets[self.head].insert(&value);
        Ok(())
    }

    fn read(&mut self, now: SystemTime) -> FeatureValue {
        self.advance_to(now);
        // Merge all non-empty buckets into a temp HLL
        let mut merged = Hll::new();
        let mut any_data = false;
        for bucket in &self.buckets {
            if !bucket.is_empty() {
                merged.merge(bucket);
                any_data = true;
            }
        }
        if !any_data {
            FeatureValue::Missing
        } else {
            FeatureValue::Int(merged.count().round() as i64)
        }
    }
}
```

### LastOp (No Window)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastOp {
    field: String,
    value: Option<(FeatureValue, SystemTime)>,  // (last_value, timestamp)
}

impl Operator for LastOp {
    fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        if let Some(val) = event.get(&self.field) {
            let fv = json_to_feature_value(val);
            self.value = Some((fv, now));
        }
        // Missing field: do nothing (retain previous value)
        Ok(())
    }

    fn read(&mut self, _now: SystemTime) -> FeatureValue {
        match &self.value {
            Some((val, _ts)) => val.clone(),
            None => FeatureValue::Missing,
        }
    }
}
```

### ViewDefinition and Lookup Resolution
```rust
pub struct ViewDefinition {
    pub name: String,
    pub key_field: String,
    pub features: Vec<(String, ViewFeatureDef)>,
}

pub enum ViewFeatureDef {
    Derive { expr: Expr },
    Lookup {
        target_stream: String,
        target_feature: String,
        on_field: String,  // event field or entity feature for foreign key
    },
}

// In PipelineEngine::get_features, after collecting stream features:
fn evaluate_view_features(
    views: &AHashMap<String, ViewDefinition>,
    key: &str,
    features: &mut FeatureMap,
    store: &StateStore,
    event: Option<&serde_json::Value>,
    now: SystemTime,
) {
    for view in views.values() {
        if view.key_field == /* matches entity key type */ {
            for (name, def) in &view.features {
                match def {
                    ViewFeatureDef::Derive { expr } => {
                        let ctx = EvalContext { features, event };
                        features.insert(name.clone(), eval(expr, &ctx));
                    }
                    ViewFeatureDef::Lookup { target_stream, target_feature, on_field } => {
                        // Resolve foreign key from event or entity features
                        let foreign_key = resolve_foreign_key(on_field, event, features);
                        if let Some(fk) = foreign_key {
                            // Point-read from StateStore
                            let target_features = store.get_all_features(&fk, now);
                            let qualified = format!("{}.{}", target_stream, target_feature);
                            let value = target_features.get(target_feature)
                                .cloned()
                                .unwrap_or(FeatureValue::Missing);
                            features.insert(name.clone(), value);
                        } else {
                            features.insert(name.clone(), FeatureValue::Missing);
                        }
                    }
                }
            }
        }
    }
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| postcard for snapshots (v1) | postcard v1 with version bump | Phase 5 | New OperatorState variants require SNAPSHOT_FORMAT_VERSION bump to 2 |
| Single-stream push | Fan-out push to multiple streams | Phase 5 | PUSH handler iterates all registered streams |
| EvalContext with features only | EvalContext with features + &StateStore | Phase 5 | Lookup resolution requires StateStore access |
| Only streams in PipelineEngine | Streams + ViewDefinitions | Phase 5 | PipelineEngine gains views AHashMap |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Relaxing RingBuffer<T> from Copy to Clone is backward-compatible and postcard-safe | Architecture Pattern 3 | HIGH -- if postcard/serde requires Copy, need alternative approach (fixed-array HLL or custom ring buffer) |
| A2 | ahash::AHasher provides sufficient distribution for HLL hash function | Code Examples | LOW -- any well-distributed hash works; ahash is fast and already a dependency |
| A3 | MinBucket/MaxBucket wrapper types with custom Default are the cleanest approach for sentinel values | Pattern 2 | LOW -- alternative is parallel event_count buffer (proven with SumOp) |
| A4 | Where-clause registration validation (reject non-event fields) is not needed | Pitfall 3 | MEDIUM -- users could write broken where clauses; documentation may suffice |
| A5 | Lookup foreign key resolution from entity features (not just event) works during GET | Pitfall 8 | MEDIUM -- requires that a `last` operator captured the foreign key; otherwise Missing |
| A6 | DistinctCountOp needs its own ring buffer logic (cannot reuse RingBuffer<Hll> with add_to_current) | Code Examples | LOW -- HLL insert is fundamentally different from numeric add; custom advance_to is needed |

## Open Questions (RESOLVED)

1. **RingBuffer Clone vs Custom HllRingBuffer** — RESOLVED: Relax Copy to Clone (Plan 05-01 Task 1). RingBuffer<Hll> is valid after this change, matching locked CONTEXT.md decision.
   - What we know: RingBuffer requires `T: Copy`. HLL is ~16KB and should not be Copy.
   - What's unclear: Whether relaxing to Clone affects postcard serialization or performance of existing operators.
   - Recommendation: Relax to Clone (safest, smallest change). If any issue, create a standalone `HllRingBuffer` that duplicates the advance_to/bucket logic specifically for Hll.

2. **Lookup Foreign Key on GET (No Event Context)** — RESOLVED: Search entity features for on_field name (e.g. last_merchant_id or merchant_id). Missing if not found.
   - What we know: On PUSH, the foreign key comes from `_event.merchant_id`. On GET, there's no event.
   - What's unclear: Exactly which entity features should be searched for the foreign key.
   - Recommendation: On GET, look for a feature named `last_{on_field}` or search all static/live features for one matching the `on_field` name. If not found, return Missing. This matches CONTEXT.md: "Lookup foreign key extracted from the current event or from the entity's last known value."

3. **View Key-Field Matching** — RESOLVED: Evaluate all views for every GET. Missing references resolve naturally for non-matching keys.
   - What we know: Views have a `key_field` (e.g., "user_id"). On GET for key "u123", views with key_field "user_id" should be evaluated.
   - What's unclear: How does the server know that "u123" is a "user_id" vs. a "merchant_id"?
   - Recommendation: In the current design, GET returns ALL features for a key regardless of which stream created them. Views should evaluate for all views whose key_field matches ANY registered stream that has state for this entity. Simpler: evaluate all views -- if a view references a stream that doesn't have features for this key, those references resolve to Missing naturally.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test framework + cargo test |
| Config file | Cargo.toml (already configured) |
| Quick run command | `~/.cargo/bin/cargo test --manifest-path /Users/petrpan26/work/tally/Cargo.toml --lib` |
| Full suite command | `~/.cargo/bin/cargo test --manifest-path /Users/petrpan26/work/tally/Cargo.toml` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OPS-01 | MinOp returns correct minimum over window | unit | `cargo test min_op` | Wave 0 |
| OPS-02 | MaxOp returns correct maximum over window | unit | `cargo test max_op` | Wave 0 |
| OPS-03 | LastOp returns most recent value | unit | `cargo test last_op` | Wave 0 |
| OPS-04 | DistinctCountOp windowed HLL approximate count | unit | `cargo test distinct_count` | Wave 0 |
| OPS-04 | HLL insert/count/merge correctness | unit | `cargo test hll` | Wave 0 |
| OPS-04 | HLL serialized size within bounds | unit | `cargo test hll_size` | Wave 0 |
| OPS-05 | Where-clause filtering skips non-matching events | unit | `cargo test where_clause` | Wave 0 |
| XSTR-01 | View derive across two streams | integration | `cargo test --test test_pipeline view` | Wave 0 |
| XSTR-02 | Lookup resolves cross-key feature | integration | `cargo test --test test_pipeline lookup` | Wave 0 |
| XSTR-03 | Fan-out updates multiple streams | integration | `cargo test --test test_pipeline fan_out` | Wave 0 |

### Sampling Rate
- **Per task commit:** `~/.cargo/bin/cargo test --manifest-path /Users/petrpan26/work/tally/Cargo.toml --lib`
- **Per wave merge:** `~/.cargo/bin/cargo test --manifest-path /Users/petrpan26/work/tally/Cargo.toml`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] HLL unit tests in `src/engine/hll.rs` -- covers OPS-04 (insert, count, merge, empty, size bounds)
- [ ] MinOp/MaxOp unit tests in `src/engine/operators.rs` -- covers OPS-01, OPS-02
- [ ] LastOp unit tests in `src/engine/operators.rs` -- covers OPS-03
- [ ] DistinctCountOp unit tests in `src/engine/operators.rs` -- covers OPS-04
- [ ] Where-clause filtering tests in pipeline unit tests -- covers OPS-05
- [ ] Cross-stream view integration tests in `tests/test_pipeline.rs` -- covers XSTR-01
- [ ] Lookup integration tests in `tests/test_pipeline.rs` -- covers XSTR-02
- [ ] Fan-out integration tests in `tests/test_pipeline.rs` or `tests/test_server.rs` -- covers XSTR-03
- [ ] Snapshot round-trip tests with new OperatorState variants in `tests/test_snapshot.rs`
- [ ] Protocol conversion tests for new feature types in `src/server/protocol.rs`

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | N/A (TCP port has no auth per CLAUDE.md out-of-scope) |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | Event field validation in operators (type checks, Missing propagation) |
| V6 Cryptography | no | HLL hash is for distribution, not security |

### Known Threat Patterns for Phase 5

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malformed where-clause expression | Tampering | Parse at registration time (existing winnow parser); reject invalid expressions |
| Oversized HLL memory from many distinct_count features | Denial of Service | 360KB per feature per key is bounded; document memory implications |
| Fan-out amplification (event triggers many stream updates) | Denial of Service | Each stream push is bounded; fan-out is O(registered_streams) which is small |
| Lookup reading arbitrary keys from StateStore | Information Disclosure | Lookup is defined at registration time, not user-controlled at event time |

## Sources

### Primary (HIGH confidence)
- Codebase inspection: src/engine/operators.rs, window.rs, pipeline.rs, expression.rs [VERIFIED: all patterns read directly]
- Codebase inspection: src/state/snapshot.rs, store.rs [VERIFIED: OperatorState enum, EntityState structure]
- Codebase inspection: src/server/protocol.rs, tcp.rs, http.rs [VERIFIED: DTO structure, command dispatch]
- Codebase inspection: python/tally/_operators.py, _view.py, _stream.py, _app.py [VERIFIED: SDK classes exist]
- CONTEXT.md locked decisions [VERIFIED: Phase 5 discuss output]

### Secondary (MEDIUM confidence)
- [HyperLogLog build-your-own in Rust](https://www.arunma.com/2023/05/01/build-your-own-hyperloglog/) -- algorithm details, hash function usage, bias correction
- [HyperLogLog Wikipedia](https://en.wikipedia.org/wiki/HyperLogLog) -- 14-bit precision gives ~0.8% error, ~16KB memory
- WebSearch: Rust HLL implementations, 14-bit precision standard [CITED: search results]

### Tertiary (LOW confidence)
- None -- all critical claims verified from codebase or official algorithm documentation

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies needed, all verified in Cargo.toml
- Architecture: HIGH -- all patterns derived from existing codebase, well-established operator template
- HLL implementation: MEDIUM -- algorithm is well-documented but from-scratch implementation has edge cases (bias correction, small/large range corrections)
- Cross-stream views: HIGH -- design follows locked decisions from CONTEXT.md, existing expression evaluator handles qualified refs
- Pitfalls: HIGH -- derived from actual codebase constraints (RingBuffer Copy bound, borrow checker patterns)

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable -- Rust ecosystem moves slowly, no fast-moving dependencies)
