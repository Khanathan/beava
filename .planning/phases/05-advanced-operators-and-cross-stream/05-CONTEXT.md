# Phase 5: Advanced Operators and Cross-Stream - Context

**Gathered:** 2026-04-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Implement all remaining operators (min, max, last, distinct_count with windowed HLL), add where-clause filtering to any windowed operator, and build cross-stream views with cross-key lookups and event fan-out. After this phase, the full operator set from CLAUDE.md is functional end-to-end through TCP and Python SDK.

</domain>

<decisions>
## Implementation Decisions

### HLL Windowing Strategy
- Use RingBuffer<Hll> pattern — same architecture as count (RingBuffer<u64>) and sum (RingBuffer<f64>), with HLL sketches as bucket values
- Merge all non-expired buckets via HLL union (bitwise max of registers) on read — identical sliding window semantics to other operators
- Same window/bucket configuration as count/sum/avg (default: window/30 buckets)
- Memory: ~360KB per distinct_count feature per key (30 buckets x 12KB each) — accepted tradeoff for accuracy
- Fixed 14-bit precision (2^14 = 16384 registers, ~12KB per sketch) per CLAUDE.md spec
- Epoch swap is event-driven via advance_to(now) — same pattern as existing RingBuffer, no background timer
- Implement HyperLogLog from scratch in hll.rs per locked decision (external crates require nightly or are minimally maintained)
- Zero events in window returns Missing (consistent with all other operators)
- Unit test asserting serialized HLL size stays within expected bounds

### Where-Clause Filtering
- Filter at pipeline level: evaluate where expression before calling operator.push(); skip push if expression evaluates to false/Missing
- Where clauses can only reference event fields (_event.field) — where runs before operators update, so current-cycle feature values aren't available
- Missing field in where expression treats as false (skip) — not an error
- Optional `where_expr: Option<Expr>` field added to each windowed FeatureDef variant (Count, Sum, Avg, Min, Max, DistinctCount)

### Cross-Stream Views
- Separate ViewDefinition type — views have no key_field for push, only derive + lookup features (no windowed operators)
- Views recompute lazily on GET only — industry consensus (Chalk, Fennel/Databricks, Flink Delta Join all use read-time evaluation)
- PUSH response returns features from the pushed stream only; GET response includes all features from all streams + views for that key
- Qualified field references (e.g. Transactions.tx_count_1h) resolved via stream-aware EvalContext that populates features from all registered streams sharing the entity key
- View registration uses separate REGISTER call with `type: "view"` — matches Python SDK @st.view being distinct from @st.stream

### Cross-Key Lookup & Fan-Out
- Lookup evaluates by reading target entity's feature from StateStore at eval time — EvalContext gains &StateStore reference for point reads (industry standard: Chalk resolvers, Flink Delta Join, Databricks FeatureLookup)
- TTL-evicted target entity returns Missing — per STATE.md blocker: "Missing propagation expected, not panic"
- Fan-out: server-level loop on PUSH — iterate all registered streams, push event to all streams whose key_field exists in the event JSON
- Fan-out PUSH response: features from primary stream only (the one named in the PUSH command)
- Lookup foreign key extracted from the current event or from the entity's last known value

### Claude's Discretion
- MinOp/MaxOp per-bucket tracking strategy (per-bucket min/max or full scan)
- LastOp internal representation details
- Exact HLL hash function choice (MurmurHash3, xxHash, etc.)
- ViewDefinition struct layout and registration DTO format
- Fan-out iteration order across streams (order should not matter)
- Test fixture design for cross-stream integration tests

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets
- src/engine/operators.rs: CountOp, SumOp, AvgOp with RingBuffer pattern — new operators (min, max, distinct_count) follow identical structure
- src/engine/window.rs: RingBuffer<T> generic over bucket type — extend with RingBuffer<Hll> for distinct_count
- src/engine/expression.rs: Expr AST with FieldRef::Qualified already parsed — resolution logic needs implementation
- src/state/snapshot.rs: OperatorState enum — add Min, Max, DistinctCount, Last variants (comment already says "Phase 5 adds")
- src/engine/pipeline.rs: PipelineEngine with push/get_features flow — extend for fan-out and view evaluation
- src/server/protocol.rs: convert_register_request with match on feature_type — add "min", "max", "last", "distinct_count", "lookup", "view" branches
- python/tally/_operators.py: Min, Max, DistinctCount, Last, Lookup classes already exist — server-side implementation matches existing SDK

### Established Patterns
- Operator trait: push(&mut self, event, now) -> Result + read(&mut self, now) -> FeatureValue
- OperatorState enum wraps concrete ops for serialization (postcard)
- FeatureDef enum in pipeline.rs mirrors operator types
- EvalContext { features, event } for expression evaluation
- Pipeline-level orchestration: push operators -> read values -> eval derives -> return FeatureMap
- Name-based operator reconciliation on push (preserves state on feature addition)

### Integration Points
- OperatorState enum needs new variants (snapshot format version bump needed)
- FeatureDef enum needs new variants + optional where_expr field on windowed variants
- convert_register_request needs new type branches
- PipelineEngine needs ViewDefinition storage + get_features view evaluation
- EvalContext needs &StateStore for lookup resolution
- HTTP API feature listing needs new FeatureDef match arms
- Python SDK operators already define the registration JSON — server must accept it

</code_context>

<specifics>
## Specific Ideas

- HLL windowing uses RingBuffer<Hll> (same merge-on-read pattern as Redis PFMERGE and Flink HOP window) — chosen after researching Chalk, Fennel, Flink, Databricks approaches
- Cross-stream view architecture follows industry consensus: lazy read-time evaluation, not eager push-time materialization
- Snapshot format version should bump (new OperatorState variants break backward compatibility)
- STATE.md blocker "HLL memory math" resolved: 360KB per feature per key accepted, validated via unit test

</specifics>

<deferred>
## Deferred Ideas

- PUSH response including view features for same-key views — useful for fraud scoring but adds latency; defer to post-v1
- Configurable HLL precision (allow lower precision for memory savings) — defer to post-v1
- All-streams-merged fan-out response — defer to post-v1

</deferred>
