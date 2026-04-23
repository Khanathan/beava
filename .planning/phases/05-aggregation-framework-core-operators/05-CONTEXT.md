# Phase 5: Aggregation framework + core operators - Context

**Gathered:** 2026-04-23
**Status:** Ready for planning
**Mode:** Smart discuss (4 batch-table questions; architectural defaults drawn from prior phases + locked memory decisions)

<domain>
## Phase Boundary

`group_by(keys).agg(name=bv.<op>(...), ...)` lands server-side. The apply loop updates per-entity aggregation state on every event flowing through the source. Core 8 operators ship: count, sum, avg, min, max, variance, stddev, ratio. `Windowed<Op>` bucket infra: uniform event-time tumbling with cap 64 buckets per windowed operator.

Out of scope: non-core operators (decay, sketch, velocity, geo, bounded-buffer — Phases 8–11), WAL group-commit (Phase 6), snapshot/recovery (Phase 7), joins (Phase 12), retraction for aggregations (v1 — architecture must keep it possible; see memory/project_stateful_architecture.md).

</domain>

<decisions>
## Implementation Decisions

### D-01 — Operator state dispatch: concrete enum + match arms

`enum AggOp { Count(CountState), Sum(SumState), Avg(AvgState), Min(MinState), Max(MaxState), Variance(VarianceState), StdDev(StdDevState), Ratio(RatioState), Windowed(Box<WindowedOp>) }` (or similar). Apply loop matches on the enum variant and calls the concrete state method directly. Zero-cost dispatch; no Box<dyn AggOp>.

**Rationale:** Matches the locked "per-op handcrafted per-backend" decision (memory/project_stateful_architecture.md) — no shared trait abstraction that would force all ops through a vtable. Best fit for 3M EPS/core target. Adding a new op = enum variant + match arm.

**Implication:** The enum-variant explosion will be ~40 variants by Phase 11. Acceptable cost. Can be macro-generated later if maintenance pain hits (deferred).

### D-02 — Feature query response envelope: `{value}` only in v0

`GET /get/{feature}/{key}` and `POST /get` batch both return `{value: <aggregation output>}` — no metadata envelope in Phase 5.

**Rationale:** Minimal wire surface. Phase 13 (observability) can add `{value, updated_at, window_meta}` behind a version bump if needed. Pre-1.0 API can evolve. Shipping `{value}` alone lets every operator's query method stay a pure `&State -> Value` — no threading of metadata.

**Implication:** Users wanting staleness checks in v0 must use `/metrics` endpoint (Phase 13) or compute from push time on their side. Document this in `python/README.md`.

### D-03 — `where=...` predicate on aggregations: supported, reuses Phase 4 expr/eval

Every core aggregation accepts optional `where: bv.col(...)` kwarg:

```python
bv.count(where=bv.col("status") == "ok", window="5m")
bv.sum(field="amount", where=bv.col("currency") == "USD", window="1h")
```

Apply-time: evaluate the `where` predicate against the event Row using Phase 4's `eval::eval_with_depth`. If predicate is `Bool(false)` or `Null` (three-valued null → drop), skip the aggregation update. If `Bool(true)`, update as normal.

**Rationale:** Zero new machinery — thread Phase 4 Expr/eval through to apply time. Matches how fraud users write "rate of declined transactions" (`count(where=status=="declined")`).

**Implication:** Every op descriptor grows an optional `where: Option<Arc<Expr>>` field. Register-time validation must check the predicate's `referenced_fields()` are in the upstream schema (same pattern as Phase 4 op-chain expression validation).

### D-04 — Bucketing: fixed event-time tumbling with cap 64 buckets

`bucket_ms = window_ms / 64` rounded up to the nearest clean value (per plan-time spec). `bucket_index(event) = floor(event.event_time_ms / bucket_ms) mod 64` (ring buffer).

On update: add to `state.buckets[bucket_index(event)]`.
On query: rotate through buckets where `now - bucket_start_ms < window_ms`; fold across active buckets (sum for count/sum, weighted average for avg, min/max across bucket mins/maxes, Welford combine for variance).

**Rationale:** Matches PROJECT.md's locked "uniform event-time bucketing, cap 64 buckets per windowed operator" constraint. Deterministic replay — bucket index is a pure function of event-time, no wall-clock reads in the apply loop. Windowless mode = 1-bucket special case.

**Implication:** Short windows (e.g., `window="64s"`) get 1-second buckets; long windows (`window="1d"`) get ~22.5-minute buckets. Bucket-boundary rounding error is acceptable for v0 (≤1/64 = 1.56% of window). Document this in operator docs.

### D-05 — Aggregation output = implicit TableDerivation

`Event.group_by(*keys).agg(**features)` produces a TableDerivation where:
- `key` = `group_by` keys (server validates they exist in upstream schema)
- `schema` = keys (inherited types) + each feature's `output_type_for(upstream_schema)` (e.g., count → Int; sum/avg/variance/stddev → Float; min/max → field's type; ratio → Float in [0,1])
- `temporal` = false (aggregations are NOT temporal tables in v0 — SDK-AGG-05 explicitly rejects aggregation-on-Table; retraction for aggregations is a v1 concern)

**Rationale:** Aligns with v1 Python SDK shape. Downstream can `.join(agg_table)` (Phase 12). SDK-AGG-05 rejects aggregation-on-Table in v0 because retraction propagation through a second aggregation layer is a v1 design problem.

### D-06 — Replay determinism invariants (SC4)

The apply loop MUST be pure wrt event order:
- No wall-clock reads (`SystemTime::now()` forbidden in apply loop — use `event.event_time_ms` only)
- No random sources (no `rand`, no HashMap iteration order at query time — use `BTreeMap` for entity state when iteration order matters for serialization)
- No background thread interleaving (single OS thread per PROJECT.md)
- Welford's algorithm for variance (deterministic, numerically stable)

**Rationale:** SC4 requires byte-identical state after replay of the same event stream. WAL replay (Phase 6) depends on this.

**Implication:** Lint rule / clippy deny for `SystemTime::now()` inside beava-core apply paths. Test: replay a fixture event stream twice; assert `serialize_state()` bytes match.

### D-07 — Validation at register time

`SDK-AGG-04` + register-time Rule 11 (analogous to Phase 4's Rule 10 for op-chain):
- Every `group_by` key must exist in upstream schema (error: `unknown_field`, path includes key name)
- Every operator's `field` (for sum/avg/min/max/variance/stddev) must exist in upstream schema
- Every operator's `where` predicate (if present) → `referenced_fields()` must all exist in upstream schema
- `window` duration must parse (matches `\d+(ms|s|m|h|d)` or `forever`) — SDK-AGG-06 covers SDK-side; server re-validates
- Aggregation on a Table source → 400 with kind `"aggregation_on_table_not_supported"` (SDK-AGG-05)

All errors follow Phase 4's wire shape: `{kind, path, message}` on HTTP+TCP.

### D-08 — WAL entry shape for aggregation-relevant events (Phase 6 dependency, surfaced here)

Every push event lands in WAL with: `{lsn, event_id, event_time_ms, source_name, payload, push_received_at_ms}`. Event ID is stable (future retraction handle per memory/project_stateful_architecture.md). Phase 5 doesn't ship WAL but plans should assume this shape when designing the apply-loop hook.

### Claude's Discretion

- Exact enum variant naming in D-01 (can be `AggOp` or `Op` or `AggregationOp` — prefer the clearest name given Phase 4 already has `Op` in op-chain; likely `AggOp`)
- Bucket-index rounding strategy in D-04 (exact formula for converting `window_ms` to `bucket_ms` — whatever is cleanest given the 64-bucket cap)
- Whether to fold variance via Welford pairwise or Chan's algorithm (both deterministic; pick the one with best stability for typical fraud-shape data)
- Welford state shape for windowed variance (per-bucket (mean, m2, count) vs sum-of-squares-at-bucket-level) — planner picks based on replay-determinism fit

</decisions>

<code_context>
## Existing Code Insights

### Reusable Assets (from Phases 1–4)

- **`crates/beava-core/src/row.rs`** — Row/Value types with three-valued null logic. Aggregations consume events as `&Row`.
- **`crates/beava-core/src/expr.rs` + `eval.rs`** — Expression parser + evaluator. `where` predicate reuses these directly; no new expression machinery needed.
- **`crates/beava-core/src/schema_propagate.rs`** — Schema propagation through op chains. Extend for aggregation nodes: upstream schema + group_by keys + feature output types → TableDerivation schema.
- **`crates/beava-core/src/op_chain.rs`** — Op-chain compiled-at-register-time pattern. Aggregation operators follow the same pattern: compile to concrete state at register time, cached in RegistryInner.
- **`crates/beava-server/src/register.rs` + `register_validate.rs`** — Rule 10 (op-chain expression validation). Add Rule 11 for aggregation field/where/window validation.
- **`crates/beava-server/src/registry.rs`** — `RegistryInner.compiled_chains` parallel map. Phase 5 adds a parallel `compiled_aggregations: HashMap<NodeName, Arc<AggOp>>` map — same pattern, server-authoritative.

### Established Patterns

- **Plan-level file ownership:** each plan owns its `files_modified` files. Phase 4 proved cross-plan file edits cause confusion; keep each plan's state additions isolated.
- **TDD discipline:** every task split into .a (red) + .b (green) with commit-message regex (`^test\(05-NN\):` / `^feat\(05-NN\):`). CLAUDE.md mandatory from Phase 3 onward.
- **Wire error parity:** HTTP + TCP return the same error kind + path. Phase 4 set the precedent; Phase 5 extends it for new kinds (`aggregation_on_table_not_supported`, `unknown_field` in aggregation context).
- **Per-op apply dispatch:** Phase 4's `OpChain::apply` matches on op enum. Phase 5's aggregation apply follows the same shape.

### Integration Points

- **Apply loop hook:** Phase 5 is the FIRST phase where the apply loop runs stateful work. The hook point must support: (a) stateless op-chain transform (Phase 4 already ships this), then (b) for every aggregation whose source is this event's source, update the aggregation state. Ordering matters: op-chain runs FIRST (transforms the event row) BEFORE aggregations see it.
- **Feature query:** `GET /get/{feature}/{key}` needs to look up `(feature_name, key_value) → aggregation state → query → Value`. New endpoint; reuses registry's `compiled_aggregations` map.
- **Apply loop concurrency:** single OS thread per PROJECT.md. No locks around aggregation state — single-writer invariant.

</code_context>

<specifics>
## Specific Ideas

- **Welford's algorithm** for variance/stddev (numerically stable, deterministic, combinable across buckets)
- **BTreeMap over HashMap** for any iteration-order-dependent state (e.g., serialization for WAL/snapshot)
- **Lint rule:** deny `std::time::SystemTime::now()` inside `crates/beava-core/src/agg*.rs` (apply loop determinism)
- **Per-op unit tests:** every operator has a table-driven test (input event stream → expected state → expected query value) for SC3

</specifics>

<deferred>
## Deferred Ideas

- **Aggregation retraction** — v1. Locked in memory/project_stateful_architecture.md; SDK-AGG-05 rejects aggregation-on-Table in v0 because retraction propagation is a v1 concern.
- **Non-core operators** (decay, sketch, velocity, recency, geo, bounded-buffer) — Phases 8–11. Phase 5 only ships core 8.
- **Aggregation on Table** — SDK-AGG-05 deferred to v0.1.
- **SpeedB / hybrid backend for aggregation state** — v1.1+. Phase 5 ops keep dispatch surface thin so future per-backend handcrafted impls are additive (not a refactor).
- **`{value, meta}` response envelope** — Phase 13+ if needed.
- **Metadata endpoint** (`/metrics`) — Phase 13.
- **Temporal aggregations** (aggregation-as-of-time) — v1 alongside Phase 11.5 temporal tables + stream retraction.

</deferred>

---

*Phase: 05-aggregation-framework-core-operators*
*Context gathered: 2026-04-23 via smart-discuss (4 batch questions; architectural defaults from locked memory + Phase 4 VERIFICATION.md carry-forward)*
