# Feature Landscape: v2.0 New API & Engine

**Domain:** Function-based streaming pipeline API with EventSet/FeatureSet types
**Researched:** 2026-04-12
**Confidence:** HIGH (Fennel pattern well-understood from founder's direct experience; Pathway/Hamilton/Bytewax documented from official sources; engine gaps identified from codebase inspection)

---

## Executive Summary

The v2.0 API redesign replaces Tally's decorator-based `@st.stream` pattern with a function-based `@tl.dataset(depends_on=[...])` pattern using honest `EventSet`/`FeatureSet` types. This is informed by the founder's direct experience at Fennel (now acquired by Databricks) and validated against Pathway, Hamilton, Bytewax, and dbt patterns.

The ecosystem has converged on three principles: (1) explicit dependency declaration via decorators or function signatures, (2) explicit grouping via `.group_by().agg()` instead of implicit keying, and (3) typed input/output contracts. Tally's differentiator is that EventSet/FeatureSet are NOT DataFrames -- they honestly represent "stream of events" and "computed features per key" without pretending to be Pandas.

Three engine changes unlock the new API: enriched event propagation (~50 LOC Rust), feature projection, and union node. Everything else is Python SDK work.

---

## Table Stakes

Features users expect from a function-based streaming pipeline API. Missing = the API feels half-baked.

| Feature | Why Expected | Complexity | Dependencies | Notes |
|---------|-------------|------------|--------------|-------|
| **`@tl.dataset(depends_on=[...])`** | Fennel uses `@dataset` + `@pipeline(@inputs(...))`. Hamilton uses function params as dependencies. dbt uses `ref()`. Every DAG-based system makes dependencies explicit at definition site. | Low | Python SDK only | Function-based, not class-based. The function body IS the pipeline. |
| **`EventSet` input type** | Fennel has `Dataset` (typed columns, timestamps). Pathway has `pw.Table`. Bytewax has typed dataflow sources. Users expect a type that says "this is a stream of events with a schema." | Low | Python SDK only | Schema declaration + pipeline handle. NOT a DataFrame. No `.head()`, no `.shape`, no `.iloc`. |
| **`FeatureSet` output type** | Fennel explicitly separates `Dataset` (state) from `Featureset` (served features backed by stateless extractors). Users need a clear contract for "what gets returned on GET." | Low | Python SDK only | In Tally, derive expressions serve the extractor role. FeatureSet is a Python-side type annotation, not a separate server concept. |
| **Explicit `.group_by("key").agg(...)`** | Fennel: `.groupby("uid").aggregate(Count(...))`. Pathway: `.groupby().reduce()`. PySpark: `.groupBy().agg()`. Flink Table: `.group_by().select(agg)`. Universal pattern. | Low | Already built in DataFrame API | Port from `_dataframe.py` GroupBy/Table to new `@tl.dataset` surface. |
| **`filter()` operator** | Fennel: `.filter(lambda df: ...)`. Pathway: `.filter()`. Bytewax: `filter()`. Every pipeline system has it. | Low | Already in engine (where-clause) | Wrap existing where-clause. Use expression objects, NOT Python lambdas. |
| **`transform()` / `map()` operator** | Fennel: `.transform(Cls, lambda df: ...)`. Pathway: `.select(pw.this.x * 2)`. Bytewax: `map()`. Stateless row-level transforms. | Low | Already built (derive expressions) | Maps to derive. Existing `Stream.map()` already does this. |
| **`join()` operator** | Fennel: `.join(other, how="left", on=["key"])`. Pathway: `.join(other, pw.left.k == pw.right.k)`. Same-key and cross-key joins. | Low | Already built (views + lookups) | Existing JoinedTable compiles to view registration. Carry forward. |
| **`select()` / `drop()` / `rename()`** | Fennel has all three. Pathway has `.select()`. Standard projection ops for controlling which features exist in the output. | Medium | **Engine: feature projection needed** | SDK has `Table.select()`/`drop()`/`rename()` today but engine computes ALL features regardless. Projection pushdown tells engine which features to skip. |
| **`union()` operator** | Fennel: `dataset_a + dataset_b` (keyless, same schema). Pathway: `pw.Table.concat()`. Merging multiple event sources into one stream is a common pattern. | Medium | **Engine: union node needed (~50 LOC Rust)** | New DAG node type. Takes N keyless streams with identical schema, produces one stream. |
| **Enriched event propagation** | Fennel's pipeline engine automatically makes upstream dataset columns available to downstream transforms. Pathway does this via incremental dataflow. Without this, derived datasets can't reference upstream features. | Medium | **Engine: ~50 LOC Rust change** | Current cascade forwards RAW event. Must augment with computed upstream features before forwarding downstream. THE critical engine unlock. |
| **Typed column schema** | Fennel: Pydantic-style typed fields with `field(key=True)`, `field(timestamp=True)`. Pathway: `pw.Schema`. Users expect schema validation at registration time. | Medium | Python SDK + server validation | Catch type mismatches at `register()` time, not at first event. |
| **Pipeline as runtime REGISTER** | Already works. Tally's REGISTER is a runtime operation. | Already done | None | Must preserve -- this is the primitive that enables on-demand compute. |

## Differentiators

Features that set Tally apart. Not expected from the ecosystem, but uniquely valuable.

| Feature | Value Proposition | Complexity | Dependencies | Notes |
|---------|-------------------|------------|--------------|-------|
| **Portable pipeline definitions** | Same `@tl.dataset` definition compiles to identical JSON whether used for startup registration, runtime REGISTER, or future on-demand ephemeral pipelines. No comparable system does this. | Low | Design constraint only | Fennel definitions are tightly coupled to their managed platform. Pathway definitions are tied to a long-running process. Tally's are portable JSON. |
| **Synchronous push-through** | POST event, get updated features in the response. Fennel, Pathway, Flink all separate ingestion from serving. Tally returns features synchronously with <100us p99. | Already done | None | Preserve in v2.0. Unique competitive advantage. |
| **On-demand ephemeral pipelines (future)** | Sub-second pipeline creation + TTL lifecycle + zero infrastructure. Materialize/RisingWave/Feldera/ksqlDB all require persistent DDL. Panel-validated as novel. | Medium | Ephemeral flag, TTL, memory limits | **Architect now, build post-launch.** Same definition format, only lifecycle differs. |
| **One-shot replay (future)** | "Run this pipeline against last 24h for this key, return result, discard state." No comparable system offers this without persistent infra. | High | S3 replay log (post-launch) | Do NOT build now. But `@tl.dataset(ephemeral=True, ttl="0")` should be the primitive. |
| **No Python in hot path** | Fennel serializes Python lambdas to server (fragile, slow). Pathway uses Rust FFI for Python (complex). Bytewax ran Python UDFs and maxed at ~50K eps before dying. Tally's expression language keeps Python out entirely. | Already done | None | Expression-only transforms. 18 builtins + arithmetic + boolean + string ops cover 95% of use cases. |
| **Function composition over class inheritance** | Hamilton's key insight: functions as DAG nodes are independently testable. `@tl.dataset` functions can be composed, tested in isolation, and reused. Fennel's class-based datasets are harder to compose. | Low | Python SDK design | Functions returning EventSet/FeatureSet can be called in tests with mock inputs. |

## Anti-Features

Features to explicitly NOT build. These look good but are traps.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **DataFrame simulation (`tl.DF`)** | Users expect `.head()`, `.shape`, `.iloc`, `.apply()`, `len()`. Polars explicitly rejects Pandas compat. Fennel avoids it. Every missing method is a support ticket. | Honest `EventSet`/`FeatureSet` types. No pretending to be a DataFrame. |
| **Python UDFs / lambdas in pipeline** | Fennel's `.transform(lambda df: ...)` requires serializing Python closures server-side. Breaks on local state capture, closure imports, and version mismatches. Pathway pays FFI overhead. Bytewax's pure-Python hot path killed throughput. | Expression language only. If users need Python logic, pre-compute and use SET/MSET. |
| **Fennel-style `@extractor` functions** | Fennel separates Dataset (state) from FeatureSet (Python extractors that run at read time). Adds indirection. For Tally, derive expressions already serve this purpose with zero Python overhead. | Derive expressions ARE the extraction mechanism. `FeatureSet` is a Python-side type annotation, not a server-side concept with Python execution. |
| **`Continuous("1d")` window syntax** | Fennel's window syntax is unintuitive. "Continuous" is jargon. Developers need to learn Fennel-specific vocabulary. | `window="1d"` string. Simple, obvious, no jargon. Already established in Tally's API. |
| **Session windows** | Require tracking gap-based boundaries per key. Significant state complexity. Neither Fennel nor Pathway supports them as first-class citizens. | Sliding and tumbling only. Use `last("timestamp")` + derive for session-like behavior. |
| **Cross-key aggregations** | "Count across all users where X" requires scanning all keys. Fundamentally incompatible with per-key state model. Flink/Pathway can do it because they're distributed. | Per-key only. Document as explicit scope boundary. Use SET/MSET for pre-computed cross-key features. |
| **Auto schema evolution on live pipelines** | dbt has `on_schema_change`. Fennel versions datasets and triggers recompute. Live schema evolution on streaming state is extremely complex (in-flight windows, partial state). | Version-bump: register new version, backfill from event log, swap. Already supported in v1.1. |
| **Deprecation period for old API** | Maintaining `@st.stream` alongside `@tl.dataset` doubles SDK surface, test matrix, and docs. Fennel never had two APIs. Hamilton never had two APIs. | Clean break. Remove `@st.stream`/`@st.view` entirely. Pre-launch, no external users to migrate. |
| **Watermarks / late-arrival handling** | Flink/Pathway use watermarks. Adds latency (must wait for watermark advance) and complexity (retraction/correction semantics). | Wall-clock processing. Events are never "late." For batch backfill, use event timestamps from payload. Simpler and sufficient for fraud/ML. |
| **Automatic version-triggered recomputation** | Fennel bumps dataset version to trigger full recompute cascading through deps. Expensive and surprising for users. | Explicit backfill command. User controls when recomputation happens. |

## Feature Dependencies

```
EventSet type ──────────────────────┐
                                    ├──> @tl.dataset decorator ──> Full new API
FeatureSet type ────────────────────┘          │
                                               ├──> Old API removal
Enriched event propagation (engine) ───────────┤    (after new API passes all tests)
                                               │
Feature projection (engine) ───────────────────┤
                                               │
Union node (engine) ───────────────────────────┘

On-demand compute architecture:
  @tl.dataset (portable definitions)
  + REGISTER stays runtime (already true)
  + ephemeral flag (schema only, not lifecycle)
  + TTL field (schema only)
  + memory limits (schema only)
  ──> One-shot queries (post-launch, needs S3 replay log)
```

### Critical Path

1. **EventSet/FeatureSet types** (Python only) -- foundation for everything
2. **@tl.dataset decorator** (Python only) -- the new API surface
3. **Enriched event propagation** (Rust, ~50 LOC) -- unlocks derived datasets referencing upstream features
4. **Feature projection** (Rust, small) -- select/drop restricts what engine computes
5. **Union node** (Rust, ~50 LOC) -- merge multiple event sources
6. **Old API removal** (Python only) -- clean break
7. **Ephemeral pipeline schema fields** (Rust, small) -- add fields to RegisterRequest, don't implement lifecycle

### Parallelizable Work

- Steps 1-2 (Python SDK) and steps 3-5 (Rust engine) can proceed in parallel
- Step 6 depends on steps 1-2 being complete and tested
- Step 7 can happen any time

## Enriched Event Propagation: Detailed Specification

**Current behavior (from `src/engine/pipeline.rs:852-917`):** `push_with_cascade_internal` forwards the raw event to all downstream streams. Line 906: `self.push_internal(stream_in_order, event, store, now, read_features)` -- the same `event` is passed unchanged.

**Problem:** A derived dataset that depends on upstream features can't access them. Example:
```python
@tl.dataset(depends_on=[raw_events])
def enriched(events: EventSet) -> EventSet:
    return events.map(amount_usd=events["amount"] * events["fx_rate"])

@tl.dataset(depends_on=[enriched])
def user_features(events: EventSet) -> FeatureSet:
    return events.group_by("user_id").agg(
        total_usd=tl.sum("amount_usd", window="1h")  # needs amount_usd from enriched
    )
```

The `amount_usd` field exists only as a computed derive in the `enriched` stream. When the cascade forwards to `user_features`, the raw event doesn't have `amount_usd`.

**Required change:**
```rust
// In push_with_cascade_internal, after line 906:
// 1. Compute features for this downstream node
let downstream_features = self.push_internal(stream_in_order, &enriched_event, store, now, read_features);
// 2. Merge computed features into the event for further downstream
if let Ok(ref features) = downstream_features {
    for (k, v) in features.iter() {
        enriched_event[k] = v.to_json();
    }
}
```

**Naming collisions:** Upstream features are already namespaced in Tally's expression language as `StreamName.feature_name`. The enriched event should use flat names (matching the derive/where expression resolver), with a convention that upstream features overwrite raw event fields of the same name. This matches Fennel's behavior where downstream pipelines see the most recently computed value.

**Complexity:** ~50 LOC. The cascade loop already iterates in topo order. Add a mutable `enriched_event` that accumulates features between iterations.

## Union Node: Detailed Specification

**Use case:** Merge `web_clicks + mobile_clicks + api_calls` into `all_events`, then aggregate by user.

**Fennel pattern:** `dataset_a + dataset_b` using Python `__add__`. Constraints: both keyless, same schema.

**Tally implementation:**
- Python SDK: `tl.union(stream_a, stream_b, ...)` function returning a new EventSet
- Engine: New `union` node type in the DAG. When any parent receives an event, the union node also receives it (via depends_on). Schema validation at registration: all parents must have compatible schemas.
- Wire format: Union node is registered as a stream with `depends_on: [parent_a, parent_b, ...]` and a `type: "union"` marker. No operators of its own -- it's a pass-through that merges event flows.

**Complexity:** ~50 LOC Rust. The cascade already handles multi-parent depends_on. Union is "forward event from any parent to my downstream" which the cascade already does. The new part is schema validation at registration.

## Feature Projection: Detailed Specification

**Current state:** `Table.select(["a", "b"])` in the SDK creates a new Table Python object with only features `a` and `b`, but the server still computes ALL features for the original stream.

**Required change:** Add optional `projection: Vec<String>` to the stream registration JSON. When set, the engine:
1. Only evaluates the listed operators (skipping others)
2. Only returns the listed features in GET/PUSH responses
3. Still stores state for all operators (for future re-projection without data loss)

**Alternative (simpler):** Projection only affects the response, not computation. All operators run, but only projected features are serialized in the response. This is simpler and still useful for reducing response payload size.

**Recommendation:** Start with response-only projection (simpler, ~20 LOC). Full computation pruning is an optimization that can come later.

## Fennel API Pattern Analysis

### What Fennel Gets Right (adopt)

| Pattern | Fennel Implementation | Tally v2.0 Equivalent |
|---------|----------------------|----------------------|
| Explicit dependencies | `@pipeline` + `@inputs(Dataset1, Dataset2)` | `@tl.dataset(depends_on=[Dataset1, Dataset2])` |
| Explicit grouping | `.groupby("uid").aggregate(Count(...))` | `.group_by("uid").agg(tl.count(...))` |
| Typed columns | Pydantic-style `uid: int = field(key=True)` | Schema class on EventSet (lighter weight) |
| Dataset = pipeline output | `@dataset` class with `@pipeline` method | `@tl.dataset` function, return value is the pipeline output |
| Union via `+` operator | `ds_a + ds_b` for keyless datasets | `tl.union(stream_a, stream_b)` function |
| Join with explicit keys | `.join(other, how="left", on=["uid"])` | `.join(other, on="uid", how="left")` |
| filter/transform/rename/drop/select | Full operator set on datasets | Expression-based equivalents (no Python lambdas) |

### What Fennel Gets Wrong (avoid)

| Pattern | Problem | Tally's Approach |
|---------|---------|-----------------|
| Python lambdas in `transform`/`filter` | Serializing closures is fragile. Breaks on imports, local state. | Expression language only. |
| Separate Dataset + FeatureSet | Two concepts for often one thing. Extractors add indirection. | FeatureSet is a lightweight Python-side type, not a separate server concept. |
| `Continuous("1d")` windows | Jargon. "Continuous" vs "sliding" vs "tumbling" confusion. | `window="1d"` string. |
| Class-based `@dataset` | Must define schema as typed fields AND pipeline as method inside the class. Verbose. | Function-based. Schema inferred from operators or declared via type hint. |
| Versioned datasets with cascading recompute | Version bump triggers automatic full recompute of all downstream. Surprising and expensive. | Explicit backfill. User controls when. |
| Managed platform coupling | Definitions only work with Fennel's hosted service. | Portable JSON definitions. Run anywhere. |

## MVP Recommendation

### Build in v2.0 (priority order)

1. **EventSet/FeatureSet types + @tl.dataset decorator** -- the user-facing API. Port existing DataFrame capabilities (group_by, agg, join, filter, map, select, drop, rename) to the new pattern. Mostly SDK refactoring since the DataFrame API already exists.

2. **Enriched event propagation** -- the single most important engine change. Without it, derived datasets referencing upstream features don't work. ~50 LOC Rust in `push_with_cascade_internal`.

3. **Feature projection (response-only)** -- add `projection` field to RegisterRequest. Engine filters response to listed features only. ~20 LOC Rust.

4. **Union node** -- merge multiple event sources. New DAG node type. ~50 LOC Rust.

5. **Old API removal** -- delete `@st.stream`, `@st.view`, all legacy operator aliases. Clean break.

6. **Ephemeral pipeline schema** -- add `ephemeral: bool`, `ttl: Option<String>`, `max_keys: Option<u64>` to RegisterRequest schema. Don't implement lifecycle. Architecture-only.

### Defer

- **Typed schema validation at registration:** Nice but not blocking. Can add in follow-up.
- **On-demand compute lifecycle (TTL enforcement, memory limits):** Post-launch.
- **One-shot replay queries:** Needs S3 replay log (month 1 post-launch).
- **Computation-pruning projection:** Start with response-only projection. Optimize later.

---

## Sources

- Fennel AI Dataset/Pipeline docs: [GitHub fennel-ai/client](https://github.com/fennel-ai/client/blob/main/docs/pages/concepts/dataset.md) -- MEDIUM confidence (official but may be stale post-Databricks acquisition)
- Fennel AI FeatureSet: [fennel.ai/docs/concepts/featureset](https://fennel.ai/docs/concepts/featureset) -- MEDIUM confidence
- Fennel AI Pipeline operators: [fennel.ai/docs/api-reference/operators](https://fennel.ai/docs/api-reference/operators) -- MEDIUM confidence
- Pathway documentation: [pathway.com/developers](https://pathway.com/developers/user-guide/introduction/welcome/) -- HIGH confidence (active project, v0.27.1 Jan 2026)
- Pathway pw.Table API: [pathway.com/developers/api-docs/pathway-table](https://pathway.com/developers/api-docs/pathway-table/) -- HIGH confidence
- Hamilton: [github.com/apache/hamilton](https://github.com/apache/hamilton) -- HIGH confidence (Apache incubating)
- Bytewax: [github.com/bytewax/bytewax](https://github.com/bytewax/bytewax) -- MEDIUM confidence (project winding down, last OSS release Nov 2024)
- dbt incremental models: [docs.getdbt.com](https://docs.getdbt.com/best-practices/how-we-handle-real-time-data/2-incremental-patterns) -- HIGH confidence
- Tally codebase: `python/tally/_dataframe.py`, `src/engine/pipeline.rs` -- HIGH confidence (primary source, inspected this session)
- Prior research: `.planning/research/horizon/HORIZON-DATAFRAME-API.md` -- HIGH confidence (validated in prior milestone)
- User decisions: `project_v2_api_redesign.md`, `project_on_demand_compute.md` -- HIGH confidence (direct founder input)
