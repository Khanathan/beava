# Pitfalls Research: v2.0 New API & Engine

**Domain:** Replacing `@st.stream` decorator API with function-based `@tl.dataset(depends_on=[...])`, EventSet/FeatureSet types, enriched event propagation, feature projection, union node, on-demand compute architecture. Removing old API entirely.
**Researched:** 2026-04-12
**Confidence:** HIGH on integration-specific pitfalls (grounded in existing code at cited paths), MEDIUM on on-demand compute lifecycle pitfalls (pattern-based, not yet code-grounded).

---

## Key context verified before writing

- Current `RegisterRequest` JSON format (`src/server/protocol.rs:409-456`) has NO `transient` field, NO `ephemeral` field, NO `projection` field. Every new engine concept requires a wire format addition that old SDKs cannot produce.
- `_to_register_json()` is the single compilation target for BOTH `@st.stream` classes (`_stream.py:97-134`) and DataFrame objects (`_dataframe.py:366-381`). They produce identical JSON. The v2.0 API must also compile to RegisterRequest JSON, but with new fields.
- `push_with_cascade_internal` (`src/engine/pipeline.rs:852-917`) passes the **original event** (`&serde_json::Value`) to every downstream node unchanged. Enriched event propagation requires merging derived values INTO the event before cascade -- this changes the signature and semantics of the hottest path in the system.
- 744 tests (622 lib + 122 integration) exist. 601 Rust tests are in `src/` inline modules. 122 are in `python/tests/`. The Rust tests use `PipelineEngine` and `StateStore` directly -- they will break if struct signatures change.
- `App.push()` and `App.push_many()` access `stream_class._tally_stream_name` (`_app.py:192,216`). The new API must preserve this protocol or these methods break.
- DashMap concurrency model (`ConcurrentAppState` with per-stream `DashMap<EntityKey, StreamEntityState>`) means enriched propagation must work correctly with concurrent entity access across streams.
- The DataFrame API (`_dataframe.py`) already exists as `Stream`, `Table`, `GroupBy`, `JoinedTable`. It coexists with `@st.stream`. v2.0 replaces BOTH with `@tl.dataset`/EventSet/FeatureSet. This means the DataFrame API is ALSO being removed/rewritten, not just the decorator API.

---

## CRITICAL pitfalls (cause data corruption, test suite collapse, or performance regression)

### C-1. Enriched event propagation changes hot-path allocation pattern -- performance cliff

**Phase:** Engine changes (enriched propagation)
**Severity:** CRITICAL
**What goes wrong:** `push_with_cascade_internal` currently passes `&serde_json::Value` (a borrowed reference) through the entire cascade. Enriched propagation requires **cloning the event, inserting computed fields, and passing the enriched copy downstream**. This turns a zero-copy borrow into an allocation-per-cascade-hop. On a 3-level DAG with fan-out, a single PUSH event that touches 4 downstream streams goes from 0 allocations to 4 `serde_json::Value::clone()` + 4 map inserts. At 1.1M eps aggregate throughput, even 1us of added allocation cost per event = 1.1 seconds of CPU per second = performance collapse.
**Warning signs:**
- `perf stat` shows a jump in `cache-misses` or `instructions` on the cascade path
- Single-client async throughput drops below 100k eps on medium pipeline (was 139k)
- `jemalloc` stats show a spike in small-object allocations
**Prevention:**
- Do NOT clone `serde_json::Value` per cascade hop. Instead, use a **side-channel `AHashMap<String, FeatureValue>`** that accumulates enriched fields. Pass `(&original_event, &enrichment_map)` as a tuple. The expression evaluator checks enrichment_map first, falls back to original event. Zero cloning.
- Benchmark the enriched path in isolation BEFORE wiring into the server. Run the full pipeline matrix (small/medium/large) from Phase 11 lesson.
- Performance gate: enriched cascade must add **< 5% regression** to the 1.1M eps baseline.
**Detection:** Run `bench.py --mode async --pipeline medium` before and after the enriched propagation change. Compare median eps across 5 runs.

### C-2. Removing old API breaks 744 tests in a cascade -- paralysis by red CI

**Phase:** Old API removal
**Severity:** CRITICAL
**What goes wrong:** The old `@st.stream` and `@st.view` decorators are used in:
- `python/tests/test_stream.py` -- tests StreamMeta metaclass directly
- `python/tests/test_view.py` -- tests view validation
- `python/tests/test_app.py` -- tests `app.register()` with decorated classes
- `python/tests/test_integration.py` -- full E2E tests using `@st.stream`
- `python/tests/test_dataframe.py` -- tests DataFrame API (ALSO being replaced)
- ALL 601 Rust-side tests that use `PipelineEngine::register_stream()` and `StateStore` directly
If old API removal is done in one phase before new API is tested, the entire CI goes red simultaneously. No incremental validation is possible.
**Warning signs:**
- A plan that says "remove old API" without first having equivalent new-API tests for every old-API test case
- Merge conflicts from having two large changes (new API + old API removal) in flight
**Prevention:**
- **Phase order must be: (1) Build new API, (2) Port ALL tests to new API, (3) Verify test count >= 744, (4) THEN remove old API.** Never remove old API in the same phase as building new API.
- Create a test migration checklist: for every `test_*.py` file, map each test function to its new-API equivalent.
- Rust-side tests (`src/engine/pipeline.rs` 54 tests, `src/server/protocol.rs` 103 tests) test `RegisterRequest` JSON parsing -- these should NOT change because the wire format is additive. Verify this explicitly.
- Gate: `cargo test && pytest` must pass with BOTH APIs coexisting before any removal begins.

### C-3. RegisterRequest JSON wire format backward incompatibility silently breaks existing snapshots

**Phase:** Engine changes (transient flag, projection, union node)
**Severity:** CRITICAL
**What goes wrong:** `PipelineEngine` stores `raw_register_jsons: AHashMap<String, serde_json::Value>` (`pipeline.rs:269`). These are persisted in snapshots and reloaded on startup. If v2.0 changes the RegisterRequest schema (adding `transient`, `projection`, `event_set_type` fields), snapshots written by v2.0 may not load correctly by v1.3, and v1.3 snapshots may not load correctly by v2.0 if new required fields are missing.
**Warning signs:**
- A new field added to `RegisterRequest` without `#[serde(default)]`
- Snapshot recovery test failing after format changes
- Users lose all pipeline definitions on upgrade
**Prevention:**
- ALL new fields in `RegisterRequest` MUST have `#[serde(default)]` (as existing fields do at `protocol.rs:411-423`). This ensures old JSON loads with sensible defaults.
- Add an explicit snapshot round-trip test: serialize v1.3-format RegisterRequest JSON, load in v2.0, verify all fields parse correctly with defaults.
- Bump snapshot format version (currently at v6/v7). Include a migration path that handles loading old pipeline JSONs without new fields.
- The `raw_register_jsons` approach is the right one -- it decouples serialization from the Expr AST. Keep it.

### C-4. DataFrame SDK dependency: _dataframe.py is ALSO being replaced, not just @st.stream

**Phase:** New SDK API
**Severity:** CRITICAL
**What goes wrong:** The v2.0 plan says "replace `@st.stream` with function-based API." But the project already HAS a function-based DataFrame API (`_dataframe.py`: `Stream`, `Table`, `GroupBy`, `JoinedTable`) that was built in v1.3/v1.4. The v2.0 API (`@tl.dataset`, `EventSet`, `FeatureSet`) is DIFFERENT from this DataFrame API. So v2.0 must replace TWO APIs, not one:
1. `@st.stream` / `@st.view` (decorator pattern)
2. `Stream` / `Table` / `GroupBy` / `JoinedTable` (DataFrame pattern)
If the plan only accounts for migrating `@st.stream` tests and forgets the DataFrame tests (`test_dataframe.py`), those tests go red silently.
**Warning signs:**
- Plan mentions "remove old API" but only lists `_stream.py` and `_view.py`, forgetting `_dataframe.py`
- `python/tests/test_dataframe.py` not in the test migration checklist
**Prevention:**
- Explicitly list ALL files being replaced: `_stream.py`, `_view.py`, `_dataframe.py`, `_expr.py` (if Expr changes), `__init__.py` (re-exports)
- Map every test in `test_dataframe.py` to its v2.0 equivalent before removing any code
- Consider: can the new `@tl.dataset` API reuse `_dataframe.py`'s `GroupBy`, `JoinedTable` internals? If yes, refactor rather than rewrite. If no, document why.

### C-5. Enriched propagation breaks DashMap concurrent access pattern

**Phase:** Engine changes (enriched propagation)
**Severity:** CRITICAL
**What goes wrong:** `push_with_cascade_internal` currently runs under a single DashMap shard lock per entity. If enriched propagation requires reading derived values from one stream's entity state and inserting them into the event for a downstream stream's entity state, this creates a read-then-write pattern across two DashMap entries. With concurrent pushes to the same entity from different connections, this can cause:
- Stale reads (enrichment picks up pre-update values from upstream stream)
- Double-borrow violations if the same entity's state is borrowed mutably for stream A and immutably for enrichment read
**Warning signs:**
- `DashMap` deadlock or panic under concurrent load
- Intermittently wrong feature values in cascade downstream streams
- Tests pass single-threaded but fail with `#[tokio::test(flavor = "multi_thread")]`
**Prevention:**
- Enrichment values must be computed DURING the upstream push (while the entity lock is held) and stored in a local variable. Pass them to the downstream push as a side-channel argument. Never reach back into the upstream stream's DashMap entry during downstream push.
- The expression evaluator's `EvalContext` already takes `&features` by reference. Compute enriched features once, store in a local `AHashMap`, pass as `EvalContext.features` to downstream. This avoids re-entering the DashMap.
- Write a concurrency stress test: 8 threads, 10k events each, 3-level cascade with enriched fields. Assert final state matches single-threaded execution.

---

## HIGH pitfalls (significant regression, hard-to-diagnose failures, or blocked phases)

### H-1. App.push() hardcoded to `_tally_stream_name` attribute -- new API objects need the same protocol

**Phase:** New SDK API
**Severity:** HIGH
**What goes wrong:** `App.push()` at `_app.py:192` does `stream_class._tally_stream_name`. The new `@tl.dataset` decorated functions/classes must expose this same attribute, or `push()` breaks. Similarly, `App.push_many()` at line 216 uses the same pattern. If the new API uses a different attribute name (e.g., `._name` or `._dataset_name`), all push operations fail at runtime with `AttributeError`.
**Warning signs:**
- `AttributeError: 'DatasetFunction' object has no attribute '_tally_stream_name'`
- Push works with old API in tests but fails with new API objects
**Prevention:**
- Define a protocol: any object passed to `App.push()` must have `_tally_stream_name: str`. Either the new `@tl.dataset` decorator sets this attribute, or `App.push()` is updated to check for `_tally_stream_name` OR `._name` (duck-typing).
- Better: define a `Protocol` (Python typing.Protocol) that both old and new API objects satisfy. This makes the contract explicit and type-checkable.

### H-2. EventSet/FeatureSet type distinction exists only in Python -- server has no concept

**Phase:** New SDK API + Engine
**Severity:** HIGH
**What goes wrong:** The v2.0 vision says EventSet = stream of events, FeatureSet = computed features grouped by key. But the Rust server has ONE type: `StreamDefinition` with optional `key_field`. There is no server-side distinction between "event set" and "feature set." If the Python SDK introduces these types but the server does not enforce the distinction, users can accidentally define an EventSet with windowed operators (which is a FeatureSet), and the error surfaces as a confusing server-side validation failure rather than a clear Python-side type error.
**Warning signs:**
- Users get server errors like "keyless stream cannot have windowed operators" instead of Python-side type errors
- The EventSet/FeatureSet distinction is only cosmetic (different class names, same behavior)
**Prevention:**
- Validate EventSet/FeatureSet constraints IN THE PYTHON SDK at definition time, not at server registration time. EventSet must reject windowed operators; FeatureSet must require a key.
- The server's existing validation (`StreamMeta.__new__` in `_stream.py:77-84` already validates keyless streams) should be replicated in the new types.
- Consider: does the server need to know about EventSet vs FeatureSet? Probably not -- it is a Python-layer abstraction that compiles to the same `RegisterRequest`. Document this as a locked decision.

### H-3. `register_all()` sends individual REGISTER commands per stream -- no atomic batch registration

**Phase:** New SDK API + Engine
**Severity:** HIGH  
**What goes wrong:** `App.register_all()` at `_app.py:146-148` iterates through DAG nodes and sends individual `OP_REGISTER` commands. If the 5th of 8 streams fails validation, streams 1-4 are registered but 5-8 are not. The pipeline is in a partial state. With the new `@tl.dataset` API creating deeper DAGs (source -> map -> filter -> group_by -> derive -> join), partial registration failures become more common and more damaging.
**Warning signs:**
- Server has 3 of 6 pipeline nodes registered after a failed `register_all()`
- Users call `app.push()` against a partially-registered pipeline and get "unknown stream" errors for downstream nodes
**Prevention:**
- Add a `REGISTER_BATCH` command (or use existing `OP_REGISTER` with a wrapper) that accepts the full DAG and registers atomically. On any validation failure, none are registered.
- If batch registration is deferred: at minimum, `register_all()` should validate ALL definitions locally (Python-side) before sending any to the server. Catch errors early.
- Add a `validate()` method on Dataset objects that checks the full DAG for cycles, missing dependencies, and type mismatches WITHOUT sending to the server.

### H-4. Transient streams still consume memory for operator state -- users assume "transient = free"

**Phase:** Engine changes (transient flag)
**Severity:** HIGH
**What goes wrong:** The transient flag (skip snapshot, reject GET) saves disk I/O but does NOT save memory. Operator state for transient streams still lives in DashMap entries. Users who create 10 intermediate map/filter nodes will expect them to be "free" but they still consume per-key operator state. With a deep DAG (source -> map -> map -> filter -> group_by -> derive), the intermediate keyless streams accumulate derive operator state for every passing event.
**Warning signs:**
- `/debug/memory` shows unexpected memory from `_anon_map_0`, `_anon_filter_0` nodes
- Memory usage scales with DAG depth, not just leaf feature count
**Prevention:**
- Keyless transient streams with only derive features should NOT maintain per-key state. They are pure pass-through -- evaluate expressions and propagate. Verify that the engine's `push_internal` for a keyless stream with no operators just evaluates filters/derives and cascades, without creating `EntityState` entries.
- Current code at `pipeline.rs:902-913`: keyless streams already skip key extraction and don't create entity state. Verify this is preserved after enriched propagation changes.
- Add a memory regression test: register a 5-node DAG with 3 transient keyless intermediates. Push 100k events. Assert memory growth is proportional to ONLY the keyed (leaf) nodes, not intermediates.

### H-5. On-demand ephemeral pipelines: no lifecycle GC leads to unbounded memory growth

**Phase:** On-demand compute architecture
**Severity:** HIGH
**What goes wrong:** On-demand pipelines (registered at runtime via HTTP POST, used briefly, then abandoned) accumulate state indefinitely if there is no automatic cleanup. An ML engineer testing 50 feature definitions per day creates 50 stale pipelines with 50 sets of operator state. Without lifecycle management, memory grows monotonically.
**Warning signs:**
- `/debug/memory` shows hundreds of pipeline names the user doesn't recognize
- Memory usage climbs over days/weeks with no correlation to active traffic
- Server OOM after weeks of on-demand experimentation
**Prevention:**
- **Hard limits from day one** (per memory note `project_on_demand_compute.md`): max total pipelines (e.g., 256), max ephemeral pipelines (e.g., 64), max keys per ephemeral pipeline (e.g., 100k), total memory budget for ephemeral state (e.g., 1GB).
- **TTL on ephemeral pipelines**: if no PUSH or GET for 30 minutes (configurable), auto-deregister and free state. The server already has TTL-based key eviction (`state/eviction.rs`) -- extend the pattern to pipeline-level eviction.
- **Admin kill switch**: `DELETE /pipelines/:name` already exists. Add `DELETE /pipelines?ephemeral=true` to purge all ephemeral pipelines.
- **Pipeline counter in metrics**: `tally_pipelines_total{ephemeral="true"}` and `tally_ephemeral_memory_bytes`. Alert when approaching limits.

### H-6. On-demand compute makes REGISTER a concurrent hot-path operation

**Phase:** On-demand compute architecture
**Severity:** HIGH
**What goes wrong:** Today, REGISTER runs once at startup and is protected by `parking_lot::RwLock` write-lock on `PipelineEngine`. In on-demand compute, REGISTER becomes a frequent runtime operation (multiple times per minute). The RwLock write-lock blocks ALL concurrent PUSH operations for the duration of DAG reconstruction (topological sort, downstream_map rebuild, cycle detection). At 1.1M eps, even 1ms of write-lock hold time drops ~1100 events.
**Warning signs:**
- PUSH latency p99 spikes correlating with REGISTER calls
- `PipelineEngine` RwLock contention visible in lock profiling
**Prevention:**
- Build the new `PipelineEngine` **outside** the write lock. Only hold the write lock for the final pointer swap (< 1us). This is exactly the `ArcSwap` pattern already recommended in the v1.3 architecture research (`ARCHITECTURE.md:200-208`).
- If `ArcSwap` is not yet implemented: build the new engine as a separate `PipelineEngine` instance, validate it, then swap under write lock. The swap itself is a pointer assignment.
- Rate-limit REGISTER: max 10 registrations per second. Reject with 429 if exceeded.

### H-7. `_collect_registrations()` deduplication is fragile -- auto-generated names collide

**Phase:** New SDK API
**Severity:** HIGH
**What goes wrong:** The DataFrame API auto-generates names like `{source}_by_{key}`, `{name}__mapped`, `{name}__filtered` (`_dataframe.py:109,122,181`). If two independent DAGs share a source name, their auto-generated intermediate names collide. `register_all()` deduplicates by name (`_app.py:138`), so the second DAG's intermediate overwrites the first's. Silent data corruption.
**Warning signs:**
- Two pipelines sharing a source produce fewer registered streams than expected
- Features from one pipeline appear in another's GET results
**Prevention:**
- Auto-generated names must include a unique suffix (UUID4 short hash, monotonic counter). Example: `transactions_raw__mapped_a3f2` instead of `transactions_raw__mapped`.
- Better: require ALL nodes to have explicit user-provided names. Auto-naming is a convenience that causes debugging nightmares. The `@tl.dataset` pattern naturally requires explicit names because the function name IS the dataset name.
- `_collect_registrations()` should raise an error on name collision rather than silently deduplicating.

---

## MODERATE pitfalls

### M-1. Expression evaluator EvalContext needs enrichment-aware field resolution

**Phase:** Engine changes (enriched propagation)
**Severity:** MODERATE
**What goes wrong:** The expression evaluator (`expression.rs`) resolves fields via `EvalContext { features, event }`. Enriched fields from upstream map nodes are neither in `features` (they are not operator state) nor in `event` (they are not in the original event JSON). Without a third resolution source, enriched fields silently resolve to `Missing`.
**Prevention:**
- Add a third field to `EvalContext`: `enrichment: Option<&AHashMap<String, FeatureValue>>`. Resolution order: enrichment -> features -> event -> Missing.
- Alternative: merge enrichment into the event before passing to downstream. But this requires cloning the event (see C-1). The side-channel approach is cheaper.

### M-2. `@tl.dataset(depends_on=[...])` with class references creates import ordering issues

**Phase:** New SDK API
**Severity:** MODERATE
**What goes wrong:** If `@tl.dataset(depends_on=[Transactions])` requires `Transactions` to be defined BEFORE the dependent dataset, users must carefully order their class definitions. With circular or complex dependency structures, this becomes impossible in a single file.
**Prevention:**
- Allow string references in `depends_on`: `@tl.dataset(depends_on=["Transactions"])`. Resolve names to objects at registration time, not at definition time.
- Current `@st.stream(depends_on=[...])` at `_stream.py:127-130` already handles both class refs and strings. Carry this pattern forward.

### M-3. Union node concept has no existing server-side equivalent

**Phase:** Engine changes (union node)
**Severity:** MODERATE
**What goes wrong:** A union node (merge multiple input streams into one) is new. Tally's current `depends_on` model is a 1-to-N cascade (one upstream pushes to N downstreams). Union is N-to-1 (N upstreams push to one downstream). The cascade BFS in `push_with_cascade_internal` may visit the union node multiple times in a single event push if multiple upstream paths converge.
**Prevention:**
- Track a `visited: AHashSet<String>` per push invocation (already exists at `pipeline.rs:864`). Ensure the union node is only pushed to ONCE per event, even if multiple upstream paths converge on it.
- Define semantics: does a union node's event contain fields from ALL upstream events, or only the one that triggered this push? Answer: only the triggering upstream's event. Document this explicitly.
- Alternative: a union node is just a keyless stream with multiple entries in other streams' `depends_on`. The existing cascade already handles this via the BFS. Test with a diamond-shaped DAG to verify no double-push.

### M-4. Feature projection (selecting a subset of features) can break downstream derives

**Phase:** Engine changes (feature projection)
**Severity:** MODERATE
**What goes wrong:** If a Table is projected to only serve features [A, B] but has a downstream derive that references feature C, the derive fails silently with `Missing`. Projection must be a SERVING concern (what GET returns), not a COMPUTATION concern (what the engine maintains).
**Prevention:**
- Projection applies ONLY to GET responses and snapshot serialization. The engine always computes all features for all downstream dependencies.
- Implement projection as a post-read filter in `handle_get`, NOT as a restriction on operator state.
- The transient flag has the same shape: it restricts serving, not computation. Align the implementation patterns.

### M-5. Module rename from `tally` to `tl` changes every import in every test

**Phase:** New SDK API
**Severity:** MODERATE
**What goes wrong:** If the Python package is renamed from `import tally as st` to `import tally as tl` (or a new module name entirely), every test file's imports break. The `__init__.py` re-exports change. The `conftest.py` fixture changes. This is a mechanical change but touches 13+ test files.
**Prevention:**
- Keep the package name as `tally`. Add `tl` as a submodule or namespace: `from tally import tl` or `import tally.v2 as tl`. This allows old imports to coexist during migration.
- Alternative: keep `import tally as st` working as a compatibility shim that re-exports the new API under old names. Deprecation warnings, not hard breaks.
- Run `grep -r "import tally" python/tests/` to enumerate all import sites before any rename.

### M-6. Snapshot format must handle mixed old/new pipeline definitions during rolling upgrade

**Phase:** Engine changes
**Severity:** MODERATE
**What goes wrong:** If a v1.3 snapshot contains pipeline definitions in the old format and v2.0 adds new fields, the snapshot loader must handle both. If v2.0 changes how pipeline definitions are STORED in the snapshot (e.g., new JSON keys in `raw_register_jsons`), old snapshots may fail to load.
**Prevention:**
- `raw_register_jsons` stores the literal JSON sent by the client. If old clients sent JSON without `transient`, the loader defaults `transient=false`. This already works because of `#[serde(default)]`.
- Test: create a snapshot with v1.3, load with v2.0. Verify all streams restored.
- Test: create a snapshot with v2.0 (new fields), load with v1.3 (hypothetical rollback). v1.3 should ignore unknown fields via `serde(deny_unknown_fields)` NOT being set. Verify this.

### M-7. `app.serve()` pattern creates user confusion about what is queryable

**Phase:** New SDK API
**Severity:** MODERATE
**What goes wrong:** If `app.serve(user_features)` is required to make a Table queryable, users who forget `serve()` will wonder why GET returns nothing. The old API's behavior (everything is queryable by default) was simpler. The new pattern requires explicit opt-in.
**Prevention:**
- Default: all keyed Tables are served (queryable + snapshotted). `transient` must be explicitly requested: `@tl.dataset(transient=True)` or `app.source("raw", transient=True)`.
- Only keyless intermediate streams are transient by default (they have no key to query by).
- Clear error message: `GET for key X: no served datasets match this key. Did you forget app.serve()?`

---

## LOW pitfalls

### L-1. EventProxy (`table.event["field"]`) compiles to `_event.field` -- enriched fields are not event fields

**Phase:** SDK + Engine
**Severity:** LOW
**What goes wrong:** After enriched propagation, downstream datasets can reference enriched fields (computed by upstream map nodes). But `table.event["enriched_field"]` compiles to `_event.enriched_field`, which looks for the field in the original event JSON, not in the enrichment side-channel.
**Prevention:** Define resolution order in expression evaluator: enriched fields > event fields > features. Document that `_event.X` checks enrichment first.

### L-2. Existing `test_dataframe.py` tests may have outdated assertions after DataFrame API removal

**Phase:** Old API removal
**Severity:** LOW
**Prevention:** Include `test_dataframe.py` in the test migration checklist alongside `test_stream.py` and `test_view.py`.

### L-3. On-demand pipeline names may collide with pre-registered pipeline names

**Phase:** On-demand compute architecture
**Severity:** LOW
**Prevention:** Namespace ephemeral pipelines: `_ephemeral/{name}` prefix. Reject REGISTER for names starting with `_ephemeral/` from the normal registration path.

### L-4. Debug UI topology DAG grows unwieldy with many on-demand pipelines

**Phase:** On-demand compute architecture
**Severity:** LOW
**Prevention:** Add a filter toggle in the Debug UI: "Show ephemeral pipelines" (default: off). Ephemeral nodes get a distinct visual style (dashed border, gray).

### L-5. Python `__init__.py` re-exports become a mess during the transition

**Phase:** New SDK API + Old API removal
**Severity:** LOW
**Prevention:** During migration, `__init__.py` exports BOTH old and new API. After removal, clean up. Use `__all__` to control what tab-completes. Never have a phase where `__init__.py` exports broken symbols.

---

## Phase attribution summary

| # | Pitfall | Phase | Severity |
|---|---------|-------|----------|
| C-1 | Enriched propagation allocation cliff | Engine (enriched propagation) | CRITICAL |
| C-2 | Removing old API breaks 744 tests | Old API removal | CRITICAL |
| C-3 | RegisterRequest wire format backward compat | Engine (new fields) | CRITICAL |
| C-4 | DataFrame API also being replaced | New SDK API | CRITICAL |
| C-5 | Enriched propagation + DashMap concurrency | Engine (enriched propagation) | CRITICAL |
| H-1 | App.push() hardcoded attribute name | New SDK API | HIGH |
| H-2 | EventSet/FeatureSet server-side distinction | New SDK API + Engine | HIGH |
| H-3 | No atomic batch registration | New SDK API + Engine | HIGH |
| H-4 | Transient streams still consume memory | Engine (transient flag) | HIGH |
| H-5 | Ephemeral pipeline memory leak | On-demand architecture | HIGH |
| H-6 | REGISTER becomes concurrent hot-path | On-demand architecture | HIGH |
| H-7 | Auto-generated name collisions | New SDK API | HIGH |
| M-1 | EvalContext needs enrichment resolution | Engine (enriched propagation) | MODERATE |
| M-2 | depends_on import ordering | New SDK API | MODERATE |
| M-3 | Union node cascade semantics | Engine (union node) | MODERATE |
| M-4 | Feature projection breaks downstream derives | Engine (projection) | MODERATE |
| M-5 | Module rename breaks all test imports | New SDK API | MODERATE |
| M-6 | Snapshot format mixed old/new definitions | Engine | MODERATE |
| M-7 | app.serve() confusion about queryability | New SDK API | MODERATE |
| L-1 | EventProxy vs enriched field resolution | SDK + Engine | LOW |
| L-2 | test_dataframe.py migration | Old API removal | LOW |
| L-3 | Ephemeral pipeline name collision | On-demand architecture | LOW |
| L-4 | Debug UI with many ephemeral nodes | On-demand architecture | LOW |
| L-5 | __init__.py export mess during transition | New SDK API | LOW |

---

## The "Phase 11 class" of bug: enriched propagation silently changes feature values

The v1.2 milestone taught that subtle hot-path changes can produce correct results on simple pipelines but catastrophically wrong results on complex ones (HLL 148x slowdown only surfaced on large pipeline with 3 HLL operators).

The v2.0 equivalent traps:

1. **Enriched propagation changes what downstream derives "see."** A derive expression `failure_rate = failed_count / total_count` works today because both fields resolve from the same entity's features. With enriched propagation, if `failed_count` comes from an upstream map node's enrichment and `total_count` comes from operator state, the expression evaluator must resolve from two different sources. Test this with a 3-level DAG where derives reference BOTH enriched and operator-state fields.

2. **Run the full test matrix after EVERY engine change.** Not just the tests you wrote for the new feature. 601 Rust tests and 122 Python tests. Every time.

3. **Benchmark after every engine change.** The 1.1M eps baseline is load-bearing. Use it as a regression gate.

4. **Test old-format snapshots loading in new code.** Every time the RegisterRequest schema changes.

---

## Sources

- `src/engine/pipeline.rs` (lines 239-276, 852-917) -- StreamDefinition, push_with_cascade_internal
- `src/server/protocol.rs` (lines 409-456) -- RegisterRequest, FeatureDefRequest
- `python/tally/_app.py` (lines 86-302) -- App class, push(), register(), register_all()
- `python/tally/_stream.py` (lines 23-178) -- StreamMeta metaclass, @stream decorator
- `python/tally/_dataframe.py` (lines 36-500) -- DataFrame API: Stream, Table, GroupBy, JoinedTable
- `python/tally/__init__.py` (lines 1-76) -- re-exports for both APIs
- `.planning/research/horizon/HORIZON-DATAFRAME-API.md` -- DataFrame API design
- `.planning/research/horizon/HORIZON-STREAM-TABLE-DUALITY.md` -- stream-table duality, enriched propagation
- `.planning/PROJECT.md` -- current state, v2.0 milestone definition
- Memory notes: `project_v2_api_redesign.md`, `project_on_demand_compute.md`
- [MCSI Library: Memory Leaks and Resource Exhaustion](https://library.mosse-institute.com/articles/2023/07/resource-exhaustion.html)
- [Memory Leaks in Long-Running Data Jobs](https://www.mhtechin.com/support/memory-leaks-in-long-running-data-jobs-deep-dive/)
