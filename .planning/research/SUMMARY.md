# Project Research Summary

**Project:** Tally v2.0 — New API & Engine
**Domain:** Function-based streaming pipeline API with EventSet/FeatureSet types, Rust engine enrichment, on-demand compute architecture
**Researched:** 2026-04-12
**Confidence:** HIGH

## Executive Summary

Tally v2.0 is a disciplined API redesign of an existing, functioning real-time feature server. The change replaces the `@st.stream` decorator pattern with a function-based `@tl.dataset(depends_on=[...])` pattern using explicit `EventSet` / `FeatureSet` types — informed directly by the founder's experience at Fennel and validated against Pathway, Hamilton, Bytewax, and dbt. The ecosystem has converged on three principles that v2.0 must implement: explicit dependency declaration, explicit grouping via `.group_by().agg()`, and typed input/output contracts. The critical differentiator is that Tally's types are NOT DataFrames — they honestly represent event streams and keyed feature tables without the Pandas compat trap.

The recommended approach is a staged, incremental rewrite. The Python SDK and Rust engine work streams can proceed in parallel since all new Python types compile to a superset of the same `RegisterRequest` JSON the engine already consumes. The Rust engine requires exactly three surgical additions totaling roughly 200 LOC: enriched event propagation (the critical unlock — ~50 LOC in `push_with_cascade_internal`), feature projection (~20 LOC response filter), and union node semantics (~30 LOC in `rebuild_dag`). Zero new Rust crates. Zero wire protocol changes. The single new Python dependency is `typing_extensions>=4.6` for `dataclass_transform` backport. Old API removal is a clean break justified by the pre-launch stage — no external users to migrate.

The key risk cluster is centered on enriched event propagation. Naively cloning `serde_json::Value` per cascade hop can collapse throughput at 1.1M eps; the correct approach is a side-channel `AHashMap<String, FeatureValue>` that passes enriched fields without copying the event. A secondary risk is the "two APIs being replaced" confusion — v2.0 replaces BOTH `@st.stream` AND the DataFrame API (`_dataframe.py`), which means 744 tests need migrating before any code deletion. The safe sequencing is: build new API, port all tests, verify count >= 744, then remove old API.

---

## Key Findings

### Recommended Stack

The existing Rust stack is locked in and requires no new crates. All v2.0 engine changes use crates already in `Cargo.toml`: `serde_json` for enriched event mutation, `petgraph 0.8` for multi-parent union DAGs, and `parking_lot` for the existing lock hierarchy. The Python SDK adds exactly one new dependency.

**Core technologies:**
- `typing_extensions>=4.6` (Python, new): `dataclass_transform` backport for PEP 681 — gives IDE autocomplete on `@tl.source`/`@tl.dataset` without Pydantic's 5.4MB Rust core
- `serde_json::Value` side-channel (Rust, existing): enrichment accumulator as `AHashMap<String, FeatureValue>` — zero-copy approach avoids per-hop allocation cliff at 1.1M eps
- `petgraph 0.8` (Rust, existing): multi-parent DAG for union nodes — already supports multiple `depends_on` entries natively
- Custom `Field` descriptor (Python, new, zero deps): typed schema fields for EventSet/FeatureSet — proxy objects that compile to `RegisterRequest` JSON

**Explicitly rejected:** Pydantic v2 (5.4MB Rust core for definition-only SDK), new Rust crates for enriched propagation (50 LOC of `serde_json` manipulation), `uuid` crate for ephemeral IDs (SDK-side naming convention suffices).

### Expected Features

**Must have (table stakes):**
- `@tl.dataset(depends_on=[...])` function-based decorator — every DAG-based pipeline system makes dependencies explicit at definition site
- `EventSet` and `FeatureSet` types — honest types without DataFrame pretense
- Explicit `.group_by("key").agg(...)` — universal pattern; already built in `_dataframe.py`, port to new surface
- `filter()`, `transform()`/`map()`, `join()`, `select()`/`drop()`/`rename()` — table stakes; all map to existing engine capabilities
- `union()` — merge multiple event sources; multi-parent `depends_on` already works in the DAG
- Enriched event propagation — the single most important engine change; without it, derived datasets cannot reference upstream computed fields
- Feature projection (response-only) — `select()` restricts PUSH/GET responses; ~20 LOC Rust filter
- Old API removal — clean break; `@st.stream`, `@st.view`, and `_dataframe.py` all replaced

**Should have (competitive differentiators):**
- Portable pipeline definitions — same JSON works for startup registration, runtime REGISTER, and future ephemeral pipelines
- Ephemeral pipeline schema fields (`ephemeral: bool`, `ttl`, `max_keys`) — add to `RegisterRequest` now, implement lifecycle post-launch
- `validate()` method for local DAG validation before server submission — prevents partial registration state
- Stable dataset names via function names — prevents auto-naming collision bugs

**Defer to post-launch:**
- On-demand compute lifecycle (TTL enforcement, memory limits, pipeline-level eviction)
- One-shot replay queries — requires S3 replay log as primitive
- Typed schema validation at REGISTER time
- Computation-pruning projection (start response-only, optimize later)
- Session windows, cross-key aggregations, watermarks/late-arrival handling

**Anti-features (explicitly avoid):**
- DataFrame simulation — every missing method is a support ticket
- Python UDFs/lambdas in pipeline — serializing closures is fragile; expression language covers 95% of use cases
- Fennel-style `@extractor` functions — derive expressions already serve this purpose with zero Python overhead

### Architecture Approach

The v2.0 architecture is primarily a Python SDK rewrite that compiles to a superset of the existing `RegisterRequest` JSON format. The Rust engine receives three surgical additions but its core structures survive unchanged. `EventSet` and `FeatureSet` are Python-side compile-time abstractions only; the server has no concept of them. This means the new API is 100% testable on the existing server before any Rust changes land.

**Major components (new or modified):**
1. `@tl.source` / `@tl.dataset` decorators (`_source.py`, `_dataset.py`) — compile Python function definitions to `RegisterRequest` JSON; replace all three legacy API layers
2. `EventSet` / `FeatureSet` types (`_types.py`) — Python-only type annotations; validate constraints at definition time
3. Enriched event propagation (`pipeline.rs`) — side-channel `AHashMap` accumulates upstream derive results; `EvalContext` gains third resolution source (enrichment → features → event → Missing)
4. `StreamDefinition` additions (`pipeline.rs`) — `projection: Option<Vec<String>>`, `ephemeral: bool`; both additive with `#[serde(default)]`
5. `EphemeralLimits` manager — max pipelines, max keys, memory budget; snapshot filtering

**Unchanged:** `StateStore`/`DashMap`, `OperatorState` enum, wire protocol opcodes, expression evaluator, event log, window/HLL/operators.

### Critical Pitfalls

1. **Enriched propagation allocation cliff (C-1)** — Never clone `serde_json::Value` per cascade hop. Use side-channel `AHashMap<String, FeatureValue>`. Gate: < 5% regression from 1.1M eps baseline before merging.

2. **Old API removal breaks 744 tests (C-2)** — Mandatory sequencing: build new API → port ALL tests → verify count >= 744 → then remove old API. Never in the same phase.

3. **RegisterRequest wire format backward compat (C-3)** — ALL new fields must have `#[serde(default)]`. Add snapshot round-trip test. Bump snapshot format version.

4. **DataFrame API ALSO being replaced (C-4)** — v2.0 removes three APIs: `@st.stream`, `@st.view`, AND `_dataframe.py`. Test migration checklist must include `test_dataframe.py`.

5. **Enriched propagation + DashMap concurrency (C-5)** — Enrichment values computed during upstream push, stored in local variable, never re-enter DashMap during downstream push. Write 8-thread concurrency stress test.

---

## Implications for Roadmap

Based on combined research, the natural phase structure follows the dependency graph: Python SDK types first (de-risks API design before touching Rust), critical engine unlock second, additive engine features third, cleanup last.

### Phase 1: Python SDK — New Types and Decorators
**Rationale:** 100% testable on existing server without Rust changes. De-risks the API design. If JSON format needs adjustment, discover it before touching the hot path.
**Delivers:** `@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet`, `tl.union()`, `Field` descriptor, `dataclass_transform` IDE integration, local `validate()` method, `_tally_stream_name` protocol on new objects
**Addresses features:** All new Python-surface API (decorator, types, operators, union, portable definitions)
**Avoids pitfalls:** C-4 (explicit plan for `_dataframe.py`), H-1 (`_tally_stream_name` protocol), H-7 (explicit names via function names), M-2 (string `depends_on` refs), M-5 (keep `tally` package name)
**Test plan:** Python unit tests verifying JSON compilation. Integration tests on existing unmodified server. New-API test count must meet or exceed 744 before Phase 4 can run.

### Phase 2: Rust Engine — Enriched Event Propagation
**Rationale:** The single most critical engine change. Without it, the multi-stage pipeline pattern (`map` → `group_by` → downstream `sum("amount_usd")`) does not work and the entire v2.0 value proposition breaks.
**Delivers:** Side-channel enrichment in `push_with_cascade_internal`; `EvalContext.enrichment` third resolution source; `needs_derive_for_cascade` pre-computed in `rebuild_dag` for async optimization
**Addresses features:** Derived datasets referencing upstream computed fields
**Avoids pitfalls:** C-1 (side-channel, no event clone), C-5 (enrichment computed under entity lock), M-1 (EvalContext resolution order)
**Test plan:** Multi-stage integration tests. Sync and async push mode verification. Full pipeline matrix benchmark; gate on < 5% regression from 1.1M eps baseline.

### Phase 3: Rust Engine — Feature Projection and Ephemeral Flag
**Rationale:** Small, additive, independent changes. Can run in parallel with Phase 4. Projection correctness depends on Phase 2 being stable (must confirm projection does not interfere with enrichment propagation).
**Delivers:** `projection` on `StreamDefinition`; response-layer feature filtering; `ephemeral: bool`; snapshot filtering; new `RegisterRequest` optional fields with `#[serde(default)]`
**Avoids pitfalls:** C-3 (all new fields defaulted, snapshot round-trip test), M-4 (projection is response-only), H-4 (keyless streams with only derives do not create entity state — verify preserved)
**Test plan:** Projection unit tests. Empty projection and nonexistent field name edge cases. v1.3-format snapshot load test in v2.0 server. No benchmark regression from Phase 2 baseline.

### Phase 4: Old API Removal
**Rationale:** Clean break correct pre-launch. Deferred until Phase 1 tests are stable and count >= 744. Phases 3 and 4 can run in parallel after Phase 2 merges.
**Delivers:** Deletion of `_stream.py`, `_view.py`, legacy operator aliases, `@st.stream`/`@st.view` surface, legacy `_dataframe.py` public API; clean `__init__.py`
**Avoids pitfalls:** C-2 (port all tests first), L-2 (`test_dataframe.py` in migration checklist), L-5 (no phase where `__init__.py` exports broken symbols)
**Test plan:** `cargo test && pytest` pass on new API only. Test count >= 744. No `@st.stream` references outside archived files.

### Phase Ordering Rationale

- Python SDK first: API design mistakes surface before Rust is touched; new types compile to existing JSON, 100% testable immediately
- Enriched propagation second: highest-risk hot-path change; must be validated and benchmarked in isolation before other Rust changes layer on top
- Projection and ephemeral flag third: both small and additive; projection correctness depends on enrichment stability; can run as parallel work streams within the phase
- Old API removal last: sequential dependency on Phase 1 test count being >= 744; Phases 3 and 4 can run in parallel after Phase 2 merges

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2 (Enriched Propagation):** The async path optimization (`needs_derive_for_cascade` pre-computation) interacts with `push_no_features` in non-obvious ways. Recommend a design doc with traced examples for the 3-level cascade before coding.
- **Phase 3 (Ephemeral Flag):** Memory limit enforcement design (per-pipeline key count, global budget, eviction policy) is pattern-based, not yet code-grounded. Needs implementation design before `EphemeralLimits` is finalized.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Python SDK):** `dataclass_transform` is well-documented (PEP 681, Fennel, attrs); compilation model already exists in `_dataframe.py`
- **Phase 4 (Old API Removal):** Mechanical deletion with clear inventory; no design decisions

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All Rust changes use existing verified crates; single Python dep confirmed against PEP 681 and 3.10 compat |
| Features | HIGH | Fennel pattern from direct founder experience; Pathway/Hamilton/dbt from active official sources; engine gaps from direct code inspection |
| Architecture | HIGH | Design grounded in direct reading of `pipeline.rs`, `protocol.rs`, `_dataframe.py`; changes are well-scoped additions |
| Pitfalls | HIGH (integration); MEDIUM (ephemeral lifecycle) | Integration pitfalls cited to line numbers; ephemeral lifecycle is pattern-based, lifecycle deferred |

**Overall confidence:** HIGH

### Gaps to Address

- **Async derive evaluation cost:** The ~1-2us per intermediate cascade stage estimate is analytical. Validate with benchmarks in Phase 2 before declaring complete.
- **EphemeralLimits defaults:** `max_ephemeral_pipelines`, `max_keys_per_ephemeral_pipeline`, `max_ephemeral_memory_bytes` need validation against real memory profiles. Design in Phase 3 planning.
- **PUSH response UX for new API:** PUSH to keyless `@tl.source` returns empty FeatureMap; downstream keyed features require GET. Research recommends deferring a fix post-v2.0 — confirm this UX tradeoff before Phase 1 ships.
- **`_dataframe.py` as internal backend:** `@tl.dataset` can reuse `_dataframe.py`'s `GroupBy`/`JoinedTable` as an internal compilation backend rather than rewriting. Confirm this design choice before starting Phase 1 — it affects scope significantly.

---

## Sources

### Primary (HIGH confidence)
- Tally codebase: `python/tally/_dataframe.py`, `python/tally/_app.py`, `src/engine/pipeline.rs` (lines 239-276, 852-917), `src/server/protocol.rs` (lines 409-456) — direct code inspection
- [PEP 681 — Data Class Transforms](https://peps.python.org/pep-0681/) — `dataclass_transform` specification
- [Pathway documentation v0.27.1](https://pathway.com/developers) — active project, Jan 2026
- [Apache Hamilton](https://github.com/apache/hamilton) — function-as-DAG-node pattern
- [dbt incremental models](https://docs.getdbt.com) — official docs
- Memory notes: `project_v2_api_redesign.md`, `project_on_demand_compute.md` — direct founder input
- `.planning/research/horizon/HORIZON-DATAFRAME-API.md`, `HORIZON-STREAM-TABLE-DUALITY.md` — prior validated research

### Secondary (MEDIUM confidence)
- [Fennel AI dataset/pipeline docs](https://github.com/fennel-ai/client) — official but potentially stale post-Databricks acquisition
- [Fennel AI FeatureSet](https://fennel.ai/docs/concepts/featureset) — post-acquisition update risk
- [Bytewax](https://github.com/bytewax/bytewax) — project winding down, last OSS release Nov 2024

### Tertiary (LOW confidence)
- Ephemeral lifecycle memory limit defaults — analytical estimates, not measured; validate in Phase 3

---
*Research completed: 2026-04-12*
*Ready for roadmap: yes*
