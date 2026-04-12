# Roadmap: Tally

## Milestones

- v1.0 Core Feature Server — Phases 1-5 (shipped 2026-04-09)
- v1.1 Composable Pipeline & Event Log — Phases 6-10.2 (shipped 2026-04-11)
- v1.2 Performance — Phase 11 (shipped 2026-04-11)
- v1.3 Concurrency & Client Batching — Phases 12-15 (partially shipped 2026-04-12; PERF-03/OPS-05 deferred)
- v2.0 New API & Engine — Phases 16-19 (active)

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>v1.0 Core Feature Server (Phases 1-5) — SHIPPED 2026-04-09</summary>

- [x] Phase 1: Core Engine (4/4 plans) — In-memory state store, windowed ring buffer, count/sum/avg, expression evaluator
- [x] Phase 2: TCP Server and Binary Protocol (5/5 plans) — tokio TCP server, binary protocol, all 5 commands
- [x] Phase 3: Python SDK (4/4 plans) — @st.stream/@st.view decorators, operator classes, TCP client
- [x] Phase 4: Persistence and Operational Readiness (3/3 plans) — Snapshots, crash recovery, TTL eviction, HTTP API
- [x] Phase 5: Advanced Operators and Cross-Stream (3/3 plans) — min/max/last/distinct_count, where-clause, views, lookups, fan-out

Full details: [v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md)

</details>

<details>
<summary>v1.1 Composable Pipeline & Event Log (Phases 6-10.2) — SHIPPED 2026-04-11</summary>

- [x] **Phase 6: Foundation** - EntityState refactor for per-stream isolation, SSD event log with history TTL and compaction, per-stream entity TTL, MGET
- [x] **Phase 7: Composable Pipeline** - Keyless streams, keyed streams with depends_on, DAG execution with topological cascade, cycle detection, LEFT JOIN semantics
- [x] **Phase 8: Backfill & Schema Evolution** - Add/remove features without state reset, backfill replay from event log with event timestamps
- [x] **Phase 9: Incremental Snapshots** - Dirty-key tracking, delta snapshot files, base + delta recovery
- [x] **Phase 10: Debug UI** - Embedded web UI for stream topology DAG, live throughput, memory breakdown, entity inspection
- [x] **Phase 10.1: Interactive Debug UI Redesign (INSERTED)** - Clickable DAG nodes, per-stream drill-in, edge throughput
- [x] **Phase 10.2: Latency Debugger (INSERTED)** - Per-command p50/p95/p99 histograms, slow-query view

Full details in phase sections below.

</details>

<details>
<summary>v1.2 Performance (Phase 11) — SHIPPED 2026-04-11</summary>

- [x] **Phase 11: Fire-and-Forget PUSH + Binary Wire Protocol** - app.push() fire-and-forget, binary event payload, 128-142k eps single-client

</details>

<details>
<summary>v1.3 Concurrency & Client Batching (Phases 12-15) — PARTIALLY SHIPPED 2026-04-12</summary>

- [ ] **Phase 12: Server-side async push coalescing** - Deferred (PERF-03, low ROI vs batch API)
- [x] **Phase 13: SDK batch push API + OP_PUSH_BATCH** - push_many(), 359k eps single-client batch
- [x] **Phase 14: Per-stream locks + DashMap concurrency** - 1.1M eps aggregate @ 8 proc
- [ ] **Phase 15: Snapshot I/O off main thread** - Deferred (OPS-05, revisit post-launch)

</details>

### v2.0 New API & Engine (Active)

**Milestone Goal:** Replace the `@st.stream` decorator API with a function-based `@tl.dataset(depends_on=[...])` pipeline pattern using `EventSet`/`FeatureSet` types. Fill engine gaps (enriched event propagation, feature projection, ephemeral pipeline flag). Remove old API. Architect for on-demand compute.

- [ ] **Phase 16: Python SDK -- New Types and Decorators** - @tl.source, @tl.dataset, EventSet/FeatureSet, .group_by().agg(), tl.union(), validate(), portable definitions
- [ ] **Phase 17: Enriched Event Propagation** - Side-channel enrichment in push_with_cascade_internal so downstream datasets see upstream computed fields
- [ ] **Phase 18: Feature Projection and Ephemeral Schema** - select()/drop() response filtering, ephemeral pipeline fields on RegisterRequest with #[serde(default)]
- [ ] **Phase 19: Test Migration and Old API Removal** - Port all tests to new API (>= 744), delete @st.stream/@st.view/_dataframe.py, benchmark regression gate

## Phase Details

<details>
<summary>v1.1 Phase Details (Phases 6-10.2)</summary>

### Phase 6: Foundation
**Goal**: Restructure entity state for per-stream isolation and establish the SSD event log as the persistence foundation for all subsequent v1.1 features
**Depends on**: Phase 5 (v1.0 complete)
**Requirements**: ELOG-01, ELOG-02, ELOG-03, ELOG-04, ELOG-05, OPS-01, OPS-02
**Success Criteria** (what must be TRUE):
  1. User can push events and observe them persisted to an append-only log file on disk, with keyed stream logs eligible for compaction and keyless stream logs append-only
  2. User can configure `history_ttl` per stream at registration time and expired events are removed by background compaction
  3. Event log writes do not measurably degrade PUSH p99 latency (remains under 100us with buffered async writes)
  4. User can fetch features for multiple keys in a single MGET call and receive all results in one response
  5. User can configure entity state TTL per stream, and keys expire independently per stream (short-TTL stream expiry does not evict long-TTL stream state for the same entity)
**Plans:** 4 plans
Plans:
- [x] 06-01-PLAN.md — EntityState restructure for per-stream isolation + snapshot v4
- [x] 06-02-PLAN.md — Per-stream entity TTL eviction + MGET command
- [x] 06-03-PLAN.md — SSD event log module + background timers integration
- [x] 06-04-PLAN.md — Python SDK updates (mget, entity_ttl, history_ttl)

### Phase 7: Composable Pipeline
**Goal**: Users can define multi-stage streaming pipelines where events automatically cascade through dependent streams in topological order
**Depends on**: Phase 6
**Requirements**: PIPE-01, PIPE-02, PIPE-03, PIPE-04, PIPE-05
**Success Criteria** (what must be TRUE):
  1. User can define a keyless stream that ingests raw events with no aggregation and no key field, with events persisted to the event log only
  2. User can define a keyed stream with `depends_on` declaring upstream dependencies, and pushing an event to an upstream stream automatically updates all downstream streams in correct topological order
  3. Registering a pipeline with circular dependencies is rejected with an error message identifying the cycle
  4. Downstream streams that depend on upstream values not yet computed receive null/missing values (LEFT JOIN semantics) rather than errors
**Plans:** 4 plans
Plans:
- [x] 07-01-PLAN.md — Rust type changes: keyless streams, depends_on, filter, petgraph dependency
- [x] 07-02-PLAN.md — Python SDK: optional key, depends_on, filter on @st.stream()
- [x] 07-03-PLAN.md — DAG construction with petgraph, cascade execution, cycle detection
- [x] 07-04-PLAN.md — TCP handler cascade integration + E2E tests

### Phase 8: Backfill & Schema Evolution
**Goal**: Users can evolve stream definitions over time -- adding and removing features without state reset -- and backfill new features from the event log for deterministic results
**Depends on**: Phase 6, Phase 7
**Requirements**: SCHM-01, SCHM-02, SCHM-03, SCHM-04, SCHM-05
**Success Criteria** (what must be TRUE):
  1. User can re-register a stream with a new feature added and existing feature state is preserved without reset
  2. User can re-register a stream with a feature removed and remaining features continue operating correctly
  3. User can register a new feature with `backfill=True` and the system replays historical events from the event log to populate the feature, producing deterministic results matching what live processing would have produced (using event timestamps, not wall clock)
  4. During backfill replay, live PUSH and GET requests continue to be served without noticeable latency degradation (cooperative yielding)
**Plans:** 2 plans
Plans:
- [x] 08-01-PLAN.md — Schema diff engine, backfill type system, lazy GC, REGISTER diff response, Python SDK backfill kwarg
- [x] 08-02-PLAN.md — Backfill replay engine, cooperative yielding, HTTP /debug/backfill, integration tests

### Phase 9: Incremental Snapshots
**Goal**: Snapshot persistence only serializes changed entities, reducing snapshot write time and disk I/O proportional to change rate rather than total state size
**Depends on**: Phase 6
**Requirements**: OPS-03, OPS-04
**Success Criteria** (what must be TRUE):
  1. After a period of writes affecting a subset of keys, the snapshot written is proportional to the number of changed keys rather than total keys
  2. Server can recover from a base snapshot plus subsequent delta snapshots and restore full state correctly
  3. Full snapshots are periodically written (every Nth cycle) to bound recovery time even with many deltas
**Plans:** 2/2 plans complete
Plans:
- [x] 09-01-PLAN.md — Dirty/deleted tracking in StateStore + v6 snapshot format (base/delta) + recovery + v5 migration
- [x] 09-02-PLAN.md — Wire incremental snapshots into timer, mutations, eviction, HTTP trigger, startup + integration tests

### Phase 10: Debug UI
**Goal**: Users can observe and debug the running system through an embedded web UI served from the existing HTTP management port
**Depends on**: Phase 6, Phase 7
**Requirements**: DBUI-01, DBUI-02, DBUI-03, DBUI-04, DBUI-05
**Success Criteria** (what must be TRUE):
  1. User can open a browser to the HTTP management port and see the stream topology rendered as a DAG
  2. User can see real-time per-stream throughput (messages/sec) updating live without manual refresh
  3. User can search for any entity key and inspect its current feature values across all streams
  4. User can see a memory usage breakdown showing per-stream and total memory consumption
  5. The debug UI is embedded in the Tally binary with no separate process, npm build, or external files required
**Plans:** 5/5 plans complete
Plans:
- [x] 10-01-PLAN.md — Add rust-embed dep, vendor htmx/d3/dagre-d3, write VENDOR.md manifest, browser smoke test
- [x] 10-02-PLAN.md — ThroughputTracker (EWMA 5s/60s/5m) + AppState wiring with cascade/fan-out dedup
- [x] 10-03-PLAN.md — Backend handlers: rust-embed UiAssets + /debug/topology + /debug/throughput + extended /debug/memory
- [x] 10-04-PLAN.md — Frontend assets: index.html, app.css, app.js, icons.svg, favicon.svg + browser smoke test
- [x] 10-05-PLAN.md — Integration tests (test_debug_ui.rs, 15 cases, SHA256 pins) + fix stale test_server.rs AppState
**UI hint**: yes

### Phase 10.1: Interactive Debug UI Redesign (INSERTED)
**Goal**: Users can explore the running pipeline through an interactive topology DAG where nodes are clickable drill-ins showing per-stream memory profile, state, and entity lookup scoped to that stream, and edges carry live throughput numbers updated at the existing 1 Hz polling cadence
**Depends on**: Phase 10
**Requirements**: DBUI-06
**Success Criteria** (what must be TRUE):
  1. User can click a node on the Topology DAG and see a per-stream drill-in panel showing that stream's current memory footprint, live state summary, and an entity lookup input scoped to that stream
  2. User can query an entity key from within a node drill-in and see that entity's feature values for only the selected stream (not a global lookup)
  3. User can see live per-edge throughput numbers on the DAG, refreshed from `/debug/throughput` at 1 Hz, with visual distinction between cascade and lookup edges
  4. The flat Streams / Entity / Memory tabs from Phase 10 are either removed or demoted to a secondary navigation surface — the interactive DAG becomes the primary Debug UI entry point
  5. Every DOM write for user-supplied strings (entity keys, feature values, stream names) continues to use `.textContent` or d3 `.text()` — zero `.innerHTML` for user data (preserved from Phase 10 XSS defense contract)
**Plans:** 3/3 plans complete
Plans:
- [x] 10.1-01-PLAN.md — Backend /debug/topology additive `operators` field
- [x] 10.1-02-PLAN.md — Frontend shell rewrite (index.html + app.css)
- [x] 10.1-03-PLAN.md — Frontend behavior rewrite (app.js)

### Phase 10.2: Latency Debugger (INSERTED)
**Goal**: Users can observe and debug per-command latency through percentile histograms (p50/p95/p99) broken down by TCP command and stream
**Depends on**: Phase 10, Phase 10.1
**Requirements**: DBUI-07
**Success Criteria** (what must be TRUE):
  1. User can call GET /debug/latency and receive a JSON document with per-TCP-command p50/p95/p99 latencies plus per-stream breakdown
  2. User can see live per-command latency histograms refreshing at 1 Hz in the Debug UI drill-in panel
  3. User can see a slow-query view listing the 20 slowest observed requests per command
  4. Latency tracking adds no measurable overhead to the PUSH hot path (p99 remains under 100us)
  5. The estimator is memory-bounded per stream regardless of request rate
**Plans:** 3/3 plans complete
Plans:
- [x] 10.2-01-PLAN.md — Backend core: Histogram, RollingHistogram, SlowQueryHeap, LatencyTracker
- [x] 10.2-02-PLAN.md — Backend integration: AppState wiring, TCP command instrumentation, /debug/latency endpoint
- [x] 10.2-03-PLAN.md — Frontend: renderLatencySection, histogram bars, slow-query list, 1 Hz polling

</details>

<details>
<summary>v1.2 Phase Details (Phase 11)</summary>

### Phase 11: Fire-and-Forget PUSH + Binary Wire Protocol
**Goal:** Ship `app.push()` as fire-and-forget + `app.push_sync()` + `app.flush()`, and replace JSON event payload with typed binary format on PUSH paths. Target >= 100k eps single Python client.
**Depends on:** v1.1 complete
**Requirements:** PERF-01, PERF-02
**Plans:** 6 total
Plans:
- [x] 11-01 through 11-06 (all complete)

</details>

<details>
<summary>v1.3 Phase Details (Phases 12-15)</summary>

### Phase 12: Server-side async push coalescing
**Goal:** Buffer incoming OP_PUSH_ASYNC frames per-connection, process in batches.
**Depends on:** v1.2 complete
**Requirements:** PERF-03
**Status:** Deferred (low ROI vs batch API)
**Plans:** 1/3 plans executed
Plans:
- [x] 12-01-PLAN.md — Add batch primitives
- [ ] 12-02-PLAN.md — ConnAccumulator + handle_push_batch
- [ ] 12-03-PLAN.md — Bench matrix

### Phase 13: SDK batch push API + OP_PUSH_BATCH
**Goal:** Client-side batching API, push_many(), 359k eps single-client batch.
**Depends on:** Phase 12
**Requirements:** PERF-04
**Plans:** 2/2 plans complete
Plans:
- [x] 13-01-PLAN.md — OP_PUSH_BATCH server-side decode + dispatch
- [x] 13-02-PLAN.md — Python SDK push_many + bench matrix

### Phase 14: Per-stream locks + DashMap concurrency
**Goal:** Replace global Mutex with per-stream DashMap + parking_lot RwLock. 1.1M eps @ 8 proc.
**Depends on:** Phase 12, Phase 13
**Requirements:** PERF-05
**Plans:** 3/3 plans complete
Plans:
- [x] 14-01-PLAN.md — Core refactor: ConcurrentAppState, StreamStore with DashMap
- [x] 14-02-PLAN.md — Background systems: snapshot/eviction/event-log DashMap adaptation
- [x] 14-03-PLAN.md — Benchmark gate

### Phase 15: Snapshot I/O off main thread
**Goal:** Move snapshot writes off main event-loop thread.
**Depends on:** Phase 14
**Requirements:** OPS-05
**Status:** Deferred (revisit post-launch)
**Plans:** TBD

</details>

### Phase 16: Python SDK -- New Types and Decorators
**Goal**: Users can define streaming pipelines using the new function-based API with explicit dependency declaration, typed schemas, and explicit aggregation -- all compiling to the existing RegisterRequest JSON format and testable on the current server without Rust changes
**Depends on**: Phase 14 (v1.3 shipped state)
**Requirements**: API-01, API-02, API-03, API-04, API-05, API-06, API-07
**Success Criteria** (what must be TRUE):
  1. User can define a source with `@tl.source` that compiles to a keyless stream RegisterRequest, and push events to it via the existing server
  2. User can define a derived dataset with `@tl.dataset(depends_on=[...])` that declares upstream dependencies and uses `.group_by("key").agg(count=tl.count(window="1h"), ...)` for explicit aggregation, compiling to a keyed stream RegisterRequest
  3. User can declare typed input schemas with `EventSet` and output schemas with `FeatureSet` using `Field` descriptors, with IDE autocomplete working via `dataclass_transform`
  4. User can merge multiple event sources into one dataset with `tl.union(source_a, source_b)` and the resulting RegisterRequest has multi-parent `depends_on`
  5. User can call `pipeline.validate()` locally and get clear error messages for cycles, missing dependencies, and type mismatches -- without contacting the server
**Plans:** 2/2 plans complete
Plans:
- [x] 16-01-PLAN.md — Core types: _schema.py (EventSet/FeatureSet/Field), _source.py (@tl.source), _dataset.py (@tl.dataset/group_by/union)
- [x] 16-02-PLAN.md — Validation, exports, integration: _validate.py, __init__.py wiring, JSON compat tests

### Phase 17: Enriched Event Propagation
**Goal**: Downstream datasets can reference upstream computed fields (derives, aggregations) during cascade execution, enabling multi-stage computed features like map -> group_by -> downstream sum("amount_usd")
**Depends on**: Phase 16
**Requirements**: ENG-01
**Success Criteria** (what must be TRUE):
  1. User can define a multi-stage pipeline where an upstream dataset computes a derived field and a downstream dataset aggregates that derived field, and PUSH returns the correct downstream result in a single request-response cycle
  2. Enriched fields propagate via a side-channel accumulator (not event clone) and full benchmark matrix passes within -5% of 1.1M eps baseline (pitfall C-1 gate)
  3. Enrichment works correctly under multi-threaded tokio runtime with 8 concurrent clients (pitfall C-5 -- enrichment values never re-enter DashMap during downstream push)
**Plans:** 3/3 plans complete
Plans:
- [x] 17-01-PLAN.md — Operator trait + EvalContext enrichment parameter (contracts)
- [x] 17-02-PLAN.md — Cascade enrichment accumulator in push_with_cascade_internal
- [x] 17-03-PLAN.md — Integration tests + concurrent correctness + benchmark gate

### Phase 18: Feature Projection and Ephemeral Schema
**Goal**: Users can control which features appear in PUSH/GET responses, and the RegisterRequest schema is extended with ephemeral pipeline fields for future on-demand compute
**Depends on**: Phase 17
**Requirements**: ENG-02, ENG-03
**Success Criteria** (what must be TRUE):
  1. User can call `select()` or `drop()` on a dataset and only the projected features appear in PUSH and GET responses for that stream
  2. All new RegisterRequest fields (`projection`, `ephemeral`, `ttl`, `max_keys`) use `#[serde(default)]` and a v1.3-format RegisterRequest loads successfully on the v2.0 server (pitfall C-3 backward compat)
  3. Snapshot round-trip test passes: register with new fields, snapshot, restart, verify fields preserved
**Plans:** 2 plans
Plans:
- [ ] 18-01-PLAN.md -- Rust: Projection enum, RegisterRequest fields, push_internal/get_features filtering, backward compat + snapshot tests
- [ ] 18-02-PLAN.md -- Python SDK select()/drop() on DatasetDef + end-to-end integration tests

### Phase 19: Test Migration and Old API Removal
**Goal**: All existing tests are ported to the new API surface, the old API is cleanly removed, and performance is verified unchanged
**Depends on**: Phase 16, Phase 17, Phase 18
**Requirements**: MIG-01, MIG-02, MIG-03
**Success Criteria** (what must be TRUE):
  1. All existing tests (>= 744) pass using only `@tl.source`, `@tl.dataset`, `EventSet`, and `FeatureSet` -- no references to `@st.stream`, `@st.view`, or `_dataframe.py` public API remain in test code
  2. `@st.stream`, `@st.view`, legacy operator aliases, and `_dataframe.py` public API are deleted from the SDK -- `import tally` no longer exposes any old API symbols
  3. `cargo test && pytest` pass with >= 744 tests on the new API only
  4. Full benchmark matrix (small/medium/large x sync/async/batch x 1c/4c/8c) passes within -5% of 1.1M eps baseline
  5. No `@st.stream` or `@st.view` references exist outside archived files (grep verification)
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 16 -> 17 -> 18 -> 19

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Core Engine | v1.0 | 4/4 | Complete | 2026-04-09 |
| 2. TCP Server and Binary Protocol | v1.0 | 5/5 | Complete | 2026-04-09 |
| 3. Python SDK | v1.0 | 4/4 | Complete | 2026-04-09 |
| 4. Persistence and Operational Readiness | v1.0 | 3/3 | Complete | 2026-04-09 |
| 5. Advanced Operators and Cross-Stream | v1.0 | 3/3 | Complete | 2026-04-09 |
| 6. Foundation | v1.1 | 4/4 | Complete | 2026-04-10 |
| 7. Composable Pipeline | v1.1 | 4/4 | Complete | 2026-04-10 |
| 8. Backfill & Schema Evolution | v1.1 | 2/2 | Complete | 2026-04-10 |
| 9. Incremental Snapshots | v1.1 | 2/2 | Complete | 2026-04-10 |
| 10. Debug UI | v1.1 | 5/5 | Complete | 2026-04-10 |
| 10.1 Interactive Debug UI | v1.1 | 3/3 | Complete | 2026-04-10 |
| 10.2 Latency Debugger | v1.1 | 3/3 | Complete | 2026-04-10 |
| 11. Fire-and-Forget PUSH + Binary Wire | v1.2 | 6/6 | Complete | 2026-04-11 |
| 12. Server-side async push coalescing | v1.3 | 1/3 | Deferred | - |
| 13. SDK batch push + OP_PUSH_BATCH | v1.3 | 2/2 | Complete | 2026-04-12 |
| 14. Per-stream locks + DashMap concurrency | v1.3 | 3/3 | Complete | 2026-04-12 |
| 15. Snapshot I/O off main thread | v1.3 | 0/? | Deferred | - |
| 16. Python SDK -- New Types and Decorators | v2.0 | 2/2 | Complete    | 2026-04-12 |
| 17. Enriched Event Propagation | v2.0 | 3/3 | Complete    | 2026-04-12 |
| 18. Feature Projection and Ephemeral Schema | v2.0 | 0/2 | Planning    | - |
| 19. Test Migration and Old API Removal | v2.0 | 0/? | Not started | - |
