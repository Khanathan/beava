# Roadmap: Tally

## Milestones

- v1.0 Core Feature Server — Phases 1-5 (shipped 2026-04-09)
- v1.1 Composable Pipeline & Event Log — Phases 6-10 (in progress)

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

### v1.1 Composable Pipeline & Event Log

**Milestone Goal:** Transform Tally into a composable streaming pipeline with SSD event log for replay/backfill, operational improvements, and a debug UI for observability.

- [x] **Phase 6: Foundation** - EntityState refactor for per-stream isolation, SSD event log with history TTL and compaction, per-stream entity TTL, MGET
- [x] **Phase 7: Composable Pipeline** - Keyless streams, keyed streams with depends_on, DAG execution with topological cascade, cycle detection, LEFT JOIN semantics
- [x] **Phase 8: Backfill & Schema Evolution** - Add/remove features without state reset, backfill replay from event log with event timestamps
- [x] **Phase 9: Incremental Snapshots** - Dirty-key tracking, delta snapshot files, base + delta recovery (completed 2026-04-10)
- [x] **Phase 10: Debug UI** - Embedded web UI for stream topology DAG, live throughput, memory breakdown, entity inspection (completed 2026-04-10)

## Phase Details

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

## Progress

**Execution Order:**
Phases execute in numeric order: 6 -> 7 -> 8 -> 9 -> 10

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
| 9. Incremental Snapshots | v1.1 | 2/2 | Complete   | 2026-04-10 |
| 10. Debug UI | v1.1 | 5/5 | Complete   | 2026-04-10 |

### Phase 10.1: Interactive Debug UI Redesign (INSERTED)
**Goal**: Users can explore the running pipeline through an interactive topology DAG where nodes are clickable drill-ins showing per-stream memory profile, state, and entity lookup scoped to that stream, and edges carry live throughput numbers updated at the existing 1 Hz polling cadence
**Depends on**: Phase 10
**Requirements**: DBUI-06 (new -- added during discuss)
**Success Criteria** (what must be TRUE):
  1. User can click a node on the Topology DAG and see a per-stream drill-in panel showing that stream's current memory footprint, live state summary, and an entity lookup input scoped to that stream
  2. User can query an entity key from within a node drill-in and see that entity's feature values for only the selected stream (not a global lookup)
  3. User can see live per-edge throughput numbers on the DAG, refreshed from `/debug/throughput` at 1 Hz, with visual distinction between cascade and lookup edges
  4. The flat Streams / Entity / Memory tabs from Phase 10 are either removed or demoted to a secondary navigation surface — the interactive DAG becomes the primary Debug UI entry point
  5. Every DOM write for user-supplied strings (entity keys, feature values, stream names) continues to use `.textContent` or d3 `.text()` — zero `.innerHTML` for user data (preserved from Phase 10 XSS defense contract)
**Plans:** 1/3 plans executed
Plans:
- [ ] 10.1-01-PLAN.md — Backend /debug/topology additive `operators` field (pass-through from raw_register_jsons) + 3 operator shape tests
- [ ] 10.1-02-PLAN.md — Frontend shell rewrite (index.html + app.css) — split-view layout, delete tab bar, inherit Phase 10 tokens verbatim + 2 shell structure tests
- [ ] 10.1-03-PLAN.md — Frontend behavior rewrite (app.js) — click-to-select drill-in panel, render-once dagre DAG, 1 Hz edge label updater, pause gate, stream-scoped entity lookup + browser smoke checkpoint

### Phase 10.2: Latency Debugger (INSERTED)
**Goal**: Users can observe and debug per-command latency through percentile histograms (p50/p95/p99) broken down by TCP command (PUSH/GET/SET/MSET) and stream, surfaced as a new latency view in the Debug UI (exact surface determined by Phase 10.1's interactive layout) and a `/debug/latency` JSON endpoint on the HTTP management port
**Depends on**: Phase 10, Phase 10.1
**Requirements**: DBUI-07 (new -- added during discuss)
**Success Criteria** (what must be TRUE):
  1. User can call GET /debug/latency and receive a JSON document with per-TCP-command p50/p95/p99 latencies plus per-stream breakdown
  2. User can see live per-command latency histograms refreshing at 1 Hz somewhere in the Debug UI (as a tab, as a node drill-in panel, or as edge tooltips — decided in discuss after Phase 10.1 lands)
  3. User can see a slow-query view listing the N slowest observed requests per command with the originating stream (if applicable)
  4. Latency tracking adds no measurable overhead to the PUSH hot path (p99 remains under 100us per existing Phase 6 budget)
  5. The estimator is memory-bounded per stream regardless of request rate (explicit choice between t-digest, HDR histogram, or bucketed histogram made during discuss)
**Plans:** 0 plans (to be planned)

Plans:
- [ ] TBD (run /gsd-plan-phase 10.2 to break down)
