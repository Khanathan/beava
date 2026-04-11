# Roadmap: Tally

## Milestones

- v1.0 Core Feature Server — Phases 1-5 (shipped 2026-04-09)
- v1.1 Composable Pipeline & Event Log — Phases 6-10.2 (shipped 2026-04-11)
- v1.2 Performance — Phase 11 (shipped 2026-04-11)
- v1.3 Concurrency & Client Batching — Phases 12-15 (planned)

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
**Plans:** 3/3 plans complete
Plans:
- [ ] 10.1-01-PLAN.md — Backend /debug/topology additive `operators` field (pass-through from raw_register_jsons) + 3 operator shape tests
- [ ] 10.1-02-PLAN.md — Frontend shell rewrite (index.html + app.css) — split-view layout, delete tab bar, inherit Phase 10 tokens verbatim + 2 shell structure tests
- [ ] 10.1-03-PLAN.md — Frontend behavior rewrite (app.js) — click-to-select drill-in panel, render-once dagre DAG, 1 Hz edge label updater, pause gate, stream-scoped entity lookup + browser smoke checkpoint

### Phase 10.2: Latency Debugger (INSERTED)
**Goal**: Users can observe and debug per-command latency through percentile histograms (p50/p95/p99) broken down by TCP command (PUSH/GET/SET/MSET) and stream, surfaced as a contextual latency view in the Debug UI drill-in panel and a `/debug/latency` JSON endpoint on the HTTP management port
**Depends on**: Phase 10, Phase 10.1
**Requirements**: DBUI-07 (new -- added during discuss)
**Success Criteria** (what must be TRUE):
  1. User can call GET /debug/latency and receive a JSON document with per-TCP-command p50/p95/p99 latencies plus per-stream breakdown
  2. User can see live per-command latency histograms refreshing at 1 Hz in the Debug UI drill-in panel
  3. User can see a slow-query view listing the 20 slowest observed requests per command with the originating stream (if applicable)
  4. Latency tracking adds no measurable overhead to the PUSH hot path (p99 remains under 100us per existing Phase 6 budget)
  5. The estimator is memory-bounded per stream regardless of request rate (bucketed histogram, ~248 bytes per histogram)
**Plans:** 3/3 plans complete

Plans:
- [x] 10.2-01-PLAN.md — Backend core: Histogram, RollingHistogram, SlowQueryHeap, LatencyTracker in src/server/latency.rs + unit tests
- [x] 10.2-02-PLAN.md — Backend integration: AppState wiring, TCP command instrumentation (PUSH/GET/SET/MSET), /debug/latency endpoint + integration test
- [x] 10.2-03-PLAN.md — Frontend: renderLatencySection, renderGlobalLatencyDashboard, histogram bars, slow-query list, 1 Hz polling + visual checkpoint

---

### v1.2 Performance — SHIPPED 2026-04-11

**Milestone Goal:** Lift single-node Tally throughput from ~17.5k eps (v1.1 baseline, single Python client, medium pipeline) toward the 100k-1M eps range by attacking the hot-path JSON cost, the round-trip response overhead, and eventually the single-threaded-runtime concurrency cliff.

**Outcome:** Phase 11 delivered the first big step — single-client async lands at 128–142k eps across small/medium/large pipelines on 1 core, with sync p99 = 87–90µs. Large pipeline moved from 865 eps → 128k eps (148×) after a mid-phase re-verification caught and fixed three latent bottlenecks:

| Pipeline | Mode | EPS | p99 µs |
|---|---|---:|---:|
| small | async 1c | 138k | — |
| medium | async 1c | 142k | — |
| large | async 1c | 128k | — |
| small | sync 1c | 20.4k | 87 |
| medium | sync 1c | 20.2k | 87 |
| large | sync 1c | 19.4k | 90 |

The 100k floor is hit on every pipeline size; the 1M ceiling is deferred to v1.3 (multi-threading path). See `benchmark/tally-throughput/RESULTS.md` for the full matrix and `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` for the post-verification fix analysis.

**Context documents:**
- `benchmark/FINDINGS.md` — original benchmark spike (synthetic Rust, macOS)
- `benchmark/tally-throughput/RESULTS.md` — real v1.1 wall-clock numbers + Phase 11 matrix
- `benchmark/tally-throughput/PROFILE.md` — callgrind profile (46% JSON, 8% engine)
- `benchmark/tally-throughput/FINDINGS-VS-REALITY.md` — scorecard of spike claims
- `benchmark/tally-throughput/PATH-TO-100K-1M.md` — lever math and target tiers
- `.planning/research/FINDINGS-GAP-ANALYSIS.md` — gap analysis vs FINDINGS

### Phase 11: Fire-and-Forget PUSH + Binary Wire Protocol — SHIPPED 2026-04-11
**Goal:** Ship `app.push()` as fire-and-forget (no feature response) + `app.push_sync()` + `app.flush()`, and replace the JSON event payload with a typed binary format on PUSH paths. Targets **≥ 100k events/sec single Python client** on medium pipelines (5.7x over v1.1 baseline).
**Depends on:** v1.1 complete
**Requirements:** PERF-01 (fire-and-forget ingest), PERF-02 (binary event payload)
**Plans:** 6 total (11-01 binary decoder + opcodes, 11-02 Python SDK, 11-03 tcp.rs dispatch, 11-04 raw-TCP tests, 11-05 Python tests + bench gate, **11-06 binary event log format subplan**)
**Result:** PASSED. Medium async single-client hit 166k eps at the original gate and 128–142k across all pipeline sizes after post-verification fixes. Sync p99 87–90µs across all sizes (v1.1 baseline was 129µs). 532/532 tests green. See `11-VERIFICATION.md` post-verification section for the three bugs (HLL read on async hot path, drain fast-path regression, residual JSON serialize) and `11-06-SUMMARY.md` for the mid-phase subplan.
**Context:** [.planning/phases/11-fire-and-forget-push/11-CONTEXT.md](phases/11-fire-and-forget-push/11-CONTEXT.md)

---

### v1.3 Concurrency & Client Batching

**Milestone Goal:** Break past the single-core ceiling hit in v1.2. Target **500k–1M eps on a single node** by (1) adding key-partitioned multi-threading so the 47 idle cores can carry load, (2) adding server-side async push coalescing to amortize per-event fixed costs, (3) adding an SDK batch-push API so a single client can produce events faster than the Python per-event loop ceiling, and (4) moving snapshot I/O off the main thread so large-state pipelines stop stalling during writes. Each phase is independently measurable against the v1.2 baseline in `benchmark/tally-throughput/RESULTS.md`.

**Why v1.3:** Phase 11 proved that on 1 core the server is bound at ~128–142k eps (66% CPU utilization; 7µs per push of real work). With 47 other cores on a typical box sitting idle, multi-threading is the single highest-ROI change — estimated 10–40× on aggregate throughput with N concurrent clients. The other phases target residual per-event cost (coalescing + batching) and the 15–25% duty-cycle loss to snapshot stalls.

**Context documents:**
- `benchmark/tally-throughput/RESULTS.md` — v1.2 final matrix (baseline for v1.3 regression checks)
- `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` — post-verification perf analysis (bottleneck breakdown)
- CLAUDE.md — "Scaling Path" section explicitly calls out key-partitioned multi-threading as the v2 upgrade

### Phase 12: Server-side async push coalescing
**Goal:** Buffer incoming `OP_PUSH_ASYNC` frames per-connection, process them in batches under a single `state.lock()` acquisition. Target **+50–100% async throughput** for multi-client workloads by amortizing fixed per-event costs (lock acquisition, event log append, fan-out target iteration, dirty-mark set insert).
**Depends on:** v1.2 complete
**Requirements:** PERF-03 (async coalescing)
**Success Criteria** (what must be TRUE):
  1. Server read loop accumulates up to N async push frames (default N=64) or waits up to T microseconds (default T=200µs) per connection before flushing to a single batch handler
  2. `handle_push_batch` takes a single state lock, groups events by primary stream, and issues one `engine.push_no_features` call + one `event_log.append` per stream
  3. Error attribution preserves the existing drain semantic: errors from events inside a batch surface on the client's next `push`/`flush`/`get` call, with stable ordering (first bad event first)
  4. Multi-client aggregate async throughput on the medium pipeline ≥ 200k eps with 4 clients (v1.2 baseline for 4 clients was ~30k due to per-event lock contention)
  5. Single-client async throughput on medium pipeline is within ±5% of the v1.2 baseline (142k) — coalescing must not regress single-client latency
  6. Latency impact documented: coalescing adds up to T µs to async p50 (acceptable, async is already fire-and-forget)
  7. All 532 existing tests remain green
**Plans:** TBD (research → plan phase)

### Phase 13: SDK batch push API + OP_PUSH_BATCH opcode
**Goal:** Expose a client-side batching API (`app.push_many(stream, events)`) that wraps N events into a single wire frame, reducing Python per-event loop overhead from ~7µs to ~0.3µs. Target **single-client async ≥ 300k eps** on medium pipeline when using `push_many`.
**Depends on:** Phase 12
**Requirements:** PERF-04 (client batch API)
**Success Criteria** (what must be TRUE):
  1. `app.push_many(stream_cls, events)` accepts an iterable of event dicts, encodes them into one binary frame, sends via a new `OP_PUSH_BATCH` (0x0A) opcode
  2. Server decodes the batch, dispatches to `handle_push_batch` (from Phase 12) for the grouped events
  3. Backward-compatible: `app.push()` single-event API continues to work, emits `OP_PUSH_ASYNC`
  4. Single-client async throughput on medium pipeline via `push_many` ≥ 300k eps (2× v1.2 single-client baseline)
  5. Error semantic: batch-level failures surface via `drain_errors_nonblock` with a payload indicating the offending event index within the batch
  6. Python SDK: `bench.py --mode async-batch` flag exercises the new API; results in `RESULTS.md`
  7. All 532 existing tests remain green; new batch tests cover encode/decode roundtrip, mixed-valid/invalid event handling, partial batch errors
**Plans:** TBD

### Phase 14: Key-partitioned multi-threaded engine (v2 architectural upgrade)
**Goal:** Break the single-core ceiling. Shard the `EntityState` map across N worker threads (N = `num_cpus::get()`) by hashing the entity key. Each worker owns its shard with no cross-thread locking. Target **aggregate 1M+ eps** on 16+ cores, proportional to core count.
**Depends on:** Phase 12, Phase 13 (coalescing makes per-worker amortization more effective)
**Requirements:** PERF-05 (key-partitioned concurrency)
**Success Criteria** (what must be TRUE):
  1. `StateStore` internals split into `Vec<Mutex<ShardStore>>` with `num_shards = num_cpus`; entity key → shard via stable hash
  2. TCP read loop dispatches pushes to the correct shard's channel; shard worker runs on its own tokio thread or a dedicated std::thread
  3. Cross-shard operations (fan-out to a different stream with a different key) go through an explicit cross-shard channel with batching
  4. Snapshot serialization works per-shard and merges into a single file on recovery, preserving v1.2 snapshot format compatibility
  5. Aggregate throughput on medium pipeline with 16 clients × 16 shards ≥ 1,000,000 eps (≈8× v1.2 single-core)
  6. Single-client throughput is within ±10% of v1.2 (acceptable: some overhead from shard dispatch for workloads that don't benefit from parallelism)
  7. All 532 existing tests remain green; new concurrency tests cover shard-routing correctness, cross-shard fan-out, and snapshot recovery from a multi-shard state
**Plans:** TBD. This is the largest architectural change since v1.0; expect research + plan + multi-wave execution.

### Phase 15: Snapshot I/O off main thread
**Goal:** Move snapshot writes off the main event-loop thread so large-state pipelines don't stall during the 2–7 second full-snapshot windows that currently cause 15–25% duty-cycle loss on sustained workloads.
**Depends on:** Phase 14 (easier to reason about with per-shard snapshots)
**Requirements:** OPS-05 (non-blocking snapshot write)
**Success Criteria** (what must be TRUE):
  1. Full snapshot serialization runs on a dedicated `spawn_blocking` task or a separate thread pool
  2. During a snapshot write, async PUSH throughput on the main path regresses by ≤ 5% (was 15–25% on v1.2 — measured by running a sustained-load bench during a snapshot cycle)
  3. Snapshot write still completes within the OPS-01 budget (< 1 second per 100k entities)
  4. Crash recovery from a partially-written snapshot is handled correctly (the existing `.tmp` → atomic rename pattern continues to work)
  5. Incremental snapshot (Phase 9) dirty-tracking integrates with the off-thread write — dirty set is snapshotted under lock, write runs without the lock
  6. All 532 existing tests + new "push during snapshot" stress tests green
**Plans:** TBD
