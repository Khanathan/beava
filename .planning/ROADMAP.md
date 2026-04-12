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
Phases execute in numeric order: 6 -> 7 -> 8 -> 9 -> 10 -> 10.1 -> 10.2 -> 11 -> 12 -> 13 -> 14 -> 15

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
| 10.1 Interactive Debug UI | v1.1 | 3/3 | Complete | 2026-04-10 |
| 10.2 Latency Debugger | v1.1 | 3/3 | Complete | 2026-04-10 |
| 11. Fire-and-Forget PUSH + Binary Wire | v1.2 | 6/6 | Complete | 2026-04-11 |
| 12. Server-side async push coalescing | v1.3 | 1/3 | In Progress|  |
| 13. SDK batch push + OP_PUSH_BATCH | v1.3 | 2/2 | Complete   | 2026-04-12 |
| 14. Key-partitioned multi-threaded engine | v1.3 | 3/3 | Complete   | 2026-04-12 |
| 15. Off-thread snapshot I/O | v1.3 | 0/? | Not started | - |

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

**Milestone Goal:** Break past the single-core ceiling hit in v1.2. Target **500k–1M eps on a single node** by (1) adding server-side async push coalescing to amortize per-event fixed costs, (2) adding an SDK batch-push API so a single client can produce events faster than the Python per-event loop ceiling, (3) adding key-partitioned multi-threading so the 47 idle cores can carry load, and (4) moving snapshot I/O off the main thread so large-state pipelines stop stalling during writes. Each phase is independently measurable against the v1.2 baseline in `benchmark/tally-throughput/RESULTS.md`.

**Why v1.3:** Phase 11 proved that on 1 core the server is bound at ~128–142k eps (66% CPU utilization; ~7µs per push of real work). With 47 other cores on a typical box sitting idle, multi-threading is the single highest-ROI change — estimated 10–40× on aggregate throughput with N concurrent clients. The other phases target residual per-event cost (coalescing + batching) and the 15–25% duty-cycle loss to snapshot stalls.

**Build order:** 12 → 13 → 14 → 15. Phase 12 establishes `handle_push_batch` as the shared primitive that Phase 13's wire format reuses verbatim and Phase 14's cross-shard workers reuse as their inbound dispatch handler. Phase 15 becomes a trivial parallel-split after Phase 14.

**Locked Decisions (v1.3) — see REQUIREMENTS.md LD-1..LD-4:**
- **LD-1:** Cross-shard fan-out errors are **fire-and-forget** — target-shard errors surface in per-shard metrics, NOT in the originating client's drain queue. Deliberate regression from v1.2 semantics, required to preserve shared-nothing hot path.
- **LD-2:** `num_shards` persisted in snapshot manifest + config file. Changing across restarts requires `TALLY_ALLOW_RESHARD=1` + one-time re-route migration on load.
- **LD-3:** Snapshots are **shard-local consistent**, not globally consistent. Manifest guarantees per-shard files exist and hash-match, not that they reflect the same logical moment.
- **LD-4:** Shard routing uses `xxh3_64` with a fixed seed (not ahash). Hash-version byte included in manifest header.

**Context documents:**
- `benchmark/tally-throughput/RESULTS.md` — v1.2 final matrix (baseline for v1.3 regression checks)
- `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` — post-verification perf analysis (bottleneck breakdown)
- `.planning/research/SUMMARY.md` — v1.3 research consolidated index
- `.planning/research/STACK.md` — per-crate evaluations (parking_lot, crossbeam-channel ≥0.5.15, crossbeam-utils, core_affinity, xxhash-rust)
- `.planning/research/ARCHITECTURE.md` — integration seams per phase, cross-shard fan-out mechanism, snapshot v7 manifest protocol
- `.planning/research/PITFALLS.md` — 24 pitfalls (7 CRITICAL), Phase 11-class bench-matrix meta-lesson
- `.planning/research/FEATURES.md` — table stakes vs differentiators, competitor matrix, concrete API shapes
- CLAUDE.md — "Scaling Path" section explicitly calls out key-partitioned multi-threading as the v2 upgrade

### Phase 12: Server-side async push coalescing
**Goal:** Buffer incoming `OP_PUSH_ASYNC` frames per-connection, process them in batches under a single `state.lock()` acquisition. Amortizes fixed per-event costs (lock acquisition, event log append, fan-out target iteration, dirty-mark set insert). Establishes `handle_push_batch` as the shared primitive reused by Phase 13 (wire format) and Phase 14 (cross-shard dispatch).
**Depends on:** v1.2 complete
**Requirements:** PERF-03 (async coalescing)
**Stack additions:** None. Uses existing `tokio::time::Instant` + explicit `sleep_until(deadline)` inside `select!` — **no `tokio::time::sleep(200µs)` which hits the 1ms wheel floor.** Deadline-armed `select!` with `biased;` branch on the read.
**Success Criteria** (what must be TRUE):
  1. Server read loop accumulates up to N async push frames (default N=64) or waits up to T microseconds (default T=200µs) per connection before flushing to a single batch handler via `select! { biased; read | sleep_until(deadline) if !empty }`
  2. `handle_push_batch` takes a single state lock, groups events by primary stream, and issues one `engine.push_batch_no_features` call + one `event_log.append_many` + one `store.mark_dirty_many` per stream group (stream lookups — `key_field`, cascade targets, `fan_out_targets` — happen once per group, not once per event)
  3. **Sync PUSH bypasses the coalescer entirely** (pitfall H-2): any non-`OP_PUSH_ASYNC` opcode arriving on a connection force-flushes the accumulator before dispatch, and sync PUSH p99 on medium pipeline is **within ±5% of v1.2 baseline (87µs)**. Mixed sync+async workload test asserts sync p99 unchanged.
  4. Error attribution preserves the existing drain semantic: errors from events inside a batch surface on the client's next `push`/`flush`/`get` call, in **per-connection seq order** (monotonic `seq: u64` attached pre-dispatch, drain streams sorted by seq — pitfall C-2)
  5. Accumulator is **connection-local stack-allocated** (never on `AppState`) — no new shared state, no lock contention introduced by coalescing itself
  6. `std::MutexGuard` never held across `.await` inside `handle_connection` (pitfall C-7); batch critical section stays strictly synchronous
  7. Multi-client aggregate async throughput on the medium pipeline ≥ **200k eps with 4 clients** (v1.2 baseline for 4 clients was ~30k due to per-event lock contention)
  8. Single-client async throughput on medium pipeline is **within ±5% of the v1.2 baseline (142k)** — coalescing must not regress single-client latency
  9. Bench gate covers **small / medium / large × sync / async** matrix (Phase 11 lesson — pitfall "Phase 11 class"); each run is a 5-run median with σ < 10%
  10. Latency impact documented: coalescing adds up to T µs to async p50 (acceptable, async is already fire-and-forget)
  11. All 532 existing tests remain green
**Plans:** 1/3 plans executed
Plans:
- [x] 12-01-PLAN.md — Add batch primitives (append_many, mark_dirty_many, push_batch_no_features) + unit tests
- [ ] 12-02-PLAN.md — ConnAccumulator + handle_push_batch + select!/sleep_until deadline loop + sync force-flush + seq-ordered drain + await_holding_lock gate
- [ ] 12-03-PLAN.md — Bench matrix + mixed-mode harness + PERF-03 gate run + RESULTS.md + human verification

### Phase 13: SDK batch push API + OP_PUSH_BATCH opcode
**Goal:** Expose a client-side batching API (`app.push_many(stream, events)`) that wraps N events into a single wire frame, reducing Python per-event loop overhead from ~7µs to ~0.3µs. Target **single-client async ≥ 300k eps** on medium pipeline when using `push_many`. Server-side handler is Phase 12's `handle_push_batch` verbatim — zero new hot-path logic.
**Depends on:** Phase 12
**Requirements:** PERF-04 (client batch API)
**Stack additions:** None. Hand-rolled wire encoding for consistency with `OP_PUSH_ASYNC`; pure-Python SDK side (no C extension — pitfall M-5).
**Success Criteria** (what must be TRUE):
  1. `app.push_many(stream_cls, events)` accepts an iterable of event dicts, encodes them into one binary frame via existing `encode_push_binary_payload` (zero new serialization code), sends via a new `OP_PUSH_BATCH` (0x0A) opcode. Wire format: `[u16 stream_len][stream][u32 batch_id][u32 count][for each: [u32 event_len][event_bytes]]`
  2. Server decodes the batch into a pre-sized `Vec<DecodedEvent>` and dispatches to `handle_push_batch` (from Phase 12) for the grouped events — zero new hot-path logic
  3. **Batch size is hard-capped at 16,384 events per frame** (pitfall H-7 — OOM attack); frames claiming more are rejected with `STATUS_ERROR "batch too large"` and the connection is closed. Raw-TCP test asserts clean reject with no OOM and no crash on `count=10B`.
  4. Backward-compatible: `app.push()` single-event API continues to work unchanged, still emits `OP_PUSH_ASYNC` (0x07). Both opcodes coexist indefinitely.
  5. Single-client async throughput on medium pipeline via `push_many` ≥ **300k eps** (2× v1.2 single-client baseline)
  6. Error semantic: batch-level failures surface via `drain_errors_nonblock` with a payload indicating `(batch_id, event_index)` — reuses the per-connection seq ordering from Phase 12 criterion 4
  7. Python SDK: `bench.py --mode async-batch` flag exercises the new API; results recorded in `RESULTS.md` across small / medium / large pipeline sizes (Phase 11-class bench matrix)
  8. Decode path benchmarked in isolation before wiring into the server (pitfall H-6)
  9. All 532 existing tests remain green; new batch tests cover encode/decode roundtrip, mixed-valid/invalid event handling, partial batch errors, oversized-frame reject
**Plans:** 2/2 plans complete
Plans:
- [x] 13-01-PLAN.md — OP_PUSH_BATCH server-side decode + dispatch + tests + decode micro-bench
- [x] 13-02-PLAN.md — Python SDK push_many + encode_push_batch + bench.py async-batch mode + matrix run

### Phase 14: Per-stream locks + DashMap concurrency (incremental concurrency)
**Goal:** Replace the global `Mutex<AppState>` with per-stream locks + DashMap entity-level concurrency. Each stream gets its own `DashMap<EntityKey, StreamEntityState>` for concurrent reads/writes to different keys. PipelineEngine behind `parking_lot::RwLock`. Background systems (snapshot, eviction, event log) adapted for DashMap iteration. Multi-client throughput improvement, no single-client regression.
**Depends on:** Phase 12, Phase 13
**Requirements:** PERF-05 (per-stream + entity-level concurrency — incremental step toward full key-partitioned sharding)
**Stack additions (2 crates):**
  - `dashmap = "6.1"` — per-stream `DashMap<EntityKey, StreamEntityState>` for entity-level concurrency
  - `parking_lot = "0.12"` — `RwLock` for PipelineEngine, `Mutex` for per-field state locks; no poisoning (C-5 defense)
**Success Criteria** (what must be TRUE):
  1. Global `Mutex<AppState>` eliminated — `ConcurrentAppState` uses individually-locked fields
  2. Per-stream `DashMap<EntityKey, StreamEntityState>` for entity-level concurrency within each stream (D-03)
  3. `PipelineEngine` behind `parking_lot::RwLock` — concurrent reads on hot path, write only on REGISTER (D-04)
  4. Snapshot serialization iterates per-stream DashMaps correctly (D-09)
  5. Per-stream eviction via `DashMap::retain()` — no global lock during eviction
  6. Multi-client throughput (4 clients, async, medium) exceeds Phase 12 baseline (28k eps)
  7. Single-client throughput within -10% of Phase 12 baseline (~142k eps)
  8. 5+ concurrency integration tests pass under multi-threaded tokio runtime
  9. All 505+ existing tests remain green
  10. `#![deny(clippy::await_holding_lock)]` C-7 gate preserved
**Plans:** 3/3 plans complete
Plans:
- [x] 14-01-PLAN.md — Core refactor: ConcurrentAppState, StreamStore with DashMap, refactor all state.lock() call sites
- [x] 14-02-PLAN.md — Background systems: snapshot/eviction/event-log DashMap adaptation + concurrency integration tests
- [x] 14-03-PLAN.md — Benchmark gate: multi-client throughput, single-client regression check, results documentation

### Phase 15: Snapshot I/O off main thread
**Goal:** Move snapshot writes off the main event-loop thread (per shard) so large-state pipelines don't stall during full-snapshot windows. After Phase 14 this becomes a trivial parallel-split: each shard clones its own dirty subset under its own lock, releases, then `spawn_blocking` writes its own shard file — all N shards in parallel.
**Depends on:** Phase 14 (per-shard state makes per-shard parallel snapshot straightforward)
**Requirements:** OPS-05 (non-blocking snapshot write)
**Stack additions:** None. Uses existing `tokio::task::spawn_blocking` with `max_blocking_threads(2)`.
**Success Criteria** (what must be TRUE):
  1. `SnapshotCoordinator` broadcasts `CoordMsg::PrepareSnapshot { seq, full }` to every shard worker via their existing inboxes; each shard acquires its own lock, clones dirty (or full), releases lock, `spawn_blocking` serializes + writes + fsyncs its own shard file, replies via `oneshot`
  2. Per-shard stall is ~1/N of v1.2's stall; PUSH throughput on other shards is unaffected while one shard is cloning
  3. **Never start a new snapshot cycle while the previous one is still writing** (pitfall H-4 — dirty-set backpressure): snapshot cycle is serialized on itself. Metric for skipped cycles; `/metrics` alerts if > 0. Test: sustained 500k eps with snapshot interval shorter than write time — asserts no cycle overlap, asserts dirty set does not grow unbounded.
  4. During a snapshot write, async PUSH throughput on the main path regresses by **≤ 5%** (was 15–25% on v1.2) — measured by running a sustained-load bench that DELIBERATELY OVERLAPS a snapshot cycle (Phase 11 lesson — snapshot regression is only meaningful if the bench runs during the write, not between)
  5. Snapshot write completes within the existing budget (**< 1 second per 100k entities**) aggregated across shards
  6. Manifest commit protocol: coordinator waits for all shard replies → `manifest.{seq}.tmp` → fsync → rename → fsync parent dir → cleanup old shard files whose seq < previous manifest. Strict fsync ordering (pitfall H-5): all shard writes → all shard fsyncs → parent dir fsync → manifest.tmp → fsync → rename → dir fsync.
  7. Crash recovery from a partially-written snapshot set: missing manifest for seq N with shard files for N present → incomplete cycle → roll back to previous manifest (LD-3, pitfall C-3)
  8. Incremental dirty set is per-shard (never global — pitfall C-3); integrates with off-thread write
  9. `POST /snapshot?wait=true&timeout_ms=N` (differentiator D3) added as a trivial extension — mirrors Redis `SAVE` vs `BGSAVE`; returns 200 with `{bytes, duration_ms}` on success, 408 on timeout
  10. `cleanup_old_snapshots` extended to glob `tally.snapshot.*.shard-*` patterns and match on cycle seq; unit tested
  11. All 532 existing tests + new "push during snapshot" stress tests remain green
**Plans:** TBD
