---
gsd_state_version: 1.0
milestone: v1.1
milestone_name: Composable Pipeline & Event Log
status: Roadmap refined with research findings and locked decisions LD-1..LD-4; ready for plan-phase
stopped_at: Completed 12-02-PLAN.md — per-connection coalescer wired
last_updated: "2026-04-11T23:30:00.000Z"
last_activity: 2026-04-11 — Phase 12 Wave 2 coalescer landed (ConnAccumulator + select! loop + single-lock handle_push_batch)
progress:
  total_phases: 7
  completed_phases: 7
  total_plans: 43
  completed_plans: 43
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-11)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** v1.3 Concurrency & Client Batching — break past the single-core ceiling (500k–1M eps target) via async coalescing, client batch API, key-partitioned multi-threading, and off-main-thread snapshot I/O.

## Current Position

Milestone: v1.3 Concurrency & Client Batching — ROADMAP DRAFTED 2026-04-11
Phase: 12 (not yet started — awaiting /gsd-plan-phase 12)
Plan: —
Status: Roadmap refined with research findings and locked decisions LD-1..LD-4; ready for plan-phase
Last activity: 2026-04-11 — gsd-roadmapper refined phases 12-15 in place

**v1.3 phase summary:**

- Phase 12: Server-side async push coalescing (PERF-03) — no new crates, `sleep_until` deadline pattern, sync PUSH bypass, Phase-11-class matrix bench
- Phase 13: SDK batch push + OP_PUSH_BATCH 0x0A (PERF-04) — 16,384 hard cap, `(batch_id, event_index)` drain errors, pure-Python SDK
- Phase 14: Key-partitioned multi-threaded engine (PERF-05) — 5 new crates (parking_lot, crossbeam-channel ≥0.5.15, crossbeam-utils, core_affinity gated, xxhash-rust), LD-1..LD-4 lock-ins, 1-day runtime spike pre-plan
- Phase 15: Off-thread snapshot I/O per shard (OPS-05) — no new crates, manifest commit boundary, snapshot-cycle serialization, bench DURING write

Prior milestone summary (v1.2 Performance — SHIPPED 2026-04-11):

  - Phase 11 delivered fire-and-forget async push + binary wire protocol + binary event log format
  - Final throughput (1 core, 1 client, 3-run mean): small 138k / medium 142k / large 128k eps async; sync p99 87–90µs across sizes
  - 100k floor achieved on every pipeline size; the 1M ceiling deferred to v1.3 multi-threading work
  - Large pipeline went from 865 eps to 128k eps (148×) after post-verification DistinctCountOp::read fix

## Performance Metrics

**Velocity:**

- Total plans completed: 37 (v1.0) + v1.1 + v1.2 Phase 11
- Total phases completed: 11 integers + 2 decimals through v1.2

**By Phase (v1.0):**

| Phase | Plans | Duration | Tasks | Files |
|-------|-------|----------|-------|-------|
| 01 Core Engine | 4 | ~17min | 8 | 20 |
| 02 TCP Server | 5 | ~14min | 9 | 18 |
| 03 Python SDK | 4 | ~16min | 7 | 23 |
| 04 Persistence | 3 | ~12min | 6 | 13 |
| 05 Advanced Ops | 3 | ~22min | 6 | 19 |
| Phase 06 P01 | 33min | 2 tasks | 6 files |
| Phase 06 P02 | 9min | 2 tasks | 7 files |
| Phase 06 P03 | 5min | 2 tasks | 6 files |
| Phase 06 P04 | 3min | 2 tasks | 7 files |
| Phase 07 P01 | 10min | 2 tasks | 10 files |
| Phase 07 P02 | 2min | 2 tasks | 2 files |
| Phase 07 P03 | 3min | 2 tasks | 2 files |
| Phase 07 P04 | 3min | 2 tasks | 3 files |
| Phase 08 P01 | 11min | 2 tasks | 9 files |
| Phase 08 P02 | 11min | 2 tasks | 6 files |
| Phase 09 P01 | 6min | 2 tasks | 2 files |
| Phase 09 P02 | 110m | 2 tasks | 6 files |
| Phase 10 P02 | 10min | 2 tasks | 4 files |
| Phase 10 P03 | 3min | 2 tasks | 3 files |
| Phase 10 P05 | 8min | 3 tasks | 6 files |
| Phase 10.1 P01 | 6min | 2 tasks | 2 files |
| Phase 10.1 P02 | 5min | 3 tasks | 3 files |
| Phase 10.1 P03 | ~25min | 2 tasks | 1 files |
| Phase 12 P01 | ~25min | 3 tasks | 4 files |
| Phase 12 P02 | ~25min | 2 tasks | 2 files |

## Accumulated Context

### Decisions

All v1.0 decisions archived in PROJECT.md Key Decisions table.

**v1.3 Locked Decisions (adopted during research, approved for Phase 14 execution):**

- **LD-1** Cross-shard fan-out errors are fire-and-forget (per-shard metrics, NOT origin drain queue). Preserves shared-nothing hot path.
- **LD-2** `num_shards` persisted in manifest + config; changing requires `TALLY_ALLOW_RESHARD=1` + re-route migration.
- **LD-3** Snapshots are shard-local consistent (per-shard hash-match, not same logical moment). Sibling to "lose ~30s on crash".
- **LD-4** Shard routing uses `xxh3_64` with fixed seed (not ahash — not spec-stable across versions). Hash-version byte in manifest header.

Key v1.1 architectural decisions (from research):

- EntityState refactor (per-stream grouping) must precede all other v1.1 work
- Event log uses BufWriter + periodic fdatasync (never sync on hot path)
- petgraph for DAG construction/topological sort
- rust-embed for debug UI asset embedding (single binary preserved)
- Backfill rate-limited to 64 events per yield cycle
- [Phase 06]: Per-stream entity eviction uses most-recent last_event_at across all streams
- [Phase 06]: Borrow conflict in push() resolved via scoped borrows of entity.streams.get_mut()
- [Phase 06]: Per-stream eviction delegates from evict_expired_keys to evict_expired_stream_entries for backward compatibility
- [Phase 06]: MGET routed through sync command path (not chunked) since reads are fast and non-destructive
- [Phase 06]: MGET strips qualified Stream.feature names from response (T-06-03 mitigation)
- [Phase 06]: Borrow conflict in REGISTER handler resolved by extracting history_ttl before borrowing event_log mutably
- [Phase 06]: Event log uses Option<EventLog> in AppState for backward compatibility -- system works without event log
- [Phase 06]: encode_mget uses simple [u32 count][u16-string key]... format matching Rust MGET handler
- [Phase 06]: TTL fields conditionally omitted from RegisterRequest JSON when None for backward compatibility
- [Phase 06]: Views reject entity_ttl/history_ttl at StreamMeta.__new__ level for consistent validation
- [Phase 07]: key_field changed to Option<String> -- None = keyless stream, Some = keyed; keyless streams reject windowed operators
- [Phase 07]: Stream-level filter evaluated early in push() before key extraction -- filtered events skip all processing
- [Phase 07]: Keyless streams reject windowed operators at class creation time (fail-fast TypeError)
- [Phase 07]: depends_on stores class refs, resolves to string names only at JSON serialization
- [Phase 07]: DAG edges go upstream->downstream; toposort gives correct cascade order; cycle detection rolls back failed registration
- [Phase 07]: push_with_cascade replaces push in TCP handler; fan-out excludes cascade targets (T-07-09); cascade events logged to downstream logs (T-07-10)
- [Phase 08]: Schema diff uses std::mem::discriminant for type equality -- simple, correct, no false positives
- [Phase 08]: Lazy GC on snapshot (not on re-register) to avoid blocking the push hot path
- [Phase 08]: Both snapshot callers (main.rs periodic + http.rs trigger) wired to clone_for_snapshot_with_gc
- [Phase 08]: run_backfill clears operator state before replay for idempotent restart correctness
- [Phase 08]: Snapshot format bumped to v5 for backfill_complete with serde(default) backward compat
- [Phase 09]: Dirty set lives on StateStore (not AppState); mark_deleted removes key from dirty_keys for mutual exclusion
- [Phase 09]: Snapshot v6 uses [version][type_tag 0x00/0x01][postcard] header; legacy save/load_snapshot preserved with transparent v5 migration
- [Phase 09]: apply_delta processes deletes before inserts so delete+reinsert in same delta lands as insert
- [Phase 09]: Delta-rot skip: snapshot ticks with no dirty/deleted keys write no file but still advance cycle counter
- [Phase 09]: cleanup_old_snapshots runs only after successful base write so deltas are never deleted before their owning base exists
- [Phase 09]: Eviction restructured to two-phase (collect plan, then apply) to allow mark_deleted without borrow checker conflict
- [Phase 10]: ThroughputTracker uses lock-once instrumentation inside existing AppState mutex (RESEARCH Pattern 3 option A) — zero new contention on single-threaded core; bump_unique with HashSet dedup is the canonical Push-arm call site to prevent double-counting across primary/cascade/fan-out overlap
- [Phase 10]: /debug endpoints follow lock-once-then-build-JSON pattern (no .await across AppState mutex); /debug/memory extended additively; axum 0.8 brace-wildcard syntax for /static/{*file}; view nodes emit depends_on:[] and participate in DAG only via lookup edges
- [Phase 10]: raw TCP HTTP/1.1 over tokio::net::TcpStream for integration tests; random 127.0.0.1:0 ports per test; SHA256 drift tests re-hash embedded vendored bytes
- [Phase 10.1]: /debug/topology operators field uses raw_register_jsons pass-through; field rename type -> op at projection boundary; empty-array fallback for snapshot-restored streams
- [Phase 10.1]: Split-view shell rewrite: minmax(0, 1fr) 360px CSS Grid with overflow:hidden + min-height:0 escape-hatch, always-visible drill-in panel with data-empty attribute
- [Phase 10.1]: Static HTML shell owns zero htmx attributes; app.js uses vanilla fetch + setInterval for polling
- [Phase 10.1]: Grep-based shell regression tests (forbidden + required substring pairs) as enforcement layer for wholesale rewrites
- [Phase 10.1]: app.js wholesale rewrite for interactive Debug UI — render-once dagre-d3 + d3-text-in-place edge labels, shared state.paused gate, stream-scoped entity lookup with 7 sub-states, el()/svgEl() textContent chokepoint for XSS safety
- [Phase 12]: push_batch_with_cascade_no_features inlines fan-out filter logic at call entry (mirrors TCP handler src/server/tcp.rs:364-398) — Rule 2 deviation to make the load-bearing fan-out test pass; cascade-only delegation alone did not satisfy v1.2 parity
- [Phase 12 Wave 2]: per-connection ConnAccumulator (stack-local, N=64, 200µs deadline via absolute tokio::time::Instant + sleep_until) wired into handle_connection as a biased tokio::select! { read | sleep_until } loop; handle_push_batch takes one state.lock() per batch and routes cascade + fan-out via the Wave 1 push_batch_with_cascade_no_features primitive; handle_push_async removed entirely (batch path is the only async path); #![deny(clippy::await_holding_lock)] at src/server/tcp.rs top is the compile-time C-7 gate; per-connection pending_drain Vec<(u64, String)> sorted by seq flushes BEFORE every sync response (D-13), guaranteeing per-connection isolation and seq-ordered error attribution

### Roadmap Evolution

- **2026-04-10 — Phase 10.1 Interactive Debug UI Redesign inserted after Phase 10** (URGENT). Makes topology DAG the primary Debug UI entry point with clickable nodes that drill into per-stream memory + state + entity lookup, and edges carrying live throughput numbers. Source: user request during Phase 10 Plan 10-04 smoke test.
- **2026-04-10 — Phase 10.2 Latency Debugger inserted after Phase 10.1** (URGENT). Percentile latency tracker per TCP command with per-stream breakdown, `/debug/latency` JSON endpoint. Source: user request mid Phase 10 Wave 1.
- **2026-04-11 — v1.2 Performance milestone** (Phase 11 Fire-and-Forget PUSH + Binary Wire Protocol) shipped. 128–142k eps async single-client achieved.
- **2026-04-11 — v1.3 Concurrency & Client Batching milestone started.** Research phase complete (SUMMARY/STACK/ARCHITECTURE/PITFALLS/FEATURES). Requirements PERF-03/PERF-04/PERF-05/OPS-05 + locked decisions LD-1..LD-4 ratified. ROADMAP v1.3 section refined with research findings and pitfall coverage across phases 12-15.

### Pending Todos

- /gsd-plan-phase 12 — Server-side async push coalescing
- Phase 14 runtime-coordination spike (1 day) before plan decomposition
- Retroactively close v1.1 and v1.2 in MILESTONES.md (not blocking)

### Blockers/Concerns

- None. Roadmap approved-ready. Phase 14 carries highest risk (5-crate addition, runtime model change, 5 critical pitfalls clustered); 1-day spike will land before planning.

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260409-f8y | Generate AI image generation prompts for Tally logo/mascot | 2026-04-09 | ed7363e | [260409-f8y-generate-a-prompt-to-generate-logo-for-t](./quick/260409-f8y-generate-a-prompt-to-generate-logo-for-t/) |

## Session Continuity

Last session: 2026-04-11T23:30:00.000Z
Stopped at: Completed 12-02-PLAN.md — per-connection coalescer wired (ConnAccumulator + select! + handle_push_batch, 632 tests green)
Resume: `/gsd-execute-phase 12` next wave (12-03 bench matrix gate, if planned)
