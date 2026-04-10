---
gsd_state_version: 1.0
milestone: v1.1
milestone_name: Composable Pipeline & Event Log
status: executing
stopped_at: Completed 10-03-PLAN.md (debug endpoints + embedded UI routes)
last_updated: "2026-04-10T12:53:46.096Z"
last_activity: 2026-04-10
progress:
  total_phases: 5
  completed_phases: 4
  total_plans: 17
  completed_plans: 15
  percent: 88
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 10 — Debug UI

## Current Position

Phase: 10 (Debug UI) — EXECUTING
Plan: 4 of 5
Status: Ready to execute
Last activity: 2026-04-10

Progress: [..........] 0%

## Performance Metrics

**Velocity:**

- Total plans completed: 31 (v1.0)
- Total phases completed: 5 (v1.0)
- Total tasks completed: 36 (v1.0)

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

## Accumulated Context

### Decisions

All v1.0 decisions archived in PROJECT.md Key Decisions table.

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
- [Phase 09]: [Phase 09]: Dirty set lives on StateStore (not AppState); mark_deleted removes key from dirty_keys for mutual exclusion
- [Phase 09]: [Phase 09]: Snapshot v6 uses [version][type_tag 0x00/0x01][postcard] header; legacy save/load_snapshot preserved with transparent v5 migration
- [Phase 09]: [Phase 09]: apply_delta processes deletes before inserts so delete+reinsert in same delta lands as insert
- [Phase 09]: Delta-rot skip: snapshot ticks with no dirty/deleted keys write no file but still advance cycle counter
- [Phase 09]: cleanup_old_snapshots runs only after successful base write so deltas are never deleted before their owning base exists
- [Phase 09]: Eviction restructured to two-phase (collect plan, then apply) to allow mark_deleted without borrow checker conflict
- [Phase 10]: [Phase 10]: ThroughputTracker uses lock-once instrumentation inside existing AppState mutex (RESEARCH Pattern 3 option A) — zero new contention on single-threaded core; bump_unique with HashSet dedup is the canonical Push-arm call site to prevent double-counting across primary/cascade/fan-out overlap (RESEARCH Pitfall 4)
- [Phase 10]: Plan 10-03: /debug endpoints follow lock-once-then-build-JSON pattern (no .await across AppState mutex); /debug/memory extended additively (original 3 fields preserved + per_stream array); axum 0.8 brace-wildcard syntax for /static/{*file}; view nodes emit depends_on:[] and participate in DAG only via lookup edges; edge kind discriminator (cascade vs lookup) gives frontend a stable style hook

### Pending Todos

- **Phase 10.1 Latency Debugger (scope addition, 2026-04-10)** — After Phase 10 verification passes and BEFORE the v1.1 milestone lifecycle (audit → complete → cleanup), insert decimal phase 10.1 for a latency debugger. Scope sketch: percentile tracker (t-digest vs HDR vs bucketed — real research decision) per TCP command (PUSH/GET/SET/MSET) with per-stream breakdown, new `/debug/latency` JSON endpoint on port 6401, fifth tab in the Debug UI with p50/p95/p99 histograms + slow-query view, Nyquist tests via raw TCP matching `tests/test_debug_ui.rs`. Invoke via `gsd-insert-phase 10.1` or `gsd-add-phase`; run full discuss → research → plan → execute cycle (do NOT skip discuss — histogram-estimator choice needs explicit decision). Source: user request mid Phase 10 Wave 1.

### Blockers/Concerns

- Phase 8: Backfill + live traffic boundary semantics need explicit design (live PUSH during mid-backfill)
- Phase 9: Incremental snapshot recovery edge cases need test case design before implementation

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260409-f8y | Generate AI image generation prompts for Tally logo/mascot | 2026-04-09 | ed7363e | [260409-f8y-generate-a-prompt-to-generate-logo-for-t](./quick/260409-f8y-generate-a-prompt-to-generate-logo-for-t/) |

## Session Continuity

Last session: 2026-04-10T12:53:46.094Z
Stopped at: Completed 10-03-PLAN.md (debug endpoints + embedded UI routes)
Resume: `/gsd-plan-phase 6`
