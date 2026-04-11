---
gsd_state_version: 1.0
milestone: v1.2
milestone_name: Performance
status: discussing
stopped_at: Phase 11 CONTEXT.md written — fire-and-forget PUSH + binary wire protocol. Target ≥100k events/sec single client. Ready for research phase.
last_updated: "2026-04-11T02:12:29.440Z"
last_activity: 2026-04-11
progress:
  total_phases: 7
  completed_phases: 7
  total_plans: 43
  completed_plans: 43
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 10.1 — Interactive Debug UI Redesign

## Current Position

Phase: 11 Fire-and-Forget PUSH + Binary Wire Protocol
Plan: CONTEXT written, research pending
Status: Discussing — ready for /gsd-plan-phase 11
Last activity: 2026-04-11

Progress: v1.2 milestone kickoff. Phase 11 CONTEXT.md captures 6 decisions (async+sync API split, OP_PUSH_ASYNC + OP_FLUSH opcodes, binary event payload for PUSH, non-blocking error drain, OP_FLUSH as no-op barrier, folded Phase 10.2 histogram). Target ≥100k eps single client (5.7x v1.1 baseline of 17.5k).

## Performance Metrics

**Velocity:**

- Total plans completed: 37 (v1.0)
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
| Phase 10 P05 | 8min | 3 tasks | 6 files |
| Phase 10.1 P01 | 6min | 2 tasks | 2 files |
| Phase 10.1 P02 | 5min | 3 tasks | 3 files |
| Phase 10.1 P03 | ~25min | 2 tasks | 1 files |

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
- [Phase 10]: [Phase 10]: Plan 10-05: raw TCP HTTP/1.1 over tokio::net::TcpStream for integration tests (no reqwest); random 127.0.0.1:0 ports per test; SHA256 drift tests re-hash embedded vendored bytes at test time against VENDOR.md manifest; sha2=0.10 added to dev-dependencies only
- [Phase 10.1]: [Phase 10.1]: Plan 10.1-01: /debug/topology operators field uses raw_register_jsons pass-through (RESEARCH Pattern 8) instead of walking FeatureDef enum — avoids AST-to-string conversion for parsed Expr where-clauses. Field rename type -> op at projection boundary for frontend readability. Empty-array fallback for snapshot-restored streams (Pitfall 7). No new .await inside handler lock scope.
- [Phase 10.1]: Split-view shell rewrite: minmax(0, 1fr) 360px CSS Grid with overflow:hidden + min-height:0 escape-hatch, always-visible drill-in panel with data-empty attribute, Phase 10 design tokens preserved verbatim
- [Phase 10.1]: Static HTML shell owns zero htmx attributes; Plan 03's app.js will use vanilla fetch + setInterval for polling — decouples shell contract from behavior layer
- [Phase 10.1]: Grep-based shell regression tests (forbidden + required substring pairs) as enforcement layer for wholesale HTML/CSS rewrites
- [Phase 10.1]: [Phase 10.1]: Plan 10.1-03: app.js wholesale rewrite (936 lines) for interactive Debug UI — render-once dagre-d3 + d3-text-in-place edge labels (Pattern 6), shared state.paused gate for both polling loops, stream-scoped entity lookup with 7 sub-states, el()/svgEl() textContent chokepoint for XSS safety (app_js_has_no_innerhtml_or_eval_sinks regression green)

### Roadmap Evolution

- **2026-04-10 — Phase 10.1 Interactive Debug UI Redesign inserted after Phase 10** (URGENT). Makes topology DAG the primary Debug UI entry point with clickable nodes that drill into per-stream memory + state + entity lookup, and edges carrying live throughput numbers. Replaces flat 4-tab layout from Phase 10. Source: user request during Phase 10 Plan 10-04 smoke test. Routing confirmed via autonomous Option A.
- **2026-04-10 — Phase 10.2 Latency Debugger inserted after Phase 10.1** (URGENT). Percentile latency tracker per TCP command (PUSH/GET/SET/MSET) with per-stream breakdown, new `/debug/latency` JSON endpoint on HTTP management port, latency visualization surface TBD in discuss (determined by Phase 10.1's interactive layout). Histogram estimator choice (t-digest vs HDR vs bucketed) requires explicit discuss-phase decision. Full cycle: discuss (required) → research → plan → execute. Source: user request mid Phase 10 Wave 1. **Ordering:** 10.2 runs after 10.1 per user decision so latency UI fits into the new interactive drill-in paradigm instead of being built against the flat 4-tab layout and discarded.

### Pending Todos

- Phase 10.1 (Interactive UI Redesign) planning pending (discuss → research → plan → execute cycle).
- Phase 10.2 (Latency Debugger) planning pending, blocked on Phase 10.1 completion.

### Blockers/Concerns

- Phase 8: Backfill + live traffic boundary semantics need explicit design (live PUSH during mid-backfill)
- Phase 9: Incremental snapshot recovery edge cases need test case design before implementation

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260409-f8y | Generate AI image generation prompts for Tally logo/mascot | 2026-04-09 | ed7363e | [260409-f8y-generate-a-prompt-to-generate-logo-for-t](./quick/260409-f8y-generate-a-prompt-to-generate-logo-for-t/) |

## Session Continuity

Last session: 2026-04-10T18:22:31.954Z
Stopped at: Completed 10.1-03-frontend-behavior-PLAN.md (Task 2 auto-approved; 31-step browser smoke test pending human verification)
Resume: `/gsd-plan-phase 6`
