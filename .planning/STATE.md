---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: New API & Engine
status: Defining requirements
stopped_at: null
last_updated: "2026-04-12"
last_activity: 2026-04-12 — Milestone v2.0 started
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-12)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** v2.0 New API & Engine — replace @st.stream with function-based @tl.dataset pattern, fill engine gaps, remove old API, architect for on-demand compute.

## Current Position

Milestone: v2.0 New API & Engine
Phase: Not started (defining requirements)
Plan: —
Status: Defining requirements
Last activity: 2026-04-12 — Milestone v2.0 started

## Performance Metrics

**Velocity:**

- Total plans completed: 37 (v1.0) + 23 (v1.1) + 6 (v1.2) + 8 (v1.3/v1.4)
- Total phases completed: 15 integers + 2 decimals through v1.3

## Accumulated Context

### Decisions

All v1.0–v1.2 decisions archived in PROJECT.md Key Decisions table.

**v1.3 Locked Decisions (executed):**

- **LD-1** Cross-shard fan-out errors are fire-and-forget (per-shard metrics, NOT origin drain queue).
- **LD-2** `num_shards` persisted in manifest + config; changing requires `TALLY_ALLOW_RESHARD=1`.
- **LD-3** Snapshots are shard-local consistent (per-shard hash-match, not same logical moment).
- **LD-4** Shard routing uses `xxh3_64` with fixed seed (not ahash).

**v2.0 API Design Decisions:**

- Function-based `@tl.dataset(depends_on=[...])` replaces `@st.stream` decorator
- `EventSet` (input stream) / `FeatureSet` (computed features grouped by key) are the honest types
- `.group_by("key").agg(...)` makes aggregation explicit
- DataFrame simulation rejected — users expect Pandas behavior
- Old API removed, not deprecated alongside
- REGISTER stays a runtime operation (enables on-demand compute post-launch)
- On-demand compute: architect for it, don't build the product layer yet

### Roadmap Evolution

- **v1.0** (Phases 1-5) shipped 2026-04-09
- **v1.1** (Phases 6-10.2) shipped 2026-04-11
- **v1.2** (Phase 11) shipped 2026-04-11
- **v1.3** (Phases 12-14) partially shipped 2026-04-12 — PERF-04 (batch API) + PERF-05 (DashMap concurrency) complete; PERF-03 (async coalescing) and OPS-05 (off-thread snapshot) deferred
- **v2.0** started 2026-04-12

### Pending Todos

- None — fresh milestone

### Blockers/Concerns

- None

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260409-f8y | Generate AI image generation prompts for Tally logo/mascot | 2026-04-09 | ed7363e | [260409-f8y-generate-a-prompt-to-generate-logo-for-t](./quick/260409-f8y-generate-a-prompt-to-generate-logo-for-t/) |

## Session Continuity

Last session: 2026-04-12
Stopped at: Milestone v2.0 initialization
Resume: Define requirements, then `/gsd-plan-phase [N]`
