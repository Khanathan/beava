---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: New API & Engine
status: Ready to plan
stopped_at: Phase 16 ready for planning
last_updated: "2026-04-12"
last_activity: 2026-04-12 — Roadmap created for v2.0 (Phases 16-19)
progress:
  total_phases: 4
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-12)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 16 -- Python SDK New Types and Decorators

## Current Position

Milestone: v2.0 New API & Engine
Phase: 16 of 19 (Python SDK -- New Types and Decorators)
Plan: 0 of ? in current phase
Status: Ready to plan
Last activity: 2026-04-12 — Roadmap created (4 phases, 13 requirements mapped)

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**

- Total plans completed: 37 (v1.0) + 23 (v1.1) + 6 (v1.2) + 8 (v1.3/v1.4)
- Total phases completed: 15 integers + 2 decimals through v1.3

## Accumulated Context

### Decisions

All v1.0-v1.3 decisions archived in PROJECT.md Key Decisions table.

**v2.0 Decisions:**

- Function-based `@tl.dataset(depends_on=[...])` replaces `@st.stream` decorator
- `EventSet`/`FeatureSet` are honest types (not DataFrame simulation)
- `.group_by("key").agg(...)` makes aggregation explicit
- Old API removed, not deprecated alongside (clean break before launch)
- REGISTER stays runtime operation (enables on-demand compute post-launch)
- Enriched propagation uses side-channel AHashMap (never clone serde_json::Value per hop)
- All new RegisterRequest fields use #[serde(default)] for backward compat

### Critical Pitfalls (from research)

- **C-1:** Enriched propagation allocation cliff -- side-channel, no event clone. Gate: <5% regression from 1.1M eps.
- **C-2:** Old API removal breaks 744 tests -- port ALL tests first, verify count >= 744, THEN delete.
- **C-3:** RegisterRequest backward compat -- all new fields #[serde(default)], snapshot round-trip test.
- **C-4:** Two APIs being replaced -- @st.stream AND _dataframe.py. Test migration covers both.
- **C-5:** Enrichment + DashMap concurrency -- enrichment values never re-enter DashMap during downstream push.

### Pending Todos

None.

### Blockers/Concerns

None.

## Session Continuity

Last session: 2026-04-12
Stopped at: Roadmap created for v2.0 milestone
Resume: `/gsd-plan-phase 16`
