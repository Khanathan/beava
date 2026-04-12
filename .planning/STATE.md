---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: New API & Engine
status: executing
stopped_at: Completed 17-02-PLAN.md
last_updated: "2026-04-12T22:56:26.704Z"
last_activity: 2026-04-12
progress:
  total_phases: 16
  completed_phases: 1
  total_plans: 5
  completed_plans: 4
  percent: 80
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-12)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 17 — Enriched Event Propagation

## Current Position

Milestone: v2.0 New API & Engine
Phase: 17 (Enriched Event Propagation) — EXECUTING
Plan: 3 of 3
Status: Ready to execute
Last activity: 2026-04-12

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**

- Total plans completed: 39 (v1.0) + 23 (v1.1) + 6 (v1.2) + 8 (v1.3/v1.4)
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
- [Phase 16]: Used __init_subclass__ (not metaclass) for EventSet/FeatureSet schema types
- [Phase 16]: SourceDef/DatasetDef are plain objects returned by decorators, not modified classes
- [Phase 16]: Kahn's algorithm for cycle detection in validate() -- O(V+E), pure Python, no server
- [Phase 17]: Enrichment param uses Option<&AHashMap> side-channel: serde_json::Value for operators, FeatureValue for EvalContext
- [Phase 17]: Dual enrichment maps: enrichment_json (serde_json::Value) for operators, enrichment_fv (FeatureValue) for EvalContext; no-cascade fast path skips allocation

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

Last session: 2026-04-12T22:56:26.702Z
Stopped at: Completed 17-02-PLAN.md
Resume: `/gsd-plan-phase 16`
