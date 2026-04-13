---
gsd_state_version: 1.0
milestone: v2.0
milestone_name: New API & Engine
status: executing
stopped_at: Completed 19-04-PLAN.md
last_updated: "2026-04-13T00:30:04.343Z"
last_activity: 2026-04-13
progress:
  total_phases: 16
  completed_phases: 14
  total_plans: 48
  completed_plans: 48
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-12)

**Core value:** Events go in, features come out -- synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 19 — Test Migration and Old API Removal

## Current Position

Milestone: v2.0 New API & Engine
Phase: 19 (Test Migration and Old API Removal) — EXECUTING
Plan: 5 of 5
Status: Ready to execute
Last activity: 2026-04-13

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**

- Total plans completed: 44 (v1.0) + 23 (v1.1) + 6 (v1.2) + 8 (v1.3/v1.4)
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
- [Phase 17]: Derive values not assertable via get_features -- verify via downstream aggregated values instead
- [Phase 17]: Concurrent enrichment test uses TCP wire protocol for real DashMap concurrency path
- [Phase 18]: Projection applied after derives but before views -- derives can reference any feature regardless of projection
- [Phase 18]: Ephemeral fields are schema-only -- stored on StreamDefinition but no runtime enforcement yet
- [Phase 18]: select()/drop() immutable builder pattern on DatasetDef; function-scoped server for projection E2E isolation
- [Phase 19]: Push to keyless @source returns empty features; tests verify downstream via GET
- [Phase 19]: Added filter parameter to @dataset decorator (was missing, blocking test migration)
- [Phase 19]: 57 new tests (15 source + 42 dataset) replace test_stream.py + test_view.py with expanded v2.0 API coverage
- [Phase 19]: 28 behavioral tests ported from test_dataframe.py; test_expr.py/test_new_api.py cleaned of old API refs
- [Phase 19]: Deleted test_expr.py (11 tests) with _expr.py since expression node classes only existed there

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

Last session: 2026-04-13T00:30:04.340Z
Stopped at: Completed 19-04-PLAN.md
Resume: `/gsd-plan-phase 16`
