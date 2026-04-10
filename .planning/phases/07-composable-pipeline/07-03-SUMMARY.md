---
phase: 07-composable-pipeline
plan: 03
subsystem: engine
tags: [rust, pipeline, dag, petgraph, cascade, cycle-detection, left-join]

requires:
  - phase: 07-composable-pipeline
    plan: 01
    provides: StreamDefinition with optional key_field, depends_on, filter

provides:
  - PipelineEngine with petgraph DAG (DiGraph, node_indices, topo_order, downstream_map)
  - rebuild_dag() with cycle detection via toposort
  - push_with_cascade() for topological cascade execution
  - LEFT JOIN semantics (missing key_field silently skips downstream)
  - get_topo_order() accessor for testing/debugging

affects: [07-04, composable-pipeline, tcp-handler]

tech-stack:
  added: []
  patterns: [dag-cascade, topological-push, left-join-semantics, cycle-detection-on-register]

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - tests/test_pipeline.rs

key-decisions:
  - "DAG edges go upstream -> downstream (data flow direction); toposort gives correct processing order"
  - "rebuild_dag() called on every register() and remove_stream(); cycle detection rolls back failed registration"
  - "push_with_cascade() uses BFS to find reachable downstream, then executes in topo order"
  - "LEFT JOIN: keyed downstream with missing key_field in event is silently skipped"
  - "Cascade returns primary stream features only; downstream side effects are fire-and-forget"
  - "Self-dependency detected as cycle by petgraph toposort (self-edge creates trivial cycle)"

patterns-established:
  - "DAG cascade pattern: BFS reachability + topo-order execution"
  - "Cycle rollback: insert stream, rebuild_dag, remove on error"

requirements-completed: [PIPE-03, PIPE-04, PIPE-05]

duration: 3min
completed: 2026-04-10
---

# Phase 7 Plan 3: DAG Engine and Cascade Execution Summary

**petgraph DAG construction with topological cascade, cycle detection at registration, and LEFT JOIN semantics for missing keys**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-10T01:47:10Z
- **Completed:** 2026-04-10T01:50:43Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- PipelineEngine extended with petgraph DiGraph for DAG construction from depends_on edges
- rebuild_dag() builds graph, runs toposort for cycle detection, caches topo_order and downstream_map
- push_with_cascade() cascades events through downstream streams in topological order
- Circular dependencies (including self-dependency) detected and rejected at registration time
- LEFT JOIN semantics: downstream streams with missing key_field in event are silently skipped
- Multi-level cascade (A->B->C) processes all levels in correct topological order
- Stream-level filter on downstream respected during cascade (filtered events skip that stream)
- All 457 tests pass (403 lib + 19 pipeline + 28 server + 7 snapshot)

## Task Commits

Each task was committed atomically:

1. **Task 1: Write failing tests for DAG cascade, cycle detection, LEFT JOIN** - `030a815` (test)
2. **Task 2: Implement petgraph DAG, cascade execution, and cycle detection** - `af260a9` (feat)

## Files Created/Modified
- `src/engine/pipeline.rs` - Added petgraph imports, DAG fields (dag, node_indices, topo_order, downstream_map), rebuild_dag(), push_with_cascade(), get_topo_order(); updated register() with cycle detection rollback; updated remove_stream() to rebuild DAG
- `tests/test_pipeline.rs` - Added 10 new tests: cascade keyless-to-keyed, multi-level cascade, LEFT JOIN skip, cycle detection, self-dependency, filter on downstream, keyed-to-keyed cascade, multiple sources, DAG no deps, topo order verification

## Decisions Made
- DAG edges represent data flow (upstream -> downstream); toposort yields correct processing order
- rebuild_dag() called on every register/remove to keep DAG consistent; failed registration is rolled back
- push_with_cascade uses BFS for reachability then topo-order for execution -- correct and efficient
- LEFT JOIN semantics: missing key = silent skip, not error -- matches SQL LEFT JOIN behavior
- Self-dependency is a trivial cycle caught by petgraph toposort

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- DAG cascade engine ready for TCP handler integration (Plan 04 will wire push_with_cascade into PUSH command)
- All PIPE-03, PIPE-04, PIPE-05 requirements satisfied
- Backward compatible: existing push() still works for non-cascade use cases

## Self-Check: PASSED

- src/engine/pipeline.rs contains `use petgraph::graph::{DiGraph, NodeIndex}` - VERIFIED
- src/engine/pipeline.rs contains `fn rebuild_dag` - VERIFIED
- src/engine/pipeline.rs contains `fn push_with_cascade` - VERIFIED
- src/engine/pipeline.rs contains `fn get_topo_order` - VERIFIED
- src/engine/pipeline.rs contains `circular dependency detected` - VERIFIED
- Task 1 commit 030a815 verified
- Task 2 commit af260a9 verified
- 457 tests pass (0 failures)

---
*Phase: 07-composable-pipeline*
*Completed: 2026-04-10*
