---
phase: 01-core-engine
plan: 02
subsystem: engine
tags: [rust, operators, ring-buffer, windowed-aggregation, count, sum, avg]

# Dependency graph
requires:
  - phase: 01-core-engine plan 01
    provides: RingBuffer<T> with add_to_current, sum_all, count_nonzero, advance_to
provides:
  - Operator trait (push/read interface for all streaming operators)
  - CountOp (windowed event counter using RingBuffer<u64>)
  - SumOp (windowed numeric field accumulator using RingBuffer<f64>)
  - AvgOp (windowed average using paired count+sum ring buffers)
  - Redis-strict type checking pattern (TallyError::Type on non-numeric fields)
  - optional flag pattern (absent field -> silent skip vs error)
affects: [01-core-engine plan 03, 01-core-engine plan 04, phase 02, phase 05]

# Tech tracking
tech-stack:
  added: [serde_json (event field extraction)]
  patterns: [Operator trait with &mut read for lazy expiration, Redis-strict type enforcement, optional field handling]

key-files:
  created: [src/engine/operators.rs]
  modified: [src/engine/mod.rs]

key-decisions:
  - "read(&mut self, now) calls advance_to(now) internally for accurate window expiration on GET-only paths -- safe in single-threaded design"
  - "SumOp/AvgOp use serde_json::Value::as_f64() for numeric extraction, accepting both Int and Float JSON values"
  - "Zero events in window returns FeatureValue::Missing (not 0 or NaN) per CONTEXT.md locked decision"

patterns-established:
  - "Operator trait: push(&mut self, event, now) -> Result<(), TallyError> + read(&mut self, now) -> FeatureValue"
  - "Redis-strict type enforcement: non-numeric field -> TallyError::Type with field name and got-value"
  - "Optional field pattern: optional=true skips absent fields silently, optional=false errors"
  - "count_nonzero() == 0 check for Missing detection in sum/avg (not sum == 0)"

requirements-completed: [ENG-03, ENG-04, ENG-05]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 01 Plan 02: Core Operators Summary

**Operator trait + CountOp/SumOp/AvgOp with windowed ring buffers and Redis-strict type checking**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T13:36:42Z
- **Completed:** 2026-04-09T13:39:33Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Defined Operator trait with push/read interface that all streaming operators implement
- CountOp wraps RingBuffer<u64>, counts events regardless of shape, returns Int(N) or Missing
- SumOp wraps RingBuffer<f64>, extracts named numeric field, Redis-strict type checking
- AvgOp uses paired count+sum ring buffers, divides on read, returns Missing for zero events
- optional flag on SumOp/AvgOp: absent field skips silently (true) or errors (false)
- 24 unit tests covering normal operation, type errors, optional flag, window expiration, int coercion

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement Operator trait and CountOp** - `0bbc9f2` (feat)
2. **Task 2: Implement SumOp and AvgOp with Redis-strict type checking** - `57a775c` (feat)

## Files Created/Modified
- `src/engine/operators.rs` - Operator trait, CountOp, SumOp, AvgOp implementations + 24 unit tests
- `src/engine/mod.rs` - Added `pub mod operators` module declaration

## Decisions Made
- `read(&mut self, now)` calls `advance_to(now)` to expire stale buckets before aggregating -- ensures GET-only paths (no preceding PUSH) still return accurate window-aware values. Safe in single-threaded Redis-like design.
- SumOp/AvgOp use `serde_json::Value::as_f64()` which accepts both JSON integers and floats, consistent with CONTEXT.md Int+Float->Float coercion rule.
- SumOp uses `count_nonzero() == 0` to detect zero-event windows (not `sum == 0.0`), because a sum of 0.0 from actual events is valid.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Operator trait and three core operators ready for pipeline engine integration
- min/max/distinct_count/last operators will follow in Phase 5
- Expression evaluator (Plan 03) can reference these operators for derive evaluation
- State store (Plan 04) will hold EntityState containing these operator instances

## Self-Check: PASSED

- All created files exist on disk
- Both task commits (0bbc9f2, 57a775c) verified in git log
- Full test suite: 38 passed, 0 failed (24 operator + 14 window)

---
*Phase: 01-core-engine*
*Completed: 2026-04-09*
