---
phase: 05-advanced-operators-and-cross-stream
plan: 02
subsystem: engine
tags: [hyperloglog, hll, distinct-count, approximate, cardinality, ring-buffer]

# Dependency graph
requires:
  - phase: 05-advanced-operators-and-cross-stream plan 01
    provides: RingBuffer Clone relaxation (Copy -> Clone bound), update_current method
provides:
  - Hll struct (HyperLogLog sketch) with insert/count/merge/is_empty
  - DistinctCountOp implementing Operator trait with windowed approximate distinct counting
  - hll module in engine
affects: [05-03 cross-stream views, pipeline registration for distinct_count operator]

# Tech tracking
tech-stack:
  added: []
  patterns: [merge-on-read HLL aggregation across ring buffer buckets, parallel event_count buffer for Missing detection]

key-files:
  created: [src/engine/hll.rs]
  modified: [src/engine/mod.rs]

key-decisions:
  - "Vec<u8> registers (not [u8; 16384]) for Clone compatibility with RingBuffer"
  - "Parallel event_count RingBuffer<u64> tracks zero-event state for Missing detection"
  - "String/numeric/bool field values accepted; arrays/objects rejected as type errors"

patterns-established:
  - "Merge-on-read: bucket HLLs merged at read time, not maintained incrementally"
  - "update_current closure pattern for non-additive bucket updates (HLL insert)"

requirements-completed: [OPS-04]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 5 Plan 2: HyperLogLog and DistinctCountOp Summary

**HyperLogLog from scratch with 14-bit precision (16384 registers, ~1.6% error) and DistinctCountOp with epoch-based windowed rotation via RingBuffer<Hll>**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T20:41:11Z
- **Completed:** 2026-04-09T20:44:07Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- HyperLogLog implementation from scratch with insert/count/merge/is_empty, 14-bit precision, linear counting correction for small cardinalities
- DistinctCountOp wrapping RingBuffer<Hll> for windowed approximate distinct counting with merge-on-read semantics
- 23 comprehensive tests covering accuracy bounds, expiry, merge, serialization round-trip, optional/required field handling, type errors

## Task Commits

Each task was committed atomically:

1. **Task 1: HyperLogLog implementation from scratch** - `692eb3e` (feat)
2. **Task 2: DistinctCountOp using RingBuffer<Hll>** - `230ae17` (feat)

## Files Created/Modified
- `src/engine/hll.rs` - HyperLogLog sketch and DistinctCountOp with windowed rotation (new file)
- `src/engine/mod.rs` - Added `pub mod hll` module declaration

## Decisions Made
- Used `Vec<u8>` for HLL registers instead of `[u8; 16384]` array -- enables Clone without 16KB memcpy on Copy, compatible with RingBuffer<T: Clone> constraint
- Parallel `event_count: RingBuffer<u64>` tracks whether any events were pushed in window -- needed because empty HLL edge cases could produce non-zero count estimates
- Accepted string, numeric, and bool field values for distinct counting -- all converted to string representation for hashing. Arrays and objects rejected as type errors.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- DistinctCountOp ready for integration into pipeline registration (05-03 or pipeline updates)
- Operator trait implementation complete -- can be instantiated like other operators (CountOp, SumOp, etc.)
- RingBuffer<Hll> pattern validated -- merge-on-read produces correct approximate counts

---
*Phase: 05-advanced-operators-and-cross-stream*
*Completed: 2026-04-09*
