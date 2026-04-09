---
phase: 05-advanced-operators-and-cross-stream
plan: 01
subsystem: engine
tags: [rust, operators, min, max, last, where-clause, ring-buffer, windowed-aggregation]

# Dependency graph
requires:
  - phase: 01-core-engine
    provides: RingBuffer, Operator trait, CountOp/SumOp/AvgOp, expression parser
  - phase: 04-persistence-and-operational-readiness
    provides: OperatorState enum, snapshot versioning, HTTP pipeline CRUD
provides:
  - MinOp windowed minimum operator with ring buffer
  - MaxOp windowed maximum operator with ring buffer
  - LastOp stores most recent field value (no window)
  - Where-clause filtering on any windowed operator
  - OperatorState Min/Max/Last variants with postcard serialization
  - FeatureDef Min/Max/Last/where_expr variants
  - Protocol support for "min"/"max"/"last" types and "where" field
  - HTTP get_pipeline renders new operator types
affects: [05-02, 05-03, python-sdk]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "MinBucket/MaxBucket sentinel wrappers (INFINITY/-INFINITY) for type-safe ring buffer min/max"
    - "Parallel event_count RingBuffer for empty-window detection (avoids sentinel leaking to client)"
    - "update_current closure-based bucket mutation for conditional replacement (not additive)"
    - "Where-expr evaluated per-event before operator push; falsy/Missing skips operator"

key-files:
  created: []
  modified:
    - src/engine/window.rs
    - src/engine/operators.rs
    - src/engine/pipeline.rs
    - src/state/snapshot.rs
    - src/server/protocol.rs
    - src/server/http.rs
    - src/server/tcp.rs
    - src/state/eviction.rs
    - tests/test_pipeline.rs
    - tests/test_snapshot.rs

key-decisions:
  - "RingBuffer relaxed from Copy to Clone bound for MinBucket/MaxBucket wrapper compatibility"
  - "MinBucket default INFINITY / MaxBucket default NEG_INFINITY -- sentinels never returned to client (event_count guard)"
  - "LastOp stores FeatureValue directly (not raw JSON) for consistent type handling"
  - "Where-expr uses empty features AHashMap for eval context (only _event fields accessible in where clauses)"
  - "SNAPSHOT_FORMAT_VERSION bumped 1->2 -- old snapshots cleanly rejected per Phase 4 design"

patterns-established:
  - "update_current<F: FnOnce(&mut T)>: closure-based bucket mutation for non-additive operators"
  - "buckets_iter(): iterate all ring buffer buckets for aggregate computation"
  - "get_where_expr() helper: extract optional Expr from any FeatureDef variant"

requirements-completed: [OPS-01, OPS-02, OPS-03, OPS-05]

# Metrics
duration: 11min
completed: 2026-04-09
---

# Phase 05 Plan 01: Min/Max/Last Operators and Where-Clause Filtering Summary

**MinOp, MaxOp, LastOp operators with windowed ring buffer aggregation, where-clause event filtering on all windowed operators, full TCP/HTTP plumbing**

## Performance

- **Duration:** 11 min
- **Started:** 2026-04-09T20:27:36Z
- **Completed:** 2026-04-09T20:38:55Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- Implemented MinOp/MaxOp with bucketed ring buffer tracking and parallel event_count for Missing detection
- Implemented LastOp storing most recent field value (string, numeric, bool) with no window expiry
- Added where-clause filtering: any windowed operator can have an optional filter expression evaluated per-event
- Full end-to-end wiring through REGISTER (TCP + HTTP) -> PUSH -> GET with snapshot persistence support

## Task Commits

Each task was committed atomically:

1. **Task 1: RingBuffer Clone relaxation, update_current method, MinOp and MaxOp** - `848a2ee` (feat)
2. **Task 2: LastOp, where-clause filtering, OperatorState/FeatureDef/protocol/HTTP plumbing** - `5ec2b3e` (feat)

_Note: TDD tasks -- tests written first (RED), then implementation (GREEN), verified passing._

## Files Created/Modified
- `src/engine/window.rs` - RingBuffer Clone bound, update_current(), buckets_iter()
- `src/engine/operators.rs` - MinBucket, MaxBucket, MinOp, MaxOp, LastOp implementations
- `src/engine/pipeline.rs` - FeatureDef Min/Max/Last/where_expr, create_operator, where-clause push filtering
- `src/state/snapshot.rs` - OperatorState Min/Max/Last variants, version bump 1->2
- `src/server/protocol.rs` - FeatureDefRequest where/on/target fields, min/max/last convert branches
- `src/server/http.rs` - get_pipeline renders Min/Max/Last features
- `src/server/tcp.rs` - Updated test FeatureDef constructors for new fields
- `src/state/eviction.rs` - Updated test FeatureDef constructors for new fields
- `tests/test_pipeline.rs` - Updated FeatureDef constructors for where_expr field
- `tests/test_snapshot.rs` - Updated version byte and FeatureDef constructors

## Decisions Made
- RingBuffer bound relaxed from Copy to Clone: enables MinBucket/MaxBucket wrappers while preserving backward compatibility (u64/f64 are still Copy)
- MinBucket(f64::INFINITY) / MaxBucket(f64::NEG_INFINITY) sentinels: natural identity elements for min/max, never leak to client via event_count zero-check (T-5-03 mitigation)
- LastOp converts JSON to FeatureValue at push time (not stored as raw serde_json::Value) for type consistency
- Where-clause evaluation uses empty features map -- only `_event.*` field access works in where expressions, consistent with the "filter before aggregation" semantic
- Snapshot version bumped to 2: old v1 snapshots cleanly rejected with "starting fresh" message per existing Phase 4 design

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- MinOp, MaxOp, LastOp are fully functional through REGISTER -> PUSH -> GET
- Where-clause filtering works on any windowed operator (count, sum, avg, min, max)
- Ready for Plan 02 (HyperLogLog distinct_count) and Plan 03 (cross-stream views, cross-key lookups, event fan-out)
- Snapshot format v2 accepted; old snapshots cleanly rejected

---
*Phase: 05-advanced-operators-and-cross-stream*
*Completed: 2026-04-09*
