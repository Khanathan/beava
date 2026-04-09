---
phase: 01-core-engine
plan: 01
subsystem: engine
tags: [rust, ring-buffer, sliding-window, ahash, serde, thiserror, winnow, postcard]

# Dependency graph
requires: []
provides:
  - Rust project skeleton with all Phase 1 dependencies (ahash, winnow, thiserror, serde, serde_json, postcard)
  - FeatureValue enum (Float/Int/String/Missing) with serde derives
  - TallyError enum (Parse/Type/Window/Expression/Protocol) with thiserror
  - RingBuffer<T> time-bucketed sliding window with configurable granularity
  - EntityKey, Timestamp, FeatureMap type aliases
affects: [01-core-engine, 02-server, 04-persistence]

# Tech tracking
tech-stack:
  added: [rust 1.94.1, ahash 0.8, winnow 1.0, thiserror 2.0, serde 1.0, serde_json 1.0, postcard 1.1]
  patterns: [ring-buffer-with-lazy-expiration, feature-value-enum-with-missing, single-error-enum]

key-files:
  created:
    - Cargo.toml
    - src/lib.rs
    - src/main.rs
    - src/types.rs
    - src/error.rs
    - src/engine/mod.rs
    - src/engine/window.rs
    - src/state/mod.rs
    - src/state/store.rs
  modified: []

key-decisions:
  - "Used edition 2021 (not 2024) for broader compatibility"
  - "RingBuffer uses Vec<T> with head pointer, not VecDeque -- simpler for fixed-size ring"
  - "bucket_start_for uses integer division truncation for clean alignment"
  - "advance_to uses f64 arithmetic for bucket calculation to handle non-divisible windows"

patterns-established:
  - "Ring buffer lazy expiration: no background timers, expire on advance_to()"
  - "SystemTime arithmetic safety: always unwrap_or(Duration::ZERO) per pitfall 1"
  - "Full-window-gap detection: if gap >= num_buckets, zero ALL buckets"
  - "Generic ring buffer: RingBuffer<T> works with u64, f64, or any Default+Copy type"

requirements-completed: [ENG-02]

# Metrics
duration: 3min
completed: 2026-04-09
---

# Phase 01 Plan 01: Project Foundation and Ring Buffer Summary

**Rust project skeleton with FeatureValue/TallyError types and a time-bucketed RingBuffer for sliding window aggregation, fully tested with 14 unit tests**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-09T13:30:59Z
- **Completed:** 2026-04-09T13:34:21Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments
- Installed Rust 1.94.1 toolchain and initialized tally crate with all Phase 1 dependencies
- Created FeatureValue enum (Float/Int/String/Missing) with serde derives and as_f64 promotion
- Created TallyError enum with Parse/Type/Window/Expression/Protocol variants using thiserror
- Implemented RingBuffer<T> with time-bucketed sliding window, lazy expiration, and configurable granularity
- 14 comprehensive unit tests covering creation, advancement, wrap-around, pitfall mitigations

## Task Commits

Each task was committed atomically:

1. **Task 1: Install Rust toolchain and create project skeleton with core types** - `36d99ea` (feat)
2. **Task 2: Implement time-bucketed RingBuffer (TDD RED)** - `7a1b4b5` (test)
3. **Task 2: Implement time-bucketed RingBuffer (TDD GREEN)** - `9c5e6f3` (feat)

## Files Created/Modified
- `Cargo.toml` - Project manifest with all Phase 1 dependencies
- `src/lib.rs` - Crate root with module declarations
- `src/main.rs` - Minimal binary entry point placeholder
- `src/types.rs` - FeatureValue, Timestamp, EntityKey, FeatureMap type definitions
- `src/error.rs` - TallyError enum with thiserror derives
- `src/engine/mod.rs` - Engine module with window submodule
- `src/engine/window.rs` - RingBuffer<T> with 14 unit tests (338 lines)
- `src/state/mod.rs` - State module with store submodule
- `src/state/store.rs` - Placeholder for Plan 04

## Decisions Made
- Used edition 2021 (not 2024 which cargo init defaults to) for broader compatibility with the specified dependency versions
- RingBuffer uses Vec<T> with head pointer rather than VecDeque -- simpler and more cache-friendly for fixed-size ring buffers
- advance_to() uses f64 arithmetic for bucket count calculation to correctly handle non-divisible window/bucket combinations
- bucket_start_for() uses integer division truncation on epoch seconds for clean boundary alignment

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Project compiles cleanly with all Phase 1 dependencies
- RingBuffer<T> is ready for operators to wrap in Plan 02 (count, sum, avg)
- FeatureValue and TallyError types ready for use across all modules
- Module structure (engine/, state/) ready for Plan 02-04 additions

## Self-Check: PASSED

- All 9 created files verified present on disk
- All 3 task commits verified in git log (36d99ea, 7a1b4b5, 9c5e6f3)

---
*Phase: 01-core-engine*
*Completed: 2026-04-09*
