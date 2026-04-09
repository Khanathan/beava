---
phase: 04-persistence-and-operational-readiness
plan: 03
subsystem: server
tags: [axum, http-api, prometheus, metrics, pipeline-crud, debug, snapshot-trigger]

# Dependency graph
requires:
  - phase: 04-persistence-and-operational-readiness
    plan: 01
    provides: OperatorState enum, SnapshotState, save_snapshot/load_snapshot, clone_for_snapshot, PipelineEngine list_streams/get_stream/remove_stream/get_raw_register_json
  - phase: 04-persistence-and-operational-readiness
    plan: 02
    provides: Periodic snapshot timer with Instant::now() timing capture, periodic eviction timer, startup recovery
  - phase: 02-tcp-server
    provides: AppState, SharedState, PUSH/GET/SET/MSET/REGISTER command handlers, HTTP /health endpoint, RegisterRequest/convert_register_request
provides:
  - Full HTTP management API: pipeline CRUD, Prometheus metrics, debug key/memory, manual snapshot trigger
  - Metrics struct with events_total, push_latency_seconds, snapshot_duration_ms
  - Per-PUSH latency tracking in TCP command handler
  - Snapshot duration metric wired into both periodic and manual snapshot paths
  - 11 integration tests for HTTP management endpoints
affects: [05-remaining-operators]

# Tech tracking
tech-stack:
  added: []
  patterns: [Prometheus text format exposition, axum State extractor with SharedState, chunked HTTP response parsing in tests]

key-files:
  created: []
  modified:
    - src/server/http.rs
    - src/server/tcp.rs
    - src/main.rs
    - tests/test_server.rs

key-decisions:
  - "Metrics struct uses last-observed gauge pattern for push_latency_seconds (not histogram) -- simplest approach for v1"
  - "Memory estimate uses 2048 bytes per entity as rough heuristic for tally_memory_bytes"
  - "POST /pipelines stores raw JSON via store_raw_register_json for snapshot pipeline persistence (same as TCP REGISTER)"
  - "trigger_snapshot converts serde_json::Value to String via serde_json::to_string for SerializablePipeline.raw_register_json (postcard compatibility)"

patterns-established:
  - "HTTP management handlers use State(state): State<SharedState> extractor pattern with poison-recovery lock"
  - "Prometheus text format: HELP/TYPE/value triplets with text/plain; version=0.0.4 content type"
  - "Debug endpoint collects owned data from immutable borrow first, then calls mutable get_all_features to avoid borrow conflict"

requirements-completed: [SRV-08]

# Metrics
duration: 4min
completed: 2026-04-09
---

# Phase 04 Plan 03: HTTP Management API Summary

**Full HTTP management API with pipeline CRUD, Prometheus metrics (5 gauges including push latency and snapshot duration), debug key/memory inspection, and manual snapshot trigger with pipeline persistence**

## Performance

- **Duration:** 4 min
- **Started:** 2026-04-09T19:18:02Z
- **Completed:** 2026-04-09T19:22:50Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments
- Expanded HTTP management API from /health-only to 8 endpoints: pipeline CRUD (list, get, create, delete), Prometheus metrics, debug key inspection, debug memory overview, and manual snapshot trigger
- Added Metrics struct to AppState with events_total, push_latency_seconds, snapshot_duration_ms; per-PUSH Instant::now() timing in TCP handler
- Wired snapshot_duration_ms into both periodic snapshot timer (main.rs) and manual trigger_snapshot (http.rs), so tally_snapshot_duration_seconds is non-zero after first snapshot
- Added 11 integration tests covering all HTTP endpoints with success and error paths; total 291 tests passing

## Task Commits

Each task was committed atomically:

1. **Task 1: Metrics struct, push latency, snapshot_duration_ms, HTTP management endpoints** - `d49e7ab` (feat)
2. **Task 2: Integration tests for HTTP management endpoints** - `c3e4a8a` (test)

**Plan metadata:** (pending)

## Files Created/Modified
- `src/server/tcp.rs` - Added Metrics struct (events_total, push_latency_seconds, snapshot_duration_ms), added metrics field to AppState, per-PUSH timing with Instant::now()
- `src/server/http.rs` - Expanded from /health-only to full management API with 8 endpoints, build_router function, axum State extractor
- `src/main.rs` - Added Metrics::default() to AppState construction, wired snapshot_duration_ms in periodic snapshot timer Ok(Ok(_)) arm
- `tests/test_server.rs` - Added Metrics import and field to test AppState, 11 new integration tests, HTTP helper functions (http_get, http_post, http_delete)

## Decisions Made
- Used last-observed gauge pattern for push_latency_seconds rather than histogram (simplest for v1; users can scrape at arbitrary intervals)
- Memory estimate of 2048 bytes per entity is a rough heuristic for tally_memory_bytes; no actual memory measurement in v1
- POST /pipelines handler stores raw JSON via store_raw_register_json (same as TCP REGISTER handler) so manually triggered snapshots include pipeline definitions
- trigger_snapshot uses serde_json::to_string to convert &serde_json::Value to String for SerializablePipeline.raw_register_json (postcard cannot serialize serde_json::Value)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed unused import warning for axum routing::delete**
- **Found during:** Task 1 (HTTP management endpoints)
- **Issue:** Imported `delete` from `axum::routing` but axum 0.8's `.delete()` method on MethodRouter doesn't need the standalone function import
- **Fix:** Removed `delete` from the routing import list
- **Files modified:** src/server/http.rs
- **Verification:** `cargo test --lib` compiles with zero warnings
- **Committed in:** d49e7ab

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Trivial import fix. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 4 is complete: serialization foundation (Plan 01), snapshot wiring and eviction timers (Plan 02), and HTTP management API (Plan 03) all operational
- All 291 tests passing across the full suite
- Ready for Phase 5: remaining operators (min, max, distinct_count, last, cross-key lookup, event fan-out)
- Metrics infrastructure in place for Phase 5 operators to benefit from

## Self-Check: PASSED

All files verified present. All commit hashes verified in git log.

---
*Phase: 04-persistence-and-operational-readiness*
*Completed: 2026-04-09*
