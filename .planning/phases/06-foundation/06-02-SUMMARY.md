---
phase: 06-foundation
plan: 02
subsystem: engine, server
tags: [eviction, ttl, mget, tcp-protocol, per-stream]

# Dependency graph
requires:
  - phase: 06-01
    provides: "Per-stream EntityState grouping (StreamEntityState with independent last_event_at)"
provides:
  - "Per-stream entity TTL eviction (evict_expired_stream_entries)"
  - "entity_ttl and history_ttl fields on StreamDefinition"
  - "MGET command (opcode 0x06) for batch key reads"
  - "entity_keys() iterator on StateStore"
  - "get_stream_entity_ttl() accessor on PipelineEngine"
affects: [06-03, 06-04, event-log, schema-evolution]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-stream eviction: two-phase (remove expired stream entries, then remove empty entities)"
    - "MGET T-06-03 mitigation: strip qualified Stream.feature names from response"

key-files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/state/eviction.rs
    - src/state/store.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - tests/test_pipeline.rs
    - tests/test_snapshot.rs

key-decisions:
  - "evict_expired_keys delegated to evict_expired_stream_entries as thin wrapper for backward compatibility"
  - "Per-stream eviction uses two-phase: remove expired streams, then remove_empty_entities"
  - "MGET routed through handle_sync_command (not chunked like MSET) since reads are fast"

patterns-established:
  - "Per-stream TTL: each stream can define entity_ttl independently; None falls back to global"
  - "MGET strips qualified names: T-06-03 mitigation prevents leaking internal Stream.feature names"

requirements-completed: [OPS-01, OPS-02]

# Metrics
duration: 9min
completed: 2026-04-09
---

# Phase 6 Plan 2: Per-Stream Entity TTL Eviction and MGET Command Summary

**Per-stream entity TTL eviction with configurable entity_ttl/history_ttl on StreamDefinition, plus MGET (0x06) batch read command**

## Performance

- **Duration:** 9 min
- **Started:** 2026-04-09T23:32:38Z
- **Completed:** 2026-04-09T23:41:55Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Per-stream entity TTL eviction removes individual stream entries independently without removing entire entities
- entity_ttl and history_ttl optional fields on StreamDefinition with RegisterRequest parsing via parse_duration_str
- MGET command (opcode 0x06) returns nested JSON map with per-key feature maps, empty {} for missing keys
- T-06-03 mitigation: MGET strips qualified "Stream.feature" names from response
- Full backward compatibility: evict_expired_keys delegates to new evict_expired_stream_entries

## Task Commits

Each task was committed atomically:

1. **Task 1: Add entity_ttl/history_ttl and per-stream eviction** - `dcc4adb` (feat)
2. **Task 2: Add MGET command to protocol and TCP handler** - `07d0465` (feat)

## Files Created/Modified
- `src/engine/pipeline.rs` - Added entity_ttl, history_ttl to StreamDefinition; get_stream_entity_ttl accessor
- `src/state/eviction.rs` - New evict_expired_stream_entries with per-stream TTL logic; legacy wrapper
- `src/state/store.rs` - Added entity_keys() iterator
- `src/server/protocol.rs` - OP_MGET opcode, Command::Mget variant, MGET parsing, entity_ttl/history_ttl on RegisterRequest
- `src/server/tcp.rs` - MGET handler with qualified name stripping
- `tests/test_pipeline.rs` - Updated StreamDefinition constructions for new fields
- `tests/test_snapshot.rs` - Updated StreamDefinition constructions and eviction test semantics

## Decisions Made
- evict_expired_keys kept as thin wrapper delegating to evict_expired_stream_entries for backward compatibility with main.rs callsite
- Per-stream eviction uses two-phase approach: first remove expired stream entries per-entity, then call remove_empty_entities to clean up
- MGET routed through synchronous command path (not chunked like MSET) because reads are fast and non-destructive
- Streams with entity_ttl=None and max_window=0 are skipped (not evicted) to preserve derive-only stream entities

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed empty entity eviction in old tests**
- **Found during:** Task 1
- **Issue:** Old eviction tests created entities with no streams (completely empty). The new evict_expired_stream_entries calls remove_empty_entities which correctly removes empty entities, causing old test assertions to fail.
- **Fix:** Updated tests to create entities with stream entries so they are not empty, matching realistic usage.
- **Files modified:** src/state/eviction.rs, tests/test_snapshot.rs
- **Verification:** All eviction tests pass
- **Committed in:** dcc4adb (Task 1 commit)

**2. [Rule 3 - Blocking] Added entity_keys() to StateStore**
- **Found during:** Task 1
- **Issue:** evict_expired_stream_entries needs to iterate entity keys to avoid borrow conflicts, but StateStore had no public key iterator
- **Fix:** Added entity_keys() method returning Iterator<Item = String>
- **Files modified:** src/state/store.rs
- **Verification:** Compilation succeeds, eviction tests pass
- **Committed in:** dcc4adb (Task 1 commit)

**3. [Rule 3 - Blocking] Updated integration test files for new StreamDefinition fields**
- **Found during:** Task 1
- **Issue:** tests/test_pipeline.rs and tests/test_snapshot.rs construct StreamDefinition directly and were missing entity_ttl/history_ttl fields
- **Fix:** Added entity_ttl: None, history_ttl: None to all StreamDefinition constructions in integration tests
- **Files modified:** tests/test_pipeline.rs, tests/test_snapshot.rs
- **Verification:** cargo test passes all 410 tests
- **Committed in:** dcc4adb (Task 1 commit)

---

**Total deviations:** 3 auto-fixed (1 bug fix, 2 blocking)
**Impact on plan:** All auto-fixes necessary for correctness and compilation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- entity_ttl and history_ttl fields on StreamDefinition ready for Plan 03 (event log) to use history_ttl
- MGET functional for Python SDK integration in later plans
- Per-stream eviction ready for independent stream lifecycle management

## Self-Check: PASSED

All files exist. All commits verified. All key content present (entity_ttl, history_ttl, OP_MGET, Command::Mget, evict_expired_stream_entries).

---
*Phase: 06-foundation*
*Completed: 2026-04-09*
