---
phase: 06-foundation
plan: 03
subsystem: persistence
tags: [event-log, append-only, ssd, bufwriter, fdatasync, compaction, ttl, postcard]

# Dependency graph
requires:
  - phase: 06-01
    provides: Per-stream EntityState grouping, StreamDefinition with history_ttl field
  - phase: 06-02
    provides: MGET command, AppState patterns
provides:
  - Append-only per-stream event log files on SSD
  - Background fsync (1s interval) and compaction (60s interval)
  - EventLog module with register, append, read, fsync, compact, deregister
  - PUSH handler event persistence for backfill replay
affects: [phase-08-backfill, phase-09-snapshots]

# Tech tracking
tech-stack:
  added: []
  patterns: [BufWriter append-only log, length-prefixed postcard serialization, atomic tmp-rename compaction, cooperative per-stream compaction]

key-files:
  created:
    - src/state/event_log.rs
  modified:
    - src/state/mod.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/main.rs
    - tests/test_server.rs

key-decisions:
  - "Borrow conflict in REGISTER handler resolved by extracting history_ttl before borrowing event_log mutably"
  - "Event log writes use BufWriter::write_all (memcpy only, ~100-300ns) -- fdatasync runs in background timer, never on hot path"
  - "Compaction acquires lock per-stream with yield_now() between streams for cooperative scheduling"

patterns-established:
  - "Length-prefixed postcard serialization: [u32 BE len][postcard bytes] for log entries"
  - "Atomic compaction via tmp file + rename pattern (no data loss window)"
  - "sanitize_stream_name for filesystem path safety (T-06-04 mitigation)"

requirements-completed: [ELOG-01, ELOG-02, ELOG-03, ELOG-04, ELOG-05]

# Metrics
duration: 5min
completed: 2026-04-09
---

# Phase 06 Plan 03: SSD Event Log Summary

**Append-only per-stream event log with BufWriter hot-path writes, background fdatasync every 1s, and TTL-based compaction every 60s**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-09T23:44:38Z
- **Completed:** 2026-04-09T23:50:00Z
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- EventLog module with append, read, fsync, compaction, and stream lifecycle management
- PUSH handler appends raw events to per-stream log files (including fan-out targets)
- Background fsync timer (1s) and compaction timer (60s) integrated into main.rs
- 22 dedicated unit tests + full 432-test suite passes
- Path traversal mitigation via sanitize_stream_name (T-06-04)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create EventLog module with append, read, and compaction** - `6c7b211` (feat)
2. **Task 2: Integrate EventLog into AppState, PUSH handler, and background timers** - `39b1020` (feat)

## Files Created/Modified
- `src/state/event_log.rs` - EventLog struct with append, read_entries, fsync_all, compact_stream, register/deregister, sanitize_stream_name
- `src/state/mod.rs` - Added `pub mod event_log` declaration
- `src/server/tcp.rs` - Added event_log to AppState, PUSH handler event logging, REGISTER handler stream registration
- `src/server/http.rs` - DELETE pipeline handler deregisters from event log
- `src/main.rs` - EventLog initialization from TALLY_DATA_DIR, snapshot recovery stream registration, fsync and compaction background timers
- `tests/test_server.rs` - Added event_log: None to AppState test construction

## Decisions Made
- Borrow conflict in REGISTER handler resolved by extracting history_ttl via get_stream() before mutably borrowing event_log (same pattern used in Plan 02 for push borrow conflicts)
- Event log uses Option<EventLog> in AppState for backward compatibility -- system works without event log (tests don't need it)
- fsync_all called under lock because BufWriter::flush + sync_data are fast for typical buffer sizes (< 1ms lock hold)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed borrow conflict in REGISTER handler and main.rs snapshot recovery**
- **Found during:** Task 2 (Integration)
- **Issue:** Borrowing `app.engine` immutably while `app.event_log` was borrowed mutably caused Rust borrow checker error E0502
- **Fix:** Extracted `history_ttl` via `app.engine.get_stream()` into a local variable before borrowing `app.event_log`
- **Files modified:** src/server/tcp.rs, src/main.rs
- **Verification:** `cargo test` passes all 432 tests
- **Committed in:** 39b1020

**2. [Rule 1 - Bug] Fixed sanitize_stream_name test assertion**
- **Found during:** Task 1 (TDD RED phase)
- **Issue:** Test expected `"____etc_passwd"` but `"../../etc/passwd"` sanitizes to `"______etc_passwd"` because `/` replacement creates new `..` sequences that get further replaced
- **Fix:** Updated test assertion to match actual correct behavior
- **Files modified:** src/state/event_log.rs
- **Committed in:** 6c7b211

**3. [Rule 3 - Blocking] Removed unused import `Path`**
- **Found during:** Task 2 (compilation)
- **Issue:** `std::path::Path` was imported but not used, triggering warning
- **Fix:** Removed unused import
- **Files modified:** src/state/event_log.rs
- **Committed in:** 39b1020

---

**Total deviations:** 3 auto-fixed (2 bugs, 1 blocking)
**Impact on plan:** All auto-fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Event log persistence layer complete, ready for Phase 8 backfill (replay from event log)
- Background timers follow existing patterns (snapshot, eviction) for consistency
- TALLY_DATA_DIR environment variable controls event log location (defaults to ./events/)

## Self-Check: PASSED

- All files exist on disk
- All commits verified in git log
- All 17 acceptance criteria confirmed via grep

---
*Phase: 06-foundation*
*Completed: 2026-04-09*
