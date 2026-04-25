---
phase: 18-redis-hand-roll
plan: "18-00"
subsystem: infra
tags: [redis, event-loop, mio, io-threads, wal, rust, architecture, research]

# Dependency graph
requires:
  - phase: 13.3-lockless-apply
    provides: Phase 13.3 bottleneck investigation confirming tokio reactor overhead (43% of server thread = kevent calls)
provides:
  - Redis 7.x hot-path architecture summary (event loop, I/O threads, client state machine, AOF, RESP parsing, command dispatch)
  - Redis-to-Rust pattern translation table (20 patterns mapped, Send/Sync rules, atomics ordering, cross-runtime handoff)
  - Locked decisions D-01..D-16 + success criteria SC1-SC8 + file inventory for Phase 18 implementation plans
affects:
  - 18-01-PLAN.md (hand-rolled event loop + HTTP + TCP listeners)
  - 18-02-PLAN.md (inline WAL + pthread fsync)
  - 18-03-PLAN.md (I/O threads for reads)
  - 18-04-PLAN.md (I/O threads for writes)
  - 18-05-PLAN.md (io_uring on Linux, hard-gate)
  - 18-06-PLAN.md (wire polish + VERIFICATION)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Redis-mirrored single-apply-thread event loop: serialize command execution on one OS thread, parallelize I/O across N threads guarded by AtomicUsize spin-barrier"
    - "Inline WAL append per command (no mpsc hop), flush once per tick in before_sleep, fsync on dedicated std::thread"
    - "Cross-runtime handoff: bounded std::sync::mpsc::SyncSender from tokio HTTP tasks to apply std::thread for HTTP push path"

key-files:
  created:
    - .planning/phases/18-redis-hand-roll/18-redis-research.md
    - .planning/phases/18-redis-hand-roll/18-rust-translation.md
    - .planning/phases/18-redis-hand-roll/18-CONTEXT.md

key-decisions:
  - "D-01: Hand-rolled event loop handles both HTTP/1.1 + framed TCP data-plane; admin endpoints stay on tokio/axum on port 8081"
  - "D-02: Apply thread is a single OS thread owning Rc<RefCell<AppState>> — no Arc needed, no tokio fairness invariant"
  - "D-03: I/O thread count = num_cpus() - 1 (configurable)"
  - "D-04: Coordination via AtomicUsize per I/O thread, spin_loop() with exponential backoff then park"
  - "D-05: WAL inline append in apply thread, fsync on dedicated std::thread, Periodic vs PerEvent modes"
  - "D-15: New crate beava-runtime-core (not beava-redis-core); feature-flagged --features hand-rolled-runtime until cutover"
  - "D-16: Apple-M4 gates are informational through Stage 18.4; Linux Xeon is the hard-gate platform from Stage 18.5 onward"

patterns-established:
  - "Redis-to-Rust translation reference: 20 C patterns mapped to Rust equivalents with pitfall notes"
  - "Atomic ordering rules: Release/Acquire for io_pending and durable_lsn; Relaxed only for pure counters"
  - "BytesMut as querybuf equivalent; Bytes (Arc-counted) for argv across parse/execute boundary"

requirements-completed: []

# Metrics
duration: 5min
completed: 2026-04-25
---

# Phase 18 Plan 00: Research + Design Summary

**Redis 7.x architecture distilled into 20-pattern Rust translation table plus 16 locked design decisions (D-01..D-16) providing the complete spec for Phase 18's hand-rolled event loop implementation.**

## Performance

- **Duration:** ~5 min (verification + SUMMARY only — all artifacts pre-committed)
- **Started:** 2026-04-25T12:05:54Z
- **Completed:** 2026-04-25T12:10:00Z
- **Tasks:** 3 (all pre-complete at plan start)
- **Files modified:** 0 (artifacts already committed; SUMMARY created)

## Accomplishments

- Verified all three research artifacts are present and their authoritative commits are reachable
- `18-redis-research.md` (commit `a5edf82`): 235-line summary of Redis 7.x hot path covering event loop (`ae.c`), I/O threads (`networking.c`), client state machine, AOF inline-append + scheduled-fsync, RESP parse, and command dispatch. Includes "What this means for Beava Phase 18" section mapping each pattern to concrete Rust tasks.
- `18-rust-translation.md` (commit `4798bd3`): 103-line translation table with 20 numbered patterns (Redis C idiom → Rust equivalent → Beava file location → pitfall), plus Send/Sync rules, allocations comparison, atomic ordering rules, and cross-runtime handoff design.
- `18-CONTEXT.md` (commit `050cb32`): 140-line locked decisions document covering D-01..D-16, success criteria SC1-SC8, file inventory per stage, plan structure table (18-00..18-06), grey areas, and spec interpretation rationale.

## Task Commits

Pre-existing commits (inherited from base branch `v2/greenfield`):

1. **Task 0.1: Redis 7.x research** - `a5edf82` (docs)
2. **Task 0.2: Rust translation spec** - `4798bd3` (docs)
3. **Task 0.3: CONTEXT.md D-01..D-16** - `050cb32` (docs)

**Plan metadata:** *(committed by this SUMMARY)*

## Files Created/Modified

- `.planning/phases/18-redis-hand-roll/18-redis-research.md` — Redis 7.x hot-path architecture summary (235 lines)
- `.planning/phases/18-redis-hand-roll/18-rust-translation.md` — Pattern-by-pattern C → Rust translation table (103 lines)
- `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` — Locked decisions D-01..D-16, success criteria, file inventory, plan structure (140 lines)

## Decisions Made

All 16 architectural decisions for Phase 18 were locked in `18-CONTEXT.md`. Key decisions relevant to subsequent plans:

- **D-01:** HTTP/1.1 and framed TCP both served by the hand-rolled event loop on the data plane. Admin (`/metrics`, `/health`, `/ready`, `/registry`) on a separate tokio/axum port 8081.
- **D-02:** Apply thread owns `Rc<RefCell<AppState>>` directly. Tokio eliminated from hot path entirely.
- **D-15:** New crate `beava-runtime-core`; feature-flagged behind `--features hand-rolled-runtime` until cutover. Existing `beava-server` shape retained.
- **D-16:** Linux Xeon is the hard-gate platform from Stage 18.5 (io_uring) onward. Apple-M4 numbers are informational through Stage 18.4. The ≥3M EPS/core SC1 target must pass on Linux Xeon.

## Deviations from Plan

None — plan executed exactly as specified. All three artifacts were pre-committed on the base branch. Verification confirmed artifact presence and commit reachability. SUMMARY created as directed.

## Issues Encountered

Worktree was checked out at an older base commit (`e9ace7c`) and required `git reset --soft e24e00292` to align with the expected base. After reset, `git checkout HEAD -- .planning/phases/18-redis-hand-roll/` was needed to populate the working tree with files that existed in the git index but not on disk. No data loss — the reset was soft and the files were already in git.

## Next Phase Readiness

Plan 18-01 (`18-01-PLAN.md`) is ready to execute. It has all context it needs:

- `18-CONTEXT.md` provides locked decisions D-01..D-16 as the binding spec
- `18-rust-translation.md` provides the 20-pattern implementation map
- `18-redis-research.md` provides the Redis source-level reference for each pattern

TDD red-green discipline applies from Plan 18-01 onward (per CLAUDE.md §Conventions and `18-CONTEXT.md` §Grey areas note 5). Plan 18-01 tasks must split into `test:` (red) then `feat:` (green) commits.

---
*Phase: 18-redis-hand-roll*
*Completed: 2026-04-25*

## Self-Check: PASSED

Artifacts verified:
- FOUND: `.planning/phases/18-redis-hand-roll/18-redis-research.md`
- FOUND: `.planning/phases/18-redis-hand-roll/18-rust-translation.md`
- FOUND: `.planning/phases/18-redis-hand-roll/18-CONTEXT.md`

Commits verified reachable:
- FOUND: `a5edf82` — docs(18-redis-hand-roll): research summary of Redis 7.x architecture
- FOUND: `4798bd3` — docs(18-redis-hand-roll): Rust translation of Redis architectural patterns
- FOUND: `050cb32` — docs(18-redis-hand-roll): CONTEXT + plans 18-00, 18-01, 18-02
