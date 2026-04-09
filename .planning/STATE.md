---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: Completed 02-02-PLAN.md (TCP server command dispatch)
last_updated: "2026-04-09T15:14:11.376Z"
last_activity: 2026-04-09
progress:
  total_phases: 5
  completed_phases: 1
  total_plans: 7
  completed_plans: 6
  percent: 86
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 02 — TCP Server and Binary Protocol

## Current Position

Phase: 02 (TCP Server and Binary Protocol) — EXECUTING
Plan: 2 of 3
Status: Ready to execute
Last activity: 2026-04-09

Progress: [███████░░░] 71%

## Performance Metrics

**Velocity:**

- Total plans completed: 0
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| - | - | - | - |

**Recent Trend:**

- Last 5 plans: —
- Trend: —

*Updated after each plan completion*
| Phase 01-core-engine P01 | 3min | 2 tasks | 11 files |
| Phase 01-core-engine P02 | 3min | 2 tasks | 2 files |
| Phase 01-core-engine P03 | 8min | 2 tasks | 2 files |
| Phase 01-core-engine P04 | 3min | 2 tasks | 5 files |
| Phase 02-tcp-server P01 | 5min | 2 tasks | 6 files |
| Phase 02 P02 | 2min | 1 tasks | 3 files |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Init: Use AHashMap (not std HashMap) from day one — SipHash 20-25% CPU overhead at 100K+ events/sec
- Init: Use SystemTime (not Instant) for window buckets — client-supplied Unix timestamps must be comparable
- Init: Use postcard (not bincode) for snapshots — bincode has RUSTSEC-2025-0141 advisory, unmaintained
- Init: Implement HyperLogLog directly in hll.rs — external crates require nightly or are minimally maintained
- Init: Use winnow for expression parser — evolved from nom, inline combinators, no grammar files
- [Phase 01-core-engine]: Used edition 2021 (not 2024) for broader compatibility with specified deps
- [Phase 01-core-engine]: RingBuffer uses Vec<T> with head pointer (not VecDeque) for cache-friendly fixed-size ring
- [Phase 01-core-engine]: read(&mut self, now) calls advance_to(now) for accurate window expiration on GET-only paths
- [Phase 01-core-engine]: SumOp/AvgOp use serde_json as_f64() accepting both Int and Float JSON values for numeric extraction
- [Phase 01-core-engine]: winnow Alt tuple limit requires nested alt() for >9 operator alternatives
- [Phase 01-core-engine]: Keywords (and/or/not) rejected in parse_field_ref; Pratt prefix/infix handle them
- [Phase 01-core-engine]: guard_float() defense-in-depth: all f64 results checked for NaN/infinity -> Missing
- [Phase 01-core-engine]: Lazy operator instantiation: operators created on first push per entity, not at registration time
- [Phase 01-core-engine]: Static features override live features with same name (direct writes take precedence per CLAUDE.md)
- [Phase 01-core-engine]: Derive results collected into Vec before insertion to satisfy Rust borrow checker
- [Phase 02-tcp-server]: Flat DTO struct with serde rename from 'type' instead of internally tagged enum for REGISTER JSON
- [Phase 02-tcp-server]: Frame length = opcode + payload bytes (standard length-prefix convention)
- [Phase 02-tcp-server]: MSET per-entry format: [u16 key][u32 json_len][json_bytes] for streaming parse
- [Phase 02-tcp-server]: Default bucket = window/30 clamped to 1s minimum (consistent with Phase 1)
- [Phase 02]: Added Send bound to Operator trait for tokio::spawn compatibility
- [Phase 02]: Destructured AppState borrow pattern for split engine/store references in command handlers

### Pending Todos

None yet.

### Blockers/Concerns

- Phase 5: HLL epoch-based rotation memory math needs validation before implementation (N buckets x 12KB x key count). Add a spike task at Phase 5 start.
- Phase 5: Cross-key lookup semantics when target key has been TTL-evicted must be specified precisely (Missing propagation expected, not panic).
- Phase 2: REGISTER command access control — should REGISTER be restricted to HTTP port (6401) only? Confirm before Phase 2 implementation.
- Phase 4: Snapshot memory approach — clone-then-spawn_blocking creates up to 2x peak memory. Decide between clone approach and chunked cooperative yielding before Phase 4.

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260409-f8y | Generate AI image generation prompts for Tally logo/mascot | 2026-04-09 | ed7363e | [260409-f8y-generate-a-prompt-to-generate-logo-for-t](./quick/260409-f8y-generate-a-prompt-to-generate-logo-for-t/) |

## Session Continuity

Last session: 2026-04-09T15:14:11.373Z
Stopped at: Completed 02-02-PLAN.md (TCP server command dispatch)
Resume file: None
