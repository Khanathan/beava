---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: verifying
stopped_at: Completed 03-04-PLAN.md (integration tests)
last_updated: "2026-04-09T16:57:23.191Z"
last_activity: 2026-04-09
progress:
  total_phases: 5
  completed_phases: 3
  total_plans: 13
  completed_plans: 13
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.
**Current focus:** Phase 03 — python-sdk

## Current Position

Phase: 4
Plan: Not started
Status: Phase complete — ready for verification
Last activity: 2026-04-09

Progress: [███████░░░] 71%

## Performance Metrics

**Velocity:**

- Total plans completed: 12
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 02 | 5 | - | - |
| 03 | 4 | - | - |

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
| Phase 02 P03 | 3min | 2 tasks | 5 files |
| Phase 02 P04 | 2min | 2 tasks | 2 files |
| Phase 02 P05 | 2min | 2 tasks | 2 files |
| Phase 03-python-sdk P01 | 4min | 2 tasks | 8 files |
| Phase 03-python-sdk P02 | 5min | 2 tasks | 7 files |
| Phase 03-python-sdk P03 | 4min | 2 tasks | 5 files |
| Phase 03-python-sdk P04 | 3min | 1 tasks | 3 files |

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
- [Phase 02]: Pre-bound listener pattern for test isolation with random ports
- [Phase 02]: Test assertions use contains() substring matching for error messages to survive minor wording changes
- [Phase 02]: Gap closure tests verify existing behavior; all 6 edge case gaps had correct handling already, tests prevent regression
- [Phase 03-python-sdk]: FeatureResult uses __slots__ with object.__setattr__ for clean attribute access
- [Phase 03-python-sdk]: parse_response raises ProtocolError on STATUS_ERROR -- exception-based error handling for callers
- [Phase 03-python-sdk]: Protocol constants use type annotations (OP_PUSH: int = 0x01) for IDE support
- [Phase 03-python-sdk]: Operator constructors use Python keyword-only args for required param validation (native TypeError)
- [Phase 03-python-sdk]: Lookup target stored as plain string ref -- cross-class attribute resolution deferred to Phase 5
- [Phase 03-python-sdk]: StreamMeta walks reversed(bases) for mixin features; later-listed bases take precedence, class body always wins
- [Phase 03-python-sdk]: TallyClient auto-reconnect: catch ConnectionError, null socket, reconnect once and retry
- [Phase 03-python-sdk]: App._parse_address uses rsplit(':',1) with default port 6400
- [Phase 03-python-sdk]: App._send centralizes STATUS_ERROR check, raises ProtocolError with decoded server message
- [Phase 03-python-sdk]: Added TALLY_TCP_PORT/TALLY_HTTP_PORT env vars to main.rs for integration test port isolation
- [Phase 03-python-sdk]: Session-scoped server fixture with unique entity keys per test for isolation without restart overhead

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

Last session: 2026-04-09T16:48:10.322Z
Stopped at: Completed 03-04-PLAN.md (integration tests)
Resume file: None
