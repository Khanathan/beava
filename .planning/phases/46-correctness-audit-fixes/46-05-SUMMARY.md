---
phase: 46-correctness-audit-fixes
plan: 05
subsystem: engine/eviction, server/tcp
tags: [event-time, watermark, eviction, replica, backfill, correctness]
dependency_graph:
  requires: [46-01, 46-04]
  provides: [CORR-06, CORR-07, CORR-08]
  affects: [src/server/tcp.rs, src/state/eviction.rs, tests/test_eviction_event_time_clock.rs, tests/test_fork_watermark_propagation.rs]
tech_stack:
  added: []
  patterns: [parse_event_time fallback, WatermarkTracker.observed_max per-stream clock, watermarks.observe post-push]
key_files:
  created:
    - tests/test_eviction_event_time_clock.rs
    - tests/test_fork_watermark_propagation.rs
  modified:
    - src/server/tcp.rs
    - src/state/eviction.rs
    - src/server/protocol.rs
    - tests/test_replica_batch.rs
    - tests/test_ttl_defaults.rs
    - tests/test_config_recommendations.rs
    - (22 additional test files — Rule 3 companion watermark_lateness: None additions)
decisions:
  - "D-15: parse_event_time(&event, entry.timestamp) in run_backfill; wall-clock fallback only when payload lacks _event_time"
  - "D-17: scan_clock = engine.watermarks.observed_max(stream_name).unwrap_or(now) inside per-stream eviction loop — function signature unchanged"
  - "D-19: engine.watermarks.observe(stream_name, event_time) placed after successful push_with_cascade_no_features in replica_ingest_batch — per-event, not batch-end, to match live-path semantics"
  - "Test design: ttl_honors_event_time_not_wall_clock calls watermarks.observe explicitly (mirrors TCP live-path, since push_with_cascade_no_features does not internally observe)"
metrics:
  duration: ~45 minutes
  completed: 2026-04-17
  tasks: 3
  files_changed: 28
---

# Phase 46 Plan 05: Backfill Event-Time + TTL Clock + Replica Watermark Summary

One-liner: parse_event_time in run_backfill + per-stream eviction clock from WatermarkTracker + replica_ingest_batch observes watermarks per event — three surgical one-line fixes closing CORR-06, CORR-07, CORR-08.

## What Was Built

### Fix 1 — D-15 / CORR-06: run_backfill uses payload _event_time

`src/server/tcp.rs` line ~2741 (inside the per-entry chunk loop of `run_backfill`):

Before:
```rust
let _ = engine.push_for_backfill(&stream_name, &event, &state.store, entry.timestamp, &feature_names);
```

After:
```rust
let event_time = crate::engine::event_time::parse_event_time(&event, entry.timestamp);
let _ = engine.push_for_backfill(&stream_name, &event, &state.store, event_time, &feature_names);
```

`entry.timestamp` was the wall-clock-at-append time, causing crash-replay to bucket events differently from live-ingest. Now payload `_event_time` is used with `entry.timestamp` as fallback — matching live-path semantics exactly. This is the precondition for the ship-gate test (SHIP-01, Plan 08) to turn green.

### Fix 2 — D-17 / CORR-07: eviction clock from WatermarkTracker

`src/state/eviction.rs` inside `evict_expired_stream_entries` per-stream loop:

Before:
```rust
let age = now.duration_since(last_event).unwrap_or(Duration::ZERO);
```

After:
```rust
let scan_clock = engine.watermarks.observed_max(stream_name).unwrap_or(now);
let age = scan_clock.duration_since(last_event).unwrap_or(Duration::ZERO);
```

Historical backfills with 30-day-old events no longer immediately evict entities under a 7-day TTL. The fallback to wall-clock `now` preserves existing test semantics for streams with no observed watermark. Function signature unchanged.

### Fix 3 — D-19 / CORR-08: replica_ingest_batch observes watermarks

`src/server/tcp.rs` inside `replica_ingest_batch` after successful `push_with_cascade_no_features`:

```rust
// D-19 / CORR-08: advance the replica's watermark per event so downstream
// table-cascade γ-propagation fires. Mirrors the live-ingest call at
// tcp.rs:1750.
engine.watermarks.observe(stream_name, event_time);
```

Fork replicas were stalling watermarks at None forever. Now each successfully-applied event advances the per-stream watermark monotonically, unblocking downstream cascade propagation.

## Commits

| Hash | Message |
|------|---------|
| `60fb5cf` | fix(46-05): run_backfill uses parse_event_time + replica_ingest_batch observes watermarks (D-15 CORR-06, D-19 CORR-08) |
| `f4af631` | fix(46-05): eviction clock sources from WatermarkTracker::observed_max (D-17 CORR-07) |
| `486bfd2` | test(46-05): CORR-07 + CORR-08 integration tests un-ignored and green (D-18, D-20) |
| `0a30f3c` | chore(46-05): add watermark_lateness: None to StreamDefinition and SourceDescriptor initialisers across all test files (Rule 3) |

## Tests

- `tests/test_eviction_event_time_clock.rs::ttl_honors_event_time_not_wall_clock` — CORR-07: asserts 30d-old event NOT evicted under 7d TTL when watermark=30d-ago; advances watermark to now, asserts entity IS evicted. Green x3.
- `tests/test_fork_watermark_propagation.rs::replica_batch_advances_watermark` — CORR-08: feeds 10 events via `replica_ingest_batch`, asserts `watermarks.observed_max("Txns") >= max ts_ms`. Green x3.
- `cargo test --release --lib` — 788 tests, 0 failures.
- `cargo test --test test_replica_batch --release` — 2 tests, 0 failures (existing replica test unaffected by D-19).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing `watermark_lateness` field in StreamDefinition/SourceDescriptor initialisers**
- **Found during:** Task 2 (D-17 lib test run)
- **Issue:** Plan 46-04 (running in parallel Wave 3) added `watermark_lateness: Option<Duration>` to `StreamDefinition` and `watermark_lateness: Option<String>` to `SourceDescriptor`. The new required fields broke compilation of `src/server/protocol.rs` (1 site), `src/state/eviction.rs` (3 inline test sites), `src/server/tcp.rs` (2 inline test sites), and 22 integration test files.
- **Fix:** Added `watermark_lateness: None` to every affected `StreamDefinition` and `SourceDescriptor` initialiser. No semantic change — `None` maps to the existing 5 s default via the fallback path in `WatermarkTracker::lateness_for`.
- **Files modified:** `src/server/protocol.rs`, `src/state/eviction.rs`, `src/server/tcp.rs`, plus 22 test files
- **Commit:** `0a30f3c` (test files batch), `f4af631` (src files)

**2. [Deviation from plan test design] `push_with_cascade_no_features` does not internally advance watermarks**
- **Found during:** Task 3 (CORR-07 test first run — entity was evicted immediately)
- **Issue:** The plan's suggested test called `push_with_cascade_no_features` and expected the watermark to advance automatically. In reality, watermark observation is the TCP dispatcher's responsibility (tcp.rs:1750 for live, now tcp.rs:1113 for replica). The engine primitive does not call `watermarks.observe` internally.
- **Fix:** The test was updated to call `engine.watermarks.observe("Txns", thirty_days_ago)` explicitly after pushing the event — mirroring what the TCP live-path does. This is not a source bug, just a test fixture design correction.
- **Commit:** `486bfd2`

## Running Requirements Tally

| Requirement | Status | Closed by |
|-------------|--------|-----------|
| CORR-01 | Closed (Plan 46-02) | — |
| CORR-02 | Closed (Plan 46-02) | — |
| CORR-03 | Closed (Plan 46-04) | — |
| CORR-04 | Closed (Plan 46-04) | — |
| CORR-05 | Closed (Plan 46-03) | — |
| CORR-06 | **Closed this plan** | D-15 — run_backfill parse_event_time |
| CORR-07 | **Closed this plan** | D-17 — eviction scan_clock from observed_max |
| CORR-08 | **Closed this plan** | D-19 — replica_ingest_batch watermarks.observe |
| CORR-09 | Open (Plan 46-06) | — |
| CORR-10 | Open (Plan 46-07) | — |

Running closed total: **8 of 14** Phase 46 requirements.

## Pointer: Ship-Gate Test

`tests/ship_gate.rs` contains a `#[ignore]` test (SHIP-01) that verifies crash-replay parity with live-ingest. It was RED before D-15 (run_backfill used wall-clock timestamp). After this plan's D-15 fix, the precondition is satisfied. Plan 46-08 will un-ignore `ship_gate.rs` and verify it turns green.

## Known Stubs

None — all three fixes are complete, unconditional, and wired to production code paths.

## Self-Check: PASSED

- FOUND: tests/test_eviction_event_time_clock.rs
- FOUND: tests/test_fork_watermark_propagation.rs
- FOUND: src/server/tcp.rs
- FOUND: src/state/eviction.rs
- FOUND: commit 60fb5cf (D-15 + D-19)
- FOUND: commit f4af631 (D-17)
- FOUND: commit 486bfd2 (tests)
- FOUND: commit 0a30f3c (Rule 3 companion)
