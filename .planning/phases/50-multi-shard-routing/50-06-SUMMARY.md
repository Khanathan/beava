---
phase: 50-multi-shard-routing
plan: "06"
subsystem: shard-routing
tags: [tpc, shard-key-validation, missing-field-reject, warnings, deprecation]
dependency_graph:
  requires: [50-04]
  provides: [shard_key_missing_reject, shard_key_missing_warning, entities_shards_deprecation]
  affects: [server/tcp.rs, server/http.rs, server/signals.rs, server/protocol.rs, error.rs, state/store.rs]
tech_stack:
  added: []
  patterns: [STATUS_SHARD_KEY_MISSING constant, emit_shard_key_missing_warning registry pattern]
key_files:
  created: []
  modified:
    - src/error.rs
    - src/server/http.rs
    - src/server/http_ingest.rs
    - src/server/protocol.rs
    - src/server/signals.rs
    - src/server/tcp.rs
    - src/state/store.rs
decisions:
  - "STATUS_SHARD_KEY_MISSING = 0x12 defined in protocol.rs (separate namespace from OP codes; 0x12 is OP_SNAPSHOT_FETCH but status/op namespaces are disjoint in the wire format)"
  - "emit_shard_key_missing_warning wired in TCP v0 path, TCP v2 path, and HTTP create_pipeline — all three registration call sites covered"
  - "shard_count derived from state.shard_handles.read().len() at registration time (0 when tests bypass run_tcp_server, so warning is silent — correct per D-12)"
metrics:
  duration_minutes: 25
  completed: "2026-04-18T15:35:48Z"
  tasks_completed: 2
  files_modified: 7
---

# Phase 50 Plan 06: shard_key missing-field reject + warnings + deprecation Summary

One-liner: HTTP 400 / TCP 0x12 rejection of events missing tuple shard_key fields before routing, plus once-per-stream ShardKeyMissingWarning at N>1 and BEAVA_ENTITIES_SHARDS soft-deprecation.

## What Was Built

### Task 1: Tuple shard_key missing-field rejection at ingest (D-10, TPC-CORR-03)

`check_shard_key_fields` (in `src/server/tcp.rs`) checks all fields declared in a stream's tuple `shard_key` against the event payload before `try_send`. Shard threads never see malformed events.

- **HTTP**: `map_err_to_response` returns HTTP 400 with `{"error":"shard_key_missing","missing":["field",...]}` for `BeavaError::ShardKeyMissing`
- **TCP**: new `STATUS_SHARD_KEY_MISSING = 0x12` constant in `protocol.rs`; specific match arm in `handle_connection` response dispatch encodes JSON body with the dedicated status byte (connection stays open, not torn down)
- **Metric**: `record_shard_key_missing()` increments `beava_events_dropped_total{reason="shard_key_missing"}` on each rejection
- **BeavaError**: `ShardKeyMissing { missing: Vec<String> }` variant carries the list of absent fields

### Task 2: ShardKeyMissingWarning (D-11/D-12) + BEAVA_ENTITIES_SHARDS deprecation (D-13)

`emit_shard_key_missing_warning(registry, stream_name, shard_count)` in `src/server/signals.rs`:
- Returns immediately if `shard_count <= 1` (D-12: silent at N=1)
- Emits a `Signal` with stable id `"shard_key_missing:{stream_name}"` for dedup into `SharedRegistry`
- Severity: Warning; Category: Operational

Wired at all three stream registration call sites:
- `src/server/http.rs` `create_pipeline` — after successful non-view stream registration
- `src/server/tcp.rs` OP_REGISTER v0 path — after `engine.register(stream_def)?`
- `src/server/tcp.rs` OP_REGISTER v2 path — after `engine.register(stream_def)?`

`BEAVA_ENTITIES_SHARDS` warn-once: `StateStore::default()` checks env var and emits `tracing::warn!` if set (D-13). Server continues normally.

## Test Coverage

- `server::signals::shard_key_warning_tests::warning_silent_at_n1` — D-12: no warning at N=1
- `server::signals::shard_key_warning_tests::warning_fires_at_n2` — D-11: warning fires at N=2
- `server::signals::shard_key_warning_tests::warning_deduped_on_second_call` — once-per-stream dedup
- All 3 pass; `cargo check` clean (1 pre-existing dead_code warning in shard/metrics.rs)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] signals.rs used instead of debug/warnings.rs**
- **Found during:** Task 2
- **Issue:** Plan referenced `src/debug/warnings.rs` which does not exist; `src/server/signals.rs` is the actual `/debug/warnings` backing registry
- **Fix:** Implemented `emit_shard_key_missing_warning` in `src/server/signals.rs` following the existing `emit_register_failure` pattern using `SharedRegistry`
- **Files modified:** src/server/signals.rs
- **Commit:** 0addd88

**2. [Rule 1 - Bug] STATUS_SHARD_KEY_MISSING 0x12 reuses existing OP code value**
- **Found during:** Task 1
- **Issue:** Plan spec says TCP error code 0x12 with note "Claude's discretion"; `OP_SNAPSHOT_FETCH = 0x12` already exists in the OP code namespace
- **Fix:** Defined `STATUS_SHARD_KEY_MISSING = 0x12` in the STATUS namespace (disjoint from OP namespace in the wire protocol); clients read the status byte from response frames, not OP codes
- **Files modified:** src/server/protocol.rs
- **Commit:** 0addd88

## Known Stubs

None — all shard_key validation is fully wired.

## Self-Check: PASSED

- src/error.rs: BeavaError::ShardKeyMissing variant — FOUND
- src/server/protocol.rs: STATUS_SHARD_KEY_MISSING = 0x12 — FOUND
- src/server/signals.rs: emit_shard_key_missing_warning — FOUND
- src/server/tcp.rs: STATUS_SHARD_KEY_MISSING response arm — FOUND
- src/server/http.rs: shard_key_missing warning wiring — FOUND
- Commit 0addd88 — FOUND
