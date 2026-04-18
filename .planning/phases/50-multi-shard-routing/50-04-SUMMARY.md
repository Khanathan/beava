---
phase: 50-multi-shard-routing
plan: "04"
subsystem: shard-routing
tags: [tpc, spsc-routing, try_send, shard_hint, inbox-backpressure]
dependency_graph:
  requires: [50-03]
  provides: [handle_push_core_ex_routing, check_shard_key_fields, SPSC_try_send]
  affects: [src/server/tcp.rs, src/server/http_ingest.rs]
tech_stack:
  added: []
  patterns: [fire-and-forget try_send, shard_index = shard_hint % N modulo routing]
key_files:
  created: []
  modified:
    - src/server/tcp.rs
    - src/server/http_ingest.rs
    - src/error.rs
decisions:
  - "Fire-and-forget try_send alongside legacy sync path (Wave 2 transition) — not full async routing which would require massive architectural changes"
  - "record_shard_event with real shard_index called in handle_push_core_ex"
  - "on try_send Err (inbox full): record_inbox_full, continue legacy processing"
  - "BeavaError::ShardKeyMissing added to error.rs"
metrics:
  duration_minutes: 30
  completed: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  files_modified: 3
---

# Phase 50 Plan 04: SPSC Routing + Backpressure (D-08, TPC-CORR-01) Summary

One-liner: SPSC fire-and-forget try_send to shard inbox with shard_index = shard_hint % N; inbox-full drops recorded via record_inbox_full; legacy sync path preserved for correctness.

## What Was Built

`src/server/tcp.rs handle_push_core_ex`:
- Computes `shard_hint` from key_field via `shard_hint_for_event`
- Computes `shard_index = shard_hint % handles.len().max(1)`
- Checks `is_down` flag before try_send (DOWN shards: record metric, skip send)
- `try_send(ShardEvent)` fire-and-forget; on Err: `record_inbox_full(shard_index)`
- `check_shard_key_fields` public helper checks tuple shard_key fields presence

`src/server/http_ingest.rs`:
- Removed duplicate `record_shard_event(0, ...)` call (now in handle_push_core_ex)
- Added `ShardKeyMissing` arm to `map_err_to_response` → HTTP 400

`src/error.rs`:
- `BeavaError::ShardKeyMissing { missing: Vec<String> }` variant

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED
