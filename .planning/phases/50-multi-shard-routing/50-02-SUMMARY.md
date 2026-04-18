---
phase: 50-multi-shard-routing
plan: "02"
subsystem: shard-metrics
tags: [tpc, metrics, per-shard, d07]
dependency_graph:
  requires: [50-01]
  provides: [shard_metrics_constants, register_shard_metrics, record_shard_event, update_shard_gauges]
  affects: [src/shard/metrics.rs, src/shard/mod.rs]
tech_stack:
  added: []
  patterns: [metrics! macro with shard label, pre-registration pattern for zero-value series]
key_files:
  created:
    - src/shard/metrics.rs
  modified:
    - src/shard/mod.rs
decisions:
  - "9 metric name constants as single source of truth (no magic strings at call sites)"
  - "register_shard_metrics() pre-touches all series with zero so they appear in /metrics before first event"
  - "CROSS_SHARD_FANOUT_TOTAL defined but not incremented until Wave 3"
metrics:
  duration_minutes: 20
  completed: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  files_modified: 2
---

# Phase 50 Plan 02: Per-Shard Metrics (D-07) Summary

One-liner: 9 metric series (7 per-shard labeled + 2 global) registered via metrics! macros with pre-registration at startup for zero-value baseline visibility.

## What Was Built

`src/shard/metrics.rs`:
- 9 `pub const` name strings for metric names
- `Outcome` enum (Accepted/Dropped) and `DropReason` enum (ShardKeyMissing/InboxFull/MalformedRouting)
- `register_shard_metrics(shard_count)`: pre-touches all series with zero values
- Hot-path helpers: `record_shard_event`, `record_inbox_full`, `record_shard_key_missing`, `record_shard_down`, `update_shard_gauges`
- 5 unit tests (all no-op without recorder — no global setup required)

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED
