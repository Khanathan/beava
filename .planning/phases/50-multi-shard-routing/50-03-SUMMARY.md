---
phase: 50-multi-shard-routing
plan: "03"
subsystem: shard-thread-lifecycle
tags: [tpc, shard-threads, spawn-at-boot, ready-barrier, catch-unwind, core-affinity]
dependency_graph:
  requires: [50-01]
  provides: [spawn_shard_threads, ShardHandle, ShardEvent, shard_event_loop]
  affects: [src/shard/thread.rs, src/shard/mod.rs, src/server/tcp.rs]
tech_stack:
  added: []
  patterns: [WaitGroup ready-barrier, catch_unwind quarantine, core_affinity pinning]
key_files:
  created:
    - src/shard/thread.rs
  modified:
    - src/shard/mod.rs
    - src/server/tcp.rs
decisions:
  - "WaitGroup (crossbeam_utils::sync::WaitGroup) used for ready-barrier — each shard drops clone when ready; spawn_shard_threads blocks on wg.wait()"
  - "catch_unwind around entire shard_event_loop; on panic sets is_down=true + records shard_down metric; no auto-restart"
  - "core_affinity: warn-once eprintln on failure (D-14); never fatal"
  - "shard_handles starts as Vec::new() in ConcurrentAppState; populated by run_tcp_server after ready-barrier"
metrics:
  duration_minutes: 25
  completed: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  files_modified: 3
---

# Phase 50 Plan 03: Shard Thread Lifecycle (D-01/D-02/D-14) Summary

One-liner: N pinned shard threads spawned at boot with WaitGroup ready-barrier before listener bind; catch_unwind quarantine marks panicked shards DOWN; core_affinity best-effort pinning.

## What Was Built

`src/shard/thread.rs`:
- `ShardEvent { payload: Bytes, stream_name: Arc<str>, shard_hint: u32, response_tx: Option<oneshot::Sender<ShardResult>> }`
- `ShardHandle { shard_index, is_down: Arc<AtomicBool>, inbox_tx: Sender<ShardEvent> }`
- `spawn_shard_threads(shard_count, inbox_size)`: WaitGroup ready-barrier, pin_to_core, catch_unwind
- `shard_event_loop`: tokio current_thread runtime per shard; gauge update every 1000 events or 100ms
- `inbox_size_from_env()`: reads BEAVA_SHARD_INBOX_SIZE, clamps 1024..=1_000_000
- 5 unit tests: spawn, all-not-down, ready-barrier timing, backpressure property, inbox-size env

`src/server/tcp.rs`:
- `shard_handles: parking_lot::RwLock<Vec<ShardHandle>>` added to `ConcurrentAppState`
- `run_tcp_server`: reads shard_count, calls spawn_shard_threads, stores handles

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED
