---
phase: 49-per-shard-state-store
plan: "05"
subsystem: shard-integration
tags: [tpc, wave-1, shard, state-store, push-path]
dependency_graph:
  requires: [49-02, 49-03, 49-04]
  provides: [shard-0-live-data-path-at-n1]
  affects: [src/engine/pipeline.rs, src/server/tcp.rs, src/main.rs, src/shard/store.rs]
tech_stack:
  added: []
  patterns: [shadow-write, arc-mutex-shared-state, wave1-n1-always]
key_files:
  created: []
  modified:
    - src/engine/pipeline.rs
    - src/server/tcp.rs
    - src/main.rs
    - src/shard/store.rs
    - tests/test_config_recommendations.rs
    - tests/test_ttl_defaults.rs
    - tests/http_common.rs
    - tests/bench_replica_ingest_raw.rs
    - tests/test_fork_watermark_propagation.rs
    - tests/test_replica_snapshot_fetch.rs
    - tests/test_warnings_dedupe.rs
    - tests/test_http_metrics.rs
    - tests/test_public_http.rs
    - tests/test_demo_page.rs
    - tests/test_replica_log_fetch.rs
    - tests/profile_ingest.rs
    - tests/test_http_read.rs
    - tests/test_warnings_feed.rs
    - tests/test_replica_batch.rs
    - tests/test_admin_auth.rs
    - tests/test_warnings_integration.rs
    - tests/test_debug_warnings_endpoint.rs
    - tests/test_replica_subscribe.rs
decisions:
  - "Shadow write (not primary) — StateStore DashMap remains authoritative read path; Shard-0 participates in writes at N=1"
  - "Arc<Mutex<ShardedStateStoreV1>> in ConcurrentAppState so async HTTP handlers can share it without lifetime issues"
  - "n_shards resolved inside async_main (not just fn main) so it is in scope at make_concurrent_state_full call site"
metrics:
  duration_minutes: 45
  completed_date: "2026-04-18"
  tasks_completed: 2
  files_changed: 23
---

# Phase 49 Plan 05: Wire ShardedStateStoreV1 into Push Path Summary

ShardedStateStoreV1 wired at N=1 as a shadow write alongside the existing DashMap StateStore. Shard-0 state, dirty_set, and watermark are populated on every push. Full test suite green.

## Tasks Completed

| Task | Description | Commit |
|------|-------------|--------|
| 1 | Add sharded_store to PipelineEngine + ConcurrentAppState; update make_concurrent_state_full | af60cba |
| 2 | Shadow write in handle_push_core_ex; fix SourceDescriptor test compilations | 8a4ff49 |

## Key Changes

- `PipelineEngine.sharded_store: ShardedStateStoreV1` — initialized at `new()` with N=1; `with_shards(n)` constructor added
- `ConcurrentAppState.sharded_store: Arc<Mutex<ShardedStateStoreV1>>` — wired alongside DashMap `store` compat shim
- `make_concurrent_state_full` — gained `n_shards: u16` parameter; 13 test call sites + legacy delegate updated to pass `1`
- `handle_push_core_ex` — shadow write block acquires shard lock, populates `Shard.state.entry()`, `dirty_set`, and `watermark.observe()` after StateStore write; no `.await` inside guard (T-49-05-01 deadlock mitigation)
- `ShardedStateStoreV1` — added manual `Debug` impl (required by `#[derive(Debug)]` on `PipelineEngine`)

## Test Results

`cargo test`: **0 failures** across all test files.

Note: `hll_mode_within_2_percent_on_100k` is a pre-existing probabilistic HLL flake — passed on re-run, unrelated to plan changes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Missing Debug impl on ShardedStateStoreV1**
- Found during: Task 1 cargo check
- Issue: PipelineEngine derives Debug but ShardedStateStoreV1 had no Debug impl
- Fix: Added manual `impl std::fmt::Debug` that formats shard_count
- Files modified: src/shard/store.rs
- Commit: af60cba

**2. [Rule 1 - Bug] SourceDescriptor missing shard_key field in test initializers**
- Found during: Task 2 full cargo test
- Issue: Phase 49-04 added shard_key field to SourceDescriptor; two test files had non-exhaustive struct initializers
- Fix: Added `shard_key: None` to all SourceDescriptor initializers in test_config_recommendations.rs and test_ttl_defaults.rs
- Files modified: tests/test_config_recommendations.rs, tests/test_ttl_defaults.rs
- Commit: 8a4ff49

**3. [Rule 1 - Bug] n_shards out of scope in async_main**
- Found during: Task 1 cargo check on bin target
- Issue: n_shards was defined in fn main() but make_concurrent_state_full is called inside async_main() — different scope
- Fix: Added n_shards resolution inside async_main() using env var + CLI arg parsing
- Files modified: src/main.rs
- Commit: af60cba

## Self-Check: PASSED

- af60cba: verified via `git log --oneline`
- 8a4ff49: verified via `git log --oneline`
- `PipelineEngine.sharded_store` field: present at src/engine/pipeline.rs:460
- `ConcurrentAppState.sharded_store` field: present at src/server/tcp.rs:239
- `shard_for_event` in push path: present in handle_push_core_ex
- DashMap still in src/state/store.rs: confirmed
