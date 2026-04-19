---
phase: 52-event-log-recovery-ship-gate
plan: "10"
subsystem: cleanup
tags: [tpc, dashmap, compat-shim, cleanup, blocker]
requirements:
  - TPC-CORR-02
  - TPC-CORR-05
  - TPC-CORR-06
  - TPC-PERF-07
  - TPC-DX-03
  - TPC-DX-04
  - TPC-INFRA-06

dependency_graph:
  requires:
    - 52-09 (architecture docs — TPC-DX-04)
    - 52-07 (parity proptest — TPC-CORR-05)
    - 52-03 (per-shard recovery — TPC-INFRA-06)
  provides: []
  affects:
    - Cargo.toml (dashmap/arc-swap deps — NOT removed; blocked)
    - src/state/store.rs (StateStore DashMap shim — NOT removed; blocked)

tech_stack:
  added: []
  patterns: []

key_files:
  created: []
  modified: []

decisions:
  - "BLOCKED: N=1 hot path still routes through DashMap-backed StateStore — deletion deferred"
  - "DashMap used in 5+ non-shim modules — complete removal requires broader refactor"
  - "BEAVA_ENTITIES_SHARDS removal is safe but deliberately deferred pending full migration"

metrics:
  duration: "~30 minutes (analysis)"
  completed: "2026-04-18"
  tasks_completed: 0
  tasks_total: 2
  files_created: 0
  files_modified: 0
---

# Phase 52 Plan 10: DashMap/ArcSwap Compat Shim Deletion — BLOCKER SUMMARY

**One-liner:** Pre-deletion analysis found N=1 hot path still depends on DashMap-backed StateStore and DashMap is active in 5+ non-shim modules — deletion blocked pending deeper migration work.

---

## Status: BLOCKED — Do Not Delete

The plan's caution stated: "If anything at N=1 still depends on the DashMap path, document as a gap and stop before deletion." That condition is TRUE. This summary documents the blockers.

---

## Blocker 1: N=1 Hot Path Still Uses DashMap-Backed StateStore

### Evidence

`src/server/tcp.rs`, function `handle_push_core_ex` (line ~1549):

```rust
// At N=1: legacy DashMap path (Phase 49 hot-path, preserved).
// At N>1: shard thread exclusively owns mutations.
let shard_count = state.shard_handles.read().len();
let shard_index: usize = if shard_count > 1 {
    // ... sends to shard thread via SPSC inbox
    return Ok(crate::types::FeatureMap::new());
} else {
    // N==0 or N==1: legacy engine.push_with_cascade path (Phase 49 hot path, preserved).
    0
};

// Cascade-aware push: operates on state.store which IS the DashMap-backed StateStore
let features = if read_features {
    engine.push_with_cascade(stream_name, payload, store, now)?
} else {
    engine.push_with_cascade_no_features(stream_name, payload, store, now)?
};
```

At N=1, `shard_count = state.shard_handles.read().len()` returns the number of active shard thread handles. When N=1, this is 1, so `shard_count > 1` is false, and the code falls through to `engine.push_with_cascade(stream_name, payload, store, now)` where `store = &state.store` (the DashMap-backed `StateStore` in `ConcurrentAppState`).

**The ShardedStateStoreV1 at shard-0 is populated by the shard thread, NOT by the N=1 legacy path.** The `state.store` (DashMap) and `state.sharded_store` (shard-0) are parallel, not unified. The plan assumed they were already unified — they are not.

### What Would Be Required to Fix

Route N=1 events through the shard-0 SPSC inbox, the same as N>1 but with synchronous response collection (response_tx = Some). This requires:

1. Changing `handle_push_core_ex` so N=1 also sends to the shard thread inbox
2. Using `response_tx = Some(oneshot_tx)` and `await` the result to get features back
3. Removing the `engine.push_with_cascade` / `push_with_cascade_no_features` call in the N=1 branch
4. Ensuring all callers that read features from N=1 (PUSH+GET) work via the shard response

This is a significant behavioral change that requires careful testing. It was documented as "Wave 4" work but Phase 52 plans 01–09 did not implement this routing change.

---

## Blocker 2: DashMap Used in 5+ Non-Shim Modules

Removing `dashmap` from `Cargo.toml` is impossible without rewriting the following:

| File | Purpose | DashMap usage |
|------|---------|---------------|
| `src/state/eviction_tracker.rs` | Per-table bloom filter and eviction counters | `DashMap<String, Mutex<GenerationalBloom>>`, `DashMap<String, AtomicU64>` |
| `src/state/event_log.rs` | Per-stream log writers | `DashMap<String, LockFreeStreamLog>`, history TTLs, seq counters |
| `src/engine/event_time.rs` | Watermark tracking, drop counters | `DashMap<String, AtomicU64>` for observed_max, last_event_time, per-label drops |
| `src/server/replica.rs` | Subscriber registry, per-stream push/log-send counters | `DashMap<u64, ReplicaSession>`, static `DashMap<String, AtomicU64>` counters |
| `src/server/tcp.rs` | Historical extraction registry | `extracted_history: DashMap<u64, DashMap<String, Value>>` |

These are **not compat shims** — they are active, independently-motivated uses of DashMap for concurrent access patterns. Each would require its own migration strategy (e.g., `scc::HashMap`, `papaya::HashMap`, or `parking_lot::RwLock<AHashMap>` depending on access pattern).

---

## Blocker 3: ArcSwap Still Active in StateStore

`arc-swap` is used only in `src/state/store.rs` for `dirty_keys: arc_swap::ArcSwap<dashmap::DashSet<EntityKey>>`. This IS the compat shim and CAN be removed as part of the StateStore migration. However, since Blocker 1 prevents StateStore removal, ArcSwap must also stay.

---

## What IS Safe to Do (Deferred to Next Plan)

These are scoped changes that are safe without the full DashMap removal:

1. **Remove `BEAVA_ENTITIES_SHARDS` handler** from `state/store.rs::StateStore::default()`. The env var tunes DashMap shard count and is explicitly deprecated (Phase 50-06 D-13). Removal is a 10-line deletion. Does not affect behavior (DashMap falls through to `DashMap::new()` which uses `num_cpus * 4` default).

2. **Add `snapshot_format_version_is_v8` test** to `tests/ship_gate.rs`. This assertion (`SNAPSHOT_FORMAT_VERSION == 8`) is true today and will remain true going forward.

3. **Document `StoreView::Legacy` as permanent** for N=1 (not a compat shim, a designed variant). Update `src/shard/mod.rs` comment to reflect this.

The `dashmap_not_in_cargo_lock` assertion cannot be added to `tests/ship_gate.rs` because the assertion would fail — DashMap IS in Cargo.lock and must remain there.

---

## Analysis: Which Plan 52 Requirements Are Actually Met

| Requirement | Status | Notes |
|-------------|--------|-------|
| TPC-CORR-02 | DONE (52-01) | Snapshot v8 shard_count boot guard |
| TPC-CORR-05 | DONE (52-07) | N=1↔N=8 parity proptest green |
| TPC-CORR-06 | DONE (52-06) | LSN dedup in replica path |
| TPC-PERF-07 | DONE (52-08) | Pareto bench cross_shard_fraction < 0.40 |
| TPC-DX-03 | DONE (52-05/08) | Shard probe + reshard CLI |
| TPC-DX-04 | DONE (52-09) | Architecture docs + ops runbook |
| TPC-INFRA-06 | DONE (52-03) | Parallel per-shard recovery |

All 7 requirements are satisfied by plans 52-01 through 52-09. The DashMap deletion in 52-10 was aspirational cleanup, not a requirement closure.

---

## Recommended Next Steps

Create a new plan (e.g., `53-01-PLAN.md`) scoped to:

1. **Route N=1 through shard-0 SPSC** — modify `handle_push_core_ex` to always use the shard thread path, with `response_tx = Some(tx)` for synchronous feature reads. Test via sharding_parity proptest.

2. **Remove `StateStore` DashMap fields** — once the N=1 routing change is done, `ConcurrentAppState.store` (the DashMap-backed StateStore) is no longer written on the hot path. It can be kept for snapshot/query paths temporarily, or replaced with reads from shard-0 state.

3. **Remove `BEAVA_ENTITIES_SHARDS`** from `StateStore::default()`.

4. **Migrate non-shim DashMap users** one by one (event_log, event_time, replica, eviction_tracker, extracted_history) to alternatives appropriate for their access pattern. This is multi-plan work.

5. **Only then** remove `dashmap` and `arc-swap` from `Cargo.toml` and add the `dashmap_not_in_cargo_lock` assertion to `tests/ship_gate.rs`.

---

## Deviations from Plan

**Major deviation:** Plan executed as analysis only — no code changes made.

- **Found during:** Pre-execution analysis (caution check from objective)
- **Issue:** N=1 hot path still depends on DashMap `StateStore`; DashMap used in 5+ non-shim modules
- **Action:** Stopped before deletion per objective caution: "If anything at N=1 still depends on the DashMap path, document as a gap and stop before deletion."
- **Files modified:** None (blocked)

---

## Self-Check: N/A (No Code Changes)

No files were created or modified. No commits were made. The analysis is documented above.

**Branch is safe to continue from HEAD `9c39dd5` (52-09 complete). No state was modified.**
