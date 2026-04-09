---
phase: 04-persistence-and-operational-readiness
reviewed: 2026-04-09T00:00:00Z
depth: standard
files_reviewed: 11
files_reviewed_list:
  - Cargo.toml
  - src/engine/pipeline.rs
  - src/main.rs
  - src/server/http.rs
  - src/server/tcp.rs
  - src/state/eviction.rs
  - src/state/mod.rs
  - src/state/snapshot.rs
  - src/state/store.rs
  - tests/test_server.rs
  - tests/test_snapshot.rs
findings:
  critical: 1
  warning: 4
  info: 3
  total: 8
status: issues_found
---

# Phase 4: Code Review Report

**Reviewed:** 2026-04-09T00:00:00Z
**Depth:** standard
**Files Reviewed:** 11
**Status:** issues_found

## Summary

The Phase 4 implementation covers snapshot persistence (save/load with postcard + versioned format), TTL-based key eviction, HTTP management API (CRUD pipelines, metrics, debug, manual snapshot), and MSET cooperative yielding. The code is generally well-structured with clear separation of concerns, good error handling patterns (poisoned mutex recovery, atomic rename for snapshots), and thorough test coverage.

Key concerns: one potential data loss scenario in the snapshot trigger endpoint, several boundary/logic issues in operator reconciliation and eviction, and a memory leak in a test file.

## Critical Issues

### CR-01: Manual Snapshot Endpoint Races with Periodic Snapshot on Path

**File:** `src/server/http.rs:269-271`
**Issue:** The `trigger_snapshot` handler reads `TALLY_SNAPSHOT_PATH` from the environment at request time, independently from the periodic snapshot timer in `main.rs` which captures the path at startup. If the environment variable is modified between startup and the manual trigger (or if it was never set and there is a race between the periodic and manual snapshot), both writers could target the same file simultaneously via different tmp paths. More critically, if `TALLY_SNAPSHOT_PATH` is unset, both use `"tally.snapshot"` and the temp file pattern `tally.tmp`. Two concurrent `fs::write` to the same `tally.tmp` followed by two `fs::rename` from `tally.tmp` could cause a partially-written file to be renamed over a good snapshot, resulting in **data loss on crash recovery**.
**Fix:** Pass the snapshot path as shared state rather than re-reading the environment variable. This ensures a single source of truth and allows coordination.
```rust
// In AppState or a config struct shared via Arc:
pub struct AppState {
    pub engine: PipelineEngine,
    pub store: StateStore,
    pub metrics: Metrics,
    pub snapshot_path: std::path::PathBuf, // Add this
}

// In trigger_snapshot, use the shared path:
let path = {
    let app = state.lock().unwrap_or_else(|e| e.into_inner());
    app.snapshot_path.clone()
};
```

## Warnings

### WR-01: Operator Reconciliation Silently Drops State on Feature Addition

**File:** `src/engine/pipeline.rs:157-167`
**Issue:** When a stream is re-registered with a different number of operator features, the reconciliation logic clears all existing operators and rebuilds from scratch (`entity.live_operators.clear()`). This means if a user adds one new feature to an existing stream with 10 features and active entities, all 10 existing features lose their accumulated windowed state. The comment acknowledges this is a "WR-04 fix" but the cure may be worse than the disease for production use -- a user adding a single feature to a running system would silently lose all aggregation history.
**Fix:** Consider a name-based reconciliation that preserves existing operators whose definitions haven't changed:
```rust
// Build a map of expected operators from the definition
let expected: Vec<(String, FeatureDef)> = stream.features.iter()
    .filter(|(_, def)| !matches!(def, FeatureDef::Derive { .. }))
    .cloned()
    .collect();

// Only rebuild if names/types don't match (not just count)
let needs_rebuild = entity.live_operators.len() != expected.len()
    || entity.live_operators.iter().zip(expected.iter())
        .any(|((name, _), (exp_name, _))| name != exp_name);

if needs_rebuild {
    entity.live_operators.clear();
    // ... rebuild
}
```

### WR-02: Eviction Boundary Condition -- Entities at Exactly TTL Age Are Evicted

**File:** `src/state/store.rs:173`
**Issue:** The eviction comparison uses strict less-than (`< ttl`), which means an entity whose age is exactly equal to the TTL will be evicted. The test at `tests/test_snapshot.rs:160` confirms this: `old_user` at exactly 3600s age with TTL=3600s is evicted. While this is internally consistent, it is a boundary condition worth documenting. With `duration_since` and clock granularity, an entity that was active at exactly the TTL boundary could be evicted one tick early.
**Fix:** Consider using `<=` if the intent is "evict after TTL has fully elapsed," or document the current behavior as "evict at or after TTL":
```rust
// Change to <= if intent is "keep entities that are exactly at TTL"
now.duration_since(last).unwrap_or(std::time::Duration::ZERO) <= ttl
```

### WR-03: `save_snapshot` Panics on Serialization Failure

**File:** `src/state/snapshot.rs:78`
**Issue:** `save_snapshot` uses `.expect("snapshot serialization failed")` which will panic and crash the entire server if postcard serialization ever fails (e.g., due to a value that exceeds postcard's internal limits, or a future operator type that is not properly serializable). Since this runs on the blocking thread pool, the panic will be caught by `tokio::task::spawn_blocking` and surfaced as a `JoinError`, but the snapshot will silently fail. The periodic snapshot handler in `main.rs:137` does handle the `Err(e)` case from the join, so the server continues, but the panic is still undesirable.
**Fix:** Return a `Result` from `save_snapshot` instead of panicking:
```rust
pub fn save_snapshot(data: &SnapshotState) -> Result<Vec<u8>, postcard::Error> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION];
    buf.extend_from_slice(&postcard::to_stdvec(data)?);
    Ok(buf)
}
```

### WR-04: CountOp `read()` Returns Missing for Zero Count but Could Be Valid

**File:** `src/engine/operators.rs:51-56`
**Issue:** `CountOp::read()` returns `FeatureValue::Missing` when the total count is 0. This makes it impossible for a derive expression to distinguish "no events in window" from "feature not computed yet." After window expiration, a previously-active count feature silently becomes `Missing` rather than `Int(0)`. This matters for derive expressions like `failed_tx_30m / tx_count_30m` -- when `tx_count_30m` becomes `Missing` after window expiry, the derive evaluates differently than if it were `Int(0)`. This is a design decision documented in CONTEXT.md, so it may be intentional, but it creates a semantic gap for users expecting `0` after a quiet period.
**Fix:** If intentional, document this behavior prominently. If not, consider returning `Int(0)` when the operator has been initialized (has received at least one event historically) but the window is now empty, vs. `Missing` only when the operator has never received any events.

## Info

### IN-01: Memory Leak in Test via Box::leak

**File:** `tests/test_server.rs:303`
**Issue:** `Box::leak(format!("k{}", i).into_boxed_str())` intentionally leaks 2048 strings to satisfy the `&str` lifetime requirement of the test helper. While this only affects tests and the process exits after testing, it is unnecessary and represents poor practice.
**Fix:** Change `build_mset_payload` to accept owned strings, or collect the keys into a `Vec<String>` and borrow from it:
```rust
let keys: Vec<String> = (0..2048).map(|i| format!("k{}", i)).collect();
let entries: Vec<(&str, serde_json::Value)> = keys.iter()
    .enumerate()
    .map(|(i, k)| (k.as_str(), serde_json::json!({"score": i})))
    .collect();
```

### IN-02: Hardcoded Memory Estimate in Metrics

**File:** `src/server/http.rs:151`
**Issue:** `let memory_bytes = keys_total * 2048;` uses a hardcoded constant (2048 bytes per entity) to estimate memory usage. This estimate will become increasingly inaccurate as the number of features per entity varies, and there is no comment documenting why 2048 was chosen or that it is a rough estimate.
**Fix:** Add a doc comment explaining the estimate, or compute a more accurate figure by summing operator sizes:
```rust
// Rough estimate: ~2KB per entity assumes 10 features with small ring buffers.
// Actual usage depends on window/bucket ratios and number of features.
let memory_bytes = keys_total * 2048;
```

### IN-03: Snapshot Duration Metric Not Reset on Startup

**File:** `src/server/tcp.rs:25`
**Issue:** `snapshot_duration_ms` defaults to 0 and is only updated after the first snapshot write completes (30+ seconds after startup). During this window, `GET /metrics` reports `tally_snapshot_duration_seconds 0` which could be mistaken for "snapshots are instant" rather than "no snapshot has been taken yet." This is a minor observability gap.
**Fix:** Consider using an `Option<u64>` for the metric, or documenting that 0 means "no snapshot taken yet."

---

_Reviewed: 2026-04-09T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
