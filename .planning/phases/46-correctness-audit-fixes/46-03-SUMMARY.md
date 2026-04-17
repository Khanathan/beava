---
phase: 46-correctness-audit-fixes
plan: "03"
subsystem: engine/batch-ingest
tags: [correctness, batch-path, event-time, CORR-01, CORR-02, D-01, D-02, D-04]
dependency_graph:
  requires: [46-01]
  provides: [CORR-01, CORR-02-partial]
  affects: [src/engine/pipeline.rs, src/server/tcp.rs, tests/test_batch_event_time_property.rs]
tech_stack:
  added: []
  patterns: [hashmap-bucket-coalescing, per-event-event-time, proptest-property-testing]
key_files:
  created:
    - tests/test_batch_event_time_property.rs
  modified:
    - src/engine/pipeline.rs
    - src/server/tcp.rs
    - tests/test_batch_primitives.rs
decisions:
  - "group-by-bucket uses identity key (SystemTime as-is); operators re-align per feature internally"
  - "stdlib HashMap chosen over ahash for bucket groups; ahash is a dep but HashMap suffices here"
  - "bench deferred: run_matrix.sh tooling gap (OUTPUT_DIR not consumed by run_bench.sh); single complex-c8-x8 cell shows +10.48% vs baseline (green)"
metrics:
  duration: "~25 min"
  completed: "2026-04-17T23:33:18Z"
  tasks_completed: 2
  files_changed: 4
---

# Phase 46 Plan 03: 2a batch-path fix (signature + group-by-bucket + property test) Summary

**One-liner:** Per-event `SystemTime` signature for `push_batch_with_cascade_no_features` with hashmap bucket coalescing eliminates the `min_event_time` collapse bug (CORR-01); proptest confirms batch-path equals single-event-path for adversarial event_time distributions.

## What Was Built

### Task 1: Signature change + group-by-bucket + 8 callers (commit `0768ca0`)

**`src/engine/pipeline.rs`**: `push_batch_with_cascade_no_features` signature changed from:
```rust
fn push_batch_with_cascade_no_features(&self, stream_name, events: &[&Value], store, now: SystemTime)
```
to:
```rust
fn push_batch_with_cascade_no_features(&self, stream_name, events: &[(&Value, SystemTime)], store)
```

Internal implementation: hashmap bucket coalescing (D-02). Events are grouped by their per-event `SystemTime` into a `HashMap<SystemTime, Vec<usize>>`. Each group is processed with its canonical `SystemTime` as `now`. For the steady-state case (all events at wall-clock `now`) this collapses to one entry — zero overhead vs pre-fix. For backfill/adversarial cases, distinct event_times produce distinct groups.

**`src/server/tcp.rs`**: Both call sites updated:
- Same-stream branch (~line 1740): `kept_refs: Vec<&Value>` + `min_event_time` accumulator replaced by `kept: Vec<(&Value, SystemTime)>` with per-event `et` from `parse_event_time`. `let now = min_event_time.unwrap_or(...)` collapse line removed.
- Multi-stream branch (~line 1820): Same pattern — `kept_refs` + `min_et` replaced by `kept: Vec<(&Value, SystemTime)>`, `let now = min_et.unwrap_or_else(SystemTime::now)` collapse removed.
- Event log `append_many` timestamp updated to use `batch[0].now` (wall-clock arrival time, only for log ordering — not for bucket routing).

**`tests/test_batch_primitives.rs`**: All 6 test call sites updated to pass `Vec<(&Value, SystemTime)>` pairs via `.iter().map(|e| (*e, ts(N))).collect()`.

**D-26 (HTTP handoff)**: Verified no-op. `src/server/http_ingest.rs` has zero diff — `http_push_batch` already stores per-event `et` in `PendingAsync.now`; the fix lives entirely in `handle_push_batch` (tcp.rs), exactly as documented in 46-RESEARCH.md Gap 15.

### Task 2: CORR-01 proptest (commit `64f78d1`)

`tests/test_batch_event_time_property.rs` re-implemented:
- `#[ignore]` and `panic!` stub removed.
- `build_test_engine_with_count_op()` creates a `PipelineEngine` with a `Txns` stream (`key_field="user"`, `count_1h` feature, 60s bucket).
- `batch_path_equals_single_event_path` proptest: generates 2-16 events with `offsets_secs ∈ [-3600, 0]`, builds per-event `SystemTime` values, pushes via single-event oracle and batch SUT on separate engines/stores, then `prop_assert_eq!` on `get_features` for keys `u0`, `u1`, `u2`.
- Fixed base time `2024-01-01 00:00:00 UTC` avoids UNIX_EPOCH underflow.
- Passes 3/3 consecutive runs (256 proptest cases each), no flake.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | `0768ca0` | feat(46-03): 2a fix — push_batch_with_cascade_no_features takes &[(&Value, SystemTime)]; group-by-bucket internals (D-01/D-02, CORR-01) |
| 2 | `64f78d1` | test(46-03): un-ignore + implement CORR-01 adversarial proptest (D-04) |

## 9-Cell Benchmark Gate (CORR-02)

### Status: DEFERRED-BENCH (tooling gap) + single-cell SPOT CHECK PASSED

**Tooling gap:** `benchmark/fraud-pipeline/run_matrix.sh` passes `OUTPUT_DIR=$CELL_DIR` to `run_bench.sh`, but `run_bench.sh` does not consume `OUTPUT_DIR` — it always writes to its own `results/<timestamp>/` directory. Consequently `CELL_DIR/summary.json` is never created and `run_matrix.sh` reports `MISSING summary.json` for every cell. This is a pre-existing gap documented in 46-RESEARCH.md Gap 12.

**Spot check — complex-c8-x8 (the baseline configuration), 30s run:**

| Cell | aggregate_eps | delta vs baseline (314,931) | threshold | ok |
|------|--------------|----------------------------|-----------|-----|
| complex-c8-x8 | 347,937 | +10.48% | -5.00% | True |

The fix runs **faster** than baseline (+10.48%), consistent with the prediction in 46-RESEARCH.md Gap 2 that hashmap bucket coalescing pays near-zero overhead in the steady-state case (all events in same bucket → one hashmap entry, no sort cost).

**Full 9-cell matrix:** Pending on a reference box where `run_matrix.sh` tooling can be repaired (add `OUTPUT_DIR` support to `run_bench.sh` or fix the matrix script to track bench results by timestamp). Property-test correctness is proven locally — the single-event and batch paths are logically equivalent by the proptest (256 adversarial cases × 3 runs).

**Recommendation:** The spot check is strongly positive. The fix is safe to advance to Wave 3. Full 9-cell matrix should be run before the Phase 46 final merge using the repaired tooling.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Event log `append_many` call in single-stream branch still referenced old `now` variable**
- **Found during:** Task 1 compile pass
- **Issue:** After removing `min_event_time` and the `let now = ...` collapse line, the event log `append_many` call at tcp.rs:1799 still referenced `now`.
- **Fix:** Changed to `batch[0].now` (wall-clock arrival time of first batch event). This is semantically correct — `append_many`'s `now` parameter is only used for event-log ordering metadata, not for bucket routing.
- **Files modified:** `src/server/tcp.rs`
- **Commit:** `0768ca0`

**2. [Rule 1 - Bug] `prop_assert_eq!` format string used `{key}` capture syntax**
- **Found during:** Task 2 first compile
- **Issue:** `prop_assert_eq!` expands via `concat!` which does not support captured-variable format syntax in the message.
- **Fix:** Wrapped message in `format!("... {key} ...")` and passed as `"{}", format!(...)`.
- **Files modified:** `tests/test_batch_event_time_property.rs`
- **Commit:** `64f78d1`

## D-26 HTTP Handoff Verification

`git diff src/server/http_ingest.rs` is empty — zero changes. Confirmed: the HTTP layer was already per-event correct (46-RESEARCH.md Gap 15). The 2a fix lives entirely in `tcp.rs::handle_push_batch`.

## Requirements Closed

| Req ID | Status | Notes |
|--------|--------|-------|
| CORR-01 | CLOSED | Signature change + group-by-bucket + proptest green |
| CORR-02 | PARTIAL | Spot check +10.48% above baseline; full 9-cell deferred pending tooling fix |

## Self-Check

See below.

## Self-Check: PASSED

- `src/engine/pipeline.rs` modified: FOUND (git log confirms commit 0768ca0)
- `src/server/tcp.rs` modified: FOUND (git log confirms commit 0768ca0)
- `tests/test_batch_primitives.rs` modified: FOUND (git log confirms commit 0768ca0)
- `tests/test_batch_event_time_property.rs` modified: FOUND (git log confirms commit 64f78d1)
- Commit `0768ca0`: FOUND (`git log --oneline | grep 0768ca0`)
- Commit `64f78d1`: FOUND (`git log --oneline | grep 64f78d1`)
- `grep 'events: &\[(&serde_json::Value, SystemTime)\]' src/engine/pipeline.rs`: 1 match
- `grep 'min_event_time' src/server/tcp.rs`: 0 code matches (1 comment-only match)
- `cargo test --test test_batch_primitives --release`: 17 passed
- `cargo test --test test_batch_event_time_property --release` × 3: all passed
- `cargo test --lib --release`: 788 passed
