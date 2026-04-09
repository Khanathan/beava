---
phase: 01-core-engine
fixed_at: 2026-04-09T00:00:00Z
review_path: .planning/phases/01-core-engine/01-REVIEW.md
iteration: 1
findings_in_scope: 6
fixed: 6
skipped: 0
status: all_fixed
---

# Phase 01: Code Review Fix Report

**Fixed at:** 2026-04-09T00:00:00Z
**Source review:** .planning/phases/01-core-engine/01-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 6
- Fixed: 6
- Skipped: 0

## Fixed Issues

### WR-01: SumOp::read uses count_nonzero to detect empty state but this is wrong for all-zero sums

**Files modified:** `src/engine/operators.rs`
**Commit:** 3a08a5a
**Applied fix:** Added `event_count: RingBuffer<u64>` field to `SumOp` (mirroring `AvgOp`'s approach). The count buffer is incremented on each push alongside the sum buffer. `SumOp::read` now uses `event_count.sum_all()` instead of `buffer.count_nonzero()` to detect whether any events were pushed. This correctly returns `Float(0.0)` when all pushed values are zero, rather than incorrectly returning `Missing`.

### WR-02: Integer overflow in CountOp::read casting u64 to i64

**Files modified:** `src/engine/operators.rs`
**Commit:** b01bd19
**Applied fix:** Replaced `total as i64` with `i64::try_from(total).unwrap_or(i64::MAX)` for a saturating cast. If the u64 total exceeds i64::MAX, the value saturates to i64::MAX instead of silently wrapping to a negative value.

### WR-03: Integer overflow in eval_binary for BinOp::Add/Sub/Mul on Int values

**Files modified:** `src/engine/expression.rs`
**Commit:** 9b91cd3
**Applied fix:** Replaced `a + b`, `a - b`, and `a * b` with `a.saturating_add(*b)`, `a.saturating_sub(*b)`, and `a.saturating_mul(*b)` respectively in the `Int + Int`, `Int - Int`, and `Int * Int` arms of `eval_binary`. This prevents panics in debug builds and silent wrapping in release builds.

### WR-04: PipelineEngine::push initializes operators only when live_operators is empty, silently ignoring stream re-registration

**Files modified:** `src/engine/pipeline.rs`
**Commit:** 7999898
**Applied fix:** Replaced the `is_empty()` guard with a count comparison: compute expected non-derive operator count from the stream definition and compare against `entity.live_operators.len()`. When counts differ (indicating a re-registration changed the feature set), operators are cleared and rebuilt from the current stream definition. This ensures new features get operators and removed features don't accumulate stale state.

### WR-05: StateStore::set_static uses SystemTime::now() instead of the injected now parameter

**Files modified:** `src/state/store.rs`, `tests/test_pipeline.rs`
**Commit:** d8cea22
**Applied fix:** Added `now: SystemTime` parameter to `set_static` signature and replaced `SystemTime::now()` with the injected `now` value. Updated all callers in `src/state/store.rs` tests (3 call sites) and `tests/test_pipeline.rs` (1 call site) to pass an explicit timestamp. This restores determinism and testability consistent with the rest of the codebase.

### WR-06: EntityState and StateStore don't implement Default

**Files modified:** `src/state/store.rs`
**Commit:** 6242674
**Applied fix:** Added `impl Default for EntityState` and `impl Default for StateStore`, each delegating to the same initialization logic previously in `new()`. Updated `new()` methods to call `Self::default()`. This prepares both types for serde deserialization composition in Phase 4 and follows Rust API guidelines (types with `new()` should also impl Default).

## Skipped Issues

None -- all findings were fixed.

---

_Fixed: 2026-04-09T00:00:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
