---
phase: 04-persistence-and-operational-readiness
fixed_at: 2026-04-09T00:00:00Z
review_path: .planning/phases/04-persistence-and-operational-readiness/04-REVIEW.md
iteration: 1
findings_in_scope: 5
fixed: 4
skipped: 1
status: partial
---

# Phase 4: Code Review Fix Report

**Fixed at:** 2026-04-09T00:00:00Z
**Source review:** .planning/phases/04-persistence-and-operational-readiness/04-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 5
- Fixed: 4
- Skipped: 1

## Fixed Issues

### CR-01: Manual Snapshot Endpoint Races with Periodic Snapshot on Path

**Files modified:** `src/server/tcp.rs`, `src/server/http.rs`, `src/main.rs`, `tests/test_server.rs`
**Commit:** fa44aaa
**Applied fix:** Added `snapshot_path: std::path::PathBuf` field to `AppState` struct, ensuring a single source of truth for the snapshot file path. The manual snapshot trigger endpoint (`POST /snapshot`) now reads the path from shared state instead of re-reading `TALLY_SNAPSHOT_PATH` from the environment at request time. Updated `main.rs` to pass `snapshot_path` into `AppState` at startup, and updated all test helpers that construct `AppState` to include the new field.

### WR-01: Operator Reconciliation Silently Drops State on Feature Addition

**Files modified:** `src/engine/pipeline.rs`
**Commit:** b38c78c
**Applied fix:** Replaced count-based operator reconciliation with name-based reconciliation. The new logic compares operator names between the existing entity state and the expected stream definition, only rebuilding operators when names don't match (not just when counts differ). This preserves accumulated windowed state for unchanged operators when a new feature is added to a running stream.

### WR-02: Eviction Boundary Condition -- Entities at Exactly TTL Age Are Evicted

**Files modified:** `src/state/store.rs`, `tests/test_snapshot.rs`
**Commit:** 368ff31
**Applied fix:** Changed the eviction comparison in `remove_expired_entities` from strict less-than (`< ttl`) to less-than-or-equal (`<= ttl`), so entities exactly at the TTL boundary are kept rather than evicted. Updated the integration test in `tests/test_snapshot.rs` to verify the boundary behavior: added a `boundary_user` at exactly TTL age that is now kept, and adjusted `old_user` to be strictly older than TTL.

### WR-03: `save_snapshot` Panics on Serialization Failure

**Files modified:** `src/state/snapshot.rs`, `src/main.rs`, `src/server/http.rs`, `tests/test_snapshot.rs`
**Commit:** e47eae8
**Applied fix:** Changed `save_snapshot` return type from `Vec<u8>` to `Result<Vec<u8>, postcard::Error>`, replacing the `.expect()` panic with `?` propagation. Updated all callers: `main.rs` periodic snapshot maps the error to `std::io::Error` for the existing `Ok::<usize, std::io::Error>` return type; `http.rs` manual snapshot does the same; all test calls use `.expect()` which is appropriate for test contexts.

## Skipped Issues

### WR-04: CountOp `read()` Returns Missing for Zero Count but Could Be Valid

**File:** `src/engine/operators.rs:51-56`
**Reason:** Design decision, not a code defect. The reviewer acknowledged this behavior is documented in CONTEXT.md and may be intentional. The fix suggestion is to either document prominently or change semantics -- both require a product-level decision about whether `Missing` vs `Int(0)` is the correct semantic for an expired window. Skipping as this requires human judgment on the intended behavior.
**Original issue:** `CountOp::read()` returns `FeatureValue::Missing` when total count is 0, making it impossible to distinguish "no events in window" from "feature not computed yet" in derive expressions.

---

_Fixed: 2026-04-09T00:00:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
