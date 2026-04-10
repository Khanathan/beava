---
phase: 09-incremental-snapshots
fixed_at: 2026-04-09T12:30:00Z
review_path: .planning/phases/09-incremental-snapshots/09-REVIEW.md
iteration: 2
findings_in_scope: 1
fixed: 1
skipped: 0
status: all_fixed
---

# Phase 9: Code Review Fix Report (Iteration 2)

**Fixed at:** 2026-04-09T12:30:00Z
**Source review:** .planning/phases/09-incremental-snapshots/09-REVIEW.md
**Iteration:** 2

**Summary:**
- Findings in scope: 1 (0 Critical, 1 Warning)
- Fixed: 1
- Skipped: 0

## Fixed Issues

### WR-01: Manual `/snapshot` path does not update `last_base_seq` or `previous_base_seq`

**Files modified:** `src/server/http.rs`
**Commit:** 3aaf69d
**Applied fix:** In `trigger_snapshot` (`src/server/http.rs:304-419`), added the missing `last_base_seq` / `previous_base_seq` bookkeeping inside the initial lock guard, immediately after `app.snapshot_seq += 1`. The manual path now mirrors the periodic timer pattern at `src/main.rs:289-293`:

```rust
let prev_base = app.last_base_seq;
app.previous_base_seq = prev_base;
app.last_base_seq = seq;
```

This keeps the periodic and manual snapshot paths symmetric, restoring the invariant that `app.last_base_seq` always names the newest base on disk. Consequences:

1. **Delta header correctness:** A subsequent periodic delta now stamps its header with `base_seq = <manual base seq>` rather than the stale pre-manual value. This preserves the debuggability/validation goal of tracking `base_seq` in the delta header and prevents future `base_seq` validation from rejecting valid deltas.
2. **Fallback correctness:** After a manual base, the WR-03 fallback policy correctly treats the pre-manual base as "previous" so that the manual base is the primary recovery candidate. Recovery's descending iteration (WR-04) was already robust, but the state now matches the intended invariant.

A comment block was added at the edit site explaining the symmetry with the periodic timer so the next maintainer sees why the two paths must stay in lockstep.

**Verification performed:**
- Tier 1 (mandatory): Re-read modified section (`src/server/http.rs:338-370`) to confirm fix text is present and surrounding code (dirty/deleted clear, snapshot_seq bump, base construction) is intact.
- Tier 2 (syntax check): `cargo check` compiled cleanly with no warnings on the modified file.
- Bonus: `cargo test --lib` -> 455 passed, 0 failed. `cargo test --test test_incremental_snapshot` -> 6 passed, 0 failed. All incremental-snapshot integration tests (base+deltas recovery, eviction deltas, deleted-keys deltas, legacy v5 migration, full snapshot cycle, dirty-only delta) pass with the fix in place.

## Out of Scope (Info findings deferred)

The following Info findings from the re-review are NOT in the `critical_warning` fix scope for this iteration and were intentionally left alone:

- **IN-01** - `cleanup_old_snapshots` leaves orphaned `.tmp` files on disk (disk leak proportional to crash count, not a correctness issue).
- **IN-02** - Manual `/snapshot` endpoint still bypasses `cleanup_old_snapshots` (carryover). Related to IN-01 and the `prev_base_seq` plumbing into the blocking closure.
- **IN-03** - `mark_dirty` in PUSH runs regardless of filter/cascade success (over-marking only, no correctness impact).
- **IN-04** - `load_incremental_snapshots` double-clones every entity on recovery (startup-only overhead).
- **IN-05** - Test helper duplicates production recovery logic (test hygiene, not a correctness issue).

---

_Fixed: 2026-04-09T12:30:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 2_
