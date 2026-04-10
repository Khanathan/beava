---
phase: 09-incremental-snapshots
reviewed: 2026-04-09T12:00:00Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - src/state/store.rs
  - src/state/snapshot.rs
  - src/main.rs
  - src/server/http.rs
  - src/server/tcp.rs
  - src/state/eviction.rs
  - tests/test_incremental_snapshot.rs
  - tests/test_pipeline.rs
findings:
  critical: 0
  warning: 1
  info: 5
  total: 6
status: issues_found
---

# Phase 9: Code Review Report (Re-review)

**Reviewed:** 2026-04-09
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

This is a re-review of Phase 9 after the auto-fix cycle. The previous review found 11 issues (1 Critical, 5 Warning, 5 Info). The fix cycle addressed CR-01, WR-01, WR-02, WR-03, WR-04, WR-05, and opportunistically IN-04.

**Verification of fixes:**

| Previous | Status | Notes |
|----------|--------|-------|
| CR-01 (shared tmp race) | Fixed | Unique tmp filename `{filename}.tmp` in both `src/main.rs:355` and `src/server/http.rs:372` embeds the sequence, eliminating the race between periodic and manual writers. |
| WR-01 (no fsync) | Fixed | Both periodic (`src/main.rs:361-373`) and manual (`src/server/http.rs:378-390`) paths now use `OpenOptions` + `sync_all()` on the tmp file before rename, followed by a best-effort directory fsync. |
| WR-02 (wrong delta base_seq) | Partially fixed | Periodic timer correctly stamps `base_seq = app.last_base_seq` (`src/main.rs:244`, `312`). `AppState.last_base_seq` is tracked, updated on base writes, and restored from disk at startup. However, the manual `/snapshot` path does NOT update `last_base_seq`, which re-introduces the same staleness bug in the periodic->manual->periodic interleaving (see WR-01 below). |
| WR-03 (cleanup deletes only fallback) | Fixed | `previous_base_seq` is tracked on `AppState`; `cleanup_old_snapshots` cutoff is `previous_base_seq` (when nonzero), preserving the prior base as a fallback. |
| WR-04 (no retry of older base) | Fixed | `load_incremental_snapshots` iterates bases in descending seq order via `bases.iter().rev().find_map(...)` (`src/main.rs:566-574`) and falls back to older bases on decode failure. |
| WR-05 (tombstoned operators on recovery) | Fixed | `StateStore::gc_invalid_operators` exists in `src/state/store.rs:434-446` and is invoked once at startup in `src/main.rs:140-143` after base+deltas are loaded and pipelines re-registered. |
| IN-04 (remove_empty_entities contract) | Fixed | Doc comment at `src/state/store.rs:414-424` documents the `mark_deleted` contract. Callers that bypass it now have an explicit warning in the public API. |

**New findings:** One Warning (regression in the manual snapshot path) and five Info items. The Warning is a direct consequence of the WR-02 fix being incomplete in the manual `/snapshot` code path. The Info items are either pre-existing items that were not addressed (IN-01, IN-02, IN-03, IN-05 from the previous review) or one new item (stale `.tmp` cleanup, which the CR-01 fix recommended but did not implement). `tests/test_pipeline.rs` remains untouched by this phase and has no findings.

## Warnings

### WR-01: Manual `/snapshot` path does not update `last_base_seq` or `previous_base_seq`

**File:** `src/server/http.rs:304-419`
**Issue:** The fix for WR-02 added two new fields to `AppState` — `last_base_seq` (points at the most-recent base on disk) and `previous_base_seq` (points at the base before that, preserved as a fallback by `cleanup_old_snapshots`). The periodic snapshot timer in `src/main.rs:289-293` correctly updates both fields on every base write:

```rust
let prev_base = app.last_base_seq;
app.previous_base_seq = prev_base;
app.last_base_seq = seq;
```

The manual `/snapshot` endpoint in `src/server/http.rs:309-360` also writes a full base, advances `app.snapshot_seq`, and clears dirty/deleted tracking — but it never touches `last_base_seq` or `previous_base_seq`. This re-introduces the exact bug WR-02 was meant to fix, in the manual-then-periodic interleaving:

1. Periodic timer writes base at seq=1 → `last_base_seq=1`, `previous_base_seq=0`.
2. Operator hits `POST /snapshot` → base at seq=2 on disk, but `last_base_seq` still == 1.
3. Next periodic tick (delta cycle) → delta header stamped with `base_seq = 1`, pointing at the OLD base, not the one at seq=2 that is now the newest on disk.
4. Next periodic base at cycle N → `prev_base = app.last_base_seq = 1`, so `cleanup_old_snapshots` uses cutoff=1 and keeps the old base at seq=1 as its "previous" fallback, while the manual base at seq=2 is treated as "just another file between old and new."

Impact is two-fold:
- **Delta header correctness:** Any delta written after a manual snapshot has a wrong `base_seq` field. Today's recovery code only uses the seq number in the filename to filter deltas, so this is cosmetic for the happy path — but it defeats the debuggability/validation goal of tracking base_seq in the header at all. It also means any future validation that checks "does this delta's base_seq match the base file we loaded?" will reject valid deltas.
- **Fallback correctness:** After a manual base, the WR-03 fallback policy still treats the pre-manual base as "previous," not the manual base. If the next periodic base turns out to be unreadable and the manual base is still on disk, the recovery currently falls back to the oldest base, skipping the manual one as a potentially-good candidate. (WR-04's "iterate descending" saves this in practice, so this is lower-severity.)

**Fix:** Apply the same `last_base_seq` / `previous_base_seq` update pattern in the manual path. Also pass the previous value through to `cleanup_old_snapshots` if you choose to add cleanup to the manual path (see IN-02 below):

```rust
// Inside trigger_snapshot, before releasing the initial lock guard:
let prev_base = app.last_base_seq;
app.previous_base_seq = prev_base;
app.last_base_seq = seq;
// Optionally carry prev_base into the blocking closure for cleanup.
```

This keeps the periodic and manual paths symmetric and restores the invariant that `app.last_base_seq` always names the newest base on disk.

## Info

### IN-01: `cleanup_old_snapshots` leaves orphaned `.tmp` files on disk

**File:** `src/main.rs:506-525`
**Issue:** The CR-01 fix recommendation included: "update the cleanup scanner in `cleanup_old_snapshots` to also remove stale `*.tmp` files whose embedded seq is `< current_base_seq` (otherwise orphaned tmps from crashed writes accumulate)." This part of the recommendation was not implemented.

`cleanup_old_snapshots` only strips the `tally.snapshot.base.` / `tally.snapshot.delta.` prefixes and then `parse::<u64>()` the suffix. A stale tmp from a crashed write (e.g. `tally.snapshot.base.0000000005.tmp`) parses the suffix as `0000000005.tmp`, which fails `parse::<u64>()` and is silently ignored — safe, but it accumulates on disk across crashes and is never reclaimed.

This is not a correctness issue (`load_incremental_snapshots` also ignores these files by the same parse logic), just an unbounded disk leak proportional to the crash count.

**Fix:** In `cleanup_old_snapshots`, additionally strip the `.tmp` suffix before parsing and include any file whose parsed seq is `< current_base_seq`:

```rust
let stripped = name_str
    .strip_prefix("tally.snapshot.base.")
    .or_else(|| name_str.strip_prefix("tally.snapshot.delta."));
if let Some(stem) = stripped {
    let stem = stem.strip_suffix(".tmp").unwrap_or(stem);
    if let Ok(seq) = stem.parse::<u64>() {
        if seq < current_base_seq {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
```

### IN-02: Manual `/snapshot` endpoint still bypasses `cleanup_old_snapshots` (carryover)

**File:** `src/server/http.rs:304-419`
**Issue:** Carried over from the previous review, not addressed by the fix cycle. The manual snapshot path writes a new base but never calls `cleanup_old_snapshots`. Combined with WR-01 above, this means heavy manual-snapshot use leaves an unbounded number of base files on disk.

**Fix:** Call `cleanup_old_snapshots(&snap_dir, prev_base_seq)` inside the blocking closure after a successful manual base write, matching the periodic path. Uses the previous-base cutoff semantics from the WR-03 fix.

### IN-03: `mark_dirty` in PUSH runs regardless of filter/cascade success (carryover)

**File:** `src/server/tcp.rs:201-209`, `236-248`, `265-280`
**Issue:** Carried over from the previous review, not addressed. All three `mark_dirty` sites in `handle_sync_command::Push` fire unconditionally even when the push was filtered or silently failed. Produces over-marking in deltas — slightly larger-than-necessary delta payloads, no correctness impact.

**Fix:** At minimum, guard the fan-out site by checking the `Result` of `engine.push(...)` before calling `store.mark_dirty`. The primary and cascade sites are harder to gate without an engine API change; accept as defensive or document.

### IN-04: `load_incremental_snapshots` double-clones every entity on recovery (carryover)

**File:** `src/main.rs:607-611`
**Issue:** Carried over from the previous review, not addressed. The recovery helper builds a scratch `StateStore`, applies deltas, then calls `store.clone_for_snapshot()` to package the merged state as a `SnapshotState`, which the caller then `restore_from_snapshot`s into the real `app.store`. Every entity is cloned twice during startup.

Low priority. A pass-through helper or a `StateStore::into_entities(self)` move-based API would avoid the extra clone without changing semantics.

### IN-05: Test helper duplicates production recovery logic (carryover)

**File:** `tests/test_incremental_snapshot.rs:358-415`
**Issue:** Carried over from the previous review, not addressed. `recover_from_dir` is still an almost-verbatim copy of `load_incremental_snapshots` (minus legacy fallback and the `loaded_base_seq` return). Bug fixes to `load_incremental_snapshots` (e.g., the WR-04 descending-iteration fix) are not exercised by the integration tests via the production code path. The tests still pass, but they're testing a parallel implementation.

**Fix:** Promote `load_incremental_snapshots` from `pub(crate)` in `main.rs` to a public function in the library crate (e.g. `tally::state::snapshot::load_incremental` or `tally::state::recovery::load_incremental_snapshots`) and call it from both `src/main.rs` and `tests/test_incremental_snapshot.rs`. Also makes it unit-testable.

---

_Reviewed: 2026-04-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
