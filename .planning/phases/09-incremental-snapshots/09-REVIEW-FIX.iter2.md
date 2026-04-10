---
phase: 09-incremental-snapshots
fixed_at: 2026-04-09T00:00:00Z
review_path: .planning/phases/09-incremental-snapshots/09-REVIEW.md
iteration: 1
findings_in_scope: 6
fixed: 6
skipped: 0
status: all_fixed
---

# Phase 9: Code Review Fix Report

**Fixed at:** 2026-04-09
**Source review:** .planning/phases/09-incremental-snapshots/09-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 6 (1 Critical + 5 Warnings)
- Fixed: 6
- Skipped: 0

Info-level findings (IN-01 through IN-05) were out of scope per `fix_scope: critical_warning` and were not addressed here, with one doc-only exception: IN-04's contract comment was added opportunistically alongside WR-05 since both touched `StateStore::remove_empty_entities`.

Verification: `cargo test --lib` passes (455 tests) and `cargo test --test test_incremental_snapshot` passes (6 tests) after every commit.

## Fixed Issues

### CR-01: Concurrent snapshot writes race on shared `.tmp` filename

**Files modified:** `src/main.rs`, `src/server/http.rs`
**Commit:** f50936c
**Applied fix:** Replaced `file_path.with_extension("tmp")` in both the periodic snapshot timer (`src/main.rs`) and the manual `/snapshot` HTTP handler (`src/server/http.rs`) with `snap_dir.join(format!("{}.tmp", filename))`, so the tmp staging path embeds the full `tally.snapshot.{base|delta}.{seq}.tmp` filename and is unique per (type, seq). Two writers can no longer clobber a shared `tally.snapshot.base.tmp` in the blocking pool.

Additionally rolled in WR-01 in the same commit since it touches exactly the same lines: the tmp file is now opened via `OpenOptions` with `create/write/truncate`, written, `sync_all()`-fsynced before rename, and the containing directory is fsynced after rename so the rename itself is durable.

### WR-01: Snapshot writes are not fsynced; rename is not durable

**Files modified:** `src/main.rs`, `src/server/http.rs`
**Commit:** f50936c
**Applied fix:** Combined with CR-01 (same lines). The write path now uses `OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?`, followed by `write_all(&bytes)?`, `sync_all()?`, `drop(f)`, `rename(..)`, and a best-effort directory fsync via `File::open(&snap_dir).and_then(|d| d.sync_all())`. Brings snapshot durability in line with the existing per-second event log fsync timer.

### WR-02: Delta header `base_seq` diverges from the true latest base

**Files modified:** `src/main.rs`, `src/server/tcp.rs`, `tests/test_pipeline.rs`
**Commit:** 82746c6
**Applied fix:**
- Added `last_base_seq: u64` and `previous_base_seq: u64` fields to `AppState` (`src/server/tcp.rs`) and all three constructors (`src/main.rs`, `src/server/tcp.rs` test helper, `tests/test_pipeline.rs` test helper).
- In the periodic snapshot timer, capture `app.last_base_seq` under the lock before branching, and use that value — not `seq.saturating_sub(1)` — in the `SnapshotType::Delta { base_seq }` header.
- After a successful base snapshot is prepared, advance `previous_base_seq = last_base_seq` and `last_base_seq = seq` inside the same lock critical section.
- On recovery, `load_incremental_snapshots` now returns the seq of the base it actually loaded (or 0 for legacy v5), and `main` restores it into `app.last_base_seq` so the first post-recovery delta stamps the correct pointer.

Note: this commit intentionally also ships WR-03 and WR-04 plumbing because those fixes share the same tuple, state fields, and recovery path. See those sections for details. Each fix was developed and tested against the full suite before committing.

### WR-03: `cleanup_old_snapshots` deletes the only fallback

**File modified:** `src/main.rs`
**Commit:** 82746c6
**Applied fix:** The periodic snapshot timer now threads `prev_base_seq_for_cleanup` through the prepared tuple and into the blocking closure. `cleanup_old_snapshots(&snap_dir, cutoff)` is called with `cutoff = previous_base_seq` (preserving the old base and any deltas that sit between it and the new base) rather than `seq` (which deleted the old base immediately). On the very first base write (`previous_base_seq == 0`), the old behaviour is kept — there is no older base to preserve.

### WR-04: `load_incremental_snapshots` skips unreadable base with no retry of older bases

**File modified:** `src/main.rs`
**Commit:** 82746c6
**Applied fix:** `load_incremental_snapshots` now iterates `bases.iter().rev()` and attempts to read + decode each candidate base, falling back to the next-older base on any failure, until one decodes successfully or the list is exhausted. Combined with WR-03, which keeps the previous base on disk, this means a corrupt newest base no longer forces a total-loss restart: recovery will fall back to the preserved previous base and its deltas.

### WR-05: `StateStore::apply_delta` leaves tombstoned operators when a stream is dropped

**Files modified:** `src/state/store.rs`, `src/main.rs`
**Commit:** 149ce3b (`src/state/store.rs` gc method + IN-04 contract doc), 82746c6 (`src/main.rs` call site)
**Applied fix:** Added `StateStore::gc_invalid_operators(&AHashMap<String, Vec<String>>)` which iterates all entities and drops (a) operators whose feature name is not in the current stream's valid-features list and (b) streams that are not in the valid-features map at all. The `src/main.rs` startup recovery path calls it once after all pipelines have been re-registered, matching Option 1 in the review's suggested fix ("one-shot GC pass against `engine.valid_features_map()` before accepting traffic"). Since the GC runs after restore + apply_delta, it cleans zombies from both the base and any replayed deltas.

Opportunistically also addressed IN-04 in the same commit: added a contract doc to `remove_empty_entities` warning that callers must first call `mark_deleted` to keep the delta snapshot path consistent with eviction. No behaviour change.

## Skipped Issues

None. All in-scope findings were fixed.

---

_Fixed: 2026-04-09_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
