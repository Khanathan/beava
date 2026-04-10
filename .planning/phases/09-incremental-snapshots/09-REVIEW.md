---
phase: 09-incremental-snapshots
reviewed: 2026-04-09T00:00:00Z
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
  warning: 0
  info: 4
  total: 4
status: clean
---

# Phase 9: Code Review Report (Iteration 3 Re-review)

**Reviewed:** 2026-04-09
**Depth:** standard
**Files Reviewed:** 8
**Status:** clean (no critical or warning issues; info-level observations only)

## Summary

This is the third code review iteration for Phase 9 (Incremental Snapshots). The previous iteration (09-REVIEW.iter3.md) flagged a single Warning — **WR-01**: the manual `POST /snapshot` handler did not update `last_base_seq` / `previous_base_seq`, re-introducing the WR-02 staleness bug in the "periodic base → manual base → periodic delta" interleaving.

The iter3 fix was applied in `src/server/http.rs:345-354`. This re-review confirms:

1. **WR-01 is fixed correctly.** The manual path now mirrors the periodic timer's bookkeeping invariant byte-for-byte.
2. **No new issues were introduced.** The fix is purely additive — three lines inside an existing lock critical section.
3. **All previously tracked findings remain resolved** (CR-01 tmp file naming, WR-02 delta `base_seq` header, WR-03 fallback preservation, WR-04 descending base scan, WR-05 one-shot GC).

The phase is now clean from a code-review standpoint. Four Info-level observations below are carried forward for future iterations but do not block shipping.

## Fix Verification (WR-01)

**File:** `src/server/http.rs:309-369`
**Fixed at:** lines 345-354 (inside the lock guard that prepares the snapshot)

The manual `trigger_snapshot` handler now includes:

```rust
app.snapshot_seq += 1;
// Phase 9 WR-01 (re-review): keep the manual path symmetric with the
// periodic timer in src/main.rs:289-293. Advance `last_base_seq` and
// roll the previous one into `previous_base_seq` ...
let prev_base = app.last_base_seq;
app.previous_base_seq = prev_base;
app.last_base_seq = seq;
```

**Comparison with the periodic timer** (`src/main.rs:287-293`, base branch):

```rust
app.snapshot_cycle += 1;
app.snapshot_seq += 1;
let prev_base = app.last_base_seq;
app.previous_base_seq = prev_base;
app.last_base_seq = seq;
```

The two update sequences are structurally identical for the three fields that matter (`snapshot_seq`, `previous_base_seq`, `last_base_seq`). The manual path does not touch `snapshot_cycle` — this is correct, because `snapshot_cycle` drives the base-vs-delta decision on the *periodic* timer, and a manual trigger should not shift that cadence (if it did, an operator repeatedly hitting `/snapshot` could starve delta writes).

**Lock atomicity:** Both updates are captured inside a single `state.lock()` scope (http.rs:310-370). No intermediate `.await` or lock release interleaves between the `seq` read at line 311 and the `last_base_seq = seq` assignment at line 354, so concurrent periodic timer ticks observe a consistent `(snapshot_seq, last_base_seq, previous_base_seq)` tuple. Verified.

**Seq capture ordering:** `let seq = app.snapshot_seq;` (line 311) is read **before** the increment at line 344, which matches the periodic timer convention (`main.rs:234, 288`). The captured `seq` is used consistently for:
- the filename (`tally.snapshot.base.{:010}`, line 376)
- the header `sequence` field (line 363)
- the `last_base_seq = seq` assignment (line 354)

So the same sequence number ends up on disk, in the header, and in `AppState`. No off-by-one.

### Scenario walk-throughs

All four interleavings now behave correctly:

| Scenario | Pre-fix behavior | Post-fix behavior |
|---|---|---|
| Periodic base → manual base → periodic delta | Delta header stamps stale `base_seq = 1` even though the manual base at seq=2 is the newest on disk. Recovery would mis-order the chain. | Manual path updates `last_base_seq = 2`. Next periodic delta reads `last_base_seq_for_delta = 2` at `main.rs:244` and stamps the correct pointer. |
| Periodic base → manual base → next periodic base | `prev_base_seq_for_cleanup = 1` (stale), so `cleanup_old_snapshots` uses cutoff=1, *keeping the pre-manual base at seq=1 as its "previous" fallback* and treating the manual base as an intermediate file. Storage leaks; wrong fallback. | `prev_base_seq_for_cleanup = 2` (manual), so cleanup uses cutoff=2 and correctly deletes seq=1 while keeping the manual base seq=2 as the fallback. |
| Recovery → manual → periodic delta | Manual didn't update `last_base_seq`, so the delta after manual still pointed at `loaded_base_seq`, not the manual base. | Manual advances `last_base_seq` from `loaded_base_seq` to the new manual seq; delta points at the correct base. |
| Cold start → manual first → periodic delta | `last_base_seq` stays 0 after manual; delta stamps `base_seq=0`, pointing at a nonexistent base. | Manual updates `last_base_seq` to its own seq; delta points correctly. |

### Related callers audited

Searched for every site that mutates `snapshot_seq`:

- `src/main.rs:86` — startup recovery (`app.snapshot_seq = next_seq;`). Immediately followed by `app.last_base_seq = loaded_base_seq` (line 91) and `app.previous_base_seq = 0` (line 92). Consistent with the invariant.
- `src/main.rs:288` — periodic base branch. Updates `last_base_seq` / `previous_base_seq` in the same critical section. Correct.
- `src/main.rs:321` — periodic delta branch. Deliberately does **not** update `last_base_seq` (a delta does not create a new base). Correct.
- `src/server/http.rs:344` — manual base. Now updated per iter3 fix. Correct.

No other caller of `snapshot_seq` exists; the invariant "every `snapshot_seq` advancement on a base write also advances `last_base_seq` and `previous_base_seq`" holds across all paths.

### Test helper consistency

All three `AppState` constructors have been kept in sync with the new fields:
- `src/main.rs:53-65` (production)
- `src/server/tcp.rs:536-548` (unit test helper)
- `tests/test_pipeline.rs:534-549` (integration test helper)

Each initializes `last_base_seq: 0` and `previous_base_seq: 0` at startup, which is the correct "no base yet" sentinel. No stale default was left behind.

## Status of Previously Tracked Findings

| Finding | Status | Notes |
|---|---|---|
| CR-01 (tmp filename race) | Fixed | Unique tmp per (type, seq) in both paths (`main.rs:355`, `http.rs:382`). |
| WR-01 (manual path last_base_seq) | **Fixed this iteration** | `http.rs:345-354` now mirrors `main.rs:289-293`. |
| WR-02 (wrong delta base_seq) | Fixed | Periodic timer stamps `base_seq = last_base_seq` (`main.rs:244`, `312-314`); manual path now keeps the pointer current. |
| WR-03 (cleanup deletes only fallback) | Fixed | `previous_base_seq` tracked; cleanup uses it as cutoff when nonzero (`main.rs:383-387`). |
| WR-04 (corrupt newest base strands older base) | Fixed | `load_incremental_snapshots` iterates bases in descending seq order and skips undecodable ones (`main.rs:564-574`). |
| WR-05 (zombie operators after startup) | Fixed | One-shot `gc_invalid_operators` pass after recovery (`main.rs:141-143`; `store.rs:434-446`). |

## Info

Four Info-level observations. None block this phase. All are pre-existing behaviors preserved by the iter3 fix (i.e., the fix did not introduce them), but they are worth noting for future hardening.

### IN-01: Manual `/snapshot` advances in-memory seq/pointers even when the disk write fails

**File:** `src/server/http.rs:344-354, 404-427`
**Issue:** The manual path increments `snapshot_seq`, sets `last_base_seq = seq`, and updates `previous_base_seq` **before** `spawn_blocking` attempts to write the file. If the blocking write fails (IO error, disk full, serialization panic), the error is returned to the caller but the in-memory `AppState` is not rolled back. After such a failure:
- `last_base_seq` points at a base seq that does not exist on disk.
- The next periodic delta will stamp its header with that nonexistent `base_seq`.
- On restart, `load_incremental_snapshots` will not find a base at `last_base_seq`, fall back to the next-newest base via WR-04 descending scan, and deltas stamped with the missing `base_seq` will be loaded via the "apply any delta with seq > base_seq" rule in `main.rs:582-605`. The wrong `base_seq` in the header is tolerated (only `sequence` is checked), so recovery still works — but the header metadata is a lie and complicates offline analysis.

The periodic timer in `main.rs:287-294` has identical semantics, so this is not a regression from iter3. It is an underlying architectural choice (advance-then-write, no rollback on failure) shared by both paths.

**Fix (optional, future iteration):** Capture `old_last_base_seq`, `old_previous_base_seq`, and `old_snapshot_seq` before the update, and roll them back in the `Ok(Err(_))` / `Err(_)` arms of the `match result`. Alternatively, defer the pointer updates until the blocking task succeeds (requires a second lock acquisition on the success path). The first approach is simpler and matches how fallible state transitions are usually handled.

### IN-02: Manual `/snapshot` does not call `cleanup_old_snapshots`

**File:** `src/server/http.rs:304-429`
**Issue:** The manual path writes a new base but never runs `cleanup_old_snapshots`. A sufficiently determined operator hitting `POST /snapshot` repeatedly would leave every base on disk until the next *periodic* base triggers cleanup. This is a storage-pressure concern, not a correctness bug: recovery is still correct, it just processes more files.

The iter3 fix is agnostic to this — it correctly maintains `previous_base_seq` so that *when* the next periodic base runs cleanup, the manual base(s) are respected as part of the sequence chain. If a future iteration wires cleanup into the manual path, it should pass `prev_base` (the pre-update value captured at `http.rs:352`) as the cutoff, matching the periodic timer's contract.

### IN-03: Manual `/snapshot` does not fsync the parent directory on Windows-style platforms

**File:** `src/server/http.rs:398-400` (and `src/main.rs:371-373` in the periodic path)
**Issue:** Both paths attempt `std::fs::File::open(&snap_dir)` followed by `sync_all()` to durably record the `rename` into the directory entry. On Linux this works. On Windows, opening a directory with `File::open` returns an error (`std::io::ErrorKind::PermissionDenied` or similar), and the code silently swallows it via `if let Ok(dir) = ...`. The crash durability story on Windows is therefore weaker than on Linux: a power loss immediately after rename but before the directory metadata reaches disk could leave the filesystem without the rename.

This is pre-existing and not regressed by iter3. Tally's target is Linux (per CLAUDE.md "single binary"), so the Windows gap is acceptable. Worth documenting in `09-CONTEXT.md` as a platform assumption.

### IN-04: `load_incremental_snapshots` silently skips unreadable deltas

**File:** `src/main.rs:589-604`
**Issue:** When scanning deltas during recovery, an unreadable file (IO error) or an undecodable file is silently skipped via `continue`. If a delta in the middle of a chain is missing, subsequent deltas will still be applied, producing a state that has a hole in the event history. The final state will be self-consistent (operators are commutative and idempotent for the delta-skipping case) but not equivalent to the state at the time the last delta was written.

In practice, deltas are only ever skipped if the file is corrupt (partial write after a power loss) or unreadable (permission error). The iter3 fix to WR-01 interacts with this because if cleanup runs with the wrong cutoff and deletes an intermediate delta, recovery would silently skip it. The iter3 fix prevents the wrong cutoff in the first place, so this path is now defensive-only.

**Fix (optional, future iteration):** Log a warning when a delta is skipped, including its sequence number, so operators can detect the hole. Or treat any skip in the middle of the chain as a recovery failure and fall back to the base alone.

## Notes on Test Coverage

The iter3 fix is a three-line bookkeeping change in a path that is exercised by integration tests via `POST /snapshot`. Searching the test suite:

- `tests/test_incremental_snapshot.rs` exercises base+delta recovery, dirty tracking, legacy v5 migration, and eviction → delta integration. It does **not** cover the "periodic → manual → periodic" interleaving. The bug WR-01 addressed was a pure in-memory state-machine bug that the existing test suite could not detect.
- A follow-up test verifying the invariant would assert: after calling `trigger_snapshot`, `app.last_base_seq == seq_of_manual_write` and `app.previous_base_seq == prior_value`. This is trivial to add and would prevent regression.

Not flagging as a warning because the fix itself is obviously correct under inspection and all runtime scenarios have been walked through above. But it would be a cheap addition to the test suite.

---

_Reviewed: 2026-04-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
_Iteration: 3 (re-review)_
