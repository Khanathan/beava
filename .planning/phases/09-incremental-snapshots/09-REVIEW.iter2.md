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
  critical: 1
  warning: 5
  info: 5
  total: 11
status: issues_found
---

# Phase 9: Code Review Report

**Reviewed:** 2026-04-09
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Phase 9 adds incremental snapshots (base + delta chain) to the Streamlet/Tally state store. The core design is sound: dirty/deleted tracking is threaded through all mutation paths (PUSH primary + cascade + fan-out, SET, MSET, REGISTER backfill, and eviction), the v6 wire format cleanly layers a type discriminator on top of postcard, and recovery is corruption-tolerant. The integration tests in `test_incremental_snapshot.rs` exercise the end-to-end lifecycle well.

The review found one Critical concurrency bug, five Warnings (durability, metadata correctness, robustness gaps), and five informational items. The Critical is a shared-tmp-file race between the periodic snapshot timer and the manual `/snapshot` HTTP endpoint that can corrupt snapshot files under concurrent writes. None of the findings relate to `test_pipeline.rs`, which is unchanged by this phase.

## Critical Issues

### CR-01: Concurrent snapshot writes race on shared `.tmp` filename

**File:** `src/main.rs:319-322` and `src/server/http.rs:366-370`
**Issue:** Both the periodic snapshot timer (main.rs) and the manual `/snapshot` HTTP handler (http.rs) use `file_path.with_extension("tmp")` as the write-then-rename staging path. For a filename like `tally.snapshot.base.0000000005`, `Path::with_extension("tmp")` replaces the segment after the final dot, producing `tally.snapshot.base.tmp` — a name that does NOT embed the sequence number.

Both code paths release the `Arc<Mutex<AppState>>` before calling `tokio::task::spawn_blocking`, so two writes can be in flight simultaneously (e.g., a periodic delta at seq=5 completes its state clone, releases the lock; a manual `POST /snapshot` immediately acquires the lock, clones state, releases, and both tasks end up in the blocking pool at once). Because both tasks call `std::fs::write(&tmp_path, &bytes)` on the same `tally.snapshot.base.tmp` (or `delta.tmp`) path, they race:

1. Task A opens, truncates, writes partial bytes
2. Task B opens, truncates (clobbering A's partial), writes bytes
3. Task A's write continues into B's buffer and then renames
4. Renamed file contains an arbitrary interleaving of A's and B's bytes

The renamed file then lives forever as `tally.snapshot.base.{seq}` and will be loaded as truth on next recovery, corrupting state. Postcard deserialization will likely fail and recovery will silently start fresh (per the "fail closed" policy), losing all post-base updates since the last successful write.

Trigger probability: any production deployment that uses the manual `/snapshot` endpoint alongside the default 30s periodic timer. Also triggered by two manual triggers back-to-back.

**Fix:** Make the tmp filename unique per write. Easiest: embed the sequence number before `.tmp`.

```rust
// In both src/main.rs line 319 and src/server/http.rs line 368:
let tmp_path = snap_dir.join(format!("{}.tmp", filename));
// e.g. "tally.snapshot.base.0000000005.tmp" -- unique per (type, seq)
```

And update the cleanup scanner in `cleanup_old_snapshots` (src/main.rs:444) to also remove stale `*.tmp` files whose embedded seq is `< current_base_seq` (otherwise orphaned tmps from crashed writes accumulate).

## Warnings

### WR-01: Snapshot writes are not fsynced; rename is not durable

**File:** `src/main.rs:321-322` and `src/server/http.rs:369-370`
**Issue:** The write sequence is:

```rust
std::fs::write(&tmp_path, &bytes)?;
std::fs::rename(&tmp_path, &file_path)?;
```

There is no `File::sync_all()` on the tmp file before `rename`, and no fsync on the containing directory after `rename`. On ext4/xfs with default mount options, a power loss between the write and a subsequent fsync can lose all the bytes but leave the rename visible, producing a zero-byte or partially-written snapshot file. This is especially important for Phase 9 because `cleanup_old_snapshots` deletes older base snapshots immediately after the new base is renamed — there is no fallback. A crash-corrupt new base plus a just-deleted old base = total state loss on restart (beyond legacy v5 fallback, which disappears once a v6 base is ever written).

Compare to the event log path in main.rs:372-390 which explicitly runs a per-second `log.fsync_all()` timer — snapshots deserve at least the same level of durability since they are the RPO anchor.

**Fix:** Use `OpenOptions` + `sync_all()` + directory fsync pattern:

```rust
use std::fs::OpenOptions;
use std::io::Write;

let mut f = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(&tmp_path)?;
f.write_all(&bytes)?;
f.sync_all()?;
drop(f);
std::fs::rename(&tmp_path, &file_path)?;
// Fsync the directory so the rename itself is durable
if let Ok(dir) = std::fs::File::open(&snap_dir) {
    let _ = dir.sync_all();
}
```

### WR-02: Delta header `base_seq` diverges from the true latest base

**File:** `src/main.rs:283`
**Issue:** When building a delta, the header is constructed as:

```rust
snapshot_type: SnapshotType::Delta {
    base_seq: seq.saturating_sub(1),
},
```

This hard-codes `base_seq = seq - 1`, which is only correct when the immediately previous snapshot was the base. For any delta after the first in a chain, `base_seq` ends up pointing at the previous DELTA rather than the true base. Example with `full_snapshot_interval = 10`:

| cycle | seq | type | stored `base_seq` | actual latest base seq |
|------:|----:|------|------------------:|-----------------------:|
|     0 |   1 | base |   n/a             | 1                      |
|     1 |   2 | delta|   1               | 1  (correct)           |
|     2 |   3 | delta|   2 ✗             | 1                      |
|     3 |   4 | delta|   3 ✗             | 1                      |
|   ... | ... | ...  | ...               | ...                    |
|    10 |  11 | base |   n/a             | 11                     |

The recovery code in `load_incremental_snapshots` (main.rs:495-544) doesn't validate `base_seq` against the base it loaded — it only filters deltas by `seq > base_seq_found_on_disk` — so this is currently cosmetic. But:

1. Any future validation logic that uses the header's `base_seq` will be wrong.
2. Debug tooling / ops inspection of a delta file will report a misleading base_seq.
3. Out-of-order deltas caused by partial cleanup (e.g., a stale delta.5 from an aborted prior run left on disk during a new run that hit seq 5 again after wraparound) cannot be detected without a trustworthy `base_seq`.

**Fix:** Track the latest base sequence in AppState and stamp it into every delta's header:

```rust
// In AppState (src/server/tcp.rs:67):
pub last_base_seq: u64,

// In main.rs base write path, after writing base at seq=X:
app.last_base_seq = seq;

// In main.rs delta write path:
let last_base_seq = app.last_base_seq;
// ...
snapshot_type: SnapshotType::Delta { base_seq: last_base_seq },
```

On recovery (main.rs:82-91), also restore `last_base_seq` from the just-loaded base file.

### WR-03: `cleanup_old_snapshots` deletes the only fallback

**File:** `src/main.rs:325-327`
**Issue:** After a successful base write, the code unconditionally deletes every snapshot file with `seq < current_base_seq` (base AND delta). If the new base later turns out to be unreadable (disk corruption, partial write surviving WR-01, postcard decode failure introduced by a future refactor), recovery has nothing to fall back to. The previous base — which was known-good — has just been removed.

Redis RDB handles this by retaining at least the previous RDB until a new one is verifiable (via SHA or a post-write read). Tally currently has no such guard.

Combined with WR-01 (no fsync), the failure window is: crash during base fsync → partial new base on disk, old base and all deltas deleted → startup can only start from empty state (or legacy v5 if it was never deleted, which it will be the first time a v6 base gets written because v5 is at the legacy_path NOT in the cleanup scan directory — but only by accident).

**Fix:** Keep at least the previous base on disk. Simplest policy: delete only files with `seq < previous_base_seq` after a successful new base write, not `< current_base_seq`. Requires tracking `previous_base_seq` alongside `last_base_seq`.

Alternative: load the base back immediately after writing (before cleanup) and verify it decodes; only then run cleanup.

### WR-04: `load_incremental_snapshots` skips unreadable base with no retry of older bases

**File:** `src/main.rs:498-507`
**Issue:** The recovery flow takes `bases.last()` (highest seq), reads it, and if load fails, returns `None` — dropping straight to legacy v5 fallback OR empty state:

```rust
let (base_seq, base_path) = bases.last().cloned();
let bytes = std::fs::read(&base_path).ok()?;       // returns None on I/O error
let base = match load_snapshot_file(&bytes)? {     // returns None on decode error
    SnapshotFile::Base(b) => b,
    _ => return None,                               // corruption => None
};
```

If there are multiple base files on disk (e.g., because WR-03 is fixed or because manual snapshots left them) and the newest is corrupt, the recovery should try the next-newest base. Currently it cannot.

**Fix:** Iterate bases in descending seq order, attempting to load each until one decodes successfully:

```rust
bases.sort_by_key(|(seq, _)| *seq);
let (base_seq, base) = bases.iter().rev().find_map(|(seq, path)| {
    let bytes = std::fs::read(path).ok()?;
    match load_snapshot_file(&bytes)? {
        SnapshotFile::Base(b) => Some((*seq, b)),
        _ => None,
    }
})?;
```

### WR-05: `StateStore::apply_delta` leaves tombstoned operators when a stream is dropped

**File:** `src/state/store.rs:365-389`
**Issue:** `apply_delta` replaces an entity wholesale when it appears in `changed_entities`, and deletes it outright when it appears in `deleted_keys`. But it does NOT handle the middle case: an entity that still exists, is NOT in `changed_entities` (because it wasn't dirtied this cycle), but whose schema changed in a way that removed a stream or feature.

Concretely: if a user unregisters `StreamA` (lazy GC via `valid_features`), a later base captured with `clone_for_snapshot_with_gc` will correctly drop StreamA's operators. But if the user unregisters StreamA and no events arrive for some entity before the next base, the delta chain leaves stale StreamA operators on that entity — and the eventual base captures them correctly. Correct, but wasteful.

More importantly: on recovery, deltas are applied on top of the base with no GC pass. If a base was written with a stale entity (because the stream was unregistered after the base, then before any delta), the recovered state carries zombie operators until the user hits that entity with an event. This is a minor memory leak and a potential correctness concern if a future "stream exists?" check treats the presence of an operator as truth.

**Fix:** Either:
1. On startup, after loading base + deltas, run a one-shot GC pass against `engine.valid_features_map()` before accepting traffic.
2. Or, in `restore_from_snapshot` and `apply_delta`, accept an optional `valid_features` parameter and filter as we insert.

Option 1 is simpler and matches the existing "lazy GC on snapshot write" pattern; it just moves the GC to one additional point (startup).

## Info

### IN-01: `mark_dirty` runs even when the event was filtered out or the push failed

**File:** `src/server/tcp.rs:190-199`, `228-237`, `257-262`
**Issue:** All three dirty-marking sites in `handle_sync_command::Push` fire unconditionally after the push attempt:

- Primary: runs after `push_with_cascade` regardless of whether the `where`/`filter` gate dropped the event.
- Cascade targets: runs regardless of whether the cascade actually applied.
- Fan-out: runs even after `let _ = engine.push(...)` silently swallowed an error.

Effect: entities can be added to the dirty set when their underlying state didn't actually change, producing slightly bigger deltas than necessary. Not a correctness bug — just over-marking.

**Fix:** Either check whether the push actually mutated state (requires engine API change), or accept the over-mark as defensive and document it. If the MVP ignores this, at minimum catch the fan-out push error and skip mark_dirty on Err:

```rust
if engine.push(target_name, &payload, store, now).is_ok() {
    store.mark_dirty(key_val);
    // ... event log append ...
}
```

### IN-02: Manual `/snapshot` endpoint bypasses `cleanup_old_snapshots`

**File:** `src/server/http.rs:304-399`
**Issue:** The manual snapshot path creates a base file with an incremented `snapshot_seq` but never calls `cleanup_old_snapshots`. If an operator uses manual triggers heavily (e.g., pre-deploy checkpoint), old delta files from prior periodic cycles accumulate on disk. They are harmless to recovery (the latest base wins) but waste disk.

**Fix:** After the manual base write succeeds, call `cleanup_old_snapshots(&snap_dir, seq)` from inside the blocking closure, matching the periodic path at main.rs:324-327.

### IN-03: `load_incremental_snapshots` uses `clone_for_snapshot` (not `_with_gc`) after apply

**File:** `src/main.rs:539-544`
**Issue:** After merging base + deltas into a scratch `StateStore`, the recovery helper calls:

```rust
let state = SnapshotState {
    entities: store.clone_for_snapshot(),  // no GC
    ...
};
```

The caller in `main` then passes `state.entities` to `app.store.restore_from_snapshot`. This double-clones every entity (once into the scratch store's `apply_delta`, once out via `clone_for_snapshot`). For large states this is a measurable startup cost that could be avoided by draining the scratch store's internal map directly.

**Fix:** Low priority. A simple improvement is to expose a `StateStore::into_entities(self) -> AHashMap<String, EntityState>` that moves the map, or just restore directly into the real store rather than scratching. Not blocking.

### IN-04: Eviction's `remove_empty_entities` can drop entities without `mark_deleted`

**File:** `src/state/eviction.rs:96`
**Issue:** `evict_expired_stream_entries` marks an entity deleted at line 85 only when `will_be_empty == true`. But `remove_empty_entities()` at line 96 retains on `!entity.is_empty()` and removes silently. The two-phase design is correct for the normal eviction path, but `remove_empty_entities` is a public StateStore method that could be called from other sites in the future without the matching `mark_deleted` contract. Any future caller that removes an empty entity through this method without first marking it deleted will produce a delta that misses the deletion, and recovery will resurrect the entity from the base snapshot.

**Fix:** Add a debug assertion or mark-and-remove helper that enforces the invariant. Or rename `remove_empty_entities` to `remove_empty_entities_unchecked` to flag the contract. At minimum, add a doc comment warning.

### IN-05: Test helper duplicates production recovery logic

**File:** `tests/test_incremental_snapshot.rs:358-415`
**Issue:** The `recover_from_dir` helper is an almost-verbatim copy of `load_incremental_snapshots` from main.rs (minus the legacy fallback). This couples the test suite to the implementation: a bug fix in main.rs's recovery logic will not be caught by these tests unless the helper is manually kept in sync.

**Fix:** Expose `load_incremental_snapshots` as a public `pub fn` in a library module (e.g., `tally::state::snapshot::load_incremental`) and call it from both `main.rs` and the test. Currently it lives in `main.rs` under `pub(crate)`, which is not reachable from integration tests. Moving it into the library crate also makes it unit-testable.

---

_Reviewed: 2026-04-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
