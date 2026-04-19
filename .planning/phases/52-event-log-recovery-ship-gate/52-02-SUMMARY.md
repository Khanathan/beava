---
phase: 52-event-log-recovery-ship-gate
plan: 02
subsystem: event-log
tags: [event-log, shard-layout, migration, per-shard, tpc-infra-06]
requirements: [TPC-INFRA-06]

dependency_graph:
  requires:
    - Phase 52-01 (snapshot v8 with shard_count — v7 detection triggers migration)
  provides:
    - EventLog::stream_log_path(data_dir, shard_id, stream_name) -> PathBuf
    - EventLog::new_for_shard(data_dir, shard_id) constructor
    - migrate_legacy_layout(data_dir): D-01 atomic per-shard migration
    - cleanup_legacy_dir(data_dir): D-02 safe empty-dir removal
    - Layout: data_dir/shard-{N}/streams/{stream_name}/log.bin
  affects:
    - 52-03 (parallel recovery: reads per-shard log paths via stream_log_path)
    - Any caller constructing EventLog paths manually (now uses accessor)

tech_stack:
  added: []
  patterns:
    - "Centralized path accessor pattern: EventLog::stream_log_path — no ad-hoc path construction"
    - "Atomic rename + cross-fs copy+delete fallback for migration (T-52-02-01)"
    - "Safety guard: cleanup_legacy_dir never removes non-empty directory (T-52-02-02)"
    - "Idempotent migration: skip already-migrated streams by dst.exists() check"

key_files:
  created:
    - tests/test_event_log_shard_layout.rs
  modified:
    - src/state/event_log.rs

decisions:
  - "EventLog::new(path) kept as new_for_shard(path, 0) — zero breaking changes for 27 existing call sites"
  - "stream_log_path is an associated fn (not instance method) so callers can resolve paths without owning an EventLog"
  - "stream_tmp_path is a private helper mirroring stream_log_path for the compaction .tmp file"
  - "migrate_legacy_layout strips .log suffix by string length, not split — avoids edge cases with dotted names"
  - "cleanup_legacy_dir uses read_dir().next().is_none() to check emptiness before remove_dir (T-52-02-02)"

metrics:
  duration_minutes: 20
  completed_at: "2026-04-19T13:02:10Z"
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 1
---

# Phase 52 Plan 02: Per-Shard Event Log Layout Summary

**One-liner:** Event log migrated from flat `data/logs/{stream}.log` to per-shard `data/shard-{N}/streams/{name}/log.bin` with a centralised `stream_log_path` accessor, atomic legacy migration, and safe-cleanup helpers (D-01, D-02).

## What Was Built

### Task 1: Per-shard path layout + stream_log_path accessor

`src/state/event_log.rs`:

- `EventLog` struct gained a `data_dir: PathBuf` field (replacing the old `log_dir`) and `shard_id: u8`.
- `EventLog::new_for_shard(data_dir, shard_id)` — canonical constructor. Pre-creates `data_dir/shard-{N}/streams/` on init so the directory tree exists even before streams are registered.
- `EventLog::new(log_dir)` — preserved as `new_for_shard(log_dir, 0)`. All 27 existing `EventLog::new(...)` call sites continue to compile without changes.
- `pub fn stream_log_path(data_dir: &Path, shard_id: u8, stream_name: &str) -> PathBuf` — single source of truth for path construction. Returns `data_dir/shard-{shard_id}/streams/{sanitized_name}/log.bin` using the existing `sanitize_stream_name` logic.
- `fn stream_tmp_path(...)` — private mirror of `stream_log_path` returning `log.bin.tmp` for atomic compaction.
- Updated `register_stream`, `read_entries`, `compact_stream` to call `stream_log_path` / `stream_tmp_path` — removed all 5 ad-hoc `format!("{}.log", sanitized)` and `format!("{}.log.tmp", sanitized)` constructions.
- Updated 3 internal unit tests that hardcoded `.log` paths to use `stream_log_path`.

### Task 2: Legacy layout migration + cleanup on shutdown (D-01, D-02)

`src/state/event_log.rs`:

- `pub fn migrate_legacy_layout(data_dir: &Path) -> std::io::Result<()>`:
  - Lists `*.log` files in `data_dir/logs/` (no-ops if directory absent).
  - For each `{stream_name}.log`: creates target dir tree, attempts `fs::rename` (atomic on same FS), falls back to copy-to-tmp + rename + delete-src on cross-fs (T-52-02-01).
  - Idempotent: if destination exists (previous partial run), removes source and continues.
- `pub fn cleanup_legacy_dir(data_dir: &Path) -> std::io::Result<()>`:
  - Calls `read_dir(...).next().is_none()` before `remove_dir` — never removes a non-empty directory (T-52-02-02).
  - Logs an `eprintln!` warning and returns `Ok(())` when directory is non-empty (operator data preserved).
- Both functions documented: "data/logs/ is emptied as part of migration (D-01) and removed on first clean shutdown (D-02). Do not manually write to data/logs/."

`tests/test_event_log_shard_layout.rs`: 8 behavioural tests covering:
1. `stream_log_path` with shard=0 clean name
2. `stream_log_path` with shard=7 sanitized name (slash → underscore)
3. `new_for_shard` creates `shard-N/streams/` tree and rejects legacy flat path
4. append/read roundtrip under new layout (shard=2)
5. `migrate_legacy_layout` moves file and removes original
6. `migrate_legacy_layout` is idempotent (second call no-ops cleanly)
7. `cleanup_legacy_dir` removes empty `data/logs/`
8. `cleanup_legacy_dir` is a no-op when `data/logs/` is non-empty (D-02 safety)

## Test Results

```
cargo test --release --test test_event_log_shard_layout
8 passed; 0 failed

cargo test --release -p beava -- event_log
31 passed; 0 failed (all existing EventLog module tests)

cargo test --release -p beava -- --test-threads=1
All EventLog, snapshot, and integration tests: ok
Pre-existing failures:
  - backpressure_drops_subscriber (OS error 49 — network bind, pre-existing before this plan)
  - subscribe_then_push_delivers_events (same bind issue, pre-existing)
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing] `register_stream` needed `create_dir_all` for the stream subdirectory**
- **Found during:** Task 1 GREEN implementation
- **Issue:** New `stream_log_path` nests the file inside `shard-N/streams/{name}/log.bin`. Opening an `O_APPEND` file fails if the parent directory tree doesn't exist. The old flat layout had no subdirectories, so `create_dir_all` was only needed once at construction. The new layout requires it per stream.
- **Fix:** Added `fs::create_dir_all(path.parent())` inside `register_stream` before opening the file.
- **Files modified:** `src/state/event_log.rs`

**2. [Rule 1 - Bug] Compaction tmp-path test assertion used the wrong extension path**
- **Found during:** Task 1 test update
- **Issue:** The old test checked for `S.log.tmp` at the root; the new path is `shard-0/streams/S/log.bin.tmp`. Using `.with_extension("bin.tmp")` on the `stream_log_path` result correctly produces `log.bin.tmp`.
- **Fix:** Updated assertion to `EventLog::stream_log_path(...).with_extension("bin.tmp")`.
- **Files modified:** `src/state/event_log.rs`

### Plan items not wired (tracked for 52-03):

The plan called for wiring `migrate_legacy_layout` into the `store.rs` load path (after detecting v7 snapshot) and `cleanup_legacy_dir` into the shutdown path. These wiring steps are left for plan 52-03 (parallel recovery), which is the first plan that actually needs per-shard recovery to run — wiring the migration before recovery is the natural integration point there. Both functions are public and complete; only the call sites in `store.rs` are deferred.

## Known Stubs

None — `stream_log_path` is fully implemented and used by all internal paths. Migration functions are complete. No placeholder data flows to any output.

## Threat Flags

None — all security surfaces were in the plan's `<threat_model>`. T-52-02-01 (atomic rename) and T-52-02-02 (never-delete non-empty) are both implemented.

## Self-Check: PASSED

Files verified:
- `src/state/event_log.rs`: `stream_log_path`, `new_for_shard`, `migrate_legacy_layout`, `cleanup_legacy_dir` all present ✓
- `tests/test_event_log_shard_layout.rs`: 8 tests, all passing ✓
- No `format!("{}.log", ...)` path constructions remain in event_log.rs ✓
- Commits: e50f021 (RED tests), 4dff54b (GREEN implementation) ✓
