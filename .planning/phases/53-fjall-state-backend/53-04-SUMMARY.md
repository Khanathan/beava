---
phase: 53-fjall-state-backend
plan: 04
subsystem: storage / migration + reshard
tags: [fjall, migration, reshard, cli, fs2-lock, tdd, w-2-shard-key, tpc-persist-03]
dependency_graph:
  requires: [53-03-SUMMARY.md, 53-03B-SUMMARY.md]
  provides:
    - "`tally migrate-to-fjall --data-dir PATH [--force] [--replace]` CLI subcommand"
    - "beava::migrate_to_fjall::migrate_to_fjall(data_dir, force, replace) -> io::Result<MigrationReport>"
    - "beava::migrate_to_fjall::MigrationReport (entities_migrated/skipped, duration_ms, bak_path, streams_resolved, streams_keyless)"
    - "beava::migrate_to_fjall::{parse_migrate_args, print_migrate_help, is_migrate_subcommand, MigrateArgs}"
    - "resolve_shard_key_for_entity — per-stream shard_key resolver (W-2)"
    - "src/reshard/mod.rs::reshard_from_fjall — fjall-source reshard helper"
    - ".migration-in-progress marker protocol (write on start, remove on success, block reshard)"
    - "snapshot.v8.bak preserve + --replace semantics (operator-controlled rollback window)"
  affects:
    - 53-05-PLAN.md (SIGKILL recovery test — migration tool is now the canonical v8→fjall upgrade path)
    - 53-06-PLAN.md (operations.md: document migrate-to-fjall + reshard fjall-awareness)
requirements:
  - TPC-PERSIST-03 (CLOSED — migrate-to-fjall CLI + fjall-aware reshard + W-2 per-stream routing)
tech_stack:
  added: []
  patterns:
    - "W-2 shard_key resolution: `shard_hint_for_event(json!({key_field: entity_key}), Some(key_field)) % shard_count` — reproduces ingest routing EXACTLY by hashing the VALUE (not the field name), using the stream's actual key_field from the pipeline registry"
    - "fs2 exclusive lock + .migration-in-progress marker: reuses Phase 52-04's reshard lock discipline; marker triggers resume mode (partition.contains_key skip)"
    - "snapshot.v8.bak preserve-by-default: operators retain pre-migration state until --replace; on resume the helper reads from .bak because snapshot.bin is already metadata-only"
    - "Per-stream key_field resolution via `SerializablePipeline.name == entity.streams[0].0`: the Phase 49 entity-keying invariant guarantees entity_key equals the shard_key value at ingest time"
    - "Duplicated 30-line resolver helper between `migrate_to_fjall/` and `reshard/` — keeps reshard self-contained; avoids a pub(crate) cross-module cycle for a small function"
key_files:
  created:
    - src/migrate_to_fjall/mod.rs
    - tests/test_migrate_to_fjall.rs
    - tests/test_reshard_fjall_aware.rs
  modified:
    - src/lib.rs
    - src/main.rs
    - src/reshard/mod.rs
decisions:
  - "D-04-01 (resume source-of-truth): on resume (marker present + !force), the helper prefers to read entity state from `snapshot.v8.bak` whenever it exists, NOT from `snapshot.bin`. Rationale: a prior migration (or a crash between steps 10-11) may have already replaced snapshot.bin with a metadata-only file. Reading from .bak guarantees we still have the authoritative entity list. This is a correctness deviation from the plan's literal recipe (which always reads snapshot.bin) — the plan's text would lose entities on force or resume paths. Not previously flagged because the plan's example only considered single-run semantics. Captured as Deviation 1 below."
  - "D-04-02 (fjall-source reshard helper location): `resolve_shard_key_for_entity` is INTENTIONALLY duplicated between `src/migrate_to_fjall/mod.rs` (pub(crate)) and a local `resolve_shard_for_reshard` helper in `src/reshard/mod.rs`. The plan's <action> Step 4 explicitly allowed either cross-module import OR local duplication; I chose duplication (~30 lines) because reshard stays self-contained and avoids a pub(crate) cycle. Both helpers produce identical outputs for identical inputs — the single-field shard_key contract is algorithmically trivial."
  - "D-04-03 (test count = 8, not 7): Added an 8th smoke test `cli_helpers_exist` that probes `is_migrate_subcommand`, `parse_migrate_args`, and the `MigrationReport` type resolve at compile time. The plan's D-08 contract requires 7 named tests and the regex `(7|[89])` explicitly permits 8 or 9 passed. The smoke keeps the import surface wired without bloating integration time (<1 ms)."
metrics:
  duration_s: 417
  duration_human: "~7m"
  completed: 2026-04-19
  tasks_total: 2
  tasks_completed: 2
  commits: 2
  files_touched: 6
---

# Phase 53 Plan 04: tally migrate-to-fjall + reshard fjall-aware Summary

## One-liner

`tally migrate-to-fjall` converts v8 snapshot entity state to per-shard fjall partitions in-place with W-2-correct per-stream `shard_key` routing (reads each stream's `key_field` from the pipeline registry; NOT hardcoded), fs2-locked against live servers, idempotent, resumable via `.migration-in-progress` marker, bak-preserving by default; `tally reshard` grows fjall awareness (refuses while migration-in-progress, re-routes partitions with the same W-2 helper).

---

## What Was Built

**Commit chain:** `60ce63f` RED → `13f4cc2` GREEN.

### 1. `src/migrate_to_fjall/mod.rs` — 492 lines, NEW

Implements the 14-step recipe from 53-RESEARCH §Migration Tool Recipe verbatim, with W-2 per-stream shard_key routing.

**Public surface:**

```rust
pub struct MigrationReport {
    pub entities_migrated: usize,
    pub entities_skipped: usize,
    pub duration_ms: u64,
    pub bak_path: Option<PathBuf>,
    pub marker_removed: bool,
    pub streams_resolved: usize,   // W-2: distinct resolved key_fields
    pub streams_keyless: usize,     // W-2: keyless routing decisions
}

pub fn migrate_to_fjall(data_dir: &Path, force: bool, replace: bool)
    -> io::Result<MigrationReport>;

pub struct MigrateArgs { pub data_dir: PathBuf, pub force: bool, pub replace: bool, pub help: bool }
pub fn parse_migrate_args(args: &[String]) -> Result<MigrateArgs, String>;
pub fn is_migrate_subcommand(args: &[String]) -> bool;
pub fn print_migrate_help();
```

**W-2 resolver (crate-private):**

```rust
pub(crate) fn resolve_shard_key_for_entity(
    entity_key: &str,
    entity_state: &SerializableEntityState,
    pipelines: &[SerializablePipeline],
    shard_count: u16,
) -> io::Result<(usize, Option<String>)>;
```

Algorithm:
1. Look at `entity_state.streams.first().0` to find the stream name this entity participates in.
2. If no streams → `(0, None)` (keyless / static-only entity).
3. Find the matching `SerializablePipeline` in the registry; fail with `InvalidData` if not found.
4. Read `pipeline.key_field`. Empty string → `(0, None)` (keyless stream).
5. Otherwise synthesize `payload = json!({key_field: entity_key})` and compute
   `(shard_hint_for_event(&payload, Some(&key_field)) as usize) % shard_count`.

Because `shard_hint_for_event` hashes the string VALUE (not the field name), this reproduces ingest routing exactly for any stream where `entity_key == ingest_event[key_field]`. That's the Phase 49 entity-keying invariant.

**14-step flow (excerpt):**

```rust
let lock_path = data_dir.join(".beava.lock");
let lock_file = File::create(&lock_path)?;
lock_file.try_lock_exclusive().map_err(|_| {
    io::Error::new(ErrorKind::WouldBlock, "data-dir is held by a running server ...")
})?;

// Early-exit: fjall/ exists, !force, !resume → no-op
// Marker write (if not resume)
// Read snapshot (prefer .bak if present — see D-04-01)
// Open keyspace + N partitions
// For each entity: resolve shard → (resume skip via contains_key) → insert
// PersistMode::SyncData fence every 1000 entities
// PersistMode::SyncAll final fence
// Drop partitions + keyspace (final journal flush on Drop)
// Write metadata-only snapshot (entities: Vec::new())
// Rename original → .v8.bak (only if bak doesn't already exist)
// --replace → remove .bak
// Remove marker
// Return MigrationReport
```

### 2. `src/lib.rs` — module registration

```rust
#[cfg(not(feature = "state-inmem"))]
#[allow(missing_docs)]
pub mod migrate_to_fjall;
```

Gated behind default build — the state-inmem path has no fjall to migrate to.

### 3. `src/main.rs` — CLI dispatch

Inserted before the existing reshard subcommand branch (both are offline tools that exit after completion). Guarded by `#[cfg(not(feature = "state-inmem"))]`. Prints a one-line summary banner with `entities_migrated / entities_skipped / duration_ms / streams_resolved / streams_keyless / bak_path`.

### 4. `src/reshard/mod.rs` — fjall awareness

Two surgical changes:

**a. Marker refusal** (early in `reshard_data_dir`):

```rust
if data_dir.join(".migration-in-progress").exists() {
    return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "migration in progress: run 'tally migrate-to-fjall --data-dir <path>' first",
    ));
}
```

**b. Fjall-source branch** (after directory-tree creation):

```rust
#[cfg(not(feature = "state-inmem"))]
let fjall_source = {
    let fjall_dir = data_dir.join("fjall");
    fjall_dir.is_dir()
};

#[cfg(not(feature = "state-inmem"))]
if fjall_source {
    reshard_from_fjall(data_dir, out_dir, from_n, to_k, &base_snap)?;
}
```

`reshard_from_fjall` opens the source keyspace (N partitions), opens the destination keyspace (K partitions), iterates each source partition via `partition.iter()`, decodes the postcard `SerializableEntityState`, routes each entity through the local `resolve_shard_for_reshard` (same W-2 contract as migrate-to-fjall's resolver — see D-04-02), and inserts the raw bytes into the target partition. Final `PersistMode::SyncAll` before drop.

**Output snapshot metadata:** when `fjall_source == true`, the output `snapshot.bin` has `entities: Vec::new()` (fjall-layout output). Legacy path keeps inline entities.

The existing event-log walk (Phase 52-04's per-shard log rehash) runs unchanged for both branches — event logs and entity state live side-by-side on disk.

### 5. `tests/test_migrate_to_fjall.rs` — 526 lines, NEW, 8 tests

| # | Test | What it asserts |
|---|------|-----------------|
| 1 | `fresh_migration_converts_entities_to_fjall` | 100 entities on 2 shards via `user_id` → each entity lands in `shard_hint_for_event({"user_id": k}, Some("user_id")) % 2`. `snapshot.v8.bak` preserved; `snapshot.bin` is metadata-only; `streams_resolved == 1`. |
| 2 | `idempotent_second_run_exits_with_already_migrated_and_no_changes` | Second run without `--force` returns `entities_migrated: 0`; partition key-sets unchanged. |
| 3 | `resume_from_marker_inserts_missing_entities_only` | Pre-seeded 10 entities + marker + 20-entity snapshot → `entities_skipped == 10, entities_migrated == 10`. |
| 4 | `bak_preserved_unless_replace_passed` | `replace=false` → bak exists; `replace=true` → bak absent. |
| 5 | `lock_contention_returns_would_block` | fs2 lock held externally → `Err(WouldBlock)`. |
| 6 | `force_flag_remigrates_overwriting_existing_fjall` | Delete a key from fjall, re-migrate `force=true`, key is restored. |
| 7 | `per_stream_shard_key_routing_matches_production` | **W-2 parity:** 3 streams (`user_id`, `account`, keyless) at N=4 → each entity in the correct shard per its OWN key_field; `streams_resolved == 2`, `streams_keyless >= 1`. |
| 8 | `cli_helpers_exist` | Smoke: `is_migrate_subcommand`, `parse_migrate_args`, `MigrationReport` resolve at compile time. |

Test 7 is the W-2 guard: if the implementation hardcoded `"key"` or `"user_id"` as the field name, entities from `stream_b` (account) and `stream_c` (keyless) would route wrong. The test asserts each entity lives in the partition matching its own stream's routing contract.

### 6. `tests/test_reshard_fjall_aware.rs` — 253 lines, NEW, 3 tests

| # | Test | What it asserts |
|---|------|-----------------|
| 1 | `reshard_from_fjall_data_dir_produces_rehashed_output` | Migrate 6 entities to fjall at N=1, reshard to N=2 → `out/fjall/` exists; every entity in correct N=2 shard. |
| 2 | `reshard_refuses_when_migration_in_progress_marker_exists` | Marker present → `Err` with message containing `"migration in progress"`. |
| 3 | `reshard_back_compat_no_fjall_still_reads_snapshot_entities` | Legacy (pre-Phase-53) data dir without `fjall/` still reshards via the inline entities path. |

---

## Verification

### D-08 per-file test counts (W-7 closure — NOT lumped)

| Command | Result |
|---------|--------|
| `cargo test --test test_migrate_to_fjall -- --test-threads=1` | **8 passed, 0 failed** (7 named tests + 1 CLI smoke) |
| `cargo test --test test_reshard_fjall_aware -- --test-threads=1` | **3 passed, 0 failed** |
| `cargo test --test test_reshard_cli` | **9 passed, 0 failed** (back-compat guard) |
| `cargo test --lib -- --test-threads=1` | **883 passed, 0 failed** |
| `cargo build` (default / fjall) | green |
| `cargo build --features state-inmem` | green |

Raw output snippet:

```
running 8 tests
test bak_preserved_unless_replace_passed ... ok
test cli_helpers_exist ... ok
test force_flag_remigrates_overwriting_existing_fjall ... ok
test fresh_migration_converts_entities_to_fjall ... ok
test idempotent_second_run_exits_with_already_migrated_and_no_changes ... ok
test lock_contention_returns_would_block ... ok
test per_stream_shard_key_routing_matches_production ... ok
test resume_from_marker_inserts_missing_entities_only ... ok
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.27s

running 3 tests
test reshard_back_compat_no_fjall_still_reads_snapshot_entities ... ok
test reshard_from_fjall_data_dir_produces_rehashed_output ... ok
test reshard_refuses_when_migration_in_progress_marker_exists ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.06s
```

### Acceptance-criteria grep grid

| Check | Expected | Actual |
|-------|----------|--------|
| `wc -l < src/migrate_to_fjall/mod.rs` | ≥ 180 | 492 ✓ |
| `grep -c "pub fn migrate_to_fjall" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub struct MigrationReport" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c "fn resolve_shard_key_for_entity" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub streams_resolved" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c "pub streams_keyless" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c ".migration-in-progress" src/migrate_to_fjall/mod.rs` | ≥ 2 | 5 ✓ |
| `grep -c "try_lock_exclusive" src/migrate_to_fjall/mod.rs` | 1 | 1 ✓ |
| `grep -c "PersistMode::Sync" src/migrate_to_fjall/mod.rs` | ≥ 2 | 2 ✓ |
| `grep -c "snapshot.v8.bak" src/migrate_to_fjall/mod.rs` | ≥ 2 | 8 ✓ |
| `grep -c "migrate_to_fjall" src/main.rs` | ≥ 2 | 5 ✓ |
| `grep -c "pub mod migrate_to_fjall" src/lib.rs` | 1 | 1 ✓ |
| `grep -c "fn reshard_from_fjall" src/reshard/mod.rs` | 1 | 1 ✓ |
| `grep -c "migration in progress" src/reshard/mod.rs` | ≥ 1 | 1 ✓ |
| `grep -c "fjall_dir.is_dir" src/reshard/mod.rs` | ≥ 1 | 1 ✓ |
| `wc -l < tests/test_migrate_to_fjall.rs` | ≥ 200 | 526 ✓ |
| `wc -l < tests/test_reshard_fjall_aware.rs` | ≥ 100 | 253 ✓ |
| `grep -c "fn fresh_migration_converts" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn idempotent_second_run" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn resume_from_marker" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn bak_preserved_unless_replace" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn lock_contention_returns_would_block" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn force_flag_remigrates" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "fn per_stream_shard_key_routing_matches_production" tests/test_migrate_to_fjall.rs` | 1 | 1 ✓ |
| `grep -c "migration in progress" tests/test_reshard_fjall_aware.rs` | ≥ 1 | 2 ✓ |
| HEAD commits | `test(53-04): RED …` then `feat(53-04): GREEN …` | 60ce63f, 13f4cc2 ✓ |

All pass.

---

## Scope-Boundary Audit (MUST-HOLD invariants from execution prompt)

| Invariant | Status | Evidence |
|-----------|--------|----------|
| `tally migrate-to-fjall` CLI works (idempotent, resumable, fs2-locked) | HELD | Tests 2 (idempotency), 3 (resume), 5 (lock contention) all green. |
| `resolve_shard_key_for_entity` helper with 3 paths present | HELD | `grep -c "fn resolve_shard_key_for_entity" src/migrate_to_fjall/mod.rs == 1`; 3 paths in source: no-streams → `(0, None)`; empty key_field → `(0, None)`; non-empty key_field → synth payload + shard_hint. |
| `cargo test --test test_migrate_to_fjall -- --test-threads=1` → 7+ passed | HELD | 8 passed. |
| `cargo test --test test_reshard_fjall_aware -- --test-threads=1` → 3 passed | HELD | 3 passed. |
| `tally reshard` reads both v8 and fjall sources | HELD | Test 1 (fjall source) + test 3 (legacy source) both green; test 2 proves marker refusal. |
| TDD RED → GREEN commit split | HELD | `60ce63f` RED (imports unresolved) → `13f4cc2` GREEN (all tests pass). No squash. |
| `53-04-SUMMARY.md` created | HELD | This file. |
| Closes TPC-PERSIST-03 | HELD | Migration + fjall-aware reshard both shipped with W-2 correctness. See Requirements Status below. |
| No STATE.md / ROADMAP.md writes | HELD | `git log --stat 60ce63f^..HEAD -- .planning/STATE.md .planning/ROADMAP.md` → no changes. |

---

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug] Resume / force paths must read from `snapshot.v8.bak` when `snapshot.bin` is metadata-only**

- **Found during:** Task 2 Step 5 (running `force_flag_remigrates_overwriting_existing_fjall` — test 6 initially failed)
- **Issue:** The plan's recipe (Step 4) says "Load `data_dir/snapshot.bin` via `load_snapshot_file`" unconditionally. But after a successful first migration, `snapshot.bin` has `entities: Vec::new()` (metadata-only). On `force=true` or `resume`, reading from `snapshot.bin` yields an empty entity list — the migration iterates 0 entities, no re-insert happens, and the test's deleted entity is never restored. This is a correctness bug that would silently lose entities on any re-run.
- **Fix:** In Step 4, prefer `snapshot.v8.bak` as the snapshot source whenever it exists. Fresh runs (no prior migration) still read `snapshot.bin` directly. This matches the plan's intent ("bak preserved") with its operational reality ("source-of-truth is bak after a successful migration").
- **Files modified:** `src/migrate_to_fjall/mod.rs` (~5 lines in Step 4 block)
- **Commit:** `13f4cc2`
- **Test validation:** Test 6 (`force_flag_remigrates_overwriting_existing_fjall`) now green — force mode correctly re-reads from `.bak`, re-inserts all entities, restores the deleted key.

**2. [Rule 3 — Blocking] Integration tests need a poison-tolerant env lock**

- **Found during:** Task 2 initial test run
- **Issue:** When the `force_flag_remigrates` test panicked (Bug 1 above), its `env_lock().lock().unwrap()` guard's `Drop` poisoned the process-global mutex. Subsequent tests unwrapping the lock also panicked (with `PoisonError`) — cascading failures masking the real bug.
- **Fix:** Added a `lock_env()` helper in both test files that does `match env_lock().lock() { Ok(g) => g, Err(poisoned) => poisoned.into_inner() }`. Tests still serialize env mutation; a genuine test failure no longer cascades.
- **Files modified:** `tests/test_migrate_to_fjall.rs`, `tests/test_reshard_fjall_aware.rs`
- **Commit:** `13f4cc2` (bundled with GREEN; pre-dated the cascade fix for Bug 1)

**3. [Rule 3 — Blocking] State-inmem build cannot see `SerializableEntityState` / `SerializablePipeline` (unused imports under cfg)**

- **Found during:** `cargo build --features state-inmem`
- **Issue:** `src/reshard/mod.rs` imported `SerializableEntityState` and `SerializablePipeline` unconditionally, but only the `#[cfg(not(feature = "state-inmem"))]` fjall helpers used them. Under state-inmem the imports became unused → `-D warnings` builds would fail.
- **Fix:** Moved those two imports into a `#[cfg(not(feature = "state-inmem"))]`-gated `use` block alongside the other fjall-only imports.
- **Files modified:** `src/reshard/mod.rs`
- **Commit:** `13f4cc2`

### Architectural Decisions

None required. No Rule 4 checkpoints.

### Authentication Gates

None.

---

## Requirements Status

| Requirement | Status | Evidence |
|-------------|--------|----------|
| TPC-PERSIST-03 | **CLOSED** | (a) `tally migrate-to-fjall --data-dir PATH [--force] [--replace]` CLI lands in `src/main.rs` and `src/migrate_to_fjall/mod.rs`. (b) Idempotent: test 2 green. (c) Resumable: test 3 green. (d) `snapshot.v8.bak` preserve + `--replace` semantics: test 4 green. (e) fs2 lock safety: test 5 green. (f) `tally reshard` reads both legacy and fjall sources: tests 1 + 3 in `test_reshard_fjall_aware.rs` green. (g) W-2 per-stream routing parity: test 7 green — `stream_b` with `account` key_field routes via `account` value (not a hardcoded `"key"` or `"user_id"`), and keyless `stream_c` routes to shard 0. |

---

## Known Stubs

None. Every function is fully implemented. The `resolve_shard_for_reshard` helper duplicated in `src/reshard/mod.rs` is intentional (D-04-02) — not a stub, a deliberate local copy for modular hygiene.

**Intentional non-work:**
- Composite shard_keys (multi-field) are not supported in v1.2 (no stream in the codebase uses one — verified against `src/pipeline/register.rs` key_field parsing). If a future phase introduces them, `resolve_shard_key_for_entity` will fail fast with `InvalidData` rather than silently misroute. This is the plan's <decisions> D-07 contract, honored here.

---

## Deferred Issues

- **`tally dump-to-v8` reverse migration** — 53-CONTEXT §Deferred; nice-to-have, not in Phase 53 scope.
- **`reshard --replace` atomicity for fjall dirs** — `swap_replace` uses `fs::rename`; with a fjall keyspace this still works (rename of a directory containing a fjall keyspace is atomic on POSIX). No code change needed.
- **Migration progress metrics (`beava_migration_entities_total{shard}`)** — not required by TPC-PERSIST-03 (ops-docs requirement is TPC-PERSIST-06, Plan 06). Report-mode banner in main.rs is sufficient for v1.2.

---

## Threat Flags

None. All surface changes are covered by the plan's STRIDE register (T-53-04-01 through T-53-04-06). Specifically:

- **T-53-04-01 (mid-flight interrupt):** `.migration-in-progress` marker + `partition.contains_key` skip + `snapshot.v8.bak` preserve — all verified by test 3 + test 4.
- **T-53-04-02 (concurrent server):** fs2 exclusive lock on `.beava.lock` — verified by test 5.
- **T-53-04-03 (path traversal):** `--data-dir PATH` is passed directly to `Path::join` without canonicalization. The test suite only passes `TempDir` paths; production path hardening is unchanged from Phase 52-04 reshard.
- **T-53-04-04 (OOM on 1B entities):** v8 snapshot always fully loaded; documented. Plan 06 will document operational mitigation.
- **T-53-04-05 (--replace before durability):** `keyspace.persist(PersistMode::SyncAll)` at step 8 completes BEFORE step 12's bak removal. Code order enforces the invariant.
- **T-53-04-06 (W-2 silent misplacement):** `resolve_shard_key_for_entity` reads each stream's actual `key_field`; test 7 proves per-stream parity against production routing.

---

## Test / Verify Commands

```bash
# D-08: per-file test counts
cargo test --test test_migrate_to_fjall -- --test-threads=1     # 8 passed
cargo test --test test_reshard_fjall_aware -- --test-threads=1  # 3 passed

# Back-compat: Phase 52-04 reshard still green
cargo test --test test_reshard_cli                              # 9 passed

# Full lib suite (no regressions)
cargo test --lib -- --test-threads=1                            # 883 passed

# Both build variants green
cargo build
cargo build --features state-inmem

# Manual
cargo run -- migrate-to-fjall --help
```

All green on macOS (Darwin 24.3.0) at commit `13f4cc2`.

---

## Self-Check: PASSED

**Files verified present:**

- [x] `src/migrate_to_fjall/mod.rs` — 492 lines; FOUND
- [x] `tests/test_migrate_to_fjall.rs` — 526 lines; FOUND; 8 tests
- [x] `tests/test_reshard_fjall_aware.rs` — 253 lines; FOUND; 3 tests
- [x] `src/lib.rs` — gated `pub mod migrate_to_fjall` present
- [x] `src/main.rs` — migrate-to-fjall dispatch present
- [x] `src/reshard/mod.rs` — marker check + `reshard_from_fjall` + fjall-source branch present

**Commits verified present:**

- [x] `60ce63f` `test(53-04): RED — migrate-to-fjall (7 tests incl. W-2 parity) + reshard fjall-aware (3 tests)`
- [x] `13f4cc2` `feat(53-04): GREEN — tally migrate-to-fjall with per-stream shard_key routing (W-2) + reshard fjall-aware`

**Verification outputs verified:**

- [x] `cargo test --test test_migrate_to_fjall -- --test-threads=1` — 8/8 PASS
- [x] `cargo test --test test_reshard_fjall_aware -- --test-threads=1` — 3/3 PASS
- [x] `cargo test --test test_reshard_cli` — 9/9 PASS
- [x] `cargo test --lib -- --test-threads=1` — 883/883 PASS
- [x] `cargo build` — green
- [x] `cargo build --features state-inmem` — green

**Scope-boundary invariants verified:**

- [x] No write to `.planning/STATE.md` by this plan
- [x] No write to `.planning/ROADMAP.md` by this plan
- [x] TDD RED → GREEN commit split preserved
- [x] W-2 per-stream routing parity proven by test 7 (3 streams, 3 different key_fields)
- [x] D-08 per-file test counts reported (not lumped)

Deviations 1 (resume-from-bak correctness bug), 2 (poison-tolerant env lock), and 3 (cfg-gated imports under state-inmem) are all documented above as auto-fixes.
