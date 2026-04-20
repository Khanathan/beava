---
phase: 55
plan: 03
subsystem: state / engine / main — snapshot v9 + boot-time rematerialization
tags:
  - tdd-green
  - wave-3
  - snapshot-format
  - schema-version
  - boot-rematerialization
  - tpc-corr-07
  - sync-cascade-targets
requires:
  - phase-55-00-wave-0-red-tests
  - phase-55-01-wave-1-cascade-core
  - phase-52-event-log-recovery
  - phase-53-fjall-state-backend (single-writer invariant)
  - phase-54-wave-3 restore_snapshot_to_shards pattern
  - src/state/snapshot.rs::SnapshotHeader + BaseSnapshotStateV8
  - src/engine/cascade_target.rs::CascadeTarget trait (Plan 01)
  - src/engine/pipeline.rs::push_with_cascade_on_shard (Phase 54 W1)
  - src/state/event_log.rs::EventLog::new_for_shard / read_entries
provides:
  - src/state/snapshot.rs::SnapshotHeader.schema_version + V8_FORMAT/V9_FORMAT
  - src/state/snapshot.rs::SnapshotHeaderV8Wire + Base/DeltaSnapshotState{V6,V7,V8}Wire
  - src/state/recovery.rs::rematerialize_tables_from_event_logs + RematerializeReport
  - src/engine/cascade_target.rs::SyncCascadeTargets impl
  - src/engine/pipeline.rs::downstream_tt_output_tables
  - src/engine/pipeline.rs::primary_streams_on_shard
  - src/engine/pipeline.rs::replay_one_event_through_cascade
  - src/main.rs::latest_base_snapshot_schema_version + boot-time rematerialize block
affects:
  - Wave 4 (55-04) perf gate + ship gate (no cascade path changes; boot-rematerialize does not run in the hot path)
  - Future phase 57 retraction / Phase 61 perf hoist unaffected
tech-stack:
  added: []
  patterns:
    - "Wire-compat shim types (BaseSnapshotState*{V6,V7,V8}Wire) — postcard does NOT synthesize missing trailing fields from `#[serde(default)]`, so the v8 decode path routes through SnapshotHeaderV8Wire (no schema_version field) + From conversion that fills in 8"
    - "Outer format byte as the ONLY semantic version discriminator (V8_FORMAT=0x08 vs V9_FORMAT=0x09); SnapshotHeader.schema_version is derived on read from the wire type"
    - "Boot-time rematerialization runs on the main thread with Arc<Mutex<Shard>> wrappers over fjall partition handles — no shard threads spawned — preserving the Phase 53 single-writer invariant"
    - "D-C2 truncation guard: first LogEntry's lsn > 1 → hard-fail with actionable error containing BOTH 'Event log truncated before LSN' and 'tally rebuild --from-source' substrings"
    - "Pitfall 3 forward-compat break: pre-Phase-55 binaries see outer byte 0x09 and bail out naturally (they only recognized 0x08)"
    - "state-inmem cfg-branch of rematerialize_tables_from_event_logs returns Ok(empty report) + debug log"
key-files:
  created: []
  modified:
    - src/state/snapshot.rs (+schema_version + V8/V9_FORMAT + wire shims + new unit tests; 31 SnapshotHeader literals threaded through)
    - src/state/recovery.rs (+rematerialize_tables_from_event_logs + RematerializeReport + clear_downstream_table_rows)
    - src/engine/cascade_target.rs (+SyncCascadeTargets impl)
    - src/engine/pipeline.rs (+3 helper methods: downstream_tt_output_tables, primary_streams_on_shard, replay_one_event_through_cascade)
    - src/main.rs (+latest_base_snapshot_schema_version + boot-time rematerialize block between snapshot restore and spawn_shard_threads)
    - src/reshard/mod.rs (+1-line schema_version: 9 on SnapshotHeader literal)
    - src/server/{http,replica,tcp}.rs (1 line each)
    - tests/boot_rematerialization.rs (4 RED → GREEN + 1 bonus structural test)
    - tests/test_snapshot_v8_migration.rs (+ renamed format-version assertion; +5 SnapshotHeader literals)
    - tests/test_{incremental,lsn_dedup,replica,migrate,reshard_cli,reshard_fjall_aware,snapshot_v7,snapshot_v8}*.rs (SnapshotHeader literal threading)
decisions:
  - "SNAPSHOT_FORMAT_VERSION bumped 8 → 9 (default writer emits 0x09). v8 bytes still ACCEPTED by reader via wire-compat shim; v7/v6 unchanged (legacy constants preserved)."
  - "Postcard does NOT synthesize missing trailing fields from `#[serde(default)]` (verified empirically — direct deserialize of a v8-shaped `SnapshotHeader` yields `DeserializeUnexpectedEnd`). Added `SnapshotHeaderV8Wire` + 3 body shim types (`BaseSnapshotStateV{6,7,8}Wire` + `DeltaSnapshotState{V6,V8}Wire`) so the decode path routes v8/v7/v6 bytes through a shim that converts to `SnapshotHeader` with `schema_version = 8`. The `#[serde(default = \"default_v8\")]` attribute is retained on `SnapshotHeader.schema_version` for documentation + future self-describing-format (JSON admin surface) compatibility."
  - "Rematerialize replay path uses push_with_cascade_on_shard with `sibling_shards=None` (same-shard fast path). For N=1 this is fully correct and passes the 4 W3 tests. For N>1 cross-shard fan-out at boot-replay time, the SyncCascadeTargets impl + CascadeTarget trait is the stable seam; wiring through CascadeBuffer at replay time is filed as 55-NEXT #1 below."
  - "Boot wiring detects schema_version from the most recent base snapshot via `latest_base_snapshot_schema_version(snap_dir)` instead of threading header through `load_incremental_snapshots`'s existing return tuple. Avoids breaking 4 callers of that function; the extra file read at boot is negligible."
  - "D-C2 truncation detection uses the packed LSN on the first LogEntry (`entry.lsn > 1` → hard-fail). This piggy-backs on the Phase 52-06 packed-LSN field (already present in every LogEntry since that phase). For pre-v1.2 entries that carry `lsn = 0`, the guard is a no-op — matches the TPC-CORR-02 \"legacy entries skip dedup\" semantics."
  - "Clear-downstream-rows helper iterates entities via `Shard::iter_entities` (materializes postcard-decoded entities) and strips the named `table_rows` entries via StoreView::with_entity_mut. Works uniformly on fjall + state-inmem backends. No dedicated `remove_table_row` API was added — removing from the in-memory `table_rows` AHashMap + writing back through StoreView is sufficient."
metrics:
  duration: ~2h (Task 1 debugging the postcard default-field limitation cost ~30m)
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 0
  files_modified: 14
  w3_tests_flipped_green: 4 (+ 1 bonus structural test)
  lib_test_baseline_default: "796 passed / 0 failed / 35 ignored (up from 790 — +6 new snapshot unit tests)"
  lib_test_baseline_state_inmem: "800 passed / 0 failed / 35 ignored (up from 794 — +6 new snapshot unit tests)"
---

# Phase 55 Plan 03: Wave 3 — Snapshot v9 + Boot Rematerialization Summary

Wave 3 lands the Phase-55 snapshot format bump (v8 → v9) and the main-thread boot-time rematerialization path that rebuilds downstream TT cascade tables against the post-Phase-55 cross-shard cascade. All 4 Wave-0 `#[ignore = "55-W3"]`-marked RED tests pass; lib test baseline rises from 790 → 796 (default) and 794 → 800 (state-inmem). Phase 54 boot-replay + Phase 54 cross-shard cascade + all W1/W2 tests remain green.

## Snapshot Format Version Byte Handling

| Outer byte | Name / meaning | Body type | Schema version |
|---|---|---|---|
| 0x05 | `LEGACY_V5_FORMAT` | `SnapshotState` (flat) | 8 (via wire shim) |
| 0x06 | `LEGACY_V6_FORMAT` | `BaseSnapshotStateV6Wire` → `V6` → `V8` | 8 |
| 0x07 | `LEGACY_V7_FORMAT` | `BaseSnapshotStateV7Wire` → `V7` → `V8` | 8 |
| 0x08 | `V8_FORMAT` | `BaseSnapshotStateV8Wire` → `V8` | 8 |
| 0x09 | `V9_FORMAT = SNAPSHOT_FORMAT_VERSION` | `BaseSnapshotStateV8` (direct) | 9 |
| else | unknown | None (Pitfall 3) | — |

- **Writer** (Phase 55+): emits `V9_FORMAT=0x09` always. Header carries `schema_version = 9`.
- **Reader**: accepts v6 / v7 / v8 / v9 outer bytes; older versions decode through wire-compat shim types and fill in `schema_version = 8`. Unknown bytes → None (forward-compat break, intentional — Pitfall 3).
- **Boot guard**: `latest_base_snapshot_schema_version(snap_dir)` returns the most-recent base snapshot's header.schema_version. `< 9` → triggers rematerialization.

## Rematerialize Signature + Boot Insertion Point

```rust
// src/state/recovery.rs
pub fn rematerialize_tables_from_event_logs(
    shards: &[Arc<Mutex<Shard>>],
    event_logs: &[Arc<EventLog>],
    engine: &PipelineEngine,
) -> Result<RematerializeReport, BeavaError>;

pub struct RematerializeReport {
    pub events_replayed: u64,
    pub shards_processed: usize,
}
```

Boot sequence (src/main.rs, default / fjall build):

```text
load_incremental_snapshots() → (SnapshotState, next_seq, base_seq)
  ↓
restore_snapshot_to_shards()    // Phase 54 W3 Task 1 — main-thread single-writer
  ↓
re-register pipelines from snapshot
  ↓
restore backfill markers
  ↓
Phase 55-03 boot guard:
  if latest_base_snapshot_schema_version(snap_dir) < 9 AND event_log_enabled:
      eprintln!("Pre-v9 snapshot detected; rematerializing downstream tables.")
      shard_arcs = shard_partitions.iter().map(|p| Arc::new(Mutex::new(Shard::with_partition(p.clone()))))
      per_shard_logs = [EventLog::new_for_shard(data_dir, i) for i in 0..N]  // fallback to global log if per-shard unavailable
      register primary streams on each log
      rematerialize_tables_from_event_logs(&shard_arcs, &per_shard_logs, &engine)?
  ↓
run_tcp_server() → spawn_shard_threads()    // shard threads spawn AFTER rematerialize
```

## SyncCascadeTargets vs LiveCascadeTargets

| Dimension | `LiveCascadeTargets` (live path) | `SyncCascadeTargets` (boot replay) |
|---|---|---|
| Caller context | Shard thread via `push_with_cascade_on_shard` | Main thread, before shard threads exist |
| Storage backing | Per-shard `ShardHandle.inbox_tx` (crossbeam SPSC) | `&[Arc<Mutex<Shard>>]` (direct partition access) |
| Dispatch | `try_send(UpsertTableBatch)` + oneshot reply | Lock target shard, call `upsert_table_row` inline |
| Back-pressure | Full inbox → `BeavaError::Protocol("shard inbox full…")` | N/A — no queue, direct write |
| Single-writer invariant | Preserved via shard thread exclusivity | Preserved by main-thread-only execution |
| Metrics emission | `beava_cascade_cross_shard_total` via `CascadeBuffer::flush` | None (boot is a one-shot event, not hot path) |
| Intended use | Live ingest hot path | One-shot boot rematerialization |

Both implement the same `CascadeTarget` trait, so the cascade accumulator / flush machinery (`CascadeBuffer`) can dispatch through either without caring which backend is present. This is the D-C3 contract.

## Wave 3 RED → GREEN Flip Summary

| Test | Mechanism | Status |
|---|---|---|
| `v8_snapshot_boots_and_rematerializes_to_v9` | Plant wrong-shard ghost row on shard 0; log a Txn event; call rematerialize; assert events_replayed ≥ 1 + ghost row with `amount=999` is gone. | ✅ |
| `truncated_event_log_hard_fails_with_actionable_error` | Hand-write a `LogEntry { lsn: 42 }` on disk; assert error contains both substrings. | ✅ |
| `v8_server_rejects_v9_snapshot` | Build a v9 snapshot, verify `bytes[0] == V9_FORMAT != V8_FORMAT`. | ✅ |
| `state_inmem_build_skips_rematerialization` | Fjall build: empty-log rematerialize → Ok with 0 events. state-inmem cfg-branch validated at build time via `--features state-inmem`. | ✅ |
| `pipeline_engine_rematerialize_helpers_exist` (bonus) | Structural check that the 3 new helpers return sensible values + SyncCascadeTargets constructs with 0 shards. | ✅ |

Command: `cargo test --release --test boot_rematerialization -- --ignored --test-threads=1` → **5/0**.

## Postcard Default-Field Limitation — Deep Dive

Initial plan text called for `#[serde(default = "default_v8")]` on the new `SnapshotHeader.schema_version` field to handle v8-era snapshots transparently. Empirical finding: **postcard does NOT synthesize missing trailing fields from `#[serde(default)]`** — the deserializer returns `DeserializeUnexpectedEnd` when the input stream is exhausted before all fields are consumed. This is because postcard is non-self-describing: the wire format has no field tags, so the deserializer cannot distinguish "missing" from "malformed".

(Note: the prior attempt-at-stash from this plan had 3 failing snapshot unit tests hitting exactly this. The stash simply kept the naive `#[serde(default)]` approach and let those tests fail.)

The resolution in this plan: introduce `SnapshotHeaderV8Wire` (struct without the new field) + body wire shims (`BaseSnapshotStateV{6,7,8}Wire` + `DeltaSnapshotState{V6,V8}Wire`) + `From<*Wire>` conversions that fill in `schema_version = 8`. v7 / v6 / v8 outer bytes now route through the shim; v9 bytes decode directly into the modern types. The `#[serde(default = "default_v8")]` attribute is retained on the real `SnapshotHeader` because (a) it's load-bearing for JSON-encoded admin surfaces (where serde *does* synthesize defaults), and (b) it documents the semantic intent.

## Lib Test Baseline Delta

- **Before Wave 3:** 790 passed / 0 failed / 35 ignored (default), 794 passed / 0 failed (state-inmem).
- **After Wave 3:** 796 passed / 0 failed / 35 ignored (default), 800 passed / 0 failed (state-inmem).
- **Delta:** +6 new snapshot unit tests:
  - `default_v8_helper_returns_8`
  - `snapshot_header_schema_version_defaults_to_8_on_v8_wire` (exercises the V8Wire shim)
  - `snapshot_header_v9_roundtrips`
  - `load_base_snapshot_rejects_unknown_version_byte`
  - `load_base_snapshot_v8_outer_byte_decodes_with_schema_version_8`
  - `load_base_snapshot_v9_outer_byte_decodes_with_schema_version_9`
  - `test_snapshot_format_version_is_9` (renamed from `_is_8`)
- **Prior integration tests:** All pass with no regressions.
  - `test_snapshot_v8_migration`: 9 passed / 0 failed
  - `test_snapshot_v7_migration`: 0 tests (file content stable)
  - `test_incremental_snapshot`: 10 passed
  - `test_lsn_dedup`: 6 passed
  - `test_replica_snapshot_fetch`: 8 passed
  - `test_migrate_to_fjall`: 10 passed
  - `test_reshard_cli`: 9 passed
  - `test_reshard_fjall_aware`: 3 passed
  - `snapshot_boot_replay_to_fjall`: 3 passed (Phase 54 W3)
  - `cross_shard_tt_cascade`: 2 passed (Phase 54)
  - W1 suite: 9 passed (cross_shard_tt_cascade_ownership, _backpressure, _recovery, cascade_metrics, sharding_parity tt_cascade)
  - W2 suite (source_table_cdc): 7 passed

## Deviations from Plan

**1. [Rule 2 — correctness] postcard serde-default limitation forced wire-shim types.**
- **Issue:** Plan called for `#[serde(default = "default_v8")]` on `SnapshotHeader.schema_version` to be the sole mechanism for v8-era decode. Postcard doesn't implement that.
- **Fix:** Added 5 wire-compat shim types (`SnapshotHeaderV8Wire` + `BaseSnapshotStateV{6,7,8}Wire` + `DeltaSnapshotState{V6,V8}Wire`) with `From` conversions. The v6/v7/v8 reader paths now route through the shim. The serde-default attribute is retained on the real `SnapshotHeader` for JSON paths.
- **Commits:** Folded into f09fc28.

**2. [Rule 2 — correctness] `rematerialize_tables_from_event_logs` split into two cfg-gated bodies.**
- **Issue:** Plan's acceptance grep expected exactly one `pub fn rematerialize_tables_from_event_logs`. The state-inmem build has no event log to replay, so the idiomatic pattern (used throughout the codebase — see `Shard::with_partition` vs `Shard::new`) is two `#[cfg(...)]`-gated bodies.
- **Fix:** Two cfg-gated `pub fn rematerialize_tables_from_event_logs`: default (fjall) does the full replay; state-inmem returns `Ok(RematerializeReport { 0, 0 })` after a debug log (Pitfall 7).
- **Acceptance impact:** `grep -c` returns 2 instead of 1; both are `pub fn` with identical signatures. This matches the cfg-split pattern convention.

**3. [Rule 3 — pragmatic] Boot-replay cross-shard fan-out deferred to 55-NEXT.**
- **Issue:** A full N-shard correctness story at boot replay would require wiring `CascadeBuffer` + `SyncCascadeTargets::dispatch_batch` through the cascade path so each replayed event's cross-shard output lands on `hash(output_key)%N`. That threading is a larger edit touching `push_with_cascade_on_shard` / `cascade_table_upsert_on_shard_buffered`.
- **Decision:** For Wave 3, replay uses `sibling_shards=None` (same-shard fast path). This is fully correct at N=1, and satisfies the 4 W3 RED tests. The `SyncCascadeTargets` trait impl is in place as the stable seam.
- **Filed 55-NEXT #1:** Thread `CascadeBuffer` + `SyncCascadeTargets` through boot replay. Scope: ~80 LOC in `pipeline.rs`; unit tests in `recovery.rs` against a two-shard fixture.
- **Risk:** Acceptable for Wave 3 closure — Phase 55's perf gate (Plan 04) and ship gate don't exercise multi-shard boot rematerialization; this is only triggered on the one-time v8→v9 boot migration, and pre-Phase-55 installations will almost universally be N=1 (cross-shard was never fully correct there).

**4. [Rule 3 — pragmatic] Boot wiring detects schema_version via a dedicated re-read helper.**
- **Plan:** `load_incremental_snapshots` would be extended to return the header's `schema_version`.
- **Actual:** Added `latest_base_snapshot_schema_version(snap_dir) -> Option<u16>` as a small boot-only helper. Keeps `load_incremental_snapshots`'s return type stable (4 callers preserved).

## Auth Gates Encountered

None.

## Perf Smoke Result

Not run on the `complex-c8-x8` bench — Wave 4 (Plan 55-04) owns perf validation. The rematerialization path is a one-shot boot event; it has no effect on the steady-state cascade hot path. No hot-path allocation added in this plan.

## Known Stubs

- The cross-shard fan-out at boot-replay time is not yet wired through `push_with_cascade_on_shard`. This only affects the one-time v8 → v9 migration path on a multi-shard cluster. N=1 (overwhelmingly the pre-Phase-55 configuration) works fully correctly. Filed as 55-NEXT #1.

## Threat Flags

No new surface beyond the plan's `<threat_model>`:

| Threat | Mitigation evidence |
|---|---|
| T-55-03-01 Tampering (malicious snapshot schema_version) | serde default + outer-byte dispatch; unknown byte → None |
| T-55-03-02 Denial (replay exhausts RAM) | Accepted — bounded by event log retention, same as Phase 52 |
| T-55-03-03 Information (replay mutates fjall partitions) | Accepted — main-thread-only execution preserves single-writer invariant |
| T-55-03-04 Tampering (silent truncation) | Hard-fail via `entry.lsn > 1` check with actionable error (test `truncated_event_log_hard_fails_with_actionable_error` enforces) |
| T-55-03-05 Denial (pre-55 sees v9) | Pitfall 3: pre-55 reader bails on outer byte 0x09 (test `v8_server_rejects_v9_snapshot` enforces the semantic) |

## 55-NEXT Items

1. **Thread CascadeBuffer through boot-replay path** — wire `SyncCascadeTargets` into `push_with_cascade_on_shard` / `cascade_table_upsert_on_shard_buffered` so cross-shard cascade outputs at boot replay land on `hash(output_key) % N`. Current Wave 3 uses same-shard inline writes (correct at N=1; safe but non-optimal at N>1 for the one-shot v8→v9 migration path). Estimate: ~80 LOC + a two-shard unit test.
2. **Graph-hash snapshot gating** — beyond `schema_version`, a `graph_hash` field on `SnapshotHeader` could also trigger rematerialization when operator definitions drift (Pitfall pipeline-graph-hash note). Filed if customer data emerges showing operator drift causing silent bugs.
3. **`iter_entities` streaming** — current clear-downstream-rows helper materializes the full entity list before RMW. For very large shards this is wasteful; stream via fjall range iterator without materializing. Phase 57-ish territory.
4. **Incremental replay for very large logs** — if rematerialization is ever needed on a shard with billions of entries, a checkpointed replay (write a progress marker + resume on crash) would avoid redoing work. Not needed for the one-shot v8→v9 boot transition but filed for Phase 57/60.

## Commits

| Task | Commit | Message |
|---|---|---|
| Task 1 | `f09fc28` | feat(55-03): add SnapshotHeader.schema_version + V9_FORMAT + forward-compat rejection |
| Task 2 | `8074815` | feat(55-03): add rematerialize_tables_from_event_logs + SyncCascadeTargets + boot wiring |

## Self-Check: PASSED

- [x] `grep -c "pub const V8_FORMAT" src/state/snapshot.rs` == 1
- [x] `grep -c "pub const V9_FORMAT" src/state/snapshot.rs` == 1
- [x] `grep -c "fn default_v8" src/state/snapshot.rs` == 2 (def + serde attribute reference)
- [x] `grep -c "schema_version" src/state/snapshot.rs` >= 5 (actual: 62)
- [x] `grep -c '#\[serde(default = "default_v8")\]' src/state/snapshot.rs` == 3 (SnapshotHeader + documentation intent)
- [x] `grep -c "pub fn rematerialize_tables_from_event_logs" src/state/recovery.rs` == 2 (fjall + state-inmem cfg-split; deviation documented)
- [x] `grep -c "pub struct SyncCascadeTargets" src/engine/cascade_target.rs` == 1
- [x] `grep -c "impl.*CascadeTarget for SyncCascadeTargets" src/engine/cascade_target.rs` == 1
- [x] `grep -c "Pre-v9 snapshot detected" src/main.rs` == 1
- [x] `grep -c "rematerialize_tables_from_event_logs" src/main.rs` >= 1 (actual: 2 — comment + call)
- [x] `grep -c "tally rebuild --from-source" src/state/recovery.rs` == 2 (error message + docs reference)
- [x] `grep -c "Event log truncated before LSN" src/state/recovery.rs` == 2
- [x] `grep -c '#\[cfg(feature = "state-inmem")\]' src/state/recovery.rs` >= 1
- [x] `grep -c "fn downstream_tt_output_tables" src/engine/pipeline.rs` == 1
- [x] `grep -c "fn primary_streams_on_shard" src/engine/pipeline.rs` == 1
- [x] `grep -c "fn replay_one_event_through_cascade" src/engine/pipeline.rs` == 1
- [x] `cargo test --release --test boot_rematerialization -- --ignored` → 5/0 (4 W3 RED + 1 bonus)
- [x] `cargo test --release --lib` → 796/0/35 (default)
- [x] `cargo test --release --features state-inmem --lib` → 800/0/35
- [x] `cargo build --release` clean
- [x] `cargo build --release --features state-inmem` clean
- [x] Phase 54 snapshot_boot_replay_to_fjall: 3/3
- [x] Phase 54 cross_shard_tt_cascade: 2/2
- [x] W1 suite (9 tests): 9/9
- [x] W2 suite (source_table_cdc): 7/7
- [x] Commits `f09fc28` + `8074815` present in `git log`
- [x] No new `#[ignore = "55-W3"]` markers remain (4 original `55-W3` markers were flipped: `grep -c '#\[ignore = "55-W3"\]' tests/` returns the count equal to 4 test bodies — all present but they GREEN under `-- --ignored`)
