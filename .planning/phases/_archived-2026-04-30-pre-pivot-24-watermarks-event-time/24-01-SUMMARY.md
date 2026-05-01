---
phase: 24-watermarks-event-time
plan: 01
subsystem: state+snapshot
tags: [state, storage, snapshot, foundation, table-row]
dependency_graph:
  requires:
    - 23-03   # Phase 23's marker-based TT cascade & single-entity limitation
  provides:
    - TABLE-STORE-01   # EntityState.table_rows first-class field
    - TABLE-STORE-02   # upsert / tombstone / get primitives
    - TABLE-STORE-03   # gc_tombstones with 7d grace
    - SNAPSHOT-V7-01   # codec v7 with v6 migration
  affects:
    - src/state/store.rs      # TableRow + TableRowState, 4 new methods, EntityState shape
    - src/state/snapshot.rs   # SNAPSHOT_FORMAT_VERSION → 7; V6 legacy types & migration
tech-stack:
  added: []
  patterns:
    - serializable-shadow-type   # TableRow (AHashMap) ↔ SerializableTableRow (Vec<(k,v)>)
    - legacy-type-on-read        # v6 decode struct + From for v7 promotion
key-files:
  created:
    - tests/test_table_row_storage.rs
    - tests/test_snapshot_v7_migration.rs
  modified:
    - src/state/store.rs
    - src/state/snapshot.rs
decisions:
  - "TableRow.fields is AHashMap<String, FeatureValue> at runtime but projects through a parallel SerializableTableRow { fields: Vec<(k,v)>, state, updated_at } for serialization. AHashMap lacks serde derives in this codebase and we match the existing pattern used for SerializableEntityState.static_features."
  - "v6→v7 migration uses a parallel SerializableEntityStateV6 + BaseSnapshotStateV6 / DeltaSnapshotStateV6 rather than a #[serde(default)] field. postcard is not self-describing, so adding a field to the live struct would corrupt v6 decodes; the legacy-type-on-read approach mirrors Phase 9's v5→v6 pattern and keeps the runtime struct clean."
  - "TOMBSTONE_GRACE is a module-level `pub const` (7d) in store.rs rather than a #[cfg]-gated or config-file setting. The 7d default is locked by the v0-restructure-spec §3.1 @tl.table(tombstone_grace=\"7d\") contract; plan 03's cascade rework and a later phase's tunable per-table grace will read from this constant."
  - "get_table_row returns Option<TableRow> by clone rather than a DashMap Ref guard. Cloning a row (AHashMap + FeatureValues) is cheap relative to the simplification it buys callers — no shard-lock lifetimes leak into the call graph, which matters because plan 02 (opcodes) and plan 03 (cascade) will call get_table_row from handlers that already hold other guards."
  - "Task 1 and Task 2 were intentionally coupled in a single codec change. The plan separated them but EntityState gaining `table_rows` forces SerializableEntityState to grow `table_rows` in the same compile unit; splitting the commits would have left the repo in a broken state between them. Task 1 commit carries the full codec bump + v6 migration types; Task 2 commit carries only the dedicated migration test file."
metrics:
  duration: ~45min
  completed: 2026-04-14
  tasks: 2
  commits:
    - fa260a8   # Task 1: TableRow + TableRowState storage primitive on StateStore
    - 3ac04ad   # Task 2: v7 snapshot round-trip + v6→v7 migration tests
---

# Phase 24 Plan 01: Table row storage primitive — Summary

**One-liner:** Shipped `EntityState.table_rows` as a first-class
`AHashMap<String, TableRow>` with `Live | Tombstoned { since }` states,
four StateStore primitives (upsert / tombstone / get / gc_tombstones)
with a locked 7-day tombstone grace window, and snapshot codec v7 with
transparent v6-on-read migration — laying the storage foundation for
plan 02's TCP opcodes and plan 03's cascade migration off Phase 23's
marker scheme.

## What shipped

### 1. TableRow + TableRowState (commit `fa260a8`)

In `src/state/store.rs`:

```rust
pub const TOMBSTONE_GRACE: Duration = Duration::from_secs(7 * 86400);

pub enum TableRowState {
    Live,
    Tombstoned { since: SystemTime },
}

pub struct TableRow {
    pub fields: AHashMap<String, FeatureValue>,
    pub state: TableRowState,
    pub updated_at: SystemTime,
}

pub struct EntityState {
    pub streams: AHashMap<String, StreamEntityState>,
    pub static_features: AHashMap<String, StaticFeature>,
    pub table_rows: AHashMap<String, TableRow>,   // NEW
}
```

Four new `StateStore` methods:

| Method | Semantics |
| ------ | --------- |
| `upsert_table_row(key, table_name, fields, now)` | Replaces any prior (Live or Tombstoned) row with a fresh `Live`. Marks key dirty. |
| `tombstone_table_row(key, table_name, now)` | Flips to `Tombstoned { since: now }`. Creates a tombstone-only row if absent. Returns `true` iff a Live row existed. Marks key dirty. |
| `get_table_row(key, table_name)` | Returns `Option<TableRow>` by clone (both Live and Tombstoned variants — callers filter). |
| `gc_tombstones(now)` | Sweeps every entity via `DashMap::iter_mut`; drops Tombstoned rows older than `TOMBSTONE_GRACE`. Returns count removed. |

`EntityState::is_empty()` now also checks `table_rows.is_empty()` so
empty-entity eviction (`remove_empty_entities`) treats an entity with
only a Live row as non-empty.

### 2. Serializable shadow + v7 codec (commit `fa260a8`)

In `src/state/store.rs`, added `SerializableTableRow { fields:
Vec<(String, FeatureValue)>, state, updated_at }` with `From` impls in
both directions. `AHashMap` does not implement `serde::Deserialize` in
this codebase, so the AHashMap-to-Vec projection at the serialization
boundary matches how `SerializableEntityState.static_features` already
handles the same constraint.

In `src/state/snapshot.rs`:

* `SNAPSHOT_FORMAT_VERSION: u8 = 7`, new `LEGACY_V6_FORMAT: u8 = 6`.
* `SerializableEntityState` gained `table_rows:
  Vec<(String, SerializableTableRow)>`.
* Added `SerializableEntityStateV6`, `BaseSnapshotStateV6`,
  `DeltaSnapshotStateV6` as migration-read-only types with `From` impls
  that promote to v7 by initializing `table_rows` empty.
* `load_snapshot` and `load_snapshot_file` both grew a v6 branch that
  decodes the legacy struct and converts. v5 and v7 paths unchanged;
  delta snapshots on disk written before this plan continue to round-
  trip because `SerializableEntityState.table_rows` is always present
  in v7 writes.
* Exposed `save_base_snapshot_v6_for_test` /
  `save_delta_snapshot_v6_for_test` as plain `pub fn` (not feature-
  gated) so integration tests in `tests/` can exercise the v6 read
  path without duplicating encoding logic. Not used at runtime.

### 3. Dedicated test files

**`tests/test_table_row_storage.rs` (7 tests, all passing):**

| Test | Covers |
| ---- | ------ |
| `upsert_creates_live_row` | Fields and state after first upsert. |
| `tombstone_flips_live_to_tombstoned` | `since` = supplied now. |
| `tombstone_on_absent_creates_tombstone_only` | Returns false; row has empty fields. |
| `upsert_over_tombstone_resurrects` | State flips back to Live; fields replaced whole. |
| `gc_tombstones_respects_7d_grace` | 6d → 0 removed; 7d+1s → 1 removed. |
| `gc_tombstones_leaves_live_rows_alone` | Mix of Live / fresh-tomb / expired-tomb. |
| `table_rows_independent_from_static_features` | No cross-map leakage either direction. |

**`tests/test_snapshot_v7_migration.rs` (5 tests, all passing):**

| Test | Covers |
| ---- | ------ |
| `v7_roundtrip_table_rows` | Save → load preserves Live + Tombstoned rows and all fields. |
| `v7_roundtrip_tombstone_since_preserved` | `Tombstoned.since` survives byte-for-byte. |
| `v6_snapshot_loads_with_empty_table_rows` | Hand-crafted v6 base → load under v7 binary → streams + static_features preserved, `table_rows` empty. |
| `unknown_version_returns_none` | Version byte 0xFE → None (no panic, no deserialization). |
| `v7_mixed_live_tombstoned_gc_friendly` | Save → load → `restore_from_snapshot` → `gc_tombstones(t0+grace+1s)` removes only the expired tombstone. |

## Test results

* `cargo test --lib` — **679 / 679** (up from 678; added `test_snapshot_format_version_is_7` and `test_legacy_v6_format_constant`; replaced `test_snapshot_format_version_is_6`).
* `cargo test --test test_table_row_storage` — **7 / 7**.
* `cargo test --test test_snapshot_v7_migration` — **5 / 5**.
* `cargo test --test test_snapshot_hybrid_ops` — **6 / 6** (no regression).
* `cargo test --test test_snapshot_v0_ops` — **14 / 14** (no regression).
* `cargo test --test test_snapshot` — **7 / 7** (no regression).
* `cargo test --test test_incremental_snapshot` — **6 / 6** (no regression; delta snapshots route through the same `SerializableEntityState` so they automatically carry `table_rows` forward, currently always empty until plan 02 wires opcodes).
* `cargo test --test test_join_table_table` — **5 pass / 7 ignored** (unchanged Phase 23 baseline; un-ignoring happens in plan 03).
* `cargo test` (full suite) — all test results green across 30+ integration binaries.

## Deviations from plan

### [Intentional coupling] Task 1 commit includes the snapshot codec change

**Found during:** Task 1 build.

**Issue:** The plan scoped Task 1 to "types + StateStore methods" and
Task 2 to "snapshot codec bump + migration". But extending
`EntityState` with a new field forces `SerializableEntityState` to
grow the matching field *in the same commit* — otherwise
`clone_for_snapshot`, `restore_from_snapshot`, and `apply_delta`
wouldn't compile and the repo would be broken between the two commits.

**Fix:** Task 1 commit (`fa260a8`) carries the complete codec change
(v7 bump, legacy v6 types, load-path migration, test helpers). Task 2
commit (`3ac04ad`) carries only the dedicated
`tests/test_snapshot_v7_migration.rs` file. Functionally identical to
the plan; just a cleaner history.

### [Rule 2 — Missing functionality] `TableRow.fields` needed a serialization shadow

**Found during:** Task 1 first build.

**Issue:** `AHashMap<String, FeatureValue>` does not implement
`serde::Serialize` / `Deserialize` in this codebase (the `ahash` crate
is pulled in without the `serde` feature). Deriving Serialize/Deserialize on
`TableRow` failed with "trait bound `AHashMap: Deserialize<'de>` not
satisfied".

**Fix:** Added `SerializableTableRow` with
`fields: Vec<(String, FeatureValue)>` and `From` impls in both
directions. This matches how `SerializableEntityState.static_features`
projects the runtime `AHashMap<String, StaticFeature>` to a `Vec` at
the serialization boundary — same pattern, same rationale. No
architectural change; just a shadow type that serde can handle.

### [Rule 3 — Blocking issue] `#[cfg(any(test, feature = "test-helpers"))]` on test helpers

**Found during:** Task 2 build warnings.

**Issue:** My first pass on `save_base_snapshot_v6_for_test` was gated
on `#[cfg(any(test, feature = "test-helpers"))]`, but `test-helpers` is
not a declared feature in `Cargo.toml`. This emitted an
`unexpected_cfg` warning and — more importantly — integration tests in
`tests/*.rs` would not have seen the helper because they compile
against the non-test `lib` artifact.

**Fix:** Dropped the cfg gate and kept the helpers as plain `pub fn`
with doc-comments stating they're test-only. No runtime code path
references them; the helpers are exercised only by
`tests/test_snapshot_v7_migration.rs`. If the footprint matters later,
a proper `test-helpers` Cargo feature can be added in Phase 25.

## Known stubs

None introduced by this plan. Plan 24-01 is pure foundation; it adds
capacity without removing any existing surface:

* Phase 23's marker-based `cascade_table_upsert` still runs on
  `static_features`; plan 03 migrates it to `table_rows`.
* 7 ignored TT-join tests remain ignored; plan 03 un-ignores them.
* No TCP opcode wiring — `OP_PUSH_TABLE` / `OP_DELETE_TABLE` arrive in
  plan 02.

Delta snapshots written by a v7 binary always include a `table_rows`
field per entity (empty until plan 02 starts writing to it). The empty-
entity eviction path (`remove_empty_entities`) now also treats an
entity-with-only-a-live-table-row as non-empty, which is correct going
forward but means operators that relied on "entity is empty iff
streams + static_features empty" must be re-audited when plan 02/03
land. No such operator exists in the current tree.

## Threat flags

Plan's register (T-24-01-01 … 05) all mitigated or accepted as
designed:

* **T-24-01-01 (v6 migration tampering)** — mitigated. `load_snapshot`
  validates the version byte before invoking `postcard::from_bytes`;
  an unknown byte returns `None`. Covered by
  `unknown_version_returns_none`.
* **T-24-01-02 (gc_tombstones DoS)** — mitigated. `DashMap::iter_mut`
  acquires per-shard write locks, not a single global lock; the sweep
  is lock-bounded to the shard currently being processed.
* **T-24-01-03 (tombstone info disclosure)** — mitigated by contract.
  `get_table_row` returns the full `TableRow` including Tombstoned
  variant; the doc-comment is explicit that consumers must filter by
  `state`. Plan 02's GET handler and plan 03's cascade are the
  downstream consumers that implement the filter.
* **T-24-01-04 (tombstone timestamp repudiation)** — accepted for v0.
  `since: SystemTime` is the server wall-clock at tombstone time; no
  notarization.
* **T-24-01-05 (arbitrary FeatureValue in fields)** — accepted. Same
  attack surface the existing SET path already exposes.

## Self-Check: PASSED

Verified files exist (absolute paths):

* `/data/home/tally/src/state/store.rs` — FOUND (modified)
* `/data/home/tally/src/state/snapshot.rs` — FOUND (modified)
* `/data/home/tally/tests/test_table_row_storage.rs` — FOUND (created)
* `/data/home/tally/tests/test_snapshot_v7_migration.rs` — FOUND (created)
* `/data/home/tally/.planning/phases/24-watermarks-event-time/24-01-SUMMARY.md` — FOUND (this file)

Verified commits exist on `main`:

* `fa260a8` feat(24-01): TableRow + TableRowState storage primitive on StateStore
* `3ac04ad` test(24-01): v7 snapshot round-trip + v6→v7 migration tests

Verified test gates:

* `cargo test --lib` — 679 / 679
* `cargo test --test test_table_row_storage` — 7 / 7
* `cargo test --test test_snapshot_v7_migration` — 5 / 5
* `cargo test --test test_snapshot_hybrid_ops` — 6 / 6
* `cargo test --test test_join_table_table` — 5 / 5 + 7 ignored (unchanged)
* `cargo test --test test_incremental_snapshot` — 6 / 6
* `cargo test` (full suite) — all test binaries green

Phase 24 Plan 01 is complete. Plan 02 can now wire `OP_PUSH_TABLE` /
`OP_DELETE_TABLE` opcodes against `upsert_table_row` /
`tombstone_table_row`; plan 03 can rework `cascade_table_upsert` to
read from `table_rows` instead of `static_features` markers, which
will un-ignore the 7 Phase 23 TT-join tests.
