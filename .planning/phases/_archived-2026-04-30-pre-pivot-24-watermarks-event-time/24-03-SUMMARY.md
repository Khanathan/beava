---
phase: 24-watermarks-event-time
plan: 03
subsystem: engine+tests
tags: [engine, cascade, tt-join, tombstone, migration]
dependency_graph:
  requires:
    - 24-01   # EntityState.table_rows + upsert/tombstone/get primitives
    - 24-02   # OP_PUSH_TABLE / OP_DELETE_TABLE opcodes + merged GET view
  provides:
    - CASCADE-MIGRATE-01   # cascade_table_upsert reads/writes table_rows
    - TT-TESTS-UNIGNORE-01 # 7 previously-ignored TT tests un-ignored
  affects:
    - src/engine/pipeline.rs                      # rewritten cascade_table_upsert
    - tests/test_join_table_table.rs              # harness ported to table_rows; 7 #[ignore] removed
    - tests/test_tt_cascade_migration.rs          # new — 5 migration-focused tests
tech-stack:
  added: []
  patterns:
    - state-is-truth                              # cascade derives output from real row state, not shadow markers
key-files:
  created:
    - tests/test_tt_cascade_migration.rs
  modified:
    - src/engine/pipeline.rs
    - tests/test_join_table_table.rs
decisions:
  - "The `_tombstoned: bool` parameter on cascade_table_upsert is retained in the signature (prefixed `_` to silence dead-param warnings) for call-site compatibility with the two TCP handlers and the Phase 23 test aliases. The body no longer reads it — `get_table_row(...).state` is the ground truth. Rewriting the call sites was out of scope and would have churned tcp.rs for no behavioral gain."
  - "Left-join null-padding emits `FeatureValue::Missing` rather than an explicit `FeatureValue::Null` or dropping the field, because Missing is this codebase's canonical null-equivalent (it round-trips to JSON null via `to_json_value`) and it's what the existing j_field helper in the test harness already expects. This lets the same cascade output surface correctly through both the merged GET view and the direct get_table_row path."
  - "The `tt_cascades_recursively_through_chain` assertions moved from J to K. Under the Phase 23 marker model every cascade output landed in one shared entity's static_features, so `j_field(store, key, 'z')` returned the field from whichever source had written it. Under the new model K = J.join(C) lives in `table_rows['K']` and J in `table_rows['J']`, so chain correctness must be verified on K directly. Same intent (end-to-end cascade through A→B→J→K), cleaner observation."
  - "Dead-code removed: the `json_to_fv` helper in `pipeline.rs` was only used by the old marker cascade. Dropped along with the marker code in the same commit to keep the grep clean and avoid leaving a dormant shim that might silently get re-wired in a future plan."
metrics:
  duration: ~35min
  completed: 2026-04-14
  tasks: 3
  commits:
    - 5352e21   # Task 1: rewrite cascade_table_upsert against table_rows + 5 migration tests
    - b4f0038   # Task 2: un-ignore 7 TT-join tests; port harness to table_rows
---

# Phase 24 Plan 03: Cascade migration + un-ignore TT tests — Summary

**One-liner:** Migrated Phase 23's marker-based Table↔Table cascade to
the real `table_rows` storage from plan 01 — `cascade_table_upsert`
now reads `get_table_row(key, left_table)` / `get_table_row(key,
right_table)` and writes `upsert_table_row` / `tombstone_table_row`
on the output — un-ignoring all 7 TT tests deferred from Phase 23-03
(12/12 pass) and removing the `__tt_left_*` / `__tt_right_*` shadow
markers entirely.

## What shipped

### 1. Rewritten cascade (commit `5352e21`)

`src/engine/pipeline.rs::cascade_table_upsert` — full replacement of
the Phase 23-03 marker body. The method signature is unchanged so the
two TCP call sites (`handle_push_table`, `handle_delete_table`) and
the legacy SET-tombstone path keep working without edits:

```rust
pub fn cascade_table_upsert(
    &self,
    input_table: &str,
    key: &str,
    _tombstoned: bool,          // retained; no longer read — state IS truth
    store: &StateStore,
    now: SystemTime,
) -> Result<(), TallyError> {
    // For each registered TableTableJoin J referencing `input_table`:
    let left_row  = store.get_table_row(key, &left_table);
    let right_row = store.get_table_row(key, &right_table);
    let l_live = matches!(left_row.as_ref().map(|r| &r.state),  Some(TableRowState::Live));
    let r_live = matches!(right_row.as_ref().map(|r| &r.state), Some(TableRowState::Live));

    let (emit_live, null_right) = match join_type {
        JoinType::Inner => if l_live && r_live { (true, false) } else { (false, false) },
        JoinType::Left  => if l_live && r_live { (true, false) }
                           else if l_live       { (true, true)  }
                           else                 { (false, false) },
    };

    if emit_live {
        let mut merged: AHashMap<String, FeatureValue> = AHashMap::new();
        // left_fields copy from left_row; null-pad where absent.
        // right_fields map (src_in_right → emitted_in_output); respect null_right.
        // left wins on any emitted-name overlap.
        store.upsert_table_row(key, &output_name, merged, now);
    } else {
        store.tombstone_table_row(key, &output_name, now);
    }

    // Recurse so TT-join-of-TT-join stacks.
    self.cascade_table_upsert(&output_name, key, !emit_live, store, now)?;
}
```

Also dropped the dead `json_to_fv` helper that only existed to feed
the old marker code path.

### 2. Migration-focused tests (commit `5352e21`)

`tests/test_tt_cascade_migration.rs` — 5 tests, all passing, all
driving inputs via `store.upsert_table_row` / `tombstone_table_row`
and observing exclusively via `store.get_table_row(key, "J")`:

| Test | Covers |
| ---- | ------ |
| `cascade_migration_inner_both_live_merges` | A alone → no Live; A+B → Live merged (x,y). |
| `cascade_migration_inner_right_tombstone_retracts_output` | A+B live → J Live; tombstone B → J Tombstoned. |
| `cascade_migration_left_join_null_pads_missing_right` | left-only → Live with right=Missing; B added → merged; delete B → left stays Live + right back to Missing; delete A → J Tombstoned. |
| `cascade_migration_collision_suffix_through_real_storage` | Both sides have `status` → J gets `status` (left) + `status_right` (right). |
| `cascade_migration_recurses_through_chain_a_b_j1_c_j2` | A→B→J1, J1→C→J2 recursion; tombstone B retracts both J1 and J2. |

### 3. 7 TT-join tests un-ignored (commit `b4f0038`)

`tests/test_join_table_table.rs`:

* Dropped every `#[ignore = "v0: single-entity storage limitation ..."]`
  attribute (and the bare `#[ignore]` on `tt_inner_upsert_both_sides`).
* Rewrote the test harness:
  * `set_and_cascade` → `store.upsert_table_row(key, table, fields, now)`
    (was `set_static` per column + `cascade_tt_after_upsert`).
  * `delete_and_cascade` → `store.tombstone_table_row(key, table, now)`
    (was `store.delete_entity(key)`, which wiped everything including
    the other-side's input row).
  * `j_field` → reads from `store.get_table_row(key, "J")` filtered on
    `TableRowState::Live`; returns JSON Null for absent/tombstoned rows
    OR missing fields.
  * `j_absent` → true iff the J row is missing or Tombstoned.
* Refreshed the top-of-file banner to point to this plan.
* Adjusted `tt_cascades_recursively_through_chain` to assert on K (the
  final cascade output Table) directly rather than on the flattened
  merged-entity view the marker model produced.

Final count: **12 passed, 0 ignored, 0 failed.**

## Test results

### Primary gates

| Suite | Result |
| ----- | ------ |
| `cargo test --test test_tt_cascade_migration` | **5 / 5** |
| `cargo test --test test_join_table_table` | **12 / 12, 0 ignored** |
| `cargo test --lib` | **682 / 682** |

### Phase 23 regression gauntlet (all green)

| Suite | Result |
| ----- | ------ |
| `cargo test --test test_join_stream_table` | **6 / 6** |
| `cargo test --test test_join_stream_stream` | **14 / 14** |
| `cargo test --test test_join_integration` | **3 / 3** |
| `cargo test --test test_composite_group_by` | **5 / 5** |
| `cargo test --test test_register_json_v0` | **21 / 21** |
| `cargo test --test test_op_push_table` | **6 / 6** |
| `cargo test --test test_table_row_storage` | **7 / 7** |
| `cargo test --test test_snapshot_v7_migration` | **5 / 5** |

### Full suite

* `cargo test` — all integration binaries green.
* `pytest python/tests/test_v0_joins_e2e.py` — **3 / 3**.
* `pytest python/tests/` — **418 passed, 2 skipped** (no regressions
  vs Phase 24-02's 418+2 baseline; Phase 23's one "pre-existing
  failure" flagged in its summary no longer reproduces after Phase
  24-02's merged-GET view).

### Marker removal gate

```
grep -rE '__tt_left|__tt_right' src/ tests/
  tests/test_tt_cascade_migration.rs://! `__tt_left_*` / `__tt_right_*` markers from Phase 23-03.
```

Only the migration test's top-of-file doc-comment references the old
marker names. Zero code references remain.

## Deviations from plan

### [Rule 1 — Bug] `tt_cascades_recursively_through_chain` asserted on J, not K

**Found during:** Task 2 test run.

**Issue:** The Phase 23 version of this test asserted
`j_field(store, "u1", "z")`. Under the marker model every cascade
output landed in a single entity's `static_features` so `j_field`
(which read static_features) returned K's `z` field even though it
was named "J". Under the new per-Table row model, J's row and K's
row are distinct storage locations; `j_field` reads J, so `z` is
correctly absent there — it lives in K.

**Fix:** Rewrote the final assertion block to `store.get_table_row(
"u1", "K")` and check `fields.get("x" / "y" / "z")` on the K row.
The test's intent (A→B→J→C→K chain cascades all three fields to K)
is unchanged; only the observation point moved to the correct row.

**Files modified:** `tests/test_join_table_table.rs` (1 test body).

### [Rule 3 — Blocking issue] Dead-code warning on `json_to_fv`

**Found during:** Task 1 first build after cascade rewrite.

**Issue:** `json_to_fv` in `pipeline.rs` was only used by the old
marker cascade to map JSON values back into FeatureValues for
static_features writes. Leaving it emitted a `dead_code` warning
and risked silent re-wiring by a future plan.

**Fix:** Removed the helper and its doc-comment. The new cascade
reads FeatureValues directly off `TableRow.fields` — no
JSON round-tripping needed.

## Known stubs

None introduced. The Phase 23-03 "Known Stubs" table is now resolved
or retired:

| Phase 23 stub | Status after Phase 24-03 |
| ------------- | ------------------------- |
| TT-join collision suffix (needed per-Table storage) | Resolved. Left wins on emitted-name overlap; right side surfaces with the SDK-applied `_right` suffix. Verified by `tt_collision_suffix_on_output` + `cascade_migration_collision_suffix_through_real_storage`. |
| 7 `#[ignore]`'d TT tests | Resolved. All un-ignored and passing. |
| Single-entity storage limitation | Resolved. Input Tables A, B live in separate `table_rows` entries; the output Table J is also its own row. |
| TT cascade fan-out on SET | Unchanged. The legacy SET path in `tcp.rs` still fans out to all registered Tables because SET doesn't carry a table name; the rewritten cascade handles this by reading `get_table_row` for both input sides on every trigger — if the input_table has no matching row, `l_live` and `r_live` stay false and the output is correctly tombstoned (or not emitted, depending on join_type). |
| `OP_DELETE` opcode | Resolved earlier in plan 24-02 via `OP_DELETE_TABLE`. Empty-object-SET-as-tombstone is still operational for legacy static_features callers but no longer has any role in the TT cascade. |

## Threat flags

Plan's register (T-24-03-01 … 04) all mitigated or accepted as
designed:

* **T-24-03-01 (cascade recursion depth)** — mitigated. Register-time
  cycle guard (Phase 23-03 T-23-11) still rejects self-references;
  recursion depth is bounded by the register-validated DAG depth.
  The recursive test (`tt_cascades_recursively_through_chain`,
  `cascade_migration_recurses_through_chain_a_b_j1_c_j2`) exercises a
  2-deep chain end-to-end.
* **T-24-03-02 (stale row leaked through cascade)** — mitigated. The
  new cascade re-reads `get_table_row` on every invocation; no
  cached row state carries between cascades.
* **T-24-03-03 (tombstoned row leaks into output)** — mitigated. The
  `l_live` / `r_live` checks explicitly match on
  `Some(TableRowState::Live)`; any `Tombstoned` variant triggers the
  output-tombstone path via the join-type decision table.
* **T-24-03-04 (cascade races with gc_tombstones)** — accepted.
  `gc_tombstones` only removes rows past the 7d grace window; the
  cascade sees Tombstoned state during that window and handles it
  correctly. No elevation path.

No new threat surface introduced — this plan is an internal rewire,
not a new ingress.

## Self-Check: PASSED

Verified files exist (absolute paths):

* `/data/home/tally/src/engine/pipeline.rs` — FOUND (modified)
* `/data/home/tally/tests/test_join_table_table.rs` — FOUND (modified)
* `/data/home/tally/tests/test_tt_cascade_migration.rs` — FOUND (created)
* `/data/home/tally/.planning/phases/24-watermarks-event-time/24-03-SUMMARY.md` — FOUND (this file)

Verified commits exist on `main`:

* `5352e21` feat(24-03): migrate TT cascade to table_rows storage
* `b4f0038` test(24-03): un-ignore 7 TT-join tests; port harness to table_rows

Verified test gates (executed 2026-04-14):

* `cargo test --lib` — 682 / 682
* `cargo test --test test_tt_cascade_migration` — 5 / 5
* `cargo test --test test_join_table_table` — 12 / 12, 0 ignored
* `cargo test --test test_join_stream_table` — 6 / 6
* `cargo test --test test_join_stream_stream` — 14 / 14
* `cargo test --test test_join_integration` — 3 / 3
* `cargo test --test test_composite_group_by` — 5 / 5
* `cargo test --test test_register_json_v0` — 21 / 21
* `cargo test --test test_op_push_table` — 6 / 6
* `cargo test --test test_table_row_storage` — 7 / 7
* `cargo test --test test_snapshot_v7_migration` — 5 / 5
* `cargo test` (full suite) — all integration binaries green
* `pytest python/tests/test_v0_joins_e2e.py` — 3 / 3
* `pytest python/tests/` — 418 passed, 2 skipped (no regression)
* `grep -rE '__tt_left|__tt_right' src/ tests/` — only a doc-comment in
  `tests/test_tt_cascade_migration.rs`; zero code references.

Phase 24 Plan 03 is complete. The Phase 23 TT-join storage carry-forward
is closed out. Plan 24-04 can now layer per-stream watermarks and
`_event_time` parsing on top of the now-stable `table_rows` substrate
without any marker-based legacy to accommodate.
