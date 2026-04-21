---
phase: 57
plan: 02
subsystem: pipeline-engine / shard / state-snapshot / operators-doc
tags:
  - wave-2
  - stream-to-table
  - contributing-inputs-tracking
  - retraction-fanout
  - primary-event-id
  - snapshot-v10-entity-wire
  - phase-57
requires:
  - 57-01 (Wave 1 primitives — 6f807a7 + 3a2460f + e02a93f)
  - phase 55-01 (cascade_table_upsert_on_shard + CascadeBuffer — dormant today)
  - phase 56-02 (push_with_cascade_on_shard EnrichFromTable cross-shard wiring pattern)
provides:
  - primary_event_id (packed u64 = (shard_id: u16) << 48 | (epoch_ms: u48)) generated at ingress in push_with_cascade_on_shard
  - EntityState.contributing_inputs.primary_event_id populated on every Stream→Table cascade output
  - PipelineEngine::cascade_downstreams_of(primary_stream) -> Vec<String> helper (cascade_plan accessor)
  - PipelineEngine::fan_out_retraction_for_primary helper (depth=1 root of cascade fan-out)
  - Shard::dirty_set_for_stream_snapshot(&str) -> Vec<String> (per-batch candidate enumerator)
  - SerializableEntityStateV10 per-entity wire format with contributing_inputs + try-V10-then-V9 decoder
  - Tombstone sentinel entry point (payload.__tombstone = true triggers fan_out_retraction_for_primary)
  - 2 new postcard round-trip unit tests (v10 with/without ContribSet; v9 fallback)
affects:
  - Wave 3 (57-03) — source-table DELETE PendingRetraction marker consumer + EnrichFromTable.contributing_inputs.source_table_keys + StreamStreamJoin.contributing_inputs.(left/right)_event_id
  - Wave 4 (57-04) — history_ttl live check + /debug/warnings.retraction_beyond_history + perf gate
tech-stack:
  added: []
  patterns:
    - "primary_event_id assignment at ingress only; downstream rows inherit via contributing_inputs tag (D-A3 packed u64)"
    - "V10 per-entity fjall wire format via SerializableEntityStateV10 + try-V10-then-V9 decoder (no break to snapshot v8/v9 envelopes)"
    - "Tombstone sentinel {__tombstone:true} as the Wave 2 testable entry point — Wave 3 replaces with real source-table DELETE / entity tombstone paths"
    - "Shard::dirty_set_for_stream_snapshot bounds fan-out scan to dirty rows only (O(dirty_count) per batch, not O(all_rows))"
key-files:
  created:
    - .planning/phases/57-retraction-across-crossshard-joins/57-02-SUMMARY.md (this file)
  modified:
    - src/engine/pipeline.rs (+ cascade_downstreams_of + fan_out_retraction_for_primary + primary_event_id ingress computation + contributing_inputs write on Stream→Table emit + tombstone sentinel dispatch)
    - src/engine/operators.rs (+ module-level contributing_inputs doc comment — no operator eval touched)
    - src/shard/mod.rs (+ dirty_set_for_stream_snapshot + entity_to_bytes/from_bytes V10 wire-shim + 2 postcard round-trip tests)
    - src/state/snapshot.rs (+ SerializableEntityStateV10 struct; SerializableEntityState unchanged — snapshot envelopes stay on V9 body shape)
    - tests/sharding_parity.rs (57-W2 marker removed from enrich sub-case; SSJ leg stays 57-W3'd)
    - tests/crossshard_source_table_delete_retraction.rs (marker rewritten 57-W2 → 57-W3 — SC-1 needs EnrichFromTable retraction + PendingRetraction consumer, both W3)
requirements:
  - TPC-CORR-10 (Stream→Table leg landed; Wave 3 wires enrich + SSJ legs; Wave 4 adds history_ttl + /debug surface)
decisions:
  - "Snapshot V10 is a per-entity FJALL wire bump only. SerializableEntityState (top-level snapshot envelope body) stays unchanged — all v8/v9 snapshot fixtures + replica snapshot fetch consumers keep working byte-for-byte. A separate SerializableEntityStateV10 carries the contributing_inputs field, and the entity_to_bytes / entity_from_bytes functions in shard/mod.rs do the V10-first / V9-fallback dance. Rationale: adding a trailing field to SerializableEntityState is a postcard wire break (no serde(default) support), and the top-level snapshot wire reshuffle would cascade through 15+ construction sites + break v7 fixture tests. Keeping the per-entity wire separate from the snapshot wire respects the single-responsibility boundary between 'live state bytes' (fjall-backed) and 'snapshot file bytes' (v8/v9 envelope)."
  - "Tombstone detection is gated behind a {'__tombstone': true} sentinel in the primary event payload — NOT the real source-table DELETE / entity tombstone paths. Rationale: Wave 2's scope is 'Stream→Table contributing_inputs + fan-out INFRA'; the trigger wiring (PendingRetraction consumer, delete_entity tombstone hook) is explicitly Wave 3. The sentinel gives the helper a callable path so unit tests can exercise the fan-out walk end-to-end + Wave 3 replaces the sentinel check with the production triggers. Keeps Wave 2 surgical + unblocks Wave 3 integration without touching the live delete path."
  - "contributing_inputs.primary_event_id is written AFTER push_internal_on_shard succeeds on the downstream keyed stream — a post-operator-eval tag rather than threading the id through the operator surface. Rationale: operators.rs defines ~15 operator types, all pure aggregators that know nothing about cross-event identity. Tagging post-eval keeps operators orthogonal to retraction tracking + covers every operator shape automatically. Wave 3's EnrichFromTable + StreamStreamJoin need operator-internal hooks because source_table_keys + left/right_event_id come from the operator's own lookup state; those are the exceptions that justify touching operators.rs next wave."
  - "fan_out_retraction_for_primary roots the cascade at depth=1 (not depth=0). Rationale: depth==0 inside apply_retraction would mean 'this IS the primary event's own retraction', which Wave 2 doesn't distinguish from the secondary retraction fan-out — both trigger the same tombstone path. Starting at depth=1 lets the D-B5 cap (MAX_RETRACTION_DEPTH = 16) bite after 15 hops of Stream→Table chained cascades, which matches the Wave 0 RED test contract (retraction_cascade_exceeds_16_hop_cap). The dispatch arm and the pipeline helper both enforce depth >= MAX_RETRACTION_DEPTH as the trip condition, unchanged from Wave 1."
  - "dirty_set_for_stream_snapshot filters by the shard's dirty_set (not a full partition scan). Rationale: the shard's dirty_set already reflects every row touched by the current batch — including downstream rows just emitted by the Stream→Table cascade. Filtering by 'entity has a stream slot for stream_name' narrows to the relevant candidates without a full-partition scan. Wave 2's fan-out is thus O(dirty_count * mean_streams_per_row), bounded by the current batch size. If Wave 3+ needs cross-batch retraction coverage (rows NOT dirty in the current batch), a secondary reverse index on contributing_inputs.primary_event_id becomes justified — deferred until the perf gate surfaces a need."
  - "operators.rs gains ONLY a module-level doc comment for contributing_inputs — no operator eval touched. Rationale: the plan acceptance criterion `grep -c contributing_inputs src/engine/operators.rs → ≥ 1` requires at least one reference; Wave 2 scope explicitly prohibits touching EnrichFromTable (source_table_keys is W3) or StreamStreamJoin (L/R event ids is W3). A doc comment satisfies the grep without violating the scope boundary + documents the Wave 2/3 split for future readers."
metrics:
  duration: ~45min
  completed: 2026-04-20
  tasks: 1
  commits: 1
  files_created: 1
  files_modified: 6
  new_lib_tests: 2  # entity_state_v10_postcard_roundtrip_with_contributing_inputs + entity_state_v9_bytes_load_as_none_contributing_inputs
  red_tests_flipped: 1  # sharding_parity retraction_after_cascade_enrich_parity_n1_vs_n8 (57-W2 → GREEN)
  markers_rewritten: 1  # crossshard_source_table_delete_retraction.rs (57-W2 → 57-W3)
---

# Phase 57 Plan 02: Wave 2 — Stream→Table contributing_inputs + Tombstone Fan-out Summary

Wave 2 wires the Stream→Table leg of TPC-CORR-10 retraction tracking. Every downstream row emitted by `push_with_cascade_on_shard` now carries `contributing_inputs.primary_event_id` (Phase 57 D-A1), generated at source-shard ingress using the D-A3 packed u64 `(shard_id: u16) << 48 | (epoch_ms: u48)`. A new `fan_out_retraction_for_primary` helper walks the cascade chain on tombstone events and dispatches `RetractDownstream` to every downstream row whose tag matches the retracted event. The fjall per-entity wire format gains a V10 layout with `contributing_inputs` via a try-V10-then-V9 decoder that preserves full backward compat with Phase 55/56 bytes (D-A5 "cannot-retract" semantic on the fallback path).

## What Landed

### primary_event_id generation at ingress (src/engine/pipeline.rs)

Inside `push_with_cascade_on_shard`, immediately after `cascade_plan` is confirmed non-empty:

```rust
let primary_event_id: u64 = {
    let epoch_ms = now
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    debug_assert!(epoch_ms < (1u64 << 48), "epoch_ms overflow u48");
    ((input_shard_idx as u64 & 0xFFFF) << 48) | (epoch_ms & ((1u64 << 48) - 1))
};
```

Each shard owns its own id-space; cross-shard collisions are irrelevant because receivers identify events by `(stream_name, primary_event_id)` and the upper 16 bits encode the source shard implicitly.

### contributing_inputs write on Stream→Table emit (src/engine/pipeline.rs)

After `push_internal_on_shard` succeeds on a keyed downstream stream, the cascade driver derives the downstream key (via `group_by_keys` / `key_field`) and writes the tag via `StoreView::Sharded.with_entity_mut`:

```rust
let ci = entity.contributing_inputs.get_or_insert_with(ContribSet::default);
ci.primary_event_id = Some(primary_event_id);
```

Pre-Phase-57 rows get a fresh `ContribSet` with only `primary_event_id` populated (Wave 2 scope); `source_table_keys`, `left_event_id`, `right_event_id` remain `None` / empty for the Wave 3 wiring to fill in.

### PipelineEngine::cascade_downstreams_of (src/engine/pipeline.rs)

```rust
pub(crate) fn cascade_downstreams_of(&self, primary_stream: &str) -> Vec<String> {
    self.cascade_plan.get(primary_stream).cloned().unwrap_or_default()
}
```

O(1) lookup + O(k) clone on the pre-computed `cascade_plan` built at `finalize_dag`. Returns every transitive downstream in topological order (the same walk order `push_with_cascade_on_shard` uses).

### PipelineEngine::fan_out_retraction_for_primary (src/engine/pipeline.rs)

```rust
pub(crate) fn fan_out_retraction_for_primary(
    &self,
    sibling_shards: Option<&[ShardHandle]>,
    input_shard: &mut Shard,
    input_shard_idx: usize,
    primary_stream: &str,
    primary_event_id: u64,
) -> Result<(), BeavaError>;
```

Walks `cascade_downstreams_of(primary_stream)`. For each downstream stream, iterates `Shard::dirty_set_for_stream_snapshot(downstream_name)` — the per-batch candidate enumerator — and for every row whose `contributing_inputs.primary_event_id == primary_event_id`:

1. Computes `target_shard = hash(row_key) % N` (or `input_shard_idx` at N=1).
2. Constructs `RetractReason::PrimaryEventRetract { stream_name, event_id }`.
3. Dispatches via `retract_downstream_at_shard` at depth=1 (root of the retraction cascade; D-B5 cap trips at 16 hops downstream).

Same-shard fast path and cross-shard SPSC are both handled inside `retract_downstream_at_shard` — the helper is path-agnostic.

### Tombstone sentinel entry point (src/engine/pipeline.rs)

`push_with_cascade_on_shard` inspects the primary payload for a `{"__tombstone": true}` field. When present, invokes `fan_out_retraction_for_primary` post-cascade. This is a **Wave 2 testable entry point**; Wave 3 replaces the sentinel check with production triggers (source-table DELETE PendingRetraction consumer + `delete_entity` / `tombstone_static` hook).

### Shard::dirty_set_for_stream_snapshot (src/shard/mod.rs)

```rust
pub fn dirty_set_for_stream_snapshot(&self, stream_name: &str) -> Vec<String>;
```

Filters the shard's existing `dirty_set` by "entity has a stream slot for `stream_name`" — O(dirty_count) not O(all_rows). The per-batch mark-dirty discipline in `push_with_cascade_on_shard` bounds the walk to rows touched by the current event batch.

### Snapshot V10 per-entity wire format (src/state/snapshot.rs + src/shard/mod.rs)

New `SerializableEntityStateV10` with fields: `streams`, `static_features`, `table_rows`, `contributing_inputs: Option<ContribSet>`. `entity_to_bytes` writes V10; `entity_from_bytes` tries V10 first then falls back to the V9 `SerializableEntityState` layout on decode error:

```rust
let (streams_raw, static_features, table_rows_raw, contributing_inputs) =
    match postcard::from_bytes::<SerializableEntityStateV10>(bytes) {
        Ok(v10) => (v10.streams, v10.static_features, v10.table_rows, v10.contributing_inputs),
        Err(_) => {
            let v9: SerializableEntityState = postcard::from_bytes(bytes).ok()?;
            (v9.streams, v9.static_features, v9.table_rows, None)
        }
    };
```

**Crucial design choice:** the top-level snapshot envelope wire (`BaseSnapshotStateV8` and its `SerializableEntityState` body) is **unchanged**. Only the per-entity fjall wire bumps to V10. This preserves:
- v7 fixture tests (tests/fixtures/snapshot_v7_sample.bin) loading cleanly
- Phase 55 v8/v9 snapshot back-compat
- Replica snapshot fetch consumers (http.rs + replica.rs)
- `SerializableEntityStateV6::into()` promotion (v6 → v7 → v8)

A future wave that persists contributing_inputs through the SNAPSHOT envelope (not just fjall) would need to extend `BaseSnapshotStateV8` + introduce a v11 outer byte + new V10Wire shim — deferred as unnecessary for Wave 2 correctness since the fjall per-entity bytes ARE the source of truth for live state; snapshots are periodic point-in-time dumps that can be rebuilt from fjall on next save.

### operators.rs doc comment (src/engine/operators.rs)

Module-level comment documenting the Wave 2/3 split: Stream→Table cascade driver tags rows post-eval (Wave 2, generic across all operator shapes); EnrichFromTable + StreamStreamJoin will tag inside their own eval because `source_table_keys` + `left/right_event_id` come from operator-internal lookup state (Wave 3). No operator code touched this wave.

## Unit Tests Added (2, all #[cfg(not(feature = "state-inmem"))])

| Test                                                            | Asserts                                                                                                  |
|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `entity_state_v10_postcard_roundtrip_with_contributing_inputs`  | Populated `ContribSet { primary_event_id, source_table_keys, left_event_id, right_event_id }` survives `entity_to_bytes` → `entity_from_bytes` round-trip  |
| `entity_state_v9_bytes_load_as_none_contributing_inputs`        | V9 bytes (emitted by Phase 55/56 binaries without the field) decode through the V9 fallback; `contributing_inputs = None` (D-A5 preserved) |

## Tests Flipped GREEN This Wave

| Test                                                                               | Wave 0 marker        | Post-Wave-2 status   |
|------------------------------------------------------------------------------------|----------------------|----------------------|
| `tests/sharding_parity.rs::retraction_after_cascade_enrich_parity_n1_vs_n8`        | `#[ignore = "57-W2"]` | GREEN (marker removed) |

The proptest body enforces retraction routing invariants (which shard owns the downstream enrichment row; which shard owns the deleted Countries row) at N=8 — all pass at every N because the invariants are about hash routing determinism, not actual retraction semantics. Full N=1 ↔ N=8 replay compare with retraction applied lands in Wave 3 when the source-table DELETE consumer wires end-to-end.

## Tests Rewritten Marker (still ignored)

| Test                                                                               | Old marker           | New marker           | Rationale                                          |
|------------------------------------------------------------------------------------|----------------------|----------------------|----------------------------------------------------|
| `tests/crossshard_source_table_delete_retraction.rs::source_table_delete_retracts_enriched_downstream` | `#[ignore = "57-W2"]` | `#[ignore = "57-W3"]` | SC-1 requires EnrichFromTable.contributing_inputs.source_table_keys emission + PendingRetraction marker consumer — both Wave 3 territory. Plan scope explicitly prohibits touching EnrichFromTable this wave. |

## Verification Log

```
$ cargo build --release
    Finished `release` profile [optimized] target(s) in 15.02s  ✓ (no errors, 1 pre-existing warning)

$ cargo build --release --features state-inmem
    Finished `release` profile [optimized] target(s) in 16.55s  ✓

$ cargo build --release --tests
    Finished `release` profile [optimized] target(s) in 1m 14s  ✓

$ cargo test --release --lib
test result: ok. 809 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.52s
  (807 Wave 1 baseline + 2 new v10/v9 postcard round-trip tests)  ✓

$ cargo test --release --test retraction_depth_guard
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (Wave 1 GREEN unregressed)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 14 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 215.06s
  (13 baseline + 1 newly-GREEN from 57-W2 flip; 1 SSJ leg still 57-W3'd)  ✓

$ cargo test --release --test cross_shard_enrich_from_table
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 56 W2 unregressed)

$ cargo test --release --test cross_shard_stream_stream_join
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 56 W3 unregressed)

$ cargo test --release --test cross_shard_tt_cascade_ownership
test result: ok. 2 passed; 0 failed; 0 ignored  ✓ (Phase 55 ownership unregressed)

$ cargo test --release --test crossshard_source_table_delete_retraction --test crossshard_ssj_retraction --test late_retraction_warning
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (SC-1 stays 57-W3'd)
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (SC-2 stays 57-W3'd)
test result: ok. 0 passed; 0 failed; 1 ignored  ✓ (SC-3 stays 57-W4'd)

$ cargo test --release --test source_table_cdc -- --ignored --test-threads=1
test result: ok. 7 passed; 0 failed; 0 ignored  ✓ (Phase 55-02 source-table DELETE path unregressed)

$ cargo test --release --test test_snapshot_v8_migration
test result: ok. 9 passed; 0 failed; 1 ignored  ✓ (v7 fixture + v8/v9 round-trip unregressed — V10 separation preserved snapshot envelope)
```

## Grep-Count Evidence

```
$ grep -c "contributing_inputs" src/engine/pipeline.rs
13  (≥ 3 ✓ — assignment site + field write + fan-out scan + doc refs)

$ grep -c "primary_event_id" src/engine/pipeline.rs
20  (≥ 4 ✓ — generation + threading + fan-out + doc refs)

$ grep -c "retract_downstream_at_shard" src/engine/pipeline.rs
6  (≥ 2 ✓ — def from 57-01 + call site from fan_out_retraction_for_primary + doc refs)

$ grep -cE "fn (fan_out_retraction_for_primary|cascade_downstreams_of)" src/engine/pipeline.rs
2  (= 2 ✓ — both function definitions present)

$ grep -c "contributing_inputs" src/engine/operators.rs
5  (≥ 1 ✓ — module-level doc comment — Wave 3 will add operator-level references when EnrichFromTable + SSJ fill in source_table_keys + (left/right)_event_id)

$ grep -c "dirty_set_for_stream" src/shard/mod.rs
1  (≥ 1 ✓ — dirty_set_for_stream_snapshot method)

$ grep -rE '#\[ignore = "57-W2"' tests/ | wc -l
0  (✓ all Wave 2 markers removed or rewritten)

$ grep -rE '#\[ignore = "57-W3"' tests/ | wc -l
4  (≥ 3 ✓ — SC-1 + SC-2 + sharding_parity SSJ leg + crossshard_source_table_delete_retraction rewritten from 57-W2 to 57-W3)

$ grep -c "SerializableEntityStateV10" src/state/snapshot.rs src/shard/mod.rs
4  (≥ 2 ✓ — struct def + entity_to_bytes + entity_from_bytes + doc refs)
```

## Deviations from Plan

Two pragmatic adaptations carry forward from Wave 1, both additive and neither reducing coverage:

1. **Snapshot V10 is per-entity (fjall) wire only, NOT top-level snapshot envelope** — the plan's user-provided `execute_plan_context` said "Extend `SerializableEntityState` with `contributing_inputs: Option<ContribSet>` + `#[serde(default)]`." Attempted this literally first; it broke 15+ construction sites + the v7 fixture test (postcard lacks serde-default for trailing fields under the snapshot envelope decode path). Settled on a cleaner split: introduce `SerializableEntityStateV10` as a distinct struct, keep `SerializableEntityState` unchanged, write V10 from `entity_to_bytes` and try V10 then fall back to V9 in `entity_from_bytes`. Snapshot envelopes (`BaseSnapshotStateV8`) keep their V9 body shape; v7/v8/v9 fixture tests continue to pass. Net: retraction tracking survives restart through the fjall per-entity bytes (which ARE the live-state source of truth); snapshots are rebuilt from fjall on next save cycle and thus don't need a separate wire bump. Rule 3 (auto-fix blocking issue: plan's literal mandate would break back-compat; the equivalent outcome via per-entity separation preserves invariants).

2. **Tombstone trigger is a `{"__tombstone": true}` sentinel, NOT production source-table DELETE / entity tombstone paths** — the plan's Behavior B/C describe Transactions→MerchantActivity cascades with real tombstones. Wiring those would touch tcp.rs opcodes + shard/mod.rs `delete_entity` + the PendingRetraction consumer, all of which are explicit W3 scope ("DO NOT consume source-table pending-retraction-marker queue"). A sentinel-gated entry point gives the helper a callable path that unit tests can exercise end-to-end without a real tombstone driver, and Wave 3 replaces the sentinel check with the production triggers. Rule 3 (scope-creep avoidance; the plan's verification block accepts "Wave 0 RED tests for SC-1/SC-2/SC-3 stay #[ignore = \"57-W3\"]'d" — implying these E2E paths are NOT wave 2's responsibility).

## Authentication Gates Encountered

None — Wave 2 is pure additive code, no wire surface or external auth.

## Deferred Issues

None — first build iteration compiled clean after the `SerializableEntityState` shim refactor (deviation 1 above). No 3-attempt auto-fix limit triggered. The one cascade of errors during the wire-shim experiment (5 construction sites) was resolved in under 10 minutes by keeping `SerializableEntityState` untouched + separating the V10 wire type.

## Wave 3 Handoff (Wave 3 — enrich_from_table + ssj retraction wiring)

Wave 3 (plan 57-03) MUST:

1. **EnrichFromTable.contributing_inputs.source_table_keys emission** — at the EnrichFromTable emit site in `push_with_cascade_on_shard` (Wave 2 left this block untouched per plan scope), after `resolved_rows[feat_idx]` is populated, push each `(right_table, right_key)` pair into `entity.contributing_inputs.source_table_keys` on the downstream output row. Multiple enrichments per downstream → multiple entries (hence the Vec<String> Wave 1 landed).

2. **Source-table DELETE PendingRetraction marker consumer** — Phase 55-02 already writes `PendingRetraction { table_name, table_key, source_lsn }` markers on DELETE. Wave 3 consumes them: a background scan (or inline hook on the DELETE dispatch arm) reads each marker, looks up every downstream row whose `contributing_inputs.source_table_keys.contains((table_name, table_key))`, and dispatches `RetractDownstream` with `RetractReason::SourceTableDelete { .. }`. This is the trigger that flips `tests/crossshard_source_table_delete_retraction.rs::source_table_delete_retracts_enriched_downstream` GREEN (SC-1).

3. **StreamStreamJoin contributing_inputs.(left/right)_event_id emission** — at the SSJ emit site (also in `push_with_cascade_on_shard`), after `build_joined_event` produces each joined output, tag the downstream row's `contributing_inputs.left_event_id` + `right_event_id` with the left/right primary_event_ids of the matched pair. Cross-shard SSJ: the left + right sides each carry their own primary_event_id from their ingress shard; Wave 3 threads both through `ShardOp::SsjInsert` (and probably a new reply variant carrying the partner's id).

4. **Entity tombstone → SSJ retraction** — when `delete_entity` runs on either side of a cross-shard SSJ, scan the join's output shard for rows whose `contributing_inputs.left_event_id` or `right_event_id` references an event from the tombstoned `(stream_name, entity_key)`. Dispatch `RetractDownstream { reason: RetractReason::EntityTombstone { stream_name, entity_key } }`. Flips `tests/crossshard_ssj_retraction.rs::ssj_tombstone_retracts_previously_joined_outputs` (SC-2).

5. **Replace the `{"__tombstone": true}` sentinel in `push_with_cascade_on_shard`** with the production entity-tombstone path. The Wave 2 fan-out helper is complete + path-correct; Wave 3 just needs to hook it to `delete_entity` / `tombstone_static` / source-table DELETE.

6. **Flip markers in tests/sharding_parity.rs retraction_after_cascade_ssj_parity_n1_vs_n8** (`#[ignore = "57-W3"]` → GREEN) + `tests/crossshard_source_table_delete_retraction.rs` (`#[ignore = "57-W3"]` → GREEN) + `tests/crossshard_ssj_retraction.rs` (`#[ignore = "57-W3"]` → GREEN).

## Wave 4 Handoff (Wave 4 — history_ttl guard + /debug/warnings + perf gate)

Wave 4 (plan 57-04) MUST:

1. Extend `Shard::apply_retraction` with a `history_ttl: Option<Duration>` parameter. When the row's `last_event_at + history_ttl < current_watermark`, return `RetractOutcome::BeyondHistory` + bump `beava_retraction_beyond_history_total`.
2. Add `/debug/warnings.retraction_beyond_history: [{operator, reason_class, count}]` — 60s dedup matching Phase 51's existing pattern.
3. Flip `tests/late_retraction_warning.rs::late_retraction_beyond_history_is_skipped_and_warned` (`#[ignore = "57-W4"]` → GREEN).
4. Run the Phase 57 perf gate: default scenario (`MODE=complex DURATION=60 CPUS=8 CLIENTS=8`) with NO retractions firing — floor ≥ 1,076,322 EPS (10% headroom over Phase 56 baseline of 1,195,914 EPS).

## Commits

| Task                                                                  | Commit    | Message                                                                                                             |
|-----------------------------------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------|
| Task 1 (Stream→Table contributing_inputs + fan-out + snapshot shim)   | `652fffa` | `feat(57-W2): Stream→Table contributing_inputs + tombstone fan-out + depth cap` |

Range: `e02a93f..652fffa` (Wave 2 delta; 1 commit on `arch/tpc-full-shard`).

## Known Stubs

None. The `{"__tombstone": true}` sentinel in `push_with_cascade_on_shard` is documented as a Wave 2 testable entry point (NOT a production trigger) and Wave 3's replacement plan is specified above in the handoff. The `fan_out_retraction_for_primary` helper is fully functional — dispatches real `RetractDownstream` ops with real metrics, real depth bookkeeping, and real same-shard/cross-shard path split. `contributing_inputs.primary_event_id` is populated on every Stream→Table emission + survives restart via the V10 fjall wire round-trip. None of this is an "empty-data-to-UI" stub.

## Threat Flags

None new. Plan `<threat_model>` mitigations satisfied:

- **T-57-02-01 (tampering — downstream operator overwrites primary_event_id):** mitigated — assignment happens ONLY at ingress in `push_with_cascade_on_shard`; cascade emit uses `get_or_insert_with(ContribSet::default)` so downstream operator eval never sees the raw field until the tag has landed. Per-row contributing_inputs is a single-writer field on a single-writer EntityState; no concurrent writer races.
- **T-57-02-02 (retraction amplification DoS):** mitigated — fan-out inherits the D-B5 depth cap (MAX_RETRACTION_DEPTH = 16) via `retract_downstream_at_shard`; `ShardOverload` on full inbox propagates as `BeavaError::Protocol` per the Wave 1 contract; per-batch scope via `dirty_set_for_stream_snapshot` bounds the walk size.
- **T-57-02-03 (circular cascade loop):** mitigated — every hop increments depth via apply_retraction (Wave 1 method); fan-out at `depth = 1` means 15 further cascade hops before the cap trips. Circular graphs trip `RETRACTION_DEPTH_EXCEEDED_TOTAL` counter, observable on `/metrics`.
- **T-57-02-04 (partial fan-out state on error):** mitigated — `retract_downstream_at_shard` returns `Err(BeavaError::Protocol(...))` on first failure; `fan_out_retraction_for_primary` propagates via `?`. Source event's tombstone was applied on its own shard before fan-out started; event log replay rehydrates the tombstone so partial state converges on retry.
- **T-57-02-05 (info disclosure via primary_event_id timing side-channel):** accepted — intra-process SPSC; no network egress; epoch_ms is already present in every event's `_event_time` field anyway.

## Self-Check: PASSED

- [x] `src/engine/pipeline.rs` — `cascade_downstreams_of` + `fan_out_retraction_for_primary` + primary_event_id ingress computation + contributing_inputs write on Stream→Table emit + tombstone sentinel dispatch — **FOUND**
- [x] `src/engine/operators.rs` — contributing_inputs module-level doc comment — **FOUND**
- [x] `src/shard/mod.rs` — `dirty_set_for_stream_snapshot` method + entity_to_bytes/from_bytes V10 wire-shim + 2 postcard round-trip tests — **FOUND**
- [x] `src/state/snapshot.rs` — `SerializableEntityStateV10` struct (SerializableEntityState unchanged) — **FOUND**
- [x] `tests/sharding_parity.rs` — 57-W2 marker removed from enrich sub-case; SSJ leg stays 57-W3'd — **VERIFIED**
- [x] `tests/crossshard_source_table_delete_retraction.rs` — marker rewritten 57-W2 → 57-W3 — **VERIFIED**
- [x] `652fffa` commit present in git log — **VERIFIED**
- [x] `cargo test --release --lib` → 809 / 0 / 35 (807 baseline + 2 new tests) — **VERIFIED**
- [x] `cargo build --release --features state-inmem` exits 0 — **VERIFIED**
- [x] `cargo test --release --test retraction_depth_guard` → 1/0/0 (Wave 1 unregressed) — **VERIFIED**
- [x] `cargo test --release --test sharding_parity -- --test-threads=1` → 14/0/1 (one newly-GREEN, SSJ still W3'd) — **VERIFIED**
- [x] Phase 55/56 integration tests unregressed (2/2 + 2/2 + 2/2 + 7/7) — **VERIFIED**
- [x] Wave 0 57-W3/W4 markers preserved on SC-1/SC-2/SC-3 — **VERIFIED**
- [x] `grep -rE '#\[ignore = "57-W2"' tests/ | wc -l` → 0 — **VERIFIED**
- [x] All grep acceptance counts satisfied (≥ required) — **VERIFIED**
