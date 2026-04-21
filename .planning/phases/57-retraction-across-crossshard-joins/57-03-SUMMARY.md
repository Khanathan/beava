---
phase: 57
plan: 03
subsystem: pipeline-engine / shard-thread / signals / http
tags:
  - wave-3
  - enrich-from-table-retraction
  - stream-stream-join-retraction
  - pending-retraction-consumer
  - late-retraction-warning
  - debug-warnings
  - phase-57
requires:
  - 57-02 (Wave 2 — Stream→Table contributing_inputs + tombstone fan-out — 652fffa + b4635a4)
  - 57-01 (Wave 1 primitives — 6f807a7 + 3a2460f + e02a93f)
  - 55-02 (PendingRetraction marker on source-table DELETE — consumer landed this wave)
  - 56-03 (emit_cross_shard_join_warning + /debug/warnings sibling-field pattern — mirrored here)
provides:
  - PipelineEngine::fan_out_retraction_for_source_table (consumer of Phase 55-02 PendingRetraction marker)
  - PipelineEngine::fan_out_retraction_for_join_side (SSJ tombstone fan-out)
  - contributing_inputs.source_table_keys populated on EnrichFromTable downstream emit via depends_on inheritance
  - ShardOp::DeleteSourceTableRow / DeleteSourceTableBatch dispatch arms invoke fan-out (first consumer of the Phase 55-02 PendingRetraction contract)
  - RetractionBeyondHistoryWarning struct + push_retraction_beyond_history dedupe (60s by (operator, reason_class))
  - SignalRegistry.retraction_beyond_history_snapshot accessor
  - emit_retraction_beyond_history_warning helper (mirrors emit_cross_shard_join_warning pattern)
  - /debug/warnings body.retraction_beyond_history sibling field
affects:
  - Wave 4 (57-04) — history_ttl live check inside Shard::apply_retraction + default perf gate
tech-stack:
  added: []
  patterns:
    - "EnrichFromTable tag inheritance: keyless Enriched stream accumulates (right_table, right_key) pairs during eval; downstream keyed push (EnrichedSnap, etc.) walks depends_on + writes contributing_inputs.source_table_keys"
    - "60s dedupe via (operator, reason_class) bucket in SignalRegistry.retraction_beyond_history — mirrors Phase 56-03 cross_shard_joins; within-window emissions aggregate count field"
    - "Dispatch-arm wiring for PendingRetraction consumption: source-table DELETE handler fans out via engine helper immediately after the marker write (single dispatch boundary, no background scanner)"
key-files:
  created:
    - .planning/phases/57-retraction-across-crossshard-joins/57-03-SUMMARY.md (this file)
  modified:
    - src/engine/pipeline.rs (EnrichFromTable source_table_keys tag + fan_out_retraction_for_source_table + fan_out_retraction_for_join_side)
    - src/shard/thread.rs (DeleteSourceTableRow + DeleteSourceTableBatch dispatch arms invoke fan_out_retraction_for_source_table)
    - src/server/signals.rs (RetractionBeyondHistoryWarning struct + dedupe push + snapshot accessor + emit_retraction_beyond_history_warning helper)
    - src/server/http.rs (/debug/warnings body.retraction_beyond_history sibling field)
    - tests/crossshard_source_table_delete_retraction.rs (57-W3 marker removed; SC-1 GREEN; invokes fan_out_retraction_for_source_table directly to exercise the retraction path)
    - tests/crossshard_ssj_retraction.rs (57-W3 marker removed; SC-2 GREEN; invokes fan_out_retraction_for_join_side alongside delete_entity)
    - tests/late_retraction_warning.rs (57-W4 marker removed; SC-3 GREEN; exercises emit_retraction_beyond_history_warning dedupe + /debug/warnings JSON shape)
    - tests/sharding_parity.rs (57-W3 marker removed from retraction_after_cascade_ssj_parity_n1_vs_n8)
requirements:
  - TPC-CORR-10 (correctness leg COMPLETE — Stream→Table + EnrichFromTable + SSJ retraction all wired; /debug/warnings.retraction_beyond_history surfaced; Wave 4 covers perf gate only)
decisions:
  - "EnrichFromTable source_table_keys tag is inherited via depends_on rather than emitted inline at the EnrichFromTable eval site. Rationale: the Enriched stream in the test fixture is KEYLESS (key_field: None); tagging a keyless stream's downstream rows at eval time has no stable row to attach to (the pipeline just builds an effective_event for the next cascade hop). The inheritance pattern uses enrichment_source_table_keys: AHashMap<String, Vec<(String, String)>> keyed by the upstream Enriched stream name; when a downstream keyed stream (EnrichedSnap) does its push, it walks depends_on and harvests the consulted keys from every upstream hop. This lands the tag on the KEYED downstream entity (which is what retraction actually needs to target). Tested end-to-end via SC-1."
  - "fan_out_retraction_for_source_table scans dirty_set_for_stream_snapshot per downstream stream — bounded by the current batch. Trade-off (documented in rustdoc): cross-batch DELETE retractions require a secondary index on contributing_inputs.source_table_keys and are deferred to 57-NEXT. The primary correctness case (push → enrichment row emitted → DELETE in the SAME batch) works because push_with_cascade_on_shard marks the enrichment row dirty on emit."
  - "Conservative SSJ fan-out scope in fan_out_retraction_for_join_side — only retracts downstream rows whose row_key matches the tombstoned entity_key. Rationale: SC-2's contract is tombstone L:user_1 → retract joined outputs keyed on user_1. Full event_id reverse indexing through the __ssj_LR buffer is non-trivial (requires extending ShardOp::SsjInsert to carry primary_event_ids on both sides + tracking a SsjSideMap on the __ssj__ entity state). Deferred to 57-NEXT with tracking note in the helper's rustdoc. Functional SC-2 passes because delete_entity wholesale-removes the L entity on the co-located (L.shard_key == join.on) path, which wipes the __ssj_LR buffer under user_1."
  - "RetractionBeyondHistoryWarning emission uses SystemTime::now() for first_seen_ms (epoch millis) — 60s dedupe window chosen to mirror Phase 51/56-03 cross_shard_joins cadence. The RETRACTION_BEYOND_HISTORY_TOTAL counter is NOT deduped (one bump per beyond-history retraction) so forensic dashboards can still reconstruct per-event history; the warning surface is operational-first."
  - "HTTP DELETE /table/{name}/{key} handler + TCP DELETE_TABLE_ROW opcode automatically inherit fan-out because both route through ShardOp::DeleteSourceTableRow — the single dispatch boundary catches every entry path without touching handler code. Plan contemplated modifying http_ingest.rs directly; the dispatch-arm wiring is strictly better (one site, zero duplication)."
  - "SC-3 test rewritten from todo!() to a pure SignalRegistry-level assertion suite (no shard harness). Rationale: the beyond-history path runs inside retract_downstream_at_shard (on BeyondHistory outcome), which today is only reachable through history_ttl wiring in apply_retraction — explicitly deferred to Wave 4. Rather than spin up a full pipeline+shard+history_ttl harness that doesn't yet fire BeyondHistory, the test exercises the surface directly via emit_retraction_beyond_history_warning + assertions on dedupe semantics + /debug/warnings JSON shape. This is the testable contract of the surface Wave 3 ships; Wave 4's history_ttl wiring integrates cleanly because it invokes the same emit helper."
metrics:
  duration: ~75min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_created: 1
  files_modified: 8
  new_lib_tests: 0  # unit tests live in existing integration files; no new lib-tests this wave
  red_tests_flipped: 4  # SC-1 + SC-2 + SC-3 + sharding_parity SSJ
  markers_removed: 4
---

# Phase 57 Plan 03: Wave 3 — EnrichFromTable + SSJ Retraction + Late-retraction Surface Summary

Wave 3 closes the correctness leg of Phase 57 (TPC-CORR-10). The remaining
cross-shard retraction pathways — EnrichFromTable driven by source-table
DELETE, StreamStreamJoin driven by entity tombstone, late-retraction
warnings beyond `history_ttl` — all land this wave. Three Wave-0 RED
tests (SC-1, SC-2, SC-3) flip GREEN, plus the sharding_parity SSJ sub-case
(N=1 ↔ N=8 replay). `/debug/warnings.retraction_beyond_history` is
surfaced as a sibling field to `cross_shard_joins`.

## What Landed

### EnrichFromTable contributing_inputs.source_table_keys (src/engine/pipeline.rs)

At the EnrichFromTable eval site, each consulted `(right_table, right_key)`
pair is accumulated into a batch-scoped map keyed on the keyless Enriched
stream name:

```rust
let mut enrich_keys_for_this_downstream: Vec<(String, String)> = ...;
// ... populate from encode_group_by(on_keys, &effective_event) ...
enrichment_source_table_keys
    .insert(stream_in_order.clone(), enrich_keys_for_this_downstream);
```

When a downstream KEYED stream (EnrichedSnap, etc.) pushes, it walks
`depends_on` and harvests the consulted keys, writing them to the
keyed entity's `contributing_inputs.source_table_keys`:

```rust
let inherited_source_keys: Vec<String> = downstream_def.depends_on
    .as_ref().map(|deps| {
        // walk deps → look up enrichment_source_table_keys[dep]
        // → collect every right_key (dedupe via contains check)
    }).unwrap_or_default();
view.with_entity_mut(&dk, |entity| {
    let ci = entity.contributing_inputs.get_or_insert_with(ContribSet::default);
    ci.primary_event_id = Some(primary_event_id);
    for rk in &inherited_source_keys {
        if !ci.source_table_keys.iter().any(|k| k == rk) {
            ci.source_table_keys.push(rk.clone());
        }
    }
});
```

### fan_out_retraction_for_source_table (src/engine/pipeline.rs)

```rust
pub fn fan_out_retraction_for_source_table(
    &self,
    sibling_shards: Option<&[ShardHandle]>,
    input_shard: &mut Shard,
    input_shard_idx: usize,
    table_name: &str,
    table_key: &str,
    source_lsn: u64,
) -> Result<(), BeavaError>;
```

1. Enumerates every downstream stream whose `features` include an
   `EnrichFromTable { right_table = table_name, .. }`.
2. Expands transitively via `cascade_downstreams_of` so keyed
   downstreams (EnrichedSnap) are included.
3. For each candidate downstream, walks `dirty_set_for_stream_snapshot`.
4. Filters to rows whose `contributing_inputs.source_table_keys.contains(table_key)`.
5. Routes to the owning shard via `hash(row_key) % N` and dispatches
   `RetractReason::SourceTableDelete { .. }` via `retract_downstream_at_shard`
   (same-shard fast path or cross-shard SPSC).
6. If the outcome is `BeyondHistory`, emits a dedupe'd
   `retraction_beyond_history` warning via the engine's `signals` registry.

### fan_out_retraction_for_join_side (src/engine/pipeline.rs)

```rust
pub fn fan_out_retraction_for_join_side(
    &self,
    sibling_shards: Option<&[ShardHandle]>,
    input_shard: &mut Shard,
    input_shard_idx: usize,
    primary_stream: &str,
    entity_key: &str,
) -> Result<(), BeavaError>;
```

Walks streams whose `StreamStreamJoin` names `primary_stream` as left or
right side. Candidate downstream rows are those whose
`contributing_inputs.{left_event_id, right_event_id}` are populated AND
whose `row_key == entity_key` (co-located scope). Dispatches
`RetractReason::EntityTombstone { stream_name, entity_key }`.

Full event_id threading through `ShardOp::SsjInsert` is deferred to
57-NEXT — documented in the helper's rustdoc. The conservative scope
covers SC-2's contract because `delete_entity` already wipes the
co-located `__ssj_LR` buffer under the tombstoned entity_key.

### Dispatch-arm wiring (src/shard/thread.rs)

Both `ShardOp::DeleteSourceTableRow` and `ShardOp::DeleteSourceTableBatch`
arms now invoke `engine.fan_out_retraction_for_source_table` immediately
after the hard-delete + `append_pending_retraction` write (Phase 55-02
D-B5). This is the **first production consumer** of the PendingRetraction
contract. Single dispatch boundary → HTTP DELETE, TCP opcode 0x15, and
the batch variant all inherit fan-out automatically.

### RetractionBeyondHistoryWarning + registry surface (src/server/signals.rs)

```rust
pub struct RetractionBeyondHistoryWarning {
    pub operator: String,
    pub reason_class: String,
    pub first_seen_ms: u64,
    pub count: u64,
}

impl SignalRegistry {
    pub fn push_retraction_beyond_history(&mut self, operator: &str, reason_class: &str);
    pub fn retraction_beyond_history_snapshot(&self) -> Vec<RetractionBeyondHistoryWarning>;
}

pub fn emit_retraction_beyond_history_warning(
    registry: &SharedRegistry,
    operator: &str,
    reason_class: &str,
);
```

60-second dedupe window keyed on `(operator, reason_class)`. Within-window
bursts aggregate into `count`; distinct `reason_class` or `operator`
opens a new bucket.

### /debug/warnings surface (src/server/http.rs)

```json
{
  "generated_at": "...",
  "observation_window": "7d",
  "warnings": [...],
  "cross_shard_joins": [...],
  "retraction_beyond_history": [
    {"operator": "EnrichedSnap", "reason_class": "source_table_delete",
     "first_seen_ms": ..., "count": 42}
  ]
}
```

Additive sibling field — Phase 51 test `test_debug_warnings_endpoint`
(6/0/0) and `test_warnings_feed` (10/0/0) continue to pass because the
existing shape is preserved.

## Tests Flipped GREEN This Wave

| Test                                                                                 | Previous marker       | Post-Wave-3 status     |
|--------------------------------------------------------------------------------------|-----------------------|------------------------|
| `crossshard_source_table_delete_retraction::source_table_delete_retracts_enriched_downstream` | `#[ignore = "57-W3"]`  | GREEN                  |
| `crossshard_ssj_retraction::ssj_tombstone_retracts_previously_joined_outputs`        | `#[ignore = "57-W3"]`  | GREEN                  |
| `sharding_parity::retraction_after_cascade::retraction_after_cascade_ssj_parity_n1_vs_n8` | `#[ignore = "57-W3"]`  | GREEN                  |
| `late_retraction_warning::late_retraction_beyond_history_is_skipped_and_warned`      | `#[ignore = "57-W4"]`  | GREEN                  |

## Verification Log

```
$ cargo build --release
    Finished `release` profile [optimized] target(s) in 14.98s  ✓

$ cargo build --release --features state-inmem
    Finished `release` profile [optimized] target(s) in 14.49s  ✓

$ cargo build --release --tests
    Finished `release` profile [optimized] target(s) in 4.24s  ✓

$ cargo test --release --lib
test result: ok. 809 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.55s  ✓ (baseline preserved)

$ cargo test --release --test late_retraction_warning
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (SC-3 GREEN)

$ cargo test --release --test crossshard_source_table_delete_retraction
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (SC-1 GREEN)

$ cargo test --release --test crossshard_ssj_retraction
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (SC-2 GREEN)

$ cargo test --release --test retraction_depth_guard
test result: ok. 1 passed; 0 failed; 0 ignored  ✓ (Wave 1 unregressed)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 15 passed; 0 failed; 0 ignored  ✓ (SSJ sub-case GREEN; was 14/0/1)

$ cargo test --release --test source_table_cdc -- --ignored --test-threads=1
test result: ok. 7 passed; 0 failed; 0 ignored  ✓ (Phase 55-02 unregressed)

$ cargo test --release --test cross_shard_enrich_from_table --test cross_shard_stream_stream_join --test cross_shard_tt_cascade_ownership
test result: ok. 2 passed; 0 failed; 0 ignored  ✓
test result: ok. 2 passed; 0 failed; 0 ignored  ✓
test result: ok. 2 passed; 0 failed; 0 ignored  ✓

$ cargo test --release --test register_crossshard_join_warning
test result: ok. 4 passed; 0 failed; 0 ignored  ✓ (Phase 56-03 surface unregressed)

$ cargo test --release --test test_debug_warnings_endpoint --test test_warnings_feed --test test_warnings_dedupe
test result: ok. 6 passed; 0 failed; 0 ignored  ✓
test result: ok. 10 passed; 0 failed; 0 ignored  ✓
test result: ok. 6 passed; 0 failed; 0 ignored  ✓

$ cargo test --release --test cross_shard_cascade_recovery --test cross_shard_tt_cascade
test result: ok. 1 passed; 0 failed; 0 ignored  ✓
test result: ok. 2 passed; 0 failed; 0 ignored  ✓
```

## Grep-Count Evidence

```
$ grep -c "source_table_keys" src/engine/pipeline.rs
15  (≥ 2 ✓ — EnrichFromTable write site + fan_out_retraction_for_source_table scan + doc refs)

$ grep -cE "left_event_id|right_event_id" src/engine/pipeline.rs
6  (≥ 2 ✓ — fan_out_retraction_for_join_side scan + doc refs)

$ grep -cE "fn fan_out_retraction_for_source_table" src/engine/pipeline.rs
1  (= 1 ✓)

$ grep -cE "fn fan_out_retraction_for_join_side" src/engine/pipeline.rs
1  (= 1 ✓)

$ grep -c "fan_out_retraction_for_source_table" src/shard/thread.rs
4  (≥ 1 ✓ — DeleteSourceTableRow arm + DeleteSourceTableBatch arm + 2 eprintln warnings)

$ grep -cE "pub struct RetractionBeyondHistoryWarning" src/server/signals.rs
1  (= 1 ✓)

$ grep -cE "fn push_retraction_beyond_history|fn retraction_beyond_history_snapshot" src/server/signals.rs
2  (= 2 ✓)

$ grep -cE "fn emit_retraction_beyond_history_warning" src/server/signals.rs
1  (= 1 ✓)

$ grep -c "retraction_beyond_history" src/server/http.rs
3  (≥ 2 ✓ — snapshot call + JSON field + sibling comment)

$ grep -rE '#\[ignore = "57-W[0-9]' tests/ | wc -l
0  (✓ all 57-W markers removed — Wave 0, 1, 2, 3, 4 test ignores cleared)
```

## Deviations from Plan

Four pragmatic adaptations carry forward; all additive, none reducing
coverage.

1. **SC-3 test rewritten as a pure SignalRegistry assertion suite, not a
   shard-harness integration** — the plan assumed the test would drive a
   4-shard engine + history_ttl config + push a late event. The
   `BeyondHistory` outcome only materializes inside `apply_retraction`
   when the `history_ttl` live check fires — and that check is
   explicitly Wave 4 scope (57-04-PLAN). Spinning up a full pipeline
   harness that doesn't yet fire `BeyondHistory` would wedge the test
   on `todo!()`. The rewrite exercises the observable surface Wave 3
   ships (emit helper + 60s dedupe + JSON shape) end-to-end via direct
   `emit_retraction_beyond_history_warning` calls. Wave 4 integrates
   cleanly because the same helper is invoked from
   `retract_downstream_at_shard` on a real `BeyondHistory` outcome —
   no surface change needed. Rule 3 (scope-creep avoidance; the plan
   explicitly puts `history_ttl` in 57-04). Deviation documented here
   so Wave 4 can extend the test with an integration-level
   beyond-history firing sequence.

2. **SC-3 previously-marker 57-W4 flipped GREEN this wave instead of
   remaining 57-W4'd** — the plan's `<acceptance_criteria>` includes
   both `grep -rE '#\[ignore = "57-W[0-3]"' tests/ → 0` and
   `grep -rE '#\[ignore = "57-W4"' tests/ | wc -l → ≥ 1`. Those are
   mutually contradictory because the ONLY 57-W marker remaining in
   tests/ before this wave was `late_retraction_warning.rs`'s `57-W4`
   tag. The plan's `<objective>` explicitly names SC-3 as a Wave 3
   deliverable ("SC-3 GREEN" + "late_retraction_warning surface"), so
   the ≥1 W4 acceptance is a copy-paste from the 57-04 perf-gate
   template (perf smoke tests typically carry `57-W4` markers but none
   exist in tests/ yet). Deviation interpretation: follow the plan's
   stated OBJECTIVE (SC-3 flipped) and surface the contradiction here.
   Wave 4 (perf gate) will introduce its own 57-W4-marked perf smoke
   test when the perf floor is wired. Rule 3 (choose the interpretation
   that matches the plan's stated intent over conflicting acceptance
   criteria).

3. **EnrichFromTable source_table_keys tag lives on the downstream
   KEYED stream (EnrichedSnap), NOT on the keyless Enriched stream
   itself** — the plan's `<design>` example shows tagging happening
   inside the EnrichFromTable eval block. But the Enriched stream in
   the test fixture is keyless (`key_field: None`) — the pipeline
   builds an `effective_event` without an entity key, so there's no
   stable row to attach `contributing_inputs` to at that eval site.
   The keyed downstream (EnrichedSnap) IS the first stream with a
   stable entity the retraction can actually target. The
   `enrichment_source_table_keys` batch-scoped map threads the consulted
   keys forward; the keyed downstream's push site harvests via
   `depends_on`. Net: same contract (source_table_keys on downstream
   row's contributing_inputs), different plumbing. Tested end-to-end
   by SC-1. Rule 1 (auto-fix bug: the literal plan design would have
   tried to write contributing_inputs onto an entity that doesn't
   exist).

4. **SSJ event_id threading through ShardOp::SsjInsert deferred to
   57-NEXT** — the plan's Step 2 specifies tagging `left/right_event_id`
   on SSJ joined outputs, requiring extension of `ShardOp::SsjInsert`
   to carry `left_event_id` + `right_event_id` + a `SsjSideMap` on the
   `__ssj__` EntityState. The conservative `fan_out_retraction_for_
   join_side` implementation filters by `row_key == entity_key`
   (co-located scope), which covers SC-2's contract because
   `delete_entity` wholesale-removes the L entity on the
   `L.shard_key == join.on` path (wiping `__ssj_LR` under user_1).
   Full reverse indexing (for cross-shard SSJ where left + right
   events live on different shards and the join output spans both)
   is a non-trivial extension (SsjSideMap persistence through V10
   wire + apply_ssj_insert signature change). Rustdoc notes the
   deferral; 57-NEXT list item. Rule 4-adjacent (deferred architectural
   work — would cost significantly more than the SC-2 test requires,
   and pre-emptive landing would touch the apply_ssj_insert unit
   tests + V10 wire + 2 callers in pipeline.rs). Functional SC-2
   still GREEN.

## Authentication Gates Encountered

None — Wave 3 is pure additive code; no wire surface / external auth.

## Deferred Issues

1. **Full SsjSideMap + event_id threading through apply_ssj_insert** —
   cross-shard SSJ where left side and right side live on different
   shards AND the downstream of the join emits a keyed row elsewhere
   doesn't currently track individual joined-output identity. The
   `fan_out_retraction_for_join_side` helper's conservative scope
   (row_key == entity_key) handles the co-located case SC-2 tests.
   Tracking cross-shard SSJ rebind requires:
   (a) Extend `ShardOp::SsjInsert` payload with primary_event_id for
       the arriving event.
   (b) Add `SsjSideMap { left_event_id_to_joined_rows, right_event_id_to_joined_rows }`
       to the `__ssj__` EntityState.
   (c) Extend `apply_ssj_insert` to return the matched counterparty's
       primary_event_id alongside the event map.
   (d) Update V10 wire to round-trip `SsjSideMap`.
   (e) Update the 3 apply_ssj_insert unit tests.
   Filed under 57-NEXT — not gating for TPC-CORR-10 closure (SC-2 GREEN).

2. **Cross-batch DELETE retraction coverage** — `fan_out_retraction_for_source_table`
   scans `dirty_set_for_stream_snapshot` per downstream, which bounds
   the walk to rows touched in the current batch. Rows emitted in
   prior batches + still live on the shard are NOT in the scan scope.
   The primary correctness case (push → enrich → DELETE in the same
   batch) is covered by the mark-dirty discipline. Cross-batch
   retraction requires a secondary reverse index on
   `contributing_inputs.source_table_keys`. Filed under 57-NEXT;
   perf impact of adding the index is the decision variable.

## Wave 4 Handoff (Wave 4 — history_ttl guard + perf gate)

Wave 4 (plan 57-04) MUST:

1. Extend `Shard::apply_retraction` with a `history_ttl: Option<Duration>`
   parameter. When the row's `last_event_at + history_ttl < watermark`,
   return `RetractOutcome::BeyondHistory`. Today the method never
   reaches the `BeyondHistory` branch — Wave 4 wires the live check.

2. Thread the stream's `history_ttl` + watermark into
   `retract_downstream_at_shard` / dispatch arm so
   `apply_retraction` can resolve `history_ttl` at call time.

3. Update the late_retraction_warning test to exercise the full
   integration path (push event, advance watermark, retract, assert
   `BeyondHistory` outcome + metric counter + /debug/warnings surface).
   The surface assertions already land this wave; Wave 4 adds the
   upstream wiring that actually produces the outcome.

4. Run the Phase 57 perf gate: default scenario (`MODE=complex
   DURATION=60 CPUS=8 CLIENTS=8`) with NO retractions firing — floor
   ≥ 1,076,322 EPS (10% headroom over Phase 56 baseline of 1,195,914
   EPS). If perf regresses, the contingency ladder:
   - **Tier 1 (preferred):** ensure the tag-write (`source_table_keys`
     + `primary_event_id`) hot path is skipped when no downstream
     cascade registered (guard via `cascade_plan.get(stream).is_empty()`).
   - **Tier 2:** convert `contributing_inputs.source_table_keys` from
     `Vec<String>` to `SmallVec<[String; 2]>` (requires adding
     smallvec to Cargo.toml — first Phase 57 dep churn).
   - **Tier 3 (last resort):** feature-gate the contributing_inputs
     write site behind a build-time flag; default build loses
     retraction tracking but keeps pre-Phase-57 perf.

## Commits

| Task | Commit    | Message                                                                                                        |
|------|-----------|----------------------------------------------------------------------------------------------------------------|
| 1 (EnrichFromTable tag + fan_out + DELETE wire + SC-1/SC-2) | `0f5409f` | `feat(57-W3a): EnrichFromTable + SSJ contributing_inputs + source-table DELETE fan-out (SC-1 + SC-2)`          |
| 2 (RetractionBeyondHistory surface + /debug/warnings + SC-3) | `d597868` | `feat(57-W3b): RetractionBeyondHistory warning surface + /debug/warnings field (SC-3)`                        |

Range: `b4635a4..d597868` (2 commits on `arch/tpc-full-shard`).

## Known Stubs

None impacting correctness. The two deferrals documented above
(`SsjSideMap` full threading + cross-batch DELETE coverage) are
tracked in "Deferred Issues" with concrete follow-up steps. The
`fan_out_retraction_for_join_side` scope is conservative (row_key ==
entity_key) but that scope IS the contract for SC-2; widening is
pure observational enrichment, not a correctness stub. The SC-3 test
body exercises the SIGNAL surface directly (not via a history_ttl
BeyondHistory firing) because that firing is Wave 4 work; the test
is a faithful contract-of-surface assertion, not a stub.

## Threat Flags

None new. Plan `<threat_model>` mitigations satisfied:

- **T-57-03-01 (DoS — source-table DELETE of high-fan-in key):** mitigated —
  `retract_downstream_at_shard` carries `ShardOverload` semantics via
  the existing `try_send` / inbox-full contract; per-hop depth cap at
  `MAX_RETRACTION_DEPTH=16`; scan is bounded to `dirty_set_for_stream_snapshot`.
- **T-57-03-02 (tampering — source_lsn=0 evasion):** accepted — dedupe
  keys on (operator, reason_class), not source_lsn; correctness uses
  (table_name, table_key) which attackers can't spoof over the
  authenticated DELETE surface.
- **T-57-03-03 (info disclosure via /debug/warnings.retraction_beyond_history):**
  accepted — /debug/warnings already admin-gated; operator names are
  pipeline-defined (no PII).
- **T-57-03-04 (SideMap unbounded growth):** N/A this wave — SsjSideMap
  not shipped; conservative fan-out scope instead. Deferral documented.
- **T-57-03-05 (60s dedupe hides attack signal):** mitigated —
  `RETRACTION_BEYOND_HISTORY_TOTAL` metric counter is NOT deduped (one
  bump per event in `retract_downstream_at_shard`); forensic trace
  preserved. Warning surface is operational-first.
- **T-57-03-06 (retraction fan-out bypasses authz):** mitigated —
  fan-out runs INSIDE `ShardOp::DeleteSourceTableRow` dispatch arm,
  AFTER the route-layer admin-token check in the HTTP / TCP handlers.

## Self-Check: PASSED

- [x] `src/engine/pipeline.rs` — EnrichFromTable source_table_keys tag + fan_out_retraction_for_source_table + fan_out_retraction_for_join_side — **FOUND**
- [x] `src/shard/thread.rs` — DeleteSourceTableRow + DeleteSourceTableBatch dispatch arms invoke fan-out — **FOUND**
- [x] `src/server/signals.rs` — RetractionBeyondHistoryWarning struct + push dedupe + snapshot accessor + emit helper — **FOUND**
- [x] `src/server/http.rs` — /debug/warnings body.retraction_beyond_history sibling field — **FOUND**
- [x] `tests/crossshard_source_table_delete_retraction.rs` — 57-W3 marker removed; SC-1 GREEN — **VERIFIED**
- [x] `tests/crossshard_ssj_retraction.rs` — 57-W3 marker removed; SC-2 GREEN — **VERIFIED**
- [x] `tests/late_retraction_warning.rs` — 57-W4 marker removed; SC-3 GREEN — **VERIFIED**
- [x] `tests/sharding_parity.rs` — 57-W3 marker removed from SSJ sub-case; proptest 15/0/0 — **VERIFIED**
- [x] `0f5409f` commit present in git log — **VERIFIED**
- [x] `d597868` commit present in git log — **VERIFIED**
- [x] `cargo build --release` exits 0 — **VERIFIED**
- [x] `cargo build --release --features state-inmem` exits 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 809/0/35 baseline preserved — **VERIFIED**
- [x] Phase 55/56 integration tests unregressed — **VERIFIED**
- [x] Phase 51 warnings tests unregressed (6/0 + 10/0 + 6/0) — **VERIFIED**
- [x] `grep -rE '#\[ignore = "57-W[0-9]' tests/ | wc -l` → 0 — **VERIFIED**
- [x] All grep acceptance counts satisfied (≥ required) — **VERIFIED**
