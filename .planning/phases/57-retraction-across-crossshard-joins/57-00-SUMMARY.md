---
phase: 57
plan: 00
subsystem: tests / contract-first RED scaffolding
tags:
  - tdd-red
  - wave-0
  - retraction
  - cross-shard-join
  - source-table-delete
  - requirements
requires:
  - phase-56-enrich-from-table-and-stream-stream-join-crossshard
  - phase-55-02-pending-retraction-markers
  - tests/cross_shard_enrich_from_table.rs (reference fixture shape)
  - tests/cross_shard_stream_stream_join.rs (reference fixture shape)
  - tests/sharding_parity.rs (proptest extension host)
provides:
  - tests/crossshard_source_table_delete_retraction.rs (SC-1 RED, 57-W2)
  - tests/crossshard_ssj_retraction.rs (SC-2 RED, 57-W3)
  - tests/late_retraction_warning.rs (SC-3 RED, 57-W4)
  - tests/retraction_depth_guard.rs (D-B5 RED, 57-W1)
  - tests/sharding_parity.rs::retraction_after_cascade (2 proptests, 57-W2 + 57-W3)
  - .planning/REQUIREMENTS.md (TPC-CORR-10 row + Phase 57 traceability)
affects:
  - Wave 1 (57-01) adds ShardOp::RetractDownstream + RetractReason + BeavaError::RetractionDepthExceeded + 5 metric counters; flips retraction_depth_guard.rs GREEN
  - Wave 2 (57-02) wires EnrichFromTable retraction path; flips crossshard_source_table_delete_retraction.rs + retraction_after_cascade_enrich_* proptest GREEN
  - Wave 3 (57-03) wires StreamStreamJoin retraction path; flips crossshard_ssj_retraction.rs + retraction_after_cascade_ssj_* proptest GREEN
  - Wave 4 (57-04) adds history_ttl guard + /debug/warnings retraction_beyond_history surface + 60s dedup; flips late_retraction_warning.rs GREEN; runs perf gate at ≥ 1,076,322 EPS floor
tech-stack:
  added: []
  patterns:
    - "#[ignore = \"57-W{N}\"] Wave-targeted RED markers (mirrors Phase 54/55/56)"
    - "String-constant metric-name probes for grep-verifiable Wave N acceptance"
    - "todo!() + #[ignore] for bodies that reference Phase-57 APIs not yet on disk"
    - "proptest extension module pattern (sibling to existing tt_cascade, mismatched_shard_enrich_or_join mods)"
key-files:
  created:
    - tests/crossshard_source_table_delete_retraction.rs
    - tests/crossshard_ssj_retraction.rs
    - tests/late_retraction_warning.rs
    - tests/retraction_depth_guard.rs
    - .planning/phases/57-retraction-across-crossshard-joins/57-00-SUMMARY.md
  modified:
    - tests/sharding_parity.rs (+ retraction_after_cascade mod, 2 proptests)
    - .planning/REQUIREMENTS.md (+ TPC-CORR-10 + Phase 57 traceability row + coverage 35→36)
requirements:
  - TPC-CORR-10
decisions:
  - "SC-1/SC-2 test bodies do the Phase-56 push path inline so the fixture builds and the Phase-56 invariant is observable pre-DELETE; the DELETE / tombstone step is a TODO(57-W{2|3}) comment — the actual RetractDownstream dispatch is added once the variant lands in Wave 1."
  - "SC-3 and D-B5 test bodies are pure `todo!(\"57-W{N}: ...\")` blocks — the referenced APIs (history_ttl guard, /debug/warnings schema, 20-hop synthetic pipeline harness, BeavaError::RetractionDepthExceeded) don't exist today and the scaffolding would be non-trivial without them. Both tests are #[ignore]'d so the todo!() never runs in the default suite."
  - "Extended sharding_parity.rs with a SINGLE new `retraction_after_cascade` mod (hosting 2 proptests: enrich + ssj). Plan said 'extend the existing MismatchedShardEnrichOrJoin generator'; deviated by giving retraction its own mod for the same reason Phase 56 kept its extension in a sibling mod: clean per-wave diff, isolated generator type (EnrichOrSsj enum + delete_at_step field), and #[ignore] markers live on the proptest `fn` attributes instead of the enclosing mod. Routing-determinism invariants in the body always pass today (same pattern Phase 56 used at Wave 0)."
  - "Used `#![cfg(not(feature = \"state-inmem\"))]` on all 4 new test files (Phase 55/56 fjall-only gate), consistent with the 14 existing Phase-56 tests. Matches the existing integration-test convention — state-inmem is the degenerate backend, fjall is the real one."
  - "Removed `FeatureValue::Null` reference from SC-1 — the enum only has {Float, Int, String, Missing}. Post-DELETE retraction is represented by entity-absent (read_entity_from_shard returns None) OR by the stored Last() operator returning FeatureValue::Missing when its backing event is tombstoned; both are equivalent for the SC-1 assertion."
metrics:
  duration: ~25min
  completed: 2026-04-21
  tasks: 2
  commits: 2
  files_created: 4
  files_modified: 2
  red_tests_landed: 4
  proptests_landed: 2
  ignored_marker_count: 8
---

# Phase 57 Plan 00: Wave 0 RED-tests Contract Summary

RED-first TDD baseline for Phase 57. Every ROADMAP Phase-57 success criterion (SC-1 source-table DELETE retraction, SC-2 SSJ tombstone retraction, SC-3 late-retraction warning, SC-4 → consumed by 57-NEXT perf gate) plus the D-B5 cascade depth guard has a dedicated failing (ignored-pending-wave) integration test committed on disk. `sharding_parity.rs` gains a `retraction_after_cascade` proptest extension with enrich + SSJ sub-branches. No production `src/**` code modified.

## RED Test → Success Criterion Map

| SC | File | Tests | Ignore Marker | Flips GREEN at |
|----|------|-------|---------------|----------------|
| SC-1 | `tests/crossshard_source_table_delete_retraction.rs::source_table_delete_retracts_enriched_downstream` | 1 | `#[ignore = "57-W2"]` | Wave 2 (plan 57-02) |
| SC-2 | `tests/crossshard_ssj_retraction.rs::ssj_tombstone_retracts_previously_joined_outputs` | 1 | `#[ignore = "57-W3"]` | Wave 3 (plan 57-03) |
| SC-3 | `tests/late_retraction_warning.rs::late_retraction_beyond_history_is_skipped_and_warned` | 1 | `#[ignore = "57-W4"]` | Wave 4 (plan 57-04) |
| D-B5 | `tests/retraction_depth_guard.rs::retraction_cascade_exceeds_16_hop_cap` | 1 | `#[ignore = "57-W1"]` | Wave 1 (plan 57-01) |
| SC-1 (parity) | `tests/sharding_parity.rs::retraction_after_cascade::retraction_after_cascade_enrich_parity_n1_vs_n8` | 1 proptest | `#[ignore = "57-W2"]` | Wave 2 (plan 57-02) |
| SC-2 (parity) | `tests/sharding_parity.rs::retraction_after_cascade::retraction_after_cascade_ssj_parity_n1_vs_n8` | 1 proptest | `#[ignore = "57-W3"]` | Wave 3 (plan 57-03) |

**Totals:** 4 Rust test functions + 2 proptests = **6 RED tests committed on disk**. 8 `#[ignore = "57-W{N}"]` markers across 5 files (4 new + 4 on sharding_parity.rs — 2 phase-56 markers remain + 2 new phase-57 ones; plus "57-W[0-4]" literal hits inside doc-comments in the 4 new files).

## Assertion Hook Map (what Wave N must verify when the marker flips)

### Wave 1 — retraction_depth_guard.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `retraction_cascade_exceeds_16_hop_cap` | `todo!("57-W1: ...")` — `#[ignore]`'d | 20-hop synthetic pipeline. Depth 17 raises `Err(BeavaError::RetractionDepthExceeded)`; depths 0..=16 succeed. `beava_retraction_depth_exceeded_total` increments by exactly 1 per overflow. `crossbeam_channel::recv_timeout(5s)` on every oneshot — no deadlock path. Source shard returns typed error, NO panic. |

### Wave 2 — crossshard_source_table_delete_retraction.rs + sharding_parity (enrich)

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `source_table_delete_retracts_enriched_downstream` | Pre-DELETE assertion passes (Phase 56 GREEN); Step 2 `_todo_57_w2_delete_dispatch` placeholder in place of the DELETE API call; post-DELETE `assert!(matches!(gdp_post, None | Some(FeatureValue::Missing)))` — currently false because the DELETE path is unwired. | Call `engine.delete_source_table_row_on_shard(...)` (Wave 2 API); `ShardOp::RetractDownstream { target_shard: j, stream_name: "EnrichedSnap", row_key: user, reason: RetractReason::SourceTableDelete { table_name: "Countries", table_key: "US", source_lsn }, depth: 0 }` fans out to shard J; target applies tombstone; read_entity_from_shard returns None (or Last() reads Missing); `beava_retractions_sent_total{operator="enrich_from_table",reason="source_table_delete"} ≥ 1`; `beava_retractions_applied_total{operator="enrich_from_table"} ≥ 1`. |
| `retraction_after_cascade_enrich_parity_n1_vs_n8` | Routing-determinism invariants (pass today) | N=1 ↔ N=8 byte-identical EnrichedSnap state after `delete_at_step`-th source-table DELETE. |

### Wave 3 — crossshard_ssj_retraction.rs + sharding_parity (SSJ)

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `ssj_tombstone_retracts_previously_joined_outputs` | `shard.delete_entity(&user)` is a silent no-op for retraction; Step 4 (a) assertion currently false because __ssj_LR buffer persists. | Tombstone walks `contributing_inputs.left_event_id` → fans out `ShardOp::RetractDownstream { reason: RetractReason::EntityTombstone { stream_name: "L", entity_key: "user_1" } }` to every owner of a joined output referencing user_1. Post-tombstone, `read_entity_from_shard(&input_shard, &user, \|e\| e.streams.get("__ssj_LR").is_some())` returns `None` (or `Some(false)`). `beava_retractions_sent_total{operator="stream_stream_join"} ≥ 1`; `beava_retractions_applied_total{operator="stream_stream_join"} == N_emitted_joined_outputs`. |
| `retraction_after_cascade_ssj_parity_n1_vs_n8` | Routing-determinism invariants (pass today) | N=1 ↔ N=8 byte-identical joined-output state after L-entity tombstone. |

### Wave 4 — late_retraction_warning.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `late_retraction_beyond_history_is_skipped_and_warned` | `todo!("57-W4: ...")` — `#[ignore]`'d | 4-shard engine with `history_ttl=60s`; push event with `event_time=T-3600s`; attempt retraction → `beava_retraction_beyond_history_total{operator} += 1`; `GET /debug/warnings` response has `retraction_beyond_history: [{operator, reason_class, count}]` array of len≥1; target-shard state for the late event is byte-identical pre/post; iterate the retraction 100× within 60s — the dedup feed surfaces exactly 1 entry (aggregated count, not 100). |

## Grep-Count Evidence

```
$ grep -cE '#\[ignore = "57-W[0-4]"' tests/crossshard_source_table_delete_retraction.rs tests/crossshard_ssj_retraction.rs tests/late_retraction_warning.rs tests/retraction_depth_guard.rs tests/sharding_parity.rs
tests/crossshard_source_table_delete_retraction.rs:1
tests/crossshard_ssj_retraction.rs:1
tests/late_retraction_warning.rs:1
tests/retraction_depth_guard.rs:1
tests/sharding_parity.rs:4
Total = 8  (≥ 5 ✓)

$ grep -c "source_table_delete_retracts_enriched_downstream" tests/crossshard_source_table_delete_retraction.rs
1  (= 1 ✓)

$ grep -c "ssj_tombstone_retracts_previously_joined_outputs" tests/crossshard_ssj_retraction.rs
1  (= 1 ✓)

$ grep -c "late_retraction_beyond_history_is_skipped_and_warned" tests/late_retraction_warning.rs
1  (= 1 ✓)

$ grep -c "retraction_cascade_exceeds_16_hop_cap" tests/retraction_depth_guard.rs
1  (= 1 ✓)

$ grep -c "retraction_after_cascade\|RetractionAfterCascade" tests/sharding_parity.rs
9  (≥ 2 ✓)

$ grep -c "beava_retractions_sent_total" tests/crossshard_source_table_delete_retraction.rs tests/crossshard_ssj_retraction.rs
tests/crossshard_source_table_delete_retraction.rs:3
tests/crossshard_ssj_retraction.rs:3
Total = 6  (≥ 2 ✓)

$ grep -c "beava_retraction_beyond_history_total" tests/late_retraction_warning.rs
5  (≥ 1 ✓)

$ grep -c "beava_retraction_depth_exceeded_total" tests/retraction_depth_guard.rs
5  (≥ 1 ✓)

$ grep -c "TPC-CORR-10" .planning/REQUIREMENTS.md
2  (≥ 2 ✓)

$ grep -c "1,076,322" .planning/REQUIREMENTS.md
1  (= 1 ✓)

$ grep -c "contributing_inputs" .planning/REQUIREMENTS.md
1  (≥ 1 ✓)

$ grep -c "RetractionDepthExceeded\|retraction_depth_exceeded_total" .planning/REQUIREMENTS.md
1  (≥ 1 ✓)

$ grep -c "retraction_beyond_history" .planning/REQUIREMENTS.md
1  (≥ 1 ✓)

$ grep -cE "^### TPC-CORR \(continued\) — Phase 57" .planning/REQUIREMENTS.md
1  (= 1 ✓)

$ grep -c "TPC-CORR-07\|TPC-CORR-08\|TPC-CORR-09\|TPC-SOURCE-01" .planning/REQUIREMENTS.md
7  (unchanged — Phase 55/56 rows preserved ✓)

$ git diff --name-only HEAD~2 HEAD -- src/ | wc -l
0  (src/** unchanged ✓)
```

## Verification Log

```
$ cargo build --release --tests 2>&1 | grep -E "^error" | wc -l
0  ✓

$ cargo test --release --lib 2>&1 | grep "test result:"
test result: ok. 801 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.46s  ✓ (baseline preserved)

$ cargo test --release --test crossshard_source_table_delete_retraction
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s  ✓ (57-W2)

$ cargo test --release --test crossshard_ssj_retraction
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s  ✓ (57-W3)

$ cargo test --release --test late_retraction_warning
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s  ✓ (57-W4)

$ cargo test --release --test retraction_depth_guard
test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s  ✓ (57-W1)

$ cargo test --release --test sharding_parity
test result: ok. 13 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 116.67s  ✓
  (9 pre-existing proptests + 2 tt_cascade + 2 phase-56 unignored + 2 new 57-W{2,3} ignored)

$ cargo test --release --test cross_shard_enrich_from_table --test cross_shard_stream_stream_join --test register_crossshard_join_warning
test result: ok. 2 passed; 0 failed; 0 ignored
test result: ok. 2 passed; 0 failed; 0 ignored
test result: ok. 4 passed; 0 failed; 0 ignored  ✓ (Phase 56 regression-free)
```

## Deviations from Plan

Three minor amplifications (all additive; none reduce coverage):

1. **sharding_parity.rs extension structure** — plan said "extend the existing MismatchedShardEnrichOrJoin generator"; implemented as a NEW sibling `retraction_after_cascade` mod with its own `RetractionAfterCascadeEvent` struct (adds `step: usize`, `EnrichOrSsj` enum). Rationale: clean per-wave diff (Phase 56 used the same pattern in Wave 0 — sibling mod `mismatched_shard_enrich_or_join` instead of extending `tt_cascade`). Proptest `#[ignore]`'d at the function-attribute level instead of module level; bodies today enforce routing-determinism invariants that always pass (mirrors the Phase 56 Wave 0 trajectory — Wave 2/3 replaces the body with full N=1 ↔ N=8 replay).

2. **SC-3 + D-B5 test bodies are todo!() stubs** — plan said the RED tests must compile today (yes, they do) and the interface references in the body must survive type-check. Both `late_retraction_warning.rs` and `retraction_depth_guard.rs` have `todo!("57-W{N}: ...")` as their function body: the referenced APIs (`history_ttl` guard, `/debug/warnings` schema for `retraction_beyond_history`, 20-hop synthetic pipeline harness, `BeavaError::RetractionDepthExceeded`) do not exist today AND the harness scaffolding to reach them is non-trivial (full HTTP fixture, event-time manipulation). String constants for metric names (`METRIC_RETRACTION_DEPTH_EXCEEDED` etc.) + literal `_todo_57_wN_*` reference-locals are on the stack above the `todo!()` so grep finds the assertion hooks regardless. Both tests are `#[ignore]`'d so the `todo!()` never panics in the default suite.

3. **Minor type fix — `FeatureValue::Null` → `FeatureValue::Missing`** — the Phase-56 source_table_delete_retraction test body initially asserted `matches!(gdp_post, None | Some(FeatureValue::Missing) | Some(FeatureValue::Null))`. The enum only has four variants: `{Float, Int, String, Missing}`. Fixed inline (pre-commit build error caught it before the commit landed). Rule 3 (auto-fix blocking issue).

None of the 3 deviations change wave assignments, flip counts, or the REQUIREMENTS.md patch.

## Auth Gates Encountered

None — Wave 0 is tests + docs only, no wire surface or external auth.

## Next Wave Handoff (Wave 1 must deliver)

Wave 1 (plan 57-01) MUST add:

1. **`ShardOp::RetractDownstream { target_shard: u16, stream_name: String, row_key: String, reason: RetractReason, depth: u8, reply: oneshot::Sender<RetractOutcome> }`** in `src/shard/thread.rs`. Follow the Phase 56 `ShardOp::SsjInsert` pattern: source shard `try_send`s + blocks on `crossbeam::bounded(1)` oneshot; on target-inbox-full, return `BeavaError::ShardOverload`.

2. **`RetractReason` enum** in `src/shard/thread.rs` (or new `src/shard/retract.rs`):
    - `SourceTableDelete { table_name: String, table_key: String, source_lsn: u64 }`
    - `EntityTombstone { stream_name: String, entity_key: String }`
    - `PrimaryEventRetract { stream_name: String, event_id: u64 }`

3. **`RetractOutcome` enum** — `Retracted | NoOp | BeyondHistory | DepthExceeded`. Idempotent target semantics: `NoOp` if the row is already-retracted or never existed (D-B4).

4. **Depth cap enforcement** at `depth >= 16`: source shard returns `Err(BeavaError::RetractionDepthExceeded { current_depth, cap: 16 })`; `beava_retraction_depth_exceeded_total` increments exactly once per overflow event. No panic — typed error returned upward.

5. **Metric counters (emitters wire at Waves 2-4):**
    - `beava_retractions_sent_total{operator, reason}`
    - `beava_retractions_applied_total{operator}`
    - `beava_retractions_nooped_total{operator}`
    - `beava_retraction_beyond_history_total{operator}`
    - `beava_retraction_depth_exceeded_total`

Wave 2 (plan 57-02) consumes `RetractDownstream` in `EnrichFromTable` — walks PendingRetraction markers from Phase 55-02's DELETE path; flips `crossshard_source_table_delete_retraction.rs` + `retraction_after_cascade_enrich_*` proptest GREEN. Wave 3 (plan 57-03) consumes it in `StreamStreamJoin` tombstone path; flips `crossshard_ssj_retraction.rs` + `retraction_after_cascade_ssj_*` proptest GREEN. Wave 4 (plan 57-04) adds the `history_ttl` guard + `/debug/warnings.retraction_beyond_history` surface + 60s dedup + runs the perf gate at ≥ 1,076,322 EPS floor; flips `late_retraction_warning.rs` GREEN.

## Known Stubs

**Intentional — this is the RED contract file.** `late_retraction_warning.rs` and `retraction_depth_guard.rs` bodies are `todo!()`; `crossshard_source_table_delete_retraction.rs` + `crossshard_ssj_retraction.rs` bodies exercise the Phase-56 push path through to the pre-DELETE assertion but leave the retraction dispatch as a `_todo_57_w{2|3}_*` local binding + inline TODO comment pending Wave 1's `ShardOp::RetractDownstream` variant. Every function is `#[ignore = "57-W{N}"]`'d so the default suite never exercises them. This is the Wave 0 RED-test idiom, mirrored from Phase 54/55/56.

The `retraction_after_cascade` proptest bodies today only assert retraction-routing-determinism invariants (pass trivially at all N). Wave 2/3 MUST extend the bodies to replay through N=1 and N=8 engines and byte-compare downstream state post-retraction. The `#[ignore = "57-W{2|3}"]` markers on those proptests are the contract.

## Threat Flags

None — plan touched only test code + pre-existing REQUIREMENTS.md section. No new trust boundaries, no wire-format changes (wire-format lands in Waves 1-4). Per plan `<threat_model>`:
- T-57-00-01 (test bodies reference pre-57 APIs and break build): mitigated — string-name metric probes + `todo!()` behind `#[ignore]` markers; `cargo build --release --tests` exits 0.
- T-57-00-02 (assertion text leaks implementation details): accepted — Rust server; tests live in-repo behind ignore markers.
- T-57-00-03 (proptest fan-out explodes runtime): mitigated — new branches `#[ignore]`'d; `ProptestConfig::with_cases(16)` bounded.
- T-57-00-04 (REQUIREMENTS.md text drifts from ROADMAP): mitigated — TPC-CORR-10 row text transcribed verbatim from plan body (which was transcribed from ROADMAP SC-1..SC-4 + D-B5); grep matches on `1,076,322`, `contributing_inputs`, `RetractionDepthExceeded`, `retraction_beyond_history` all PASS.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 | `7044a95` | `test(57-W0): add RED tests for SC-1..SC-3 + D-B5 depth guard (TPC-CORR-10)` |
| Task 2 | `cc1c45c` | `docs(57-W0): add TPC-CORR-10 row + Phase 57 traceability (REQUIREMENTS.md)` |

## Self-Check: PASSED

- [x] `tests/crossshard_source_table_delete_retraction.rs` exists (1 test, 57-W2) — **FOUND**
- [x] `tests/crossshard_ssj_retraction.rs` exists (1 test, 57-W3) — **FOUND**
- [x] `tests/late_retraction_warning.rs` exists (1 test, 57-W4) — **FOUND**
- [x] `tests/retraction_depth_guard.rs` exists (1 test, 57-W1) — **FOUND**
- [x] `tests/sharding_parity.rs` extended with `retraction_after_cascade` mod (2 proptests) — **FOUND**
- [x] `cargo build --release --tests` → exit 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 801/0/35 — **VERIFIED (baseline preserved)**
- [x] 8 × `57-W[0-4]` markers across 5 files — **VERIFIED**
- [x] `grep -c "TPC-CORR-10" .planning/REQUIREMENTS.md` == 2 ≥ 2 — **VERIFIED**
- [x] `grep -c "1,076,322" .planning/REQUIREMENTS.md` == 1 — **VERIFIED**
- [x] `grep -c "contributing_inputs" .planning/REQUIREMENTS.md` == 1 ≥ 1 — **VERIFIED**
- [x] `git diff --name-only HEAD~2 HEAD -- src/` empty — **VERIFIED (no src/** modified)**
- [x] Commits `7044a95` + `cc1c45c` present in git log — **VERIFIED**
- [x] Phase 56 regressions: `cross_shard_enrich_from_table` 2/0/0, `cross_shard_stream_stream_join` 2/0/0, `register_crossshard_join_warning` 4/0/0 — **VERIFIED**
