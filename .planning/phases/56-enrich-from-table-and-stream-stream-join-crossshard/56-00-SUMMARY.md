---
phase: 56
plan: 00
subsystem: tests / contract-first RED scaffolding
tags:
  - tdd-red
  - wave-0
  - cross-shard-enrich
  - cross-shard-ssj
  - crossshard-join-warning
  - requirements
requires:
  - phase-55-cascade-and-source-tables
  - tests/common/cascade_harness.rs (reference pattern)
  - tests/sharding_parity.rs (proptest layout)
provides:
  - tests/cross_shard_enrich_from_table.rs (SC-1 RED, 56-W2)
  - tests/cross_shard_stream_stream_join.rs (SC-2 RED, 56-W3)
  - tests/register_crossshard_join_warning.rs (SC-3 RED, 56-W3)
  - tests/crossshard_enrich_perf_smoke.rs (SC-4 + SC-5 RED, 56-W4)
  - tests/sharding_parity.rs::mismatched_shard_enrich_or_join (2 proptests, 56-W2 + 56-W3)
affects:
  - Wave 1 (56-01) adds ShardOp::ReadEntityAt / ReadEntityBatch / SsjInsert variants
  - Wave 2 (56-02) flips 4 tests GREEN (cross_shard_enrich + enrich-parity proptest)
  - Wave 3 (56-03) flips 7 tests GREEN (SSJ routing + register relaxation + debug endpoint)
  - Wave 4 (56-04) flips 2 tests GREEN (perf smoke + EPS-floor gate)
tech-stack:
  added: []
  patterns:
    - "#[ignore = \"56-W{N}\"] Wave-targeted RED markers (mirror Phase 54/55)"
    - "todo!() at runtime + compile-clean contract surface"
    - "proptest extension module pattern (sibling to existing tt_cascade mod)"
key-files:
  created:
    - tests/cross_shard_enrich_from_table.rs
    - tests/cross_shard_stream_stream_join.rs
    - tests/register_crossshard_join_warning.rs
    - tests/crossshard_enrich_perf_smoke.rs
    - .planning/phases/56-enrich-from-table-and-stream-stream-join-crossshard/56-00-SUMMARY.md
  modified:
    - tests/sharding_parity.rs (+ mismatched_shard_enrich_or_join mod, 2 proptests)
requirements:
  - TPC-CORR-04 (relaxed)
  - TPC-CORR-08
  - TPC-CORR-09
decisions:
  - "Added 2 tests instead of 1 for SC-1 (primary cross-shard + same-shard fast-path corollary) — matches the Phase 55 SC-1 pattern in cross_shard_tt_cascade_ownership.rs (2 tests there too). Acceptance criteria use '≥ 1' thresholds, so additive coverage is safe."
  - "Added 2 tests instead of 1 for SC-2 (cross-shard primary + co-located fast-path corollary) — same rationale."
  - "Added 3 tests instead of 2 for SC-3 (primary register-warning + co-located quiet-path + HTTP /debug/warnings surface) — explicitly listed in plan body Step 4 as two separate functions; I added the quiet-path corollary to symmetrize with D-B5 'co-location preserves no-op path'."
  - "BASELINE_P99_MICROS=50 µs chosen from the Phase 55 engineering baseline note (STATE.md Phase 55 engineering-complete entry). Wave 4 re-measures on the same hardware; if the spot measurement shifts, the constant updates in-place (test logic unchanged; 2× tolerance factor absorbs drift)."
  - "SC5_EPS_FLOOR=1,059,261 derived as 0.85 × PHASE_55_EPS_BASELINE(1,246,190) per D-D3; wired into the test as a const for grep-ability."
  - "Used #![cfg(not(feature = \"state-inmem\"))] (not the plan's literal `#![cfg(feature = \"server\")]`). Rationale: `server` is the default feature, so cfg(feature=\"server\") is a no-op on default build; the phase 55 tests used `#![cfg(not(feature = \"state-inmem\"))]` as the real gate (fjall-only codepath). Matches existing test convention verbatim."
  - "sharding_parity.rs mismatched_shard_enrich_or_join proptest body today enforces routing-determinism invariants only (always passes). Wave 2/3 MUST extend the body to run N=1 ↔ N=8 replay and byte-identical compare. Tests are #[ignore = \"56-W{N}\"]'d so they don't run in the default suite today."
metrics:
  duration: ~25min
  completed: 2026-04-20
  tasks: 1
  commits: 1
  files_created: 4
  files_modified: 1
  red_tests_landed: 11
  proptests_landed: 2
  ignored_marker_count: 20
---

# Phase 56 Plan 00: Wave 0 RED-tests Contract Summary

RED-first TDD baseline for Phase 56. All 5 success criteria (SC-1 EnrichFromTable cross-shard, SC-2 StreamStreamJoin cross-shard, SC-3 register() relaxation, SC-4 perf smoke, SC-5 perf gate) have dedicated failing (ignored-pending-wave) integration tests committed on disk. sharding_parity.rs gains a `MismatchedShardEnrichOrJoin` proptest extension. No production `src/*.rs` code modified.

## RED Test → Success Criterion Map

| SC | File | Tests | Ignore Marker | Flips GREEN at |
|----|------|-------|---------------|----------------|
| SC-1 | `tests/cross_shard_enrich_from_table.rs` | 2 (cross-shard primary + same-shard fast-path) | `#[ignore = "56-W2"]` | Wave 2 (plan 56-02) |
| SC-1 (parity) | `tests/sharding_parity.rs::mismatched_shard_enrich_or_join` | 1 proptest (`mismatched_shard_enrich_parity_n1_vs_n8`) | `#[ignore = "56-W2"]` | Wave 2 (plan 56-02) |
| SC-2 | `tests/cross_shard_stream_stream_join.rs` | 2 (cross-shard routing + co-located fast-path) | `#[ignore = "56-W3"]` | Wave 3 (plan 56-03) |
| SC-2 (parity) | `tests/sharding_parity.rs::mismatched_shard_enrich_or_join` | 1 proptest (`mismatched_shard_join_parity_n1_vs_n8`) | `#[ignore = "56-W3"]` | Wave 3 (plan 56-03) |
| SC-3 | `tests/register_crossshard_join_warning.rs` | 3 (primary warning + quiet co-located + HTTP `/debug/warnings`) | `#[ignore = "56-W3"]` | Wave 3 (plan 56-03) |
| SC-4 | `tests/crossshard_enrich_perf_smoke.rs::crossshard_enrich_p99_under_2x_baseline` | 1 | `#[ignore = "56-W4"]` | Wave 4 (plan 56-04) |
| SC-5 | `tests/crossshard_enrich_perf_smoke.rs::crossshard_enrich_eps_floor` | 1 (BEAVA_PERF_GATE=1) | `#[ignore = "56-W4"]` | Wave 4 (plan 56-04) |

**Totals:** 9 Rust test functions + 2 proptests = **11 RED tests committed on disk**. 20 total `#[ignore = "56-W{N}"]` markers across 5 files (9 test-attributes + 11 doc references).

## Assertion Hook Map (what Wave 2/3/4 must verify when the marker flips)

### Wave 2 — cross_shard_enrich_from_table.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `enrich_from_table_crosses_shard_boundary` | `todo!()` panic on run | Output features contain `gdp_usd == 800_000` after pushing `{user_id: uJ, country_code: "CH"}` where `J ≠ hash("CH") % 4`. Metric `beava_enrich_cross_shard_total{table="Countries"} ≥ 1`. |
| `enrich_from_table_same_shard_fast_path` | `todo!()` panic on run | Output contains `gdp_usd == 800_000`. `beava_enrich_intra_shard_total{table="Countries"} ≥ 1`. `beava_enrich_cross_shard_total{table="Countries"}` unchanged (zero for this test). |

### Wave 2 — sharding_parity.rs (parity extension)

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `mismatched_shard_enrich_parity_n1_vs_n8` | Passes trivially today (routing-determinism only) | Wave 2 extends body to replay generated batch through N=1 and N=8 engines; asserts byte-identical EnrichFromTable output for every enriched feature. |

### Wave 3 — cross_shard_stream_stream_join.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `stream_stream_join_routes_to_join_key_shard` | `todo!()` panic | `read_entity_from_shard(shard=J, key="u1", ...)` returns joined output; `read_entity_from_shard(shard=hash(session_id)%N, key="s1", ...)` returns None. `beava_ssj_cross_shard_total ≥ 2`. |
| `stream_stream_join_colocated_fast_path` | `todo!()` panic | Join output on shard J. `beava_ssj_cross_shard_total == 0`. |

### Wave 3 — register_crossshard_join_warning.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `register_emits_crossshard_warning_not_error` | `todo!()` panic | `register()` returns `Ok`. Captured tracing logs contain `"CrossShardJoinWarning"`, `"user_id"`, `"session_id"`. Counter `beava_crossshard_joins_registered_total ≥ 1`. |
| `register_colocated_join_emits_no_warning` | `todo!()` panic | Captured logs do NOT contain `"CrossShardJoinWarning"`. Counter unchanged. |
| `debug_warnings_endpoint_lists_cross_shard_joins` | `todo!()` panic | GET `/debug/warnings` → 200. `body.warnings.cross_shard_joins` is an array of length 1 with `{join_id, left_shard_key:"user_id", right_shard_key:"session_id", on_field:"user_id", perf_note contains "+1 inbox hop"}`. |

### Wave 3 — sharding_parity.rs (parity extension)

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `mismatched_shard_join_parity_n1_vs_n8` | Passes trivially today (routing-determinism only) | Wave 3 extends body to replay generated SSJ batch through N=1 and N=8 engines; asserts byte-identical joined output per join key. |

### Wave 4 — crossshard_enrich_perf_smoke.rs

| Test | Pre-flip state | Post-flip assertions |
|------|----------------|----------------------|
| `crossshard_enrich_p99_under_2x_baseline` | `todo!()` panic | 10_000-event sample; sorted latencies[9899] ≤ 2 × BASELINE_P99_MICROS (=100 µs today). Total wall-clock < 5 s. |
| `crossshard_enrich_eps_floor` | Early-return when `BEAVA_PERF_GATE != 1` | With env set, spawns `benchmark/fraud-pipeline/run_bench.sh MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_ENRICH_CROSSSHARD_SCENARIO=1`. Parses `"Aggregate EPS: <N>"`. Assert N ≥ 1_059_261. |

## BASELINE_P99_MICROS Justification

Starting value **50 µs** reflects Phase 55 engineering-complete spot-measurement captured in `.planning/STATE.md` during Phase 55 close. Rationale:

- Phase 55 baseline EPS: 1,246,190 events/sec on 8 CPUs = 123 k EPS/CPU.
- Per-event CPU budget: ~8 µs.
- p99 latency under load is typically 5-10× the mean — 50 µs is a conservative ceiling consistent with that Phase 55 workload.
- The test asserts `p99 ≤ 2 × 50 = 100 µs`, giving Phase 56 cross-shard enrichment a **2× latency budget** (accounting for one extra SPSC hop + oneshot recv).
- Wave 4 re-measures the same workload *without* cross-shard enrichment as the true baseline; if it's materially different from 50 µs, update the constant in-place and commit.

## REQUIREMENTS.md Diff Summary

REQUIREMENTS.md was already patched by the planner before this wave ran. Confirmed intact via grep:

- `TPC-CORR-04` — wrapped with `(**RELAXED in Phase 56**)` callout; text explains `register()` emits `CrossShardJoinWarning` + surfaces via `/debug/warnings` + counter `beava_crossshard_joins_registered_total`. 1 occurrence of "RELAXED in Phase 56".
- `TPC-CORR-08` — new row for EnrichFromTable cross-shard: `ShardOp::ReadEntityAt` / `ReadEntityBatch` dispatch, same-shard fast path, Missing semantics preserved, `beava_enrich_cross_shard_total` / `intra_shard_total` / `missing_total` metrics. 3 occurrences across file (header + traceability + cross-ref).
- `TPC-CORR-09` — new row for StreamStreamJoin: buffer lives on `hash(join.on)%N` in `ssj-<join_id>/` partition; `ShardOp::SsjInsert`; TPC-CORR-04 relaxation at register time; metrics `beava_ssj_cross_shard_total`, `beava_crossshard_joins_registered_total`. 2 occurrences.
- Traceability table: `| 56 | enrich-from-table-and-stream-stream-join-crossshard | TPC-CORR-04 (relaxation), TPC-CORR-08, TPC-CORR-09 |`.
- Coverage note updated: `35/35 requirements mapped` (adds 2 Phase 56 requirements).

## Grep-Count Evidence

```
$ grep -c "56-W[0-9]" tests/cross_shard_enrich_from_table.rs tests/cross_shard_stream_stream_join.rs tests/register_crossshard_join_warning.rs tests/crossshard_enrich_perf_smoke.rs tests/sharding_parity.rs
tests/cross_shard_enrich_from_table.rs:4
tests/cross_shard_stream_stream_join.rs:4
tests/register_crossshard_join_warning.rs:6
tests/crossshard_enrich_perf_smoke.rs:4
tests/sharding_parity.rs:2
Total = 20 (≥ 5 ✓)

$ grep -c "TPC-CORR-08" .planning/REQUIREMENTS.md
3  (≥ 2 ✓)

$ grep -c "TPC-CORR-09" .planning/REQUIREMENTS.md
2  (≥ 2 ✓)

$ grep -c "RELAXED in Phase 56" .planning/REQUIREMENTS.md
1  (≥ 1 ✓)

$ git diff --name-only HEAD~1 HEAD -- src/
(empty ✓)
```

## Verification Log

```
$ cargo build --release --tests
Finished `release` profile [optimized] target(s) in 37.35s  ✓

$ cargo test --release --lib
test result: ok. 796 passed; 0 failed; 35 ignored  ✓ (Phase 55 baseline preserved)

$ cargo test --release --test cross_shard_enrich_from_table -- --test-threads=1
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W2)

$ cargo test --release --test cross_shard_stream_stream_join -- --test-threads=1
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W3)

$ cargo test --release --test register_crossshard_join_warning -- --test-threads=1
test result: ok. 0 passed; 0 failed; 3 ignored  ✓ (56-W3)

$ cargo test --release --test crossshard_enrich_perf_smoke -- --test-threads=1
test result: ok. 0 passed; 0 failed; 2 ignored  ✓ (56-W4)

$ cargo test --release --test sharding_parity -- --test-threads=1
test result: ok. 11 passed; 0 failed; 2 ignored  ✓
  (9 pre-existing proptests + 2 tt_cascade + 2 new mismatched_shard_* ignored)
```

## Deviations from Plan

Four minor amplifications (all additive, none reduce coverage):

1. **SC-1 test count** — plan listed 1 test (`enrich_from_table_crosses_shard_boundary`). Added a 2nd test (`enrich_from_table_same_shard_fast_path`) to mirror Phase 55's 2-test SC-1 pattern in `cross_shard_tt_cascade_ownership.rs`. The plan's acceptance criteria say "1 test" but the plan body Step 2 only sketches one; since SC-1 must cover both the cross-shard primary path AND the D-A3 "same-shard fast path" contract, I symmetrized with Phase 55. Still satisfies "1 ignored" minimum.

2. **SC-2 test count** — added a 2nd test (`stream_stream_join_colocated_fast_path`) to cover D-B5 ("co-location preserved — no extra hop"). Same rationale as (1).

3. **SC-3 test count** — added a 3rd test (`register_colocated_join_emits_no_warning`) to guard against false-positive warnings on co-located joins. The plan sketched 2 tests (the other being the HTTP endpoint test, which is separate). `register_colocated_join_emits_no_warning` makes the TPC-CORR-04 relaxation's "only fires for mismatched case" clause observable.

4. **cfg gate choice** — used `#![cfg(not(feature = "state-inmem"))]` instead of the plan's literal `#![cfg(feature = "server")]`. Rationale in top-level decisions block above: `server` is the default feature, so cfg(feature="server") is a no-op on default build. Phase 55 tests used `cfg(not(state-inmem))` as the real gate (fjall-only codepath). No grep on the plan's cfg string was specified in acceptance criteria, so this is a purely corrective deviation.

None of the 4 deviations change wave assignments, flip counts, or the REQUIREMENTS.md patch. Each adds a corollary test that the phase would have to write at the GREEN wave anyway.

## Auth Gates Encountered

None — Wave 0 is tests + docs only, no wire surface or external auth.

## Next Wave Handoff (Wave 1 must deliver)

Wave 1 (plan 56-01) MUST add:

1. **`ShardOp::ReadEntityAt { target_shard, table_name, key, reply: oneshot::Sender<Option<EntityState>> }`** in `src/shard/thread.rs`. Mirror the pattern from Phase 55's `UpsertTableRow` scatter-gather: source shard `try_send`s + blocks on `crossbeam::bounded(1)` oneshot. On target-inbox-full, return `BeavaError::ShardOverload`.

2. **`ShardOp::ReadEntityBatch { target_shard, table_name, keys: Vec<String>, reply: oneshot::Sender<Vec<Option<EntityState>>> }`** — per-target coalesced batch variant. Same invariants as single-key but amortizes inbox hops.

3. **`ShardOp::SsjInsert { join_id, side: JoinSide, join_key, event, within_ms, reply: oneshot::Sender<Vec<Map<String,Value>>> }`** — the cross-shard join-buffer write primitive. Target shard evaluates the match inline and returns any matched joined rows via oneshot; source shard emits the matched output through the existing (Phase 55) cascade path.

4. **Metric plumbing (counters only, emitters wire at Waves 2-3):**
    - `beava_enrich_cross_shard_total{table}`
    - `beava_enrich_intra_shard_total{table}`
    - `beava_enrich_missing_total{table}`
    - `beava_ssj_cross_shard_total{join_id}`
    - `beava_crossshard_joins_registered_total{join_id}`

Wave 2 (plan 56-02) consumes `ReadEntityAt` / `ReadEntityBatch` in `EnrichFromTable` operator path. Wave 3 (plan 56-03) consumes `SsjInsert` in `StreamStreamJoin` + relaxes `validate_shard_keys` + extends `/debug/warnings`. Wave 4 (plan 56-04) re-measures BASELINE_P99_MICROS and flips the perf-gate tests GREEN.

## Known Stubs

**Intentional — this is the RED contract file.** Every new Rust test function body contains `todo!("56-W{N}: ...")` at runtime; each is `#[ignore = "56-W{N}"]`'d so the default suite never exercises them. This is the Wave 0 RED-test idiom, mirrored from Phase 54/55.

The `mismatched_shard_enrich_or_join` proptest bodies today only assert routing-determinism invariants (pass trivially at all N). Wave 2/3 MUST extend the bodies to replay through N=1 and N=8 engines and byte-compare EnrichFromTable / StreamStreamJoin output. The `#[ignore = "56-W{N}"]` markers on those proptests are the contract.

## Threat Flags

None — plan touched only test code + pre-existing REQUIREMENTS.md rows. No new trust boundaries, no wire-format changes (wire-format lands in Waves 1-3). Per plan `<threat_model>`:
- T-56-00-01 (REQUIREMENTS.md tampering): accepted — planner-authored, grep-verifiable.
- T-56-00-02 (test fixtures leaking secrets): accepted — synthetic data only (`u_N`, `s_N`, `country="CH"`).
- T-56-00-03 (perf test unbounded): mitigated — SC-4 test caps at 10K events + 5 s wall-clock budget; asserts total elapsed < 5 s at flip-GREEN.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 | `97caab0` | `test(56-W0): add RED tests for SC-1..SC-5 + TPC-CORR-04 relaxation (TPC-CORR-08, TPC-CORR-09)` |

## Self-Check: PASSED

- [x] `tests/cross_shard_enrich_from_table.rs` exists (2 tests, 56-W2) — **FOUND**
- [x] `tests/cross_shard_stream_stream_join.rs` exists (2 tests, 56-W3) — **FOUND**
- [x] `tests/register_crossshard_join_warning.rs` exists (3 tests, 56-W3) — **FOUND**
- [x] `tests/crossshard_enrich_perf_smoke.rs` exists (2 tests, 56-W4) — **FOUND**
- [x] `tests/sharding_parity.rs` extended with `mismatched_shard_enrich_or_join` mod (2 proptests) — **FOUND**
- [x] `cargo build --release --tests` → exit 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 796 passed / 0 failed / 35 ignored — **VERIFIED (Phase 55 baseline preserved)**
- [x] 20 × `56-W[0-9]` markers across 5 files — **VERIFIED**
- [x] `grep -c "TPC-CORR-08" .planning/REQUIREMENTS.md` == 3 ≥ 2 — **VERIFIED**
- [x] `grep -c "TPC-CORR-09" .planning/REQUIREMENTS.md` == 2 ≥ 2 — **VERIFIED**
- [x] `grep -c "RELAXED in Phase 56" .planning/REQUIREMENTS.md` == 1 ≥ 1 — **VERIFIED**
- [x] `git diff --name-only HEAD~1 HEAD -- src/` empty — **VERIFIED (no src/** modified)**
- [x] Commit `97caab0` present in git log — **VERIFIED**
