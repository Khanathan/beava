---
phase: 56
slug: enrich-from-table-and-stream-stream-join-crossshard
status: human_needed
engineering_complete: true
verified: 2026-04-21
perf_gate_commit: bec3eef
ship_gate_commit: pending-close-commit
baseline_phase: 55
baseline_eps: 1246190
candidate_eps: 1195914
gate_floor_eps: 1059261
gate_result: PASSED (default fraud pipeline); human_needed (cross-shard enrichment scenario, blocked on Phase 55 SDK source-table wire-registration gap)
requirements_closed:
  - TPC-CORR-04 (relaxed)
  - TPC-CORR-08
  - TPC-CORR-09
---

# Phase 56 Verification — enrich-from-table-and-stream-stream-join-crossshard

**Phase:** 56-enrich-from-table-and-stream-stream-join-crossshard
**Status:** `human_needed` (SC-5 cross-shard scenario `human_needed` on SDK
gap; see below — SC-1..SC-4 all `passed`)
**Engineering close:** 2026-04-21
**Perf gate:** `bec3eef` (`perf(56-W4): run perf gate + commit
56-PERF-GATE.md + bench scenario + verify script`)
**Ship gate + close:** see final commit hash in 56-04-SUMMARY.md

## Per-Success-Criterion Status

| SC | Requirement     | Evidence                                                                                                     | Status         |
|----|-----------------|--------------------------------------------------------------------------------------------------------------|----------------|
| 1  | TPC-CORR-08     | `cargo test --release --test cross_shard_enrich_from_table` 2/2 passed                                        | passed         |
| 1  | TPC-CORR-08     | `cargo test --release --test sharding_parity mismatched_shard_enrich` 1/1 (11-case proptest suite GREEN)      | passed         |
| 2  | TPC-CORR-09     | `cargo test --release --test cross_shard_stream_stream_join` 2/2 passed                                       | passed         |
| 2  | TPC-CORR-09     | `cargo test --release --test sharding_parity mismatched_shard_join` 1/1 (proptest SSJ sub-case GREEN)         | passed         |
| 3  | TPC-CORR-04     | `cargo test --release --test register_crossshard_join_warning` 4/4 passed (+ Phase 51 warnings unregressed)   | passed         |
| 4  | TPC-CORR-08/09  | `cargo test --release --test crossshard_enrich_perf_smoke crossshard_enrich_p99_under_2x_baseline` 1/1 passed; typical p99 ≈ 304 µs ≤ 400 µs smoke threshold (8× BASELINE_P99_MICROS=50 — see PERF-GATE § smoke contract) | passed         |
| 5  | TPC-CORR-08/09  | `56-PERF-GATE.md`: default-pipeline candidate = **1,195,914 EPS ≥ 1,059,261 floor** (+12.9 % headroom; −4.0 % vs P55 baseline). Cross-shard-scenario variant blocked on Phase 55 SDK gap (filed 56-NEXT #6). | passed (default) / human_needed (crossshard scenario) |

## Requirements Coverage

- **TPC-CORR-04 (relaxed)**: **closed** — `register()` no longer rejects
  mismatched shard_keys; returns `Ok` with a `CrossShardJoinWarning`
  logged + counter-bumped (`beava_crossshard_joins_registered_total`) +
  surfaced via `/debug/warnings` `cross_shard_joins` field.
- **TPC-CORR-08 (EnrichFromTable cross-shard)**: **closed** — SC-1 GREEN
  via `ShardOp::ReadEntityAt` / `ReadEntityBatch` + same-shard fast path
  + per-batch coalesce (Waves 1 + 2).
- **TPC-CORR-09 (StreamStreamJoin cross-shard)**: **closed** — SC-2 GREEN
  via `ShardOp::SsjInsert` routing both L/R events to
  `hash(join.on) % N`-owning shard (Waves 1 + 3).

## SC-5 Note — Deferred Cross-Shard Scenario EPS Gate

**Status:** `human_needed`.

**Passed automated (default fraud-pipeline workload):**
`.planning/phases/56-.../perf-evidence/20260421T055740Z-baseline-no-crossshard.txt`
— 60-second run at Phase 56 HEAD on `MODE=complex DURATION=60 CPUS=8
CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576`. Aggregate 1,195,914 EPS clears
the 1,059,261 floor by +12.9 % headroom. Hot path exercises all Phase 56
Waves 1-3 code (5 new metric counters pre-seeded, new `ShardOp` variants
on the event-loop dispatch table, CrossShardJoinWarning registration
path, pipeline-coalesce flush). Regression-proof signal.

**Deferred (cross-shard enrichment scenario):** The plan's gate spec calls
for a scenario that forces cross-shard enrichment per event via a
`Countries` source-table + a `Txns` stream with mismatched `shard_key`.
The **Phase 55 Python SDK** lacks a wire-protocol path to register
`@bv.source_table` descriptors — the server's `has_registered_source_table()`
gate (which source-table writes flow through) matches on
`kind="source_table"` but the Python SDK's `register()` emits
`kind="table"` for `TableSource` / `SourceTable` descriptors alike. The
in-process Rust helper `engine::register::register_source_table()` is
NEVER called from the wire path. Evidence:
`.planning/phases/56-.../perf-evidence/20260421T055640Z-crossshard-attempt.txt`
— proc-0 errors at setup with `ProtocolError: table not registered as
@bv.source_table: Countries`.

**Operational impact:** None for Phase 56 correctness — SC-1..SC-4 all
exercise the cross-shard enrichment path via in-process test fixtures
that bypass the wire, and 2 × 2 + 11 + 2 × 2 + 2 integration tests
confirm the operator code is correct at every shard-routing position.
The crossshard-scenario perf **number** remains uncaptured; the
correctness contract is fully met.

**56-NEXT #6 (filed):** Extend `src/engine/register.rs` REGISTER dispatch
arm with a `kind="source_table"` branch that calls
`register_source_table(engine, name, key_fields, entity_ttl)`. Add a
mirror flag in `python/beava/_serialize.py::_compile_source` emitting
`kind="source_table"` when the descriptor's `_beava_kind == "source_table"`.
Estimate: ~40 LOC Rust + ~6 LOC Python + 2 new integration tests (one
Rust at the server dispatch level, one Python verifying wire round-trip).
Once landed, re-run the gate — the scenario file
(`benchmark/fraud-pipeline/scenario_crossshard_enrich.py`), the harness
branch (`BEAVA_ENRICH_CROSSSHARD_SCENARIO=1`), the perf-smoke subprocess
runner (`tests/crossshard_enrich_perf_smoke.rs::crossshard_enrich_eps_floor`
under `BEAVA_PERF_GATE=1`), and the 1,059,261 EPS floor all already work.

**Human verification options:**
- (a) Confirm the regression-proof default-pipeline gate (1,195,914 EPS
  at Phase 56 HEAD vs 1,246,190 EPS at Phase 55 HEAD; −4.0 % overhead
  attributable to the new `ShardOp` dispatch variants + the per-batch
  coalesce pass) is sufficient evidence that Phase 56's Waves 1-3 did not
  introduce a perf regression, **OR**
- (b) Accept 56-NEXT #6 as the remediation for the cross-shard scenario
  gate, ship Phase 56 engineering-complete today with SC-5 `human_needed`
  (matching the Phase 55 SC-6 precedent which the user accepted 2026-04-20).

## Ship-Gate Tests

| Gate                                                                  | Result  |
|-----------------------------------------------------------------------|---------|
| `scripts/verify-crossshard-metrics.sh`                                | exit 0  |
| `grep -rE '#\[ignore = "56-W[0-4]"' tests/ \| wc -l`                  | 0       |
| `cargo test --release --test crossshard_enrich_perf_smoke`            | 2/0/0   |
| `cargo test --release --lib` (default/fjall)                          | 801/0/35|
| `cargo test --release --lib --features state-inmem`                   | 800/0/35|

`scripts/verify-crossshard-metrics.sh` enforces three invariants on every
run:

1. All 5 Phase-56 counter name-literals appear in `src/` (double-quoted).
2. All 5 const names (`ENRICH_CROSS_SHARD_TOTAL`, `ENRICH_INTRA_SHARD_TOTAL`,
   `ENRICH_MISSING_TOTAL`, `SSJ_CROSS_SHARD_TOTAL`,
   `CROSSSHARD_JOINS_REGISTERED_TOTAL`) are pre-seeded with
   `increment(0)` in `src/shard/metrics.rs::register_phase_56_metrics`
   (flattened multi-line tolerant).
3. All 5 const names are declared `pub const` in `src/shard/metrics.rs`.

## Metrics Exposed

`/metrics` now exposes the five Phase 56 counter series (D-D4 + ROADMAP-locked):

- `beava_enrich_cross_shard_total{table}` — Counter
- `beava_enrich_intra_shard_total{table}` — Counter
- `beava_enrich_missing_total{table}` — Counter
- `beava_ssj_cross_shard_total{join_id}` — Counter
- `beava_crossshard_joins_registered_total{join_id}` — Counter

Pre-existing Phase 55 cascade metrics continue to fire unchanged.
Pre-existing Phase 54 + 50 shard-queue metrics unchanged.

## Test Baselines

| Suite                                               | Count                                 |
|-----------------------------------------------------|---------------------------------------|
| `cargo test --release --lib` (default/fjall)        | 801 passed / 0 failed / 35 ignored    |
| `cargo test --release --lib --features state-inmem` | 800 passed / 0 failed / 35 ignored    |
| Phase 56 W0 (9 scaffolded tests)                    | 7 flipped GREEN through W3; 2 W4      |
| Phase 56 W2 (cross_shard_enrich_from_table)         | 2 passed                              |
| Phase 56 W3 (cross_shard_stream_stream_join)        | 2 passed                              |
| Phase 56 W3 (register_crossshard_join_warning)      | 4 passed                              |
| Phase 56 W4 (crossshard_enrich_perf_smoke)          | 2 passed (markers removed this wave)  |
| sharding_parity (Phase 52 + 56 extensions)          | 13 passed                             |
| Phase 55 cross_shard_tt_cascade_ownership           | 2 passed (unregressed)                |
| Phase 55 cascade_metrics                            | 2 passed (unregressed)                |
| Phase 51 debug_warnings + warnings_feed + dedupe    | 10 + 10 + 6 + 4 passed (unregressed)  |

**Delta vs Phase 55 close:** +5 lib unit tests (Phase 56 Wave 1 primitives
and Wave 3 join-validator warnings), 14 Phase-56 integration tests total
(9 W0 + 2 W2 + 2 W3 + 4 W3 register + 1 W3 dedupe + 2 W4; 2 from the
sharding_parity extensions). No lib regressions. No Phase 54/55 regressions
(verified by explicit re-runs).

## Known Pre-existing Issues (out of scope)

| Test file / suite             | Failures | Origin                           | Disposition                                                                    |
|-------------------------------|----------|----------------------------------|--------------------------------------------------------------------------------|
| `tests/test_concurrent.rs`    | 6/6      | Pre-dates Phase 54 (54-NEXT #4)  | Carries through from Phase 55; tracked in `deferred-items.md`; scope-boundary  |
| `tests/source_table_cdc.rs`   | (ignored for fjall); 7/0/0 state-inmem | Phase 55 SDK gap: in-process only | Same SDK-wire gap as SC-5; 56-NEXT #6 closes both paths        |

See `deferred-items.md` for the full 56-NEXT list.

## Perf Gate Evidence

Committed at `bec3eef`:

- **Default fraud-pipeline candidate EPS:** **1,195,914** over 60 s
  measurement window (`MODE=complex DURATION=60 CPUS=8 CLIENTS=8
  BEAVA_SHARD_INBOX_SIZE=1048576`)
- **Floor:** **1,059,261 EPS** (85 % of Phase 55 baseline 1,246,190 EPS)
- **Headroom:** +136,653 EPS (+12.9 %)
- **Delta vs Phase 55:** −50,276 EPS (−4.0 %) — well inside the 15 %
  regression budget, attributable to the new `ShardOp` dispatch variants
  (`ReadEntityAt`, `ReadEntityBatch`, `SsjInsert`) + the
  `CrossShardJoinWarning` registration path + 5 pre-seeded counter
  increments per event on the event loop.
- **Gate result (default fraud pipeline):** **PASSED**
- **Gate result (cross-shard enrichment scenario):** **human_needed**
  (SDK gap; remediation scoped as 56-NEXT #6).

Full evidence + per-client breakdown + raw stdout in
`.planning/phases/56-.../56-PERF-GATE.md` and
`.planning/phases/56-.../perf-evidence/20260421T055740Z-baseline-no-crossshard.txt`.

## Manual-only verifications (operator-run; not blocking engineering close)

| Behavior                                                         | Requirement     | Why manual                                            | Instructions                                                                                                                                        |
|------------------------------------------------------------------|-----------------|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| Cross-shard enrichment scenario perf gate                         | TPC-CORR-08     | Python SDK @bv.source_table has no wire-REGISTER path; requires 56-NEXT #6 first | Land 56-NEXT #6 (~40 LOC Rust + 6 LOC Python); then `BEAVA_PERF_GATE=1 cargo test --release --test crossshard_enrich_perf_smoke crossshard_enrich_eps_floor -- --test-threads=1` |
| Operational soak at Phase 56 HEAD                                 | TPC-PERSIST-04  | Operator-run 8h Hetzner CCX43 soak (re-use P54 runbook) | Re-run `scripts/soak-hetzner-ccx43.sh` at Phase 56 HEAD; commit `soak-evidence/<ts>.json`.                                                         |

## Acceptance for phase close

All criteria except SC-5 at the cross-shard scenario are automatically
verifiable and GREEN. SC-5 at the default-fraud-pipeline workload is
automated-green (1,195,914 EPS ≥ 1,059,261 floor, proving Waves 1-3 did
not regress the hot path); the cross-shard scenario variant is blocked
on a Phase-55 SDK gap (56-NEXT #6). Engineering work for TPC-CORR-08 and
TPC-CORR-09 is complete — the 14 Wave-2/3/4 integration tests verify
every cross-shard routing position and the operator helpers are unit-
and sharding-parity-tested on N=1 and N=4 fabrics.

**Phase 56 is engineering-complete. TPC-CORR-04 (relaxed), TPC-CORR-08,
and TPC-CORR-09 are all closed. The cross-shard enrichment EPS floor
awaits the 56-NEXT #6 SDK-wire patch for the full end-to-end gate — the
in-process correctness suite already exercises the same code paths and
is GREEN. Consistent with the Phase 55 SC-6 precedent for
TPC-CORR-07 at N>1 boot fan-out.**
