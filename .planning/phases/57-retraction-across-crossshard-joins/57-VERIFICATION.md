---
phase: 57
slug: retraction-across-crossshard-joins
status: passed
engineering_complete: true
verified: 2026-04-21
perf_gate_commit: 3a41f35
ship_gate_commit: pending-close-commit
baseline_phase: 56
baseline_eps: 1195914
candidate_eps: 1297293
gate_floor_eps: 1076322
gate_result: PASSED (+20.5% headroom over floor; +8.5% vs Phase 56 baseline)
requirements_closed:
  - TPC-CORR-10
---

# Phase 57 Verification — retraction-across-crossshard-joins

**Phase:** 57-retraction-across-crossshard-joins
**Status:** `passed` — all success criteria GREEN
**Engineering close:** 2026-04-21
**Perf gate:** `3a41f35` (`perf(57-W4): perf gate PASSED 1,297,293 EPS + verify-retraction-metrics.sh + retraction_perf_smoke.rs`)
**Ship gate + close:** see final commit hash in 57-04-SUMMARY.md
**Requirement closed:** TPC-CORR-10 (retractions flow through cross-shard joins and cascades end-to-end)

## Per-Success-Criterion Status

| SC | Description                                                    | Test / Evidence                                                                                                   | Status   |
|----|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|----------|
| SC-1 | Source-table DELETE retracts downstream EnrichFromTable outputs | `cargo test --release --test crossshard_source_table_delete_retraction` — 1/1 `source_table_delete_retracts_enriched_downstream` GREEN | passed   |
| SC-1 | Source-table DELETE retraction — N=1 ↔ N=8 parity              | `cargo test --release --test sharding_parity retraction_after_cascade_enrich_parity_n1_vs_n8` — 1/1 GREEN         | passed   |
| SC-2 | SSJ side tombstone retracts previously-joined outputs          | `cargo test --release --test crossshard_ssj_retraction` — 1/1 `ssj_tombstone_retracts_previously_joined_outputs` GREEN | passed |
| SC-2 | SSJ tombstone retraction — N=1 ↔ N=8 parity                   | `cargo test --release --test sharding_parity retraction_after_cascade_ssj_parity_n1_vs_n8` — 1/1 GREEN            | passed   |
| SC-3 | Late retraction beyond history_ttl warns + skips              | `cargo test --release --test late_retraction_warning` — 1/1 `late_retraction_beyond_history_is_skipped_and_warned` GREEN | passed |
| SC-4 | Perf ≤ 10 % overhead on write path (zero retractions firing)   | `57-PERF-GATE.md`: candidate = **1,297,293 EPS ≥ 1,076,322 floor** (+20.5 % headroom; +8.5 % vs Phase 56 baseline) | passed   |
| D-B5 | Retraction cascade depth guard at 16 hops                     | `cargo test --release --test retraction_depth_guard` — 1/1 `retraction_cascade_exceeds_16_hop_cap` GREEN          | passed   |

## TPC-CORR-10 Coverage Checklist

- [x] **D-A1..A4** — `contributing_inputs: Option<Box<ContribSet>>` field on `EntityState`; primary_event_id + source_table_keys + left/right_event_id variants per operator
- [x] **D-A5** — Backwards-compat for pre-Phase-57 rows (v9 snapshot): `#[serde(default)]` loads as `None` = "cannot-retract"; v10 schema bump
- [x] **D-B1** — `ShardOp::RetractDownstream { target_shard, stream_name, row_key, reason, reply }` variant + dispatch arm
- [x] **D-B2** — `RetractReason::{SourceTableDelete, EntityTombstone, PrimaryEventRetract}` enum
- [x] **D-B3** — `try_send` + `crossbeam::bounded(1)` oneshot + blocking `recv` + `BeavaError::ShardOverload` on Full
- [x] **D-B4** — Idempotency on target — `RetractOutcome::NoOp` when row already retracted or never existed
- [x] **D-B5** — Depth cap at 16 hops via `retraction_depth` counter in ShardOp + `BeavaError::RetractionDepthExceeded` + `beava_retraction_depth_exceeded_total` counter — unit-tested
- [x] **D-C1** — `history_ttl` live check inside `Shard::apply_retraction`; emits `RetractOutcome::BeyondHistory` + `tracing::warn!` + `beava_retraction_beyond_history_total{operator}`
- [x] **D-C2** — `StreamDefinition.history_ttl` (existing) is the source of truth; no new knob added
- [x] **D-C3** — 60 s warning dedupe keyed on `(operator, reason_class)` — mirrors Phase 51/56 `cross_shard_joins` pattern; surfaced via `/debug/warnings.retraction_beyond_history`
- [x] **D-D1** — 4 RED tests + `sharding_parity` extension — all GREEN as of Wave 3 close
- [x] **D-D2** — 5 correctness metrics pre-seeded + emitted (see Ship-Gate Tests below)
- [x] **D-D3** — Perf gate ≥ 1,076,322 EPS floor — PASSED at 1,297,293 EPS
- [x] **D-D4** — Advisory retraction-firing micro-bench — DEFERRED (same SDK gap as Phase 56 SC-5; see 57-PERF-GATE.md §Advisory); NOT a gate per plan

## Test Counts (cargo test --release)

| Suite                                                         | Count                                 |
|---------------------------------------------------------------|---------------------------------------|
| `cargo test --release --lib` (default / fjall)                | 809 passed / 0 failed / 35 ignored    |
| `cargo test --release --lib --features state-inmem`           | 801 passed / 0 failed / 35 ignored    |
| `crossshard_source_table_delete_retraction` (SC-1)            | 1 passed                              |
| `crossshard_ssj_retraction` (SC-2)                            | 1 passed                              |
| `late_retraction_warning` (SC-3)                              | 1 passed                              |
| `retraction_depth_guard` (D-B5)                               | 1 passed                              |
| `sharding_parity` (incl. 2 retraction_after_cascade subcases) | 15 passed (+2 vs Phase 56 close)      |
| `cross_shard_enrich_from_table` (Phase 56 SC-1 unregressed)   | 2 passed                              |
| `cross_shard_stream_stream_join` (Phase 56 SC-2 unregressed)  | 2 passed                              |
| `register_crossshard_join_warning` (Phase 56 SC-3 unregressed)| 4 passed                              |
| `cross_shard_tt_cascade_ownership` (Phase 55 unregressed)     | 2 passed                              |
| `cascade_metrics` (Phase 55 unregressed)                      | 2 passed                              |
| `test_debug_warnings_endpoint` (Phase 51 unregressed)         | 10 passed                             |
| `test_warnings_feed` (Phase 51 unregressed)                   | 6 passed                              |
| `test_warnings_dedupe` (Phase 51 unregressed)                 | 10 passed                             |

## Ship-Gate Tests

| Gate                                                                 | Result   |
|----------------------------------------------------------------------|----------|
| `scripts/verify-retraction-metrics.sh`                               | exit 0   |
| `grep -rE '#\[ignore = "57-W[0-4]"' tests/ \| wc -l`                 | 0        |
| `cargo test --release --test retraction_perf_smoke` (gate-off)       | 0/0/1 (ignored; BEAVA_PERF_GATE unset) |
| `cargo test --release --lib`                                         | 809/0/35 |
| `cargo test --release --lib --features state-inmem`                  | 801/0/35 |

`scripts/verify-retraction-metrics.sh` enforces three invariants on every run:

1. All 5 Phase-57 retraction counter name-literals appear in `src/` (double-quoted).
2. All 5 const names (`RETRACTIONS_SENT_TOTAL`, `RETRACTIONS_APPLIED_TOTAL`,
   `RETRACTIONS_NOOPED_TOTAL`, `RETRACTION_BEYOND_HISTORY_TOTAL`,
   `RETRACTION_DEPTH_EXCEEDED_TOTAL`) are pre-seeded with `.increment(0)`
   in `src/shard/metrics.rs` bootstrap (flattened multi-line tolerant).
3. All 5 const names are declared `pub const` in `src/shard/metrics.rs`.

## Metrics Exposed

`/metrics` now exposes the five Phase 57 counter series (D-D2 + ROADMAP-locked):

- `beava_retractions_sent_total{operator, reason}` — per ShardOp::RetractDownstream send
- `beava_retractions_applied_total{operator}` — per successful target-side retraction apply
- `beava_retractions_nooped_total{operator}` — idempotent no-op count
- `beava_retraction_beyond_history_total{operator}` — SC-3 surface (NOT deduped at metric level; only the warning is)
- `beava_retraction_depth_exceeded_total` — D-B5 guard trip

Pre-existing Phase 55/56 cascade + cross-shard metrics continue to fire unchanged.

## Known Pre-existing Issues (out of scope — carried forward from Phase 55/56)

| Test file / suite             | Failures | Origin                                  | Disposition                                                           |
|-------------------------------|----------|-----------------------------------------|-----------------------------------------------------------------------|
| `tests/test_concurrent.rs`    | 6/6      | Pre-dates Phase 54 (54-NEXT #4)          | Still present; scope-boundary; filed in Phase 54 deferred list       |
| `tests/source_table_cdc.rs`   | ignored (fjall); 7/0/0 on state-inmem | Phase 55 SDK gap — in-process Rust API only | Same SDK gap as Phase 56 SC-5; 56-NEXT #6 closes this path + Phase 57 D-D4 advisory |

See `deferred-items.md` for the Phase 57 57-NEXT list (carry-forwards included).

## Perf Gate Evidence

Committed at `3a41f35`:

- **Default fraud-pipeline candidate EPS:** **1,297,293** over 60 s measurement
  window (`MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576`)
- **Floor:** **1,076,322 EPS** (90 % of Phase 56 baseline 1,195,914 EPS;
  user-locked non-negotiable per 57-04-PLAN `<user_decision_fidelity>`)
- **Headroom:** +220,971 EPS (+20.5 %)
- **Delta vs Phase 56 baseline:** +101,379 EPS (**+8.5 %** — within run-to-run
  noise; Phase 57 did not regress the write path)
- **Delta vs Phase 55 baseline:** +51,103 EPS (+4.1 %)
- **Gate result:** **PASSED**
- **Contingency invoked:** NONE (C1/C2/C3 all unused)

Full evidence + per-client breakdown + raw stdout in
`.planning/phases/57-.../57-PERF-GATE.md` and
`.planning/phases/57-.../perf-evidence/20260421T080934Z.txt`.

## Advisory D-D4 Retraction-Firing Micro-bench — Deferred

**Status:** blocked on Phase 56 SC-5 / Phase 55 SDK gap (56-NEXT #6).

The D-D4 scenario (1000 paired push + source-table DELETE events, recording
end-to-end retraction latency p50/p99) requires the Python SDK to wire-register
`@bv.source_table` descriptors. That path does not yet exist (56-NEXT #6:
`kind="source_table"` REGISTER dispatch, ~40 LOC Rust + 6 LOC Python). The
plan `<objective>` C provides the explicit off-ramp ("document 'blocked on
same SDK wire-register gap as P56' and skip"), and D-D4 is **advisory only,
NOT a gate**. Correctness of the retraction-firing path is proven by the
4 Wave-0/1/2/3 integration tests (all GREEN). Phase 57 does not require this
number to close.

## Manual-only verifications (operator-run; not blocking engineering close)

| Behavior                                                         | Requirement     | Why manual                                            | Instructions                                                                                                                                        |
|------------------------------------------------------------------|-----------------|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| D-D4 retraction-firing micro-bench latency                       | TPC-CORR-10     | Python SDK @bv.source_table has no wire-REGISTER path (56-NEXT #6) | Land 56-NEXT #6 (~40 LOC Rust + 6 LOC Python); then re-run advisory burst harness from 57-PERF-GATE.md §Advisory |
| Operational soak at Phase 57 HEAD                                 | TPC-PERSIST-04  | Operator-run 8h Hetzner CCX43 soak (re-use P54 runbook) | Re-run `scripts/soak-hetzner-ccx43.sh` at Phase 57 HEAD; commit `soak-evidence/<ts>.json`.                                                         |

## Acceptance for phase close

All success criteria SC-1..SC-4 and D-B5 depth guard are automatically
verifiable and GREEN. The perf gate PASSED with 20.5 % headroom. The
advisory D-D4 micro-bench is explicitly optional per plan. Phase 55/56
regression battery unchanged. All 57-W{0..4} markers removed from tests/.

**Phase 57 is engineering-complete. TPC-CORR-10 is closed. The
retraction correctness leg of v1.2 is delivered — retractions flow
through cross-shard joins (SSJ on hash(join.on) % N), cross-shard
cascades (EnrichFromTable driven by source-table DELETE), and primary
stream tombstones, with the 16-hop depth guard preventing accidental
fan-out explosions and the 60 s dedupe'd /debug/warnings surface
absorbing beyond-history cases.**
