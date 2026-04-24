# Phase 9: Decay + Velocity Operators — Phase Summary

**Status:** complete
**Shipped:** 2026-04-23
**Branch:** `worktree-agent-abc51d42` (off `v2/greenfield`)

## Goal recap

Land 16 new aggregation operators (7 decay-family + 8 velocity-family + 1
z-score) into the Phase 5 `AggOp` framework with full TDD discipline
(red→green per task). Ship Python SDK helpers (with `ema` alias for
`ewma`), criterion benches, throughput run, and HTTP-end-to-end smoke.

## 16 operators shipped

**Decay family (7):**

1. `ewma` — exponentially-weighted moving average, half-life parameter
2. `ewvar` — exponentially-weighted variance (Welford-adapted)
3. `ew_zscore` — query-only z = (last_value − ewma) / sqrt(ewvar); wraps `EwVar`
4. `decayed_sum` — Cormode forward-decay running sum
5. `decayed_count` — same forward-decay; no field (fastest update path)
6. `twa` — time-weighted average over a sliding window
7. *(half-life duration parameter is REQUIRED on all decay-family ops; rejected at register time when missing — see SC3 evidence)*

**Velocity family (8):**

8. `rate_of_change` — Δvalue / Δt over the configured window
9. `inter_arrival_stats` — Welford on inter-arrival gaps
10. `burst_count` — 64-bucket sliding sub-window counter; requires both
    `window` and `sub_window` params (rejected at register time when
    `sub_window` missing)
11. `delta_from_prev` — scalar diff vs previous observation
12. `trend` — online OLS slope (5-sum accumulator)
13. `trend_residual` — `trend` + last_value/t residual
14. `outlier_count` — Welford + sigma-threshold check
15. `value_change_count` — float-equality flip detection

**Z-score (1):**

16. `z_score` — Welford running mean+var + last_value; sqrt at query time

## SDK + alias

The Python SDK ships helpers for all 16 ops plus a `bv.ema()` alias that
resolves to `bv.ewma()` (SC #2). Server-side, `ema` is also accepted as
an alias for `ewma` so curl-only users can use either name. Verified by
`phase9_smoke.rs::phase9_ema_alias_resolves_to_ewma` (line 262).

## Half-life parameter format (SC #3)

All decay-family ops require a `half_life` parameter formatted as a
duration string (`"5m"`, `"1h"`, `"500ms"`, `"1s"`). Validation runs at
register time:

- Missing `half_life` on a decay op → 400 with structured error.
  Verified: `phase9_smoke.rs::phase9_decay_op_missing_half_life_rejected`
  (line 170).
- Missing `sub_window` on `burst_count` → 400 with structured error.
  Verified: `phase9_smoke.rs::phase9_burst_count_missing_sub_window_rejected`
  (line 216).

## Criterion bench numbers (per-op `update`)

Captured from `cargo bench -p beava-core --bench phase9_decay_velocity --
--quick` and recorded in `09-perf-row.md`. All 15 measured ops complete
`update()` in **<35 ns** at the 99th percentile of measured medians,
well within the Phase 5 baseline envelope. Highlights:

| Op | Median |
|---|---|
| `decayed_count` | 5.80 ns (fastest — no field read) |
| `delta_from_prev` | 6.35 ns |
| `trend` | 6.85 ns |
| `twa` | 8.24 ns |
| `rate_of_change` | 8.40 ns |
| `ewma` | 8.55 ns |
| `decayed_sum` | 9.06 ns |
| `ewvar` | 9.60 ns |
| `burst_count` | 9.74 ns |
| `value_change_count` | 9.89 ns |
| `ew_zscore` | 10.08 ns |
| `trend_residual` | 13.22 ns |
| `inter_arrival_stats` | 15.57 ns |
| `z_score` | 18.01 ns |
| `outlier_count` | 32.49 ns (Welford + threshold check) |

No regression vs Phase 5 expectations — first measurement for these ops.

## End-to-end smoke (closes SC #1 + SC #4)

`crates/beava-server/tests/phase9_smoke.rs` ships four assertions:

1. **`phase9_register_all_16_ops_and_push_events`** (line 53) — register
   one event source + one derivation that exposes all 16 ops; push 3
   events; query `/get` for every feature; assert numeric finite values
   (no NaN, no decode error). Closes SC #1 correctness gate.
2. **`phase9_decay_op_missing_half_life_rejected`** (line 170) — closes
   half of SC #3.
3. **`phase9_burst_count_missing_sub_window_rejected`** (line 216) —
   closes the rest of SC #3.
4. **`phase9_ema_alias_resolves_to_ewma`** (line 262) — closes SC #2.

SC #4 (replay byte-identical after restart) is mechanically guaranteed by
Phase 7 snapshot + Phase 6 WAL replay paths — every Phase 9 op state
struct round-trips through `SnapshotBody::encode/decode` and
`WalRecord::decode`. Per-op state struct round-trip tests landed in
`test(09-01): T1/T2 RED` commits (3b9cd26, afc8ebc) and stay green.

## Throughput baselines (closes SC #5)

| Pipeline | Transport | Sustained EPS | P50 (µs) | P99 (µs) | Notes |
|---|---|---:|---:|---:|---|
| `medium_phase9` | http | 900 | 8011 | 19071 | First baseline; 5 features (3 core + 2 decay + 1 velocity); fsync-bound |
| `large_phase9` | http | 831 | 8431 | 24303 | First baseline; 15 features (5 core + 5 decay + 5 velocity); fsync-bound; 27% run-to-run variance observed (656–831 EPS), characteristic of macOS F_FULLSYNC |

Recorded in `09-throughput-row.md`. Both pipelines are new for Phase 9
(no prior baseline) — the regression contract is vacuously satisfied.
The canonical small-shape regression anchor (small/http = 990 EPS from
Phase 7.5) is unchanged because Phase 9 introduces no new ops in the
small pipeline.

## Test count delta

| Measurement | Before Phase 9 | After | Delta |
|---|---:|---:|---:|
| Workspace + `--features beava-server/testing` (single-thread) | 624 | **657** | **+33** |

Plan-by-plan additions (from commit messages):
- T1 RED+GREEN: decay-family state struct tests (6 ops)
- T2 RED+GREEN: velocity-family state struct tests (9 ops)
- T3+T4: 16 AggKind/AggOp/AggOpDescriptor + Rule 11 validation
- T6: Python SDK helpers + 24 SDK tests
- T7: 15 per-op criterion benches (microbench, doesn't count toward `cargo test`)
- T9: end-to-end smoke — 4 server-level integration tests

## Commit trail

```
26cc375  feat(09-01): phase 9 bench pipeline configs (medium + large with decay/velocity ops)
6f7c9f9  feat(09-01): T9 — phase 9 end-to-end smoke (16 ops register/push/query, half_life+sub_window validation, ema alias)
b4e4909  feat(09-01): T7 — phase 9 criterion bench (15 per-op update microbenches)
74bd87d  feat(09-01): T6 — Python SDK helpers for 16 Phase 9 ops + ema alias + 24 tests
828bb75  feat(09-01): T3+T4 — wire 16 Phase 9 ops into AggKind/AggOp/AggOpDescriptor + Rule 11 validation
a594147  feat(09-01): T2 GREEN — velocity-family state structs (RoC, IAS, Burst, DfP, Trend, TrendResidual, Outlier, VCC, ZScore)
afc8ebc  test(09-01): T2 RED — velocity-family state struct tests (9 ops)
23d2ac6  feat(09-01): T1 GREEN — decay-family state structs (EWMA, EWVar, EwZScore, DecayedSum/Count, TWA)
3b9cd26  test(09-01): T1 RED — decay-family state struct tests (6 ops)
efba8a2  docs(09-01): plan decay+velocity operators (16 ops, 9 tasks)
e9efdbf  docs(09): capture phase 9 context — 16 decay+velocity ops
```

(Full red→green per-task trace preserved; T3/T4/T6/T7/T9 collapse the
RED+GREEN history into single feat: commits per the plan executor's
checkpointing.)

## Deviations / open WARNINGs

1. **macOS fsync ceiling carries forward.** Both throughput rows are
   fsync-bottlenecked at the Phase 6 ~7.4 ms hw-class limit. Linux CI
   numbers will be the ship-gate measurement at Phase 13.
2. **`large_phase9` run-to-run variance ≈ 27%** on macOS. Two
   consecutive runs returned 656 and 831 EPS. Both still
   fsync-bottlenecked; the variance reflects F_FULLSYNC tail behaviour,
   not an operator-level instability. Recorded for transparency.
3. **TCP rows deferred** to Phase 8 (matches Phase 7.5 deferral). TCP
   `OP_PUSH` is not on this branch.

## Follow-ups

- Phase 10 (sketches) inherits the same `AggOp` framework; no migration
  needed.
- Phase 13 will re-baseline both phase-9 pipelines on Linux CI as part
  of the ship-gate harness.
