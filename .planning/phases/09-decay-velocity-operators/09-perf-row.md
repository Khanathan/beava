# Phase 9 — Criterion microbench rows (NOT canonical)

**Captured:** 2026-04-23
**hw-class:** Apple-M4 / Darwin-24.3.0 / 10 cores
**Bench:** `cargo bench -p beava-core --bench phase9_decay_velocity -- --quick`
**Purpose:** Per-phase regression tripwire (CLAUDE.md §Performance Discipline). The
canonical baseline ledger (`.planning/perf-baselines.md`) is updated by the
orchestrator after Phase 9 merges back.

## agg_op_p9/* — `AggOp::update` per-variant (one event per iteration)

| Bench | Median | Notes |
|---|---|---|
| agg_op_p9/ewma | 8.55 ns | EWMA with α = 1 - exp(-Δt·ln2/half_life) |
| agg_op_p9/ewvar | 9.60 ns | EW Welford-adapted variance |
| agg_op_p9/ewzscore | 10.08 ns | wraps EwVar; query-only z |
| agg_op_p9/decayedsum | 9.06 ns | Cormode forward decay |
| agg_op_p9/decayedcount | 5.80 ns | no field — fastest |
| agg_op_p9/twa | 8.24 ns | sum_v_dt + sum_dt + last_v + last_t |
| agg_op_p9/rateofchange | 8.40 ns | Δvalue / Δt |
| agg_op_p9/interarrivalstats | 15.57 ns | Welford on inter-arrival gaps |
| agg_op_p9/burstcount | 9.74 ns | 64-bucket sliding sub-window |
| agg_op_p9/deltafromprev | 6.35 ns | scalar diff |
| agg_op_p9/trend | 6.85 ns | online OLS accumulator (5 sums) |
| agg_op_p9/trendresidual | 13.22 ns | trend + last_value/t |
| agg_op_p9/outliercount | 32.49 ns | Welford + sigma-threshold check |
| agg_op_p9/valuechangecount | 9.89 ns | float-equality flip detection |
| agg_op_p9/zscore | 18.01 ns | Welford + last_value, sqrt at query |

All 15 ops complete `update()` in **<35 ns** at the 99th percentile of measured
medians, well within the Phase 5 baseline envelope (1.8–12.1 ns for simpler
core ops). No regression vs Phase 5 expectations — this is the first measurement
for these ops.

## Apply-loop bench

Deferred to Phase 10+ (apply_event_to_aggregations registry-coupling makes
end-to-end bench less surgical than per-op `update`; the per-op timings above
already saturate the regression-tripwire goal).

## Reproduction

```bash
cargo bench -p beava-core --bench phase9_decay_velocity -- --quick
```
