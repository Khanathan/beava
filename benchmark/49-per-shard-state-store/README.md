# Benchmark: Phase 49 per-shard-state-store (N=1 migration-compat gate)

**Committed:** 2026-04-18
**Branch:** arch/tpc-full-shard
**Runner:** dev machine (macOS 15.3.2, Apple M4 10-core, 32 GB — same ref box as LAUNCH-VERIFY.md)
**Ship-gate:** `complex-c8-x8` at N=1 within −5% of committed v1.0-launch baseline (314,931 EPS)

## Context

Phase 49 (per-shard-state-store) introduced the `Shard` struct, `ShardedStateStoreV1`,
and per-shard `WatermarkState` with a shadow write in the push path at N=1. This
benchmark matrix verifies that those changes add no observable overhead relative to the
pre-Phase-49 code path (migration-compat gate per 49-CONTEXT.md D-04/D-05).

The only cell with a committed single-config baseline is `complex-c8-x8` (the v1.0-launch
baseline from `benchmark/fraud-pipeline/results/baseline/summary.json`). The other 8 cells
have no prior committed numbers — they are informational.

## Run Configuration

```bash
cd benchmark/fraud-pipeline
DURATION=30 bash run_matrix.sh
```

- `DURATION=30` (30s per cell; baseline was 60s — shorter run acceptable per Phase 48 pattern)
- 5s warmup discarded
- N=1 (BEAVA_SHARDS not set; server defaults to 1 shard)
- Matrix results dir: `benchmark/fraud-pipeline/results/matrix-20260418-104746/`

## 9-Cell Matrix Results (N=1, dev machine, 30s run)

| Cell | Mode | Workers | Clients | EPS | vs Baseline* | Gate |
|------|------|---------|---------|-----|-------------|------|
| simple-c1-x1 | simple | 1 | 1 | 503,600 | +59.9% | N/A |
| simple-c4-x4 | simple | 4 | 4 | 1,066,968 | +238.8% | N/A |
| simple-c8-x8 | simple | 8 | 8 | 1,132,870 | +259.7% | N/A |
| simple-c1-x4 | simple | 1 | 4 | 550,775 | +74.9% | N/A |
| simple-c4-x1 | simple | 4 | 1 | 501,710 | +59.3% | N/A |
| simple-c4-x8 | simple | 4 | 8 | 925,902 | +194.0% | N/A |
| complex-c1-x1 | complex | 1 | 1 | 109,494 | -65.2% | N/A† |
| complex-c4-x4 | complex | 4 | 4 | 317,150 | +0.70% | N/A |
| **complex-c8-x8** | complex | 8 | 8 | **306,207** | **-2.77%** | **PASS** |

\* vs baseline: compared to committed v1.0-launch baseline (complex-c8-x8, 314,931 EPS).
  Simple-mode cells are expected to far exceed the complex baseline — the baseline was
  committed for the complex-c8-x8 config only.

† `complex-c1-x1` at 109,494 EPS is -65% vs the complex-c8-x8 baseline because it uses
  1 worker and 1 client — naturally 8× lower. `compare_baseline.sh` flags this as a
  regression, which is a known tooling limitation (per LAUNCH-VERIFY.md "9-Cell Matrix"
  section). This is NOT a Phase 49 regression — no per-cell `complex-c1-x1` baseline
  exists to compare against.

## Ship-Gate Verdict

**PASS.** `complex-c8-x8` at N=1 delivers **306,207 EPS**, delta = **-2.77%** vs the
committed v1.0-launch baseline (314,931 EPS). Well within the −5% migration-compat gate.

The Phase 49 shadow write (Shard-0 state + dirty_set + WatermarkState.observe() on every
push) adds negligible overhead at N=1. The DashMap + ArcSwap compat shim remains the
authoritative read path; the shadow write cost is dominated by the existing StateStore write.

## Regression Policy

- Migration-compat gate: `complex-c8-x8` at N=1 within −5% of v1.0-launch baseline.
  **This gate is now CLOSED for Phase 49.**
- Architecture gate: ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT — Phase 50 ship-gate.
- `shard_probe` cross_shard_fraction <40% — Phase 50 + 52 gate.
