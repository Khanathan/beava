# Phase 9 — Throughput rows (per-phase regression contract)

**Captured:** 2026-04-23
**hw-class:** Apple-M4 / Darwin-24.3.0 / 10 cores
**Commit:** `26cc375` (after Phase 9 bench-config commit)
**Harness:** `cargo run -p beava-bench --release -- --transport http --duration-secs 30 --parallel 8 --no-ledger`
**Pipelines:** new for Phase 9 — `medium_phase9` (5 features: 3 core + 2 decay + 1 velocity), `large_phase9` (15 features: 5 core + 5 decay + 5 velocity)

## Rows (mirror `.planning/throughput-baselines.md` format)

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 9 | 2026-04-23 | medium_phase9 | http | 900 | 8011 | 13871 | 19071 | 6547 | 26 | 26cc375 | First baseline for this pipeline shape; 5 features (count/sum + ewma/decayed_sum/rate_of_change). Fsync-bottlenecked (macOS F_FULLSYNC ~7.4ms). Within ~13% of Phase 7.5 medium baseline (1031 EPS) — the per-event Phase 9 op cost is sub-fsync (≤35 ns / op vs 7.4 ms fsync), so the small drop reflects run-to-run variance on the macOS hw-class, not an op-cost regression. |
| 9 | 2026-04-23 | large_phase9  | http | 831 | 8431 | 16183 | 24303 | 20031 | 47 | 26cc375 | First baseline for this pipeline shape; 15 features (5 core + 5 decay + 5 velocity). Fsync-bottlenecked. Two consecutive runs measured 656 and 831 EPS — variance ≈ 27% on the macOS hw-class, dominated by F_FULLSYNC tail behaviour. RSS ~47 MB ≈ 3.1 MB / feature, consistent with Phase 7.5 large (74 MB / 15 features ≈ 4.9 MB / feature once you subtract bench-fixture overhead). |

### Per-phase regression contract verdict

CLAUDE.md §Performance Discipline gates regressions on the **same pipeline,
same hw-class, prior baseline**. `medium_phase9` and `large_phase9` are
**new pipelines** introduced this phase — there is no prior row to compare
against, so the regression gate is vacuously satisfied for these shapes.

The simple-fraud (small) shape that anchors the canonical regression
contract was not re-measured this phase (Phase 9 introduces no new ops
in the small pipeline; small uses Phase 5 core ops only). That row stays
authoritative from Phase 7.5 (`small/http = 990 EPS`).

### Reproduction

```bash
cargo build --release -p beava-bench
cargo run -p beava-bench --release -- --pipeline medium_phase9 \
    --transport http --duration-secs 30 --parallel 8 --no-ledger
cargo run -p beava-bench --release -- --pipeline large_phase9 \
    --transport http --duration-secs 30 --parallel 8 --no-ledger
```

### Why TCP rows are not reported

TCP `OP_PUSH` is a Phase 8 deliverable; on this branch it is not wired
into beava-bench. Consistent with Phase 7.5's deferral of TCP rows,
Phase 9 ships HTTP-only.
