# Phase 50 Multi-Shard Routing Benchmark Results

## Ship-Gate Criteria

| Criterion | Target | Status |
|-----------|--------|--------|
| complex-c8-x8 at N=CPU_COUNT | >= 918,621 EPS (3× Phase 49 baseline of 306,207 EPS) | PENDING human-verify |
| shard_probe cross_shard_fraction | < 0.40 | PENDING human-verify |
| N=1 regression (complex-c8-x8) | >= 290,897 EPS (within -5% of Phase 49) | PENDING human-verify |
| All 9 metric series in /metrics | present with non-zero values | AUTOMATED (test_metrics_parity passes) |

## How to Run

### N=CPU_COUNT ship-gate benchmark

```bash
cargo build --release

BEAVA_SHARDS=auto DURATION=30 bash benchmark/fraud-pipeline/run_matrix.sh
```

`BEAVA_SHARDS=auto` resolves to `$(nproc)` on Linux or `$(sysctl -n hw.physicalcpu)` on macOS.

### N=1 regression baseline

```bash
BEAVA_SHARDS=1 DURATION=30 bash benchmark/fraud-pipeline/run_matrix.sh
```

### Verify cross_shard_fraction gate

After the benchmark completes (server still running):

```bash
curl http://localhost:${BEAVA_PORT}/metrics | grep -E "cross_shard|beava_shard_events_total"
```

Or check the `/debug/shard_probe` endpoint for the full routing distribution.

## 9-Cell Matrix Results

> Replace the placeholder rows below with actual results from your run.
> Record: N=CPU_COUNT, host, date, commit hash.

**Run info:** PENDING (human-verify checkpoint not yet completed)

| Cell | Phase 49 Baseline (EPS) | N=1 Result (EPS) | N=CPU_COUNT Result (EPS) | 3x Gate Pass? |
|------|------------------------|-------------------|--------------------------|---------------|
| simple-c1-x1 | TBD | PENDING | PENDING | — |
| simple-c4-x4 | TBD | PENDING | PENDING | — |
| simple-c8-x8 | TBD | PENDING | PENDING | — |
| simple-c1-x4 | TBD | PENDING | PENDING | — |
| simple-c4-x1 | TBD | PENDING | PENDING | — |
| simple-c4-x8 | TBD | PENDING | PENDING | — |
| complex-c1-x1 | TBD | PENDING | PENDING | — |
| complex-c4-x4 | TBD | PENDING | PENDING | — |
| **complex-c8-x8** | **306,207** | **PENDING** | **PENDING** | **PENDING** |

## Phase 50 Implementation Summary

| Plan | Description | Status |
|------|-------------|--------|
| 50-01 | Prometheus recorder + /metrics parallel emit | DONE |
| 50-02 | Per-shard metrics (9 series, D-07) | DONE |
| 50-03 | Shard thread lifecycle (D-01/D-02/D-14) | DONE |
| 50-04 | SPSC routing + SO_REUSEPORT (D-08/D-09) | DONE |
| 50-05 | SO_REUSEPORT per-shard sockets | DONE |
| 50-06 | shard_key missing-field reject + warnings (D-10/D-11/D-12/D-13) | DONE |
| 50-07 | Gauge emission + routing counters + N=2 test | DONE |
| 50-08 | Benchmark ship-gate + metrics parity | Task 1 DONE; human-verify PENDING |

## Notes

- `BEAVA_SHARDS` env var controls thread count (default: 1, preserves Phase 49 behavior)
- All N=1 cells should be within -5% of Phase 49 baseline (migration-compat gate)
- `shard_probe cross_shard_fraction` is computed from `record_routed_event` counters in the ingest path
- Metrics parity test (`cargo test --test test_metrics_parity`) verifies all 9 D-07 series appear after events
