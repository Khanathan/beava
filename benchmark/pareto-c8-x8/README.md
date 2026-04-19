# pareto-c8-x8 Benchmark Results

<!-- Results committed after Task 2 ship-gate run -->

## Cell Spec

| Parameter | Value |
|-----------|-------|
| Streams | 8 |
| Event multiplier | 8x |
| Key distribution | Zipf s=1.0 (Pareto 80/20) |
| Key space | 10,000 distinct keys |
| Shard count | 8 (N=CPU_COUNT for ship-gate run) |
| Ship-gate | cross_shard_fraction < 0.40 (TPC-PERF-07) |

## How to Run

```bash
# Criterion benchmark (reports EPS, cross_shard_fraction, ship-gate assertion):
cargo bench -p beava --bench pareto_workload -- --nocapture

# Run unit tests only (Zipf sampler correctness, determinism):
cargo bench -p beava --bench pareto_workload -- --test

# Full ship-gate matrix (all 9 cells + pareto cell):
cargo bench -p beava -- --nocapture 2>&1 | tee /tmp/bench-52-08.txt
cargo bench -p beava --bench pareto_workload -- --nocapture 2>&1 | tee -a /tmp/bench-52-08.txt
```

## Ship-Gate Results

> **Pending human-run verification (Task 2 checkpoint).**
> Update this section with actual numbers from the ship-gate run before approving.

| Criterion | Gate | Measured | Status |
|-----------|------|----------|--------|
| N=1 throughput regression | within -5% of Phase 48 baseline | — | pending |
| complex-c8-x8 at N=CPU_COUNT | >= 3x vs N=1 baseline | — | pending |
| pareto-c8-x8 cross_shard_fraction | < 0.40 | — | pending |

## Architecture Notes

The `pareto-c8-x8` cell validates that hot-key Zipf workloads do not cause
cross-shard fan-out in the routing layer. For single-key-field streams (`user_id`),
each PUSH event routes to exactly one shard regardless of key distribution:

- **Uniform distribution**: each key hashes uniformly across 8 shards
- **Zipf s=1.0**: top 20% of keys receive ~80% of traffic, all landing on their
  home shard (no cross-shard spillover)

`cross_shard_fraction = 0.0` for single-key workloads — this is the architectural
invariant asserted by the ship-gate. The assertion is in code (`benches/pareto_workload.rs`)
and will panic (fail CI) if the routing layer introduces multi-key fan-out.

Cross-shard fraction is non-zero only for **multi-key-field pipelines** (e.g., the
fraud pipeline with `user_id + merchant_id + device_id + ip_address` cascade keys).
Those are tracked separately; the Pareto gate specifically validates single-key routing.
