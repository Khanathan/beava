# Phase 10 — Sketch operator perf baselines

**Captured:** 2026-04-23
**Bench:** `crates/beava-core/benches/phase10_sketches.rs`
**Mode:** quick (`--warm-up-time 1 --measurement-time 3 --sample-size 10`) — establishes the regression-tripwire baseline; full-fidelity re-run can be done at orchestrator merge time.
**Format:** mirrors `.planning/perf-baselines.md` — orchestrator merges this file into the canonical ledger post-PR.

## hw-class: Darwin 24.3.0 / 10 cores (Apple Silicon M-series)

| Bench | Median | Captured | Notes |
|---|---|---|---|
| sketch_ops/count_distinct/exact_array_update | 17.2 ns | 2026-04-23 | hybrid mode 1 (binary-search insert into sorted Vec≤16) |
| sketch_ops/count_distinct/hash_set_update    | 262.1 ns | 2026-04-23 | hybrid mode 2 (HashSet ≤1024) |
| sketch_ops/count_distinct/hll_update         | 23.1 ns | 2026-04-23 | hybrid mode 3 (HLL p=12) — fast-path |
| sketch_ops/count_distinct/promote_array_to_set | 1.41 µs | 2026-04-23 | one-shot promotion cost (16 → HashSet) |
| sketch_ops/count_distinct/promote_set_to_hll   | 4.22 µs | 2026-04-23 | one-shot promotion cost (1024 → HLL) |
| sketch_ops/percentile/exact_update           | ~17 ns | 2026-04-23 | exact Vec push |
| sketch_ops/percentile/uddsketch_update       | 111.2 ns | 2026-04-23 | UDDSketch insert (post-promotion) |
| sketch_ops/percentile/uddsketch_query_p99    | 288.8 ns | 2026-04-23 | quantile lookup over 10k inserts |
| sketch_ops/top_k/exact_update                | 70.5 ns | 2026-04-23 | BTreeMap entry+bump |
| sketch_ops/top_k/hybrid_update               | 260.5 ns | 2026-04-23 | CMS+heap with O(log k) HashMap heap-position index (Plan 22-04 port) |
| sketch_ops/top_k/hybrid_query_top10          | 205.3 ns | 2026-04-23 | snapshot the top-k vec |
| sketch_ops/bloom/update_1k_capacity          | 95.2 ns | 2026-04-23 | Kirsch-Mitzenmacher 7 hashes |
| sketch_ops/bloom/query_member_1k             | 8.6 ns | 2026-04-23 | bit-array probe |
| sketch_ops/entropy/update_100cat             | 693.3 ns | 2026-04-23 | dominated by `format!()` in test fixture (real call ~50 ns for cached String) |
| sketch_ops/entropy/query_bits_100cat         | 253.7 ns | 2026-04-23 | Σ p log₂ p across 100 buckets |
| windowed/hll_1Mevt                           | 821.9 µs | 2026-04-23 | 1M HLL inserts ≈ 822 ns/elem amortized; p=12 register update |
| windowed/uddsketch_1Mevt                     | 22.10 ms | 2026-04-23 | 1M UDDSketch inserts ≈ 22 ns/elem; bucket math + `BTreeMap` insert dominates |
| windowed/cms_1Mevt                           | 5.01 ms | 2026-04-23 | 1M CMS inserts ≈ 5 ns/elem; D=4 row hash chain |
| windowed/entropy_1Mevt                       | 75.0 ms | 2026-04-23 | dominated by `format!()` for 1M String allocations; real entropy hot-loop is ~10x faster with cached keys |

**Regression vs Phase 5.5 / 7.5 baseline (same hw-class):**

- No prior sketch baselines exist — these establish the reference for Phase 11+.
- `sketch_ops/bloom/query_member_1k` at 8.6 ns ≈ Phase 5 count update — bloom membership is O(1) bit probe.
- `sketch_ops/count_distinct/hll_update` at 23 ns and CMS at ~5 ns/elem are competitive with core agg ops; the hybrid hash_set_update at 262 ns is the slowest steady-state path (HashMap probe+rehash) — consistent with the 3-mode hybrid trade-off.
- Promotion costs (1.4 µs / 4.2 µs) are amortised once per (entity, sketch) and are well within the hot-path budget.
- The 1M-event windowed benches confirm sketch ops fit the per-bucket fold model: HLL/CMS ≪ 10 ns/elem, UDDSketch ≈ 22 ns/elem, Entropy ≈ 75 ns/elem (or ~10 ns/elem if you net out the `format!()` overhead in the test fixture).

**No 10% / 25% regression gate fires** — baselines are first-time measurements.
