# Phase 10 — Sketch operators — SUMMARY

**Status:** COMPLETE
**Branch:** `phase-10-sketches`
**Commit range:** `157630f..278e2ff` (40 commits)
**Test count:** 624 → 705 (+81 across phase)

## Operators shipped (5)

| Op | Algorithm | Memory bound | Snapshot tag(s) | Port credit |
|---|---|---|---|---|
| `count_distinct` | 3-mode hybrid: ExactArray (≤16) → HashSet (≤1024) → HLL p=12 | ≤128 B / ≤16 KB / ~5 KB | `v0_count_distinct_exact_array`, `v0_count_distinct_hash_set`, `v0_count_distinct_hll` | HLL ported from `main:src/engine/hll.rs` |
| `percentile` | 2-mode hybrid: Exact Vec (≤256) → UDDSketch α₀=0.01, max_buckets=2048 | ≤2 KB / ≤48 KB | `v0_percentile_exact`, `v0_percentile_uddsketch` | UDDSketch ported from `main:src/engine/uddsketch.rs` |
| `top_k` | 2-mode hybrid: BTreeMap exact (≤1024 distinct) → CMS (W=2048, D=4) + bounded TopKHeap | ≤32 KB / ~64 KB | `v0_top_k_exact`, `v0_top_k_hybrid` | CMS+heap ported from `main:src/engine/cms.rs`, **incl. Plan 22-04 O(log k) `AHashMap<TopKValue, usize>` heap-position side-index** |
| `bloom_member` | Standard bit-array Bloom filter w/ Kirsch-Mitzenmacher 7 hashes (capacity=1024 / fpr=0.01 default) | ~1.2 KB | none (struct-tagged) | greenfield |
| `entropy` | Shannon entropy bits (log₂) over a categorical histogram with cap-and-spill at 1024 distinct | ≤32 KB | none (struct-tagged) | greenfield |

## Plan-by-plan recap

| Plan | Status | Highlights |
|---|---|---|
| 10-01 | DONE | REQ AGG-SKETCH-03 algorithm-name fix (CMS+heap, not SpaceSaving) + FieldType::Json + greenfield Bloom + Entropy + RetractingRing port |
| 10-02 | DONE | HLL port (944 LOC, bias-correction tables verbatim) + CountDistinctState 3-mode hybrid with serde rename tags |
| 10-03 | DONE | UDDSketch port (411 LOC, decrement support) + PercentileState 2-mode hybrid |
| 10-04 | DONE | CMS+TopKHeap port (554 LOC, **Plan 22-04 O(log k) optimization included**) + TopKState 2-mode hybrid |
| 10-05 | DONE | All 5 sketch ops wired through AggKind/AggOp/agg_compile/agg_apply + WindowedOp + AggOpDescriptor.sketch_params; e2e HTTP smoke; bloom_member windowless rejection at register time |
| 10-06 | DONE | SC2 — sketch state survives snapshot+restart and WAL-only replay byte-equally; 5 cross-sketch bincode round-trip proptests |
| 10-07 | DONE | criterion microbench (20 benches), perf-row + throughput-row + VERIFICATION.md |

## Performance summary

- bloom_member query: 8.6 ns (O(1) bit probe)
- count_distinct HLL update: 23 ns; CMS update: ~5 ns/elem (1Mevt fold)
- percentile UDDSketch update: 111 ns
- top_k hybrid update: 260 ns
- end-to-end throughput (medium-with-sketches/HTTP): 982 EPS — within 1% of Phase 7.5 small baseline; macOS fsync-bound at ~1k EPS regardless of sketch CPU cost.

## v0 limitations (deferred to v0.1)

1. **bloom_member query placeholder**: `Value::Bool(true)` once non-empty (signal, not membership test). Full `bloom_member.test(value)` API needs GET-with-arg endpoint design.
2. **Windowed count_distinct/percentile/top_k**: query returns most-recently-active bucket's value. v0.1 should add HLL-merge / pairwise UDDSketch / TopK merge across buckets. Entropy DOES merge.
3. **HLL retraction limitation**: HLL is lossy on `decrement` — for windowed `count_distinct` we use the most-recent-bucket strategy rather than retraction. Documented.
4. **Custom HLL precision**: fixed at p=12. Plumbing custom `p` into `Hll::new(p)` is v0.1+.
5. **windowed_member op (windowed Bloom)**: deferred. AGG-SKETCH-04 explicitly windowless.
6. **Python SDK helpers** (`bv.count_distinct`, etc.): not added in Phase 10. JSON dispatch works via the existing op-name mechanism (verified by phase10_sketch_smoke.rs); a thin wrapper task can land in Phase 11.
7. **TCP push throughput row**: pending Phase 8 sibling.

## Gates (all green)

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --features beava-server/testing -- --test-threads=1` — 705 passed
- `cargo bench -p beava-core --bench phase10_sketches --no-run`

## REQ coverage

All 5 AGG-SKETCH-0[1-5] requirements covered (see `10-VERIFICATION.md` §REQ-ID coverage).
