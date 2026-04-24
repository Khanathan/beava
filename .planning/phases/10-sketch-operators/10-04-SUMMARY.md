# Plan 10-04 Summary — CMS + TopKHeap + TopKState hybrid

**Status:** complete (impl + tests + microbench).

## What landed

- `crates/beava-core/src/sketches/cms.rs` (~360 LOC) — full port of `main:src/engine/cms.rs`:
  - `CountMinSketch` with W=2048, D=4, signed-i64 counters that saturate at 0 on decrement.
  - `TopKValue` enum (Str/Int/Float-via-OrderedFloat/Bool) with `from_json`/`to_json`/`hash64`.
  - `TopKHeap` with **Plan 22-04 O(log k) optimization** (`AHashMap<TopKValue, usize>` heap-position side-index) — verbatim port from main per the audit. Insert path: O(1) HashMap lookup + O(log k) sift; index updated in-step with every swap during sift-up / sift-down. The index is `#[serde(skip)]` and reconstructed lazily via `ensure_index` after deserialize.

- `crates/beava-core/src/sketches/top_k.rs` (~140 LOC) — `TopKState` 2-mode hybrid:
  - `Exact { BTreeMap<TopKValue,u64>, k, threshold, hybrid_width, hybrid_depth }` — exact counts up to `threshold` (default 1024) distinct values.
  - `Hybrid { CountMinSketch, TopKHeap, k }` — promoted on the (threshold+1)th distinct value; promotion folds existing counts into the CMS via one `update(hash, count)` per key, then seeds the heap.
  - Serde rename tags `v0_top_k_exact` / `v0_top_k_hybrid` survive bincode + JSON round-trip.

- `crates/beava-core/benches/phase10_topk.rs` — criterion microbench (per Phase 6+ Performance Discipline).

## Test count delta

+9 (cms) +8 (top_k) = **+17 tests** added. All green when sibling RED modules in the workspace are not blocking the test target build.

## O(log k) HashMap-position optimization — landed

Confirmed via `grep -n 'AHashMap<TopKValue, usize>' crates/beava-core/src/sketches/cms.rs`:
```
210:    index: ahash::AHashMap<TopKValue, usize>,
```

### Microbench results (`cargo bench --bench phase10_topk`, M-class laptop, release)

| Bench | Time | Per-call (approx) |
| --- | --- | --- |
| `insert_or_bump_below_capacity_k10` (10 inserts/batch) | 367 ns | ~37 ns/insert |
| `insert_or_bump_at_capacity_k10_d80` (100 mixed inserts/batch, includes `format!` per iter) | 198 µs | ~2 µs/insert (alloc-dominated) |

Below-capacity per-insert is well under the Plan 22-04 ~300 ns target. The at-capacity number is allocation-dominated by `format!("v{}", i)` inside the iter; future revisions can pre-build the value pool to isolate the heap path.

Full bench output: `/private/tmp/.../phase10_topk` last run captured 2026-04-23.

## CMS parameters (canonical)

| Param | Value | Notes |
| --- | --- | --- |
| `DEFAULT_CMS_WIDTH` | 2048 | per Plan 22-03 |
| `DEFAULT_CMS_DEPTH` | 4 | per Plan 22-03 |
| Hybrid promotion threshold | 1024 distinct | matches CountDistinct/Percentile |
| Snapshot tags | `v0_top_k_exact`, `v0_top_k_hybrid` | bincode + JSON round-trippable |

## TDD trace

```
test(10-04): add failing CMS + TopKHeap + TopKValue tests with O(log k) index check
feat(10-04): port CountMinSketch + TopKHeap (with Plan 22-04 O(log k) index) + TopKValue
test(10-04): add failing TopKState hybrid + heavy-hitters tests
feat(10-04): TopKState 2-mode hybrid (BTreeMap exact -> CMS+heap) with serde rename tags
chore(10-04): add phase10_topk microbench + SUMMARY
```

## Sibling-agent context

This plan ran in parallel with sibling agents executing 10-02 (HLL), 10-05 (UDDSketch), and 10-06 (CountDistinct/Percentile) on the same `phase-10-sketches` branch. Multiple snapshots showed sibling RED-only commits to `hll.rs`, `uddsketch.rs`, `percentile.rs`, and `count_distinct.rs` blocking the test/bench target build at various points. My local test runs were performed with sibling RED modules temporarily commented out in `mod.rs`; the commit only contains `cms.rs` + `top_k.rs` + bench + Cargo.toml additions, leaving sibling files untouched. Once siblings land their GREEN, the workspace `cargo test` will be fully green for plan 10-04 contributions.

## Deviations under Claude's Discretion

- **AggKind/AggOp dispatch wiring deferred** — the plan's title mentioned wiring AggKind into the dispatch table, but the plan body's `<tasks>` only specify Tasks 1+2 (CMS + TopKState landing) and Task 3 (verification gate). The AggKind wiring is implicitly part of a downstream plan (10-07 or later) that consumes these primitives. I shipped the primitives; wiring is single-line additions when consumers land.
- **`TopKHeap` API shape diverges from main**. Main exposes `observe(value, &CMS)` + `top_k(&CMS)`. The plan's tests pin `insert_or_bump(value, count)` + `top()` (count provided externally). I implemented the plan's API; the O(log k) HashMap-position optimization works the same way — the `insert_or_bump` hot path is O(log k) at all times.
- **`#[serde(tag = "mode", content = "data")]` attempted but rejected**: bincode does not support `deserialize_any` or `deserialize_identifier` for adjacently-tagged enums. Reverted to default serde representation with `#[serde(rename = ...)]` on each variant. JSON serializes as `{"v0_top_k_exact": {...}}` — still satisfies the `serde_tag_in_json` test, and bincode round-trips using variant indices.
