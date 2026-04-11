# Horizon Next Steps

**Date:** 2026-04-11
**Purpose:** A short, opinionated ordering of the survey's findings into **what to actually do next**, after v1.3 lands. Each item answers:
- **Why now** — is infrastructure ready?
- **Quick prototype cost** — days to a throwaway spike.
- **Decision gate** — what measurement or outcome would commit/abandon.
- **Target milestone** — v1.4 / v2 / v3+.

Ordered by attractiveness = (memory or latency win) × (maturity) × (low integration risk).

---

## Tier 1 — v1.4 candidates (do these next, after v1.3 ships)

### 1. Sparse HLL representation (§2.6 in HORIZON-SURVEY.md)

- **Why now:** v1.3 completes sharding, so the HLL code lives inside a per-shard store — safe to refactor without touching the hot path. The win is unambiguous: **30–160× memory reduction** on the median `distinct_count` operator, faster reads on small cardinalities, identical behavior on large.
- **Prototype cost:** **3–5 days.** The Presto/Druid reference design is well-specified. Tally's current `Hll` struct in `src/engine/hll.rs` is ~100 lines; sparse variant is ~200 lines + serde variant for snapshot compat.
- **Decision gate:** Land the variant behind a feature flag. Benchmark the large pipeline (which has 3 HLLs per entity) at 100 K entities: if average HLL size drops below 1 KB and the 150× `read` cost regression goes away, ship it.
- **Risk:** Low. Sparse/dense switch has one edge case (merge across sparse+dense) that's well-documented in Presto's blog post (https://engineering.fb.com/2018/12/13/data-infrastructure/hyperloglog/).
- **Target milestone:** **v1.4.** This is the single best "free win" in the survey.

### 2. String interning for stream/operator/field names (§2.5)

- **Why now:** Unlocks every dense-ID downstream (Roaring bitmaps, learned filters, per-shard arrays). Small immediate win (~1–5% memory) but load-bearing for v2 architectural moves.
- **Prototype cost:** **5–7 days.** Add a per-shard `lasso::Rodeo`. Intern stream names, operator names, static-feature keys. Change `AHashMap<String, ...>` to `AHashMap<Spur, ...>` at the hot-path boundaries. Add debug inspector that round-trips names back out.
- **Decision gate:** Cargo bench the store lookup path; verify that `Spur` comparison is not slower than `String::eq`. Verify snapshot round-trip through `lasso`'s serde integration.
- **Risk:** Medium — this touches many call sites. Mitigation: start with static strings only (stream names, operator names) before user-supplied values.
- **Target milestone:** **v1.4.** Required enabler for items 5, 6, 8.

### 3. Shared / global sketches (§6.4, Combination C6)

- **Why now:** This is the **lowest-implementation-cost feature addition in the whole survey**. Memory cost is basically zero; the user-facing surface is a small SDK extension (`@st.global` or `shared=True`). Unlocks "top-K merchants globally", "total distinct users today", etc. — features users ask for but cannot express.
- **Prototype cost:** **4–6 days.** A new `GlobalSketchStore` on the server side, a new Python SDK decorator, a new HTTP endpoint for reading (since these aren't per-entity). REGISTER protocol extension.
- **Decision gate:** Write a small fraud-scenario benchmark that would use "top-5 merchants by global volume." If it drops from "impossible" to "~500 bytes of server memory", ship it.
- **Risk:** Low. Completely additive; no interaction with existing invariants.
- **Target milestone:** **v1.4.** Ship alongside item 1.

### 4. Zstd dictionary snapshots (§4.4, part of C3)

- **Why now:** v1.3 Phase 15 moves snapshot IO off the main thread, so compression cost is off-hot-path by construction. The win is real (**8–12×** on-disk reduction per `HORIZON-SURVEY.md §4.4` and confirmed by Zstd dict references).
- **Prototype cost:** **4–5 days.** Train a dictionary from a sample of postcard-serialized entities (one-time, at install or snapshot 0). Add `zstd::block::Compressor` with dictionary. Version the dictionary in the snapshot header.
- **Decision gate:** Measure snapshot size before/after on the v1.3 benchmark dataset. If ratio is <5×, investigate (dictionary might be poorly trained). If ratio is >5×, ship.
- **Risk:** Low-medium. Dictionary retraining on schema change is a correctness trap — need a migration test.
- **Target milestone:** **v1.4.**

### 5. Space-Saving top-K operator (§1.4, part of C4)

- **Why now:** Adds a qualitatively new feature surface (`st.top_k(field, k=20)`) that users regularly ask for. Implementation is mature (Rust crate `pdatastructs::topk` available). Memory is bounded and small.
- **Prototype cost:** **5 days.** New operator variant in `OperatorState`, new Python SDK class, snapshot support. The hard part is merging two Space-Saving structures correctly on cross-shard lookup.
- **Decision gate:** Push 10 K events with a skewed merchant distribution, verify top-5 matches exact counting within ε. Latency overhead <200 ns on push.
- **Risk:** Low. Reference implementation exists.
- **Target milestone:** **v1.4.**

### 6. mimalloc + huge pages evaluation (§4.3)

- **Why now:** Cheap to try, single-digit % memory and latency win for a Cargo feature flag change.
- **Prototype cost:** **1–2 days.** Add `mimalloc` as a dep behind a feature flag, benchmark, check RSS vs observed allocations.
- **Decision gate:** If RSS drops >5% and hot-path p99 drops >3%, ship. If not, shelf and document.
- **Risk:** Low. Swap is reversible.
- **Target milestone:** **v1.4 polish.**

---

## Tier 2 — v1.4 to v2 candidates (likely next after Tier 1)

### 7. Exponential histograms for large-window operators (§1.5, core of C1)

- **Why now:** **This is the single highest-ROI memory reduction in the survey** (430×–2160× for 30-day windows). But: no mature Rust crate, so it requires a port.
- **Prototype cost:** **10–15 days** for a working invertible-aggregate implementation (count, sum). Non-invertible (min, max) is another 5–10 days via smooth histograms.
- **Decision gate:**
  - A 30-day count operator drops from 346 KB to <2 KB per key in a benchmark.
  - 1–5% accuracy error on a synthetic event stream.
  - Push latency within 10% of current ring-buffer implementation.
- **Risk:** Medium. Bucket merge rules are subtle. Edge cases around window boundary transitions. Needs rigorous unit testing.
- **Target milestone:** **v1.4 stretch or v2.** Worth the effort.

### 8. Exponentially decayed window operator family (§5.2, part of C4)

- **Why now:** Semantically more appropriate for ML features than hard windows. O(1) state vs O(window/bucket). Implementation is trivial (one running f64).
- **Prototype cost:** **4–6 days.** New operator variants (`CountDecayedOp`, `SumDecayedOp`, `AvgDecayedOp`). Python SDK class. Snapshot support.
- **Decision gate:** Behavior matches paper formula. Single benchmark showing user-visible memory drop of >100× for a 7-day-equivalent decayed operator vs a 7-day ring buffer.
- **Risk:** Low. Math is trivial. Main risk is user confusion about semantics — mitigate with docs.
- **Target milestone:** **v1.4 or v2.**

### 9. Count-Min sketch operator for approximate per-field frequency (§1.2)

- **Why now:** Enables "count events matching a predicate on an unbounded field" without per-value state. ~1–8 KB per operator per key.
- **Prototype cost:** **5 days.** `streaming_algorithms::CountMinSketch` or `probably` is already Rust-native; wrap in a Tally `Operator` impl.
- **Decision gate:** Accuracy within paper bounds on skewed stream.
- **Risk:** Low.
- **Target milestone:** **v1.4 if time, else v2.**

### 10. Binary Fuse filter negative-lookup short-circuit (§1.7)

- **Why now:** Trivial addition. ~40 ns latency saving on unknown-key GET. Zero downside.
- **Prototype cost:** **2–3 days.** `xorf::BinaryFuse16` from `xorf` crate. Rebuild at snapshot time. Check filter before HashMap lookup on GET.
- **Decision gate:** Unknown-key GET p99 drops by >50 ns. Filter build time <100 ms on 1 M keys.
- **Risk:** Low. Filter is static — needs a rebuild policy (e.g. once per snapshot cycle).
- **Target milestone:** **v1.4.**

### 11. Gorilla-compressed bucket histories (§8.5, part of C3)

- **Why now:** 4–8× compression on per-operator ring-buffer contents. Only applied to *snapshot format* and *cold-side access*, not the hot path (Tally caches `current_value` on `LiveFeature`).
- **Prototype cost:** **5–7 days.** Implement Gorilla encoder/decoder for `RingBuffer<f64>`. Wire into the postcard serialization path.
- **Decision gate:** 4× or better on-disk shrink of ring-buffer bytes in a benchmark snapshot.
- **Risk:** Medium. Decoder must be bit-exact vs encoder; loss = corruption.
- **Target milestone:** **v1.4 polish, or v2 alongside C3.**

---

## Tier 3 — v2 architectural work

### 12. Partial-state / Noria-style hot/cold tiering (§4.2, C2)

- **Why now:** Enables the "100 M addressable entities per node" headline. Requires integration with the event log replay code (already shipped as the backfill path in v1.1).
- **Prototype cost:** **15–25 days.** New eviction policy (start with LRU). "Cold-read → replay → hydrate → return" code path. Eviction metrics. Benchmark harness for cold-read latency.
- **Decision gate:**
  - Hot reads unchanged (<50 µs p99).
  - Cold reads ≤250 µs p99.
  - Memory footprint of 10 M hot entities matches v1.2's 10 M-entity footprint.
  - Addressable entity space demonstrably 10× hot footprint.
- **Risk:** High. Correctness-heavy change. Eviction policy tuning is workload-dependent.
- **Target milestone:** **v2 headline feature.**

### 13. DABA O(1) sliding-window aggregation for min/max (§5.3, part of C4)

- **Why now:** Replaces O(buckets) read on large `max`/`min` operators with O(1). **~100× read latency improvement** on 24h+ windows. Implementation is non-trivial but well-specified.
- **Prototype cost:** **10–12 days.** DABA algorithm is published; reference impls exist in C++/Scala.
- **Decision gate:** Large pipeline benchmark (which today spends ~150 µs per HLL read) drops the non-HLL large-window overhead to single-digit µs.
- **Risk:** Medium-high. Algorithm complexity + loom-style testing for correctness.
- **Target milestone:** **v2.**

### 14. Tuple / Theta sketches for cohort intersection features (§1.1, part of C5)

- **Why now:** Only if cohort intersection features become a user request. Until then, speculative.
- **Prototype cost:** **12–20 days.** Port Theta sketch from `datasketches-rs` experimental Rust to a stable, tested implementation.
- **Decision gate:** A customer asks for "users in cohort A AND cohort B" feature in production.
- **Risk:** Medium. Port risk, accuracy-on-small-intersection documented weakness.
- **Target milestone:** **v2 if demand exists, else defer.**

### 15. MinHash LSH for entity similarity features (§6.1, part of C5)

- **Why now:** Same as #14 — demand-gated.
- **Prototype cost:** **10 days.**
- **Target milestone:** **v2 if demand exists, else defer.**

---

## Tier 4 — v3+ speculative (track but don't build)

### 16. CXL memory tiering (§4.5, C8)

- **Why now:** Hardware ceiling is 2–4 TB/node by 2027. CXL 2.0+ servers mainstream 2026–2027.
- **Gate:** A single customer hits the DRAM density ceiling on v2's partial-state architecture.
- **Target milestone:** **v3 speculation.**

### 17. Learned indexes for faster point lookups (§3.1, C7)

- **Why now:** Only if benchmarks show SwissTable is the hot-path bottleneck post-v2, which seems unlikely.
- **Target milestone:** **v3 if ever.**

### 18. Learned Bloom filters (§3.2)

- **Never.** Binary Fuse (#10) closes the filter conversation with zero ML overhead.

### 19. TRIÈST / graph sketches for fraud-ring detection (§6.2)

- **Why now:** Only for a fraud-specific customer.
- **Target milestone:** **v3 speculation.**

---

## Deeper follow-ups (out of scope for this survey — need dedicated research)

These surfaced during the survey but exceed its scope. Spawn dedicated research when they come up for decision:

1. **DDSketch Rust port.** Nobody's done one. If latency-percentile features become a roadmap item, scope a 2-week port effort.
2. **Exponential histogram clock semantics under Tally's wall-clock + event-time mix.** The classic papers assume logical time; Tally mixes system time and client-supplied timestamps. Needs a careful design doc before implementation of item 7.
3. **Partial-state eviction policy for bursty fraud workloads.** LRU vs ARC vs 2Q vs S3-FIFO. Needs trace-driven benchmarks.
4. **Zstd dictionary retraining cadence under pipeline schema evolution.** Schema changes invalidate dictionary effectiveness. Need a policy doc.
5. **Cross-shard Theta sketch merge semantics.** Theta merges are associative but have subtle edge cases on intersection. If C5 lands, this needs its own correctness review.
6. **Gorilla bit-exactness for cross-shard snapshot compatibility.** If shards encode independently, decoder must match bit-for-bit.
7. **Benchmark on real workload shapes** for every memory claim in `HORIZON-SURVEY.md`. The numbers are computed from papers, not measured on Tally. A 2-day "horizon benchmark spike" on synthetic fraud traffic would ground them.
8. **Interaction between partial state and fire-and-forget PUSH.** v1.2's fire-and-forget semantics + v2's evict-on-cold semantics may silently drop events. Needs an explicit ordering contract.
9. **Apache DataSketches Rust port maturity audit.** The unofficial `notfilippo/datasketches-rs` port is a dependency risk. Evaluate whether to contribute upstream, fork, or write from scratch.
10. **Per-operator "accuracy mode" SDK surface.** If users can opt into sketches at different ε per operator, how is the SDK ergonomic? Needs a Python DX review.

---

## Decision rubric for v1.4 scoping

Use this table when scoping v1.4 from the above. Pick items whose combined prototype cost fits the milestone budget:

| Tier | Item | Prototype days | Mem win | Latency win | Feature unlock |
|---|---|---:|---:|---:|---|
| 1 | Sparse HLL | 3–5 | **30–160×** on HLLs | +read speed for small | — |
| 1 | String interning | 5–7 | 1.05–1.1× baseline | 0 | Enabler for v2 |
| 1 | Shared sketches | 4–6 | ~0 | 0 | **Global features** |
| 1 | Zstd dict snapshots | 4–5 | 8–12× on disk | 0 | Smaller snapshots |
| 1 | Space-Saving top-K | 5 | ~0 | 0 | **Top-K operator** |
| 1 | mimalloc | 1–2 | 5–10% | 5–10% | — |
| 2 | Exp histograms (count/sum) | 10–15 | **430–2160×** on large windows | ~0 | Multi-day windows cheap |
| 2 | Decayed windows | 4–6 | **1440×** | +push speed | Recency-weighted features |
| 2 | Count-Min operator | 5 | ~0 | 0 | Per-field frequency op |
| 2 | Binary Fuse filter | 2–3 | ~0 | -50 ns on miss | Faster misses |
| 2 | Gorilla buckets | 5–7 | 4–8× on snapshot | 0 | Smaller snapshots |

**Suggested v1.4 milestone: items 1, 2, 3, 4, 5, 6 from Tier 1 + items 7, 8 from Tier 2.** Estimated aggregate: **40–55 prototype days + 20–30 days of integration, testing, bench matrices.**

**Suggested v2 milestone: item 12 (partial state) as the headline, plus items 11, 13 as supporting.** Estimated aggregate: **40–60 days.**

---

*Compiled 2026-04-11 as a companion to `HORIZON-SURVEY.md` and `HORIZON-COMBINATIONS.md`. No citations duplicated here — see survey for sources.*
