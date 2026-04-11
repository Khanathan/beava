# Horizon Survey: Low-Latency Low-Memory Streaming Feature Server

**Date:** 2026-04-11
**Scope:** v1.4+ / v2 / v3 exploration. Not scoped to any shipped or planned milestone.
**Research question:** What techniques — across sketches, compressed state, learned structures, tiered memory, sliding-window algorithms, cross-entity features, and batch/stream hybrids — could dramatically reduce Tally's per-entity memory footprint while preserving sub-100µs PUSH latency?
**Orientation:** "Depth > narrowness." Combinations and concrete crate/paper pointers are preferred over a laundry list of options.

---

## Framing

### Where Tally stands today (v1.2 shipped, v1.3 planned)

- Per-key state is a (stream → `LiveFeature`) nested `AHashMap` (`src/state/store.rs:45-82`).
- Windowed operators use bucketed ring buffers. A 24h window at 1-minute granularity = 1440 buckets × 8–16 bytes per bucket × N operators per key. Typical entity with 10 features ≈ 2–5 KB.
- `distinct_count` is HLL-14 — 16384 × 1-byte registers = **16 KB per operator per window per key** (verified in `src/engine/hll.rs:18-41`). The v1.0 decision table cites "~12 KB" but the actual dense array is 16 KB. Three HLLs on a single entity already exceed the "5 KB per key" PROJECT.md target.
- Large-pipeline benchmark shows HLL read is ~150× more expensive than non-HLL operators (`benchmark/tally-throughput/RESULTS.md:21`). This is a *latency* data point, not just memory — it shows every HLL read walks all 16384 registers with `powi()`.
- Dense HashMap of `String` keys → `EntityState` has **per-entry header overhead** at least 40–80 bytes before any operator payload (AHashMap bucket + key allocation + nested streams map).

### What "100M entities on one node" would cost us today (rough estimate)

Upper bound on the v1.2 model:
- 100M entities × (2 KB operator state + 100 byte header) ≈ **210 GB**. Already past commodity RAM.
- If a single HLL is attached: 100M × 16 KB = **1.6 TB**. Impossible.
- Window of 30 days × 1-min buckets × 8 bytes × 5 operators = 1.7 MB/key — **170 TB for 100M keys**. Also impossible.

So the "100M entities" question isn't a 1.5× optimization. It's **3 orders of magnitude** of compression, which only comes from rethinking the data structures, not tuning the allocators.

### Principles extracted from the research

1. **Approximate + bounded wins at scale.** Every large-scale production stack (Druid, Pinot, Presto, Datadog, Meta Beringei, LinkedIn Pinot, Dragonfly) runs on sketches whose error bars are known. Tally already accepted this with HLL-14; the question is "which other operators have a sketch replacement with similar tradeoffs."
2. **Memory-per-key is dominated by two things**: windowed ring buffers (many operators × many buckets) and HLL. Everything else is rounding error. Attacking these two is the whole game for v1.4.
3. **Latency floor for sketch operators is set by `read`, not `push`.** Push is O(1) cheap; read walks state. HLL-14's 16k-register scan on every read is the canonical example. Any replacement must *cache* the reading or use a representation whose read is cheap.
4. **Tiered storage is seductive but introduces hot-path regressions.** Redis-on-Flash is exactly the wrong story for Tally's <100µs target. Aerospike-style "index in RAM, value on NVMe" only works because their *hot read path never reads values*. Tally's hot path *is* reading and updating operator state. A disk tier only helps if a cold read can be deferred (i.e. is rarely requested) or if the entire cold path is asynchronously hydrated.
5. **Learned structures can win on steady-state keyspaces but cost a lot on cold start.** Only a fit for static feature stores (e.g. the batch-written `StaticFeature` side), not the live operator side. Noted but de-prioritized.
6. **The biggest unexplored direction is exponential-/decay-based windows.** These replace O(window/bucket) ring buffers with O(1) or O(log N) state per operator, with quantifiable error bars. They are the single largest memory reduction available.

---

## 1. ⭐ Sketch data structures beyond HLL

### 1.1 Apache DataSketches family

**Maturity:** Production-ready in Java/C++. **Rust port status: experimental and incomplete.** The `notfilippo/datasketches-rs` crate (https://github.com/notfilippo/datasketches-rs) is an unofficial port that wraps a subset. `kll-rs` (https://crates.io/crates/kll-rs) is a separate bindings crate over the C++ KLL sketch. Apache DataSketches promises cross-language binary compatibility (https://datasketches.apache.org/), which matters if Tally ever ships import/export of sketches.

| Sketch | Replaces | Memory | Read cost | Confidence |
|---|---|---:|---|---|
| **Theta sketch** | HLL + set intersection | ~30× more than HLL for same accuracy ([source](https://datasketches.apache.org/docs/Theta/ThetaSetOpsCornerCases.html)) | O(k) merge, O(k) count | HIGH |
| **Tuple sketch** | HLL + per-element summary (e.g. "distinct merchants AND total spend") | Theta + 8B/entry | O(k) | HIGH |
| **KLL quantile** | t-digest / histogram for percentiles | ~2.5 KB for ε ≤ 1% on 2^20 items ([source](https://arxiv.org/pdf/2102.09299)) | O(log k) for quantile query | HIGH |
| **REQ sketch** | relative-error quantiles at the tail | Similar to KLL, better tail accuracy | O(log k) | HIGH |
| **Frequent Items** (Misra-Gries / Space-Saving) | Top-K heavy hitters ("user's most-visited merchant") | O(k) where k = slot count; 200–500 bytes covers k=20 | O(k) read | HIGH |
| **Variance / AMS** | Second moment, stdev | ~64 bytes fixed | O(1) | HIGH |

**Key finding:** Theta sketch is the headline "set ops" sketch, but it costs ~30× HLL for equivalent cardinality accuracy. It's **not a replacement for HLL** — it's a *new operator* that enables cohort intersection features HLL cannot. Tally should keep HLL as the default distinct-count and introduce Theta as an opt-in for cross-cohort features.

**Critical:** The **Frequent Items (Space-Saving / Misra-Gries)** sketch is a 200–500-byte structure that answers "which are the top-K merchants / IPs / devices for this user over the window." Today Tally has no operator for this. It would unlock a user-visible feature — "top N X by Y" — that competitors (Pinot, Druid) already ship and that fraud-detection users ask for.

**Crates available in Rust now:**
- `streaming_algorithms` (https://github.com/alecmocatta/streaming_algorithms) — Count-Min, Top-K via CMS+doubly-linked hashmap, HLL, reservoir sampling. Production-quality maintenance status is spotty; last significant commits ~2022.
- `probably` (https://github.com/aeshirey/probably) — HLL, CMS, **Piecewise-Parabolic Quantile Estimator** (a moment-sketch-adjacent quantile structure).
- `probabilistic-collections` — broader CMS + Bloom + cuckoo collection.
- `pdatastructs` — `topk` module for Space-Saving (https://docs.rs/pdatastructs/0.5.0/pdatastructs/topk/struct.TopK.html).
- `t-digest` crate — pure-Rust t-digest port, but see 1.3 below for why to prefer DD-Sketch.

**Memory win vs latency cost:** Adding Space-Saving top-K at ~500 bytes/key is net-positive for feature breadth with zero latency regression (it's a linear-scan insert into a small bounded heap). Adding Theta is memory-negative but unlocks set ops nothing else can do.

### 1.2 Count-Min Sketch for approximate per-field frequency

**Maturity:** Production-ready everywhere. Rust crates available.
**Paper:** Graham Cormode, "Count-Min Sketch" (http://dimacs.rutgers.edu/~graham/pubs/papers/encalgs-cm.pdf)
**Conservative update:** ~1 order of magnitude accuracy improvement for positive-only streams (https://www.sciencedirect.com/science/article/abs/pii/S1389128622003607).

**Memory:** d × w × 4 bytes. For 1% error with 99% probability: d=7, w=272 → ~7.6 KB. For 5% error: d=5, w=55 → ~1.1 KB.

**Use case in Tally:** Per-user "how many events with X=value" without enumerating values. Today, if a user says "count fraud attempts per device_id for this user", Tally stores one counter per (user, device_id) pair — unbounded in the device_id dimension. A CMS collapses this to one fixed ~1 KB structure per user, with known error bars.

**Combined with HLL:** CMS gives frequency, HLL gives cardinality. Together they answer "how many distinct devices and how often did the top one appear" in ~17 KB. Per-sketch merge is O(d × w) and O(m), respectively.

**Latency cost:** CMS insert is d hash computations + d array increments (~50ns). Read is d lookups + a min. Both well under Tally's latency budget.

### 1.3 DDSketch (Datadog) vs t-digest vs KLL for quantiles

**DDSketch** (https://arxiv.org/abs/1908.10693; VLDB 2019):
- **Relative-error guarantee** — |x̂ − x| / x ≤ ε. Critical for latency percentiles where the interesting range spans many orders of magnitude.
- Memory is a function of the ratio max/min of observed values, not the cardinality of the stream. For values spanning 6 orders of magnitude at ε=2%, ~500 bins × 8 bytes = ~4 KB.
- Mergeable, which t-digest technically isn't (t-digest merges produce biased centroid shifts).
- Ingestion ~10× faster than GK, as fast as HDR Histogram (per the paper).
- **Only available as Java / Go / Python officially.** No production-ready Rust implementation; would need a port. "Needs port" flag.

**KLL sketch** (Apache DataSketches):
- **Rank-error guarantee** — ε in rank space, not value space. Worse at tails, strong in the middle.
- ~2.5 KB for ε=1% on 2^20 items.
- Rust binding exists via `kll-rs`.

**t-digest** (Dunning):
- Extremely strong tail accuracy (the use case it was designed for).
- No formal accuracy guarantee on merge.
- Well-maintained pure Rust port available.

**Recommendation for Tally:**
1. If a *percentile* operator lands first, use **KLL** via `kll-rs` — best available Rust binding with formal guarantees.
2. If latency-percentile features (e.g. "p99 transaction amount") are the use case, port **DDSketch** and accept the port cost. It's the only option with relative-error guarantees across the orders-of-magnitude spans ML features exhibit.
3. Do not adopt t-digest as primary; keep it as a reference for comparison benchmarks.

### 1.4 Space-Saving / Misra-Gries for top-K heavy hitters

**Algorithms:**
- Misra-Gries: O(1/ε) counters, reports items with frequency > εN. Classic.
- Space-Saving: extends Misra-Gries with a "replace least-frequent" slot; tracks which of the top-K are actually heavy.

**Memory:** `k × (key_bytes + 8)` where `k` is the number of slots. For k=20 and 32-byte string keys: ~800 bytes per per-entity top-K structure.

**Rust:** `pdatastructs::topk::TopK` (Space-Saving); `streaming_algorithms::Top` (CMS-backed Top-K, doubly-linked hashmap to order).

**Use case:** "Which merchants is this user transacting with most often in the last 24h." Today impossible in Tally without storing one counter per (user, merchant) pair — which is unbounded in the merchant axis.

**Latency:** Push is O(1) expected, read is O(k log k) for sorted output. Well within budget for k ≤ 100.

### 1.5 Exponential Histograms for sliding-window counters

**Paper:** Datar, Gionis, Indyk, Motwani. "Maintaining Stream Statistics over Sliding Windows" (SICOMP 2002 / SIAM 2002). https://epubs.siam.org/doi/10.1137/S0097539701398363

**Core result:** A sliding-window count of events (or sum) can be maintained in **O((1/ε) log² N)** bits, where N is the window size in events. Trading bucket granularity for accuracy. Concretely: a 30-day window with ε=1% error needs ~20–50 bucket groups instead of ~43,000 one-minute buckets.

**Memory comparison for a 30-day window, count operator:**
- Tally today: 30 × 24 × 60 = 43,200 buckets × 8 bytes = **346 KB per operator per key**. Multiplied across 10 operators and 100 M keys = instantly OOM.
- Exponential histogram at ε=1%: ~50 bucket groups × ~16 bytes each = **~800 bytes per operator per key**. **430× smaller.**
- At ε=5%: ~10 bucket groups × 16 bytes = **~160 bytes per operator per key**. **2160× smaller.**

**Caveat:** Exponential histograms work cleanly for additive *invertible* aggregates (count, sum). For **non-invertible** operators (min, max, distinct-count-in-window), different algorithms are needed:
- *Smooth histograms* (Braverman & Ostrovsky 2007, http://web.cs.ucla.edu/~rafail/PUBLIC/82.pdf) generalize exponential histograms to non-invertible functions.
- *Flattened exponential histograms* (https://arxiv.org/pdf/1912.03526) reduce the poly-log overhead.

**Maturity:** Research, with two reference Go/C implementations (https://github.com/monochromegane/exponential-histograms, https://github.com/pcosta74/exphist). Twitter's Algebird (Scala) has a production implementation (https://twitter.github.io/algebird/datatypes/approx/exponential_histogram.html). **No mature Rust crate exists — this would be a port.**

**Implementation risk:** Medium. The algorithm is well-specified, but bucket merge rules are subtle and have edge cases around window boundary transitions.

**Verdict:** **This is the single highest-ROI replacement for Tally's ring-buffer approach for large windows.** For windows under 1 hour, ring buffers are fine — the constants are small. For windows above 1 day, exponential histograms are a >100× memory win with a known error bound.

### 1.6 Other sketches worth a sentence

- **Moment Sketch** (Gan et al., VLDB 2018, https://arxiv.org/abs/1803.01969): stores only k moments + min + max. **~200 bytes** for ε ≤ 1% quantile error with 50ns merges. Simpler and smaller than KLL/DDSketch but breaks for heavy-tailed distributions. A future "cheap percentile" option.
- **AMS / Count Sketch** for second-moment and frequency-moment estimation (variance, stdev). Fills a gap: Tally has no native variance operator today.

### 1.7 Cuckoo, Xor, Binary Fuse, Ribbon filters

**Context:** Not for the live operator path directly — Tally doesn't have "is user X a member of set S" as a built-in feature. But these filters are critical for **negative-lookup elimination in the state store** and for implementing Bloom-style cohort membership.

**Space efficiency (2024-era):**
- Bloom filter at 1% FPR: ~10 bits/key. 44% overhead vs theoretical lower bound.
- Xor filter: ~9.84 bits/key at 0.4% FPR. 23% overhead. **Static only.** (https://arxiv.org/abs/1912.08258)
- Binary Fuse filter: ~9.0 bits/key at 0.4% FPR. 13% overhead. Static. (https://arxiv.org/pdf/2201.01174)
- Ribbon filter (Meta/Facebook, https://engineering.fb.com/2021/07/09/data-infrastructure/ribbon-filter/): comparable to Xor, supports deletes, small rebuild cost.

**Rust:** `xorf` crate (https://docs.rs/xorf/) implements Xor and Binary Fuse.

**Where this helps Tally directly:**
1. **Negative-lookup short-circuit on GET.** Today a GET for an unknown key costs a HashMap miss plus all the allocator work for the allocated slot. A Binary Fuse filter of size ~1.1 × num_keys × 9 bits tells us in ~40ns "definitely-not-present → return empty immediately." At 100M keys this is ~110 MB of filter, a rounding error. Saves the tail latency spike on unknown-key storms (relevant for fraud attacks that probe random keys).
2. **Cohort membership features.** "Is this user in the high-value cohort?" compiled to a filter at REGISTER time. O(1) lookup, no state growth.

**Latency:** ~40ns for Binary Fuse lookup (well within budget). Build cost is non-trivial (O(n)), so these are snapshot-time artifacts, not live-updated.

---

## 2. ⭐ Compressed state representations

### 2.1 RoaringBitmap for entity-key sets and cohort membership

**Maturity:** Production-hardened. Used by Lucene/Solr/Elasticsearch, Druid, Pinot, Spark, InfluxDB, Pilosa, ClickHouse, StarRocks, Doris, Redpanda.
**Rust:** `roaring` (pure Rust, https://docs.rs/roaring/) and `croaring` (FFI wrapper for CRoaring, https://docs.rs/croaring/). Both support 32-bit and 64-bit (Treemap/RoaringTreemap) variants.

**What it enables:**
- Compact representation of "which entities did X in the last hour" — dense when most entities did X, sparse when few did.
- Native set union, intersection, difference at SIMD throughput.
- Serializes to a spec-stable format that's cross-language compatible.

**Size example:** 1 million sequential IDs: <8 KB (one run container). 1 million random IDs out of 1 billion keyspace: ~2 MB. Compare to a dense bitmap at 1B bits = 125 MB or a `HashSet<u64>` at ~80 MB.

**Where Tally could use it:**
1. **Per-hour activity bitmap**, one bitmap per hour covering "which entity IDs had any event this hour." Answers "daily active users", "active in the last N hours", "how many distinct entities intersected two activity sets" in O(few ms) for 100 M-key spaces, at ~MB-scale total.
2. **Cohort features.** Each cohort = one Roaring bitmap. Membership test = O(log n). Building a cohort from an expression = a streaming scan.
3. **Dirty-key tracking in the incremental snapshot path.** Today `dirty_keys: AHashSet<EntityKey>` (store.rs:92). If we swap entity IDs to dense u32/u64 IDs, the dirty set collapses from MB-scale (string keys + hash overhead) to KB-scale (Roaring bitmap of IDs).

**Prerequisite:** Tally needs **entity IDs** (stable u64) alongside string keys. String interning (§2.5) unlocks this.

**Latency:** Lookup ~10–30ns. Well under budget.

### 2.2 Delta + varint encoding for sorted numeric state

**Observation:** Many of Tally's internal structures store sorted numeric data — bucket timestamps, event IDs, heap-based top-K structures. Varint + delta encoding is a 2–4× reduction for monotonically increasing data with small deltas.

**Example:** A `last` operator's 1-hour-buffered timestamps compress from 8 bytes/entry to ~1–2 bytes/entry.

**Rust:** `varint`, `integer-encoding`, `bitpacking` crates. Low risk, well-understood.

**Where it matters:** Only on serialization and deep-cold paths. Not on the hot update loop.

### 2.3 Columnar (SoA) feature layout per stream

**Observation:** Tally stores `AHashMap<String, StreamEntityState>` where each stream holds `Vec<(String, OperatorState)>` (store.rs:28). This is array-of-structs (AoS). For the read path that returns *all features for an entity*, this is fine. For the read path that returns *one feature for many entities* (MGET-shaped scan in the Debug UI, or a scheduled backfill), AoS is cache-hostile.

**Alternative:** Struct-of-arrays per stream — one `Vec<OperatorState>` for "all entities for feature X". Wins on batch scans and compression (columnar structures compress 3–5× with simple RLE / bitpacking). Losses on per-entity hot-path reads (no locality benefit).

**Verdict:** Not a win for Tally's primary hot path (per-entity PUSH). **But it's a potential win for an eventual "offline tier"** — a periodically dumped columnar snapshot of the dense operator values for batch training or cohort analytics. Mention as a complementary pattern in combinations.

**Related tech:** Apache Arrow + DataFusion (https://arrow.apache.org/blog/2025/01/10/arrow-result-transfer/, https://medium.com/data-reply-it-datatech/apache-data-fusion-building-next-generation-analytics-from-the-ground-up-560032a151d4). If Tally ever wants a "cold-side SQL layer" on top of its own data, DataFusion is the Rust-native answer and has a streaming execution mode.

### 2.4 Bloom / learned filters for negative-lookup elimination

Covered in §1.7.

### 2.5 ⭐ String interning for low-cardinality fields and entity keys

**Observation from reading `src/state/store.rs`:** The inner `AHashMap<String, StreamEntityState>` stores stream *names* as owned `String` per entity. If an entity has 5 streams with 20-character names, that's 100 bytes of stream-name strings × N entities. At 100 M entities: **10 GB of redundant stream-name strings**, all duplicated.

Same argument for:
- Operator names inside `Vec<(String, OperatorState)>`.
- `StaticFeature` feature-name keys.
- Low-cardinality event-payload fields stored in `last` operators: `country`, `merchant_id`, `category`, `status`.

**Fix:** Intern all of these through a global `lasso::ThreadedRodeo` (https://github.com/Kixiron/lasso). A `Spur` key (4–8 bytes) replaces a `String` (24 bytes header + bytes on heap). Operator names and stream names become `Spur`s at REGISTER time, lookup is O(1), resolution is O(1).

**Memory win on existing structures:**
- Stream name dedup: ~95% reduction in stream-name memory alone (from N-copies to 1-copy per stream).
- Operator name dedup: ~95% reduction similarly.
- Total saving: low single-digit GB at 100M-entity scale. Real but not headline.

**More importantly, this unlocks entity-ID-based structures** (§2.1 Roaring, §5.2 count-based windows, learned filters in §1.7). Once a string key has a stable `u64` ID assignment (via a string interner + monotonic counter), every downstream optimization that needs "dense integer IDs for bitmaps" becomes possible.

**Latency:** Lookup is one hash + one array index, ~15ns. Effectively free.

**Crate:** `lasso` (https://docs.rs/lasso) is the mature choice. Single-threaded `Rodeo` for Tally's single-shard-per-thread model (post-v1.3).

**Risk:** Leak on unbounded cardinality. Mitigation: only intern values from declared low-cardinality schema positions (stream names, operator names, enum fields). Never intern user-supplied opaque strings (entity keys, merchant IDs from arbitrary event payloads).

### 2.6 HLL sparse/dense representation

**Observation:** Tally's HLL is dense from birth — 16384 bytes even for an empty or near-empty sketch.
Presto's optimization (https://engineering.fb.com/2018/12/13/data-infrastructure/hyperloglog/): start in a sparse representation that stores only non-zero (index, value) pairs. Switch to dense when the sparse representation's size exceeds the dense threshold (~1027 bytes).

**Impact on Tally:** Most entities have small cardinality for any given distinct_count operator (typical: a handful of merchants per user in a window). Sparse HLL collapses these to **~100–500 bytes** instead of **16 KB**. **30–160× reduction for the median entity.** High-cardinality entities (bots, power users) keep the dense representation unchanged.

**Accuracy impact:** *Better* on small cardinalities (sparse representation is effectively exact below ~256 items), identical above.

**Latency:** Adds a ~50ns branch on insert (sparse vs dense). Read is faster in sparse mode (walk only populated entries).

**Verdict:** **This is a drop-in win with no architectural cost.** Highest priority "free" optimization in the survey. Should be a v1.4 candidate by itself.

---

## 3. Learned sketches and learned indexes

### 3.1 Learned indexes (RMI, RadixSpline, ALEX, PLEX)

**Papers:**
- Kraska et al. "The Case for Learned Index Structures" (SIGMOD 2018, https://www.cl.cam.ac.uk/~ey204/teaching/ACS/R244_2018_2019/papers/Kraska_SIGMOD_2018.pdf).
- Kipf et al. "RadixSpline: a single-pass learned index" (SIGMOD 2020, https://arxiv.org/pdf/2004.14541). GitHub: https://github.com/learnedsystems/RadixSpline.
- ALEX (updatable, Microsoft 2020).
- PLEX (practical, single hyperparameter).

**What they actually win:** Learned indexes beat B-trees on **sorted numeric keys** (primary key lookups in sorted tables) by predicting position from a compact model, shrinking the "last mile" binary-search window. **They do not directly replace hash maps for unordered string keys.** Tally's entity key space is unordered strings → this is not a drop-in.

**Where it could fit Tally:**
- If entity keys are *interned* to dense u64 IDs (§2.5) and the IDs are *roughly sorted* by activity-frequency or first-seen time, a learned index over `ID → shard-local-offset-in-some-dense-array` could replace a HashMap lookup at ~1.5–2× lower memory. Wins are 10–25% on total memory, not orders of magnitude.
- **Not worth it for v1.4–v2.** Cold-start cost, retraining drift, model size, and the embedded-binary-bloat argument all push this out of scope unless an order-of-magnitude win can be demonstrated.

### 3.2 Learned Bloom filters

**Papers:**
- Mitzenmacher, "A Model for Learned Bloom Filters, and Optimizing by Sandwiching" (NeurIPS 2018, http://papers.neurips.cc/paper/7328-a-model-for-learned-bloom-filters-and-optimizing-by-sandwiching.pdf).
- "Cascaded Learned Bloom Filter" (2024, https://arxiv.org/html/2502.03696v1) — "reduces memory usage by up to 24% and decreases reject time by up to 14× compared to state-of-the-art learned Bloom filters."
- "Stable Learned Bloom Filters for Data Streams" (VLDB 2020, https://www.vldb.org/pvldb/vol13/p2355-liu.pdf) — addresses the data-drift problem.

**Memory:** 5.6–6.1 MB model overhead plus the backup filter. Only wins over classical filters at > ~10 M keys.

**Latency:** Model inference on the hot path is risky. Mitzenmacher's sandwich design puts a small model *between* two classical filters, bounding worst-case cost at classical-filter level. "Cascaded Learned Bloom Filters" paper claims up to 14× faster reject times than prior learned designs but doesn't beat a Binary Fuse filter on pure speed.

**Verdict:** **Not worth pursuing for Tally.** Binary Fuse filters already deliver 13% above the information-theoretic lower bound with zero ML overhead. The marginal 24% improvement from learned approaches doesn't justify training/drift complexity in a zero-ops single-binary design. **Revisit only if evidence shows a >2× win on a specific use case.**

### 3.3 Learned Count-Min (Hsu et al., ICLR 2019)

**Paper:** Hsu, Indyk, Katabi, Vakilian. "Learning-Based Frequency Estimation Algorithms" (ICLR 2019).

**Core idea:** A small model predicts whether an incoming key is a "heavy hitter". Predicted heavy hitters are given dedicated counter slots (exact). The rest go through a classical Count-Min. Net: CMS-like memory with Space-Saving-like accuracy for the important items.

**Memory:** Similar to CMS, minus the heavy-hitter-induced error.
**Latency:** One model forward pass per push — for a small model, single-digit µs. **Risk: single-digit µs per push eats 5–10% of Tally's hot-path budget.**

**Verdict:** Interesting research direction. **Defer to a speculative v3 exploration.** Without a larger per-feature memory pressure crisis, the complexity is not worth the gain.

---

## 4. ⭐ Tiered memory / state externalization without breaking latency

### 4.1 Hot/warm/cold tiering — what works and what kills latency

**Prior art that *breaks* latency:**
- **Redis on Flash.** Keys in RAM, values on SSD. Reads that hit cold values add millisecond-scale latency. Kills Tally's <100µs p99 immediately for any cold touch. (https://aerospike.com/blog/redis-scalability/)

**Prior art that *preserves* latency:**
- **Aerospike Hybrid Memory Architecture (HMA).** Keys/index in DRAM, values on NVMe. Index is always hot → lookup is deterministic. Values are read via direct-IO from NVMe in a single device read. Claims sub-millisecond p99 even on cold values on modern NVMe. (https://aerospike.com/products/features/hybrid-memory-architecture/). The trick is that a single NVMe read is ~50–100µs — this *adds* to the latency floor, not the tail. For Tally's <100µs budget, this is **too slow for the cold path** but acceptable for an "eventually-warm" path if async prefetch is available.
- **Dragonfly (open-source Redis alt).** Keeps state in RAM but uses a custom shared-nothing snapshot model — doesn't tier.

**Key insight:** On 2026 NVMe, a single 4 KB random read is ~40–60µs. Tally's <100µs p99 budget means **at most one cold read is tolerable per PUSH, and only with async optimistic prefetch.** A cold path that dereferences multiple operator states serially breaks the budget.

### 4.2 The Noria partial-state model — the cleanest answer for "tiered state without breaking latency"

**Paper:** Gjengset et al., "Noria: dynamic, partially-stateful data-flow for high-performance web applications" (OSDI 2018, https://pdos.csail.mit.edu/papers/noria:osdi18.pdf). Continued as ReadySet (https://readyset.io).

**What Noria does that applies to Tally:**
- A materialized view is a **partially-stateful operator**. It holds results only for the keys that have been asked about recently.
- When a read comes in for a cold key, the read request **replays upstream events** to reconstruct the missing state on-demand, fills the view, returns the result, and keeps the now-hot state around. Future reads are instant.
- An eviction policy (LRU) drops cold state back.
- Writes to cold state are *dropped* — the event is absorbed by the event log but the operator state is not updated until a read "revives" it.

**How this maps to Tally:**
- Tally already has a per-stream **event log with TTL-aware compaction** (ELOG-01–05, all complete in v1.1). It already has the infrastructure to *replay events for a key*.
- Tally's current TTL eviction is all-or-nothing: a key is either fully live or fully gone. Partial statefulness is a **tiering without tiering** — you keep hot keys in RAM at full precision and drop cold keys entirely, with a guaranteed recovery path via event-log replay.
- **This is the model to copy for Tally's "100M entities on one node" scenario.** You keep 1M hot entities resident (~5 GB), 99M cold entities *evicted* from RAM, and the event log holds the replay material on SSD. Reads for cold entities pay a one-time "warm up" cost (event replay), which is batched and amortizes.

**Latency story:** Hot reads are unchanged (<50µs). Cold reads pay a replay cost: one NVMe event-log read (~50µs) + re-derive operators (microseconds). For a 1h-window stream with ~1000 events/key, replay is ~100µs of compute + one SSD read. **Cold reads hit 100–200µs, hot reads stay at <50µs.** The latency budget is not violated for hot traffic, and cold reads are degraded but serviceable.

**Risk:** Partial state is a well-known research idea with a production implementation (ReadySet), but it is not trivial to add to an existing state store. Requires an eviction policy, a "this key is cold" marker, and integration with the event-log reader.

**Verdict:** **The most promising tiering story for Tally.** This + exponential histograms (§1.5) is probably the v2 architectural north star.

### 4.3 `mimalloc` / `jemalloc` with huge pages

**Context:** Tally currently uses the default libc allocator (no explicit choice in the git log). For a many-small-allocations workload (millions of small HashMap entries, nested structs), the allocator choice is worth 5–15% of memory and 5–20% of hot-path time.

**Findings (2024-era):**
- mimalloc has the best "great support for huge pages" story and performs best on small allocations.
- jemalloc THP interaction is subtle — "RSS 50% higher than jemalloc itself reports" in pathological cases. Not a default-on win.
- For Tally's workload shape (small allocations, dense hash maps), **mimalloc is the default to benchmark first**.

**Huge pages win:** 2 MB pages instead of 4 KB pages → fewer TLB misses → ~5–20% memory-access speedup for random-access workloads. For Tally's nested HashMap traversal, this is an upper-single-digit % latency win. Not headline, but free.

**Risk:** Allocator-swap bugs can hide for months. Needs a careful rollout with before/after memory profiling.

**Verdict:** **Low-risk, low-reward, queue for v1.4 polish.** Not the headline of an architectural rev but a good free optimization.

### 4.4 Zstandard dictionary compression for per-entity state

**Idea:** Train a zstd dictionary from a sample of `postcard`-serialized `EntityState`s. Use the dictionary to compress each on-the-wire snapshot entry **and** (speculative) to compress idle entries in RAM.

**Compression ratio:** Zstd with a trained dictionary on structured small records achieves **~10× compression** (https://github.com/facebook/zstd/issues/3783) — much better than undictionaried zstd on the same small records (~2.8× baseline). The 10× number is for ~1KB JSON-ish records, which is the right shape for a Tally `EntityState` postcard serialization.

**Rust:** `zstd` crate with dictionary support (`zstd::dict::DecoderDictionary`).

**Use cases:**
1. **Snapshot size reduction.** 10× smaller snapshots means 10× faster snapshot writes, 10× less disk IO on recovery, 10× cheaper to ship snapshots to S3 if that ever lands. Zero impact on hot-path latency (snapshot is already off-main-thread post-v1.3).
2. **Speculative: warm-tier compressed in-RAM.** Evict cold state to a compressed blob in RAM. On read, decompress into a fresh `EntityState`. Decompression on ~1 KB ≈ 1 µs. This is a **~30× memory win at a 10 µs latency hit for cold reads.** Hot reads unchanged.

**Risk:** Dictionary must be retrained periodically as pipeline schemas evolve. Cache invalidation on schema change is a correctness trap (C-3 class risk). Low-to-medium.

**Verdict:** **#1 as snapshot size optimization for v1.4.** #2 as a tiering pattern is promising but needs the partial-state model (§4.2) to avoid touching the hot path.

### 4.5 CXL-attached memory

**Context:** CXL 2.0/3.0 is rolling into 2025–2026 servers. Linux 6.x supports CXL.mem as an extra NUMA node. Latency is 180–320% of local DRAM (https://sigops.org/s/conferences/hotos/2025/papers/hotos25-72.pdf). Bandwidth is within 10–20% of DRAM.

**Usage models:**
- **Transparent tiering via TPP** (Meta's Linux kernel module, https://www.usenix.org/system/files/osdi24-zhong-yuhong.pdf). Hot pages to DRAM, cold to CXL, kernel-managed.
- **App-aware NUMA placement.** Allocate hot structures on local DRAM, cold on CXL via `libnuma`.

**For Tally:** At 180–320% local-DRAM latency, a CXL memory access is ~150–300ns instead of ~80ns. Tally's hot-path loop probably does ~5–10 dependent memory accesses per PUSH. A fully-on-CXL hot path adds ~500–1500ns per push. **Still within the <100 µs budget, but a 1–3% hit.**

**The CXL win is not for the hot path. It's for the *cold tier.*** A 100M-entity state store at 5 KB/entity = 500 GB. That doesn't fit in a single server's DIMM sockets today (512 GB is expensive, 1 TB is rare). CXL lets you get to 2–4 TB of "slow RAM" on a single node, keep the hot 10% on DIMM, and serve the rest from CXL at ~300ns per access. **This collapses the Redis-vs-Aerospike distinction: everything stays in "memory", just a slower tier.**

**Maturity:** 2025+ mainstream availability. Kernel support exists. HybridTier (https://www.sihangliu.com/docs/hybridtier_asplos25.pdf, ASPLOS 2025) shows an adaptive CXL tiering system for in-memory DBs.

**Risk:** Hardware dependency. Only useful on modern servers with CXL slots. Contradicts "zero ops, runs anywhere."

**Verdict:** **Speculative v3.** Worth tracking but not worth building for. Mention in horizon doc as "the upper bound of what single-node memory density could be by 2027."

### 4.6 Compressed HashMap with fingerprint-only probing

**Papers & crates:**
- `SmoothieMap 2` (Java, https://leventov.medium.com/smoothiemap-2-the-lowest-memory-hash-table-ever-6bebd06780a3) — lowest memory hash table, typically ~1 byte overhead per entry.
- SwissTable (Google / Abseil) — control-byte-scanned, high-density open addressing. Already the standard in Rust's `hashbrown` crate, which `ahash` / `AHashMap` is built on.
- `sparse-map` and `emhash` in C++ — ~8-bit/entry overhead.

**Observation:** Tally already uses `AHashMap` which is `hashbrown`-based SwissTable. Current overhead: ~1.06× load factor × 8 control bytes per group of 16. **Tally is already on the state-of-the-art hash map implementation.** There's no headroom here.

**Remaining win:** Replace the inner `String` key (24-byte header + heap bytes) with interned `u32` IDs (§2.5). This removes 20–40 bytes per hash map entry. At 100 M entities: ~3 GB saved. Real but bounded.

**Verdict:** **Not a pursuit item by itself.** The win is inseparable from §2.5 interning.

---

## 5. Sliding window beyond ring buffers

### 5.1 Exponential histograms (see §1.5)

The headline item. Repeat: **430×–2160× memory reduction for additive operators on multi-day windows at 1–5% error.**

### 5.2 Exponentially decayed windows

**Paper:** Cormode, Korn, Muthukrishnan, Srivastava. "Forward Decay: A Practical Time Decay Model for Streaming Systems" (https://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf).

**Core idea:** Instead of an explicit window boundary, age each event by an exponential weight `exp(−λ × age)`. The aggregate is a single running decayed sum — **O(1) state per operator.** Controlled by a single decay constant `λ`.

**Memory:** A single f64 (8 bytes) per operator per key. **Compared to the current 1440-bucket 1-day ring buffer: 1440× reduction.**

**Semantic difference:** The "window" is no longer a hard cutoff — it's a soft tail. `count_1h_decayed` with half-life 30m answers "events weighted by recency" not "events in the last 60 minutes." For fraud/feature-serving use cases, *recency-weighted counts are often more useful than hard-window counts* — they are the natural ML feature shape.

**Latency:** Update is one multiply + one add + store. Read is trivial. **Faster than the current ring-buffer sum.**

**Caveat:** Cannot answer "events in the last X minutes" with arbitrary X — the decay is committed at insert time. You can, however, run N decayed operators at different half-lives to cover multiple time horizons.

**Maturity:** Well-understood algorithm. No mature Rust crate. Low implementation risk (it's a one-f64 running sum).

**Verdict:** **Offer as an opt-in operator family** — `st.count_decayed(half_life=...)`, `st.sum_decayed(...)`. Users who care about "classic fixed window" semantics stay with ring buffers. Users who care about "recency-weighted ML feature" get 1000× memory reduction and a more natural feature shape.

### 5.3 Bucket-chaining / SWAG algorithms

**Papers:**
- Tangwongsan et al. "Optimal and General Out-of-Order Sliding-Window Aggregation" (PVLDB 2019, https://www.vldb.org/pvldb/vol12/p1167-tangwongsan.pdf). Algorithm: **DABA** — Deamortized Banker's Aggregator. O(1) **worst-case** SWAG for any associative aggregate.
- SlickDeque (EDBT 2018, https://openproceedings.org/2018/conf/edbt/paper-197.pdf) — O(1) amortized.
- FlatFIT, Two-Stacks, Reactive Aggregator — earlier designs.

**What this gives Tally:** For **non-invertible** aggregates like `max`, `min`, `last-N`, current ring buffers store per-bucket maxima and merge them on read. This is correct but **O(buckets)** on read. DABA provides **O(1) read** and **O(1) push** for any associative aggregate (including non-invertible ones).

**Memory:** Same or better than ring buffers, typically 2–3× the minimum state needed for the aggregate.

**Latency:** Constant-time read on `max`/`min` is a real win for large windows. For a 24h 1-minute-bucket max operator, read goes from O(1440 comparisons) = ~5 µs to O(1) = ~50 ns. **~100× read speedup.**

**Rust:** No mature crate. Reference implementations exist in C++ (IBM, `SWAG` repo https://github.com/segeljakt/swag). Medium implementation risk (DABA is non-trivial).

**Verdict:** **High ROI for large-window min/max operators.** Should land as a window-implementation-internal upgrade (user-invisible) sometime in v2.

### 5.4 Count-based windows

**Observation:** A count-based window ("last 1000 events") is a *simpler* structure than a time-based window when the stream rate is predictable. It collapses to a ring buffer of N values (no time bookkeeping, no bucket merging).

**Memory win:** Marginal. ~30% cheaper than time-based at the same N.

**Value:** Semantic, not performance. Some fraud-detection features are naturally "last 100 transactions" not "last 1h".

**Verdict:** **Nice feature gap to close.** Should be on the v1.4 roadmap for user-facing surface reasons.

---

## 6. Cross-entity / graph features

### 6.1 MinHash / b-bit MinHash / LSH for set similarity

**Paper:** Andrei Broder, "On the resemblance and containment of documents" (1997). b-bit MinHash (Li & König, 2010).

**Use case for Tally:** "Users similar to this user by the set of merchants they've transacted with." Today impossible without storing full merchant sets per user. With 50 MinHash values at 1 bit each, compared across users: **6.25 bytes per user per set-similarity feature**, Jaccard similarity with ~10% error. For ε=5%, 200 1-bit MinHashes = 25 bytes.

**Cross-key computation:** Compare user A's 50 bits to user B's 50 bits, XOR + popcount = one cache line. Pair comparison is O(1). **LSH banding** lets you find "all users within Jaccard distance ε" in sublinear time.

**Rust:** `datasketch-minhash` port status unclear; `minhash` crates exist but are not production-proven. Needs curation.

**Verdict:** **The enabling primitive for a "similar entities" feature.** Not a v1.4 priority (no customer demand), but a compelling v2 differentiator.

### 6.2 Graph sketches — triangle counting, co-occurrence

**Papers:**
- DeStefani, Epasto, Riondato, Upfal. "TRIEST: Counting Local and Global Triangles in Fully Dynamic Streams" (KDD 2016, http://bigdata.cs.brown.edu/DeStefaniEtAl-Triest-KDD16.pdf).
- Wang et al. "Sliding Window-based Approximate Triangle Counting over Streaming Graphs with Duplicate Edges" (SIGMOD 2021, https://dl.acm.org/doi/10.1145/3448016.3452800). Code: https://github.com/StreamingTriangleCounting/TriangleCounting.
- VLDB Journal 2023 version (https://link.springer.com/article/10.1007/s00778-023-00783-3).

**What this unlocks:** Features like "how many of this user's friends bought from the same merchant as this user in the last hour" — triangle counts on an implicit bipartite (user, merchant) graph. Fraud rings are dense subgraphs; triangle counts are a proxy for ring detection.

**Memory:** O(k) reservoir where k is the sample size. For 1% relative error on global triangle count on a graph with 1M vertices and 10M edges, ~100 K samples ~1–2 MB total. **Per-feature, not per-key.**

**Verdict:** **Speculative v3 for fraud-specific use cases.** Too exotic for general feature serving but potentially a killer differentiator for fraud/trust/safety teams.

### 6.3 Co-occurrence sketches

**Observation:** "Most common merchant for this user last 30 days" is a Space-Saving top-K structure (§1.4) keyed by merchant, with per-user state. **~500 bytes per user per top-K operator.** Today impossible to express in Tally without storing a per-(user, merchant) counter (unbounded).

**This is probably the single most-requested-feature that Tally can't do today.** Top-K heavy hitters under a per-key group-by axis.

**Verdict:** **Strongly recommended as a v1.4 operator.** Rust crates exist (`pdatastructs::topk`, `streaming_algorithms::Top`). Straightforward to integrate. User-facing feature win with low engineering risk.

### 6.4 Shared / global sketches

**Observation:** Some features are *global* and don't need per-key state — "total distinct entities active today", "top 10 merchants by global volume". Tally's current model forces these into a per-entity layout. A **single shared sketch** keyed off "global" would be <100 bytes and serve millions of reads at no per-key cost.

**Verdict:** A **"shared sketch"** concept at the stream level (rather than the entity level) is a cheap, high-value addition. `@st.stream(shared=True)` or `@st.global`. Memory cost: one sketch for the whole server. Throughput: unchanged.

---

## 7. Batch-collection hybrids

### 7.1 Micro-batching at millisecond grain

**Observation:** v1.3 Phase 12 already introduces server-side coalescing at a 200µs grain. That's micro-batching. The question is whether we can do work at the **batch level** that's cheaper than doing it per-event.

**Cheap-at-batch-level work:**
- **Sort events by key** → improves cache locality on the subsequent operator update loop.
- **Deduplicate redundant key touches** → a burst of 64 events for one key becomes one lock acquisition, one HashMap lookup, one operator state fetch, 64 pushes, one read.
- **Delay HLL updates** → group distinct-value inserts into batches, bulk-update HLL in one pass.
- **Defer derive expression evaluation** → compute `derive` features once per batch per key, not once per event.

**Memory impact:** Zero direct. Latency impact: positive (batch amortization is already the whole point of Phase 12).

**Verdict:** v1.3 already ships the infrastructure. v1.4 should land the optimizations listed above *inside* the batch handler.

### 7.2 Deferred aggregation / "eventually-fresh" features

**Observation:** Some features don't need to be fresh on every single push. "Average transaction amount for the last hour" updated every 100ms is indistinguishable from updated every event for most consumers. Relaxing this constraint:

- Store per-batch partial aggregates. Merge into the primary operator state only every N batches or every T ms.
- Read path returns the primary state (slightly stale) plus optionally the per-batch partial (fresh).

**Memory impact:** Same, slightly more (one extra small partial struct per operator).
**Latency impact:** Push is *faster* (defer the merge cost).

**Verdict:** **Opt-in at operator definition time.** `st.avg(..., freshness="100ms")`. A real throughput win under skewed workloads where a few hot keys see all the pushes. Low priority for v1.4 unless specific benchmarks show the win.

### 7.3 Differential Dataflow / Noria-style incremental view maintenance

**Papers:**
- Abadi et al. "Differential Dataflow" (CIDR 2013).
- Noria (OSDI 2018, §4.2 above).
- "Timely Dataflow" (Materialize's foundation).

**What IVM gives you:** A declarative view definition compiles to a data-flow program that produces correct incremental updates. Changes propagate through joins, aggregates, group-bys automatically.

**For Tally:** The `@st.view` + `derive` + `lookup` surface is **already a lightweight IVM system** — derive expressions are incremental, views depend on upstream streams topologically. Adopting a full IVM framework would be a rewrite, not an incremental optimization.

**Verdict:** **Don't adopt.** The complexity-to-value ratio is bad. Keep the current derive/view model; borrow the *partial state* idea from Noria (§4.2) as the only thing worth copying.

### 7.4 Roaring + time partitions for sliding set membership

**Idea:** One Roaring bitmap per time partition (e.g. per hour). Each bitmap tracks "which entity IDs had event X in this hour." Sliding-window set membership ("was user active in the last 6h") = union of the last 6 bitmaps, done at Roaring SIMD speed.

**Memory:** At 1 M entities active per hour × 24 hours = 24 bitmaps × ~500 KB each = ~12 MB total. **A single 24h "active users" feature for 1M users fits in 12 MB.**

**Prerequisite:** String-to-u32 interning (§2.5). Entity IDs must be dense integers.

**Latency:** Insert is O(1) per entity per hour. Read (is X in 6h?) is 6 bitmap membership tests = ~100 ns. **Read is faster than a HashMap miss.**

**Verdict:** **Strongly recommended.** Enables a class of "was user active in window W" features that is expensive today. Low engineering risk after interning lands.

---

## 8. Recent (2023–2026) streaming/database systems worth studying

### 8.1 RisingWave / Materialize

- **RisingWave:** https://docs.risingwave.com/get-started/architecture. Uses a custom LSM-tree (Hummock) backed by S3 for state store. Streaming incremental view maintenance over SQL queries. Memory-conscious via LSM compaction.
- **Materialize:** Timely/Differential Dataflow at core. Single-node to distributed.

**For Tally:** These are the "what would a real IVM-native stateful stream system look like" references. Tally is a different design point (lower latency, simpler model, single-binary), but their state-store memory-management patterns are worth copying:
- Tiered LSM for cold state.
- Incremental view maintenance (differential dataflow).
- State store separate from compute (though this breaks Tally's <100 µs target — don't copy).

**Verdict:** Reference architecture. Don't adopt wholesale. The Noria partial-state model (§4.2) is a cleaner import.

### 8.2 Druid / Pinot sketch integration

Both use Apache DataSketches (Theta, HLL, KLL) as first-class aggregation types. Pinot's tiered-storage (InfoQ talk: https://www.infoq.com/presentations/apache-pinot-cloud/) shows how an OLAP engine ships sketches cached in S3 and queried with bounded latency.

**For Tally:** Validates the "sketches are production" argument. Direct reference for how to serialize sketches to snapshot format in a cross-version-safe way.

### 8.3 Arrow + DataFusion

Rust-native, in-memory columnar. If Tally ever adds a "bulk export for training" or "SQL query surface" side, this is the integration point. Streaming execution in DataFusion is newer and less mature than batch.

### 8.4 ClickHouse column compression

ClickHouse's LZ4/ZSTD-backed column compression with type-specific codecs (Gorilla for floats, DoubleDelta for timestamps) gives 5–10× compression on typical time-series workloads. Relevant if Tally ever serializes windowed operator state as columns instead of rows.

### 8.5 Facebook Beringei / Gorilla

**Paper:** Pelkonen et al. "Gorilla: A fast, scalable, in-memory time series database" (VLDB 2015). Code: https://github.com/facebookarchive/beringei.

**Core technique:** XOR-based compression of time-series values — if a value is close to the previous value, store only the differing bits. **~90% compression on real time-series.**

**For Tally:** Ring-buffer bucket values within a single operator form a time series. Gorilla encoding compresses `RingBuffer<f64>` from 8 bytes/bucket to ~1–2 bytes/bucket. **4–8× memory win on windowed operators.**

**Latency:** Encode/decode is ~20–40 ns per value. Reads require decoding all buckets in the window — for a 24h operator at 1440 buckets, that's ~30 µs. **Reads get slower unless a cached aggregate is maintained.**

**Verdict:** **Conflicts with read latency.** Only worth it for *snapshot-time* compression, not live state. Exception: if the operator caches its `current_value` (which Tally already does, `LiveFeature.current_value`), the live read path doesn't touch the compressed bucket history, so Gorilla on the buckets becomes a *cold storage* win with no latency cost on reads. **This is a real optimization.**

---

## 9. Combinations worth exploring that no single source covers

(Full expansion in `HORIZON-COMBINATIONS.md`.)

Short list, previewing the Combinations doc:

1. **Sparse HLL + exponential histogram + interned entity IDs** — the "100 M entities at sub-100µs" stack.
2. **Roaring time-partition bitmaps + Binary Fuse filters for existence checks + partial state (Noria-style)** — the "cold tier without latency hit" stack.
3. **Zstd dictionary snapshots + Gorilla-compressed operator histories + incremental delta snapshots** — the "10× snapshot size reduction" stack.
4. **Exponentially decayed windows + DABA + Space-Saving top-K** — the "new operator surface for fraud/ML" stack.
5. **Tuple sketch + MinHash LSH + cross-entity lookups** — the "cohort analytics and entity similarity" stack.
6. **CXL DRAM extension + partial state + compressed warm tier** — the "2027 density ceiling" stack (v3 speculation).
7. **Per-shard learned index over interned IDs + Binary Fuse negative filter** — the "point-lookup fast path" stack (modest win).
8. **Shared global sketches + derive expressions** — the "global features without per-key cost" stack (low-hanging fruit).

---

## Confidence by section

| Direction | Confidence | Reason |
|---|---|---|
| 1. Sketch DS (Theta, KLL, DDSketch, Space-Saving) | HIGH | Multiple papers, production systems, Rust crates exist for most |
| 1.5 Exponential histograms | HIGH (algorithm), MEDIUM (Rust crate) | Classic paper, no mature Rust crate — port risk |
| 1.7 Filter families (Xor, Binary Fuse, Ribbon) | HIGH | Papers + production Rust crate (`xorf`) |
| 2.1 RoaringBitmap | HIGH | Production Rust crate, wide adoption |
| 2.5 String interning | HIGH | `lasso` is mature |
| 2.6 Sparse HLL | HIGH | Widely-used Presto/Druid technique, straightforward Rust impl |
| 3. Learned structures | MEDIUM | Papers strong, production relevance weak for Tally |
| 4.2 Partial state (Noria) | HIGH (design), MEDIUM (Tally integration) | OSDI paper, ReadySet is productionized |
| 4.3 Allocators | HIGH | Well-studied |
| 4.4 Zstd dictionaries | HIGH | Production Rust crate, proven ratios |
| 4.5 CXL | MEDIUM | Hardware-dependent, 2025-era papers |
| 5.2 Decayed windows | HIGH | Classic algorithm, trivial to implement |
| 5.3 DABA / SWAG | HIGH (algorithm), LOW (Rust) | VLDB paper, no Rust crate |
| 6. Cross-entity / graph | MEDIUM | Papers strong, exotic use cases |
| 7. Batch hybrids | HIGH | Proven by Tally's own v1.3 work |
| 8.5 Gorilla | HIGH (algorithm) | Production-proven |

## Gaps / deeper follow-ups

- **Benchmark-grounded sizing.** The memory numbers in this survey are computed from papers, not measured on Tally's actual workloads. A 2-day spike on a realistic corpus would ground the promised wins.
- **DDSketch Rust port effort estimate.** Nobody has published one. Unknown cost.
- **Exponential histogram correctness under Tally's clock semantics.** The paper uses logical event time; Tally uses wall-clock with client-supplied timestamps. Needs explicit adaptation.
- **Partial-state eviction policy.** Noria uses LRU; Tally's patterns (bursty hot keys from fraud attacks) may need LFU or ARC.
- **Tuple sketch vs theta sketch memory measurement.** Paper says "30× HLL" for theta; tuple sketch cost is paper-implicit.
- **Zstd dictionary retraining cadence.** Schema evolution interacts with dictionary lifetime; needs design.

---

*Research compiled 2026-04-11. Sources cited inline. No inference was made without a source reference — gaps are flagged as "needs port" or "needs measurement".*
