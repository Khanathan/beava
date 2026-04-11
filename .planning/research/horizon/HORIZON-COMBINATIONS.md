# Horizon Combinations: Cross-Technique Synthesis

**Date:** 2026-04-11
**Companion to:** `HORIZON-SURVEY.md`
**Goal:** Identify 5–8 **combinations** (not single techniques) whose interaction produces emergent memory and/or latency wins beyond any single component.

Each combination answers: *"What new thing would Tally be able to do that it cannot do today, and what would it cost?"*

---

## Combination Ranking Summary

| # | Combination | Mem Win | Latency Cost | Maturity | Unlocks |
|---|---|---:|---|---|---|
| C1 | Sparse HLL + Exp Histogram + Interning | **30–200×** | Neutral or slightly positive | Mostly mature; one port needed | 100M entities, multi-day windows |
| C2 | Partial State + Event Log Replay + Zstd Warm Tier | **10–50×** | Cold reads +50–200µs; hot reads unchanged | Medium (ReadySet exists) | Cold-tier without breaking hot path |
| C3 | Zstd Dict + Gorilla Columns + Incremental Deltas | **8–12× (snapshot)** | Zero hot-path | Production crates exist | Smaller snapshots, faster recovery, S3 tier feasible |
| C4 | Decayed Windows + DABA + Space-Saving Top-K | **100–1000× (large windows)** | Push ~neutral; read faster on max/min | Papers solid, porting effort | Recency-weighted and top-K features |
| C5 | Tuple/Theta Sketches + MinHash LSH + Roaring Bitmaps | **New feature class** | +50–200ns per cross-entity op | Medium (DataSketches port) | Cohort intersections, entity similarity |
| C6 | Global Sketches + Count-Based Windows + Shared Operators | **1000× for global features** | Zero | Trivial to implement | "Global top-K", "active-in-last-N" at no per-key cost |
| C7 | Per-Shard Learned Index + Binary Fuse Negative Filter | **1.2–2×** | -20–50ns per lookup | Research | Faster point lookups, fewer misses |
| C8 | CXL Tier + Partial State + Compressed In-RAM Warm | **10–20× effective density** | Cold +300ns, hot unchanged | Speculative (2027+) | 10B entities/node ceiling |

---

## C1. ⭐ Sparse HLL + Exponential Histograms + String Interning

**The "100M entities, multi-day windows, sub-100µs" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Sparse HLL | §2.6 | Distinct-count that starts at ~100 bytes, grows to 16 KB only for power users |
| Exponential histograms | §1.5 | Sliding-window counters/sums with O(log N) state, 1–5% error |
| String interning (`lasso`) | §2.5 | Stream/operator/field-name dedup; enables dense u32 entity IDs |

### Why this is a combination, not a stack of independents

- **Interning is a prerequisite for dense snapshots.** Without it, serialized state is dominated by duplicated string literals — which kills the compression ratio of any downstream format.
- **Sparse HLL plus exponential histograms together shrink *both* the dense and the sparse parts of the per-entity budget.** Individually each wins one half; together they push average per-entity memory from ~5 KB to **~200–500 bytes** for typical pipelines.
- **Exponential histograms expose a new "bucket count" parameter tied to ε, not to window duration.** Interned stream names let us carry that parameter through snapshot schemas cleanly.

### Memory math

Baseline: typical pipeline with 10 features per entity, one HLL, mixed windowed operators on windows ranging 1h–30d.
- v1.2: ~5 KB per entity (PROJECT.md target) + 16 KB if any HLL = **21 KB typical**.
- v1.2 at 100 M entities: **2.1 TB**. Infeasible.

With C1:
- Interned stream/op names: −20% (~17 KB → ~14 KB before sketches).
- Sparse HLL (median entity has ~20 distinct values): HLL drops from 16 KB → ~150 bytes. **~10 KB saved per entity.** New baseline ~4 KB.
- Exponential histograms on the 30-day count/sum operators: each drops from ~170 KB → **~1 KB**. If 3 such operators: **~500 KB → ~3 KB, net ~497 KB saved** per entity.
- At 100 M entities with the full stack: **~500 GB → ~15 GB.** Fits on a single modern server.

### Latency

- Sparse HLL insert: +50 ns branch, −0 ns on read (actually faster for small cardinalities).
- Exponential histogram push: ~80 ns (hash + bucket resolve + conditional merge). Same or slightly faster than the 1440-bucket ring buffer.
- Interning lookup: +15 ns per lookup, amortized away by pointer passing.

**Net:** ~Neutral to slightly faster. No latency regression.

### Prerequisites

- v1.3 sharding complete (C1 interacts with per-shard interners — each shard owns its own `Rodeo`).
- A "hash-version" byte in snapshots already planned for v1.3 — reuse for C1's format version bump.

### Risks

- **Port risk on exponential histograms.** No mature Rust crate. Medium implementation difficulty; a 2–3 week spike can land a working prototype.
- **Sparse HLL correctness on merge.** Presto's implementation is the reference; need to match exactly for cross-version snapshot compatibility.
- **Interning leak under adversarial traffic.** Mitigation: only intern schema-declared fields, never user-supplied opaque strings.

### User-facing unlock

- **100M-entity support on a single node** — the headline memory claim for v2.
- **Multi-day and multi-week windows** become practical, not just "technically possible."
- **"Affordable HLL"** — adding a distinct_count operator no longer instantly adds 16 KB per key. Users can use HLL more freely.

---

## C2. ⭐ Noria-style Partial State + Event Log Replay + Zstd Warm Tier

**The "cold tier that doesn't break the hot path" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Partial state / on-demand materialization | §4.2 | Hot keys resident, cold keys evicted-and-replayable |
| Event log replay (already shipped) | v1.1 ELOG-01–05 | The replay source of truth |
| Zstd dictionary compression for evicted state | §4.4 | Keep "recently cold" keys compressed in RAM instead of fully evicted |

### Why this is a combination

- **Noria's partial-state model alone** requires an event log to replay from. Tally already has one. Combining them is *free* architecturally.
- **Replay cost is the bottleneck for cold reads.** A 1000-event replay is ~100 µs of operator work + one SSD read. Adding a **compressed warm tier** in-RAM lets us bypass the event log entirely for "recently cold" keys — decompress a ~500-byte blob in ~1 µs.
- Three-tier architecture emerges: **hot (full state, <50 µs read) → warm (compressed in RAM, ~10 µs read) → cold (evicted, must replay from event log, ~200 µs read)**.

### Memory math

Baseline v1.2: 1 M hot entities at 5 KB = 5 GB. Cannot hold 100 M.

With C2:
- Hot tier: 1 M entities at 5 KB = **5 GB**.
- Warm tier (compressed): 9 M entities at 500 B (10× compression from zstd dict) = **4.5 GB**.
- Cold tier: 90 M entities materialized only from event log on demand = **0 RAM**.
- Event log on SSD for cold tier: 90 M × (say) 500 B/key × (say) 100 events/key = ~4.5 TB on SSD.

**Total RAM: ~10 GB for 100 M entities addressable.** The trick is that RAM holds only the 10% that matters at any given time.

### Latency

- **Hot reads:** unchanged (<50 µs p99).
- **Warm reads:** +10 µs for zstd decompression into a fresh `EntityState`. ~60 µs p99 total.
- **Cold reads:** +50 µs (NVMe read of event log segment) + ~100 µs (replay 1000 events through operators) ≈ 200 µs. **Breaks the <100µs budget for cold reads.**

This is the key tradeoff: **the budget is only preserved for hot traffic.** Cold traffic is explicitly a 2–5× slowdown. Acceptable if cold traffic is a small % of requests.

### Prerequisites

- v1.3 sharding (per-shard partial state, per-shard event log — already planned via `events/shard-N/stream.log`).
- Event log replay infrastructure (shipped in v1.1 as the backfill path).
- A new "evict to warm tier" code path and a "hydrate from warm tier" read path.

### Risks

- **Eviction policy correctness.** LRU is the obvious default; fraud workloads with bursty adversarial keys may need ARC or LFU. Must benchmark.
- **Thundering-herd cold reads.** If a scan over 100 M keys issues 100 M cold reads, the event-log replay cost dominates. Need backpressure or "reject with fallback" semantics.
- **Staleness contract.** An evicted key that received async pushes since eviction has those events in the event log — cold read replay picks them up. But a fire-and-forget push to a cold key might be silently dropped if the event log append is batched with other shards. Need explicit ordering contract.

### User-facing unlock

- **100M-entity addressable space**, not just hot capacity.
- **"Long-tail keys are free"** — users can have billions of entity keys across time without paying RAM cost for ones that go cold.
- **Degraded but serviceable cold reads** — customers must accept that reads on cold keys are slower. This is a product decision.

---

## C3. Zstd Dictionary + Gorilla Compression + Incremental Snapshots

**The "10× smaller snapshots, faster recovery, S3-tier feasible" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Zstd dictionary training + compression | §4.4 | Trained on sampled EntityState, 10× ratio on ~1 KB records |
| Gorilla XOR float compression | §8.5 | Per-operator bucket history compression, 4–8× on typical time-series |
| Incremental delta snapshots | v1.1 OPS-03/04 | Already shipped — only writes changed keys |

### Why this is a combination

- **Incremental snapshots win on frequency**, Gorilla wins on per-operator size, zstd wins on inter-record redundancy. Stacking them compounds.
- **Gorilla is a column-level codec** applied to the `Vec<f64>` of bucket values within a single operator. Zstd is a record-level codec applied to the postcard-serialized EntityState. They compose: Gorilla first (column-level ratio), zstd second (entity-level dedup of stream names, operator names, structure boilerplate).
- Result: snapshot on-disk size **8–12× smaller** than v1.2.

### Memory / disk math

Per-entity v1.2 snapshot entry (postcard): ~2 KB uncompressed.
- Gorilla on bucket histories: 2 KB → ~500 B.
- Zstd dict on the record: 500 B → ~50–80 B.
- **~25× on-disk reduction per entity.**

At 100 M entities: 200 GB snapshot → **~8 GB**.
At 1 M entities (more typical v1.4 target): 2 GB → **~80 MB**.

### Latency

- **Snapshot write:** faster (less IO). No hot-path cost.
- **Snapshot recovery:** slower per-entry (decompression), but less IO. Net: roughly the same wall-clock for the same state size, or faster if IO-bound.
- **Hot path:** zero impact — compression happens only at snapshot time, Gorilla only on the background bucket-flush path.

### Prerequisites

- v1.3 Phase 15 off-thread snapshot I/O (so compression cost doesn't stall the main thread).
- Zstd dictionary training pipeline — takes a sample of serialized entities on first snapshot or at install time, ships a binary dictionary with the snapshot format.
- Gorilla encoder for `RingBuffer<f64>`.

### Risks

- **Dictionary schema drift.** When pipeline definitions change (new operators, renamed streams), the dictionary becomes less effective. Mitigation: rebuild dictionary periodically, key dictionary version in snapshot header.
- **Zstd decompression speed** — ~500 MB/s for dict-decoded small records. At 1 M entities × 80 bytes each = 80 MB → 0.16 s to decode. Fine for recovery, not for hot path (don't compress live state).
- **Recovery CPU.** Decompressing 100 M zstd records serially is CPU-bound; needs parallelism. Post-v1.3 sharding, each shard decompresses its own segment — parallel by construction.

### User-facing unlock

- **Faster startup** (smaller snapshots = less disk IO).
- **S3 snapshot tier** becomes practical (10× less egress/storage cost).
- **Longer snapshot history** — store more cycles on the same disk budget.

---

## C4. Decayed Windows + DABA + Space-Saving Top-K

**The "new operator surface for fraud and recency-weighted ML" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Exponentially decayed counters/sums | §5.2 | O(1) state, recency-weighted features |
| DABA O(1) SWAG | §5.3 | Constant-time min/max on any window |
| Space-Saving Top-K | §1.4 | Heavy-hitter tracking per key |

### Why this is a combination

- These are three **new operators** that each unlock features Tally cannot express today. They share an implementation theme: "replace per-bucket scanning with a bounded state."
- Together they cover the main classes of feature requests that don't fit the current operator set: **recency weighting (decayed), fast min/max on large windows (DABA), and top-K heavy hitters (Space-Saving).**
- Implementation-wise they share bookkeeping — all three need per-key bounded state that's independent of the window size. A common `BoundedOperatorState` trait unifies them.

### Memory math

- **Decayed counter:** 8 bytes per operator per key (one f64). Vs 11,520 bytes for a 24h / 7.5s ring buffer. **1440×.**
- **DABA min/max:** ~3× the minimum state for the aggregate = ~24 bytes. Vs 11,520 bytes for the ring buffer. **480×.**
- **Space-Saving top-20:** ~500 bytes (20 slots × 24 bytes). New operator — no baseline.

Per entity with 5 operators replaced: **~50 KB → ~200 bytes. 250× reduction** on those specific operators.

### Latency

- **Decayed counter push:** 1 mul + 1 add + 1 store = ~5 ns. **20× faster than ring-buffer push.**
- **DABA push:** ~50 ns worst case. Comparable to ring buffer.
- **DABA read:** O(1) = ~5 ns. **~1000× faster than 1440-bucket sum.**
- **Space-Saving push:** O(1) expected, O(k) worst case = ~100 ns. New cost — no baseline.
- **Space-Saving read:** O(k log k) for sorted output = ~500 ns for k=20.

**Net: significant hot-path speedup on large-window operators.**

### Prerequisites

- Operator-internal refactor to support a `BoundedOperatorState` trait.
- Python SDK additions: `st.count_decayed`, `st.max_daba`, `st.top_k`.
- Snapshot format support for new operator variants.

### Risks

- **User confusion on decay semantics.** "Recency-weighted count" is not the same as "count in window." Docs must be unambiguous.
- **DABA implementation complexity.** VLDB 2019 algorithm is non-trivial. Loom-testable but subtle. Medium implementation risk.
- **Space-Saving accuracy under adversarial traffic.** Guarantees depend on the skew of the input; degenerate uniform distributions make it useless.

### User-facing unlock

- **Recency-weighted features** (the natural ML shape).
- **Cheap max/min on long windows.**
- **"Top 5 merchants for this user last 30 days"** — the #1 missing feature for fraud-detection users today.

---

## C5. Tuple/Theta Sketches + MinHash LSH + RoaringBitmap

**The "cohort analytics and entity similarity" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Theta sketch | §1.1 | Set operations (union, intersection, difference) with cardinality estimates |
| MinHash LSH | §6.1 | Jaccard similarity between entity sets |
| RoaringBitmap | §2.1 | Dense entity-ID sets for cohort membership |

### Why this is a combination

- **Theta + MinHash answer different questions about the same sets.** Theta: "how many distinct X does user Y have; how many does cohort C have; what's the intersection size." MinHash: "which users have similar sets of X." Together, they give a full set-analytics layer.
- **RoaringBitmap is the storage format.** Theta and MinHash consume membership → bitmap populate is the input stage. Bitmap intersections are the dense-side alternative when Theta's error is too big.
- Cross-entity features fall out: "users in cohort A AND cohort B", "users whose merchant set is >80% similar to user X", "top-5 users most similar to a given template."

### Memory math

- **Theta sketch** at ~30× HLL for same accuracy: ~500 KB per per-entity set if used at full precision. **Only practical at the stream or cohort level, not per-entity.**
- **MinHash** at 50×1-bit = **~6 bytes per entity per similarity feature.** Cheap.
- **RoaringBitmap** for a cohort: ~2 MB for a sparse 1M-entity-out-of-1B-keyspace cohort. **Per cohort, not per entity.**

### Latency

- Theta merge: O(k) where k is the sketch size, ~500 ns for k=512.
- MinHash Jaccard estimate: 50-bit XOR + popcount = ~5 ns. Effectively free.
- Roaring intersection on 1M-bit bitmaps: ~1 ms. Fine for HTTP API, too slow for hot path.

### Prerequisites

- String interning (§2.5) for dense entity IDs (required by Roaring).
- A new "cohort" primitive in the Python SDK and REGISTER protocol.
- A new read path for set-shaped features.

### Risks

- **Theta accuracy on small intersections.** Documented weakness — the relative error on a tiny intersection of two large sets is huge. Mitigation: fall back to exact Roaring intersection when Theta's confidence interval is too wide.
- **Rust DataSketches port status.** Theta sketch's Rust crate is experimental; a production port is a real cost.

### User-facing unlock

- **Cohort intersection features:** "is this user in the high-value cohort AND the fraud-flagged cohort."
- **Entity similarity features:** "users most similar to this user by merchant set."
- **Global cohort analytics:** "how many users overlap between cohort A and cohort B."

A class of features Tally fundamentally cannot express today.

---

## C6. Global Shared Sketches + Count-Based Windows + Shared Operators

**The "low-hanging fruit that no one does" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Global / stream-level shared sketches | §6.4 | One sketch for the whole server, not per-entity |
| Count-based windows | §5.4 | "Last N events" instead of "last T seconds" |
| Shared operator state across entities | new | A single operator that aggregates over all keys |

### Why this is a combination

- Many useful features are **not per-entity**. "Top 10 merchants by global volume last hour" is one sketch, not N. Today Tally forces users to hack it via derive expressions over per-entity state.
- **Count-based windows dovetail with global sketches** because global-sketch size is bounded by sketch parameters, not entity count.
- Shared state is almost **free** — one HLL for the whole server is 16 KB, not 16 KB × N entities.

### Memory math

**Baseline:** N/A (feature doesn't exist; users emulate it by scanning N entities).
**With C6:** constant per global feature, regardless of entity count. **~1 KB per shared sketch.**

### Latency

- Shared sketch push: +~100 ns per event (one shared-sketch update per event).
- Global feature read: O(1) HTTP API call, ~10 µs including network.

### Prerequisites

- A new stream decorator: `@st.global` or `@st.stream(shared=True)`.
- A new read path — shared features are not per-entity, so GET semantics differ.
- Minimal REGISTER extensions.

### Risks

- **Low** — this is a clean feature addition with no interaction with existing invariants.
- Shared-write contention under sharding: one shared sketch would be contended across shards. Mitigation: per-shard local sketch + periodic merge, just like Tally's throughput counter (v1.3 C-6 prevention).

### User-facing unlock

- **Global top-K merchants by volume.**
- **Global distinct entity count** (today impossible without scanning).
- **"Server-wide active-in-last-N"** metrics.

A strict superset of today's featureset for a tiny implementation cost. **Should be on the v1.4 roadmap by default.**

---

## C7. Per-Shard Learned Index + Binary Fuse Negative Filter

**The "faster lookups and faster misses" stack.**

### Components

| Component | Section | Role |
|---|---|---|
| Per-shard learned index over interned IDs | §3.1 | Replaces SwissTable lookup with model+array for known keys |
| Binary Fuse filter | §1.7 | O(1) "definitely not present" reject for unknown keys |
| String interning | §2.5 | Gives dense u64 IDs the learned index needs |

### Why this is a combination

- Learned indexes need **sorted dense numeric keys.** Interning provides them.
- Learned indexes are slow on miss-heavy workloads — hence pair with **Binary Fuse for negative short-circuit.**
- Together: **known-key lookup is ~20–50 ns faster** than SwissTable (learned index resolution fits in 1–2 cache lines), **unknown-key lookup is ~200 ns faster** (filter reject vs HashMap walk).

### Memory math

- Learned index: ~1–2 KB per shard for the model + coefficients. Constant.
- Binary Fuse filter: ~9 bits per key. At 10 M keys: ~11 MB. Per shard: 11 MB / num_shards.

Compared to SwissTable, learned index can be **~10–20% smaller** on the metadata side (no control bytes, no empty slots). **Net memory win: modest ~10–20%.**

### Latency

- **Known-key hit:** ~−20–50 ns vs SwissTable (learned index is 1–2 array fetches).
- **Unknown-key miss:** Binary Fuse reject in ~40 ns (vs ~200 ns of probing + miss handling on SwissTable).

### Prerequisites

- §2.5 interning complete.
- Learned index Rust port — no production crate exists; `learnedsystems/RadixSpline` is C++.
- Model retraining policy (when to rebuild as new keys arrive).

### Risks

- **Model staleness.** New keys between retrainings hit a backup structure. Needs a clean fallback path.
- **Cold start.** Empty store has no model. Use SwissTable until enough keys exist to train.
- **Complexity vs gain** — 1.2× memory and 10–20% lookup speedup is not a headline win. **This combination is likely not worth pursuing alone** unless the interning and filter pieces are already being built for other combinations.

### Verdict

**Low priority.** Listed for completeness. If C1 and C2 land, the incremental value of C7 is small.

---

## C8. CXL DRAM Tier + Partial State + Compressed Warm Tier

**The "2027+ density ceiling" speculative stack.**

### Components

| Component | Section | Role |
|---|---|---|
| CXL-attached DRAM as a slow memory tier | §4.5 | 2–4 TB per node at ~300 ns access (vs ~80 ns local DRAM) |
| Partial state (Noria-style) | §4.2 | Hot local, warm CXL, cold event-log replay |
| Zstd-compressed in-CXL warm tier | §4.4 | 10× compression on the CXL-resident warm tier |

### Why this is a combination

- **CXL alone gives density but loses ~3× latency.** For Tally's <100 µs p99 that's survivable for individual accesses but catastrophic if the hot path makes many accesses.
- **Partial state isolates the hot path from the cold path** — the hot ~1 % lives on local DRAM at full latency; the rest lives on CXL.
- **Compression on the warm-CXL tier** makes the density advantage *orders of magnitude* larger than CXL alone.

### Memory math

- Local DRAM: 64–256 GB, stores hot 1 M entities at full precision = ~5 GB. Headroom.
- CXL tier: 1–4 TB, stores compressed warm entities at 500 B each = 2–8 billion entities. **Density ceiling: ~8 B entities per node.**

Compare to v1.2: ~1 M hot entities per node. **Improvement: ~8000×.**

### Latency

- **Hot (local DRAM) reads:** unchanged, <50 µs p99.
- **Warm (CXL + zstd) reads:** CXL access ~300 ns × (~10 accesses to read the struct) + decompress ~1 µs ≈ ~5 µs. Well within budget.
- **Cold (event log replay) reads:** ~200 µs, breaks budget.

### Prerequisites

- Hardware: CXL 2.0+ server (mainstream ~2026–2027).
- Linux kernel with TPP or libnuma-aware allocator.
- §4.2 partial state.
- §4.4 zstd dictionary warm tier.
- Custom memory allocator that places the warm tier on the CXL NUMA node.

### Risks

- **Hardware dependency is anti-philosophy.** Tally's "single binary, runs anywhere" story doesn't love a CXL requirement. Would need to gate the feature behind a runtime detect + fall back to pure-RAM mode.
- **Unproven at scale.** CXL tiering has research implementations (HybridTier ASPLOS 2025) but no production feature-server deployment precedent.

### Verdict

**v3 speculative.** Worth tracking as the "ceiling of what single-node density can be by 2028." **Not a build-for-v1.4 target.**

---

## Cross-combination synergy notes

- **C1 is the enabler for C2 and C3.** Without interning, partial state can't dedupe cold-tier storage effectively, and zstd dictionaries can't compress redundant schema fields out.
- **C4 is independent but composes with everything.** Decayed windows + partial state = truly O(1) per-entity state in every operator.
- **C6 is free and composes with nothing in particular — just do it.**
- **C7 and C8 depend on C1** for the dense-ID substrate.
- **C5 is orthogonal.** It's about *new features*, not memory reduction — fits any cycle.

---

## Suggested build order (prerequisite chain)

```
C6 (shared sketches) ────────────────────────────────────┐
                                                         │
C1 (sparse HLL + exp hist + interning) ──┬── v1.4/v2 ──┤
                                          │             │
C3 (zstd snapshots + Gorilla cols) ──────┘             │
                                                         │
C4 (new operators: decayed, DABA, top-K) ── v1.4/v2 ────┤
                                                         │
C2 (partial state + event log replay) ──── v2 ──────────┤
                                                         │
C5 (tuple/theta + MinHash) ─────────────── v2 ──────────┤
                                                         │
C7 (learned index) ──────────────────────── v3? ────────┤
                                                         │
C8 (CXL tier) ────────────────────────────── v3? ───────┘
```

Priority order for v1.4: **C6 → C1 (sparse HLL first, then interning, then exponential histograms) → C4 → C3.**

C2, C5 are v2 architectural changes. C7, C8 are v3 speculation.

---

*Compiled 2026-04-11. All memory/latency numbers cite their source in `HORIZON-SURVEY.md`; this document synthesizes across citations without adding new ones.*
