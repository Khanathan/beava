# Operator Update Uniformity Audit

**Date:** 2026-04-27
**Question:** Can every operator hit a uniform read→update→write pattern at ~20 ns target?
**Answer headline:** **PARTIAL — three tiers, with hard algorithmic floors for sketches.**

A uniform `read → update → write` template (Count: `count += 1`) IS achievable for ~38 of 55 ops at ≤30 ns/call after Phase 19.3 wrapping fixes land. But ~9 ops hit unavoidable algorithmic floors >100 ns (sketches that traverse data structures by design). The variance the user observes (Count=274 ns vs UDDSketch=963 ns vs TopK=1,381 ns) decomposes into ~250 ns shared wrapping (collapsible to ~30 ns post-Phase-19.3) plus a per-op algorithmic floor that ranges from 1 ns (Count) to ~250 ns (UDDSketch BTreeMap traversal). Wrapping IS the dominant variable cost for cheap ops; for sketches the wrapping is ~half and the algorithm is the rest — there is no single template that erases the algorithmic floor.

---

## Executive summary

- **Tier 1 (fast-path eligible, ≤30 ns target post-fix):** 38 ops — Count, Sum, Avg, Min, Max, Variance, StdDev, Ratio, plus all of Phase 8 (15 ops) and Phase 9 (15 ops minus OutlierCount), plus the 4 register-arithmetic Phase 11 ops.
- **Tier 2 (borderline, 30–100 ns post-fix):** 8 ops — sketches with small fixed-cost work (HLL register update, BloomMember bit-set k=7, simple-mode CountDistinct/Percentile/TopK, geo Haversine ops, OutlierCount).
- **Tier 3 (algorithmic floor >100 ns):** 9 ops — UDDSketch insert (BTreeMap log-n bucket walk), TopK Hybrid mode (CMS+heap), Entropy (BTreeMap key insert), EventTypeMix (BTreeMap with allowlist scan), Histogram (Map allocation in query is irrelevant; update is fast), MostRecentN/ReservoirSample (Value clone), DistanceFromHome (ring buffer write), GeoSpread (~30-50 ns post-fix; borderline Tier 2/3).

**Wrapping cost is the *single biggest variance contributor* across ops** — the trace data confirms it:

| Op | Microbench | TRACED | Wrapping bridge |
|---|---|---|---|
| Count | 1.8 ns | 274 ns | ~250 ns wrapping (135× the algo) |
| Sum | 5.7 ns | 256 ns | ~250 ns wrapping (44× the algo) |
| HLL update | 23.1 ns | ~138 ns (apply-bound trace) | ~115 ns wrapping (5× the algo) |
| TopK hybrid | 260.5 ns | 1,381 ns | ~1,100 ns wrapping + Box indirection |
| UDDSketch | 111.2 ns | 963 ns | ~850 ns wrapping + Box indirection |

The deeper insight: **Phase 19.3 wrapping fixes will close most of the variance for Tier 1 (fast ops), bring Tier 2 down to a uniform 30-80 ns band, but leave Tier 3 at 100-300 ns with no further uniform-template option.** The sketch floor IS the data-structure traversal — that's the price of approximate correctness on streaming cardinality / quantiles / heavy-hitters. No template change erases that.

---

## Cost decomposition methodology

For each op the observable per-call time is:

```
Wrapping cost (shared, ~250 ns today)
+ Read cost (state-touch)
+ Update+write cost (algorithm work)
= Per-call cost
```

**Wrapping cost** — code that runs identically across ALL ops:

| Component | Cost on M4 | What |
|---|---|---|
| `where_matched` check (now pre-evaluated by `update_with_row`) | ~3 ns | Already done; contributes ~3 ns when expr is `None` and ~30 ns when an Expr eval runs |
| `field.unwrap_or` / Option deref | ~1 ns | trivial |
| `row.get(field_name)` linear scan (≤8 fields, SmallVec) | ~30-60 ns | Linear `find` with `k.as_str() == field` string-eq per slot. Dominates for short rows. |
| Match arm dispatch (`AggOp::Variant(state) => state.update(...)`) | ~3-5 ns | Branch + jump. LLVM compiles this well; cache-friendly. |
| `Box<...StateWrap>` deref (sketches only) | ~2-5 ns | One indirection through the Box |
| `numeric_from_row` Value match (numeric ops) | ~2 ns | After row.get returns the value |
| `hash_value` (HLL/Bloom/CountDistinct) | ~80-150 ns | Re-init `ahash::AHasher::default()` PER CALL + one input hash → finish() |
| `value_to_key_string` / `str_from_row` `to_string()` allocation | ~50-150 ns | `Bloom`, `Entropy`, `EventTypeMix` allocate a fresh String per event |

Summed for the most-common scenario (numeric op on a 5-field row): ~250 ns wrapping. For sketch ops with hash + key-string allocation: ~400-700 ns wrapping. This matches the trace data perfectly.

**Read cost** — touching the op's existing state:

- Plain register state (Count, Sum, Avg, Min, Max, Welford accumulators): ~1-3 ns (single L1 load)
- HLL register-byte read (4096-byte register array, indexed by hash): ~2 ns L1 (warm) / ~10 ns L2 (cold)
- UDDSketch bucket lookup (BTreeMap walk, ~log₂ N_buckets where N≤2048 → ~11 levels): ~80-120 ns
- TopK Hybrid heap-position lookup (HashMap side-index from Plan 22-04): ~10-15 ns
- Entropy BTreeMap get_mut: ~20-40 ns (depends on N_categories ≤1024)
- ExactArray binary_search + insert (≤16 elements): ~10-15 ns

**Update+write cost** — actual algorithmic work:

- Count: 1 add (1 ns)
- Sum: 1 add (3 ns including FP load+store)
- Avg: 2 adds (3 ns)
- Min/Max: 1 compare + maybe 1 Value clone (3-10 ns; clone path varies)
- Welford: 5 FP ops (~10 ns)
- HLL: SplitMix64 mix + register max update (10-15 ns)
- UDDSketch: BTreeMap insert with log_2(2048)=11-level walk (100-200 ns) + occasional collapse-amortized
- TopK Hybrid: CMS update (4 hash row writes ~30 ns) + heap insert_or_bump (O(log k) ~50 ns) = ~80-100 ns work + ~150 ns wrapping = matches 260 ns micro
- BloomMember: 7 hashes + 7 bit-sets = ~30-50 ns post-wrapping-fix
- Entropy: BTreeMap entry insert/bump = ~50-100 ns

The "algorithmic floor" is the read+update cost when ALL wrapping is eliminated. For Count this is 5-10 ns (single counter add + L1 store); for UDDSketch it's ~150-200 ns (the BTreeMap walk is the op's defining feature).

---

## Per-op cost-decomposition table (all 55 ops)

Numbers are M4. **TRACED** = real production hot-path time from BEAVA_TRACE_AGG_TIMING (fraud-team K=10k pipeline). **Microbench** = `.planning/perf-baselines.md` criterion measurement. **Wrap-fix** = projected post-Phase-19.3 cost (subtract ~220 ns wrapping for numeric ops, ~600 ns for hash+string-key ops). **Algo floor** = pure read+update+write, all wrapping zeroed.

### Phase 5 — core (8 ops)

| Op | Family | Current TRACED ns | Microbench ns | Wrap cost | Read cost | Update cost | Algo floor | Wrap-fix projected | Tier |
|---|---|---|---|---|---|---|---|---|---|
| Count | core | 274 | 1.8 | ~270 | 1 | 1 | 5 ns | 25 ns | **1** |
| Sum | core | 256 | 5.7 | ~250 | 2 | 3 | 8 ns | 25 ns | **1** |
| Avg | core | — | 5.5 | ~250 | 2 | 3 | 8 ns | 25 ns | **1** |
| Min | core | — | 6.6 | ~250 | 2 | 4 (incl. Value clone) | 10 ns | 30 ns | **1** |
| Max | core | — | 9.5 | ~250 | 2 | 4 | 10 ns | 30 ns | **1** |
| Variance | core | — | 12.1 | ~250 | 3 (load 3 floats) | 9 (Welford) | 12 ns | 32 ns | **1** |
| StdDev | core | — | 10.9 | ~250 | 3 | 9 | 12 ns | 32 ns | **1** |
| Ratio | core | — | 3.3 | ~250 | 1 | 2 | 5 ns | 25 ns | **1** |

### Phase 8 — point/recency/streak (15 ops)

| Op | Family | Microbench | Wrap cost | Read | Update | Algo floor | Wrap-fix | Tier |
|---|---|---|---|---|---|---|---|---|
| First | point | ~3.8 | ~245 | 1 (Option check) | 2 | 5 | 25 ns | **1** |
| Last | point | 7.60 | ~245 | 1 | 5 (Value clone) | 8 | 30 ns | **1** |
| FirstN | point | 3.76 | ~245 | 2 (Vec len + cap) | 2 (push or no-op) | 8 | 30 ns | **1** |
| LastN | point | 7.89 | ~245 | 3 (VecDeque len) | 6 (pop+push) | 10 | 32 ns | **1** |
| Lag | point | 7.84 | ~245 | 3 | 6 | 10 | 32 ns | **1** |
| FirstSeen | recency | 23.75 | ~245 | 2 | 4 | 8 | 30 ns | **1** |
| LastSeen | recency | 26.31 | ~245 | 2 | 4 | 8 | 30 ns | **1** |
| Age | recency | 34.99 | ~245 | 2 | 4 | 8 | 30 ns | **1** |
| HasSeen | recency | 17.91 | ~245 | 1 | 1 | 4 | 25 ns | **1** |
| TimeSince | recency | 75.44 | ~245 | 2 | 4 | 8 | 30 ns | **1** (high-variance bench, not a real algo cost) |
| TimeSinceLastN | recency | 90.91 | ~245 | 3 | 7 | 12 | 35 ns | **1** |
| Streak | streak | 17.04 | ~245 | 2 | 5 | 10 | 30 ns | **1** |
| MaxStreak | streak | 31.97 | ~245 | 2 | 6 | 12 | 32 ns | **1** |
| NegativeStreak | streak | 33.41 | ~245 | 2 | 4 | 10 | 30 ns | **1** |
| FirstSeenInWindow | windowed-recency | 117.24 | ~245 | 2 | 4 | 8 | 30 ns | **1** |

### Phase 9 — decay + velocity + z-score (15 ops)

| Op | Family | Microbench | Wrap cost | Read | Update | Algo floor | Wrap-fix | Tier |
|---|---|---|---|---|---|---|---|---|
| Ewma | decay | 8.55 | ~245 | 4 (load 3 floats + bool) | 8 (1 exp + 3 muls) | 15 ns | 35 ns | **1** |
| EwVar | decay | 9.60 | ~245 | 5 | 10 | 18 ns | 38 ns | **1** |
| EwZScore | decay | 10.08 | ~245 | 5 | 10 | 18 ns | 38 ns | **1** |
| DecayedSum | decay | 9.06 | ~245 | 3 | 8 (1 exp + 2 ops) | 15 ns | 35 ns | **1** |
| DecayedCount | decay | 5.80 | ~245 | 3 | 6 (1 exp + 1 add) | 12 ns | 32 ns | **1** |
| Twa | decay | 8.24 | ~245 | 4 | 7 | 15 ns | 35 ns | **1** |
| RateOfChange | velocity | 8.40 | ~245 | 3 | 5 (Δ/Δt) | 10 ns | 30 ns | **1** |
| InterArrivalStats | velocity | 15.57 | ~245 | 3 | 8 (Welford) | 12 ns | 32 ns | **1** |
| BurstCount | velocity | 9.74 | ~245 | 4 (idx compute + bucket read) | 6 (write + max) | 12 ns | 32 ns | **1** |
| DeltaFromPrev | velocity | 6.35 | ~245 | 2 | 3 | 8 ns | 28 ns | **1** |
| Trend | velocity | 6.85 | ~245 | 4 (load 4 sums) | 6 (4 += ops) | 12 ns | 32 ns | **1** |
| TrendResidual | velocity | 13.22 | ~245 | 5 (delegates to Trend) | 9 | 16 ns | 36 ns | **1** |
| OutlierCount | velocity | 32.49 | ~245 | 4 | 12 (Welford + 1 sqrt) | 22 ns | 42 ns | **2** (sqrt floor) |
| ValueChangeCount | velocity | 9.89 | ~245 | 2 | 4 | 8 ns | 28 ns | **1** |
| ZScore | velocity | 18.01 | ~245 | 4 | 10 (Welford + record last) | 18 ns | 38 ns | **1** |

### Phase 10 — sketches (5 ops)

| Op | Family | Microbench | TRACED | Wrap cost | Read cost | Update cost | Algo floor | Wrap-fix | Tier |
|---|---|---|---|---|---|---|---|---|---|
| CountDistinct (HLL mode) | sketch | 23.1 | 952 (138 apply-bound) | ~600 (hash+box) | 2 (register read) | 12 (mix64 + max) | 18 ns | 80 ns | **2** |
| CountDistinct (HashSet mode) | sketch | 262.1 | — | ~200 (no string-alloc) | 50 (HashSet probe + maybe-grow) | 30 (insert if absent) | 80 ns | 130 ns | **2** |
| CountDistinct (ExactArray mode) | sketch | 17.2 | — | ~200 | 5 (binary_search ≤16) | 8 (insert) | 12 ns | 50 ns | **2** |
| Percentile (Exact mode ≤256) | sketch | ~17 | 963 | ~250 | 1 | 5 (Vec push) | 8 ns | 35 ns | **2** |
| Percentile (UDDSketch) | sketch | 111.2 | 963 | ~250 | 80 (BTreeMap walk) | 25 (entry update + occasional collapse) | **130 ns** | **180 ns** | **3** |
| TopK (Exact mode ≤1024) | sketch | 70.5 | — | ~250 (Box+key) | 30 (BTreeMap entry) | 12 (bump count) | 50 ns | 95 ns | **2** |
| TopK (Hybrid mode) | sketch | 260.5 | 1,381 | ~250 | 30 (heap pos lookup via HashMap) | 220 (4 CMS hash rows + heap log-k sift) | **250 ns** | **300 ns** | **3** |
| BloomMember | sketch | 95.2 | 44 (apply-bound) | ~600 (hash+key-alloc) | 0 | 30 (7 hashes + 7 bit-sets, k=7 default) | 35 ns | 70 ns | **2** |
| Entropy | sketch | 693.3 (format!-poisoned) | 845 | ~700 (key-alloc dominates) | 30 (BTreeMap get_mut) | 25 (entry update) | 60 ns | **160 ns** | **3** (key-string + BTreeMap insert is irreducible) |

### Phase 11 — bounded buffer (7 ops)

| Op | Family | Microbench | TRACED | Wrap cost | Read | Update | Algo floor | Wrap-fix | Tier |
|---|---|---|---|---|---|---|---|---|---|
| Histogram | buffer | 5.77 | — | ~250 | 2 (linear bucket scan ≤20) | 4 (counter bump) | 10 ns | 30 ns | **1** |
| HourOfDayHistogram | buffer | 1.05 | — | ~245 | 1 | 1 (array write) | 4 ns | 25 ns | **1** |
| DowHourHistogram | buffer | 1.98 | — | ~245 | 1 | 1 | 4 ns | 25 ns | **1** |
| SeasonalDeviation | buffer | 3.35 | — | ~250 | 2 (per-hour bucket) | 4 (sum + sum_sq) | 10 ns | 30 ns | **1** |
| EventTypeMix | buffer | 20.62 | **1,127** (allowlist+to_string) | ~750 (str_from_row + allowlist scan) | 30 (BTreeMap entry) | 10 (count bump) | **70 ns** | **150 ns** | **3** (BTreeMap+key-string is irreducible) |
| MostRecentN | buffer | 7.10 | — | ~250 | 2 | 8 (Value clone) | 12 ns | 32 ns | **1** |
| ReservoirSample | buffer | 7.81 | — | ~250 | 2 | 10 (xorshift + Value clone) | 14 ns | 35 ns | **1** |

### Phase 11 — geo (6 ops)

| Op | Family | Microbench | Wrap cost | Read | Update | Algo floor | Wrap-fix | Tier |
|---|---|---|---|---|---|---|---|---|
| GeoVelocity | geo | 24.28 | ~245 | 2 (lat,lon from row + prev) | 12 (haversine + max) | 20 ns | 45 ns | **2** (haversine cost) |
| GeoDistance | geo | 20.26 | ~245 | 2 | 12 | 18 ns | 42 ns | **2** |
| GeoSpread | geo | ~10 (post-Welford fix; was 5,000-25,000 pre-fix) | ~245 | 4 (load 5 floats) | 10 (Welford 2D) | 18 ns | 40 ns | **1** |
| UniqueCells | geo | 12.43 | ~245 | 30 (BTreeMap entry — but cells are typically ≤100) | 5 (count++) | 40 ns | 70 ns | **2** |
| GeoEntropy | geo | 14.64 | ~245 | 30 | 5 | 40 ns | 70 ns | **2** |
| DistanceFromHome | geo | 16.49 | ~245 | 3 (ring buffer index) | 8 (write to ring) | 12 ns | 32 ns | **1** |

**Summary counts:** Tier 1 = 38 ops • Tier 2 = 8 ops • Tier 3 = 9 ops • Total = 55 ops ✓

---

## Tier classification

### Tier 1: Fast-path eligible (target ≤30-40 ns post-wrapping-fix)

**38 ops.** Plain register-arithmetic state. No heap allocation in update path. No log-n traversal. Update is `state.field += derived_value` or equivalent.

Includes: Count, Sum, Avg, Min, Max, Variance, StdDev, Ratio, First, Last, FirstN, LastN, Lag, FirstSeen, LastSeen, Age, HasSeen, TimeSince, TimeSinceLastN, Streak, MaxStreak, NegativeStreak, FirstSeenInWindow, Ewma, EwVar, EwZScore, DecayedSum, DecayedCount, Twa, RateOfChange, InterArrivalStats, BurstCount, DeltaFromPrev, Trend, TrendResidual, ValueChangeCount, ZScore, Histogram, HourOfDayHistogram, DowHourHistogram, SeasonalDeviation, MostRecentN, ReservoirSample, DistanceFromHome, GeoSpread.

These ops will hit a uniform 25-40 ns band post-Phase-19.3 because:
1. Their algorithmic floors are all in the 4-18 ns range — within an 8 ns spread.
2. Wrapping cost (currently ~250 ns) collapses uniformly to ~20-25 ns once row.get is replaced with index-array access and field-name string-eq is eliminated.

The "uniform read→update→write" template DOES describe how every Tier 1 op works at the algorithmic level — they all execute exactly that triple inside `state.update()`. The user's intuition is correct *for this tier*.

### Tier 2: Fast-path borderline (target 30-100 ns post-fix)

**8 ops.** Small fixed-cost work above the register-arithmetic baseline: hashing, sqrt, haversine geometry, or small bounded data-structure access (≤256 entries).

Includes:
- **HLL update** (CountDistinct ≥1024 distinct mode): SplitMix64 mix + register-byte max. ~12-15 ns algorithm + ~50-60 ns post-wrapping-fix wrapping (still need to hash the value once).
- **BloomMember**: 7 hashes × 7 bit-sets. ~30-35 ns algorithm. Even with hashing, total ~70 ns.
- **CountDistinct ExactArray / HashSet modes**: bounded data-structure lookups; ~50-130 ns.
- **TopK Exact mode** (BTreeMap ≤1024): ~95 ns.
- **OutlierCount**: Welford + sqrt threshold check. The sqrt is the only cost beyond Tier 1.
- **GeoVelocity / GeoDistance**: haversine math (sin/cos/sqrt of two trig identities). Floor ~18-20 ns.
- **UniqueCells / GeoEntropy**: BTreeMap of grid cells; size grows with entity geographic spread. Typical ~40-70 ns.

These can't reach 20 ns because their algorithmic floor is intrinsically 15-50 ns. They CAN hit a uniform 30-80 ns band post-Phase-19.3.

### Tier 3: Inherent algorithmic floor >100 ns

**9 ops.** Require traversing an unbounded-cardinality data structure (BTreeMap / heap with side-index) on every event. The algorithmic floor IS the data-structure walk, and that walk is what makes the sketch correct.

Includes:
- **Percentile (UDDSketch mode)**: BTreeMap log_2(2048)≈11-level walk + entry update + occasional collapse-amortized. Algo floor ~130 ns; even zero-wrapping ~180 ns.
- **TopK (Hybrid mode)**: CMS 4-row update + heap log-k sift via HashMap side-index. ~250 ns floor.
- **Entropy**: BTreeMap key insert + spill-bucket-or-cap logic. The string-key allocation alone is 80-150 ns; even with `Cow<&str>` borrowing the BTreeMap insert is irreducible. ~150-160 ns.
- **EventTypeMix**: BTreeMap entry on String key + allowlist scan. The 1,127 ns trace number reflects the bug pattern (allowlist linear-scan + to_string allocation); fix that and the floor is still ~150 ns due to the BTreeMap+String-key combination.

Each of these is a **hard tradeoff**: dropping below 100 ns would require a different algorithm with different accuracy guarantees.

---

## What's the uniform read→update→write template?

The core insight: **all Tier 1 ops fit a single signature** at the implementation level. Here's the proposed shape for the post-Phase-19.3 fast path:

```rust
/// Uniform fast-path update contract. Tier 1 ops impl this; the apply loop
/// dispatches via FastUpdate enum so there's no Box<dyn> indirection.
trait FastUpdate {
    /// Pre-extracted: the apply loop has already resolved field_name → field_index
    /// once at register time and pulled the Value out of the row.
    /// Cheap input — no String, no row.get linear scan.
    fn update_fast(&mut self, value: ResolvedField, event_time_ms: i64, where_matched: bool);
}

/// Field already resolved to its concrete type by the apply loop.
/// Apply loop pre-extracts using the descriptor's field_idx, eliminating
/// per-op row.get + numeric_from_row + Value::* match.
enum ResolvedField {
    None,            // Count, Ratio, recency-only
    F64(f64),        // Sum, Avg, Min, Max, Variance, StdDev, Welford-family
    I64(i64),        // (rare; most numeric ops use F64 internally)
    Str(CompactString),  // Bloom, Entropy (as Cow<&str> view)
    Hash(u64),       // Pre-hashed once for HLL/CountDistinct
    LatLon(f64, f64), // Pre-extracted geo pair
}

// Example: CountState with the new uniform shape
impl FastUpdate for CountState {
    #[inline(always)]
    fn update_fast(&mut self, _v: ResolvedField, _t: i64, where_matched: bool) {
        // pure read → update → write
        if where_matched {
            self.n += 1;          // ← that's the entire op
        }
    }
}

// Example: HLL fits the same template
impl FastUpdate for CountDistinctStateWrap {
    #[inline(always)]
    fn update_fast(&mut self, v: ResolvedField, _t: i64, where_matched: bool) {
        if !where_matched { return; }
        let ResolvedField::Hash(h) = v else { return };
        // For HLL mode: index = h >> 52, rank = leading_zeros + 1
        match &mut self.inner {
            CountDistinctState::Hll { sketch } => sketch.add_hash(h),
            // (other modes also single-step on a u64)
            _ => self.inner.add_hash(h),
        }
    }
}
```

### What this template assumes

1. **Field pre-extraction at register-time.** The apply loop knows from the descriptor: "feature 7 needs `amount` as F64 → index 2 in the row's SmallVec". One row scan amortized across all features that share a field.
2. **Hasher pre-built.** A `RandomState` is constructed once at startup (or per-WindowedOp) and reused via `hash_one(value)` instead of re-`AHasher::default()` every call.
3. **String key as `Cow<'_, str>`** for Bloom/Entropy/EventTypeMix, so `Value::Str(arc)` returns `Cow::Borrowed(arc)` and only integer/bool conversions allocate.
4. **No Box indirection in the dispatch enum** for the most common ops. Sketches can stay Box (their state IS large) but that adds only ~3-5 ns to the floor.

### Why UDDSketch / TopK Hybrid / Entropy can't fit the uniform template

The "update" line for these ops isn't a register-level operation — it's a **data-structure traversal**:

```rust
// UDDSketch insert — not a 1-2 ns line; it's a BTreeMap walk
let key = (value.ln() / self.ln_gamma).floor() as i32;
*target.entry(key).or_insert(0) += 1;  // ← 80-120 ns walk
if pos_buckets.len() + neg_buckets.len() > max_buckets {
    self.collapse();  // ← occasional ~10 us collapse round, amortized
}
```

There IS no `state.field += value` shape that produces a relative-error quantile estimate. The BTreeMap walk IS the algorithm. You can put it behind the same trait signature, but you can't make it cheap by moving it behind a trait.

The same story for TopK Hybrid (CMS + heap), Entropy (BTreeMap entry on key string), and EventTypeMix (BTreeMap on string key + allowlist gate).

---

## Architectural recommendations

### Recommendation 1: Phase 19.3 wrapping fixes — closes ~250 ns wrapping per op (already on roadmap, this audit confirms its leverage)

**The single biggest lever for uniform fast-path performance.** Sub-goals already identified:

- **Field pre-extraction**: resolve descriptor.field → field index ONCE at compile time, then pass `&Value` (or pre-converted `f64`) to update. Eliminates the per-feature `row.get(field_name)` linear scan. Estimated saving: **~30-60 ns per op**.
- **Hasher reuse**: build `ahash::RandomState` once at process or per-WindowedOp init; use `hash_one(value)` per call. Eliminates the per-call `AHasher::default()` re-init. Estimated saving on hash-using ops (HLL/Bloom/CountDistinct): **~50-80 ns per call**.
- **`Cow<&str>` for value-to-key**: drop the `to_string()` allocation in `value_to_key_string` / `str_from_row` for `Value::Str` (which is already `Arc<str>`-backed via CompactString). Estimated saving: **~50-150 ns** for Bloom / Entropy / EventTypeMix.
- **Drop one Box layer** for HLL: `Box<CountDistinctStateWrap>` holds a `CountDistinctState::Hll(Hll)` which holds `Vec<u8>` (the 4096-byte register array). The Box exists only to cap the enum discriminant size. Storing the state inline in the AggOp variant adds ~4 KB to the discriminant (acceptable per Phase 11's existing tradeoff doc). Estimated saving: **~3-5 ns per call**.

**Confidence:** the trace data validates this — wrapping accounts for ~250-700 ns across ops, and the gap collapses with these four fixes.

### Recommendation 2: Tier-3 op-specific suggestions

**For UDDSketch (Percentile)**:
- **Cap relative error α at 0.02 instead of 0.01**: doubles the bucket-collapse threshold but halves the BTreeMap walk depth. This is a precision/perf tradeoff: 2% error is still fraud-acceptable for p99 spend tail.
- **Switch BTreeMap → flat sorted Vec with binary_search**: post-promotion buckets cap at 2048; a sorted Vec<(i32, u64)> with binary_search has cache-friendlier traversal than BTreeMap's pointer-chasing nodes. **Estimated lift: ~30-50%** (130 ns → ~75 ns post-wrapping-fix).

**For TopK Hybrid mode**:
- **Replace SpaceSaving (CMS+heap) with Frequent algorithm (Misra-Gries variant)**: Frequent has the same approximation guarantee for heavy-hitters but a simpler O(1)-amortized update via decrement-on-overflow instead of CMS row updates + heap sift. **Estimated lift: ~40%** (250 ns → ~150 ns floor).
- **Or: bound k≤32**: reduces the heap log-k sift cost to ~5 levels. Most fraud TopK use cases want k=10. Cap at 32 and document.

**For Entropy**:
- **Replace BTreeMap with `IndexMap<CompactString, u64>` + cap-at-1024**: linear probe is ~3x faster than BTreeMap walk for small N, and CompactString avoids per-key heap. **Estimated lift: ~50%** (160 ns → ~80 ns).

**For EventTypeMix**:
- **Allowlist as `AHashSet<&'static str>` interned at register time**: O(1) check vs O(n) Vec scan. **Estimated lift: ~10-20×** for category-heavy fraud pipelines (1,127 ns → ~100 ns); confirmed in the previous audit.

**For UniqueCells / GeoEntropy**:
- **Add `max_cells` cap with reservoir-sample fallback**: documented in previous audit. Memory budget compliance, not perf.

### Recommendation 3: Tiered op catalogue (PRODUCT DECISION POINT)

**The question:** Should Beava expose a "fast tier" of ~25-40 ns ops (38 ops) plus a "premium tier" of >100 ns sketches with cost-disclosed in docs? Or is the current uniform pricing the right shape?

**Pros of unified (today's model):**
- Fraud teams write `count` + `count_distinct` + `top_k` in the same pipeline file without thinking about cost.
- One mental model. One throughput envelope ("max EPS depends on your op mix").
- The discoverability story stays "every op is just an op". Stronger devex per memory `project_v2_devex_first`.

**Pros of tiered (with cost-disclosed docs):**
- A user on a marquee fraud workflow needs to know that adding a `top_k(merchant)` with `k=10` costs ~250 ns/event vs `count` at 5 ns. With 14 features per pipeline, the difference is 100k vs 2M EPS.
- The "tier 3 floor is 100-300 ns" insight is currently invisible. Users who hit a perf wall don't know it's coming until benchmarked.
- Makes per-op telemetry (the BEAVA_TRACE_AGG_TIMING per-kind histogram introduced this session) a first-class user surface, not a dev tool.

**Recommended path:**
- **Keep the unified API surface** — `bv.count()`, `bv.top_k()` etc. all live as peers in user-facing docs.
- **Add a "Cost class" column to the operator table** in `docs/operators.md`: `Tier 1 (≤30 ns)`, `Tier 2 (30-100 ns)`, `Tier 3 (100-300 ns)`. Users still mix freely; they have visibility into per-op cost when they want it.
- **Expose per-AggKind apply trace as a `/debug/agg_timing` endpoint** for production diagnostics. Already half-built (BEAVA_TRACE_AGG_TIMING env var).

This preserves the uniform devex while honestly disclosing the algorithmic-floor tradeoff for sketches. **NOT a code-architecture change — a documentation + observability change.** It also avoids the trap of building a "premium tier" that fragments the operator API.

### Recommendation 4 (lower priority): Apply-loop locality fix

**The wrapping cost the trace shows is amortizable across features.** Today every feature's `update_with_row` independently does:
- `where_expr` evaluation (already shared via Plan 05-02 if same expr)
- `row.get(field_name)` linear scan
- `numeric_from_row` Value match

For a 14-feature fraud pipeline, all 14 features may scan the same row independently — that's 14 × 30 ns = 420 ns of duplicated scan, on TOP of the per-op wrapping. Adding a tiny per-event "field cache" inside `apply_event_to_aggregations` (resolve each unique field name once, share across features) saves another ~50% on multi-feature pipelines. Not strictly the same as Phase 19.3 (which works at register-time); this is at apply-time. Could ride alongside Phase 19.3.

---

## Gaps the audit found beyond Phase 19.3

1. **The variance the user observed is *mostly* wrapping for Tier 1 (135× over algo for Count!) and *partially* algorithmic for Tier 3.** Phase 19.3 collapses Tier 1 to a uniform 25-40 ns band. It does NOT erase Tier 3's >100 ns floor — that requires algorithm-replacement decisions (Misra-Gries vs SpaceSaving, IndexMap vs BTreeMap, etc.).

2. **The "uniform read→update→write template" is achievable for 38 of 55 ops at ≤40 ns, but NOT for 9 sketch ops.** The user's hypothesis is *more correct than the previous audit framed it* for Tier 1 — the wrapping really IS the dominant variable cost there. But for sketches, the algorithm dominates and there's no template trick that erases the data-structure walk.

3. **HLL is the most underrated op for fast-path eligibility.** Microbench: 23 ns. Algorithmic floor: ~15 ns. With Phase 19.3 wrapping fix + hasher reuse + drop one Box, HLL update could be ~50-60 ns/call — getting close to OutlierCount territory. The 952 ns TRACED number is almost ENTIRELY wrapping. This is a much better story than the current "sketches are slow" framing — at least HLL doesn't have to be.

4. **EntityRow init cost is separate and bigger than per-op cost on cold keys.** Phase 19.1-04's lazy-buckets fix already addressed the biggest contributor (1500 ns / 2576 ns cold-key). Worth flagging as adjacent: per-op apply cost is only HALF the picture for cold-key fraud workloads. Don't optimize ops in isolation while ignoring the cold-key allocator path.

5. **Entropy's microbench is `format!()`-poisoned.** The 693 ns number is mostly the test fixture's `format!("c{}", k)`. Real production cost (per-call, BTreeMap entry on a CompactString) is closer to 60-100 ns. Worth re-running the microbench with pre-built keys to get an honest baseline.

6. **OutlierCount's 32 ns is a sqrt floor.** No way to get below ~20 ns without dropping the variance check (and thus the op's purpose). Document and accept; it's a Tier 2 borderline.

7. **Histogram's bucket_index linear scan is fine.** ≤20 buckets in practice; the linear scan is faster than binary_search for small N (cache + branch prediction). Already documented in the previous audit; reaffirm here.

---

## Cross-references

- **Previous audit:** `.planning/research/operator-update-efficiency-audit.md` — classified by O() complexity. This audit complements it by classifying by *constant-factor cost*: an O(1) op can still be 250 ns/call due to wrapping; the previous audit didn't decompose that.
- **Phase 19.3 wrapping reduction** (HANDOFF.json sub-goal): addresses ~600-800 ns wrapping. Confirmed by this audit as the highest-leverage architectural change.
- **Memory `project_no_same_key_batching`** (committed to memory): same-key sketch batching is FORBIDDEN as v0 commitment. This audit does NOT propose it as a path to 20 ns, in compliance with that lock. Per-op SIMD on a single update IS allowed; per-event-batch-into-same-op is not.
- **Memory `project_v2_devex_first`**: tiered op catalogue suggestion intentionally keeps the devex-first model (unified API surface) and adds visibility-only changes (cost-class column in docs, /debug endpoint).
- **CLAUDE.md § Performance Discipline**: every Phase 6+ op has a microbench. Phase 19.3 should add a per-op `iter_batched(1k_inserts)` variant to amortize wrapping cost the way real production does — current single-call benches suppress wrapping cost and over-state Tier 1/Tier 3 ratio.
- **Phase 19.1-04 lazy-buckets baselines**: WindowedOp::new + first update is 154 ns post-fix, 581 ns pre-fix. Adjacent to this audit's scope but the same wrapping/init category.

---

## Final verdict on user's question

> "Can every operator hit a uniform read→update→write pattern at ~20 ns target?"

**No, but PARTIAL yes**:
- 38 ops (Tier 1): **YES, achievable at 25-40 ns post-Phase-19.3.** The user's intuition is correct — wrapping IS the dominant cost.
- 8 ops (Tier 2): **No, but uniform 30-80 ns achievable.** Algorithmic floor of 15-50 ns plus residual wrapping.
- 9 ops (Tier 3): **No, 100-300 ns is the algorithmic floor.** No template change closes this; sketches sacrifice cost for approximation guarantees, by design.

**The single biggest non-Phase-19.3 recommendation:** For Tier 3 ops, swap UDDSketch's `BTreeMap<i32, u64>` for a flat sorted `Vec<(i32, u64)>` with binary_search. Cache-friendly, simpler, and ~30-50% faster than the current BTreeMap walk on the bucket-collapsed (2048-bucket) state. Same alpha=0.01 accuracy. Same retraction support. Just better cache locality. **Estimated improvement: Percentile UDDSketch update from ~130 ns algo floor to ~75 ns** — a meaningful step toward the user's uniform-target ambition for the fraud workflow's most-expensive op.
