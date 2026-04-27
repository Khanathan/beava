# Operator Update Efficiency Audit

**Date:** 2026-04-27
**Scope:** All 55 shipped aggregation operators (Phase 5 / 8 / 9 / 10 / 11)
**Trigger:** Phase 19.1.2 discovered `GeoSpread` was O(n) per push; user requested systematic audit
**Methodology:** Source-walk per op (`update()` body) + cross-reference Phase 5.5/8/9/10/11 criterion microbenches in `.planning/perf-baselines.md` + Phase 19 traced-bench data in `HANDOFF.json`

---

## Executive summary

- **Total ops audited:** 55
- **Strict O(1) update:** 47
- **O(log n_buckets) update (UDDSketch path only):** 1 (Percentile post-promotion)
- **O(buckets/window) by design (Windowed wrap):** N/A — Windowed dispatch is a meta-op; inner ops keep their own complexity
- **O(n) latent BUGS found:** **2** — see Findings
- **O(n_categories) per call (allowlist linear scan):** **1** — see Findings (`EventTypeMix` with `categories=[...]`)
- **O(buckets) bounded but suspicious:** 1 (`Histogram::bucket_index` linear-scan; bounded ≤ ~20 → fine in practice)
- **All-clear:** 50

**Headline:** Most ops are correctly O(1). The two new latent issues are:
1. **`CountDistinctState::ExactArray` — O(n)/insert-into-Vec for n ≤ 16.** Bounded so practical impact is small (≤ ~16 element shifts per add), but the `values.insert(pos, hash)` is genuinely O(n) of the array length. Fix is trivial (drop the binary-search-then-insert pattern; just `push` and `sort` on promotion).
2. **`EventTypeMix.allowed.contains(&cat)` — O(allowed.len()) string-equality scan per event** when the user pre-declares `categories=[...]`. This is the suspicious 1,127 ns/call from the trace data. Plus the `to_string()` allocation in `str_from_row` adds ~50 ns even when `allowed` is None.

**Critically: `GeoSpread` is the only op that was previously O(n) of total entity history. Phase 19.1.2-01's Welford rewrite is now landed and verified. No other op walks per-entity history on update. The hot-path is clean from O(n)-history bugs.**

The big remaining lever is **per-call wrapping cost reduction (Phase 19.3 territory)**: even strict-O(1) ops show 200–1,400 ns/call in traced bench because of `row.get()` linear scan, `to_string()` allocations, and HashMap/BTreeMap entry lookup. Those are not classification bugs, just constant-factor opportunities.

---

## Per-op classification table

Legend:
- **O(1)** = strictly constant per call, regardless of entity event-history depth
- **O(1)†** = bounded constant by a register-time-fixed parameter (window slots, hour buckets, etc.)
- **O(log n)** = grows logarithmically (BTreeMap / binary search)
- **O(n)** = linearly grows with event history — this is the GeoSpread bug pattern; a bug
- **O(n_cat)** = grows with allowlist or category cardinality; only counts as a bug if cardinality is unbounded

Per-call costs are from `.planning/perf-baselines.md` Apple-M4 single-call criterion microbench unless flagged with "TRACED" (Phase 19 BEAVA_TRACE_AGG_TIMING data on fraud-team-shape pipeline).

### Phase 5 — core (8)

| AggKind | Family | Update complexity | Source location | Per-call cost | Notes |
|---|---|---|---|---|---|
| Count | core | **O(1)** | `agg_state.rs:67-77` | 1.8 ns micro / 274 ns TRACED | Pure `n += 1`. Trace overhead is wrapping (row.get hash init etc.). |
| Sum | core | **O(1)** | `agg_state.rs:94-111` | 5.7 ns micro / 256 ns TRACED | `total += v; n += 1`. |
| Avg | core | **O(1)** | `agg_state.rs:131-148` | 5.5 ns micro | `sum += v; n += 1`. |
| Min | core | **O(1)** | `agg_state.rs:168-192` | 6.6 ns micro | `value_lt` compare + `clone()` of Value (string-copy on non-numeric). |
| Max | core | **O(1)** | `agg_state.rs:207-231` | 9.5 ns micro | Mirror of Min. |
| Variance | core | **O(1)** | `agg_state.rs:259-280` | 12.1 ns micro | Welford online second-moment. |
| StdDev | core | **O(1)** | `agg_state.rs:259-280` (shares VarianceState) | 10.9 ns micro | Same state struct as Variance. |
| Ratio | core | **O(1)** | `agg_state.rs:317-329` | 3.3 ns micro | `total += 1; if where_matched: matching += 1`. |

### Phase 8 — point / ordinal / recency (15)

| AggKind | Family | Update complexity | Source location | Per-call cost | Notes |
|---|---|---|---|---|---|
| First | point | **O(1)** | `agg_state.rs:349-366` | ~3.8 ns micro | Early-exit once `current.is_some()`. |
| Last | point | **O(1)** | `agg_state.rs:381-398` | 7.60 ns micro | Single Value clone. |
| FirstN | point | **O(1)** | `agg_state.rs:425-441` | 3.76 ns micro | `Vec::push` until `len >= n` then no-op; capped at register-time `n`. |
| LastN | point | **O(1)** | `agg_state.rs:463-482` | 7.89 ns micro | `VecDeque::push_back` + `pop_front` when full. n is bounded. |
| Lag | point | **O(1)** | `agg_state.rs:515-534` | 7.84 ns micro | `VecDeque` ring of capacity n+1; bounded. |
| FirstSeen | recency | **O(1)** | `agg_state.rs:559-574` | 23.75 ns micro | Two `Option<i64>` writes — cost dominated by branch / memory write. |
| LastSeen | recency | **O(1)** | `agg_state.rs:559-574` (shares SeenState) | 26.31 ns micro | Same struct as FirstSeen; only `query` differs. |
| Age | recency | **O(1)** | `agg_state.rs:559-574` (shares SeenState) | 34.99 ns micro | Same update; query subtracts query_time. |
| HasSeen | recency | **O(1)** | `agg_state.rs:559-574` (shares SeenState) | 17.91 ns micro | Same update. |
| TimeSince | recency | **O(1)** | `agg_state.rs:559-574` (shares SeenState) | 75.44 ns micro | High variance — quiescent baseline noise per phase notes. |
| TimeSinceLastN | recency | **O(1)** | `agg_state.rs:625-638` | 90.91 ns micro | `VecDeque` ring of capacity n; bounded. |
| Streak | streak | **O(1)** | `agg_state.rs:663-677` | 17.04 ns micro | Two u64 writes. |
| MaxStreak | streak | **O(1)** | `agg_state.rs:663-677` (shares StreakState) | 31.97 ns micro | Same update. |
| NegativeStreak | streak | **O(1)** | `agg_state.rs:695-707` | 33.41 ns micro | Inverse of Streak. |
| FirstSeenInWindow | windowed-recency | **O(1)** | `agg_state.rs:732-742` | 117.24 ns micro | Single `last_ms` write; query computes age vs window. |

### Phase 9 — decay (6) + velocity (8) + z-score (1) — 15 total

| AggKind | Family | Update complexity | Source location | Per-call cost | Notes |
|---|---|---|---|---|---|
| Ewma | decay | **O(1)** | `agg_state_decay.rs:79-111` | 8.55 ns micro | One `exp()` call + scalar arithmetic. |
| EwVar | decay | **O(1)** | `agg_state_decay.rs:138-173` | 9.60 ns micro | EW-Welford. One exp + 5 scalar ops. |
| EwZScore | decay | **O(1)** | `agg_state_decay.rs:194-205` | 10.08 ns micro | Wraps `EwVarState::update`. |
| DecayedSum | decay | **O(1)** | `agg_state_decay.rs:232-259` | 9.06 ns micro | Cormode forward-decay. One exp. |
| DecayedCount | decay | **O(1)** | `agg_state_decay.rs:282-305` | 5.80 ns micro | No field — fastest decay op. |
| Twa | decay | **O(1)** | `agg_state_decay.rs:334-362` | 8.24 ns micro | Time-weighted average; 4 scalar updates. |
| RateOfChange | velocity | **O(1)** | `agg_state_velocity.rs:41-69` | 8.40 ns micro | (Δvalue / Δt). |
| InterArrivalStats | velocity | **O(1)** | `agg_state_velocity.rs:93-116` | 15.57 ns micro | Welford on inter-arrival gaps. |
| BurstCount | velocity | **O(1)†** | `agg_state_velocity.rs:151-183` | 9.74 ns micro | Bounded 64-bucket sliding sub-window. Bucket lookup is `idx = ... .rem_euclid(64)` — O(1). |
| DeltaFromPrev | velocity | **O(1)** | `agg_state_velocity.rs:205-228` | 6.35 ns micro | Two scalar writes. |
| Trend | velocity | **O(1)** | `agg_state_velocity.rs:254-276` | 6.85 ns micro | Online OLS — 4 running sums. |
| TrendResidual | velocity | **O(1)** | `agg_state_velocity.rs:317-336` | 13.22 ns micro | Wraps Trend; computes residual at query. |
| OutlierCount | velocity | **O(1)** | `agg_state_velocity.rs:367-404` | 32.49 ns micro | Welford + sigma threshold. One sqrt. |
| ValueChangeCount | velocity | **O(1)** | `agg_state_velocity.rs:421-445` | 9.89 ns micro | One scalar compare. |
| ZScore | z-score | **O(1)** | `agg_state_velocity.rs:465-487` | 18.01 ns micro | Welford + sqrt at query. |

### Phase 10 — sketches (5)

| AggKind | Family | Update complexity | Source location | Per-call cost | Notes |
|---|---|---|---|---|---|
| CountDistinct | sketch | **MIXED — see findings** | `sketches/count_distinct.rs:51-83` + `agg_state.rs:859-880` | 17.2 ns (exact) / 262.1 ns (HashSet) / 23.1 ns (HLL) micro / **952 ns TRACED** | **POTENTIAL O(n) BUG in ExactArray mode (n ≤ 16):** `values.insert(pos, hash)` after `binary_search` shifts up to 16 elements. Bounded → not a real perf hit at v0 scale, but technically not O(1). HashSet mode (≤1024) and HLL mode (>1024) are both O(1). One-shot promotions: 1.41 µs (Array→Set) and 4.22 µs (Set→HLL) — amortized fine. **Trace-vs-microbench gap (952 ns vs 23 ns)** is wrapping cost: `hash_value` switch on Value type + ahash init + row.get linear scan + Box<CountDistinctStateWrap> indirection. |
| Percentile | sketch | **O(1)** (Exact) → **O(log n_buckets)** (UDDSketch) | `sketches/percentile.rs:45-70` + `sketches/uddsketch.rs:100-120` + `agg_state.rs:898-921` | ~17 ns (exact) / 111.2 ns (UDDSketch) micro / **963 ns TRACED** | Exact mode: `Vec::push` O(1). After 256-element promotion: UDDSketch::insert is `BTreeMap::entry()` O(log n_buckets) — n_buckets capped at 2048. Bucket *count* doesn't grow with entity history beyond cap (collapse maintains it). True per-call cost is dominated by `BTreeMap::entry()` traversal, then once per ~2048 inserts the collapse pass runs. |
| TopK | sketch | **O(log k)** (always) | `sketches/top_k.rs:59-109` + `sketches/cms.rs:307-335` + `agg_state.rs:937-964` | 70.5 ns (exact) / 260.5 ns (hybrid) micro / **1,381 ns TRACED** | Exact mode: `BTreeMap::entry().or_insert(0)` = O(log distinct). Hybrid mode (>1024): CMS update is O(D=4 hash row updates) + `TopKHeap::insert_or_bump` is O(log k) via Plan 22-04's HashMap side-index. Both stay O(log k) in cardinality. |
| BloomMember | sketch | **O(1)†** | `sketches/bloom.rs:69-75` + `agg_state.rs:1001-1018` | 95.2 ns micro / 44 ns TRACED | Bounded by `num_hashes` (k=7 for capacity=1024, fpr=0.01). Each hash → `words[pos / 64] |= 1u64 << ...`. Strict O(k) with k constant. |
| Entropy | sketch | **O(1)** | `sketches/entropy.rs:48-69` + `agg_state.rs:1038-1055` | 693.3 ns micro (dominated by format!) / **845 ns TRACED** | `BTreeMap::get_mut` (O(log n_categories), n_categories ≤ cap=1024) + bumping count. Cap-and-spill for novel keys. Genuinely O(log cap) per insert — but cap is a constant register-time parameter, so amortized O(1). The 693 ns micro number includes `format!()` in the test fixture; real production cost lower. |

### Phase 11 — bounded buffer (7) + geo (6) — 13 total

| AggKind | Family | Update complexity | Source location | Per-call cost | Notes |
|---|---|---|---|---|---|
| Histogram | buffer | **O(buckets)†** | `agg_buffer.rs:66-86` | 5.77 ns micro | **`bucket_index` is a linear scan** of `self.buckets` (line 67-74). Documented intentionally — bucket count ≤ ~20 in practice → fine. Could be binary-search but not worth the complexity. |
| HourOfDayHistogram | buffer | **O(1)** | `agg_buffer.rs:133-139` | 1.05 ns micro | Direct array index `self.counts[hour]` over `[u64; 24]`. Fastest op in the codebase. |
| DowHourHistogram | buffer | **O(1)** | `agg_buffer.rs:179-185` | 1.98 ns micro | Direct index over Vec[168]. |
| SeasonalDeviation | buffer | **O(1)** | `agg_buffer.rs:233-253` | 3.35 ns micro | Per-hour bucket update: `n += 1; sum += v; sum_sq += v*v`. **Note:** uses `(sum, sum_sq)` instead of Welford → less numerically stable but equivalent O(1) cost. |
| EventTypeMix | buffer | **O(1) base / O(allowed_len) WHEN `allowed=Some([...])`** | `agg_buffer.rs:303-322` | 20.62 ns micro / **1,127 ns TRACED** | **POTENTIAL HOT-PATH BUG:** when `allowed: Option<Vec<String>>` is `Some`, line 313 does `if !allowed.contains(&cat)` — a **linear string-equality scan over the allowlist on every event**. With `categories=[a,b,c,d]` and a string-keyed event field, this is 4 string compares per call. Plus `str_from_row` allocates a `String` (`s.to_string()` line 32 / `n.to_string()` line 33) on every call. The 1,127 ns/call TRACED suggests `allowed` is set in fraud-team.json. **The microbench (20.62 ns) likely runs without `categories` set.** |
| MostRecentN | buffer | **O(1)** | `agg_buffer.rs:359-379` | 7.10 ns micro | Circular buffer of capacity n; one Value clone. |
| ReservoirSample | buffer | **O(1)** | `agg_buffer.rs:432-453` | 7.81 ns micro | Algorithm R: deterministic xorshift PRNG, one modulo, one Value clone. |
| GeoVelocity | geo | **O(1)** | `agg_geo.rs:73-91` | 24.28 ns micro | Two scalar reads + haversine + max compare. |
| GeoDistance | geo | **O(1)** | `agg_geo.rs:123-134` | 20.26 ns micro | Two scalar reads + haversine + sum. |
| **GeoSpread** | geo | **O(1) post-fix** (was **O(n)** pre-Phase-19.1.2) | `agg_geo.rs:179-195` | 64-200 ns TRACED post-fix; 5,000-25,000 ns TRACED pre-fix | **Welford rewrite landed in Phase 19.1.2-01.** Old code: `for &p in &self.samples` walked entity's full point history on every push. New code: two `m2_lat`/`m2_lon` accumulators, pure scalar update. ~80–400× speedup. |
| UniqueCells | geo | **O(log distinct)** | `agg_geo.rs:253-262` | 12.43 ns micro | `BTreeMap::entry().or_insert(0)` — O(log n_distinct_cells). Cells grow with entity geographic spread; **not bounded by a register-time cap**. For high-mobility entities this could grow without limit (memory leak risk, not a per-call perf bug). |
| GeoEntropy | geo | **O(log distinct)** | `agg_geo.rs:302-312` | 14.64 ns micro | Same pattern as UniqueCells (`BTreeMap::entry`). **Same uncapped-growth concern.** |
| DistanceFromHome | geo | **O(1)** update / **O(samples)** query | `agg_geo.rs:375-393` | 16.49 ns micro | Update: write to ring buffer at `head` index, advance — O(1). **Query** (`agg_geo.rs:395-406`) iterates the ring sum twice for centroid mean — O(samples) per query. Samples is register-time-fixed (default 100) so amortized fine, but worth flagging if queries dominate. |

---

## Findings

### O(n) BUGS (similar to pre-fix GeoSpread)

#### Finding 1: `CountDistinctState::ExactArray` is O(n)/insert in n ≤ 16

- **Op name:** `count_distinct` (CountDistinct AggKind, ExactArray mode)
- **File:line:** `crates/beava-core/src/sketches/count_distinct.rs:53-66`
- **The bug:**
  ```rust
  if let Err(pos) = values.binary_search(&hash) {
      values.insert(pos, hash);  // ← O(n) shift of all elements after pos
      ...
  }
  ```
  Each insert into the middle of a Vec shifts up to 16 u64s. Technically O(n) where n ≤ 16.
- **Severity:** **LOW.** Bounded at 16 elements → at most ~16 × 8-byte memmove per call, ~10 ns of memmove. Will not show up in benchmarks. Pure correctness/cleanliness issue.
- **Suggested fix:** Replace binary-search-then-insert with `if !values.contains(&hash) { values.push(hash); }`. Linear scan over 16 elements is faster than binary-search-and-insert because the contains() doesn't shift. Promotion path can keep its `for &h in values.iter()` loop unchanged.
- **Estimated lift:** Negligible per-event; it removes the O(n) shape but on 16 elements the constant factor wins matter more than asymptotics. Not worth a hotfix; bundle into the next sketch-cleanup pass.
- **Reference pattern:** Phase 19.1.2 GeoSpread Welford rewrite for documenting an O(n)→O(1) change in SUMMARY.md.

#### Finding 2: `EventTypeMix.allowed.contains(&cat)` is O(allowed_len) per call

- **Op name:** `event_type_mix` (EventTypeMix AggKind) **only when `categories=[...]` kwarg is set at register time**
- **File:line:** `crates/beava-core/src/agg_buffer.rs:312-314`
- **The bug:**
  ```rust
  if let Some(allowed) = &self.allowed {
      if !allowed.contains(&cat) {  // ← O(allowed.len()) string-equality scan
          return;
      }
  }
  ```
  `Vec::contains(&cat)` is a linear scan over the allowlist; each comparison is a full-string equality test on a heap-allocated `String`. Plus `str_from_row` (line 30-37) calls `s.to_string()` / `n.to_string()` allocating a fresh `String` for every event before the contains check.
- **Severity:** **MEDIUM-HIGH for fraud-team-shape pipelines.** This explains the suspicious **1,127 ns/call TRACED** vs **20.62 ns/call microbench** — the microbench runs without `categories` set, so the linear scan and the `to_string()` cost don't fire. The fraud-team config sets `categories=[click, view, ...]` at register time.
  - At 4 categories × ~50 ns/string-compare + ~50 ns string allocation + BTreeMap::entry(): the 1,127 ns/call number is consistent.
  - At 20 categories the per-call cost would scale linearly to ~2,000 ns/call. **This makes EventTypeMix the worst-scaling op for category-heavy fraud pipelines.**
- **Suggested fix (two-part):**
  1. **Replace `Vec<String>` allowlist with `BTreeSet<String>` or `AHashSet<String>`** at `EventTypeMixState::new` time. Lookup becomes O(log n) or O(1) instead of O(n).
  2. **Avoid the `to_string()` allocation in the hot path:** `str_from_row` should return a `Cow<'_, str>` borrowing from `Value::Str`'s `Arc<str>` when possible; only allocate for `Value::I64` / `Value::Bool`. Then `allowed.contains(&Cow::Borrowed(s))` works on the borrowed view. **Or:** stop allocating and pass `&str` to a hashed lookup against pre-built `HashSet<&str>`.
  - Both fixes together should drop EventTypeMix per-call cost from ~1,100 ns to ~50–100 ns — a **10-20× speedup**.
- **Severity vs GeoSpread:** smaller absolute hit (1.1 µs vs 14-24 µs pre-fix) but high impact on fraud-shape workloads where EventTypeMix is a marquee fraud feature.
- **Reference pattern:** Phase 19.1.2 GeoSpread Welford rewrite (data-structure swap in hot path).

### O(?) flagged for investigation

#### `UniqueCells` and `GeoEntropy` — **uncapped BTreeMap growth**

- **Files:** `agg_geo.rs:253-262` (UniqueCells), `agg_geo.rs:302-312` (GeoEntropy)
- **The concern:** Both grow `cells: BTreeMap<(i32, i32), u64>` unbounded. Per-call cost is O(log distinct_cells) — this is correct asymptotically. **But the *memory* is unbounded.** A taxi/rideshare entity with high spatial entropy could grow this map without limit; an adversarial geo-jitter attacker could fill it on purpose.
- **Per-call:** stays O(log n) — not a perf bug per the audit's terms.
- **Memory bug:** for memory budget compliance per CLAUDE.md "~7KB per entity for a rich 30-feature pack", an UniqueCells op storing thousands of cells per entity blows the budget for high-mobility entities.
- **Recommendation:** add a `max_cells` register-time parameter (default ~256) and either (a) drop new cells silently or (b) reservoir-sample. Consistent with EventTypeMix's `max_categories` cap pattern.
- **Severity:** MEDIUM (memory pressure bug, not a per-call perf bug). Not in scope of this audit but flagged here for the record.

### Surprisingly expensive O(1) ops (high TRACED cost despite correct complexity)

These are NOT bugs. Algorithmic complexity is correct; the high TRACED cost is **wrapping overhead** that Phase 19.3 addresses:

| Op | Microbench | TRACED | Gap dominated by |
|---|---|---|---|
| Count | 1.8 ns | 274 ns | row.get() linear scan, `Hash::default()` ahash init (per-call), enum-dispatch branch |
| Sum | 5.7 ns | 256 ns | Same wrapping; field name string compare |
| CountDistinct | 23.1 ns (HLL) | 952 ns | `hash_value` Value-type switch + ahash init; `Box<CountDistinctStateWrap>` heap indirection; row.get |
| Percentile | 111.2 ns | 963 ns | Same; UDDSketch BTreeMap traversal; `Box<PercentileStateWrap>` indirection |
| TopK | 260.5 ns | 1,381 ns | Same; CMS hash row updates; TopKHeap::insert_or_bump sift |
| Entropy | 693 ns | 845 ns | `format!()` in test fixture biases the micro number high; trace number is in line with BTreeMap::get_mut + key alloc |

The wrapping cost is shared across ALL ops:
- **Per-event:** every wrapper independently calls `row.get(field_name)` → O(row_field_count) linear scan per op (`Row` is a `BTreeMap<String, Value>` per `crates/beava-core/src/row.rs` convention).
- **Per-call:** ahash hasher state init (when computing hashes for sketches).
- **Per-call:** Box<…> indirection for sketch wrappers (CountDistinct, Percentile, TopK, BloomMember, Entropy are all `Box<…StateWrap>` in the AggOp enum).

**Phase 19.3 sub-goals address all of these:**
1. Field pre-extraction (resolve `field_name` → field index once per descriptor, then array-access at update time).
2. Hasher reuse / pre-built ahash::RandomState.
3. Inline-stored sketch state (drop `Box` for at least HLL — its 4KB register array is the only reason for the Box; consider `Box<[u8; 4096]>` instead of `Box<CountDistinctStateWrap>`).

### Anti-patterns observed across multiple ops

#### Pattern 1: `value_to_key_string` and `str_from_row` allocate on every call

- **Files:** `agg_state.rs:830-843` (`value_to_key_string`), `agg_buffer.rs:30-37` (`str_from_row`)
- **Both allocate a fresh `String`** for every event:
  - `Value::I64(n) => Some(n.to_string())` — heap allocation for each integer event
  - `Value::Bool(b) => Some(b.to_string())` — heap allocation per call
  - `Value::Str(s) => Some(s.to_string())` — copies the Arc<str> contents into a fresh String even though `Value::Str` is already `Arc<str>` and could be borrowed
- **Affected ops:** Bloom, Entropy, EventTypeMix.
- **Fix:** return `Cow<'_, str>` from these helpers. `Value::Str(arc)` returns `Cow::Borrowed(arc)`; `Value::I64(n)` returns `Cow::Owned(n.to_string())`. The downstream consumers (BloomFilter::insert, EntropyHistogram::insert, EventTypeMix counts.entry) all accept `&str`, so they only need `&*cow`.
- **Estimated impact:** ~50 ns/call savings on every event for these 3 ops; up to 150 ns/call on EventTypeMix where `to_string()` runs **before** the early-exit allowlist check.

#### Pattern 2: `row.get(field_name)` linear-scan per wrapper

- **File:** every wrapper independently calls `numeric_from_row(row, fname)` or `row.get(fname)`.
- For an N-feature pack, this is N independent linear scans of the same Row's BTreeMap on every event.
- **Fix:** Phase 19.3 sub-goal 1: pre-extract fields to a typed array at apply-loop entry, pass indices to each op. Per HANDOFF.json's Phase 19.3 outline this is already on the roadmap.

#### Pattern 3: `to_string()` / `format!()` / `clone()` of Value in update paths

- `MostRecentN`, `ReservoirSample`, `LastN`, `Lag`, `First`, `Last` all do `let val = v.clone();` on every accepted event.
- For `Value::Str(Arc<str>)` this is just an `Arc::clone` (atomic counter bump) — relatively cheap but still ~5-10 ns.
- For `Value::Bytes(Vec<u8>)` this is a full bytes clone — could be expensive for large payloads.
- **Generally OK** — required to retain the value for later query. But worth noting that buffer-typed ops are not free per-call.

---

## Cross-references

- **Phase 19.1.2 GeoSpread Welford fix** — the canonical reference for any O(n) → O(1) rewrite in this codebase. Pattern: drop the unbounded buffer (`Vec<(f64, f64)> samples`), introduce running statistics (`m2_lat`, `m2_lon`), accept BREAKING snapshot format change, document with `Welford 1962` citation and Phase 19.1.2 traceability comment. Source: `crates/beava-core/src/agg_geo.rs:141-211` and `.planning/phases/19.1.2-geo-spread-rms/19.1.2-01-SUMMARY.md`.
- **Phase 19.3 wrapping reduction** (HANDOFF.json sub-goal 1) addresses the per-call cost of correctly-O(1) ops via field pre-extraction + hasher opt — addresses ~all "Surprisingly expensive O(1)" rows above without changing op complexity.
- **Memory `project_no_sharded_apply`** (committed CLAUDE.md / memory) — same-key sketch batching is FORBIDDEN as v0 commitment (Redis-cluster pattern: scale by running multiple instances, not by sharding the apply path). Per-op SIMD on a single update is allowed; per-event-batch-into-same-op is not.
- **CLAUDE.md § Performance Discipline** — every phase 6+ ships a microbench. Phase 11 microbench covers all 13 buffer/geo ops; Phase 10 covers all 5 sketches; Phases 5/8/9 cover the 8+15+15 = 38 simpler ops. **Coverage is complete.** Any future O(n) bug should land via a regression in these benches if and only if the bench input grows with n; current per-op benches use small fixed inputs so they wouldn't catch a GeoSpread-style bug. **The Phase 11 microbench should add an `iter_batched(|| state, |s| s.update(...))` variant that runs N=1k inserts to amortize over warm state**, which would have caught GeoSpread pre-Phase-19.1.2.
- **Phase 11 SUMMARY** at `.planning/phases/11-bounded-buffer-geo-operators/` — design rationale for Histogram bounded-bucket linear scan, EventTypeMix capped-categories pattern, and DistanceFromHome ring buffer choice.

---

## Recommendations

Prioritized list of ops needing follow-up work, by impact on fraud-team-shape benchmarks:

1. **`EventTypeMix` allowlist + `to_string()` allocation removal** — TRACED 1,127 ns → projected ~50-100 ns. **~10-20× speedup. Highest ROI of any single-op fix.** Fix complexity: medium (introduce `Cow<'_, str>` helper, swap Vec<String> allowlist for `AHashSet<String>` or `BTreeSet<String>`). Predicted lift on fraud-team benchmark: **~5-8% throughput improvement** if EventTypeMix is invoked once per event in the fraud pipeline (which it is).
2. **`UniqueCells` and `GeoEntropy` add `max_cells` cap** — memory budget compliance. Fix complexity: low (mirror EventTypeMix `max_categories` pattern). Predicted lift: not a perf fix — a correctness/memory-budget fix. Severity: MEDIUM. Could be Phase 19.0.x.
3. **`CountDistinct` ExactArray O(n)/insert cleanup** — drop binary-search-then-insert in favor of contains-then-push. Fix complexity: trivial (3-line change). Predicted lift: negligible per-event, but cleaner code that doesn't have an O(n) anti-pattern lurking. Could be a drive-by during Phase 19.3 wrapping work.
4. **Phase 19.3 wrapping reduction (already planned)** — addresses the per-call gap (microbench ~20 ns vs TRACED ~250 ns) for ALL correctly-O(1) core ops. Field pre-extraction + hasher reuse + drop one layer of `Box<…StateWrap>` indirection. Predicted lift: **~30-50% throughput improvement** on apply-bound shapes per HANDOFF.json's pipeline_complexity_crossover analysis (sketch-heavy pipelines are at ~250k EPS apply ceiling vs 2M EPS for simple pipelines — most of the gap is wrapping overhead).
5. **Add a 1k-insert iter_batched variant to Phase 11 microbench** so the next O(n)-of-history bug gets caught before production traces. Per `.planning/perf-baselines.md` Phase 11 row, current micro tests run a single insert against fresh state — they would not catch GeoSpread-style bugs. Bench complexity: low (mechanical iter_batched wrap). Predicted lift: regression detection only.

**No `O(n)` history-walking bugs remain in the codebase.** The hot path is clean from GeoSpread-class issues. The remaining work is constant-factor reduction (Phase 19.3) and the two finding-1/finding-2 cleanups above.
