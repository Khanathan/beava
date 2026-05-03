# Per-entity memory budget — fraud-team 22 KB → 7 KB CLAUDE.md target

**Status:** Investigation complete 2026-05-03. Concrete fix identified — not yet promoted to a phase. Phase 13 ship-blocker candidate.

**Decision needed:** which of (a)/(b)/(c)/(d) below ships in v0? The fix is mechanical (~10 LOC + match-arm patches) but it churns `agg_op.rs` and changes the WAL/snapshot binary layout for 7 variants — touches the same files Phase 12.7/12.8 just locked.

---

## TL;DR

Empirical r8g maxcard test (this session) measured **fraud-team at ~22 KB / entity**, **3× over the CLAUDE.md 7 KB budget**. Root cause is a single design choice in `crates/beava-core/src/agg_op.rs`:

```rust
pub enum AggOp {
    Count(CountState),                          //   8 bytes
    Sum(SumState),                              //  16 bytes
    ...
    SeasonalDeviation(SeasonalDeviationState),  // 600 bytes  ← floor-setter
    HourOfDayHistogram(HourOfDayHistogramState),// 192 bytes
    EventTypeMix(EventTypeMixState),            // 128 bytes
    DistanceFromHome(DistanceFromHomeState),    // 120 bytes
    GeoVelocity(GeoVelocityState),              //  88 bytes
    GeoSpread(GeoSpreadState),                  //  88 bytes
    GeoDistance(GeoDistanceState),              //  80 bytes
    ...
    CountDistinct(Box<CountDistinctStateWrap>), //   8 bytes (boxed — good)
    Percentile(Box<PercentileStateWrap>),       //   8 bytes (boxed — good)
    Windowed(Box<WindowedOp>),                  //   8 bytes (boxed — good)
    ...
}
```

**`size_of::<AggOp>() = 600 bytes`** because of one unboxed variant. Every feature in `Vec<AggOp>` consumes 600 bytes of slot memory regardless of which variant is stored. A `Count` (logically 8 bytes) actually costs **600 bytes** when it lives next to a SeasonalDeviation in the same enum.

**For a single fraud-team `user_id` entity** (78 features across TxnByUser + LoginByUser + RefundByUser): inline-slot cost is **46.8 KB**.

**Fix:** Box the 7 fat-payload variants. `size_of::<AggOp>()` drops from **600 → ~72 bytes** (8× shrink). User entity inline cost drops from 46.8 KB → 5.6 KB. Predicted fraud-team-average per-entity drops from 22 KB → ~6 KB (clears the 7 KB budget with 14% headroom).

The 7 sketch wrappers and `WindowedOp` are *already* boxed — this fix just extends the same pattern to the 7 unboxed fat variants that Phases 9 and 11 added. Mechanical, ~10 LOC + per-variant `*` deref in match arms.

---

## Investigation method

Wrote `crates/beava-core/tests/per_entity_size_dump.rs` — a `#[test]` that prints `std::mem::size_of` for every AggOp variant's state struct, the AggOp enum itself, and a per-derivation projection against fraud-team.json's actual feature counts (62 + 8 + 6 + 8 + 4 + 8 + 4 + 3 + 8 = 111 features across 9 derivations).

Run with:

```bash
cargo test -p beava-core --test per_entity_size_dump -- --nocapture
```

The test is read-only against beava-core (no production code changes); kept uncommitted for now — promote with the fix or delete after the doc lands.

---

## Measured AggOp variant sizes

| Phase | Variant | Storage | Bytes |
|---|---|---:|---:|
| **5 core stats** | Count, Sum, Avg, Min, Max, Variance, StdDev, Ratio | inline | 8 – 32 |
| **8 ordinal/recency** | First, Last, FirstN, LastN, Lag, SeenState (×5), TimeSinceLastN, Streak, MaxStreak, NegativeStreak, FirstSeenInWindow | inline | 8 – 40 |
| **9 decay** | Ewma, EwVar, EwZScore, DecayedSum, DecayedCount, Twa | inline | 24 – 40 |
| **9 velocity** | RateOfChange, InterArrivalStats, BurstCount, DeltaFromPrev, Trend, **TrendResidual**, OutlierCount, ValueChangeCount, ZScore | inline | 24 – **72** |
| **10 sketches** ✅ | CountDistinct, Percentile, TopK, BloomMember, Entropy | **Box** | 8 (inline) |
| **11 buffer** | Histogram, **HourOfDayHistogram**, DowHourHistogram, **SeasonalDeviation**, **EventTypeMix**, MostRecentN, ReservoirSample | inline | 24 – **600** |
| **11 geo** | **GeoVelocity**, **GeoDistance**, **GeoSpread**, **DistanceFromHome** | inline | 80 – **120** |
| **5/9 windowed** ✅ | Windowed | **Box** | 8 (inline) |

The seven **bold** unboxed variants (≥72 bytes) collectively force `size_of::<AggOp>() = 600 bytes`. Phase 10 sketches got boxing right; Phase 11 + late Phase 9 additions did not.

---

## Empirical reconciliation — does this explain the 22 KB measurement?

**fraud-team feature distribution per entity** (from `crates/beava-bench/configs/fraud-team.json`):

| Group-by axis | Derivations | Features per entity |
|---|---|---:|
| `user_id` | TxnByUser + LoginByUser + RefundByUser | **78** |
| `card_fp` | TxnByCard | 8 |
| `device_id` | TxnByDevice + CardAddByDevice | 9 |
| `ip_address` | TxnByIp + SignupByIp | 12 |
| `merchant_id` | TxnByMerchant | 4 |

The maxcard test pushes events across all 5 event types, so **~5 entity rows are touched per event** (one per group-by axis). For 4M total entity rows roughly equally distributed across the 5 axes:

| Axis | Entities | Inline-slot cost | Heap state (sketches/windowed) | Total per-entity |
|---|---:|---:|---:|---:|
| user_id | 800K | 78 × 600 = **46.8 KB** | ~5 KB (HLL + UDDSketch + WindowedOp) | ~52 KB |
| card_fp | 800K | 8 × 600 = 4.8 KB | ~0.5 KB | ~5.3 KB |
| device_id | 800K | 9 × 600 = 5.4 KB | ~0.5 KB | ~5.9 KB |
| ip_address | 800K | 12 × 600 = 7.2 KB | ~0.5 KB | ~7.7 KB |
| merchant_id | 800K | 4 × 600 = 2.4 KB | ~0.5 KB | ~2.9 KB |
| **weighted avg** | 4M | **13.3 KB** | ~1.5 KB | **~14.8 KB inline + heap** |

**Predicted weighted average: ~15 KB.** Measured: **~22 KB**. The remaining ~7 KB is HashMap entry overhead (FxHash bucket + key string + last_seen_ms sidecar from Phase 12.8 cold-TTL), Vec<AggOp> headers, and a higher-than-1.5 KB heap state on user_id entities (multiple windowed ops with active buckets carrying inner sketches).

The dominant lever is the **inline-slot cost** — heap state is a small fraction. **Boxing the 7 fat variants closes ~80% of the gap.**

---

## The fix — Box the fat-payload variants

### Code change (~10 LOC + match-arm dereferences)

**`crates/beava-core/src/agg_op.rs`** — change 7 enum variants:

```rust
// Before
SeasonalDeviation(SeasonalDeviationState),
HourOfDayHistogram(HourOfDayHistogramState),
EventTypeMix(EventTypeMixState),
DistanceFromHome(DistanceFromHomeState),
GeoVelocity(GeoVelocityState),
GeoSpread(GeoSpreadState),
GeoDistance(GeoDistanceState),

// After
SeasonalDeviation(Box<SeasonalDeviationState>),
HourOfDayHistogram(Box<HourOfDayHistogramState>),
EventTypeMix(Box<EventTypeMixState>),
DistanceFromHome(Box<DistanceFromHomeState>),
GeoVelocity(Box<GeoVelocityState>),
GeoSpread(Box<GeoSpreadState>),
GeoDistance(Box<GeoDistanceState>),
```

**`crates/beava-core/src/agg_apply.rs`** + **`agg_compile.rs`** — match-arm bodies deref the boxed payload (`&mut **state` instead of `&mut *state`, or `let s = state.as_mut()`). Mechanical — same pattern already used for the 5 sketch wrappers and Windowed.

**Optional second tier (drops to ~64 bytes):** also box `TrendResidual` (72 B) and `BurstCount` (64 B). Smaller win, more variants to touch, more allocator pressure for less common ops.

### Predicted impact

| Metric | Current | After box-7 | After box-9 |
|---|---:|---:|---:|
| `size_of::<AggOp>()` | 600 B | ~72 B | ~64 B |
| user_id entity (78 feats) inline | 46.8 KB | 5.6 KB | 5.0 KB |
| weighted-avg fraud-team entity | ~15 KB | ~3 KB inline + ~3 KB heap = **~6 KB** | ~5.5 KB |
| 1M user_id entities at ~10 KB each (approx) | 10 GB | ~1.5 GB | ~1.4 GB |

### Trade-offs

| Pro | Con |
|---|---|
| Closes the 22 KB → 7 KB gap with **headroom** | Adds 1 alloc per fat-op-per-entity (one-time at register; not hot-path) |
| Already the pattern Phase 10 sketches use | Changes WAL/snapshot binary layout for 7 ops — needs `FORMAT_VERSION = 2` bump (just-reset to 1 by Phase 12.7) |
| Mechanical change, low review surface | bincode round-trip semantics may differ (verify with phase 7 snapshot replay tests) |
| Lifts the v0 ship pitch from "works on big boxes" to "works at 7 KB/entity for realistic fraud workload" | Touches Phase 12.7's freshly-locked persistence schema |
| No new mechanism needed | One more thing on Phase 13's plate |

---

## Secondary lever — WindowedOp bucket inline ratio (smaller win, deferred)

`WindowedOp.buckets: SmallVec<[(i64, Box<AggOp>); 4]>` carries 4 inline buckets × 16 bytes = 64 bytes inline + Box pointer per bucket. Each bucket's inner AggOp is heap-allocated at **600 bytes** (the same problem, recursively).

**Boxing the 7 fat variants automatically fixes this** — bucket inner AggOps drop from 600 B → 72 B too. Each windowed op shrinks by ~2.1 KB at 4 active buckets.

For fraud-team's ~30 windowed ops on a user_id entity, that's another **~63 KB** of heap state freed (at full bucket population — typically 1–2 active buckets, so realistic savings ~15–30 KB on user_id entities).

This is a free side-effect of the primary fix; no separate work required.

---

## Recommendation

**Pick (a) — box the fat variants in Phase 13.**

The fix is small enough that it doesn't deserve its own phase. Add it as Plan 13-XX (memory-fix) inside Phase 13 ship work, alongside the SDK polish + perf benchmarks. Three plans deep:

- **Plan 13-XX-A (red)**: extend `crates/beava-core/tests/per_entity_size_dump.rs` to assert `size_of::<AggOp>() <= 80`. Confirms RED.
- **Plan 13-XX-B (green)**: box the 7 variants + match-arm dereferences + WAL/snapshot `FORMAT_VERSION` bump 1→2 + recovery test for old-format rejection (Phase 12.7's pattern).
- **Plan 13-XX-C (verify)**: re-run maxcard bench on r8g, confirm fraud-team avg per-entity drops to ≤7 KB; update `.planning/throughput-baselines.md` with the post-fix numbers; amend CLAUDE.md "Memory: ~7KB per entity for a rich 30-feature pack" with a 2026-05 footnote pointing to this fix.

**Why not (b) reframe the pitch:** "1M entities per simple-shape pack" is a retreat. Real users will run fraud-shape workloads — that's literally what the fraud-team config is for. A 8× memory shrink is too cheap to skip.

**Why not (c) recommend bigger nodes:** punts to ops cost. r8g.8xlarge ($1.34/hr on-demand) vs r8g.4xlarge ($0.67/hr) for a problem that's an enum-boxing change. Bad value.

**Why not (d) combo:** the (a) fix is so mechanical that any combo reduces to "do (a) and also write better docs." Skip the meta-discussion.

---

## Open questions before promoting

1. **WAL/snapshot binary layout impact.** Boxing changes `bincode` serialization for the 7 variants. Need to verify with Phase 7's snapshot round-trip tests + Phase 6's WAL replay path. Likely needs `SNAPSHOT_BODY_FORMAT_VERSION = 1 → 2` and `FORMAT_VERSION = 1 → 2` (both just RESET to 1 by Phase 12.7's hard rip — yet another schema bump within a week may upset the section-ownership pattern; consider doing this fix in the *same* schema bump as another Phase 13 schema change if any are planned).

2. **TrendResidual / BurstCount.** At 72 B and 64 B, they're the borderline cases. Boxing TrendResidual would let us drop the floor to 64 B (BurstCount); boxing BurstCount too lets us drop to ~48 B. Each marginal box adds an allocation. Decision deferred until (a) lands and the next-largest unboxed variant becomes the visible bottleneck.

3. **Cold-entity TTL interaction.** Phase 12.8's cold-entity eviction already pages out idle entities. Combined with the boxing fix, the steady-state RSS for fraud-team should fall well below the 7 KB target. Need to re-validate the `cold_after_ms = 24h` default still makes sense once entities are 8× cheaper to keep around.

4. **Memory governance metric.** Phase 12.8 added `bytes_per_entity_p99` as a static placeholder = 7000. After this fix lands, that metric should be dynamically sampled (Phase 12.8 follow-up already noted in `.planning/STATE.md`). Land both together.

---

## Artifacts

- **Test:** `crates/beava-core/tests/per_entity_size_dump.rs` — read-only `size_of` dump + per-derivation projection (uncommitted; keep with the fix or delete after this doc lands)
- **Empirical data:** `/tmp/beava-bench-aws/maxcard-r8g.log` — 5-cell maxcard run on r8g.4xlarge 120 GiB pods (this session)
- **Bench config:** `crates/beava-bench/configs/fraud-team.json` — 9 derivations / 111 features / 5 group-by axes
- **CLAUDE.md anchor:** § Memory budget — *"~7KB per entity for a rich 30-feature pack → ~700GB for 100M entities"* (the line this fix delivers on)
- **Sibling investigation:** `.planning/ideas/valkey-io-architecture-rework.md` — IO-thread architecture rework (post-v0 / v0.1+; this memory fix should land first since it's both smaller and a v0 ship-blocker)
