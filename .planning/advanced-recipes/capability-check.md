# Advanced Recipes — Capability Check

Date: 2026-04-24
Purpose: for each proposed advanced recipe, verify whether beava's current operator catalog supports it. Those that are supported can ship as interactive recipes. Those that aren't get a plan-only entry that names the missing capability.

## beava operator catalog (source: `crates/beava-core/src/agg_op.rs` `AggKind`)

- **Core**: Count, Sum, Avg, Min, Max, Variance, StdDev, Ratio
- **Point / ordinal**: First, Last, FirstN, LastN, Lag
- **Recency**: FirstSeen, LastSeen, Age, HasSeen, TimeSince, TimeSinceLastN, FirstSeenInWindow
- **Streaks**: Streak, MaxStreak, NegativeStreak
- **Decay**: Ewma, EwVar, EwZScore, DecayedSum, DecayedCount, Twa
- **Velocity / trend**: RateOfChange, InterArrivalStats, BurstCount, DeltaFromPrev, Trend, TrendResidual, OutlierCount, ValueChangeCount
- **Entity z-score**: ZScore
- **Sketches**: CountDistinct (3-tier ExactArray→HashSet→HLL), Percentile, TopK, BloomMember, Entropy
- **Bounded-buffer / geo**: Histogram, HourOfDayHistogram, DowHourHistogram, SeasonalDeviation, EventTypeMix, MostRecentN, ReservoirSample, GeoVelocity, GeoDistance, GeoSpread, UniqueCells, GeoEntropy, DistanceFromHome
- **Phase 11.5**: temporal MVCC + retraction (table-level point-in-time queries)
- **Filter / expr**: `where=` clauses on aggregations, expression language

## Verdict table

| # | Recipe | Can build? | Primary ops needed | Notes / gaps |
|---|--------|-----------|--------------------|--------------|
| 1 | **Advanced fraud (ML features)** | ✅ BUILD | Count, Sum, CountDistinct, Streak, TimeSince, ZScore, Ratio, BurstCount, Trend, RateOfChange, Entropy, GeoVelocity | Feature generation fits cleanly. Model scoring done client-side as a linear combo of feature vector (realistic: prod ML systems call beava for features, score in app or via ONNX sidecar). Can show both shadow and prod scoring toggle. |
| 2 | **Advanced personalization (CF + embeddings)** | ⚠️ PLAN-ONLY | Compound-key tables; derived/synthetic streams; vector ops | **Gaps:** (a) beava aggregates are per-single-key; item-item co-view matrix needs compound-key `(item_a, item_b)` which isn't confirmed in the current API. (b) No first-class "emit derived stream" pattern — pair emission on session close would need a mechanism we don't have. (c) Embedding vector math is explicitly out of scope (beava is a feature server, not an inference server). Plan-only. |
| 3 | **Search ranking with online CTR feedback** | ✅ BUILD | Count (with `where=e.impression`), Count (with `where=e.click`), Ratio, LastN | Per-(query, doc) table keyed by a compound string `f"{query}\0{doc_id}"`. Ratio gives click-through rate. BM25 is precomputed external lookup. |
| 4 | **Funnel analytics** | ✅ BUILD | Count with `where=` per step, FirstSeen, HasSeen, TimeSince | Per-user row has one `step_N_seen` counter per funnel step via `where=e.step=="step_name"`. Drop-off = ratio of consecutive steps. Sankey is pure rendering. |
| 5 | **Cohort retention** | ✅ BUILD (complex) | FirstSeen, HasSeen (windowed), Count, TimeSince | Per-user row stores `signup_ts = FirstSeen(e.ts)` + `active_W = HasSeen within window W`. Cohort rollup computed client-side from batch-get over all users in the cohort. Heatmap is rendering. Bigger UI/data lift than the others but feasible. |
| 6 | **A/B test with SRM guard** | ✅ BUILD | Count per variant (with `where=e.variant==X`), Sum, Ratio, windowed Count | Per-experiment table keyed by `(experiment_id, variant)`; counts for exposures + conversions. SRM (chi-squared on exposure ratios) + win-probability (Bayesian) computed client-side from the batch-get. |
| 7 | **Multi-touch attribution** | ✅ BUILD | LastN(source), FirstSeen(source), TimeSince, Count | Per-user `touch_path = LastN(e.source, n=10)` + `first_touch = FirstSeen(e.source)`. Position-based / time-decay / U-shape weighting is pure client math over the returned array. |
| 8 | **Anomaly detection (EWMA z-scores)** | ✅ BUILD — natural fit | Ewma, EwVar, EwZScore, OutlierCount | Direct 1:1 mapping to beava primitives. `z = EwZScore(e.value, halflife="1h")` and `is_anomaly = abs(z) > 3`. OutlierCount already tracks deviation frequency. **This is the strongest advanced recipe fit.** |
| 9 | **SLO burn-rate tracking** | ✅ BUILD | Count (with `where=e.errored`), Ratio, windowed Count | Per-service row with `err_1h`, `err_6h`, `req_1h`, `req_6h` → burn rate = (err/req) / slo_budget. Multi-window alarm (1h fast, 6h slow both firing) done client-side. |
| 10 | **Session stitching** | ✅ BUILD | LastSeen, TimeSince, Count, MostRecentN, InterArrivalStats | Session key = `(user_id, session_id)` (derived upstream). Per-session dwell = LastSeen - FirstSeen, bounce = Count==1, path = MostRecentN. |

## Recommended build order (for when we resume)

1. **Anomaly detection (EWMA z-scores)** — best showcase of Phase 9 decay ops. Most natural beava story.
2. **Advanced fraud (ML features)** — evolves the basic fraud recipe into a real feature store demo.
3. **Funnel analytics** — high demand, simple primitives, big visualization payoff (Sankey).
4. **A/B test with SRM guard** — pairs well with funnel, continuous-stats topic.
5. **Search ranking with online CTR** — compact demo, shows Ratio + LastN.
6. **SLO burn-rate** — ops audience, different persona.
7. **Multi-touch attribution** — marketing audience, extends session stitching.
8. **Cohort retention** — biggest lift (heatmap UI + cohort aggregation logic).
9. **Session stitching** — foundational; may fold into funnel + attribution.

## Plan-only entries (write plan doc, do not build as recipe)

### #2 — Advanced personalization (CF + embeddings)

**Why plan-only:** three concrete capability gaps.

1. **Compound-key tables.** beava's `@bv.table(key="...")` takes a single key expression. Co-view matrices need `(item_a, item_b)` tables. Possible workaround: derive a string key `f"{a}\0{b}"` upstream, but confirm this doesn't break the 3-tier count-distinct promotion or temporal MVCC.
2. **Derived stream emission.** To populate a co-view table, a session-close event needs to emit N² pair events. No current first-class primitive for "on condition, emit derived stream". Today this requires an external transform layer.
3. **Vector operations.** Embedding dot products, cosine similarity, ANN lookup are out of scope. beava serves features; embeddings live in a vector DB (Qdrant, LanceDB, etc.).

**What to add instead of shipping a full recipe now:**
- A landing note in the recipe card explaining beava-as-feature-source for a recommender, with outbound links to "here's how you'd wire beava to a vector DB".
- File a design ticket: "RFC: derived stream emission + compound-key tables for pair aggregations".

**What the plan doc should cover (next session):**
- The RFC for the two missing primitives.
- A shim design: user-space pair emitter that pushes both events to beava, with error-budget + ordering guarantees.
- A reference architecture diagram: beava (feature store) + Qdrant (vectors) + LightGBM / ONNX runtime (scoring) for a full recommender.

## Next action

Hold on launching advanced-recipe agents. Wait for the 5 basic-recipe agents (personalization / fraud / leaderboard / rate-limiting / usage-metering) to finish. Review their output. Then queue the first two advanced recipes (anomaly detection + advanced fraud) as agents.

## Appendix: operator cross-reference

The full operator catalog is in `crates/beava-core/src/agg_op.rs` (`AggKind` enum). If a future advanced recipe needs a new primitive, add it to the enum and thread through `agg_state.rs`, `agg_apply.rs`, `agg_compile.rs`, `schema_propagate.rs` per the existing phase patterns.
