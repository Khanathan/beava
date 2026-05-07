# Positioning findings — briefing for next session

**Status:** Strategic conclusions from the 2026-04-28 → 2026-04-29 positioning conversation. Pre-loads next session (likely YC office-hours skill, six forcing questions).
**Companion:** `LAUNCH-COPY-2026-04-28.md` (copy & launch-sequencing).
**Sister briefing for:** `chapter-2/index.html` punch list (in companion doc).

---

## TL;DR — what we locked

beava is **the streaming pipeline (MQ + stream processor + serving store) collapsed into one binary, at the sub-millisecond latency tier.** Same job a Kafka + Flink + Redis stack does, in a single Python file.

The five-layer positioning stack, top-to-bottom:

1. **Hook** — *"Skip the streaming pipeline. One binary."*
2. **Latency-tier qualifier** — *"At the sub-millisecond tier where the customer's request path lives."* (Postgres collapses at 1–5 ms, ClickHouse at 5–50 ms, Materialize at seconds, beava at <1 ms.)
3. **Primitive name** — *"Pipeline-transform database for live keyed state."* (More substantial than "live aggregation database" — the abstraction includes count/sum/distinct/p95/latest-N/streaks/composition, not just windowed counters.)
4. **In-category carve-out** — *"Aggregation + serving — not ETL, CDC, joins, or workflows."*
5. **Cross-category carve-out** — *"Decisioning tier of stream processing — not data movement."*
6. **Era hook** — *"Designed to be verified, not just authored."* (Agentic angle.)

Brand voice: `damiligent.` (from "damn good at streams" → dropped "streams" word, kept dam pun).

---

## The journey — what we tested and learned

Don't re-litigate these in next session.

| Hypothesis | Tested against | Conclusion |
|---|---|---|
| "beava beats ClickHouse on volume" | OpenAI runs Postgres at millions of QPS; ClickHouse handles trillions of rows | **Wrong.** Volume isn't the axis. Latency tier is. |
| "Postgres breaks at 10k QPS" | Tuned Postgres + PgBouncer + replicas does 50k–150k+ QPS | **Wrong.** Postgres scales much further than implied. |
| "Read-your-write is a unique value prop" | Redis already provides it; Postgres provides it within transaction | **Overstated.** Same audience as sub-ms latency, not separate wedge. |
| "AI agents are beava's market" | Most agents are investigative (ClickHouse-shape), not decisioning | **Partial.** Agents that act per-request are beava-shape; investigative agents are not. Sherpa is the latter — say no. |
| "Three-tier collapse is unique to beava" | Postgres-with-triggers, ClickHouse-with-MVs, Materialize all collapse the three tiers | **Not unique.** The collapse is shared; the latency tier of the collapse is the differentiator. |
| "Beava is for low event volume + high read" | This profile is Postgres-shape; beava's RAM-cost makes it overkill | **Wrong.** Carve out explicitly. |
| "Niche is too small / no one needs this" | 80+ named candidate companies; Aerospike at $100M ARR proves the market | **No.** Niche is real and supports $30–100M ARR. |
| "Beava primitive is empty" | Pipeline-transform abstraction encapsulates a real composition pattern unowned today | **No.** Primitive is real; absorption risk by Redis/ClickHouse exists but bounded |

---

## The locked positioning — primary launch hook

**Homepage hero copy (ready to ship):**

```
Skip the streaming pipeline. One binary.

beava ingests events, computes per-entity rolling features,
and serves them in microseconds — same job a Kafka + Flink + Redis
stack does, in a single Python file.

Postgres collapses at 1–5 ms. ClickHouse at tens of ms.
Materialize at seconds. We collapse at sub-millisecond — the tier
where the customer's request path lives.

  brew install beava
  docker run beava/beava

🦫 damiligent.
```

The two-step that does the buyer-qualification work:

1. **Universal hook**: every team running real-time features has felt the Kafka + Flink + Redis pain.
2. **Latency-tier filter**: we name competing collapse-tools by tier so wrong-fit prospects route themselves.

---

## The carve-outs — name competitors, send wrong-fit elsewhere

The single most important page to ship: `/docs/architecture/scope.md`. Concrete carve-outs:

| If you need… | Use… | Not us |
|---|---|---|
| ETL / data movement to warehouse | Kafka Connect, Airbyte, Fivetran, dbt | beava doesn't do this |
| CDC | Debezium, Maxwell | beava doesn't do this |
| Workflow orchestration | Temporal, Inngest | beava doesn't do this |
| Cohort drilling, BI dashboards, joins on event volume | ClickHouse, Pinot, Druid, Materialize, BigQuery, Snowflake | beava is keyed, not columnar |
| User-facing analytics with dynamic filters & high concurrency | Tinybird, Pinot, StarTree, Druid | beava can't ad-hoc filter |
| ML training data prep, batch features, offline analytics | ClickHouse, Snowflake, BigQuery | beava is online-only |
| Apps under 5k QPS feature reads | Postgres + composite index | overkill |
| Low event volume + high aggregation reads (subscription tiers, quota balances, feature flags) | Postgres + Redis cache | beava's RAM-cost is wasted at low write rates |
| Latency tolerance >30 ms | ClickHouse with materialized views, Materialize, RisingWave | their architectures fit better |
| Investigative / analytical agents (Sherpa-shape) | ClickHouse + agent | beava is wrong shape |

**Where beava IS for** (the residual):

- Auth-path inline scoring with sub-ms p99 budget
- High-QPS rate limiting / quota with per-event read-your-write
- Real-time bidders, online recs in render path
- Online ML feature serving in latency-sensitive paths
- Per-request agentic decisioning (small now, growing)
- Teams already running Redis-Lua and feeling the maintenance bill

---

## Competitive map (the latency-tier framing)

| Tool | Collapsed-tier latency floor | Sweet spot |
|---|---|---|
| Postgres + triggers / state table | ~1–5 ms | Apps under 5k QPS, simple aggregates |
| TimescaleDB (continuous aggregates) | ~10 ms reads + minute-shape freshness | Time-series with relaxed freshness |
| ClickHouse (AggregatingMergeTree) | ~5–50 ms reads + 1–60 sec freshness | BI, agentic analytics |
| Materialize / RisingWave | seconds | Streaming SQL views |
| Aerospike + UDFs | ~0.5 ms (imperative authoring) | High-QPS KV with custom logic |
| **beava** | **<1 ms with declarative Python** | **Sub-ms keyed feature serving** |

Closest competitor is **Aerospike** — same latency tier, but Aerospike is generic KV with imperative UDFs and enterprise-priced. Beava's wedge against them: declarative Python DSL + windowing primitives + OSS-first + DuckDB-shape devex.

Closest peers structurally are **Materialize / RisingWave / Tecton** (pipeline-transform abstraction), but they target different latency tiers / DSLs / GTM.

---

## ICP — named candidate companies

The 80+ companies that fit the wedge. Use as outreach list for validation calls.

**Fintech / risk:**
Mercury, Ramp, Brex, Lithic, Marqeta, Bridge, Modern Treasury, Nubank, Revolut, Wise, Chime, Cash App (uses Tecton), Plaid (uses Tecton), Robinhood, Unit, Synapse.

**Ad-tech / RTB:**
The Trade Desk, Criteo, Magnite, PubMatic, Index Exchange, AppNexus/Microsoft, AppLovin, Liftoff, Unity Ads.

**Anti-abuse / safety:**
Discord, Reddit, Pinterest, Roblox, Twitch, Snap, Cloudflare bot detection.

**Marketplace / commerce:**
DoorDash, Uber Eats, Instacart, Etsy, Wayfair, Shopify (Shop Pay), eBay, StockX, GOAT.

**Crypto:**
Coinbase / Kraken (institutional), Polymarket, Kalshi, Hyperliquid, dYdX.

**Gaming:**
Riot, Epic, EA, Ubisoft, Activision, Roblox, Mojang.

**Agentic AI (emerging):**
Sierra, Decagon, Ada, Crew AI, Cognition (Devin), MultiOn, Browse AI, Cursor, Replit, Perplexity, Hebbia, Glean.

**API / platform infra:**
Twilio, SendGrid, Resend, Cloudflare Workers KV, Vercel, Knock, Courier.

**Real-time pricing:**
Uber, Lyft, Airbnb, DoorDash, Booking, Expedia.

Conversion math: ~20% of these have acute pain right now (~16–100 actively shopping globally). At 20% acute-pain conversion + 10% latent-pain conversion, ARR ramps to ~$5–30M by Y3, $30–80M by Y5. Aerospike-shape outcome.

---

## Strategic risks

**1. Absorption by incumbents** (3–5 year horizon):
- ClickHouse ships native windowed-aggregate types
- Redis adds native windowed primitives
- Postgres mainline ships continuous aggregates (currently TimescaleDB extension)

**Mitigation:** operational simplicity (single binary), Python DSL ergonomics, agentic verification surface, brand/community moat. DuckDB pattern.

**2. Niche size ceiling.** Realistic range $30–100M ARR over 5–7 years if executed well. Not Snowflake. Comparable to Aerospike, Materialize, Tecton. **Funding strategy must match the ceiling** — don't take $100M at 10× revenue multiple if the ceiling is $50M ARR.

**3. ML feature store category drift.** Tecton et al. pushing online feature stores up-market into "AI/agentic infra." If beava lets that framing dominate, beava sits inside Tecton's category not its own. The "live aggregation database" / "pipeline transform" positioning specifically avoids this — database category, not ML platform.

**4. Existing-shop incumbents.** Teams already on Aerospike / Tecton / built-it-themselves have zero migration motivation unless pain is acute. Greenfield + Redis-Lua-graduators are the actual funnel.

---

## The agentic angle (the upside option)

Verification surface is the wedge:

- `beava check pipeline.py` — static analysis (one-day spike)
- `beava replay --events events.jsonl --pipeline new.py` — historical replay (weeks)
- `beava sample N + run --local` — DuckDB-shape iteration loop
- `beava diff old.py new.py` — semantic diff for agent PRs

The thesis: agents can write any DSL fluently; the bottleneck is *verifying* what they wrote. Beava's narrow scope + Python DSL + replay primitives give it a structurally better verification loop than ClickHouse / Materialize / Tecton. This is an upside bet — base-case TAM doesn't depend on it, but if the agentic-decisioning wave lands as expected on a 2–3 year timeline, beava is positioned for it.

**Don't bet the company on the agentic wave.** Position to capture it; build for the base case (online feature serving for fraud, ad-tech, ML, rate-limiting).

---

## HA story (committed publicly)

Tier ladder:
- **v0 OSS (today):** single binary, no HA. Be honest.
- **v1 OSS:** async streaming WAL replica + manual `beavactl promote`. RPO < 100 ms, RTO operator-driven.
- **v1.5 OSS:** bundled sentinel for auto-failover. RTO < 5 s.
- **Commercial / cloud:** sync replica option (RPO=0, +500 µs P99 cost), cross-region async DR chain, managed failover SLA.

Architecture explicitly avoids the Redis Cluster ceiling: routing baked into the binary, pipeline-aware sharding, no multi-shard atomic ops at DSL level, topology in small registry (etcd/embedded Raft) not gossip mesh, resharding as explicit dual-write cutover.

Specific paragraph for `/docs/operations/ha.md` in `LAUNCH-COPY-2026-04-28.md` §7.

---

## Reviewer findings (chapter 2 fixes — gating)

Seven independent reviewers cross-flagged. Blockers:

1. **Welford "eviction-safe" claim contradicts page's own Lua comment** — pick one
2. **`bv.p95` / `bv.distinct` algorithm unnamed** — name the sketch family + ε guarantee + per-feature memory cost
3. **Synthetic PR curve + 12% confusion-matrix base rate quantitatively wrong** at realistic fraud base rates (0.1–2%)
4. **Heavy-tailed amount distributions break raw 3σ rule** — switch to `z-score of log(amount)`, median+MAD, or `amount > p99_30d`
5. **Latency table P99 is loopback + estimated AZ RTT, not real P99** — relabel honestly

Full punch list: `LAUNCH-COPY-2026-04-28.md` §9. **Ship the 4 blockers before any external promotion** of chapter 2.

---

## Open questions for next session

These are gating; resolve before homepage ships.

1. **Sketch algorithm for `bv.p95` and `bv.distinct`.** KLL? DDSketch? t-digest? Pick + document + name ε guarantee.
2. **Per-entity memory cost.** Validate the "7 KB/entity for 30-feature pack" claim against the chosen sketches.
3. **Cold-start policy for z-score.** `min_n` gate? Shrinkage prior?
4. **Stddev semantics under window eviction.** Welford-merge? Recompute? Document one and remove the contradicting line.
5. **Online/offline parity for ML training.** Ship `as_of=` for offline backfill, or honestly say "online only in v0."
6. **Multi-tenancy story.** Single-binary-per-tenant or shared with tenant primitive? Locks before cloud pricing.
7. **Cloud pricing tier.** Currently $50/mo waitlist; commit to a tier.
8. **First three reference customers.** Who's running beava in production we can name? Even one is enough for homepage social proof.
9. **Funding strategy.** TAM ceiling is ~$30–100M ARR. Match raise / valuation accordingly.
10. **Whether to commit.** Customer discovery (20 calls from the ICP list) before further commitment. If <5% acute-pain hit rate on calls → reconsider.

---

## Validation plan — 20 customer discovery calls

Before further strategic work, run this:

- 5 fintechs (Mercury, Ramp, Bridge, Modern Treasury, Lithic)
- 5 ad-tech mid-tier (PubMatic, Magnite, Index Exchange, AppLovin, Liftoff)
- 5 anti-abuse / marketplace (Discord, DoorDash, eBay, Etsy, Roblox)
- 5 agentic startups (Sierra, Decagon, Cursor, Crew AI, Cognition)

Pitch each: the carve-out paragraph + half-day spike offer.

Outcomes:
- **3+ say "yes, send a spike"** → niche is real, ship.
- **<2 reply or all "we're fine"** → reposition or reduce scope.

---

## Comparable niche-infra outcomes (sanity benchmarks)

| Company | Niche | ARR | Years to scale |
|---|---|---|---|
| Snowflake | Cloud-native warehouse | $3B (now); $200M (Y6) | 6 years to $200M |
| Aerospike | Sub-ms KV ad-tech/fraud | $100M+ | ~10 years |
| Tecton | Online ML feature store | ~$50M | 5 years |
| Materialize | Streaming SQL views | ~$30M | 4 years |
| Tiger Data (TimescaleDB) | Time-series Postgres | $50–100M | 6+ years |
| Redis Inc | Niche-as-deep-moat | $200M+ | 15+ years |
| DuckDB / MotherDuck | Embedded analytics | OSS-led, growing | 3 years to inflection |

**The 6-year burn is normal.** Niche-infra is a slow-cook business. Plan accordingly.

---

## Doc index

- **This doc** — strategic findings + briefing.
- **`LAUNCH-COPY-2026-04-28.md`** — copy edits + launch sequencing + reviewer punch list (chapter 2).
- **`DESIGN-2026-04-23-oss-first-redo.md`** — earlier design direction.
- **`HANDOFF-2026-04-23-late.md`** — earlier handoff notes.
- **Production:** `beava-website/project/guide/chapter-2/index.html` — fraud-detection chapter (5 interactive graphs + Pipeline + Simulator + Redis comparison).

---

## What the next session should do, in order

1. Run YC office-hours skill (six forcing questions) against this briefing. Most should pre-resolve from the doc; surface the ones that don't.
2. Decide on Open Questions §1–§4 (sketch algorithm, memory cost, cold-start, stddev semantics) — these gate homepage credibility.
3. Pick funding strategy aligned to $30–100M ARR ceiling.
4. Schedule 20 customer-discovery calls from the ICP list.
5. Land the chapter-2 4 blockers before any external promotion.
6. Ship homepage rewrite around the three-tier-collapse + latency-tier qualifier.

Don't re-litigate the positioning iterations from this conversation — they're settled. Build forward from the locked stack.

**Last touched:** 2026-04-29.
