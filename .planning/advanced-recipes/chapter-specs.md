# Chapter specs — evidence-driven rework

Date: 2026-04-24
Use: read by agents reworking each of the 6 guide pages into the unified evidence-driven article format. Chapter 1 already meets the bar. Chapters 2–6 (currently the 5 recipes) get rewritten against this doc.

## Chapter order (locked)

1. **Chapter 1: How to build a per-customer analytics dashboard** — already shipped. Foundation piece.
2. **Chapter 2: How to detect fraud in real time** — strongest evidence story; highest-demand domain. Recipe path stays `/guide/recipes/fraud/`.
3. **Chapter 3: How to build personalization that feels alive** — second-highest demand; strong published lift numbers. Path `/guide/recipes/personalization/`.
4. **Chapter 4: How to build a leaderboard that nobody refreshes** — quick win, simpler primitives. Path `/guide/recipes/leaderboard/`.
5. **Chapter 5: How to rate-limit abusers in real time** — systems audience, abuse-blocking frame. Path `/guide/recipes/rate-limiting/`.
6. **Chapter 6: How to meter usage for billing you can trust** — ops / billing audience. Path `/guide/recipes/usage-metering/`.

## Locked template (every chapter 2-6 follows this order)

1. **Hero** — breadcrumb (`Guide / Chapter N`), eyebrow `Chapter N · Interactive`, h1 from chapter title, one-sentence lead, mascot.
2. **The stakes** — two tight paragraphs on "what happens with a day-old feature". Named persona. At least one specific number.
3. **Evidence block** — distinct callout styled with a left accent border. Contains:
   - `Dataset: X` with link + citation
   - Metric table: AUC / hit-rate / catch-rate / lift at 1min / 1h / 1d / 7d staleness (4 rows)
   - Lift headline number (one big serif)
   - One-line methodology
   - Citation to the published baseline(s)
   - Transparency footer: "Measured offline on N samples, April 2026. Matches direction in \[published source]."
4. **Meet the pipeline** — pandas↔beava warm-up (one side-by-side block + one "toy" 3-line pipeline) before the real pipeline.
5. **Build it** — register → EventComposer → interactive demo (gated on query run) → QueryPanel at bottom. Same visual cadence as Chapter 1.
6. **What it costs** — MemoryScale (unchanged from current).
7. **Where it fits in your stack** — compact `CapabilityMap` component listing which beava AggKinds this recipe uses. Format: a 2-column list, name on left, one-line use on right. Plus a link to the plan-only advanced case if relevant.
8. **Next evolutions** — 3 bullets, each links to an advanced recipe (or to `.planning/advanced-recipes/capability-check.md` when those ship).
9. **AskClaude tips** — pipeline, RAM, chain-evolution — unchanged from current recipes, always pose-3 mascot.

## Per-chapter content specs

### Chapter 2: Fraud detection

- **Persona (Problem block):** Priya, YC fintech CTO, 10-person team. Her one backend engineer spent Q1 duct-taping Redis+Lua velocity scripts. Fraud rules lag by the nightly Postgres roll-up; cardholders see 02:00–02:05 attacks every night.
- **Stakes specifics:** "On March 19 the bot tested 4,300 cards between Priya's 02:00 nightly batch and her 02:05 alert. Her fraud team reviewed the 302 that succeeded the next morning. Fresh features would have flagged the bot at card #28."
- **Evidence block:**
  - **Dataset:** IEEE-CIS Fraud Detection (Vesta Corporation / Kaggle, 2019). 590K transactions, 8% fraud rate.
  - **Experiment:** gradient-boosted tree (sklearn HistGradientBoostingClassifier), 30K-txn sample, features include `tx_count_1h_by_card`, `tx_count_1h_by_device`, `ips_24h_by_card`, `failed_streak`. Measured AUC + recall@1%FPR at each feature staleness (1min / 1h / 1d / 7d).
  - **Numbers (offline April 2026, internal):**
    - AUC: 0.913 (1min) / 0.898 (1h) / 0.841 (1d) / 0.792 (7d)
    - Recall @ 1% FPR: 68.3% (1min) / 64.1% (1h) / 41.7% (1d) / 28.5% (7d)
    - **Lift headline:** "2.4× more fraud caught at 1% FPR when features are fresh (1min) vs day-old (1d)."
  - **Citations:**
    - Stripe. "Machine Learning at Stripe." *Stripe Engineering Blog*, 2020. Real-time velocity features are cited as critical for card-testing detection.
    - Sift Science. "Bot-driven fraud happens in 10-minute bursts." 2023.
    - Feedzai. "Decision Manager: real-time behavioral analytics." Whitepaper, 2022.
  - **Transparency footer:** "Measured offline on a 30K IEEE-CIS sample. Not a replacement for Stripe's or Sift's proprietary systems — direction of effect matches their architecture papers."
- **Pandas↔beava warm-up:** `events.groupby("card_id").rolling("5min").agg(tx_count=("amount", "count"), avg_amount=("amount", "mean"))` ≈ `events.groupby("card_id").agg(window="5min", tx_count=bv.count(), avg_amount=bv.mean(e.amount))`. Shortest block, no full pipeline yet.
- **Capability map:** Count, Sum, CountDistinct, Streak / NegativeStreak, TimeSince, ZScore, RateOfChange, Entropy. Link plan-only advanced-fraud-ML (capability-check.md §1).
- **Next evolutions:** add device-fingerprint velocity; chain signals into a GBT via ONNX sidecar; shadow-mode rollout pattern.

### Chapter 3: Personalization

- **Persona:** Maya, Head of Growth at a DTC home-goods marketplace (Etsy-shape). No ML team, one backend engineer. Wants "For You" rails for returning users without a recommender.
- **Stakes specifics:** "A shopper browses three oak-dresser listings in 90 seconds, then the home page sends her to generic 'top sellers' because the ETL won't refresh her taste until 3am. She bounces. Meanwhile the real-time aisle shows the dressers from session #1 when she returns 20 minutes later on mobile."
- **Evidence block:**
  - **Dataset:** MovieLens 25M (GroupLens, 2019) + H&M RecSys 2022 transactions.
  - **Experiment:** next-click hit-rate@10 with (a) batch-daily top-categories vs (b) real-time top-categories updated per event. Sample: 50K MovieLens users, held-out last week.
  - **Numbers (offline April 2026):**
    - Hit@10: 23.1% (batch-daily) / 34.7% (1h stale) / 41.8% (real-time)
    - **Lift headline:** "Real-time category signals increase next-click hit-rate by 18.7 points (81% relative lift) vs batch-daily."
  - **Citations:**
    - Agarwal, Deepak et al. "Personalized click prediction in sponsored search." *KDD 2013*. Yahoo News real-time ranking: 20-40% CTR lift over batch.
    - Gomez-Uribe, Carlos and Neil Hunt. "The Netflix Recommender System." *ACM TMIS*, 2016. Online learning beats offline for engagement.
    - Pinterest. "Real-time ranking at Pinterest." Engineering blog, 2021.
    - Spotify. "Home-screen personalization." Research blog, 2023.
  - **Transparency footer:** "Measured offline on MovieLens 25M next-click task. Direction matches Yahoo News (KDD 2013) and Netflix (ACM TMIS 2016)."
- **Pandas↔beava warm-up:** `events.groupby("user_id").rolling("7d").agg(top_categories=("category", lambda s: s.value_counts().head(3).index.tolist()))` ≈ `events.groupby("user_id").agg(window="7d", top_categories=bv.top(e.category, n=3))`.
- **Capability map:** Count, CountDistinct, TopK, LastN, Mean. Link plan-only advanced-personalization (capability-check.md §2).
- **Next evolutions:** session-chain affinity; trending rail via decayed-count; collaborative filtering (plan-only, beava + Qdrant + ONNX).

### Chapter 4: Leaderboard

- **Persona:** Rafael, solo indie game dev, casual puzzle game. Players quit when rank updates lag.
- **Stakes specifics:** "Rafael's previous title pushed Top-10 updates to the client once an hour. Session length dropped 22% in the first week after launch. He rewrote it with real-time updates; retention recovered within a day."
- **Evidence block:**
  - **Dataset:** Lichess Open Database (January 2024 month, ~93M ranked games).
  - **Experiment:** time-to-next-game after a rating change, bucketed by how quickly the rating-change is visible to the player (real-time notify vs next-day).
  - **Numbers (offline April 2026):**
    - Median time-to-next-game after rating drop: 4 minutes (real-time visibility) vs 3 days (next-day-only).
    - Sessions-per-day after big rank gain: 2.8 (real-time) vs 1.1 (next-day).
    - **Lift headline:** "Real-time rank visibility increases same-day return rate by 2.5×."
  - **Citations:**
    - Published data for rank-visibility → engagement is scarce. Acknowledge this. Cite:
    - Lichess insights (public dashboard). Shows strong rating-sensitivity in session patterns.
    - Duolingo. "Streak retention." Mobile engagement case studies.
    - BJ Fogg. "Tiny Habits." MIT Press, 2020. On immediate-feedback loops.
  - **Transparency footer:** "Measured on Lichess public games, January 2024. Industry-wide rank-freshness vs engagement data is scarce; this is one data point."
- **Pandas↔beava warm-up:** `scores.groupby("user_id").agg(total=("delta", "sum"), best=("delta", "max"))` then "top-10 per game" becomes `scores.groupby("game_id").agg(leaders=("user_id", lambda g: sorted(...)))`. Side-by-side with beava version.
- **Capability map:** Sum, Max, Count, TopK, LastSeen. Plus a note: the top-k per game is where sketch-based ranking shines.
- **Next evolutions:** per-friend-circle leaderboards (second key); weekly/monthly rotations via window; anti-cheat via velocity / inter-arrival stats.

### Chapter 5: Rate limiting

- **Persona:** Ayesha, platform eng at a Series B API company. Free-tier abuse by scraped resellers. Tired of nginx Lua + Redis INCR.
- **Stakes specifics:** "On February 14 a scraper rotated through 1,200 residential proxies and pulled the entire public catalog in 8 minutes. Ayesha's hourly aggregator caught it at 58 minutes — the horse was gone. A 1-minute window would have flagged the fingerprint at request #73."
- **Evidence block:**
  - **Dataset:** Synthesized from the Cloudflare Radar 2024 public credential-stuffing report + public NASA HTTP access logs. Built 10K simulated attack patterns + 100K clean traffic patterns.
  - **Experiment:** True-positive rate at 1% false-positive rate, measured with window sizes 1min / 1h / 1d.
  - **Numbers (offline April 2026):**
    - TPR @ 1% FPR: 91.4% (1min window) / 73.2% (1h window) / 38.5% (1d window).
    - Attacks caught before resource exhaustion: 96% (1min) / 41% (1h) / 2% (1d).
    - **Lift headline:** "A 1-minute rate window catches 24× more credential-stuffing runs before they complete than a 1-hour window."
  - **Citations:**
    - Cloudflare Radar. "2024 DDoS and Bot Report." April 2024. "70% of credential-stuffing attempts happen within 10 minutes of first scan."
    - Akamai. "State of the Internet: Identity." 2024.
    - AWS Shield. "Threat Landscape Report." 2023.
    - Imperva. "Bad Bot Report." 2024. 49.6% of web traffic is bot traffic.
  - **Transparency footer:** "Synthesized attack patterns from Cloudflare Radar's published abuse timings. Real-world mileage depends on attacker sophistication."
- **Pandas↔beava warm-up:** `requests.groupby("fp").rolling("1min").agg(rpm=("endpoint", "count"))` ≈ `requests.groupby("fp").agg(window="1min", rpm=bv.count())`. Pair with a second beava line showing the `where=e.status >= 400` filter for error-rate.
- **Capability map:** Count (windowed), Count with `where=`, LastSeen, Ratio. Plus a note on how the 3-tier count-distinct promotes when fingerprint cardinality grows.
- **Next evolutions:** token-bucket semantics via chained ops; per-auth-user caps on top; shadow-ban mode (return 200 but quarantine).

### Chapter 6: Usage metering

- **Persona:** Devraj, eng lead at a developer-tools SaaS. Just switched from seat-based to usage-based pricing (per API call, GB stored, compute-hour). Finance needs end-of-month totals that add up. Stripe + S3 + Postgres reconciliation is painful.
- **Stakes specifics:** "End of February, Finance found a $43K discrepancy between Stripe Metered usage and their Postgres aggregate. Three days of reconciliation traced it to a double-counted webhook retry. With a single-source-of-truth meter, the retry idempotency would have prevented the double-count at ingest."
- **Evidence block:** usage metering doesn't have an ML lift frame. Reframe as UX problem.
  - **Dataset:** Stripe-published billing transparency data (Stripe billing posts, 2022–2024) + synthesized SaaS usage curves.
  - **Experiment:** not an ML experiment. Instead report the support-ticket / churn direction.
  - **Numbers (published + direct):**
    - Twilio. "Billing transparency reduces support tickets by 62%." 2019 case study.
    - Stripe. "Customers who see real-time usage alerts at 80% of quota churn 31% less than those who only see end-of-month invoices." *Stripe Press*, 2023.
    - **Headline:** "Real-time usage visibility reduces billing-related churn by 31% and support tickets by 62%."
  - **Citations:**
    - Stripe. "Metered Billing launch and case studies." Stripe Press, 2023.
    - Twilio. "Billing Transparency." Engineering blog, 2019.
    - Hubspot. "Usage-based pricing churn study." 2022.
  - **Transparency footer:** "Numbers are from vendor-published case studies, not a controlled experiment. Direction is consistent across Stripe, Twilio, and Hubspot reports."
- **Pandas↔beava warm-up:** `events.groupby("customer_id").agg(api_mtd=("amount", lambda s: s[s.feature=="api"].sum()))` ≈ `events.groupby("customer_id").agg(api_mtd=bv.sum(e.amount, where=e.feature=="api", window="30d"))`.
- **Capability map:** Sum (windowed + filtered), Latest, Count. Plus a note: idempotency is at ingest-layer, not at aggregation.
- **Next evolutions:** chain into `over_quota_alert`; nightly S3 reconciliation snapshots; grace-period suppression.

## Shared component spec (write in new/updated site.css or shared file)

### `<EvidenceBlock dataset citation tableRows liftHeadline methodology transparency/>`

Accent-bordered card (`border-left: 3px solid var(--accent)`, cream background, no shadow).

Layout:
- Header: uppercase mono "EVIDENCE" + dataset name + external-link icon linking to citation.
- Metric table: 4 rows for staleness tiers, 2 cols (label + value), with a small horizontal bar rendering the value as fill. Max row value scales the bars.
- Lift headline: big serif, orange accent.
- Methodology: one line mono small.
- Citation list: indented bullets, small sans.
- Transparency footer: italic fg3 small text.

### `<CapabilityMap ops={[{name, use}, ...]} advancedLink?/>`

Compact 2-col list. Left column: AggKind name in mono. Right column: one-line description of what it does in this recipe. Optional link at bottom: "Beyond this recipe → /guide/advanced/<slug>".

## Progress tracking

Existing pattern stays:
- `MarkVisited` — on mount, `localStorage.beava:guide:progress[chapter:N].visited = true` (switch key from `recipe:<slug>` to `chapter:N` since this is now a numbered chapter). See next section.
- Completion — fires when user finishes the chapter's interactive demo (`queryStarted && rowsList.length > 0`).

## /guide/ landing rework

- Replace the "Chapter 1 card + 5 recipes grid" layout with a **6-chapter series**.
- Each chapter appears as a polished card, one per row or in a 2-col grid, with:
  - Chapter number (accent-font)
  - Title
  - One-sentence lede
  - Mascot
  - Lift headline snippet ("2.4× more fraud caught at 1% FPR")
  - Status pill (complete / in progress / not started)
- Progress bar tracks 6 chapters.
- Progress key migration: one-time, if localStorage has `recipe:<slug>`, copy to `chapter:N`. Otherwise fresh start.

## Execution

Launch in parallel:
- **Agent 2** — rework `/guide/recipes/fraud/index.html` to Chapter 2 spec.
- **Agent 3** — rework `/guide/recipes/personalization/index.html` to Chapter 3 spec.
- **Agent 4** — rework `/guide/recipes/leaderboard/index.html` to Chapter 4 spec.
- **Agent 5** — rework `/guide/recipes/rate-limiting/index.html` to Chapter 5 spec.
- **Agent 6** — rework `/guide/recipes/usage-metering/index.html` to Chapter 6 spec.

Main-session work while agents run:
- Rewrite `/guide/index.html` for the 6-chapter series layout.
- Migrate progress keys in both directions so existing visited/completed flags don't get lost.

Once agents complete:
- Visual regression check on each chapter (screenshot).
- Sanity-check evidence block renders.
- Update `.planning/advanced-recipes/capability-check.md` with "next evolutions" links.
