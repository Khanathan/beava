# Launch copy & positioning — next phase

**Status:** locked positioning calls below; copy + chapter-2 fixes are the work to ship.
**Date:** 2026-04-28
**Supersedes:** earlier "real-time feature server" framing.

---

## 1. The locked positioning stack

Five layers, each narrowing the next. Use them stacked; never use one alone.

| Layer | Statement | Audience-filter purpose |
|---|---|---|
| **Noun phrase** | *"the live aggregation database"* | Tells the reader **what it is** in 4 words — narrow, database-shaped, no streaming-priesthood vocabulary. |
| **Temporal claim** | *"sliced by time, no streaming SQL"* | Disowns the SQL-graft category by name. Signals which side of the wall you sit on. |
| **In-category carve-out** | *"aggregation + serving — not ETL, CDC, joins, or workflows"* | Tells fraud / fintech / ML buyers **which slice of stream processing** beava covers. Saves them the eval. |
| **Cross-category carve-out** | *"the decisioning tier of stream processing — not data movement"* | Tells data-eng / platform buyers that beava **isn't replacing** their Debezium / Kafka Connect / dbt / Snowflake stack. Lowers the rip-and-replace fear. |
| **Era-specific wedge** | *"designed to be verified, not just authored"* | The agentic angle: the bottleneck is now reading, not writing. Beava's narrow scope + Python DSL + replay tooling makes it the verification-first product in this category. |

Stacked, the elevator pitch reads as:

> beava is the live aggregation database. Counts, sums, distincts, quantiles — sliced by time, queried in microseconds. Not ETL, not CDC, not a workflow engine; the decisioning-tier database that sits between your event source and your auth path. One Python file, one binary, no Kafka, no JVM, no streaming SQL.

---

## 2. The contrarian thesis

The launch hook — the line a journalist or HN commenter could quote:

> **Streaming was held back by SQL.** Real-time aggregation is a perfect fit for the dataframe mental model — group, window, count — but the streaming category bet on SQL because that was where the data engineers were. The result is a query language that's familiar in pieces and weird in the whole, and a category that never reached the millions of engineers who write app code in Python and TypeScript.

Use this as the **opener** of the launch blog post / HN submission. Everything else in the chapter and the homepage is a defense of that thesis.

---

## 3. Brand voice flavor

The personality lines — never the headline, always the warm Easter egg one click in:

- **Primary:** `damiligent.` (or `damiligently counting.`) — the dam pun is folded into the word, no streaming vocabulary.
- **Mascot caption:** `🦫 dam hard at work.`
- **Footer ribbon:** `made by people who are damn good at counting.`
- **`beava --version` output:** prints `damiligence v0.x.y` under the version number.
- **404 page:** *"This page got dammed up. Try /guide instead."*
- **`beava check` success line:** *"all damiligent."*
- **Discord channel:** `#damiligence`.

Retire `damn good at streams` from any customer-facing first-impression surface (hero, GitHub README first line, package descriptions, social bios). Keep it for swag / mascot panels / internal-team contexts where the audience already gets it.

---

## 4. Audience filter (who reads → who buys)

- **Primary buyer:** backend engineer at a Series-A-to-B fintech / marketplace / SaaS who is currently writing Redis + Lua for fraud, rate-limiting, recommendations, or live-feature serving and is unhappy.
- **Secondary buyer:** ML / applied-AI engineer who needs online feature serving for an agent or model and finds Tecton/Feast heavy and Redis-Lua brittle.
- **Excluded buyer:** data-platform team running Flink / Kafka / Confluent stacks and wanting a SQL-shaped streaming engine. They are the wrong eval; the carve-out paragraph should send them away politely.

The audience filter is intentionally narrow. The whole positioning stack is designed to *qualify* visitors in the first 30 seconds — the right buyer reads "live aggregation database, no streaming SQL" and says *"yes, that's me."* The wrong buyer reads the same line and bounces. Both outcomes are wins.

---

## 5. Homepage hero — copy to ship

```
The live aggregation database.

Counts, sums, distincts, quantiles — sliced by time, queried in microseconds.
Declare your aggregations as a Python function; events stream in over HTTP or TCP;
queries return in the auth path.

One binary. No Kafka. No JVM. No streaming SQL.

  brew install beava
  docker run beava/beava
  cargo install beava

🦫 damiligent.
```

A short code snippet immediately below — the canonical 8-line example so the reader sees the shape:

```python
import beava as bv

@bv.stream
class PaymentAttempt:
    user_id: str
    amount_cents: int
    status: str
    ts: int

@bv.table(key="user_id")
def UserRisk(e: PaymentAttempt):
    return e.agg(
        attempts_10m       = bv.count(window="10m"),
        unique_cards_1h    = bv.distinct(e.card_hash, window="1h"),
        amount_p95_30d     = bv.p95(e.amount_cents, window="30d"),
    )

bv.App("0.0.0.0:6400").register(PaymentAttempt, UserRisk).serve()
```

That snippet does five jobs in 14 lines: lists the operations, names the windowing primitive, shows the authoring surface, signals "no SQL," shows how the binary becomes a server.

---

## 6. The carve-out paragraph

Ship as a docs page (`/docs/architecture/scope.md`) and as a section in the homepage below the hero. This is the cross-category honesty surgery:

> **What beava is for, and what it isn't.**
>
> beava is the database for the *decisioning* tier of stream processing — the one that fits in the auth path and answers feature queries in microseconds.
>
> We don't do ETL — Kafka Connect, Airbyte, and Fivetran are great at that.
> We don't do CDC — use Debezium.
> We don't orchestrate workflows — Temporal does.
> We don't do general stream joins — Flink does.
> We don't run BI dashboards over event volume — ClickHouse, Pinot, and Druid do.
>
> What we do: keep per-entity rolling aggregates fresh and queryable in microseconds. One binary, in a Python DSL, with windowing.
>
> Use beava for the slice; use the right tool for the rest.

That paragraph is the single most useful piece of copy on the site. It does more buyer-qualification work than the hero. Don't bury it.

---

## 7. The HA story to commit publicly

Marcus and Sam (the platform-engineer and competing-CTO reviewers) both flagged the HA gap as the single biggest open question. The architecture supports the answer; we just need to commit to it in writing.

Ship `/docs/operations/ha.md` with this paragraph:

> beava ships a primary-replica HA model: the primary streams sealed WAL buffers to N async replicas (typically 1–2 same-AZ + 1 cross-region). Replicas reapply events through the same code path the primary uses, so state is bit-identical. **RPO is bounded by replication lag (typically < 100 ms in same-AZ deployments); RTO is < 5 seconds with the bundled sentinel.** Sync replication for RPO = 0 is available on the commercial tier with a same-AZ replica and a +500 µs P99 write-latency cost. Fencing tokens prevent split-brain during partition; the operator runbook for split-brain reconciliation is at `/docs/operations/split-brain`.

Tiering:
- **v0 OSS (today)**: single binary, no HA. Be honest.
- **v1 OSS**: async streaming WAL replica + manual `beavactl promote`.
- **v1.5 OSS**: bundled sentinel for auto-failover.
- **Commercial / cloud**: sync replica option, cross-region async chain, managed failover with SLA.

This stack is designed to **avoid the Redis-cluster ceiling**: routing baked into the binary (every node is a proxy), pipeline-aware sharding instead of opaque hash slots, no multi-shard atomic ops at the DSL level, topology in a small registry instead of a gossip mesh, and resharding as explicit dual-write cutover instead of live online slot migration. See `/docs/architecture/scale.md` for the full comparison.

---

## 8. The agentic verification surface

The wedge that makes the rest of the positioning compound. Ship four CLI tools to back up the "designed to be verified" claim:

1. **`beava check pipeline.py`** — static analysis. Validates schema, types, window bounds, key consistency, threshold sanity. Spike one-day primitive version first.
2. **`beava replay --events events.jsonl --pipeline new.py`** — feed historical events through a candidate pipeline; output rule-fire counts, decision histogram, precision/recall vs labeled set. Reuses the apply path; weeks of work.
3. **`beava sample N` + `beava run --local --pipeline new.py`** — pull N events from production into local file, run pipeline locally, inspect output. The DuckDB-shaped quick-iteration loop.
4. **`beava diff old.py new.py`** — semantic (not textual) diff: "added feature X, changed threshold Y from 4 to 3, removed unused feature Z." For agent-PR review.

Document these prominently in `/docs/agents.md` and in the homepage's third section ("the agent writes; you verify"). Show the four-line CLI loop:

```
$ /beava add per-card chargeback rate over 90d
✓ pipeline.py edited (1 feature added)
✓ beava check pipeline.py — passed (memory: +0.4 KB/entity)
✓ beava replay --events last-week.jsonl
  chargeback_rate_90d > 0.05 would have flagged 7 events
  (5 true positive, 2 false positive on labeled set)
  precision: 71% · recall: +12% over previous stack
✓ ready to ship
```

---

## 9. Chapter-2 fixes (the reviewer punch list)

Seven independent reviewers (fraud engineer, platform engineer, junior dev, ML/stats lead, CISO, product designer, competing CTO) read the current chapter. Cross-cutting issues to fix:

### 🔴 Blockers (page credibility)

1. **Welford "eviction-safe" claim contradicts the page's own Lua comment.** Pick one: name the actual sketch (KLL/DDSketch), or drop "Welford-style" and call it "recompute-from-window," or document parallel-Welford merge.
2. **`bv.p95` / `bv.distinct` algorithm unnamed.** Name the sketch family + ε guarantee + per-feature memory cost in the docs *and* in the chapter's annotation.
3. **Synthetic PR curve and 12% confusion-matrix base rate are quantitatively wrong** at realistic fraud base rates (0.1–2%). Cap precision-curve at 0.45; drop confusion-matrix base rate to 2/100. Or relabel both as "illustrative, not representative" with a link to "what these look like at 0.5% base rate."
4. **Heavy-tailed amount distributions break raw 3σ.** Switch the user-baseline rule to `z-score of log(amount)`, or median+MAD, or `amount > p99_30d`. Edit the rule + the prose.
5. **Latency table P99 is loopback + estimated AZ RTT, not real P99.** Either rerun on real network with real numbers + p999, or relabel rows as "loopback" and "estimated +AZ" with separate values.

### 🟠 Soon (production gap closures)

6. **Cold-start gate** (`min_n`, shrinkage prior) on z-score. Add to the rule + show in the simulator.
7. **Online/offline parity / point-in-time correctness.** Either ship the backfill story or note "PIT correctness in v1" explicitly.
8. **HA / recovery / multi-tenancy / schema migration story.** Ship `/docs/operations/*` and link from the chapter (see §7 above).
9. **Compliance / encryption-at-rest / SBOM / signed releases.** Ship `/docs/security.md` and link.
10. **Decision-rule thresholds.** Block at `unique_cards_1h ≥ 3 AND failed_10m ≥ 4`; bump `decline_rate_5m ≥ 0.5 AND attempts_10m ≥ 10` to BLOCK not REVIEW; explain the $500 floor on the z-score rule; bump `device.accounts_24h` from 5 to 8.
11. **Lua script bug** — ZADD member collision under same-millisecond duplicates. Add `event_id` as tiebreaker.

### 🟡 Editorial polish

12. **Cap the Redis Lua block at ~180 px** with a "show full script" reveal. The argument is "look how *much*," not "look at all this."
13. **Closing CTA box pivots to product-marketing in the last 200 px.** Tone the orange-wash card down; let the editorial register hold to the footer.
14. **Citation pill density** — cap at 2 per paragraph (DistinctCounts intro currently has 3).
15. **Color-orange budget exceeded** — mute inline `Mono` so the chart is the only orange landmark per viewport.
16. **Vocabulary glossary** — small inline tooltips or an appendix glossary for HLL, TDIGEST, MFMP, p95 sketch.
17. **Missing feature categories** Ana flagged: sequence/event-pattern features, BIN/geo-mismatch, device intelligence, chargeback feedback. Add an "honorable mentions" subsection with one paragraph each.
18. **Shadow-mode promotion** beat — name it in the rule-stack section; add a "shadow mode" toggle to the confusion-matrix chart.

---

## 10. Surface sweep (rename pass)

The product-noun shift cascades through every page:

| Currently called | Should become |
|---|---|
| "real-time feature server" | the **live aggregation database** |
| "pipeline" | the **schema** (or *"your aggregations"*) |
| "stream" | the **input table** (or just *"events"*) |
| "table" | the **aggregation** (or *"live table"*) |
| "feature" | the **column** (or *"the aggregate"*) — domain word "feature" stays in fraud/ML contexts |
| "register" | **declare** |
| "feature query" | **fetch** (or `bv.batch(...)`) |
| "feature server" | the **database** |

Avoid: "streaming," "watermark," "topic," "consumer group," "exactly-once," "EMIT CHANGES." These signal the wrong category.

A grep for the old vocabulary across the site is a one-day editorial pass.

---

## 11. Launch sequencing (suggested)

Two-week ship window:

**Week 1 — credibility surgery (the four 🔴 blockers + the latency-table honesty surgery + the Lua tiebreaker).** This is non-negotiable; without it Mira and Marcus would publicly dunk if the page got HN traction.

**Week 2 — positioning landing:**

- Day 1–2: rewrite homepage hero around the locked stack.
- Day 2–3: write `/docs/architecture/scope.md` (the carve-out paragraph) and `/docs/operations/ha.md` (the HA story).
- Day 3–4: spike `beava check` in its primitive form; ship the four-line CLI demo screenshot for the homepage.
- Day 4–5: chapter-2 editorial polish (Lua block cap, closing seam, color budget, glossary).
- Day 5: surface sweep — grep + rename pass on old vocabulary.

**Week 3 (optional, post-launch) — content campaign:**

- HN submission: *"Streaming was held back by SQL"* — the contrarian-thesis blog post.
- Side-by-side `/why-not-sql` page: Flink vs ksqlDB vs Materialize vs beava implementing the same feature.
- A `/docs/agents.md` page: the verification-first wedge, the four CLI tools, the agentic-loop screenshot.

---

## 12. Open questions to resolve before launch

These are gating; pick answers before the homepage rewrite ships.

1. **Sketch algorithm for `bv.p95` and `bv.distinct`.** KLL? DDSketch? t-digest? exact for small N then sketch beyond? Pick + document + name the ε guarantee and memory cost. *Owner: core team.*
2. **Per-entity memory accounting.** The "7 KB per entity for a 30-feature pack" claim depends on the sketch choice. Recompute and publish.
3. **Cold-start policy for z-score.** `min_n` gate? Shrinkage to a population prior? Document.
4. **Stddev semantics under window eviction.** Welford-merge? Recompute? Both? Document one and remove the contradicting line.
5. **What "live" means w.r.t. point-in-time correctness for offline backfills.** Do we ship `as_of=` for the offline path, or do we honestly say "online only in v0, backfill in v1"?
6. **Multi-tenancy story.** Single-binary-per-tenant (the operator runs many)? Or shared with a tenant primitive? Lock before pricing the cloud tier.
7. **Cloud-tier pricing.** $20–$200/mo SMB shape? Banner currently says $50/mo + waitlist; commit to a tier.
8. **The first three reference customers.** Who's running beava in production we can name? (Even one is enough for the homepage social-proof bar.)

---

## 13. What this doc commits us to (and what it doesn't)

**Commits to:**

- The five-layer positioning stack as the ONLY way we describe beava externally.
- Retiring "feature server" / "streaming" vocabulary across the site.
- Shipping the carve-out paragraph (§6) and the HA story (§7) as docs, not just talking points.
- The four agentic verification CLIs as a real product surface, not marketing copy.
- Fixing the chapter-2 blockers before any external promotion of the chapter.

**Doesn't commit to:**

- Streaming SQL parity. We won't ship a SQL frontend on the write path, ever. (Read-side SQL is open for v1+.)
- Multi-key cross-shard atomic operations.
- Tumbling / session / hopping windows in v0. (Sliding-only is the deliberate scope.)
- Active-active multi-region replication. (Async DR chain only.)
- A managed cloud beyond the SMB tier in v0.

---

**Last touched:** 2026-04-28. Next checkpoint: after week-1 credibility surgery; reassess the launch-sequence items 4-5 once `beava check` is spiked.
