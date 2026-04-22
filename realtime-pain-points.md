# Why Real-Time Systems Are Hard

*A field guide for non-technical cofounders — with 2026 numbers*

---

## Why this matters for you

When your eng team says "we're going to build real-time features" or "we should adopt Kafka," your instinct is probably to say "great, ship it." This doc is so you know what they're actually walking into — and why the timeline, headcount, and budget ask is usually much bigger than it sounds.

I've watched this break three different ways at three different companies. Then I looked at what the industry data says. My stories aren't unusual — they're the norm.

---

## Part 1: The three shapes of pain (what I lived)

### 1. Evaluation paralysis — the six-month decision

At Faire, I watched a senior engineer spend six months just deciding which Flink vendor to buy.

Not implementing. Not shipping. *Deciding.*

The real-time systems market is crowded: Confluent, Ververica, Redpanda, Materialize, Tecton, Feast, Hopsworks, RisingWave. Each one has different trade-offs, different pricing, different lock-in. There's no "just pick one" — pick wrong and you pay for it for five years.

**What this looks like to you:** your engineer asks to spend weeks doing "vendor eval." Six months later you haven't shipped anything real-time. The real cost isn't the salary — it's the features your competitors shipped while your team was in Notion documents comparing SLA terms.

### 2. Operational burden — "I'm the streaming team now"

At Fennel, we built a real-time system in-house. Sounds great — we owned it, could customize it, no vendor lock-in.

Turns out even at 10,000 events per second — which isn't that big — running a Kafka + Flink + Redis stack eats one engineer's week, every week. They're not building features; they're keeping the pipes clean. When it breaks at 2am, they get paged. When a node runs out of memory, they learn what "checkpointing semantics" means.

**What this looks like to you:** you hired an ML engineer to build recommendation models. Six months in, they're a streaming engineer. They're good at it now. They can't do anything else.

### 3. Time-to-first-value — "we killed it because it took too long"

At Viggle I spearheaded a recommendation system. It was going to be the right thing. We killed it anyway.

The first working version was six months out. The company couldn't wait six months to find out if it would move any metrics. Leadership deprioritized it. The decision wasn't "this is a bad idea" — it was "we can't afford the time cost to find out."

**What this looks like to you:** you ask your team to "add personalization" or "try ranking." Three months in, it's still not live. Six months in, you're wondering if you should cut scope or cut the project. Most get cut.

---

## Part 2: What the rest of the industry looks like

My stories aren't unusual. Here's what the 2026 data shows.

### The real cost of running Kafka

Streamkap published a breakdown of DIY CDC infrastructure costs in 2025. For a "medium" deployment (5-20 data sources), total cost of ownership runs **$544K-$924K per year**:

| Deployment | Engineer FTE burn | Annual TCO |
|---|---|---|
| Small (1-5 sources) | 0.5-1.0 FTE | $90K-$250K |
| **Medium (5-20 sources)** | **1.5-2.0 FTE** | **$544K-$924K** |
| Large (20+ sources) | 2.5-4.0 FTE | $450K-$1M+ |

The medium TCO breakdown: $124K infrastructure + $270K-$500K engineer time + $30K-$60K on-call + $20K-$40K recruiting + $100K-$200K *opportunity cost* (features not built).

Direct quote from the analysis: *"Every hour an engineer spends rebalancing Kafka partitions or debugging a Debezium connector offset issue is an hour they are not spent on product development."*

Put differently: for a growth-stage company, running Kafka in-house is a half-million-dollar-a-year commitment on top of whatever infra it serves.

### The "it happened to PagerDuty" story

On August 28, 2025, PagerDuty — a company whose entire business is being online — suffered a 9-hour Kafka outage. At peak, **95% of events were rejected** for 38 minutes; 18% of create requests errored for over two hours.

Root cause: a new feature instantiated a new Kafka producer per API request instead of reusing one. Kafka ended up tracking 4.2 million extra producers per hour — 84× normal. JVM heap exhausted. Cascading failure across dependent services. (The status page itself failed to post updates publicly.)

The lesson isn't "PagerDuty is incompetent." It's: **Kafka is a distributed system with non-obvious failure modes, and even teams that live and breathe operations get bitten.** If we adopt it, we take on that class of incident.

### The hiring market

The engineers who can run this stuff are expensive and rare:

- Kafka engineer average: **$144,850/year** (talent.com, 2025)
- Senior streaming data engineer: **$200K+** (ZipRecruiter, April 2026)
- The World Economic Forum's 2025 Future of Jobs Report lists big-data specialists as the *fastest-growing* technology role — projected >100% growth through 2030. Supply isn't keeping up.

If our answer to "who runs this" is "we'll hire someone," plan for a $200K+ hire on a 4-6 month search, possibly longer.

### The 2026 market: massive consolidation

Two big moves you should know about:

- **Tecton** (enterprise feature store, formerly valued at $900M) — acquired by Databricks, August/September 2025.
- **Fennel** (Python-native feature engineering) — acquired by Databricks, April 2025.

That's the two most credible ML feature store startups, both absorbed into a single platform in one year. For buyers this means:
- If we're already on Databricks, the feature store decision is essentially made.
- If we're not, our "best of breed" options just got thinner.
- Apache Pulsar, a Kafka competitor, is characterized in Kai Waehner's 2026 landscape report as having *"adoption stalled"* and is expected to "vanish from the picture."

Meanwhile the Apache Kafka protocol is still used by ~150,000 organizations and remains the de facto standard. It's not going anywhere. The question isn't whether Kafka wins — it has. The question is whether *we* should pay its cost.

### When is Kafka actually necessary?

A Hacker News thread from late 2025 ("Kafka is Fast — I'll use Postgres") surfaced the practical threshold. Consensus from experienced operators:

- Under **~3,000 events per second**, Postgres handles it. Probably for years.
- Kafka becomes the right tool around **hundreds of millions of events per day** (~thousands/sec sustained).
- Most organizations adopt it at **~100 events/second** — where a single Postgres node would handle the load fine.

One commenter: *"What I've found to be even more common than resume-driven development has been people believing that they either have or will have huge scale."*

Another: *"Their goal posts are off by a few orders of magnitude and they will never, ever have the sort of scale required for these types of tools."*

This matches my experience at Fennel: even at 10,000 events/sec, the ops cost of the full Kafka+Flink stack wasn't paying off against simpler alternatives.

---

## Part 3: The pattern

My three war stories aren't separate pains. They're the same pain showing up at different points in the timeline:

- **Before you start** — picking the right tool takes months
- **Once it's running** — keeping it running takes 1-2 full-time engineers
- **After shipping** — the first feature took so long the project almost died

The shared root cause: **real-time infrastructure was designed for companies with dedicated streaming teams. Most startups don't have one and can't afford to build one.**

---

## Part 4: What to ask your eng team before saying yes

If your eng lead proposes building or buying a real-time system, ask these six questions. Don't accept vague answers.

1. **"How long until we serve our first real-time feature to a real user?"**
   If the answer is more than 4 weeks, the project probably won't survive politically.

2. **"What does operating this look like in 6 months — whose week does it eat?"**
   Industry data says 0.5-2.0 FTE for small-to-medium deployments. If your team says "almost no time," press. They're guessing.

3. **"What's our actual expected event throughput — not at 100× scale, but in the next 12 months?"**
   If the answer is under ~3,000 events/second, Kafka is probably overkill. Under 1,000/sec, Postgres plus good indexing will do.

4. **"If this breaks at 2am, who gets paged, and do they know how to fix it?"**
   "We'll figure it out" means "we haven't figured it out." PagerDuty didn't figure it out.

5. **"What's the annual TCO including engineer time, infra, and opportunity cost?"**
   The honest answer for a medium deployment is $500K-$1M/year. Anything less is under-counting.

6. **"What's the cheapest version we can ship this month?"**
   Force a narrow wedge. "A rolling average of the last hour's events, per user, served via HTTP" is far cheaper than "a full streaming platform."

---

## Part 5: Yellow-flag phrases

Two things from your team are signals that scope is ballooning:

- **"We're setting up Kafka."** — Kafka is the right tool for a specific throughput problem most startups don't have. Under ~3,000 events/second, it's probably premature.
- **"We're evaluating Flink."** — Flink is the right tool when you need correct stream processing at scale *and* you have a team that speaks JVM. If either isn't true, you're buying complexity you won't pay off. Multiple 2025-2026 postmortems identify JVM tuning and checkpoint management as the top ops burden.

Neither is wrong as a decision. Both are wrong as a default.

---

## Part 6: The narrow version that usually works

At the scale most startups operate — tens of thousands of users, thousands of events per second — a single-process system with a Python or SQL interface will serve you for years. It runs on a laptop for development. It deploys as one binary. When it breaks, one engineer can read the logs.

2026 converged patterns for teams that don't have a streaming team:

- **Single-binary stream processors** (Redpanda, Timeplus Proton) — no JVM, lower ops surface.
- **SQL-first stream processing** (Flink SQL, RisingWave) — production-ready now; no custom Java jobs needed.
- **Managed services** (Confluent Cloud, DeltaStream) for teams who'd rather pay the cloud premium than staff an ops team.
- **Python-native feature frameworks** for ML teams who want to write features as functions rather than stand up a platform.

The goal isn't to be anti-Kafka. The goal is to not pay Kafka's tax until we're actually at Kafka's scale.

---

## If you only remember three things

1. **Real-time is a timeline and staffing risk, not just a technology choice.** The most common failure mode is running out of patience (or budget, or engineer-years) before the first feature ships.

2. **Operational cost is permanent and large.** Industry data says a medium real-time deployment costs $500K-$1M/year fully loaded. Every system we run takes engineer-weeks forever. Pick carefully.

3. **Ask for the narrow version.** The right question isn't "what platform should we use?" — it's "what's the smallest useful thing we can ship this month, on Postgres, and learn from?"

---

## Further reading (if you want to pressure-test me)

- **Streamkap, "DIY CDC Infrastructure Costs"** (2025) — the TCO breakdown.
- **PagerDuty engineering blog, August 28, 2025 Kafka outage postmortem** — the "it happens to the best of us" story.
- **Kai Waehner, "The Data Streaming Landscape 2026"** (Dec 2025) — vendor landscape, what's winning, what's dying.
- **AxonOps, "Kafka Cost Comparison 2026"** — self-hosted vs MSK vs Confluent Cloud, real dollar numbers.
- **Hacker News discussion: "Kafka is Fast — I'll use Postgres"** (late 2025) — the scale-threshold debate.
- **Databricks acquisition announcements** — Tecton (Aug 2025), Fennel (Apr 2025).

---

*Hoang Phan*
*April 2026*
