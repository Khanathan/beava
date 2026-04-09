# Feature Research

**Domain:** Real-time feature server (streaming aggregations, ML feature serving, fraud detection)
**Researched:** 2026-04-09
**Confidence:** HIGH (corroborated across Tecton, Feast, Hopsworks, Redis, Fennel documentation and ecosystem analysis)

---

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist. Missing these = product feels incomplete or untrustworthy.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Windowed count aggregation | Core velocity check for fraud: "how many txns in last 30m?" Every fraud detection system needs this. | LOW | Bucketed ring buffer; count per window granularity bucket |
| Windowed sum aggregation | Amount totals over time windows — staple of fraud and risk scoring | LOW | Same bucketed approach as count |
| Windowed average | avg spend per window is table stakes for anomaly detection (amount_vs_avg feature) | LOW | Maintain sum + count per bucket, divide on read |
| Windowed min/max | Outlier detection ("is this transaction the largest in 24h?") | LOW | Per-bucket min/max tracking |
| Last-seen value capture | "What country was the last transaction from?" — context features, not aggregates | LOW | Single value + timestamp; no window |
| Sliding window semantics | Users expect windows that move with time — not fixed hourly buckets | MEDIUM | Bucketed ring buffer with configurable bucket granularity approximates sliding |
| Synchronous push-through | Push event, immediately get back computed features — required for <100ms inference paths | HIGH | Core architectural constraint; cannot be eventually consistent |
| Feature read by entity key | GET features for user_id / merchant_id — basic serving path | LOW | HashMap lookup on entity key |
| Pipeline/stream definition registration | Users need to declare what features exist before pushing events | MEDIUM | Validated at registration; rejects bad derive expressions early |
| Persistent TCP connections | HTTP overhead is too large for hot-path latency targets. Users building fraud detection expect sub-ms, not sub-50ms | MEDIUM | Binary protocol over persistent connections; Redis-style |
| Python SDK for stream definition | ML engineers work in Python. They expect to declare streams as Python classes/decorators, not raw JSON | MEDIUM | SDK serializes to JSON for server; Python never on hot path |
| Crash recovery / persistence | "Zero ops" means the server must survive a restart without losing all state | HIGH | Snapshot-based (Redis RDB model); acceptable to lose ~30s |
| Health check endpoint | Required for production deployments — load balancers, k8s readiness probes | LOW | HTTP GET /health |
| Direct feature write (SET/MSET) | Batch-computed offline features (lifetime value, segments) must be injectable | LOW | Bypasses pipeline engine; lands as StaticFeature |
| TTL-based key eviction | Memory must be bounded; inactive entity keys must expire | MEDIUM | Evict after 2x largest window with no events |

### Differentiators (Competitive Advantage)

Features that set this product apart from general-purpose tools (Redis, Flink) or heavyweight platforms (Feast, Tecton).

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Zero infrastructure (single binary) | Feast needs Redis + Kafka + compute cluster. Tally is one binary. Drastic reduction in ops burden for smaller teams | HIGH (architectural constraint, not a feature to implement) | Achieved by refusing to depend on external systems |
| Derived expression evaluation (string DSL) | `failure_rate = failed_tx_30m / tx_count_30m` — no Lambda, no Pandas. Computed in Rust on every event | HIGH | Expression parser + AST + evaluator in Rust; strings keep Python off hot path |
| Cross-stream views | `UserRisk.tx_to_login_ratio = Transactions.tx_count_1h / Logins.login_count_1h` — multi-stream derived features without ETL joins | HIGH | View recomputation on any upstream PUSH |
| Cross-key lookups | `merchant_chargebacks = lookup(MerchantActivity.chargeback_count_24h, on=merchant_id)` — entity graph enrichment in one request | HIGH | HashMap lookup into another stream's entity state |
| Event fan-out to multiple streams | One PUSH event updates both user and merchant state atomically — eliminates dual-write complexity | MEDIUM | Single event dispatched to all streams containing matching keys |
| Approximate distinct count (HyperLogLog) | `unique_merchants = st.distinct_count("merchant_id", window="24h")` — bounded memory (~12KB per key) with known error bounds | MEDIUM | HLL with 14-bit precision; ~0.8% standard error; well-understood |
| Conditional/filtered aggregations | `failed_tx_30m = st.count(window="30m", where="status == 'failed'")` — filter events at aggregation time | MEDIUM | Where clause uses same expression evaluator as derive |
| Synchronous feature response on PUSH | Other systems are eventually consistent. Tally returns updated features in the same request-response cycle | HIGH (core architectural constraint) | Drives the single-threaded event loop design |
| Reusable Python mixins for streams | `class Transactions(VelocityMixin, AmountMixin)` — composable feature groups for reuse across streams | LOW | Resolved in Python SDK at registration time |
| Prometheus metrics endpoint | Production teams expect `/metrics` out of the box — no custom monitoring integration needed | LOW | HTTP secondary port; include event count, latency histograms, memory |
| Debug endpoint for operator internals | `GET /debug/key/:key` exposes raw bucket values, HLL state — essential for validating window semantics | LOW | Only useful on management port; not on hot path |
| MSET chunked cooperative yielding | Bulk-loading 100K keys doesn't block live PUSH/GET traffic | MEDIUM | Process in 1024-key chunks, yield to event loop between |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem like good ideas but create disproportionate complexity for this product's positioning.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Distributed / cluster mode | "What if I outgrow one node?" | Requires consensus, distributed state, complex rebalancing — destroys the "zero ops" promise and single-binary distribution | Document client-side key sharding pattern; 100K+ events/sec is sufficient for most use cases |
| Session windows | Familiar from Flink/Kafka Streams | Session windows require unbounded state accumulation per key (gaps unknown in advance) and complicate eviction | Sliding windows with configurable bucket granularity cover the vast majority of fraud feature patterns |
| Point-in-time correct historical replay | Feature stores like Feast/Tecton emphasize this for training data | Requires append-only event log, time-travel queries, massive storage — fundamentally changes architecture from serving to storage system | Tally serves real-time. Offline/historical feature computation stays in batch systems (Spark, dbt). No confusion between training and serving |
| WAL / full durability | "I want zero data loss on crash" | Write-ahead log adds latency to every PUSH event — directly violates <100µs p99 target | Snapshot-based recovery (30s loss window) is the explicit tradeoff. Document it clearly |
| Schema evolution without restart | "Add a new feature without downtime" | Mid-flight state reinterpretation is complex; partially-populated keys have mixed schema | Post-v1. Design migration protocol once basic schema is stable |
| Multi-tenancy / namespace isolation | "I want one server for multiple teams" | Adds routing complexity, quota enforcement, auth — all off hot path but substantial engineering | Run separate instances per tenant; cost is low for a single binary |
| Arbitrary SQL queries | Some users want `SELECT * FROM features WHERE ...` | SQL engine on top of event state contradicts the feature serving model; much better served by a real database | Expose debug endpoints; direct serving is always by entity key, not query |
| Model serving / inference | "While you have features, run the model too" | Out of scope for a feature server; conflates two very different concerns | Integrate with separate inference servers (BentoML, Triton, FastAPI); Tally provides the features |
| Kafka/Flink as input sources | "I already have Kafka, can Tally consume it?" | Introduces Kafka dependency, negating zero-infrastructure positioning | PUSH via TCP from any consumer; Kafka consumer is a thin wrapper users can write themselves |

---

## Feature Dependencies

```
[In-memory state store (HashMap<EntityKey, EntityState>)]
    └──required by──> [Windowed operators (count, sum, avg, min, max)]
    └──required by──> [Last operator]
    └──required by──> [StaticFeature (SET/MSET)]
    └──required by──> [TTL-based key eviction]
    └──required by──> [Snapshot persistence]

[Windowed operators]
    └──required by──> [Derive expression evaluation]
                          └──required by──> [Cross-stream views]
                                               └──required by──> [Cross-key lookups]

[Expression parser/evaluator]
    └──required by──> [Derive expressions]
    └──required by──> [Where-clause filtering (conditional aggregations)]

[Pipeline registration (REGISTER command)]
    └──required by──> [All operators] (stream schema must be known before events arrive)
    └──required by──> [Expression validation] (expressions validated at registration time)

[TCP server (tokio)]
    └──required by──> [PUSH, GET, SET, MSET, REGISTER commands]

[PUSH command]
    └──required by──> [Synchronous push-through]
    └──required by──> [Event fan-out to multiple streams]
    └──enhanced by──> [Derived expression evaluation] (returns derive results in PUSH response)
    └──enhanced by──> [Cross-stream view recomputation] (views updated on PUSH)

[Distinct count (HyperLogLog)]
    └──standalone operator, no additional dependencies beyond state store

[Snapshot persistence]
    └──requires──> [serde + bincode serialization of all OperatorState types]
    └──requires──> [All operators defined before snapshotting can begin]

[Python SDK]
    └──requires──> [REGISTER command] (SDK serializes stream definition to JSON for server)
    └──requires──> [PUSH/GET/SET/MSET commands] (SDK wraps all protocol commands)
    └──enhanced by──> [Cross-stream views, derive, lookup] (higher-level abstractions in SDK)

[HTTP management API]
    └──standalone] (separate port, not on hot path)
    └──enhanced by──> [Prometheus metrics] (exposed via /metrics)
    └──enhanced by──> [Snapshot state] (manual trigger via POST /snapshot)
```

### Dependency Notes

- **State store is the foundation:** Every feature operator, every command, persistence, and eviction all depend on the in-memory HashMap. It must be designed before anything else.
- **Expression parser required before derive or where clauses:** Cannot evaluate `failure_rate = failed_tx_30m / tx_count_30m` without a working parser/AST/evaluator. Filtered aggregations (`where="status == 'failed'"`) use the same evaluator.
- **Cross-stream views require derive expressions:** Views are computed derives across stream boundaries. Both depend on the expression evaluator.
- **Cross-key lookups require cross-stream views:** Lookups extend views by resolving entity keys at query time — they are the most complex feature and must come last.
- **Snapshot requires all operator types defined:** bincode serialization schema must be stable. Introduces strong ordering constraint: operators must be complete before snapshot is safe to enable.
- **Pipeline registration (REGISTER) must precede PUSH:** Server rejects events for unregistered streams. This constraint drives the SDK's `app.register(...)` before `app.push(...)` pattern.
- **HyperLogLog is independent:** `distinct_count` has no dependency on other operators. Can be implemented in any phase after the basic state store exists.

---

## MVP Definition

### Launch With (v1)

Minimum viable to validate the core value proposition: "push event, get features back synchronously with zero infrastructure."

- [x] In-memory state store — no store, no features
- [x] Windowed count, sum, avg operators — core velocity features; most fraud detection patterns require these three
- [x] Expression parser + derive evaluation — failure_rate, velocity_spike patterns require derived features; without this, Tally is just a counter
- [x] TCP server + binary protocol (PUSH, GET, SET, REGISTER) — the hot path
- [x] Synchronous push-through — the core value proposition; without this, Tally is Redis with extra steps
- [x] Pipeline registration (REGISTER command) — required before any events can be processed
- [x] Python SDK with @st.stream decorator and TCP client — ML engineers won't adopt a server-only tool
- [x] Snapshot persistence + crash recovery — "zero ops" promise fails without crash recovery
- [x] TTL-based key eviction — memory unboundedness is a production blocker
- [x] HTTP health check — required for any production deployment integration

### Add After Validation (v1.x)

Features to add once core push-through is working and validated in real fraud detection scenarios.

- [ ] min/max operators — useful but count/sum/avg covers the vast majority of initial use cases
- [ ] distinct_count (HyperLogLog) — `unique_merchants` is a strong fraud signal; add after base operators proven
- [ ] last operator — `last_country`, `last_merchant` context features; simple to add
- [ ] Where-clause filtered aggregations — `failed_tx_30m = count(where="status == 'failed'")` is highly requested; needs expression evaluator to already work
- [ ] Cross-stream views — `UserRisk.tx_to_login_ratio` patterns; requires derive expressions to be proven stable first
- [ ] Cross-key lookups — `merchant_chargebacks` enrichment; highest complexity; validate simpler features first
- [ ] Event fan-out to multiple streams — needed once users have both user and merchant streams
- [ ] MSET chunked yielding — needed once bulk batch writes are a real use case
- [ ] HTTP management API (full CRUD for pipelines, metrics, debug) — management endpoints post-launch

### Future Consideration (v2+)

Features to defer until product-market fit is established and v1 is validated in production.

- [ ] Key-partitioned multi-threading — vertical scaling; single thread handles 100K+ events/sec which covers most use cases
- [ ] Schema evolution (add/remove features without restart) — complex; design once base schema is stable
- [ ] Incremental snapshots — optimization; periodic full snapshots are sufficient for v1
- [ ] Batch GET (MGET) — convenience; single GET is sufficient for v1 validation
- [ ] Multi-tenancy / namespace isolation — run separate instances for now
- [ ] Session windows — niche; sliding windows cover fraud detection patterns well
- [ ] Client-side sharding documentation — needed only once users outgrow single node

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| In-memory state store | HIGH | MEDIUM | P1 |
| Windowed count/sum/avg operators | HIGH | MEDIUM | P1 |
| Derive expression evaluator | HIGH | HIGH | P1 |
| TCP server + binary protocol | HIGH | MEDIUM | P1 |
| Synchronous push-through | HIGH | MEDIUM | P1 |
| Pipeline REGISTER command | HIGH | MEDIUM | P1 |
| Python SDK (@st.stream, client) | HIGH | MEDIUM | P1 |
| Snapshot persistence + recovery | HIGH | HIGH | P1 |
| TTL-based key eviction | HIGH | LOW | P1 |
| HTTP health check | HIGH | LOW | P1 |
| min/max operators | MEDIUM | LOW | P2 |
| distinct_count (HyperLogLog) | MEDIUM | MEDIUM | P2 |
| last operator | MEDIUM | LOW | P2 |
| Where-clause filtered aggregations | HIGH | LOW | P2 (depends on expression evaluator) |
| Cross-stream views | HIGH | HIGH | P2 |
| Cross-key lookups | MEDIUM | HIGH | P2 |
| Event fan-out | MEDIUM | MEDIUM | P2 |
| MSET chunked yielding | MEDIUM | MEDIUM | P2 |
| HTTP management API (full) | MEDIUM | MEDIUM | P2 |
| Prometheus metrics | MEDIUM | LOW | P2 |
| Debug key endpoint | LOW | LOW | P2 |
| Key-partitioned multi-threading | LOW | HIGH | P3 |
| Schema evolution | LOW | HIGH | P3 |
| Incremental snapshots | LOW | HIGH | P3 |
| Session windows | LOW | HIGH | P3 |
| Multi-tenancy | LOW | MEDIUM | P3 |

**Priority key:**
- P1: Must have for v1.0 launch
- P2: Should have; add in v1.x after core validated
- P3: Future consideration; defer until PMF established

---

## Competitor Feature Analysis

| Feature | Feast | Tecton | Hopsworks | Redis (raw) | Tally (planned) |
|---------|-------|--------|-----------|-------------|-----------------|
| Windowed aggregations | Via Spark/Flink offline; online is pre-computed | Native streaming aggregations (minute granularity) | Via Flink pipelines | No (requires custom code) | Native, built-in, bucketed ring buffer |
| Sub-millisecond serving | Via Redis online store | Via low-latency serving layer | Via RonDB (<1ms) | Yes (native) | Yes (in-memory HashMap, binary protocol) |
| Synchronous push-through | No (async materialization) | No (eventual consistency) | No (async materialization) | No (no pipeline) | YES — core differentiator |
| Expression DSL / derived features | Python transforms (heavy) | Python + Spark (heavy) | Python/Flink (heavy) | No | String-based; parsed server-side; evaluated in Rust |
| Cross-entity lookups | Via offline joins | Via streaming joins | Via Flink joins | Manual | Native st.lookup() |
| Zero infrastructure | No (needs Redis + Kafka + compute) | No (managed, cloud-dependent) | No (needs Flink + RonDB) | Partial (needs Redis cluster) | YES — single binary |
| Python SDK declarative API | Yes | Yes | Yes | No | Yes (@st.stream decorator) |
| Crash recovery | Depends on backing stores | Managed (provider handles) | Managed (provider handles) | RDB/AOF | Periodic snapshots (bincode) |
| Distinct count (HLL) | No built-in | Yes | Yes (via Flink) | No | Yes (native HyperLogLog) |
| Batch write (SET/MSET) | Yes (materialization) | Yes (materialization) | Yes (materialization) | Yes (native) | Yes (bypasses pipeline engine) |
| Cost/Ops overhead | HIGH (self-hosted complexity) | HIGH (managed cost) | MEDIUM (managed) | LOW (simple) | LOWEST (single binary) |

---

## Sources

- [Real-Time Aggregation Features for ML (Tecton/TDS)](https://towardsdatascience.com/real-time-aggregation-features-for-machine-learning-part-1-ec7337c0a504/)
- [Top 5 Feature Stores in 2025: Tecton, Feast, and Beyond](https://www.gocodeo.com/post/top-5-feature-stores-in-2025-tecton-feast-and-beyond)
- [Feature Store: Feast vs Tecton vs Redis (Calmops)](https://calmops.com/ai/feature-store-feast-tecton-redis/)
- [Redis Feature Store for Fraud Detection](https://redis.io/blog/outsmarting-fraud-in-real-time-how-redis-powers-intelligent-fraud-detection/)
- [Hopsworks Feature Store — Real-Time Serving](https://www.hopsworks.ai/product-capabilities/feature-store)
- [Hopsworks Definitive Guide to Feature Stores 2024](https://www.hopsworks.ai/news/the-definitive-guide-to-feature-stores-in-2024)
- [Fennel joins Databricks (acquisition context)](https://www.databricks.com/blog/fennel-joins-databricks-democratize-access-machine-learning)
- [Windowing in Kafka Streams — Confluent](https://www.confluent.io/blog/windowing-in-kafka-streams/)
- [Windowing in Apache Flink — Conduktor](https://www.conduktor.io/glossary/windowing-in-apache-flink-tumbling-sliding-and-session-windows)
- [Solving Training-Serving Skew with Feast](https://medium.com/@scoopnisker/solving-the-training-serving-skew-problem-with-feast-feature-store-3719b47e23a2)
- [Point-in-Time Correctness for Feature Data](https://apxml.com/courses/feature-stores-for-ml/chapter-3-data-consistency-quality/point-in-time-correctness)
- [HyperLogLog — Wikipedia](https://en.wikipedia.org/wiki/HyperLogLog)
- [Velocity Checks for Fraud Prevention — Stripe](https://stripe.com/resources/more/what-is-a-velocity-check-in-payments-what-businesses-should-know)
- [Velocity Signals — Fingerprint](https://fingerprint.com/blog/product-update-velocity-signals/)
- [RisingWave vs Materialize — Streaming Feature Stores](https://materialize.com/blog/real-time-feature-store-with-materialize/)
- [RisingWave Events API (no-Kafka streaming)](https://risingwave.com/blog/risingwave-events-api-stream-events-without-kafka/)
- [Feature pipelines deep dive — system engineering tradeoffs](https://medium.com/data-for-ai/feature-pipelines-and-feature-stores-deep-dive-into-system-engineering-and-analytical-tradeoffs-3c208af5e05f)

---
*Feature research for: Real-time feature server (Tally)*
*Researched: 2026-04-09*
