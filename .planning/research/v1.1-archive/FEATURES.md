# Feature Research: v1.1 Composable Pipeline & Event Log

**Domain:** Real-time feature server -- composable pipeline, event log, backfill, schema evolution, debug UI
**Researched:** 2026-04-09
**Confidence:** HIGH (corroborated across Flink, RisingWave, Materialize, Redis, Tecton, Feast ecosystem research)

**Scope:** This document covers ONLY the v1.1 milestone features. For v1.0 feature research, see git history.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Features users expect once Tally claims to support composable pipelines and event replay. Missing these makes the v1.1 claims feel hollow.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| DAG execution (events cascade through pipeline) | Every composable streaming system (Flink, RisingWave, Materialize) has topological execution. If you call it "composable pipeline," events MUST flow through declared dependencies automatically. | HIGH | Requires topological sort of stream definitions, cycle detection, and cascading event dispatch. Core v1 already has cross-stream views; this extends PUSH to trigger downstream streams, not just views. |
| Keyed streams with explicit `depends_on` | Flink's `keyBy()`, RisingWave's `CREATE MATERIALIZED VIEW AS SELECT ... FROM upstream` -- every composable system lets you declare "this computation depends on that one." LEFT JOIN semantics (nulls for missing upstream values) are standard. | HIGH | New stream type. Must declare upstream stream dependencies at registration time. Pipeline engine must route events from upstream to downstream after upstream operators are evaluated. |
| MGET (batch GET for multiple keys) | Redis MGET is the canonical batch read operation. Feature stores (Feast, SageMaker, Tecton) all support batch feature retrieval. Any production ML inference path fetches features for multiple entities in one call. | LOW | Wire protocol extension (new opcode). Internally loops over GET logic. Single-threaded so no parallelism needed -- just saves TCP roundtrips. |
| Schema evolution (add/remove features) | Flink supports adding POJO fields (initialized to defaults) and removing fields (dropped from future checkpoints). Snowpipe Streaming auto-adds new columns. Databricks Structured Streaming handles schema drift. Users WILL add new features over time -- requiring a full state reset is unacceptable for production systems. | HIGH | Adding features: initialize new operator with empty state for all existing keys (lazy on next event). Removing features: stop evaluating, drop from snapshots on next save. Snapshot format must handle version mismatches gracefully. |
| SSD append-only event log | Kafka's entire model is an append-only log. Redis AOF persists every write. RisingWave stores source data for backfill. Any system claiming replay/backfill capability needs durable event storage. | HIGH | Local disk, not S3 (zero infrastructure promise). Append-only writes with periodic fsync. ~100-300ns amortized per event fits within PUSH latency budget. Must be opt-in per stream (`history=True`) to avoid disk explosion on high-volume streams. |
| Configurable history TTL per stream | Redis AOF has auto-rewrite-percentage thresholds. Kafka has per-topic retention. Flink checkpoints have TTL. Without bounded retention, event log grows unbounded -- a production blocker. | MEDIUM | Per-stream TTL configuration. Background compaction process removes events older than TTL. TTL should default to largest window of that stream. |
| Entity state TTL per dataset | Already exists in v1.0 as a global TTL (2x largest window). Making it configurable per-stream/dataset is standard -- Feast has per-feature-view TTL, SageMaker has per-feature-group TTL, Spark 4.0 has OneToOneTTLState/OneToManyTTLState. | LOW | Extend existing TTL eviction to accept per-stream configuration. Falls back to current 2x-window default. |

### Differentiators (Competitive Advantage)

Features that make Tally's v1.1 uniquely valuable compared to alternatives.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Keyless streams (raw event ingestion to SSD log) | No other single-binary feature server has this. Flink has non-keyed DataStreams but requires a cluster. RisingWave has CREATE SOURCE but requires S3/Kafka. Tally can be the first zero-infrastructure system where raw events land on SSD and get transformed into keyed features downstream. This bridges real-time and batch feature computation in one binary. | HIGH | New stream type with no key_field. Events are appended to SSD log only (no in-memory operator state). Downstream keyed streams consume from keyless streams via `depends_on`. This is the foundation of the composable pipeline. |
| Backfill flag on new feature definitions | Tecton's killer feature: deploy a new Stream Feature View, and historical data is automatically replayed from batch source to populate the online store. No other single-binary system does this. With Tally's SSD event log, new features can be backfilled from local event history automatically. | HIGH | When a new feature is registered with `backfill=True`, replay matching events from the SSD event log through the new operator. Must handle: (a) reading from event log file, (b) replaying in chronological order, (c) cooperative yielding to not block live traffic, (d) marking backfill as complete. |
| Debug web UI (stream watching, memory, throughput, real-time values) | Redis has RedisInsight (separate process). Flink has a built-in web dashboard showing job DAG, throughput, backpressure, checkpoints. Materialize has Console with freshness dashboards and query analysis. Tally's differentiator: embed the debug UI directly in the single binary. No separate process, no extra deployment. | MEDIUM | Embedded static assets (rust-embed crate). HTML+JS served from HTTP management port. Features: stream topology DAG visualization, per-stream throughput counters, memory usage breakdown, entity state inspector, real-time value watch. Use htmx or vanilla JS + SSE for live updates -- no React/npm build step to keep it simple. |
| Incremental snapshot serialization | Flink's incremental checkpointing with RocksDB tracks SSTable deltas -- only new/changed files are persisted. Current Tally clones entire state for snapshots (up to 2x peak memory). Incremental snapshots would only serialize changed entities since last snapshot, dramatically reducing snapshot time and memory overhead. | HIGH | Dirty-tracking: mark entities as modified on PUSH/SET. On snapshot, serialize only dirty entities + a manifest of unchanged entities. On restore, load base snapshot + apply deltas. Flink's approach uses immutable SST files; Tally's approach can use a simpler dirty-flag per entity key. |
| Synchronous backfill response | Unlike Tecton (async batch job) or RisingWave (background backfill), Tally can report backfill progress and completion synchronously to the client, since everything is single-threaded and local. | LOW | Status endpoint showing backfill progress per stream. No distributed coordination needed. |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem natural for v1.1 but would create disproportionate complexity or contradict Tally's positioning.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Full WAL (write-ahead log) replacing snapshots | "If we have an event log, why not use it as the WAL for full durability?" | WAL on the hot path adds fsync latency to EVERY event, not just logged streams. The event log is opt-in per stream; making it mandatory kills latency for streams that don't need history. Redis explicitly separates AOF (durability) from RDB (snapshots) -- they serve different purposes. | Keep event log as opt-in replay source. Keep snapshots as crash recovery mechanism. They are complementary, not replaceable. |
| Arbitrary replay to any point in time | "Can I replay events from 3 days ago to get features as they were then?" | Point-in-time replay requires versioned state, timestamp-indexed event log scanning, and time-travel semantics. This turns Tally from a serving system into a storage/analytics system. Materialize and Flink handle this; Tally should not. | Backfill replays ALL events through NEW operators to compute CURRENT state. Not historical state reconstruction. |
| Distributed event log replication | "What if my disk fails?" | Replication requires consensus (Raft/Paxos), network overhead, and fundamentally changes from single-node to distributed system. | Local disk is the explicit tradeoff. Back up snapshot files externally. Event log is for replay, not durability guarantee. |
| Complex DAG transformations (map, filter, flatMap) | "Can keyless streams do arbitrary transformations before keying?" | General-purpose stream transformation operators (map/filter/flatMap) turn Tally into a mini-Flink. The complexity of supporting arbitrary UDFs, error handling, backpressure, etc. is enormous. | Keyless streams log raw events. Keyed streams with `depends_on` extract keys and aggregate. The `where` clause and `derive` expressions handle filtering and computation. No arbitrary code execution. |
| Multi-stream joins with temporal alignment | "Join Transactions and Logins by timestamp window" | Temporal joins require buffering, watermarks, late-event handling, and out-of-order event management. This is Flink's core complexity. | Cross-stream views with LEFT JOIN semantics (null for missing). Lookups resolve at query time, not at event time. This is simpler and sufficient for feature serving. |
| Event log as external Kafka-compatible API | "Can other systems consume from Tally's event log?" | Implementing the Kafka protocol or a compatible API is enormous scope. Tally's event log is internal implementation, not a distribution mechanism. | If events need to go elsewhere, users should publish to Kafka directly. Tally consumes, it doesn't produce. |
| Live schema migration of running operators | "Migrate existing window state when changing window size from 1h to 2h" | Changing window parameters requires reinterpreting bucket state, which is not mathematically sound for most operators (you can't extend a 1h ring buffer to 2h without the missing data). | Schema evolution covers adding/removing features. Changing parameters requires re-registration, which resets affected operators. Backfill from event log can then repopulate. |

---

## Feature Dependencies

```
[SSD Event Log]
    +-required by--> [Configurable History TTL per stream]
    +-required by--> [Backfill from event log]

[Keyless Streams]
    +-requires--> [SSD Event Log] (keyless streams ONLY write to event log, no in-memory state)
    +-required by--> [DAG Execution] (keyless streams are the entry point for composable pipelines)

[Keyed Streams with depends_on]
    +-requires--> [Existing pipeline engine] (extends current stream registration)
    +-required by--> [DAG Execution] (depends_on declarations define the DAG edges)
    +-enhanced by--> [Backfill from event log] (new keyed streams can backfill from upstream logs)

[DAG Execution]
    +-requires--> [Keyed Streams with depends_on] (DAG edges come from depends_on)
    +-requires--> [Topological sort + cycle detection] (new registration validation)
    +-enhanced by--> [Keyless Streams] (keyless -> keyed is the canonical composable pattern)

[Backfill from Event Log]
    +-requires--> [SSD Event Log] (must have events to replay)
    +-requires--> [Schema Evolution] (backfill creates new operators on existing entities)
    +-enhanced by--> [DAG Execution] (backfilled events cascade through downstream)

[Schema Evolution]
    +-requires--> [Snapshot format changes] (must handle missing/extra fields on restore)
    +-enhanced by--> [Backfill from event log] (new features can be populated from history)
    +-required by--> [Backfill from event log]

[Incremental Snapshots]
    +-requires--> [Dirty-tracking per entity] (must know what changed since last snapshot)
    +-independent of--> [Event log, DAG, keyless streams] (snapshot optimization only)

[MGET]
    +-independent of--> [All other v1.1 features] (pure protocol extension)

[Entity State TTL per Dataset]
    +-extends--> [Existing TTL eviction] (v1.0 already has global TTL)
    +-independent of--> [Event log, DAG, keyless streams]

[Debug Web UI]
    +-enhanced by--> [DAG Execution] (can visualize stream topology)
    +-enhanced by--> [SSD Event Log] (can show event log size, throughput)
    +-enhanced by--> [Incremental Snapshots] (can show snapshot delta stats)
    +-independent of--> [Core pipeline changes] (reads existing state/metrics)
```

### Dependency Notes

- **SSD Event Log is the foundation of v1.1:** Keyless streams, backfill, and history TTL all depend on it. Must be built first.
- **Keyless Streams require Event Log:** Without SSD storage, keyless streams have nowhere to put events (they don't have in-memory operator state).
- **DAG Execution requires depends_on declarations:** The DAG is defined by stream dependency declarations, not by implicit data flow. This is simpler than Flink's automatic graph construction from operators.
- **Backfill requires both Event Log AND Schema Evolution:** You cannot replay events into new features unless you can add features to existing streams without resetting state.
- **Schema Evolution and Backfill are tightly coupled:** Adding a new feature + automatically replaying from event log is the primary backfill use case. They should be designed together.
- **MGET and Debug UI are independent:** They can be built in any phase without blocking or being blocked by other features.
- **Incremental Snapshots are independent:** A pure optimization that can be done anytime after the core pipeline changes stabilize.

---

## MVP Definition

### This Milestone Must Ship (v1.1)

Core value: "Composable pipeline with event replay. Push raw events, they flow through a DAG of transformations into features."

- [ ] SSD append-only event log -- foundation for all replay/backfill; opt-in per stream via `history=True`
- [ ] Keyless streams -- raw event ingestion entry point for composable pipelines
- [ ] Keyed streams with `depends_on` -- declare upstream dependencies, LEFT JOIN semantics
- [ ] DAG execution -- events cascade through pipeline automatically after topological sort
- [ ] History TTL per stream -- bounded event log retention; defaults to largest window
- [ ] Schema evolution (add features) -- add new features without full state reset
- [ ] Backfill from event log -- replay historical events through new features
- [ ] MGET -- batch GET; trivial to implement, high user value for inference paths

### Add After Core Validated (v1.1.x)

Features that enhance the milestone but can follow the core delivery.

- [ ] Schema evolution (remove features) -- less urgent than add; users rarely remove features in production
- [ ] Entity state TTL per dataset -- extends existing global TTL; low complexity but low urgency
- [ ] Incremental snapshot serialization -- optimization; full snapshots still work fine
- [ ] Debug web UI -- high value but purely operational; does not affect pipeline correctness
- [ ] Event log compaction -- Redis-style AOF rewrite; history TTL handles most cases initially

### Defer to v1.2+ (Future Consideration)

- [ ] Complex DAG transformations (map/filter/flatMap between stages) -- keep it simple; derive + where covers most cases
- [ ] Event log as external API -- not a distribution mechanism
- [ ] Live schema migration (change window parameters) -- reset + backfill is the supported path
- [ ] Batch PUSH (MPUSH) -- useful but PUSH in a loop + pipelining works for now

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority | Dependencies |
|---------|------------|---------------------|----------|-------------|
| SSD event log (append-only, opt-in) | HIGH | HIGH | P1 | None (new subsystem) |
| Keyless streams | HIGH | MEDIUM | P1 | Event log |
| Keyed streams with depends_on | HIGH | HIGH | P1 | Existing pipeline engine |
| DAG execution (topological cascade) | HIGH | HIGH | P1 | Keyed streams + depends_on |
| History TTL per stream | HIGH | MEDIUM | P1 | Event log |
| MGET (batch GET) | HIGH | LOW | P1 | None |
| Schema evolution (add features) | HIGH | HIGH | P1 | Snapshot format changes |
| Backfill from event log | HIGH | HIGH | P1 | Event log + schema evolution |
| Schema evolution (remove features) | MEDIUM | MEDIUM | P2 | Snapshot format changes |
| Entity state TTL per dataset | MEDIUM | LOW | P2 | Existing TTL eviction |
| Incremental snapshots | MEDIUM | HIGH | P2 | Dirty-tracking system |
| Debug web UI | MEDIUM | MEDIUM | P2 | HTTP management API (exists) |
| Event log compaction | MEDIUM | HIGH | P2 | Event log + history TTL |

**Priority key:**
- P1: Must have for v1.1 milestone
- P2: Should have; can follow core delivery or be deferred to v1.1.x

---

## Competitor Feature Analysis (v1.1 Scope Only)

| Feature | Flink | RisingWave | Materialize | Redis | Tecton | Tally v1.1 (planned) |
|---------|-------|-----------|-------------|-------|--------|---------------------|
| **Composable pipeline** | Full DAG via DataStream API; keyBy() for keyed, map/filter for non-keyed | SQL-based: CREATE MATERIALIZED VIEW chains | SQL-based: CREATE MATERIALIZED VIEW chains | No pipeline; manual code | Managed pipeline; internal DAG | `depends_on` declarations; topological execution |
| **Non-keyed -> keyed composition** | DataStream -> keyBy() -> WindowedStream -> aggregate | CREATE SOURCE -> CREATE MV with GROUP BY | CREATE SOURCE -> CREATE MV with GROUP BY | N/A | Stream Source -> Feature View | Keyless stream -> keyed stream with `depends_on` |
| **Event log / replay** | Kafka-backed; full replay from topic offsets | S3/object store for source data; backfill on MV create | Uses Kafka/Postgres sources; replays from source | AOF (all writes); replay on restart | Batch source + stream source; auto-backfill from batch | Local SSD append-only log; opt-in per stream |
| **Backfill on new feature** | Stop job, add operator, restart from savepoint + Kafka replay | Automatic on CREATE MATERIALIZED VIEW (snapshot backfill) | Automatic on CREATE MATERIALIZED VIEW | N/A (no features) | Auto-backfill from batch_config; feature_start_time | `backfill=True` flag; replay from SSD event log |
| **Schema evolution** | POJO: add/remove fields. Avro: compatible changes. Keys cannot evolve. | ALTER TABLE ADD COLUMN (limited) | ALTER ... ADD COLUMN | No schema; unstructured | Managed; version-aware | Add/remove features without state reset |
| **History TTL / retention** | Via Kafka topic retention | Via source retention config | Via source retention | AOF rewrite + maxmemory | Managed retention | Per-stream history TTL with compaction |
| **Batch key read (MGET)** | N/A (not a serving system) | SQL: SELECT ... WHERE key IN (...) | SQL: SELECT ... WHERE key IN (...) | MGET (native) | Batch feature retrieval API | MGET (new TCP command) |
| **Incremental checkpoints** | RocksDB SSTable delta tracking; generalized in 1.18+ | Hummock LSM-based; incremental by design | Differential dataflow; inherently incremental | RDB snapshots (full) | Managed | Dirty-flag per entity; delta serialization |
| **Debug UI** | Built-in web dashboard: DAG, throughput, backpressure, checkpoints | Cloud console with query analysis | Console with freshness dashboard, EXPLAIN ANALYZE | RedisInsight (separate binary) | Managed console | Embedded in binary; stream topology, memory, throughput |
| **Zero infrastructure** | No (JVM cluster + Kafka + ZK) | No (requires S3 + etcd) | No (requires external sources) | Partial (single binary but no pipeline) | No (managed cloud) | YES -- all features in one binary |

### Key Takeaways from Competitor Analysis

1. **Composable pipelines are SQL-based in RisingWave/Materialize, API-based in Flink.** Tally's `depends_on` decorator approach is closest to Flink's explicit DAG but simpler -- no arbitrary operators, just declared dependencies between streams.

2. **Backfill is automatic in RisingWave/Materialize** (CREATE MV triggers it). Tecton auto-backfills from batch sources. Tally should match this: registering a new feature with `backfill=True` should automatically trigger replay from event log.

3. **RisingWave's backfill order control (2025)** is relevant: when backfilling a dependent MV, upstream data must be backfilled first. Tally's topological sort for DAG execution naturally provides this ordering.

4. **Flink's schema evolution is limited to POJOs and Avro.** Keys cannot evolve. Tally has an advantage: since operator state is a known enum (OperatorState), schema evolution is simpler -- add new variants, handle missing variants on deserialize.

5. **No competitor offers all of these in a single binary.** This remains Tally's core differentiator. The event log being local SSD (not S3/Kafka) is unique.

---

## Complexity Deep-Dive by Feature

### SSD Event Log
**What ecosystem does:** Kafka uses partitioned append-only logs with configurable retention. Redis AOF appends every write command, with periodic rewrite (compaction) using fork(). RisingWave stores source data in S3 Hummock.
**What Tally should do:** Single append-only file per stream (not per partition -- single-threaded). Periodic fsync (configurable, default every 1s or N events). Binary format using postcard for consistency with snapshots. File rotation when size exceeds threshold.
**Key design decisions:**
- One file per stream vs one global file? One per stream -- simplifies TTL and compaction per stream.
- fsync strategy? Batch fsync (every 1s or 1000 events, whichever comes first). ~100-300ns amortized.
- Format? Length-prefixed frames: `[u32 event_size][u64 timestamp][payload bytes]`. Simple, seekable.

### Keyless Streams
**What ecosystem does:** Flink has non-keyed DataStream (map, filter, flatMap). Kafka topics are non-keyed unless a key is specified. RisingWave CREATE SOURCE is non-keyed until you GROUP BY in a materialized view.
**What Tally should do:** New stream type declared with `@st.stream()` (no `key=` parameter). Events go to SSD event log only. No in-memory operator state. Downstream keyed streams declare `depends_on=[KeylessStream]` and specify which field to key by.

### DAG Execution
**What ecosystem does:** Flink compiles operator DAG, executes topologically. RisingWave compiles SQL into dataflow graph with recursive change propagation leaf-to-root. Materialize uses differential dataflow.
**What Tally should do:** At registration time, build a directed acyclic graph from `depends_on` declarations. Topological sort determines execution order. On PUSH to any stream: (1) execute that stream's operators, (2) cascade to all downstream streams in topological order. Cycle detection at registration time rejects invalid configurations.

### Backfill from Event Log
**What ecosystem does:** RisingWave automatically backfills on CREATE MATERIALIZED VIEW using snapshot backfill (default). Tecton runs batch backfill jobs from batch_config to feature_start_time. Flink requires stopping the job, adding operators, and restarting from a savepoint.
**What Tally should do:** When a new feature is registered with `backfill=True`: (1) scan event log for the relevant stream, (2) replay events in chronological order through the new operator only, (3) use cooperative yielding (like MSET chunking) to avoid blocking live traffic, (4) mark backfill as complete. RisingWave's backfill order control insight: if the new feature depends on upstream streams, backfill upstream first.

### Schema Evolution
**What ecosystem does:** Flink supports adding/removing POJO fields. New fields get Java defaults; removed fields are dropped. Keys cannot change. Snowpipe Streaming auto-adds columns. Databricks uses checkpointing to maintain state continuity through schema changes.
**What Tally should do:** Adding a feature: register new FeatureDef; on next event for an entity, create the new operator with empty/default state. Removing a feature: mark as removed in pipeline definition; stop evaluating; exclude from GET responses; drop from next snapshot. Snapshot format must handle version mismatches: if a snapshot contains a feature that no longer exists, skip it on load. If a feature exists in the definition but not the snapshot, initialize with default state.

### Incremental Snapshots
**What ecosystem does:** Flink tracks RocksDB SSTable deltas -- immutable files, only new ones uploaded. Spark uses delta + snapshot files -- delta stores changes, snapshot stores full state. Flink 1.18+ generalized this beyond RocksDB.
**What Tally should do:** Dirty-bit per entity key (set on any PUSH/SET/MSET). On snapshot: (a) write full snapshot periodically (e.g., every 10th snapshot), (b) write delta snapshot otherwise (only dirty entities + manifest). On restore: load latest full snapshot, apply all subsequent deltas. Simpler than Flink's approach because Tally's state is a flat HashMap, not an LSM tree.

### Debug Web UI
**What ecosystem does:** Flink has a built-in web UI showing job DAG, task metrics, checkpoint history, backpressure detection, watermark tracking. Materialize has Console with freshness dashboards, query history, EXPLAIN ANALYZE. RedisInsight provides key browser, memory profiler, slow log inspector, real-time metrics.
**What Tally should do:** Embedded in binary using rust-embed. Served from HTTP management port (6401). Features:
1. **Stream topology** -- DAG visualization of streams, views, depends_on relationships
2. **Throughput** -- events/sec per stream, real-time chart
3. **Memory** -- per-stream memory breakdown, total entity count, operator state sizes
4. **Entity inspector** -- search by key, see all features with current values and operator internals
5. **Event log stats** -- size per stream, events per stream, oldest event timestamp
6. **Backfill progress** -- per-stream backfill status (events replayed / total, ETA)
Technology: Keep it simple. HTML templates (askama) + htmx for live updates + SSE for streaming metrics. No React, no npm, no build step. Flink's dashboard is a good model for scope.

---

## Sources

- [RisingWave Streaming Engine Overview](https://risingwavelabs.github.io/risingwave/design/streaming-overview.html) -- composable DAG framework, change propagation model
- [RisingWave Backfill Order Control (2025)](https://risingwave.com/blog/risingwave-backfill-order-control/) -- backfill dependency ordering for materialized views
- [RisingWave CREATE MATERIALIZED VIEW](https://docs.risingwave.com/sql/commands/sql-create-mv) -- automatic backfill on MV creation
- [Flink State Schema Evolution](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/) -- POJO add/remove fields, Avro support, key evolution limitations
- [FLIP-527: State Schema Evolution for RowData](https://cwiki.apache.org/confluence/display/FLINK/FLIP-527:+State+Schema+Evolution+for+RowData) -- 2025 proposal for nested schema evolution
- [Flink Incremental Checkpointing](https://flink.apache.org/2018/01/30/managing-large-state-in-apache-flink-an-intro-to-incremental-checkpointing/) -- RocksDB SSTable delta tracking
- [Flink Full vs Incremental Checkpoint](https://dzone.com/articles/apache-flink-full-checkpoint-vs-incremental-checkpoint) -- incremental checkpoint architecture
- [Flink DataStream V2 Building Blocks (FLIP-409)](https://cwiki.apache.org/confluence/display/FLINK/FLIP-409:+DataStream+V2+Building+Blocks:+DataStream,+Partitioning+and+ProcessFunction) -- keyed vs non-keyed partitioning
- [Redis AOF Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) -- append-only log, rewrite/compaction, multi-part AOF (Redis 7.0+)
- [Redis AOF Rewrite Strategies](https://oneuptime.com/blog/post/2026-01-30-redis-aof-rewrite-strategies/view) -- compaction triggers and techniques
- [Tecton Stream Feature View](https://docs.tecton.ai/docs/defining-features/feature-views/stream-feature-view) -- automatic backfill from batch source, feature_start_time
- [Tecton Training Data with Stream Ingest + Backfills](https://docs.tecton.ai/docs/0.7/defining-features/feature-views/stream-feature-view/stream-feature-view-with-stream-ingest-api/Examples/stream-feature-view-with-backfills) -- backfill configuration patterns
- [Snowpipe Streaming Schema Evolution (Dec 2025)](https://docs.snowflake.com/en/release-notes/2025/other/2025-12-17-schema-evolution-snowpipe-streaming) -- auto-add columns in streaming pipelines
- [Kafka Backfill Playbook](https://nejckorasa.github.io/posts/kafka-backfill/) -- snapshot + replay rehydration pattern
- [Materialize Changelog](https://materialize.com/changelog/) -- Console improvements, freshness dashboard
- [RedisInsight](https://redis.io/insight/) -- embedded GUI, memory profiler, real-time metrics
- [Apache Flink Dashboard](https://dzone.com/articles/apache-flink-dashboard-for-real-time-data-processing) -- built-in web UI, DAG visualization, metrics
- [Redis MGET Performance](https://www.dragonflydb.io/guides/redis-mget-performance) -- batch read latency benefits
- [Rust + HTMX Application Patterns](https://madhanganesh.medium.com/rust-htmx-application-f6c85c546f3f) -- embedded web UI in Rust binary
- [ActivityWatch: Embed WebUI Assets into Rust Binary](https://github.com/ActivityWatch/aw-server-rust/pull/385) -- rust-embed pattern for single-binary UI
- [Spark Structured Streaming State Formats](https://www.waitingforcode.com/apache-spark-structured-streaming/delta-snapshot-state-store-formats/read) -- delta + snapshot state checkpointing
- [Amazon SageMaker Feature Store TTL](https://docs.aws.amazon.com/sagemaker/latest/dg/feature-store-time-to-live.html) -- per-feature-group TTL duration
- [Top 5 Feature Stores in 2025](https://www.gocodeo.com/post/top-5-feature-stores-in-2025-tecton-feast-and-beyond) -- ecosystem landscape

---
*Feature research for: Tally v1.1 -- Composable Pipeline & Event Log*
*Researched: 2026-04-09*
