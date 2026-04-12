# Stream-Table Duality for Tally — Architecture Research

**Date:** 2026-04-12
**Status:** Foundational architecture research
**Builds on:** HORIZON-DATAFRAME-API.md (SDK-layer DataFrame design)

---

## Executive Summary

The stream-table duality is the single most important abstraction in streaming systems. Every major platform (Kafka Streams, Flink, ksqlDB, Materialize, RisingWave) implements it: **a stream is an unbounded sequence of events; a table is a stream aggregated by key**. The transformation is one-way in the useful direction: `stream.group_by(key).agg(...)` produces a table. Tables can be re-keyed to produce new tables.

For Tally, the core insight is: **Tally already implements stream-table duality — it just doesn't name it that way.** A `@st.stream(key="user_id")` with windowed operators IS a table. The `@st.stream()` without a key IS a stream. Views ARE table-to-table joins. The proposed new API (`source()`, `group_by().agg()`, `app.serve()`) is primarily a Python SDK redesign with two genuinely new server-side concepts: (1) transient computation nodes that don't get persisted, and (2) explicit re-keying edges in the DAG.

**What's new server-side:** ~20% new functionality, ~80% syntactic sugar.
- New: transient node flag (skip snapshot/GET), re-key DAG edges, column-transform nodes
- Sugar: `group_by().agg()` compiles to existing `StreamDefinition` with `key_field`
- Sugar: `source()` compiles to existing keyless stream
- Sugar: `.map()` / `.filter()` compile to existing expression evaluator + stream filter

---

## The Duality: Streams vs Tables

### Theory (Kreps / Kleppmann)

A **stream** is a changelog — an append-only sequence of immutable events. A **table** is a materialized snapshot of current state, derived by replaying and aggregating that changelog. You can always derive a table from a stream (aggregate it). You can always derive a stream from a table (emit its changes). This is the duality.

### How Each System Implements It

**Kafka Streams (Java):**
```java
// Stream: unbounded events, each record is an INSERT
KStream<String, Transaction> txns = builder.stream("transactions");

// Stream -> Table: groupByKey + aggregate produces KTable
KTable<String, Long> counts = txns
    .groupByKey()
    .count();

// Table -> Stream: toStream() reinterprets as changelog
KStream<String, Long> countStream = counts.toStream();

// Re-key (Table -> Table with different key):
KTable<String, Long> merchantCounts = txns
    .groupBy((key, val) -> KeyValue.pair(val.getMerchantId(), val))
    .count();
// Note: groupBy on KTable triggers internal repartition topic
```

**ksqlDB (SQL):**
```sql
-- Stream: event sequence, INSERT-only semantics
CREATE STREAM transactions (
  user_id VARCHAR KEY, amount DOUBLE, merchant_id VARCHAR
) WITH (kafka_topic='txns', value_format='JSON');

-- Stream -> Table: GROUP BY produces materialized table
CREATE TABLE user_tx_counts AS
  SELECT user_id, COUNT(*) AS tx_count, SUM(amount) AS tx_total
  FROM transactions
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY user_id
  EMIT CHANGES;

-- Table is queryable with pull queries (point lookups)
SELECT * FROM user_tx_counts WHERE user_id = 'u123';
```

**Flink (Java + Python Table API):**
```java
// DataStream: unbounded event sequence
DataStream<Transaction> txns = env.addSource(kafkaSource);

// Stream -> Table: keyBy + window + aggregate
DataStream<UserStats> userStats = txns
    .keyBy(Transaction::getUserId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(new StatsAggregator());

// Table API equivalent (Python):
// orders.window(Tumble.over(lit(1).hours).on(col('ts')).alias("w"))
//   .group_by(col('user_id'), col('w'))
//   .select(col('user_id'), col('amount').sum.alias('total'))
```

Key Flink insight: **everything is a changelog stream internally**. A "table" in Flink is a dynamic table backed by a changelog with INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE events. The Table API is sugar over DataStream operations.

**Flink is lazy:** calling `.map()`, `.filter()`, `.keyBy()` builds a DAG (JobGraph). Nothing executes until `env.execute()`. The optimizer fuses operators, eliminates unnecessary shuffles, and pipelines stages.

**Materialize (SQL):**
```sql
-- Source = stream
CREATE SOURCE transactions FROM KAFKA BROKER '...' TOPIC 'txns';

-- Materialized view = table (incrementally maintained)
CREATE MATERIALIZED VIEW user_stats AS
  SELECT user_id, count(*) AS tx_count, sum(amount) AS tx_total
  FROM transactions
  GROUP BY user_id;

-- Index = serving layer (in-memory, fast lookups)
CREATE INDEX ON user_stats (user_id);

-- Only indexed materialized views are fast to query.
-- Non-indexed views exist but are slow (full scan).
```

Materialize insight: **not everything needs to be materialized**. Intermediate views can exist without indexes. Only views you CREATE INDEX on get fast serving. This maps directly to Tally's proposed `app.serve()` pattern.

**RisingWave (SQL):**
```sql
CREATE MATERIALIZED VIEW hourly_sales AS
  SELECT user_id, product_category,
    SUM(amount) AS total, COUNT(*) AS num_orders
  FROM orders_stream
  GROUP BY user_id, product_category;
-- Queryable immediately via standard SQL
-- Incrementally updated as new events arrive
```

### Summary: The Universal Pattern

| System | Stream Type | Table Type | Stream->Table | Table->Table |
|--------|-----------|-----------|---------------|--------------|
| Kafka Streams | KStream | KTable | groupByKey().aggregate() | groupBy(newKey).aggregate() |
| ksqlDB | CREATE STREAM | CREATE TABLE | SELECT...GROUP BY | Not directly supported |
| Flink | DataStream | Dynamic Table | keyBy().window().aggregate() | Re-key via new groupBy |
| Materialize | SOURCE | MATERIALIZED VIEW | CREATE MV...GROUP BY | CREATE MV over MV |
| RisingWave | SOURCE/TABLE | MATERIALIZED VIEW | CREATE MV...GROUP BY | CREATE MV over MV |
| **Tally today** | `@stream()` (no key) | `@stream(key=)` | Implicit (key_field + operators) | Views (cross-stream derive) |
| **Tally proposed** | `source()` | `group_by().agg()` | Explicit API call | `table.group_by(new_key).agg()` |

---

## Column Transforms on Streams (map / filter)

### How Platforms Handle Stateless Transforms

**Flink DataStream:**
```java
// map: one-to-one transform (stateless)
DataStream<EnrichedTxn> enriched = txns
    .map(txn -> new EnrichedTxn(txn, txn.amount * txn.fxRate));

// filter: predicate (stateless)
DataStream<Transaction> failed = txns
    .filter(txn -> txn.getStatus().equals("failed"));
```
Flink transforms are **lazy** — they add nodes to the DAG. The optimizer can fuse adjacent map/filter nodes into a single operator to eliminate intermediate serialization.

**Kafka Streams:**
```java
KStream<String, Transaction> enriched = txns
    .mapValues(txn -> txn.withAmountUsd(txn.amount * txn.fxRate));
// mapValues is preferred over map because it doesn't trigger repartition

KStream<String, Transaction> failed = txns
    .filter((key, txn) -> txn.getStatus().equals("failed"));
```

**Pathway (Python):**
```python
# Column transform via select
enriched = raw.select(
    *pw.this,  # keep all existing columns
    amount_usd=pw.this.amount * pw.this.fx_rate,
    is_high_value=pw.this.amount > 1000,
)

# Filter
failed = enriched.filter(pw.this.status == "failed")
```

**Bytewax (Python):**
```python
from bytewax.dataflow import Dataflow
import bytewax.operators as op

flow = Dataflow("pipeline")
raw = op.input("src", flow, source)
enriched = op.map("enrich", raw, lambda txn: {
    **txn, "amount_usd": txn["amount"] * txn["fx_rate"]
})
failed = op.filter("failed", enriched, lambda txn: txn["status"] == "failed")
```

**Fennel (Python):**
```python
@dataset
class EnrichedTransactions:
    # ... fields ...
    @pipeline
    @inputs(RawTransactions)
    def enrich(cls, raw: Dataset):
        return raw.assign(
            amount_usd=col("amount") * col("fx_rate"),
            is_high_value=col("amount") > 1000,
        ).filter(lambda df: df["status"] != "cancelled")
```

### Key Question: Lazy or Eager?

| System | Lazy/Eager | How |
|--------|-----------|-----|
| Flink | Lazy | DAG built at definition time, optimized and executed on env.execute() |
| Kafka Streams | Lazy | Topology built, executed when KafkaStreams.start() called |
| Pathway | Lazy | Rust engine builds computation graph, runs incrementally |
| Bytewax | Eager-ish | Dataflow DAG, but operators execute per-item as data flows |
| Fennel | Lazy | Pipeline compiled to server-side execution plan |

**Tally mapping:** Transforms should be **lazy at definition time** (build expression AST in Python), **eager at event time** (evaluate per-event in Rust). This is exactly what Tally already does — the expression parser creates an AST at REGISTER time, and `eval()` runs it per-event.

### Tally Implementation

Column transforms (`.map()`) compile to **derived fields on a keyless intermediate stream**. The server's existing expression evaluator already handles arithmetic, comparison, and boolean ops on `_event.*` fields.

```python
# Python SDK (proposed)
enriched = raw.map(
    amount_usd=raw["amount"] * raw["fx_rate"],
    is_high_value=raw["amount"] > 1000,
)
```

Compiles to server JSON:
```json
{
  "name": "_anon_map_0",
  "key_field": null,
  "depends_on": ["transactions_raw"],
  "features": [
    {"name": "amount_usd", "type": "derive", "expr": "_event.amount * _event.fx_rate"},
    {"name": "is_high_value", "type": "derive", "expr": "_event.amount > 1000"}
  ]
}
```

Filter (`.filter()`) compiles to the existing `stream.filter` field:
```json
{
  "name": "_anon_filter_0",
  "key_field": null,
  "depends_on": ["_anon_map_0"],
  "filter": "_event.status == 'failed'"
}
```

**No new server-side operator needed.** The expression evaluator and stream filter already exist.

---

## Aggregation: Stream -> Table

### When State Is Created

In every system, aggregation is the moment state is born:
- **Before aggregation:** stateless transforms, no per-key storage needed
- **After aggregation:** stateful table, per-key operator state maintained

**Kafka Streams:** `KGroupedStream` (after `groupByKey()`) is stateless — it's just a logical partitioning. State appears when you call `.aggregate()`, `.count()`, or `.reduce()`. This creates a state store backed by RocksDB.

**Flink:** `keyBy()` partitions the stream. `.window().aggregate()` creates a window operator with keyed state in the state backend. No state until aggregation.

**Fennel:** `.groupby("user_id")` returns an intermediate. `.aggregate(...)` is the terminal operator that creates the indexed dataset with state.

### Tally Mapping

`group_by().agg()` maps **directly** to the existing `StreamDefinition` with `key_field` set:

```python
# Proposed API
user_features = enriched.group_by("user_id").agg(
    tx_count_1h=st.count(window="1h"),
    tx_sum_1h=st.sum("amount_usd", window="1h"),
)
```

Compiles to:
```json
{
  "name": "user_features",
  "key_field": "user_id",
  "depends_on": ["_anon_map_0"],
  "features": [
    {"name": "tx_count_1h", "type": "count", "window": "1h"},
    {"name": "tx_sum_1h", "type": "sum", "field": "amount_usd", "window": "1h"}
  ]
}
```

This is **identical** to what `@st.stream(key="user_id")` produces today. Pure syntactic sugar.

---

## Re-keying: Table -> Table

### How Distributed Systems Handle It

**Kafka Streams:**
```java
// Re-key: KTable.groupBy() triggers repartition through internal topic
KTable<String, Long> merchantCounts = userTable
    .groupBy((userId, stats) -> KeyValue.pair(stats.merchantId, stats),
             Grouped.with(Serdes.String(), statsSerde))
    .count();
// Creates internal repartition topic: app-id-KSTREAM-AGGREGATE-repartition
```
In Kafka Streams, re-keying requires a **network shuffle** — data must be repartitioned across brokers so all records with the same new key land on the same partition.

**Flink:**
```java
// Re-key requires a new keyBy (triggers network shuffle)
DataStream<MerchantStats> merchantStats = userStats
    .keyBy(stats -> stats.getMerchantId())
    .window(TumblingEventTimeWindows.of(Time.hours(24)))
    .aggregate(new MerchantAggregator());
```

### Tally's Single-Process Advantage

In a single-process system like Tally, **re-keying is free**. No network shuffle, no repartition topic. It's just a different `DashMap` key. The question is: **when does the downstream table update?**

Two strategies:

**Strategy A — Eager propagation (recommended):**
When an event arrives for `user_id=u123` and updates the user table, immediately cascade to update the merchant table. This is what Tally's existing `push_with_cascade` already does via the DAG `downstream_map`.

Pros: Consistent reads. GET always returns up-to-date data.
Cons: Write amplification — one event may update multiple tables.

**Strategy B — Lazy evaluation on GET:**
Don't update downstream tables on PUSH. Instead, recompute on GET by scanning upstream state.

Pros: No write amplification.
Cons: GET latency proportional to upstream table size. Breaks the "sub-millisecond GET" promise.

**Recommendation:** Strategy A (eager). Tally's existing cascade mechanism handles this. The write amplification is bounded by the DAG depth, which is typically 2-3 levels.

### Re-key Implementation

A re-key node needs a new concept: **the event that flows downstream contains the aggregated table row, not the original event.** When user table updates for `user_id=u123`, the cascade to the merchant table must:

1. Read the updated row for `u123` from the user table
2. Extract the new key field (`merchant_id`) from that row or from the original event
3. Push to the merchant table keyed by `merchant_id`

This is genuinely new. Today's cascade passes the original event JSON downstream. Re-keying needs to pass the **table row** (or the original event with a different key extraction).

**Server-side change:** The cascade mechanism needs a `rekey_field` option on DAG edges. When a downstream stream has `depends_on: ["user_features"]` with `key_field: "merchant_id"`, the cascade should extract `merchant_id` from the original event (which contains both `user_id` and `merchant_id` fields from the source).

Actually, this already works if the original event propagates through the DAG (which it does today). The downstream stream just extracts a different `key_field` from the same event. **No server change needed for the common case** where the re-key field exists in the original event.

The harder case — re-keying on a field that only exists in the upstream table's output (not in the original event) — requires passing enriched events through the cascade. This can be deferred.

---

## Materialization: What Gets Stored?

### The `app.serve()` Pattern

**Materialize:** Only materialized views with an INDEX get fast point lookups. Unindexed views exist but are slow. Views without materialization are transient.

**ksqlDB:** Every persistent query materializes a table backed by a changelog topic + RocksDB. There's no "transient" intermediate — everything is stored.

**Flink:** Intermediate operators are pipelined (chained) and don't materialize unless configured with a state backend checkpoint. Only operators with `keyBy` + aggregation create keyed state.

**Fennel:** The `@dataset(index=True)` flag controls whether a dataset is queryable. Unindexed datasets exist as computation intermediates.

### Tally Implementation

Today, every registered stream gets snapshot persistence and is queryable via GET. The proposed change: add a `transient` flag.

```python
# Only served datasets get persistence + GET endpoint
app.serve(user_features)    # GET /features/user_id/u123 -> snapshot + queryable
app.serve(merchant_risk)    # GET /features/merchant_id/m456 -> snapshot + queryable
# enriched, failed = transient, no storage overhead
```

**Server-side changes needed:**

1. **StreamDefinition gains `transient: bool` field** (default: false for backward compat)
   - Transient streams: operators still maintain in-memory state for cascade correctness, but:
   - Skipped during snapshot serialization
   - Not queryable via GET (return error: "stream is transient")
   - Eligible for more aggressive memory optimization (smaller ring buffers, etc.)

2. **Snapshot serializer checks `transient` flag** — skip serializing state for transient streams. On recovery, transient streams start fresh (acceptable since they're intermediate computation).

3. **GET handler checks `transient` flag** — returns clear error instead of empty results.

This is a small, well-isolated change (~30 lines of server code).

---

## The DAG Model

### Node Types

```
Source (keyless, stateless)
  │
  ├── MapNode (keyless, stateless) — column transforms via expressions
  │     │
  │     ├── FilterNode (keyless, stateless) — predicate filter
  │     │     │
  │     │     └── AggNode (keyed, STATEFUL) — group_by + operators
  │     │           │
  │     │           └── DeriveNode (keyed, stateless) — computed columns on table
  │     │                 │
  │     │                 └── JoinNode (keyed, stateless) — cross-key lookup
  │     │
  │     └── AggNode (keyed, STATEFUL) — different key (re-key)
  │
  └── AggNode (keyed, STATEFUL) — direct aggregation
```

### Edge Types

| Edge | From | To | Data Passed |
|------|------|----|------------|
| map | Stream | Stream | Enriched event (original + computed fields) |
| filter | Stream | Stream | Filtered event (same schema, fewer events) |
| group_by | Stream | Table | Original event (table extracts key + updates operators) |
| derive | Table | Table | Table row (for computed columns on same key) |
| join/lookup | Table | Table | Cross-key reference (lookup in another table's state) |
| re-key | Table | Table | Original event re-keyed (extract different key field) |

### How It Maps to Existing Tally Concepts

| DAG Concept | Existing Tally Concept | Status |
|-------------|----------------------|--------|
| Source node | Keyless `StreamDefinition` (key_field=None) | Exists |
| Map node | Keyless stream with Derive features | Exists |
| Filter node | `StreamDefinition.filter` | Exists |
| Agg node | Keyed `StreamDefinition` with operators | Exists |
| Derive on table | `FeatureDef::Derive` in a keyed stream | Exists |
| Join/Lookup | `ViewDefinition` with `ViewFeatureDef::Lookup` | Exists |
| Re-key edge | `depends_on` + different `key_field` | Exists (cascade passes original event) |
| Transient flag | Not implemented | **New** |
| `app.serve()` | Not implemented (all streams served today) | **New** |

---

## Proposed Tally API

### Full Example

```python
import tally as st

app = st.App("localhost:6400")

# ── Source: raw event stream (keyless) ──────────────────────────
raw = app.source("transactions_raw")

# ── Stream transforms (keyless -> keyless, stateless) ──────────
enriched = raw.map(
    amount_usd = raw["amount"] * raw["fx_rate"],
    is_high_value = raw["amount"] > 1000,
)
failed = enriched.filter(enriched["status"] == "failed")

# ── Aggregate: stream -> table (keyed, stateful) ───────────────
user_features = enriched.group_by("user_id").agg(
    tx_count_1h   = st.count(window="1h"),
    tx_sum_1h     = st.sum("amount_usd", window="1h"),
    avg_amount    = st.avg("amount_usd", window="1h"),
    last_country  = st.last("country"),
)

# ── Derived columns on table (keyed, stateless) ────────────────
user_features["velocity_spike"] = st.derive(
    "(tx_count_1h / 1) / (tx_count_24h / 24)"
)

# ── Re-aggregate on different key ──────────────────────────────
merchant_risk = enriched.group_by("merchant_id").agg(
    unique_users_24h  = st.distinct_count("user_id", window="24h"),
    chargeback_count  = st.count(window="24h", where="type == 'chargeback'"),
)

# ── Failed transactions per user (from filtered stream) ────────
user_failures = failed.group_by("user_id").agg(
    failed_count_1h = st.count(window="1h"),
)

# ── Join tables (cross-key lookup) ─────────────────────────────
risk_view = user_features.join(
    merchant_risk,
    on="merchant_id",  # FK from original events
    select=["unique_users_24h", "chargeback_count"],
)
risk_view["risk_score"] = st.derive(
    "(tx_count_1h > 10) and (chargeback_count > 5)"
)
risk_view["failure_rate"] = st.derive(
    "user_failures.failed_count_1h / tx_count_1h"
)

# ── Serve: only these get persistence + GET endpoint ───────────
app.serve(user_features)    # GET /features/user_id/u123
app.serve(merchant_risk)    # GET /features/merchant_id/m456
app.serve(risk_view)        # GET /features/user_id/u123 (includes joined data)
# enriched, failed, user_failures = transient, no snapshot overhead

app.register()  # sends DAG to server
```

### What `app.register()` Sends to Server

The Python SDK compiles the DAG into a flat list of `StreamDefinition` + `ViewDefinition` JSON objects — the **exact same format** the server already accepts. The DAG is implicit in the `depends_on` fields.

```json
[
  {"name": "transactions_raw", "key_field": null, "transient": true},
  {"name": "_enriched_0", "key_field": null, "depends_on": ["transactions_raw"],
   "features": [
     {"name": "amount_usd", "type": "derive", "expr": "_event.amount * _event.fx_rate"},
     {"name": "is_high_value", "type": "derive", "expr": "_event.amount > 1000"}
   ], "transient": true},
  {"name": "_failed_0", "key_field": null, "depends_on": ["_enriched_0"],
   "filter": "_event.status == 'failed'", "transient": true},
  {"name": "user_features", "key_field": "user_id", "depends_on": ["_enriched_0"],
   "features": [...], "transient": false},
  {"name": "merchant_risk", "key_field": "merchant_id", "depends_on": ["_enriched_0"],
   "features": [...], "transient": false},
  {"name": "user_failures", "key_field": "user_id", "depends_on": ["_failed_0"],
   "features": [...], "transient": true},
  {"name": "risk_view", "type": "view", "key_field": "user_id",
   "features": [...], "transient": false}
]
```

### Backward Compatibility

The `@st.stream` and `@st.view` decorators continue to work. They compile to the same JSON they always have, with `transient: false` (all served by default). The new API is additive.

```python
# Old API (still works)
@st.stream(key="user_id")
class Transactions:
    tx_count_1h = st.count(window="1h")

# New API (same server-side effect)
raw = app.source("transactions_raw")
txns = raw.group_by("user_id").agg(tx_count_1h=st.count(window="1h"))
app.serve(txns)
```

---

## Server-Side Mapping

### What Maps to Existing Concepts (no changes needed)

| New API | Existing Server Concept | Notes |
|---------|------------------------|-------|
| `app.source("name")` | Keyless `StreamDefinition` | Already supported (key_field=None) |
| `.map(expr=...)` | Keyless stream with `Derive` features | Already supported |
| `.filter(pred)` | `StreamDefinition.filter` | Already supported |
| `.group_by(key).agg(...)` | Keyed `StreamDefinition` with operators | Exact current model |
| `table["x"] = st.derive(...)` | `FeatureDef::Derive` in feature list | Already supported |
| `.join(other, on=...)` | `ViewDefinition` with `Lookup` | Already supported |
| DAG / cascade | `depends_on` + `push_with_cascade` | Already supported |

### What's Genuinely New (server changes required)

**1. Transient flag on StreamDefinition** (~30 lines)
```rust
pub struct StreamDefinition {
    // ... existing fields ...
    /// When true, skip snapshot serialization and reject GET queries.
    pub transient: bool,  // NEW
}
```
- Snapshot serializer: skip streams where `transient == true`
- GET handler: return error for transient streams
- Memory: transient stream state still maintained in-memory for cascade correctness

**2. Enriched event propagation through DAG** (~50 lines)
Currently, cascade passes the original event JSON. For `.map()` transforms to propagate computed columns downstream, the cascade needs to merge computed derive values back into the event before passing downstream.

```rust
// In push_with_cascade_internal, after pushing to a map node:
// Merge derived values into event for downstream nodes
let mut enriched_event = event.clone();
for (name, value) in &derived_features {
    enriched_event[name] = value.to_json();
}
// Pass enriched_event to downstream nodes instead of original event
```

**3. Batch registration endpoint** (~20 lines)
Today, streams are registered one at a time. The new API sends an entire DAG at once. Add a `REGISTER_BATCH` command that registers multiple streams in dependency order (topological sort).

### What Can Be Deferred

- **Re-keying on computed fields** (when the new key only exists in a table's output, not the original event). The common case — re-keying on a field present in the original event — already works via cascade.
- **Stream-to-stream joins** (joining two streams before aggregation). Rare in feature engineering. Can be approximated with views.
- **Multi-hop re-aggregation** (Table A grouped by X -> Table B grouped by Y -> Table C grouped by Z). Works via existing cascade, but testing needed for 3+ levels.

---

## Implementation Roadmap

### Phase A: Python SDK (1-2 weeks, zero server changes)

Build the new `source()`, `.map()`, `.filter()`, `.group_by().agg()`, `.join()` API as a pure Python layer that compiles to the existing `RegisterRequest` JSON. This gives users the new API immediately.

- `Source` class: thin wrapper, produces keyless `StreamDefinition`
- `Stream` class: proxy object with `.map()`, `.filter()`, `.group_by()`
- `GroupedStream` class: intermediate with `.agg()` that produces `Table`
- `Table` class: proxy with `__setitem__` for derives, `.join()` for lookups
- `App.serve()`: marks table as non-transient (all served by default until server supports transient flag)
- `App.register()`: topological sort + serialize to JSON + send batch

### Phase B: Transient flag (1 day, small server change)

Add `transient: bool` to `StreamDefinition`. Update snapshot serializer and GET handler. This enables the memory/disk optimization for intermediate computation nodes.

### Phase C: Enriched event cascade (2-3 days, moderate server change)

Modify `push_with_cascade_internal` to merge derived values from map nodes back into the event before passing downstream. This makes `.map()` column transforms actually propagate through the DAG.

### Phase D: Batch registration (1 day)

Add `REGISTER_BATCH` command that accepts an array of stream definitions and registers them in topological order.

### Dependency Order

```
Phase A (SDK) ──> Phase B (transient) ──> ship as v2.0 API
                  Phase C (enriched cascade) ─┘
                  Phase D (batch register) ────┘
```

Phase A can ship independently. Phases B-D are independent of each other and can parallelize.
