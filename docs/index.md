# Beava Documentation

Beava is a real-time compute engine. Define pipelines, push events, read results from in-memory state. One Rust binary, sub-microsecond reads, zero infrastructure.

## Quick Links

- **[Quick Start](quickstart.md)** — Get Beava running and push your first event in under 5 minutes.
- **[Python SDK](python-sdk.md)** — Define pipelines, push events, and read features from Python.
- **[Operators Reference](operators.md)** — All 16 v0 aggregation operators: count, sum, avg, min, max, variance, stddev, percentile, count_distinct, top_k, first, last, first_n, last_n, ema, lag (plus the bv.col + with_columns expression DSL that replaces bv.derive).
- **[Architecture](architecture.md)** — How Beava works under the hood: in-memory state, snapshot + WAL persistence, pipeline DAGs.
- **[Comparison](comparison.md)** — Beava vs Flink+Kafka+Redis: cost, complexity, performance.

## What is Beava?

Beava replaces the Kafka + Flink + Redis stack that teams typically need for real-time feature computation. You define pipelines in Python, register them with the server, and push events over a persistent TCP connection. Beava computes windowed aggregations, derived expressions, and cross-stream cascades entirely in memory.

Every write is synchronous and atomic on the server. All operators update in one pass, state is immediately consistent. Reads return the latest state in microseconds. Durability comes from an append-only event log (WAL) plus periodic snapshots, so crash recovery loses at most ~1 second of data.

## Hello World

Install and run the server, then push your first event:

```python
import beava as bv

@bv.stream
class Transactions:
    user_id: str
    amount: float

@bv.table(key="user_id")
def UserFeatures(txs: Transactions) -> bv.Table:
    return txs.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
    )

app = bv.App("localhost:6400")
app.register(Transactions, UserFeatures)

app.push(Transactions, {"user_id": "u123", "amount": 50.0})
app.flush()

features = app.get("u123")
print(features.tx_count_1h)  # 1
print(features.tx_sum_1h)    # 50.0
```

Full walkthrough: [Quick Start](quickstart.md).

## When to use Beava

**Good fit:**

- 20-100 real-time aggregations at under 500K events/sec
- State that fits on one machine (up to ~2-4 TB RAM on modern cloud instances)
- Small teams without a dedicated streaming infrastructure person
- You want to ship real-time features in hours, not weeks

**Not a good fit:**

- You need distributed exactly-once processing
- State exceeds what fits in RAM on the largest available instance
- You need the Kafka connector ecosystem
- You need event-time watermarks with late-arrival handling (on the roadmap, not in v0)
