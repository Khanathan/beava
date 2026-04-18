# Concepts

Beava is a feature server built on four primitives: **Stream**, **Table**,
**Operator**, and **Fork**. Two operational semantics glue them together:
**event time** and **watermarks**. Read this page once — everything else in
the docs assumes you know these six terms.

Target audience: engineers who already know Kafka, Flink, or Redis but have
not seen Beava before. This page teaches; the [HTTP API](http-api.md) and
[Operations](operations.md) pages assume you have absorbed it.

---

## Stream

A **stream** is a typed, append-only sequence of events. Each event is a JSON
object with an optional `_event_time` field (milliseconds since epoch).

**Declaring a stream in Python:**

```python
import beava as bv

@bv.stream
class Click:
    user: str
    page: str
    # _event_time is optional here; Beava detects it in the pushed payload
```

**Auto-registration over HTTP:**

Push to any previously unseen stream name and Beava registers it automatically
with a permissive (schema-less) configuration:

```bash
curl -X POST http://localhost:6900/push/clicks \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","page":"/home","_event_time":1700000000000}'
```

**Storage:** each stream gets a per-stream WAL (write-ahead log) at
`<data-dir>/<stream>.log`. The WAL is the durability primitive; it is also the
replay source for crash recovery and for the fork workflow. The in-memory ingest
queue drains to the WAL before the server acknowledges writes on the TCP path.

**Keyed vs keyless:** a stream is keyless by default. Declaring
`@bv.stream(key="user")` (or providing a `key_field` on HTTP registration)
makes it keyed — every event is associated with an entity, and per-entity
operator state is maintained. Keyless streams route all events into a single
shared state bucket.

---

## Table

A **table** is a keyed, per-entity aggregation over one or more streams. It
holds exactly **one row per entity key**; the columns are the outputs of the
operators declared in the table definition.

```python
@bv.table(key="user")
def UserActivity(c: Click) -> bv.Table:
    return c.group_by("user").agg(
        count_10m  = bv.count(window="10m"),
        count_1h   = bv.count(window="1h"),
        last_page  = bv.last(c.page),
        pages_seen = bv.distinct_count(c.page, window="24h"),
    )
```

**Reading a table via HTTP:**

```bash
curl "http://localhost:6900/features/alice?table=UserActivity"
# → {"ok":true,"data":{"key":"alice","tables":{"UserActivity":{"count_10m":3,...}}}}
```

**TTL:** tables can declare `ttl="7d"` to evict stale entity keys. Eviction is
driven by the event-time **watermark** — not wall-clock — so backfilling
30-day-old events does not immediately evict keys under a 7-day TTL. See
[Event time](#event-time) below, and the authoritative semantics in
[docs/event-time.md § TTL Semantics](event-time.md#ttl-semantics).

---

## Operator

An **operator** is the unit of aggregation inside a table. Operators are
stateful per (entity key, stream, feature name) triple and update incrementally
on each event push — no re-scan of history.

Beava ships 16 built-in operators:

| Operator | Purpose |
|----------|---------|
| `count` | Event count over a sliding window |
| `sum` | Field sum over a sliding window |
| `avg` | Running mean over a sliding window |
| `min`, `max` | Running extrema |
| `stddev` | Running standard deviation |
| `percentile` | UDDSketch-backed approximate percentile (hybrid exact→sketch) |
| `distinct_count` | HLL++ approximate distinct count (hybrid exact→sketch) |
| `last`, `first`, `lag` | Event value selection |
| `ema` | Exponential moving average |
| `last_n` | Rolling last-N event buffer |
| `exact_min`, `exact_max` | Exact monotonic extrema (no window expiry) |
| `derive` | Stateless expression over other features |

**Windows:** every operator accepts an optional `window=` argument
(`"10m"`, `"1h"`, `"24h"`). Windows are **sliding** and bucketed by event-time.
Window expiry fires when the stream's watermark advances past the bucket
boundary — not on wall-clock tick.

See [docs/operators.md](operators.md) for per-operator parameter reference,
example declarations, and memory cost estimates.

---

## Fork

A **fork** is a scoped, read-only replica of a live production Beava server.
It runs as a second Beava process in `--replica-from` mode; it pulls historical
events via `LOG_FETCH` and live-tails via `SUBSCRIBE`, routing them through its
own ingest pipeline.

The fork model answers: "how do I iterate a new feature pipeline against
production history without touching production?"

```python
with bv.fork(
    remote="beava-prod.internal:6400",
    streams=[Click],
    keys=["u123", "u456"],
    pipelines=[UserActivityV2],   # candidate pipeline, not prod's definition
) as fork:
    print(fork.get(UserActivityV2, key="u123"))
```

**How it works:**

1. **Catchup (LOG_FETCH):** the replica requests the upstream server's event
   log for each stream, replaying historical events to build up state.
2. **Live tail (SUBSCRIBE):** after catchup, the replica subscribes to the live
   event stream. Incoming events are processed through the local pipeline.
3. **Watermark parity:** both catchup and live-tail use `_event_time` from
   event payloads as the bucketing clock — identical to the upstream server.
   Shadow-mode feature values are directly comparable to production values.

**Data isolation:** the fork replica holds its own in-memory state and event
log. It has no write path back to production.

See [docs/architecture.md](architecture.md) for the fork replica design and the
`LOG_FETCH` / `SUBSCRIBE` wire protocol.

---

## Event time

Beava is **event-time-first**. Every event's `_event_time` field — not the
server wall-clock at receipt — determines which sliding-window bucket the event
lands in. Absent `_event_time`, Beava falls back to wall-clock receipt time.

Why this matters:

- **Backfill:** a batch of 30-day-old events lands in the 30-days-ago buckets,
  not in today's buckets. Features computed over historical data match what live
  ingestion would have produced on the same events.
- **Late arrivals:** a mobile SDK that batches events over spotty connectivity
  and delivers them 5 minutes late still gets correct bucketing.
- **Batch correctness:** a batch containing events from multiple points in time
  correctly places each event in its own bucket (the Phase 46 CORR-01 fix).
  Before this fix, a shared wall-clock timestamp would collapse the batch.

See [docs/event-time.md](event-time.md) for bucket-boundary math, the
`_event_time` resolution priority order, crash-replay determinism, and
per-stream lateness configuration.

---

## Watermarks

A **watermark** is Beava's event-time clock for a stream. It tracks "we have
observed events up to approximately time T on this stream". Operators use the
watermark to decide when a bucket is closed and can be safely aggregated or
evicted.

**Lateness window:** the watermark lags behind the maximum observed event time
by a configurable `watermark_lateness` (default: 5 seconds). Events arriving
within this window are accepted and bucketed correctly. Events older than
`observed_max - lateness` are dropped as too-late.

**Per-stream override:** for streams with known-high out-of-order delivery:

```python
@bv.stream(watermark_lateness="10m")
class MobileEvent:
    user: str
    action: str
```

**γ-propagation:** downstream tables (outputs of joins or derives) receive
watermarks computed as a function of their upstream inputs:

- Stateless pass-through (derive): watermark is copied from the upstream stream.
- Stream-stream join: watermark is `min(left_watermark, right_watermark)`. The
  join output can only advance as fast as the slower input.

You do not set downstream watermarks manually — Beava computes them after every
successful event push.

**Join idle-input caveat (v1.0):** if one input stream in a join stops emitting
events, the join's output watermark stalls and downstream aggregations cease to
produce new features. Per-stream idle markers are deferred to v1.1 (DX-06).

See [docs/event-time.md § Watermark Lateness](event-time.md#watermark-lateness-defaults-and-per-stream-configuration)
and [docs/event-time.md § Fork Watermark Propagation](event-time.md#fork-watermark-propagation)
for the full semantics.

---

## Putting it together

```
Event payload
  → _event_time resolved (payload field > fallback wall-clock)
  → WAL append (durability before extract)
  → push_batch_with_cascade_no_features
      → group events by event-time bucket
      → per-key operator update (count, sum, HLL, ...)
  → watermark.observe(stream, event_time)
      → γ-propagation to downstream tables
      → TTL eviction check (fires when watermark crosses last_event_at + ttl)
  → feature value available at GET /features/{key}
```

The [architecture](architecture.md) page goes deeper on the storage layout,
runtime internals, and WAL format. The [operations](operations.md) page is what
you read before deploying.
