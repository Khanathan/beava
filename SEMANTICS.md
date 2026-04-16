# Beava Streaming Semantics

This document nails down three areas where Beava's correctness model has been
hand-waved in marketing copy. Read this before building anything where the
distinction between *eventually-similar* and *identical* numbers matters to
you (fraud, money, compliance, clinical).

Scope: Beava v0.x (pre-launch, single-node Rust server + Python SDK). Every
claim here is grounded in source pointers. If the code and this document
disagree, **this document is wrong and should be filed as an issue.**

Three sections:

1. [Fork consistency model (`bv.fork()`)](#1-fork-consistency-model)
2. [Idempotency and duplicate-event handling](#2-idempotency-and-duplicate-event-handling)
3. [Event-time, watermarks, and late events](#3-event-time-watermarks-and-late-events)

---

## 1. Fork consistency model

`bv.fork()` spawns a local Beava process that replicates a filtered slice of
a remote cluster. The Python wrapper is in `python/beava/_fork.py` (today
still packaged as `python/beava/_fork.py` during the rename); the replica
loop is in `src/server/replica_client.rs`; the push-time notify hook is in
`src/server/replica.rs`.

There is no `tail=True` flag. Every `fork()` opens both the historical
catch-up and the live tail.

### 1a. What it IS

**Asynchronous streaming replication with unbounded staleness.** Not snapshot
isolation. Not linearizable. Not bounded staleness with a published bound.

The fork boot sequence (see `replica_client.rs::run`):

1. Issue `OP_LOG_FETCH{from_ts_millis, scope}` against the remote. The remote
   streams back every event in the per-stream append-only log whose wire
   timestamp is ≥ `from_ts_millis`, in per-stream log-file append order.
2. When the `REPLICA_FRAME_TAG_END` frame arrives, the `catchup_done`
   oneshot fires. If `block_until_catchup=true` (the default, and what
   `tl.fork()` uses), the local HTTP and TCP listeners bind only after this
   point.
3. Open a second connection with `OP_SUBSCRIBE{scope}`. Every matching
   event appended on the primary is pushed to this socket as it lands
   (server-side hook: `SubscriberRegistry::notify_subscribers` in
   `src/server/replica.rs`).
4. Apply each received event through the same ingest path a local push
   would take (`replica_ingest` in `src/server/tcp.rs`).

**What this means operationally:**

- **The primary never waits for the fork.** `notify_subscribers` is a
  non-blocking `try_send` on a per-subscriber bounded mpsc channel
  (`src/server/replica.rs:368-404`, capacity `SUBSCRIBER_CHANNEL_CAPACITY =
  10_000` events, `src/server/replica.rs:41`).
- **Network and CPU delay are not measured.** There is no wire-level
  acknowledgment, no published lag bound, and no `beava_replica_lag_ms`
  metric on the primary for the fork direction. The fork itself records
  `replica_last_applied_ts_ms` (`src/server/tcp.rs:189`), but no
  process-level metric compares that to the primary's current wall-clock.
- **On slow consumers, events are dropped or the fork is disconnected.**
  When the 10 000-event buffer fills,
  `SubscriberRegistry::drop_subscriber(conn_id, "backpressure")` bumps the
  `beava_replica_drops_total{reason="backpressure"}` counter
  and closes the channel. The fork notices EOF, reconnects, and re-issues
  `OP_LOG_FETCH` from `replica_last_applied_ts_ms` to catch up
  (`replica_client.rs:160-199`).
- **Duplicate events at reconnect boundaries are expected.**
  `from_ts_millis` is **inclusive** in `OP_LOG_FETCH`, so reconnect replays
  the last-seen event a second time. The primary protocol doc calls this
  out explicitly: *"Pipeline code at the caller's end handles idempotency
  (v0 design decision)"* (`src/server/protocol.rs:157-161`).

**Under load (indicative, not contractual):** on a healthy LAN with a
~100 K eps primary, the tail buffer has slack at millisecond scale. When
the fork's network RTT, disk, or consumer thread cannot keep pace for
more than ~100 ms of sustained lag at 100 K eps, the 10 000-event channel
fills and the subscriber is dropped. **We do not publish a staleness
bound.** Treat the fork as a best-effort replica.

### 1b. What it is NOT

- **Not snapshot isolation.** The fork's state advances as events arrive.
  A query iterating over features while a new event lands can see a
  consistent view only for the current thread's single HTTP request; a
  second request immediately after may reflect more events.
- **Not monotonic-reads across processes.** The fork and the primary can
  briefly disagree on the count for a given key. A reader querying the
  primary, then the fork, can observe a lower count on the fork.
- **Not exactly-once.** Reconnect duplicates are documented. Counters
  double-count at the reconnect boundary unless the user does their own
  idempotency (see §2).
- **Windowed aggregates drift during long fork lag.** Beava's windows are
  event-time bucketed (§3). A fork receiving delayed events will compute
  the same window buckets eventually, but in the meantime its rolling
  counts will trail the primary's by the current lag.

### 1c. Opting in to stricter guarantees

Beava v0.x does not offer stricter consistency on the wire. If you need:

- **Bounded staleness.** Monitor the difference between the primary's
  latest event timestamp and `replica_last_applied_ts_ms` on the fork.
  Alert above your SLO. The fork exposes this via the `/metrics` endpoint
  on the fork process.
- **Exactly-once read-after-write.** Query the primary, not the fork. All
  state on the primary is immediately consistent within a single request
  (`src/engine/pipeline.rs` cascade is synchronous).
- **Point-in-time consistent snapshots.** Use `extract_at=[...]` on
  `bv.fork()` (`_fork.py:412-438`, `replica_client.rs:412-438`,
  Phase 44-01). The fork captures per-key feature state as it crosses each
  configured timestamp during catch-up, atomic per key. Query the result
  via `ForkedReplica.extract_history()`. This is the supported primitive
  for "give me features as of T".

### 1d. Code pointers

- Fork entry point: `python/beava/_fork.py:660` (`fork()`) and
  `_fork.py:267` (`ForkedReplica`).
- Replica boot loop: `src/server/replica_client.rs:141` (`ReplicaClient::run`).
- Subscriber registry and backpressure: `src/server/replica.rs:289-405`.
- Primary's notify hook on push: `src/server/replica.rs:368-404`
  (`notify_subscribers`, non-blocking `try_send`).
- Channel capacity: `src/server/replica.rs:41`
  (`SUBSCRIBER_CHANNEL_CAPACITY = 10_000`).
- Reconnect cursor: `src/server/replica_client.rs:188-193`.
- Drop counters: `src/server/replica.rs:60-61`, exposed via
  `subscribers_dropped_snapshot()`.

---

## 2. Idempotency and duplicate-event handling

### 2a. What it IS

**At-least-once delivery.** Beava's push path has no event-id deduplication.
There is no `event_id` field recognized by the server. There is no dedup
Bloom filter, no TTL'd exact-set, no idempotency key, no built-in
side-effect store for "I've seen this one."

Search the source: zero hits for `event_id` as a dedup concept in
`src/engine/**` and `src/server/**`. The word appears only in markdown and
skill files. Dedup mechanisms that DO exist are unrelated to event replay:

- `src/server/signals.rs` dedupes operational signals by a synthetic `id`
  (one record per operator warning class).
- `src/engine/hll.rs` dedupes observations *inside* an HLL/exact-set
  operator (that's what HLL is for).
- `src/server/throughput.rs` dedupes per-push cascade fan-out so one
  logical push doesn't double-count in throughput metrics.

None of these prevent a repeated PUSH from incrementing a counter twice.

**Practical implication:**

- If the client retries a PUSH after a network timeout, the counter
  increments again.
- If a replica reconnects after a SUBSCRIBE drop, the inclusive-boundary
  duplicate at the replay cursor increments the replica's local counter
  again (the primary's counter is fine).
- If a load balancer double-sends a request under retry, both land.

Beava's position is the same as the HN launch copy: *"If you need
distributed exactly-once, use Flink."* (`launch/show-hn.md:21`). Beava
provides **at-least-once ingestion on a single node** with crash-recovery
via snapshot + WAL replay (see §2c).

### 2b. What it is NOT

- **Not exactly-once.** No mechanism in the server can say "I already
  counted this event." If the launch copy ever says "exactly-once counters
  via event\_id dedup" — that is wrong and must be removed, because the
  feature does not exist.
- **Not "effectively-once if your retries are idempotent."** Retries at
  the transport layer are not idempotent with respect to counters unless
  you implement application-level dedup yourself.
- **Not protected against cross-node double-ingest.** There is no cross-node
  coordination (single-node product).

### 2c. What Beava DOES guarantee on the ingest path

- **Synchronous, atomic per-push cascade.** One PUSH drives all downstream
  aggregations in topological order under a single engine lock; the state
  seen by the next PUSH on the same key is the state after the previous
  PUSH fully committed (`src/engine/pipeline.rs` cascade).
- **Append-only on-disk event log** at `~/.beava/logs/<stream>.log`,
  `O_APPEND` + `libc::write()`, one syscall per event
  (`src/state/event_log.rs:114-152`). On Linux this is kernel-atomic up to
  frames ≤ 1 MiB (`event_log.rs:274-277`).
- **fsync every 1 s** on a background timer (`src/main.rs:1114-1132`).
  Maximum data loss on crash: **≤ 1 s of events**. This matches the Redis
  `appendfsync everysec` policy — documented tradeoff, same bound.
- **Periodic snapshots every 30 s** (`src/main.rs:868`). Full base snapshot
  every 10th cycle (every ~5 minutes); deltas in between
  (`main.rs:907`). Recovery: load latest base, replay deltas, replay WAL
  tail.

Net durability: on `kill -9` or kernel panic, you lose ≤ 1 s of events that
had not yet been fsync'd. No partial-event corruption (kernel `i_mutex`
guarantees atomic append on Linux).

### 2d. Opting in to stricter guarantees

Beava doesn't ship a dedup operator in v0.x. Options from strongest to
weakest:

1. **Client-side dedup store.** The client maintains a Redis/SQLite-backed
   set of recently-observed idempotency keys and skips a PUSH whose key
   was already sent. This is how production users of at-least-once
   systems handle it today. Trade-off: one external dependency.
2. **Event-time-bucketed double-count tolerance.** Configure your
   downstream aggregation to tolerate the few % of double-counts inherent
   to retries. For most fraud pipelines this is acceptable because the
   model is trained on the noisy counts. Not acceptable for money.
3. **Query-time dedup.** Store raw events with a client-generated
   `event_id` field and a custom `distinct_count("event_id", window=...)`
   feature. This uses Beava's HLL dedup inside the operator — the
   `distinct_count` of a set with duplicates is still the true cardinality.
   Counts become cardinalities, not totals. Subtle; test your model.

A first-class idempotency key is tracked as a post-v1.0 feature.

### 2e. Code pointers

- Push path: `src/server/tcp.rs::handle_push_core_ex` (called from OP_PUSH
  at `tcp.rs:1692`).
- Protocol note on at-least-once boundary replay:
  `src/server/protocol.rs:157-161`.
- Event log atomic append: `src/state/event_log.rs:103-152`.
- fsync timer: `src/main.rs:1114-1132` (1 s).
- Snapshot timer: `src/main.rs:865-960` (30 s, full every 10th).
- Launch copy's honest claim: `launch/show-hn.md:21`.

---

## 3. Event-time, watermarks, and late events

### 3a. What it IS

**Event-time primary, wall-clock fallback, with a fixed 5-second allowed
lateness and silent late-drop.** All implementation lives in
`src/engine/event_time.rs`.

Rules:

1. **Parse `_event_time` from the payload.** If present and parseable, it
   becomes the event's event-time (`event_time.rs:72-116`,
   `parse_event_time`). Supported forms:
   - ISO-8601 `YYYY-MM-DDTHH:MM:SSZ` or `.fff` fractional seconds.
   - Unix integer: < 2^31 interpreted as seconds, ≥ 2^31 as milliseconds
     (`event_time.rs:59`).
   - Unix float: same seconds/ms heuristic.
2. **Fall back to wall-clock arrival time if absent or unparseable.** The
   fallback is `SystemTime::now()` on the server at the moment the TCP
   dispatcher receives the event (`src/server/tcp.rs:1675`). Garbage
   strings, nested objects, negative numbers → fallback. Never errors.
3. **Window buckets are event-time-bucketed.** The engine passes
   `event_time` as the `now` parameter to all operators
   (`src/server/tcp.rs:1697`, `src/engine/pipeline.rs:877`). Ring-buffer
   bucket selection uses this value (`src/engine/window.rs::advance_to`).
4. **Per-stream watermark = `max(event_time observed) − 5 s`.** The
   lateness constant is `WATERMARK_LATENESS = Duration::from_secs(5)`
   (`event_time.rs:50`). Locked at 5 s for v0; per-stream tunable is
   post-v1.0.
5. **Late events are dropped silently.** Events with `event_time < watermark`
   are dropped by the TCP dispatcher after bumping
   `beava_late_events_dropped_total{stream}` (`tcp.rs:1680-1684`). The
   PUSH response is still a `{}` ack — the caller cannot tell the event
   was dropped from the response. Only the metric exposes it. This is
   intentional: late drops are expected at steady state and must not
   surface as retry-able errors.
6. **Watermarks propagate at stateful boundaries.** Stateless operators
   copy the input watermark verbatim; joins output `min(left_wm,
   right_wm)`; aggregations inherit the source stream's watermark
   (`event_time.rs:311-344`).

**Default bucket granularity** (`src/engine/register.rs:339`):

- Windows ≥ 1 hour → 1-minute buckets.
- Windows < 1 hour → 1-second buckets.

This means a `window="10m"` with no explicit `bucket=` uses 1-second
buckets, and a `window="24h"` uses 1-minute buckets. The visible clock-
edge jitter of a window result is bounded by one bucket duration.

### 3b. What it is NOT

- **Not processing-time semantics.** A client with a clock skew of +10
  minutes that doesn't send `_event_time` will have its events bucketed
  at the server's wall-clock, not the client's clock. A client that DOES
  send `_event_time` from its own (skewed) clock will have its events
  bucketed at its own time — including future-dated bucketing. Beava does
  not clamp or validate timestamps against server wall-clock in v0.
- **Not tunable-lateness.** The 5-second lateness constant is a compile-
  time value. There are no per-stream overrides on the wire or in the
  register JSON schema.
- **Not triggering/emission-based.** Beava has no trigger semantics.
  Aggregates are pull-query: `OP_GET` reads the current bucket state at
  query time. There is no "fire on watermark advance" emission; callers
  ask, and read the current state.
- **Not two-phase watermark alignment.** Joins take the min of input
  watermarks, but watermarks only advance (`propagate_join` uses
  `fetch_max` on the output, `event_time.rs:321-336`). Once the output
  watermark advances past T, a subsequent tighter `min(left, right)` at
  some T' < T will not regress the output watermark.
- **Not out-of-order re-bucketing.** An event arriving within the 5-second
  allowed lateness lands in its correct event-time bucket (not the
  arrival bucket). An event arriving outside the lateness window is
  dropped; it is **not** re-bucketed into a side-output for reprocessing.
  There is no allowed-lateness side channel.

### 3c. Opting in to stricter guarantees

- **Tighter lateness bound.** Today: edit `WATERMARK_LATENESS` in
  `src/engine/event_time.rs:50` and rebuild. Per-stream config ships
  post-v1.0.
- **Observable drops.** Scrape `beava_late_events_dropped_total{stream}`
  from `/metrics`. A non-zero counter means at least one event was
  dropped. This is your signal that lateness is too tight or clocks are
  skewed. The metric is zero if the feature is unused.
- **Guarded clock.** If you do not trust your producers' clocks, do not
  send `_event_time`. The server's wall-clock fallback eliminates client-
  side clock skew at the cost of reordering risk in arrival.
- **Exact replay.** `bv.fork(..., extract_at=[t1, t2, ...])` produces
  per-key state snapshots that are the state-as-of each `ti` after full
  historical replay (`src/server/replica_client.rs:412-438`, Phase 44-01).
  This is the supported primitive for "what did the features look like at
  time T?" and it IS consistent because replay is single-threaded and
  event-time-ordered.

### 3d. Code pointers

- Event-time parser: `src/engine/event_time.rs:72-116` (`parse_event_time`).
- Reserved field name: `event_time.rs:53` (`EVENT_TIME_FIELD =
  "_event_time"`).
- Watermark tracker: `event_time.rs:211-358` (`WatermarkTracker`,
  lock-free via DashMap + AtomicU64).
- Lateness constant: `event_time.rs:50` (`WATERMARK_LATENESS = 5 s`).
- Late-drop in dispatcher: `src/server/tcp.rs:1675-1687`.
- Late-drop counter: `event_time.rs:363-410` (`LateDropCounters`,
  `beava_late_events_dropped_total{stream}`).
- Watermark propagation rules:
  - Stateless: `event_time.rs:311-315`.
  - Join (min, monotone): `event_time.rs:321-336`.
  - Aggregation (inherits source): `event_time.rs:340-344`.
- Default bucket granularity: `src/engine/register.rs:339-345`.

---

## Summary table

| Property | Beava v0.x semantics |
|---|---|
| Fork consistency | Async replication, unbounded staleness, backpressure drops at 10 000-event buffer |
| Fork lag bound | No published SLO; monitor `replica_last_applied_ts_ms` vs primary |
| Ingest semantics | At-least-once. No event-id dedup. |
| Dedup TTL | N/A — dedup does not exist in v0.x |
| Crash durability | ≤ 1 s data loss (fsync every 1 s, same policy as Redis everysec) |
| Event clock | Event-time primary (`_event_time`), wall-clock fallback |
| Watermark | `max(event_time) − 5 s` per stream |
| Allowed lateness | 5 s, fixed, compile-time constant |
| Late events | Silently dropped, counter only (`beava_late_events_dropped_total`) |
| Trigger | None — pull-query model; readers ask for current bucket state |

If you need stronger guarantees than the column on the right, Beava v0.x is
the wrong tool. File an issue and we'll discuss the roadmap.
