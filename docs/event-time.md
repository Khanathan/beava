# Event Time Semantics

This is the authoritative reference for Beava's event-time model, covering how events
are bucketed, how watermarks advance, and the guarantees provided at stream boundaries.

## Contents

- [Bucket Assignment](#bucket-assignment)
- [Watermark Lateness Defaults and Per-Stream Configuration](#watermark-lateness-defaults-and-per-stream-configuration)
- [Crash-Replay Determinism](#crash-replay-determinism)
- [TTL Semantics](#ttl-semantics)
- [Backfill](#backfill)
- [Join Idle-Input Behavior](#join-idle-input-behavior)
- [Fork Watermark Propagation](#fork-watermark-propagation)

---

## Bucket Assignment

Beava's ring-buffer operators (count, sum, percentile over a sliding window) partition
time into fixed-width buckets. Correct bucketing is foundational: if two events with
different `_event_time` values are placed into the wrong bucket, every downstream
aggregate is wrong.

### How events are bucketed

Every event carries an optional `_event_time` field in its payload (Unix milliseconds
since epoch, or an RFC 3339 string). The `parse_event_time` helper at
`src/engine/event_time.rs` resolves the bucketing clock in the following priority order:

1. `payload._event_time` as an integer (ms since epoch).
2. `payload._event_time` as an RFC 3339 string.
3. Fallback: the server wall-clock time at the moment the event was received.

The resulting `SystemTime` is the event's **event-time clock** for all downstream
operators. This clock is used exclusively for bucket assignment and watermark
observation; the `LogEntry.timestamp` field is only the wall-clock-at-append value and
is never used for bucketing in either the live-ingest or crash-replay paths.

### Bucket boundaries are UNIX-epoch-relative

Ring buffer operators assign each event to the bucket
`floor(event_time_ms / bucket_width_ms)`. Bucket boundaries are absolute
(relative to `UNIX_EPOCH`), not relative to when the server started or when the
stream was registered. This ensures that:

- Events arriving from different machines with synchronized clocks always land
  in the same bucket.
- Historical backfills (events 30 days old) produce the same bucket IDs as they
  would have during live ingestion 30 days ago.

### Batch ingest: per-event bucket assignment

When events arrive via `POST /push-batch/{stream}`, each event in the batch is
bucketed individually by its own `_event_time`. A batch may contain events spanning
hours or days; the `push_batch_with_cascade_no_features` engine primitive
(`src/engine/pipeline.rs`) groups events by bucket before acquiring any ring-buffer
locks, so the overall batch completes in O(B) lock operations where B is the number
of distinct buckets in the batch — not O(N) per event.

This is the fix for CORR-01 (Wave 2a). If events in a single batch were bucketed by
the same shared wall-clock timestamp, historical events would be dropped as too-old
and live events would all be placed in the current bucket regardless of their actual
`_event_time`.

**Pitfall:** If you push a batch where `event_times[0]` is 1 hour ago and
`event_times[1..N]` are recent, the old approach would route every event to the
1-hour-ago bucket (the minimum), evicting all recent events as future. The per-event
path routes each event to its own bucket correctly.

### Implementation reference

See `src/engine/pipeline.rs::push_batch_with_cascade_no_features` for the
group-by-bucket implementation and `src/engine/event_time.rs::parse_event_time` for
the `_event_time` resolution logic.

---

## Watermark Lateness Defaults and Per-Stream Configuration

A **watermark** tracks the frontier of event-time progress for a stream. Beava's
watermark model is the standard out-of-order tolerance model: the watermark lags
behind the maximum observed event time by a configurable lateness window. Events
arriving within this window are still accepted and bucketed correctly; events older
than `observed_max - lateness` are dropped as too-late.

### Default: 5 seconds

The global default lateness is 5 seconds, defined as `WATERMARK_LATENESS` at
`src/engine/event_time.rs:51`:

```rust
pub const WATERMARK_LATENESS: Duration = Duration::from_secs(5);
```

For most streaming workloads with sub-second network latency, 5 seconds is
sufficient to absorb out-of-order delivery without accumulating significant state.

### Per-stream override

For workloads with higher latency or intentional out-of-order delivery (e.g.,
mobile SDK batching events over spotty connectivity, or multi-hop Kafka pipelines),
the default 5-second lateness is too short. Each stream can override the lateness
window via the `@bv.stream` decorator:

```python
@bv.stream(
    key="user_id",
    watermark_lateness="10m",  # accept events up to 10 minutes late
)
class Transactions:
    user_id: str
    amount: float
    _event_time: int  # ms since epoch
```

The string is parsed server-side by `humantime` (e.g., `"10m"`, `"30s"`, `"2h"`).
This sets `StreamDefinition.watermark_lateness = Some(Duration::from_secs(600))`.

The `WatermarkTracker` at `src/engine/event_time.rs` stores per-stream overrides in
a `DashMap<String, Duration>`. Lookup order is:

1. Stream-specific override (if registered via `set_lateness_for`).
2. Global fallback: `WATERMARK_LATENESS` (5 s).

### Backward compatibility

Snapshots written before per-stream lateness was introduced load cleanly because
`StreamDefinition.watermark_lateness` is `Option<Duration>` and defaults to `None`
when absent from the serialized form. `None` maps to the global 5-second fallback
via `WatermarkTracker::lateness_for` — no migration needed (CORR-04).

### Watermark propagation (γ rule)

Downstream stream watermarks are computed as a function of upstream watermarks:

- **Stateless pass-through:** `propagate_stateless(from, to)` copies `observed_max`
  verbatim. The downstream lateness window applies when the watermark is _read_, not
  when it is propagated.
- **Stream-stream join:** `propagate_join(left, right, out)` sets `observed_max(out)`
  to `min(observed_max(left), observed_max(right))`. The output stream can only
  advance as fast as the slower input.
- **Table cascade:** `propagate_table_cascade(source, table)` mirrors `propagate_stateless`.

This γ-propagation is applied after every successful event push and ensures downstream
pipelines always see a watermark that is safe (no future events can appear before it
from any upstream input).

---

## Crash-Replay Determinism

Beava guarantees that crash-replay produces **bit-identical feature values** to
live-ingest for the same event sequence, provided events carry explicit `_event_time`
payloads.

### Write-before-extract ordering

Every event is written to the per-stream event log **before** feature extraction:

1. HTTP handler receives event → `EventLog::append` writes the raw payload to disk.
2. `handle_push_batch` calls `push_batch_with_cascade_no_features` to extract features.

If the process crashes between steps 1 and 2, the event is in the log but features
were never extracted. On restart, `run_backfill` replays the log and re-extracts
the features. Because the payload is in the log, no data is lost.

### D-15 / CORR-06: payload event-time is the source of truth

During replay, `run_backfill` at `src/server/tcp.rs` processes each `LogEntry` and
calls:

```rust
let event_time = parse_event_time(&event, entry.timestamp);
let _ = engine.push_for_backfill(&stream_name, &event, &state.store, event_time, &feature_names);
```

`entry.timestamp` is the wall-clock time at which the event was written to the log.
It is passed as the _fallback_ to `parse_event_time`, not as the primary event-time
clock. If the payload contains `_event_time`, that value is used for bucketing — just
as it was during live ingest.

Without this fix (using `entry.timestamp` directly), events with an explicit
`_event_time` would be re-bucketed to their wall-clock arrival time on replay,
producing feature values different from the live-ingest run.

### At-least-once semantics

Events written to the log but not yet `fsync`'d may be lost on a hard crash (`kill -9`)
if the OS write buffer has not been flushed. The log is periodically `fdatasync`'d
by a background timer; the background interval is the durability window.

A `?sync=1,durable=1` upgrade path is scaffolded (D-27) but not wired in v1.0-launch.
See [EventLog::append_with_fsync](#) and [Durability Semantics](http-api.md#durability-semantics)
in the HTTP API docs.

### Verification

The crash-replay parity guarantee is exercised end-to-end by `tests/ship_gate.rs`
(`test_ship_gate_backfill_crash_recover`). The test:

1. Boots a server, registers "Txns" with a keyed count feature.
2. Pushes 1000 events with `_event_time` values spanning -30 days .. now.
3. Drops the server (simulates `kill -9`).
4. Boots a fresh server against the same data directory, triggering log replay.
5. Reads features for every key (u0..u9) and asserts bit-identical parity.

If D-15 is not applied, the test fails because backfill buckets events by
`entry.timestamp` (wall-clock), which differs from the payload `_event_time`.

---

## TTL Semantics

Beava supports two TTL mechanisms:

- **`entity_ttl`**: how long to retain an entity's state (all streams) after the
  entity's last event.
- **`history_ttl`**: how long to retain events in the per-stream event log for
  backfill purposes.

Both are measured against the **event-time clock**, not wall-clock (`SystemTime::now()`).

### CORR-07 / D-17: eviction uses the watermark clock

The eviction scan in `src/state/eviction.rs::evict_expired_stream_entries` sources
the eviction clock from `WatermarkTracker::observed_max(stream)`:

```rust
let scan_clock = engine.watermarks.observed_max(stream_name).unwrap_or(now);
let age = scan_clock.duration_since(last_event).unwrap_or(Duration::ZERO);
```

If the stream's watermark is not yet set (no events observed), the scan falls back to
wall-clock `now` — preserving existing behavior for streams that have never received
an event.

### Why event-time TTL matters for backfill

Without this fix, ingesting 30-day-old historical events under a 7-day `entity_ttl`
would immediately evict every entity — the age computation `now - last_event_at`
would return 30 days, which exceeds the TTL. No features would survive past the
backfill run.

With event-time TTL, eviction fires when the **watermark** (derived from the latest
observed `_event_time`) advances past `last_event_at + ttl`. A 30-day-old event
ingested during a historical backfill is not evicted until newer events advance the
watermark past the 30-day mark plus the TTL.

### Implementation reference

See `src/state/eviction.rs:~46-70` for the eviction loop and
`src/engine/event_time.rs::WatermarkTracker::observed_max` for the watermark
clock source.

---

## Backfill

Backfill uses the single-event ingest path; the 2a batch-path fix does not affect
backfill bucketing.

During `run_backfill`, each log entry is replayed via `push_for_backfill`
(`src/engine/pipeline.rs:push_for_backfill`) — the single-event path — not through
`push_batch_with_cascade_no_features`. This means the 2a group-by-bucket optimization
and its associated correctness fixes are not involved in backfill; backfill has always
used per-event event-time bucketing (CORR-05, verified by
`tests/test_backfill_uses_single_event_path.rs`).

---

## Join Idle-Input Behavior

In v1, joins require both sides to produce events for the downstream watermark to advance;
if one side is idle, the join output watermark stalls.
Per-stream idle markers (deferred to v1.1, see DX-06 in REQUIREMENTS.md) would fix join-stall
with silent sides by advancing the watermark when a side is quiescent, but are not in v1.

The practical implication: if a stream-stream join has one input that stops emitting
events, the downstream aggregation pipeline will cease to produce new features until
the silent input resumes. Per-stream idle markers are tracked as DX-06 for v1.1
to resolve this limitation.

---

## Fork Watermark Propagation

Beava supports a `tally fork` workflow where a local replica ingests events from an
upstream production server and runs independent local pipelines against the live feed.

### How fork watermarks advance (D-19 / CORR-08)

Fork replicas receive events via `replica_ingest_batch` at `src/server/tcp.rs:~1012-1222`.
After each event is successfully applied to the pipeline, the replica advances its
per-stream watermark:

```rust
// D-19 / CORR-08: advance the replica's watermark per event so downstream
// table-cascade γ-propagation fires. Mirrors the live-ingest call at tcp.rs:1750.
engine.watermarks.observe(stream_name, event_time);
```

This mirrors the live-ingest path (`tcp.rs:~1750`) where the TCP dispatcher calls
`watermarks.observe` after each successful push. Without this call, the fork replica's
watermarks would remain at `None` indefinitely, blocking all downstream cascade
propagation.

### Fork demo workflow

A typical `tally fork` session:

1. **LOG_FETCH catchup:** the replica requests the upstream server's event log for
   each stream, replaying historical events from disk. During this phase, watermarks
   advance per event via `run_backfill` → `parse_event_time`.
2. **SUBSCRIBE live-tail:** after catchup, the replica subscribes to the live event
   stream. Each incoming event is processed by `replica_ingest_batch`, advancing
   watermarks per event.
3. **Local pipelines:** any downstream tables or aggregations registered against the
   replica fire as watermarks advance — matching live-ingest behavior exactly.

### Watermark correctness on the replica

Because the replica uses `parse_event_time(&payload, entry.timestamp)` for both
catchup (backfill) and live-tail events, the watermark clock on the replica is
driven by the same payload `_event_time` as the upstream server. Downstream
cascade timing is therefore identical to what the upstream produces, enabling
"shadow mode" pipelines that can be compared directly against production.

---

## Related

- [HTTP API — Durability Semantics](http-api.md#durability-semantics): `?sync=1`
  (in-memory drain) vs the scaffolded `?sync=1,durable=1` fsync path (D-27).
- `docs/concepts.md` (Phase 47): high-level architecture overview, pipeline DAG,
  stream vs table distinction.
- `src/engine/event_time.rs`: authoritative implementation of `parse_event_time`,
  `WatermarkTracker`, γ-propagation rules, and `WATERMARK_LATENESS`.
- `src/server/tcp.rs::run_backfill`: crash-replay entry point; D-15 fix site.
- `src/state/eviction.rs`: TTL eviction loop; D-17 fix site.
