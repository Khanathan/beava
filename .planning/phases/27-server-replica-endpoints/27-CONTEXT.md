# Phase 27: Server-side replica endpoints (scope-aware) - Context

**Gathered:** 2026-04-14
**Revised:** 2026-04-15 (Option K — snapshot + subscribe only, no LOG_FETCH)
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Add two scope-aware TCP opcodes to the server for read-only replica clients:

1. `OP_SNAPSHOT_FETCH{scope}` — in-memory filter of the current snapshot; send filtered `SerializableEntityState` subset plus a `snapshot_taken_at: SystemTime` header.
2. `OP_SUBSCRIBE{scope}` — after handshake, push newly-accepted matching events indefinitely. Per-connection accept order (no global seq).

**Cut from v0** (deferred to v0.2 when we do the schema work):
- `OP_LOG_FETCH{from, scope}` — requires event-log seq, which doesn't exist today.
- Global seq-monotonic ordering across the cluster — requires a seq counter.
- Streamable per-entry snapshot frames — requires snapshot format v8.

**Why Option K works for v0 demo:**
Snapshot already contains the answer users want — current aggregated feature state keyed by entity. Subscribe gives live updates. The narrow gap between snapshot-read and subscribe-register is closed client-side by a subscribe-first buffered-replay dance. No schema change, no seq counter, no new persistence.

**Out of scope:** `OP_LOG_FETCH`, global seq, sample pull modes, predicate filters, write-back, rate-limiting, read-only token class.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Zero persistence changes. Two opcodes. Filter in-memory.

### Opcode numbering
- `OP_SUBSCRIBE = 0x11` — implements the stub reserved in v0 Phase 25-01.
- `OP_SNAPSHOT_FETCH = 0x12` — new.
- `OP_LOG_FETCH = 0x13` — **NOT implemented in v0**. Opcode stays unreserved; claim it in v0.2.

### Scope payload shape

```
Scope {
  streams:    Vec<String>,              // required, non-empty
  keys:       Option<Vec<String>>,      // optional explicit key set
  key_prefix: Option<String>,           // optional; mutually exclusive with keys
  pull:       String,                   // "all" only; reject others
}
```

Wire format reuses existing `u16`-prefixed string framing in `protocol.rs`.

### Scope validation (reject before streaming)
1. Empty `streams` → error.
2. Unknown stream name → error.
3. Admin-token required; missing → error.
4. `keys` AND `key_prefix` both set → error.
5. `pull != "all"` → error.
6. `keys.len() > 10_000` → error.
7. Empty `key_prefix` string → error.

### OP_SNAPSHOT_FETCH flow
1. Validate scope + auth.
2. Acquire the current snapshot: either load it from disk (`load_snapshot_file`) or read a cached in-memory copy — **whichever is simpler to wire**. The snapshot is one `postcard(BaseSnapshotState)` blob; deserializing it into memory is how the server already reads it.
3. Iterate `BaseSnapshotState.entities: Vec<(String, SerializableEntityState)>`.
4. For each entity, check: `∃ stream in entity.streams where stream.name ∈ scope.streams` AND (`scope.keys is None OR entity.key ∈ scope.keys`) AND (`scope.key_prefix is None OR entity.key.starts_with(scope.key_prefix)`).
5. Collect matched entities into a new `BaseSnapshotState` (filtered subset). Keep same entity-shape; client deserializes into an identical struct on its side.
6. Serialize the filtered `BaseSnapshotState` with `postcard`. Wrap in a single response frame prefixed with a header message carrying `snapshot_taken_at: SystemTime` (the moment this request began processing — new field, response-only, not persisted).
7. Send header frame, then payload frame. Close (or leave connection open for another request).

**Important:** We accept that SNAPSHOT_FETCH loads the snapshot into memory on the server for each request. For v0 demo on a Hetzner CX22 this is fine (snapshot is under 100MB in typical demo load). If future scale demands it, move to streamable snapshot (v0.2 schema work).

### OP_SUBSCRIBE flow
1. Validate scope + auth.
2. Register this connection in a `SubscriberRegistry` (shared `DashMap<conn_id, ReplicaSession>`). Each session has its scope + a bounded `mpsc::Sender<Event>` (cap 10,000).
3. Add a `notify_subscribers(event)` hook in the ingest path right after a PUSH is successfully accepted and logged. Hook iterates registered subscribers, scope-matches, and `try_send`s. Full queue → drop subscriber + emit drop metric + warning signal.
4. Per-connection async task drains the queue and writes `(timestamp, event_payload)` to the socket. **No seq** — only timestamp and payload per entry.
5. On socket close → remove from registry.

### Per-connection ordering guarantee (scaled-back from D1)
Events delivered via SUBSCRIBE are in the **per-subscriber receive order**, which equals the order `notify_subscribers` fires for scope-matching events. No cross-subscriber ordering guarantee. No global seq. Documented; client-side code must tolerate.

### Client-side snapshot/subscribe gap-closing dance (not server responsibility — documented here for client plan writers)
For streaming mode (Phase 31), the client:
1. Open socket, SUBSCRIBE first. Buffer all incoming events in memory.
2. Open second socket (or reuse after SUBSCRIBE sends "ready"), SNAPSHOT_FETCH.
3. Apply snapshot to local StateStore.
4. From buffered events, drop those with `timestamp ≤ snapshot_taken_at`; apply the rest via `apply_event`.
5. Resume normal streaming — every newly-arrived event is applied.

Server doesn't need to know about this; it just honors both opcodes. The dance is entirely client-side.

### Backpressure (A1, unchanged)
Drop at 10,000 queued events. Metric + warning signal.

### Auth (B1, unchanged)
Admin-token only. Reuse Phase 22 middleware.

### No rate-limiting
Known gap. Documented.

### Plan split (2 plans)
- **27-01**: `OP_SNAPSHOT_FETCH` + shared `Scope` struct + wire codec + scope validator + in-memory filter of `BaseSnapshotState` + response header with `snapshot_taken_at`. Rust integration test + Python asyncio test verifying filtered deserialization round-trip.
- **27-02**: `OP_SUBSCRIBE` + `SubscriberRegistry` + ingest-path `notify_subscribers` hook + 10k backpressure drop + metrics + signals. Rust integration test + Python asyncio test verifying per-subscriber ordering and backpressure-drop behavior.

### Metrics (extends Phase 25 `/metrics`)
- `tally_replica_subscriptions_active` gauge
- `tally_replica_events_pushed_total{stream}` counter
- `tally_replica_subscribers_dropped_total{reason="backpressure"|"disconnect"}` counter
- `tally_replica_snapshot_bytes_sent_total` counter

### Signals (extends Phase 25 `SignalRegistry`)
- `category=operational, severity=warning`: subscriber dropped (backpressure).
- `category=safety, severity=error`: auth failure on replica opcodes.

</decisions>

<code_context>
## Existing code touchpoints

- `src/server/tcp.rs` — protocol dispatch; add two opcode handlers (0x11, 0x12).
- `src/server/protocol.rs` — string framing helpers; reuse.
- `src/state/snapshot.rs` — `BaseSnapshotState`, `SerializableEntityState`. Used as-is; no new reader API needed beyond standard in-memory load.
- `src/engine/pipeline.rs` — ingest path (where PUSH events land). Add `notify_subscribers(event)` call after successful append.
- `src/server/signals.rs` — Phase 25 SignalRegistry.
- `src/server/http.rs` — `/metrics`; extend.
- Phase 22 admin-token middleware (`require_loopback_or_token`) — reuse.
- New: `src/server/replica.rs` — `SubscriberRegistry`, `ReplicaSession`, scope filter utility.

## What changed from auto-CONTEXT (pre-Option-K)
- `SnapshotReader::stream_filtered` — removed. Filter in-memory against `BaseSnapshotState.entities`.
- Snapshot HWM emission — removed. Replaced with response-header `snapshot_taken_at: SystemTime`.
- `OP_LOG_FETCH` entirely — removed.
- Per-stream log filter iterator — removed.
- Global seq-monotonic subscribe — downgraded to per-subscriber order.

</code_context>

<specifics>
## Specific technical notes

- **Snapshot memory usage**: loading a >100MB snapshot on each SNAPSHOT_FETCH is acceptable for v0 demo. If the snapshot is already cached in `AppState`, reuse the cached copy; otherwise load fresh per request. No mmap needed.
- **`snapshot_taken_at` semantics**: this is the SystemTime when the server begins handling the SNAPSHOT_FETCH request, not when the snapshot file was created. Good enough: for the client's gap-closing dance, any monotonic timestamp that's ≥ when the snapshot was taken works. Using "now at serve time" is an upper bound and therefore safe (client may drop a few events that arrived between snapshot-taken-time and serve-time, but those events are already in the snapshot's aggregated state, so dropping is correct).
- **Subscriber registry concurrency**: `DashMap` for lock-free ingest-hot-path. Same pattern as Phase 14.
- **Integration tests in Python asyncio** using raw sockets. No Rust client exists yet (Phase 28+).

</specifics>

<deferred>
## Deferred to v0.2

- `OP_LOG_FETCH` + global seq — requires event-log seq + snapshot v8 with event_log_hwm.
- Streamable snapshot format (frame-at-a-time) — requires snapshot format v8.
- Global seq-monotonic subscribe ordering — requires seq.
- Sample pull modes — v0.2.
- Predicate scope — v0.2.
- Read-only replica token class — post-v0 hardening.
- Rate-limiting — post-v0 hardening.
- BackfillSource plug — Phase 33.
- Write-back — Phase 34.

</deferred>

---

*Phase: 27-server-replica-endpoints*
*Revision: Option K (2026-04-15) — snapshot + subscribe only*
*Source directive: user "easiest for v0 and demo" 2026-04-14; "historical + latest streaming — two of that is enough" 2026-04-15*
