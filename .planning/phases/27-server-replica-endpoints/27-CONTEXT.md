# Phase 27: Server-side replica endpoints (scope-aware) - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Add three scope-aware TCP opcodes to the server for read-only replica clients:

1. `OP_SNAPSHOT_FETCH{scope}` — stream current snapshot filtered by scope; emit HWM at end.
2. `OP_LOG_FETCH{from: u64, scope}` — stream log entries with `seq > from` matching scope, in seq order, until caught up.
3. `OP_SUBSCRIBE{scope}` — after handshake, push newly-accepted matching events indefinitely.

Server-side only. No client code, no SDK changes (Phase 28+).

**Out of scope:** client-side code, Python API, sample pull modes, predicate filters, write-back, rate-limiting, read-only token class.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Ship the minimal viable implementation that makes `tally clone` work end-to-end. Defer sophistication.

### Opcode numbering (from v0 Phase 25-01 reservations)
- `OP_SUBSCRIBE = 0x11` (reserved in 25-01 with stub; this phase implements)
- `OP_SNAPSHOT_FETCH = 0x12` (new)
- `OP_LOG_FETCH = 0x13` (new)

### Scope payload shape

```
Scope {
  streams:    Vec<String>,              // required, non-empty
  keys:       Option<Vec<String>>,      // optional explicit key set
  key_prefix: Option<String>,           // optional; mutually exclusive with keys
  pull:       String,                   // "all" only in v0.1
}
```

Wire format reuses existing `u16`-prefixed string framing in `protocol.rs`.

### Scope validation (reject before streaming)
1. Empty `streams` → error.
2. Unknown stream name → error.
3. Admin-token required for all replica opcodes; missing → error.
4. `keys` AND `key_prefix` both set → error.
5. `pull != "all"` → error ("not implemented; v0.1 supports 'all'").
6. `keys.len() > 10_000` → error.
7. Empty `key_prefix` string → error.

### Backpressure (Choice A1, user-confirmed)
**Drop the subscriber at 10,000 queued events.** Emit warning signal + metric; client reconnects with `OP_LOG_FETCH{from: last_seq}` to catch up. Event-count cap, not byte-count — simpler. Tune later if prod shows pressure.

### Auth (Choice B1, user-confirmed)
**Admin token only.** Reuse existing middleware from v0 Phase 22. Read-only token class deferred to post-v0 hardening. Acceptable risk for v0 since replica clients are trusted (ops/dev boxes, not public).

### Snapshot/log consistency handoff (Choice C1, user-confirmed)
**Pin HWM at fetch-start.** `OP_SNAPSHOT_FETCH` reads the immutable on-disk snapshot (Phase 9), streams filtered entries, and emits the snapshot's HWM seq at end. Any event with `seq > HWM` lands via `OP_LOG_FETCH{from: HWM}` on the next call. Natural fit with Phase 9's immutable-snapshot model — no extra freezing logic.

### Subscribe ordering (Choice D1, user-confirmed)
**Global seq order per subscriber.** Each subscriber has one mpsc channel fed in seq-monotonic order from the ingest hook. Clients apply events with `apply_event(e)` expecting monotonic seq — matches `bootstrap → catchup → live` state machine in Phase 29.

### OP_SNAPSHOT_FETCH flow
1. Validate scope + auth.
2. Open current snapshot (v7, per Phase 24); get HWM seq `S`.
3. Stream entries via `SnapshotReader::stream_filtered(scope)` — filter-in-iterator, never load whole file into memory.
4. Emit entries with existing 4-byte-length-prefix framing.
5. Emit `S` as the terminal message.

### OP_LOG_FETCH flow
1. Validate scope + auth.
2. For each stream in `scope.streams`, open per-stream log file (Phase 6 keyed; Phase 7 keyless both work).
3. Merge entries across per-stream files in seq order.
4. Apply key filter (explicit keys or prefix) after reading entry header.
5. Emit entries where `seq > from`; stop at current tail.

### OP_SUBSCRIBE flow
1. Validate scope + auth.
2. Register in `SubscriberRegistry` (shared `DashMap<conn_id, ReplicaSession>`, one per server process — same concurrency pattern as Phase 14).
3. On every ingested PUSH, the ingest path iterates subscribers, scope-matches, and enqueues into per-subscriber bounded `mpsc::Sender<Event>` (cap 10k).
4. Per-connection async task drains queue, serializes `(seq, event)`, writes to socket.
5. On queue full → drop subscriber (A1). On socket close → remove from registry.

### Plan split (Choice E, user-confirmed)
- **27-01**: `OP_SNAPSHOT_FETCH` + Scope struct + wire codec + scope validator + filter-iterator on `SnapshotReader`.
- **27-02**: `OP_LOG_FETCH` + per-stream log filter-iterator + Python-socket integration test covering clone-then-catchup flow (SNAPSHOT_FETCH → LOG_FETCH).
- **27-03**: `OP_SUBSCRIBE` + `SubscriberRegistry` + ingest-path hook + backpressure + metrics + signals.

### Metrics (extends Phase 25 `/metrics`)
- `tally_replica_subscriptions_active` gauge
- `tally_replica_events_pushed_total{stream}` counter
- `tally_replica_subscribers_dropped_total{reason="backpressure"|"disconnect"}` counter
- `tally_replica_snapshot_bytes_sent_total` counter

### Signals (extends Phase 25 `SignalRegistry`)
- `category=operational, severity=warning`: subscriber dropped (backpressure).
- `category=safety, severity=error`: auth failure on replica opcodes.

### Reserved, not implemented
- `OP_SCAN = 0x10` stays reserved with stub — not in scope.
- `BackfillSource` trait (Phase 22 reservation) — document interface in 27-01 so Phase 33 plugs in cleanly. No code.

### No rate-limiting
Document as known gap. A reconnect-loop client can DoS the server. Fine for v0 (trusted clients). Future hardening phase owns this.

</decisions>

<code_context>
## Existing code touchpoints

- `src/server/tcp.rs` — protocol dispatch; add three opcode handlers.
- `src/server/protocol.rs` — string framing helpers; reuse.
- `src/state/snapshot.rs` — snapshot format v7; add `stream_filtered(scope) -> impl Iterator`.
- Phase 6 event log module — per-stream log files; add filter-iterator.
- `src/engine/pipeline.rs` — ingest path (push_with_cascade); add `notify_subscribers` hook after successful append.
- `src/server/signals.rs` — Phase 25 SignalRegistry.
- `src/server/http.rs` — `/metrics`; extend.
- Phase 22 admin-token middleware (`require_loopback_or_token`) — reuse.

</code_context>

<specifics>
## Specific technical notes

- **Snapshot reader** must be streaming (don't load full file). Read frame-at-a-time; filter; emit or skip.
- **Subscriber registry** uses `DashMap<u64, ReplicaSession>` for lock-free hot path on ingest.
- **Integration tests** in `tests/integration/test_replica_*.py` using raw asyncio sockets. No Rust client yet.
- **Seq-monotonic ordering** for SUBSCRIBE: ingest hook must take events in the same order they're assigned sequence numbers. Today the ingest path is serialized per-stream; for cross-stream ordering, the registry enqueues in the order `notify_subscribers` is called (which matches seq assignment order if ingest is seq-monotonic — verify this is the case in Phase 14 concurrency model, else serialize through a single channel).

</specifics>

<deferred>
## Deferred

- Sample pull modes — v0.2
- Predicate scope (`key_filter="balance > 1000"`) — v0.2
- Read-only replica token class — post-v0 hardening
- Rate-limiting — post-v0 hardening
- `OP_SCAN` — v0.2
- BackfillSource plug — Phase 33
- Write-back — Phase 34
- Byte-based backpressure cap (vs event-count) — revisit if prod shows pressure

</deferred>

---

*Phase: 27-server-replica-endpoints*
*Sources: `.planning/research/local-replica-design.md`, `.planning/phases/27-server-replica-endpoints/27-CONTEXT-auto.md`, Phase 25-01 reserved opcodes, Phase 6 event log, Phase 14 DashMap pattern*
