# Phase 27: Server-side replica endpoints (scope-aware) - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from v0.1 local replica design doc + v0 Phase 25 reserved opcodes

<domain>
## Phase Boundary

Add three new TCP opcodes to the server that enable a read-only replica client (v0.1 Phase 28+ consumer). All three are **scope-aware** — the client declares which streams (and optionally which keys) it cares about, and the server filters snapshot bytes, log entries, and live subscriptions to match. No full-cluster pull is ever served.

The three opcodes:

1. **`OP_SNAPSHOT_FETCH{scope}`** — server streams the current snapshot, filtered to only entries matching the scope. Returns the snapshot's HWM seq at the end.
2. **`OP_LOG_FETCH{from: u64, scope}`** — server streams log entries with `seq > from` and matching scope, in seq order, until caught up to current tail.
3. **`OP_SUBSCRIBE{scope}`** — after the initial protocol handshake, server continues pushing newly-accepted events that match the scope, with their seq, indefinitely.

`OP_SUBSCRIBE` was reserved in v0 Phase 25-01 (opcode `0x11` with a stub "not implemented" response). This phase delivers the full implementation.

Server-side only. No client-side work, no Python SDK changes (Phase 30 handles the SDK surface).

**Pull modes** (from design doc §"Pull modes"): in v0.1, only `pull="all"` (pull every entity matching the scope) is supported. The server accepts the parameter in the opcode payload but rejects anything other than `"all"` — future-shape compat so v0.2 sample modes land cleanly.

**Out of scope for Phase 27:**
- Client-side code (Phase 28 onward)
- Python Pipeline API (Phase 30)
- Mode state machine / resume logic (Phase 29)
- Sample modes (v0.2)
- Predicate-level key filter (v0.2)
- Write-back / promote (Phase 34, stretch)

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Opcode numbering

Reserved in v0 Phase 25-01:
- `OP_GET_MULTI = 0x0D` (shipped)
- `OP_SCAN = 0x10` (reserved, not impl'd — stays reserved in Phase 27, not part of this phase)
- `OP_SUBSCRIBE = 0x11` (reserved in 25-01 with stub; THIS phase implements)

New in Phase 27:
- `OP_SNAPSHOT_FETCH = 0x12`
- `OP_LOG_FETCH = 0x13`

### Scope payload shape

All three opcodes accept the same scope struct:

```
Scope {
  streams:    Vec<String>,              // required, non-empty
  keys:       Option<Vec<String>>,      // optional explicit key set
  key_prefix: Option<String>,           // optional key prefix (mutually-exclusive with keys)
  pull:       String,                   // "all" in v0.1; reject others
}
```

Wire format (reuse existing protocol.rs string framing):
```
[u16 count][count × u16-prefixed string] streams
[u8 flags: has_keys, has_key_prefix]
[if has_keys:    u16 count][count × u16-prefixed string] keys
[if has_key_prefix: u16-prefixed string] key_prefix
[u16-prefixed string] pull mode
```

### Scope validation at handshake

Rejection rules (return STATUS_ERROR immediately, do not start streaming):

1. `streams` is empty → "scope must declare at least one stream"
2. Unknown stream name → "unknown stream: {name}"
3. Stream requires admin token and caller didn't present one → "unauthorized for stream: {name}" (re-use Phase 22 admin-token gate)
4. Both `keys` AND `key_prefix` set → "keys and key_prefix are mutually exclusive"
5. `pull != "all"` → "pull mode '{pull}' not implemented; v0.1 supports 'all'"
6. Keys list > 10,000 entries → "keys list exceeds v0.1 cap (10k)"
7. Key prefix empty string → "key_prefix must be non-empty"

### OP_SNAPSHOT_FETCH implementation

Server flow:
1. Validate scope + auth
2. Acquire a consistent snapshot (the server keeps a current immutable snapshot on disk per Phase 9 incremental snapshots; read that)
3. Stream the snapshot bytes filtered through the scope:
   - For each keyed-entity row in the snapshot, check `entity.stream_name ∈ scope.streams` AND (`scope.keys is None OR entity.key ∈ scope.keys`) AND (`scope.key_prefix is None OR entity.key.starts_with(scope.key_prefix)`)
   - If match: emit the row (chunked framing — existing 4-byte-length-prefix pattern)
4. End of stream → send the snapshot's HWM seq as the final message
5. Close stream (or keep connection open for client to send next opcode)

Key technical detail: the snapshot file format is v7 (per Phase 24). The server's snapshot reader in `src/state/snapshot.rs` needs a "filter-iterator" mode that reads entries streamingly and applies a scope predicate. Don't load the whole snapshot into memory — it could be GBs.

### OP_LOG_FETCH implementation

Server flow:
1. Validate scope + auth
2. For each stream in `scope.streams`, open the per-stream log file (Phase 6 SSD event log — keyed streams have per-stream log files)
3. Iterate log entries in seq order (merge across per-stream files by seq)
4. Filter: `entry.stream_name ∈ scope.streams` AND key-filter logic from above
5. Emit only entries with `seq > from`
6. Stream until caught up to current tail OR until client closes

Technical detail: the per-stream log files make stream-level filtering trivial (just don't open out-of-scope files). Key filtering is applied after reading the entry header. The "catch up to current tail" semantics mean the server needs to know current HWM at stream start and bail when we pass it — subsequent events come via `OP_SUBSCRIBE`.

### OP_SUBSCRIBE implementation

Server flow:
1. Validate scope + auth
2. Register this connection in a `SubscriberRegistry` keyed by connection ID, storing the scope + a bounded send queue
3. On every newly-accepted PUSH that lands in any stream, the ingest path checks each registered subscriber's scope; matching ones get the event enqueued into their send queue
4. A per-connection async task drains the send queue, serializes events with their seq, writes to socket
5. Backpressure: if a subscriber's queue exceeds `MAX_BACKPRESSURE` (default 10,000 events), **drop the subscriber** with a warning log + signal emission (`SignalRegistry`, category=operational). Client reconnects from last-seq to catch up.

### Backpressure + observability

Metrics (wired into Phase 25's `/metrics` endpoint):
- `tally_replica_subscriptions_active{}` gauge
- `tally_replica_events_pushed_total{stream}` counter
- `tally_replica_events_filtered_total{stream,reason}` counter (out-of-scope reasons: `wrong_stream`, `wrong_key`, `prefix_mismatch`)
- `tally_replica_subscribers_dropped_total{reason}` counter (`backpressure`, `auth_expired`, `client_disconnected`)
- `tally_replica_snapshot_bytes_sent_total{}` counter

Warnings (wired into Phase 25's `/debug/warnings` / `SignalRegistry`):
- `category=operational, severity=warning`: subscriber dropped due to backpressure
- `category=safety, severity=error`: auth failure on replica endpoints

### Session state on the server

New struct (probably in `src/server/replica.rs`):

```rust
struct ReplicaSession {
    conn_id: u64,
    scope: Scope,
    last_sent_seq: u64,
    send_queue: mpsc::Sender<Event>,
    backpressure_limit: usize,
    created_at: Instant,
}

struct SubscriberRegistry {
    sessions: DashMap<u64, ReplicaSession>,
}
```

One registry shared across all TCP handlers. Inserts on `OP_SUBSCRIBE`, removes on disconnect or drop.

### Reuse: per-stream log files from Phase 6

Phase 6 shipped keyed-stream log files (`history_ttl`-bounded, per-stream compaction). Phase 27 reuses these for both `OP_LOG_FETCH` (open + stream) and as the input for `OP_SUBSCRIBE`'s ingest-path hook (events written to the log also get pushed to subscribers).

Keyless streams (Phase 7 composable-pipeline feature) have append-only logs too. Both work.

### Forward-compat hook for Phase 33 (backfill sources)

The `BackfillSource` trait reserved in v0 Phase 22 should still apply here for future "pull from S3 instead of cluster" — but for Phase 27, we only implement the in-cluster impl. Document the interface in the plan so Phase 33 can plug in cleanly.

### Auth model

Reuse the existing admin-token middleware from v0 Phase 22. For v0.1, all three replica opcodes require admin token. Phase open-questions mention a future "read-only replica token" class — reserved but not shipped this phase.

### No rate-limiting yet

Phase 27 ships without rate-limiting on replica endpoints. A single subscriber that keeps reconnecting can hit the server hard. Defer rate-limiting to a future hardening phase. Document the gap.

</decisions>

<code_context>
## Existing Code Insights

- `src/server/tcp.rs` — protocol dispatch; add three new opcodes
- `src/server/protocol.rs` — wire format helpers (reuse string framing)
- `src/state/snapshot.rs` — snapshot format v7; needs streaming/filtered reader mode
- `src/engine/log.rs` (or wherever Phase 6 event log lives) — per-stream log files
- `src/engine/pipeline.rs` — ingest path (push_with_cascade); add subscriber notification hook
- `src/server/signals.rs` — Phase 25 SignalRegistry; emit replica-related signals here
- `src/server/http.rs` — /metrics endpoint; extend with replica metrics
- Phase 22's admin-token auth middleware (`require_loopback_or_token` or similar) — reuse

Opcodes reserved in v0 Phase 25-01 (from `src/server/tcp.rs`):
- 0x0D = OP_GET_MULTI (implemented)
- 0x10 = OP_SCAN (reserved, returns STATUS_ERROR "not implemented")
- 0x11 = OP_SUBSCRIBE (reserved with stub; this phase implements)

</code_context>

<specifics>
## Specific Ideas

- **Snapshot filter iterator**: add `SnapshotReader::stream_filtered(scope) -> impl Iterator<Item=Entry>`. Don't load full snapshot into memory.
- **Subscriber registry concurrency**: use `DashMap<u64, ReplicaSession>` (same pattern as Phase 14 per-stream locks). Lock-free hot path on ingest — iterate subscribers, match scope, enqueue.
- **Wire format compatibility**: the three new opcodes reuse the existing 4-byte-length-prefix + opcode-byte framing. No framing change.
- **Plan split suggestion**:
  - **27-01**: `OP_SNAPSHOT_FETCH` + scope validator + snapshot filter iterator
  - **27-02**: `OP_LOG_FETCH` + log filter + client-facing integration test using a real socket
  - **27-03**: `OP_SUBSCRIBE` + SubscriberRegistry + backpressure + metrics + signals + docs
- **Integration testing without a client**: write the tests as Python socket tests using asyncio — no Rust client needed yet. `tests/integration/test_op_snapshot_fetch.py` etc. Phase 28+ will rewrite these against the real client.

</specifics>

<deferred>
## Deferred Ideas

- Sample modes (`pull="sample(...)"`) — v0.2
- Predicate scope (`key_filter="balance > 1000"`) — v0.2
- Read-only replica token class (currently reuse admin token) — v0.2
- Rate-limiting on replica endpoints — future hardening phase
- `OP_SCAN` implementation — v0.2 (stays reserved)
- Backfill source pluggability (`source="s3://..."`) — Phase 33
- Write-back flow — Phase 34

</deferred>

---

*Phase: 27-server-replica-endpoints*
*Sources: `.planning/research/local-replica-design.md`, Phase 25-01 reserved opcodes, Phase 6 event log, Phase 14 DashMap pattern*
