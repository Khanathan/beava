# Phase 35: OP_LOG_FETCH{from_ts, scope} - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo — persist CDC and full replay is enough")

<domain>
## Phase Boundary

Add a third replica opcode to the server: `OP_LOG_FETCH{from_ts_millis, scope}` = 0x13. Streams events from the per-stream log files where `entry.timestamp_ms >= from_ts_millis` AND `entity_matches_scope(entry) == true`, in whatever timestamp-order the per-stream append provides. Each emitted frame carries `(timestamp_ms, payload_bytes)`.

This is the primitive that enables Option M — data scientists cloning the CDC stream (raw events from prod) to their laptop and replaying through their own pipelines.

**Out of scope:**
- Global seq-monotonic ordering across streams — timestamp-order within a stream is what you get; scientist's pipeline uses watermarks (existing v0 engine) to tolerate cross-stream skew.
- Exact-once boundary semantics — at-least-once on duplicate timestamps accepted.
- Snapshot seeding (removed by user directive — just replay CDC).
- `to_ts` upper bound — always streams until current tail, then caller closes or switches to OP_SUBSCRIBE.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Opcode and wire format
- `OP_LOG_FETCH = 0x13` (new).
- Payload: `[u16 token_len][token_bytes][u64 from_ts_millis][scope_bytes]`. Same shape as SNAPSHOT_FETCH/SUBSCRIBE, with `from_ts_millis` inserted before the Scope bytes.
- Response: a stream of frames tagged `REPLICA_FRAME_TAG_EVENT` (0x03), each body = `[u64 timestamp_ms][u32 payload_len][payload_bytes]` followed by a terminal `REPLICA_FRAME_TAG_END` frame (new tag, 0x04, body empty) to signal "caught up to tail".

### Timestamp cursor semantics
- `from_ts_millis` is inclusive. Event with exactly that timestamp → emit.
- Clients that resume after an interruption resend the last-seen `timestamp_ms`; they accept duplicates at that boundary. Documented in the opcode doc-comment.

### Per-stream ordering only
- Each stream's log file is appended in ingest order → that's the order we emit from it.
- Across streams: no merging. We iterate streams in whatever order the scope declares, emit entries from each. Clients doing cross-stream pipelines use watermarks (already in v0).
- No k-way merge. No seq. No global ordering claim.

### Auth
- Admin token required, same pattern as 27-01/27-02. Token in payload prefix.

### Scope validation
- Reuse `validate_scope` from Phase 27. Same rules: non-empty streams, unknown stream reject, mutex keys/key_prefix, `pull="all"` only, 10k key cap, non-empty key_prefix if set, non-empty entries inside keys vec.

### Filter path
- Reuse Phase 6's per-stream log file reader. Iterate entries → match `entity_matches_scope` (shared with Phase 27) → gate `entry.timestamp >= from_ts_millis`.
- Load entries lazily (don't buffer the whole stream in memory). Use whatever iterator API the event log already has.

### Admin surface
- New metric: `tally_replica_log_entries_sent_total{stream}` counter.
- Signal: auth failure → category=safety, severity=error (same as 27-01/27-02).

### Plan split
- One plan (35-01) is enough. Three tasks:
  1. Protocol additions (opcode, frame tag, Command::LogFetch, parse_command arm, wire codec + unit tests).
  2. Handler in `src/server/replica.rs` (or inline — whichever matches 27-01's pattern) + per-stream log iteration + timestamp gate.
  3. Rust integration test + Python asyncio test.

</decisions>

<code_context>
- `src/server/protocol.rs` — add opcode const + Command variant + parse_command arm.
- `src/server/replica.rs` — existing `entity_matches_scope` reused; new `stream_log_entries` helper iterating the per-stream log.
- Phase 6 event log module — read API for per-stream entries. Confirm in the plan which function to call.
- `src/server/tcp.rs` — dispatch on Command::LogFetch.
- Admin-token auth: same pattern as 27-01/27-02.

</code_context>

<specifics>
- If per-stream log files are large, reading them end-to-end per request is acceptable for v0 — scientist isn't calling LOG_FETCH in a tight loop. If prod scale demands it later, add a timestamp index. Not v0.
- Boundary duplicate: document in opcode doc-comment that clients resuming at timestamp T may see the same event twice. Pipeline code at the scientist's end deals with idempotency.

</specifics>

<deferred>
- Global seq counter — v0.2.
- `to_ts` upper bound — add when scientists ask for time-windowed historical pulls.
- Streaming compression — not needed at this scale.
- S3-backed historical events — Phase 33 stretch.

</deferred>

---

*Phase: 35-op-log-fetch*
*Source: user directive 2026-04-15 "Option M + persist CDC + full replay is enough for demo"*
