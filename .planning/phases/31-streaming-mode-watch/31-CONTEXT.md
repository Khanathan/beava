# Phase 31: Streaming mode + watch - Context

**Gathered:** 2026-04-15
**Revised:** 2026-04-15 (Option K — subscribe-first buffered-replay dance; no Catchup mode)
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Add streaming-mode support on the client: open OP_SUBSCRIBE first, buffer events, fetch the snapshot via OP_SNAPSHOT_FETCH, apply snapshot then drop-timestamp-gated-replay of buffered events, then continue applying live events. Expose `.watch(key, stream)` as a generator on the PyO3 Pipeline. Wire `tally sync` CLI.

**In scope:** Rust client-side two-socket subscribe-first dance (or single-socket with pipelined ops — whichever is simpler for the TCP design), background apply thread, `parking_lot::RwLock` on client `StateStore`, `.stop()` path with thread join, Python `.watch()` generator, `tally sync` NDJSON CLI, E2E integration test pushing events mid-stream.

**Out of scope:** catchup via LOG_FETCH (does not exist in v0), persistent resume across runs (Phase 32, stretch), upstream backfill sources (Phase 33), write-back (Phase 34), client-side backpressure (slow consumers get dropped by server per Phase 27).

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Use Phase 27's SUBSCRIBE + SNAPSHOT_FETCH. Don't invent a third opcode. Close the snapshot/subscribe gap client-side with timestamps.

### A — Connection model: two sockets, simpler code path
**Two sockets.** Open socket #1, send SUBSCRIBE, start buffering events. Open socket #2, send SNAPSHOT_FETCH, receive filtered entity state + `snapshot_taken_at`. Apply snapshot to StateStore. Drain the buffer: drop events with `timestamp ≤ snapshot_taken_at`, apply the rest. Keep socket #1 open for live streaming; close socket #2.

Rationale: reusing one socket for both opcodes requires the server to handle back-to-back ops on one connection *while also subscribing*, which is state-machine gymnastics. Two sockets cost one extra FD for a short duration and make client logic trivially linear.

### B1 — `.watch(key, stream)` API: generator (unchanged)
Same as previous CONTEXT. Generator yielding `{timestamp, event, value}` dicts. GIL released around blocking `recv`.

**Removed field**: no more `seq` in yielded dict (doesn't exist). `timestamp` is the event's SystemTime as provided in the PUSH.

### C1 — Diff semantics (unchanged)
Emit every applied event touching the key. No value-change detection.

### D1 — Concurrency (unchanged)
Background Rust thread owns socket #1 + drives apply loop. `parking_lot::RwLock` on client `StateStore`. Historical mode (Phase 28-04, see below) is single-threaded and doesn't take the lock; streaming mode does.

### E1 — Stop path (unchanged)
`.stop()` closes socket, joins background thread, drops subscriptions. No resume in v0; re-run raises `RuntimeError`.

### F1 — `tally sync` CLI (unchanged)
NDJSON lines to stdout; Ctrl-C → `.stop()` → exit 130.

### G — Plan split (2 plans, unchanged count but 31-01 rewritten)
- **31-01**: Rust client streaming-mode session. Two-socket subscribe-first dance; background apply thread; `RwLock<StateStore>`; `.stop()` path with thread join; server-drop detection (socket EOF → `StopReason::ServerDropped { at_ts }`). Integration tests against Phase 27 server pushing events across the subscribe→snapshot→replay transition.
- **31-02**: PyO3 layer. `.watch()` generator backed by a `WatcherRegistry` (crossbeam channels keyed by `(stream, key)`). Streaming-mode `.run()` returns after dance completes + live stream is established. `.stop()` + `__del__`. `tally sync` CLI. Python E2E pytest covering push-during-stream, watcher observes, backpressure drop, dropped-subscriber error propagation.

### Replacement of previous mode state machine
Previous CONTEXT had `Bootstrap → Catchup → Streaming`. Under Option K the modes reduce to:
- **Historical** (Phase 28-04, not Phase 31): `Subscribed=No, Fetched=Yes, Done`.
- **Streaming** (Phase 31): `Subscribing → BufferedReplay → Live → Stopped`.

### Error types (extends Phase 30 TallyError hierarchy)
- `SubscriberDroppedError(at_timestamp)` — server dropped us (backpressure or disconnect). Stream is over; user reconstructs Pipeline to recover.
- `TransitionError` — snapshot_taken_at is older than the oldest buffered event (shouldn't happen; indicates clock skew or server bug).

</decisions>

<code_context>
## Existing code touchpoints

- Phase 27 `OP_SUBSCRIBE` + `OP_SNAPSHOT_FETCH` — client consumes both.
- Phase 27 `SubscriberRegistry` server-side backpressure — client handles socket EOF + maps to `SubscriberDroppedError`.
- Phase 28-04 (new, see 28 CONTEXT) — historical-mode session. Phase 31 re-uses parts: frame codec, scope wire format, snapshot deserializer.
- Phase 28 client feature flag — all new modules behind `#[cfg(feature="client")]`.
- Phase 30 PyO3 Pipeline class — extend with `.watch()`, streaming-mode `.run()`, `.stop()`.
- Existing `parking_lot::RwLock` — confirm in Cargo.toml (it's a common dep; add if missing).

</code_context>

<specifics>
## Specific technical notes

- **Buffer size**: client-side subscribe buffer is unbounded during the dance (typically seconds of events, tens of MB at worst). OK for v0 demo. If abuse becomes a concern post-demo, add a client-side cap with backpressure behavior to server (disconnect).
- **Two-socket auth**: both sockets auth via admin token. Reuse the token string across both connections.
- **Race: snapshot arrives before subscribe is acked**: possible. Solve by making the subscribe-socket receive the first "ack/ready" frame from the server synchronously before socket #2 is opened. If Phase 27's SUBSCRIBE handshake already ends with "you're registered" before events start flowing, use that signal. If not, add a tiny 50ms buffer window server-side at subscribe registration (document and keep minimal).
- **Timestamp monotonicity**: events within a stream have monotonic-ish timestamps but across streams they aren't guaranteed. This is OK because the gap-close compares `event.timestamp vs snapshot_taken_at`, both SystemTime, both in the same local clock on the server.
- **Ctrl-C in `.watch()`**: GIL release around blocking channel `recv`. Python signal handler fires on GIL re-entry. Standard PyO3 pattern.

</specifics>

<deferred>
## Deferred

- Single-socket subscribe+fetch optimization — v0.2 if latency matters
- Persistent resume across runs — Phase 32 (stretch)
- Auto-reconnect on `SubscriberDroppedError` — Phase 32
- Client-side backpressure/buffer cap — post-v0 hardening
- Global seq in watch yield dict — requires event-log seq (v0.2)

</deferred>

---

*Phase: 31-streaming-mode-watch*
*Revision: Option K (2026-04-15) — subscribe-first buffered-replay dance*
*Source directive: user "historical + latest streaming — two of that is enough" 2026-04-15*
