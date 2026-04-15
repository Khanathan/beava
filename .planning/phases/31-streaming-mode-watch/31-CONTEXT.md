# Phase 31: Streaming mode + watch - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Upgrade the catchup connection from Phase 29 to `OP_SUBSCRIBE` (Phase 27), introducing a streaming mode that keeps a live subscription open. Add a `.watch(key)` generator on the Python `Pipeline` plus a `.stop()` downgrade path. Wire `tally sync` CLI to stream events as NDJSON until Ctrl-C.

**In scope:** Rust-side client SUBSCRIBE upgrade on the existing socket, background apply-event thread on the client, client `StateStore` locking for concurrent access, `.watch()` generator + `.stop()` + streaming-mode `.run()` in the Python binding, `tally sync` CLI wiring, E2E integration test pushing events server-side and asserting the watcher observes them.

**Out of scope:** Mode switching / persistent resume (Phase 32, stretch), backfill from external sources (Phase 33, stretch), write-back / `.promote()` (Phase 34, stretch). Reconnect-and-resume from last-applied-seq across `.stop()` + new `.run()` calls — also Phase 32.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Reuse existing primitives: Phase 27's `OP_SUBSCRIBE`, Phase 29's session machinery, Phase 30's PyO3 binding. Add the minimum glue for a live streaming experience.

### A1 — Connection lifecycle: reuse the catchup socket
- After Phase 29's catchup loop reaches the current tail (mode would transition to `Done`), if `mode="streaming"` was requested, instead send `OP_SUBSCRIBE{scope}` on the same socket and transition to a new `Streaming` mode.
- Single socket, single auth exchange, no listener-port changes server-side. The Phase 27 server already accepts `OP_SUBSCRIBE` on any connection post-handshake.

### B1 — `.watch(key, stream)` Python API: generator
- Generator function. Caller: `for change in pipe.watch(key, stream="Transactions"): handle(change)`.
- Each yielded value: `{"seq": int, "event": dict, "value": dict | None}` — `seq` is server seq, `event` is the raw applied event, `value` is the post-apply state from the StateStore for that (stream, key).
- Internally: Rust background apply thread pushes matching events into a `crossbeam_channel` / `std::sync::mpsc`; a PyO3 generator method drains it via blocking `recv`. Ctrl-C interrupts via GIL-release around the `recv`.
- Multiple concurrent `watch()` calls on the same pipeline are allowed and independent.

### C1 — Diff semantics: emit every applied event touching the key
- No value-change detection. If an event lands and matches `(stream, key)` in the watcher's filter, yield it. User filters if they want dedup.
- Consistent with design-doc phrasing ("diff-emits on every apply") and cheaper than structural compare (sketches make "changed" ambiguous anyway).

### D1 — Concurrency model
- In streaming mode, `.run()` starts a background Rust thread that owns the socket and calls `apply_event` on incoming events. Returns immediately (non-blocking) after the bootstrap+catchup phases complete and subscription is established.
- Client `StateStore` gets a `parking_lot::RwLock` wrapper. Background thread takes write lock per event; `.get()` / `.watch()` generators take read lock for lookups.
- This is a meaningful divergence from Phase 28/29's single-threaded design — introduced intentionally here because streaming mode genuinely needs it.
- Historical mode behavior (single-threaded, no locks) is preserved: only the streaming path takes the lock.

### E1 — Stop path
- `.stop()` signals the background thread to close the socket and exit; joins the thread; no new events delivered. Existing `.get()` continues to work against the last applied state.
- No `.pause()` / `.resume()` in v0. No re-subscribe in v0. If the user calls `.run()` again on a stopped pipeline → `RuntimeError` ("pipeline stopped; construct a new Pipeline to resume"). Re-subscribe-with-resume is Phase 32.
- `.stop()` is idempotent. Destructor (`__del__`) calls `.stop()` as a safety net.

### F1 — `tally sync` CLI
- `tally sync --remote HOST --streams S --keys K --token T` → constructs pipeline in streaming mode, runs, prints each applied event as one NDJSON line to stdout.
- Ctrl-C → `.stop()` + clean exit with code 130 (standard SIGINT convention).
- Removes the "not yet implemented" stub left by Phase 28/29.

### G1 — Plan split (2 plans)
- **31-01**: Client-side SUBSCRIBE upgrade — extend `Session` with streaming-mode transition, wire `OP_SUBSCRIBE` after LOG_FETCH tail, spawn background apply thread, introduce `RwLock` on client `StateStore`, implement `.stop()` path. Rust unit + integration tests against the server from Phase 27.
- **31-02**: PyO3 layer — `.watch()` generator + matching channel registration, streaming-mode `.run()` behavior, `.stop()`, `tally sync` CLI wiring, Python E2E pytest that pushes events server-side and asserts the watcher sees them.

### Server-side is untouched
- Phase 27's `SubscriberRegistry` and backpressure logic already handle everything Phase 31 needs. No server changes.

### Error handling
- If the server drops the subscriber (backpressure, per Phase 27), the client's background thread observes the socket EOF, sets an internal `stopped_reason = Dropped { at_seq }`, and subsequent `.watch()` iterations raise `SubscriberDroppedError` (new subclass of `TallyError`). User's choice whether to reconstruct a new pipeline and resume from `at_seq` (manual; Phase 32 automates).

### Logging
- Same conventions as Phase 29: info for mode transitions (`mode: Catchup → Streaming`, `mode: Streaming → Stopped`), warn for unexpected disconnects, error for terminal failures.

</decisions>

<code_context>
## Existing code touchpoints

- Phase 27 `OP_SUBSCRIBE` + server `SubscriberRegistry` — client consumes the same wire protocol.
- Phase 29 `Session` + mode state machine (`Bootstrap / Catchup / Done`) — extend with `Streaming / Stopped` variants.
- Phase 29 bootstrap + catchup + `apply_event` pipeline — reused verbatim; streaming mode layers on top after catchup.
- Phase 28 feature-flagged `client` — no changes to feature gates, just new modules behind `#[cfg(feature="client")]`.
- Phase 28 `src/client/mod.rs` — add `session/streaming.rs` or similar.
- Phase 30 PyO3 `Pipeline` class — extend with `.watch()`, refine `.run()` to handle streaming mode, add `.stop()`.
- Phase 30 `tally query` / `tally inspect` CLI — mirror for `tally sync`.
- Existing `parking_lot::RwLock` (already a dependency in the main crate — confirm in plan).

</code_context>

<specifics>
## Specific technical notes

- **Background thread lifetime**: spawned in `.run()` for streaming mode; owned by a field on `Pipeline`; joined in `.stop()` and `Drop`.
- **Channel between Rust bg thread and Python watchers**: one `SubscriberRegistry`-like structure on the client, keyed by watcher id. Each `.watch()` registers a filter (stream, key) and gets a `crossbeam_channel::Receiver`. Dropping the receiver unregisters (use `Weak` or explicit cleanup).
- **GIL release on blocking recv**: `.watch()` generator's `__next__` uses `Python::allow_threads` around the blocking channel read so Ctrl-C fires in Python.
- **Race at mode transition**: between LOG_FETCH reaching tail and OP_SUBSCRIBE handshake, the server may have accepted more events. Phase 27's server guarantees those are delivered via SUBSCRIBE (starting from the next seq after the handshake). Verify in an integration test by pushing events across the transition boundary.
- **No backpressure on the client**: if Python callers don't drain a `.watch()` generator fast enough, the Rust-side channel fills, then the background thread blocks on send, then the server-side queue fills and eventually drops the client. This is acceptable for v0 — slow consumers should not exist in the demo. Document as a known behavior.

</specifics>

<deferred>
## Deferred

- Persistent `last_applied_seq` + resume on reconnect — Phase 32 (stretch)
- Automatic reconnect-and-resubscribe on drop — Phase 32
- `.pause()` / `.resume()` / mid-stream reconfigure — never unless asked
- Upstream backfill source (s3://, snowflake://) instead of `remote=` — Phase 33 (stretch)
- Write-back / `.promote()` — Phase 34 (stretch)
- Client-side backpressure / drop policy — post-v0 hardening
- Structural-equality diffing — never; user-level concern

</deferred>

---

*Phase: 31-streaming-mode-watch*
*Sources: `.planning/research/local-replica-design.md`, 27-30 CONTEXT chain, user directive "easiest for v0 and demo"*
