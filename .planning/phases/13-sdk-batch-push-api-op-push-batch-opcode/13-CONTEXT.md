# Phase 13: SDK batch push API + OP_PUSH_BATCH opcode - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto (discuss --auto) — decisions locked in ROADMAP.md + .planning/research/SUMMARY.md

<domain>
## Phase Boundary

Expose `app.push_many(stream_cls, events)` on the Python SDK that wraps N events into a single `OP_PUSH_BATCH` (0x0A) wire frame, and add server-side decode → dispatch to Phase 12's `handle_push_batch`. **Zero new hot-path logic** — the server handler IS `handle_push_batch` verbatim. Target **≥300k eps single-client async** on medium pipeline (2× v1.2 baseline).

**In scope:**
- `OP_PUSH_BATCH` (0x0A) opcode — wire format: `[u16 stream_len][stream][u32 batch_id][u32 count][for each: [u32 event_len][event_bytes]]`
- Server decode into `Vec<DecodedEvent>` + dispatch to `handle_push_batch`
- Python SDK `app.push_many(stream_cls, events)` using existing `encode_push_binary_payload`
- Hard cap 16,384 events/frame (H-7 OOM protection)
- Error semantic: `(batch_id, event_index)` via existing per-connection seq drain
- `bench.py --mode async-batch` flag + small/medium/large matrix
- Decode-path micro-benchmark (H-6 — measure before wiring)

**Out of scope:**
- New hot-path logic (Phase 12's handler is the hot path)
- Multi-threading (Phase 14)
- C extension for Python SDK (pitfall M-5 — stay pure Python)
- Changes to `app.push()` single-event path (backward compatible)

</domain>

<decisions>
## Implementation Decisions

### Wire Format (LOCKED in roadmap)
- **D-01:** Opcode `OP_PUSH_BATCH` = 0x0A
- **D-02:** Frame layout: `[u16 stream_name_len][stream_name_bytes][u32 batch_id][u32 count]` followed by `count` × `[u32 event_len][event_bytes]`
- **D-03:** Events encoded via existing `encode_push_binary_payload` — zero new serialization code
- **D-04:** batch_id is a per-connection monotonic u32 assigned by the SDK client

### Server Handler
- **D-05:** Decode into `Vec<DecodedEvent>` pre-sized with `Vec::with_capacity(count.min(16_384))`
- **D-06:** Dispatch to `handle_push_batch` from Phase 12 — zero new hot-path logic
- **D-07:** Hard cap: `count > 16_384` → STATUS_ERROR "batch too large", close connection (H-7)
- **D-08:** Raw-TCP test: send frame with `count = 10_000_000_000` → clean reject, no OOM, no crash

### Error Attribution
- **D-09:** Batch errors surface via `drain_errors_nonblock` with `(batch_id, event_index)` payload
- **D-10:** Reuses Phase 12's per-connection seq ordering — batch events get consecutive seq numbers

### Python SDK
- **D-11:** `app.push_many(stream_cls, events: Iterable[dict])` — main API
- **D-12:** Pure Python only — no C extension (M-5)
- **D-13:** SDK sends one `OP_PUSH_BATCH` frame per `push_many` call (no client-side chunking below 16,384)
- **D-14:** `app.push()` unchanged, still emits `OP_PUSH_ASYNC` (0x07). Both opcodes coexist.

### Benchmarking
- **D-15:** `bench.py --mode async-batch` exercises `push_many`
- **D-16:** Matrix: small / medium / large pipeline sizes (Phase 11-class coverage)
- **D-17:** Target: **≥300k eps single-client async on medium** (2× v1.2 baseline)
- **D-18:** Decode path benchmarked in isolation BEFORE wiring into server (H-6)

### Claude's Discretion
- `DecodedEvent` struct layout — planner decides
- batch_id width (u32 vs u64) — ROADMAP says u32, that's locked
- Whether to add `push_many_sync` — out of scope for Phase 13 unless trivial
- Test file names — executor decides
- Bench harness `--batch-size` flag default — executor decides

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` §"Phase 13" — 9 success criteria, stack additions (none), pitfalls H-6/H-7/M-5
- `.planning/REQUIREMENTS.md` — PERF-04 (client batch API) acceptance criteria

### Project Research (v1.3)
- `.planning/research/SUMMARY.md` — build order rationale (13 is wire-format + SDK, reuses Phase 12 handler)
- `.planning/research/PITFALLS.md` — H-6 (decode bench before wiring), H-7 (batch size OOM cap), M-5 (pure Python)

### Phase 12 Context (dependency)
- `.planning/phases/12-server-side-async-push-coalescing/12-02-SUMMARY.md` — handle_push_batch implementation details
- `.planning/phases/12-server-side-async-push-coalescing/12-VERIFICATION.md` — verified primitives
- `src/server/tcp.rs` — `handle_push_batch`, `ConnAccumulator`, `PendingAsync`, `OP_PUSH_ASYNC` constant

### Code
- `src/server/tcp.rs` — add OP_PUSH_BATCH decode + dispatch site
- `src/server/protocol.rs` — opcode constants
- `python/streamlet/client.py` — add `push_many` method
- `python/streamlet/operators.py` — `encode_push_binary_payload` (reuse for batch)
- `benchmark/tally-throughput/bench.py` — add `--mode async-batch`

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `handle_push_batch` (Phase 12) — the entire server hot path for batch dispatch
- `encode_push_binary_payload` in Python SDK — event serialization
- `PendingAsync` struct — batch items pass through the same accumulator path
- Per-connection seq + drain error queue — Phase 12 wired this

### Established Patterns
- Binary opcode dispatch in `handle_connection` select! loop (tcp.rs)
- Protocol constants in `protocol.rs` or at top of `tcp.rs`
- Python SDK method pattern: `push()` → `_send_frame()` → `_write_*` helpers
- Bench harness: `bench.py` with `--mode` flag routing to different measurement functions

### Integration Points
- `handle_connection` opcode match — add `OP_PUSH_BATCH` arm
- `ConnAccumulator` — batch events bypass the accumulator and go direct to `handle_push_batch`
- Python `Client` class — add `push_many` method alongside `push`
- bench.py — add `async-batch` mode

</code_context>

<specifics>
## Specific Ideas

ROADMAP success criteria (9 items) are the spec. Wire format, hard cap (16,384), opcode (0x0A), batch_id semantics, and throughput target (300k) are all locked.

Key insight from research: "Phase 13's OP_PUSH_BATCH server handler IS Phase 12's handle_push_batch. 13 becomes wire-format + Python SDK only." — this makes Phase 13 a focused protocol + SDK phase with minimal Rust hot-path work.

</specifics>

<deferred>
## Deferred Ideas

- `push_many_sync` (synchronous batch with feature response) — not in Phase 13 scope
- Client-side auto-batching (transparent batching in `push()`) — Phase 14+ if ever
- Batch compression on the wire — not needed at current throughput targets
- Multi-stream batch (events for different streams in one frame) — complexity vs benefit unclear

</deferred>

---

*Phase: 13-sdk-batch-push-api-op-push-batch-opcode*
*Context gathered: 2026-04-12 (auto mode — roadmap + research pre-locked decisions)*
