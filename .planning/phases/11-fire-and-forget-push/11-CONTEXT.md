# Phase 11 — Fire-and-Forget PUSH + Binary Wire Protocol: CONTEXT.md

**Milestone:** v1.2 Performance
**Requirements:** PERF-01 (fire-and-forget ingest), PERF-02 (binary event payload)

---

## Phase Goal

Users can push events via a fire-and-forget `app.push()` that does NOT wait for a feature response, unlocking client-side pipelining and dropping ~9us/push from the round-trip. The existing sync `push()` behavior is preserved as `push_sync()` for users who need inline feature responses. Both paths use a new binary event payload format (replacing `serde_json::from_slice`) to kill the remaining JSON parse cost on the PUSH hot path.

**Target:** single Python client throughput of **~150k events/sec on medium pipelines** (from 17.5k baseline), realized via:
1. Fire-and-forget removes ~9us of round-trip overhead (no response wait, no Python JSON decode, no FeatureResult construction)
2. Client-side pipelining (kernel TCP buffer batches pushes) removes the `sendall → recv → sendall → recv` ping-pong
3. Binary event payload removes `serde_json::from_slice` + `Value::Object` allocation on PUSH (~4-5% server CPU)

---

## Success Criteria

1. `app.push(stream, event)` returns `None` and does not wait for a feature response — under steady-state load, the client can issue pushes faster than 1-per-TCP-round-trip (pipelined).
2. `app.push_sync(stream, event)` returns a `FeatureResult` with the same feature map the current v1.1 `push()` returns.
3. `app.flush()` blocks until all pending async pushes on the connection have been processed by the server, and raises `ProtocolError` if any async push produced an error.
4. Server errors on async pushes (invalid event, unknown stream) surface on the client's NEXT `push()` / `push_sync()` / `flush()` / `get()` call.
5. Binary event payload replaces `serde_json::from_slice` for PUSH — server-side PUSH parsing cost drops measurably in callgrind (old serde_json hotspots gone from top 20).
6. Benchmark: `medium` pipeline single Python client hits **≥ 100k events/sec** sustained (from 17.5k baseline — 5.7x target gate; stretch goal 150k).
7. All 569 existing tests remain green. Integration tests for raw-TCP clients (`test_server.rs`, `test_pipeline.rs`, `test_debug_ui.rs`) are updated for the new binary event payload format.
8. Phase 10.2 latency tracker records BOTH sync and async pushes under their respective command kinds (PUSH for sync, PUSH_ASYNC if we track separately, or folded into PUSH histogram — decided during planning).
9. No regression on the `p99 PUSH < 100us` budget from Phase 6 (the sync PUSH arm).

---

## Decisions

### Decision 1 — Two first-class APIs: `push()` async, `push_sync()` sync

**Choice:** `app.push()` is fire-and-forget (default). `app.push_sync()` is synchronous with feature response (explicit opt-in). `app.flush()` is a barrier.

**Rationale:**
- Matches the user's stated direction ("push should be push only no feature fetch")
- Keeps sync behavior available for users who genuinely need inline features (debugging, sync assertions, tight feedback loops)
- Standard pattern in production messaging clients: Kafka producer defaults to `acks=1` batched, users explicitly wait with `.get()`. NATS does the same. Redis pipelines follow the same contract.

**Rejected alternatives:**
- Only async `push()` (no sync variant) — loses the backward-compat door and forces users into a flow that's awkward for sync-style code.
- Only sync `push()` with a background-batch mode — hides semantics; users can't tell which mode they're in.
- Callback-based async — forces users into event-driven code even when they just want a for-loop.

### Decision 2 — OP_PUSH remains sync; OP_PUSH_ASYNC is new (0x07); OP_FLUSH is new (0x08)

**Choice:** Do not break `OP_PUSH = 0x01` semantics. Add `OP_PUSH_ASYNC = 0x07` and `OP_FLUSH = 0x08`. Python SDK's `push()` uses OP_PUSH_ASYNC; `push_sync()` uses OP_PUSH.

**Rationale:**
- Preserves backward compat at the wire level for anything that might poke TCP directly (future debugging tools, third-party-language clients if they materialize)
- Zero risk to existing raw-TCP integration tests
- Opcodes 0x07 and 0x08 are currently unused (0x06 is MGET)

**Rejected alternatives:**
- Repurpose OP_PUSH as async — breaks all existing tests that call raw TCP with OP_PUSH. Too disruptive.
- Use a subcommand byte instead of a new opcode — complicates the parser for no benefit.

### Decision 3 — Binary event payload for BOTH OP_PUSH and OP_PUSH_ASYNC (this IS a wire break)

**Choice:** Replace the current PUSH event payload format:
```
BEFORE: [u16 stream_name_len][stream_name][JSON event object bytes]
AFTER:  [u16 stream_name_len][stream_name][u16 field_count][field_1]...[field_N]

  where each field = [u16 key_len][key_bytes][type_tag: u8][value]
  and value depends on type_tag:
    0x00 = null/missing (zero bytes)
    0x01 = bool (1 byte: 0 or 1)
    0x02 = i64 (8 bytes, big-endian)
    0x03 = f64 (8 bytes, big-endian IEEE 754)
    0x04 = string ([u16 len][utf-8 bytes])
```

**Rationale:**
- Hits the 4-5% CPU cost of `serde_json::from_slice` on every PUSH
- Eliminates intermediate `serde_json::Value::Object` allocation (BTreeMap<String, Value> per push) — saves significant allocator traffic
- Python side: `json.dumps(event).encode()` (~5us) → direct struct-pack encoding (~1-2us)
- Supports all FeatureValue variants that real events use (int, float, string, bool, missing)
- Applies to both sync AND async opcodes because parser is shared

**Scope boundaries — what stays JSON:**
- **Response feature map** (sync `push_sync()` return value) stays JSON. Optimizing this is a separate future phase and the sync path isn't hot.
- **GET response** (`app.get(key)` return value) stays JSON. Same reasoning.
- **SET / MSET / REGISTER payloads** stay JSON. None are hot path.
- Only the **PUSH event payload** (request side) becomes binary.

**Rejected alternatives:**
- Binary everything (requests + responses, all commands) — too large a scope for one phase, obscures the win.
- Protobuf/MessagePack — adds a codegen dependency. Hand-rolled binary is ~100 LOC and matches the existing wire style (length-prefixed strings in protocol.rs:33-112).
- Keep JSON — leaves the 4-5% CPU on the table when we're already modifying the PUSH parser anyway.

### Decision 4 — Error surfacing via non-blocking select drain

**Choice:** Before each `push()`, the Python SDK checks if the server has sent any bytes (error frames) using `select.select([sock], [], [], 0)`. If bytes are available, read one frame; if it's an error, store the exception and raise on the same call. Silent success = nothing to read.

**Rationale:**
- No background thread (simpler, no locking between main/background thread for sync call responses)
- ~1us overhead per push (one syscall with timeout 0)
- Errors are surfaced at most "one push late" compared to when they occurred — acceptable for fire-and-forget semantics
- `flush()` reads frames until it gets the OP_FLUSH ACK, surfacing any earlier error frames along the way
- Matches the "errors on next call" model used by Redis pipelines

**Rejected alternatives:**
- Background reader thread — adds threading complexity, requires locking between sync and background reads on the same socket, and the latency benefit is negligible since errors should be rare.
- Callback API — forces async-style code on users for rare events. Not Pythonic.
- No error surfacing at all (pure fire-and-forget like UDP) — loses basic error detection, bad default.

### Decision 5 — OP_FLUSH is a no-op barrier

**Choice:** `OP_FLUSH` on the server is trivially handled — server reads the opcode and immediately sends back an empty `STATUS_OK` frame. No special queueing logic.

**Rationale:**
- TCP preserves ordering within a single connection. By the time the server reads an `OP_FLUSH` frame, it has already processed all prior `OP_PUSH_ASYNC` frames on the same socket (sequential dispatch in `handle_connection`).
- The flush "wait" semantics come for free from the existing sequential read loop.
- Any errors from prior pushes are already in-flight as error frames on the socket — the client's `flush()` reads frames until it gets the OP_FLUSH ACK, picking up errors along the way.

**Rejected alternatives:**
- Make flush wait for a drain queue on the server — adds complexity for zero benefit since TCP already orders frames.
- Make flush return a sequence number / counter — useful for advanced diagnostics but not needed for basic sync semantics.

### Decision 6 — Phase 10.2 latency tracker folds async + sync PUSH into the same histogram

**Choice:** Both `OP_PUSH` and `OP_PUSH_ASYNC` record into `CommandKind::Push` in the latency tracker. Separate histograms per sub-mode would double the metric surface without meaningful benefit.

**Rationale:**
- Users want to know "how fast are pushes" — mode-specific split is noise for most debugging cases
- If a per-mode split becomes important later, it's a small additive change
- Phase 10.2 slow-query capture still works correctly since the wire-to-wire timer wraps both branches

**Rejected alternatives:**
- Separate `CommandKind::PushSync` + `CommandKind::PushAsync` — more surface, more code to maintain, slim benefit.

---

## Constraints Carried Forward

These are non-negotiable, inherited from earlier phases:

1. **No `.await` across AppState mutex lock.** Both `handle_push_sync` and `handle_push_async` follow the existing lock-process-unlock pattern.
2. **p99 PUSH < 100us budget.** Sync PUSH path must NOT regress. Async path is expected to be faster (no response serialize), but the budget applies identically.
3. **No XSS sinks in any frontend code.** (This phase has no frontend changes but the test stays green.)
4. **All 569 existing tests pass.** Integration tests that craft raw TCP PUSH frames must be updated to the new binary payload format — that's the one acceptable test modification.
5. **No regression on cascade/fan-out correctness.** `push_with_cascade` + fan-out logic is unchanged; it gets called from both sync and async branches.
6. **Event log still written.** Both sync and async PUSH append to the event log identically.
7. **Phase 10.2 latency histograms still recorded** on both sync and async paths.
8. **Phase 9 dirty marking still happens** on both paths (for incremental snapshots).
9. **Keep single-threaded tokio runtime** — this phase does NOT touch the runtime flavor. Multi-threaded + DashMap is a future phase.
10. **push_latency_seconds scalar metric** stays (for anything that depends on it).

---

## Implementation Hooks

### Server (Rust)

**`src/server/protocol.rs`:**
- Add `OP_PUSH_ASYNC: u8 = 0x07`
- Add `OP_FLUSH: u8 = 0x08`
- Add `PushAsync { stream_name, payload }` and `Flush` variants to `Command` enum (or reuse `Push` variant with a flag — planner decides)
- Write new binary event payload decoder: `fn decode_event_binary(&[u8]) -> Result<serde_json::Value, TallyError>` returning a `Value::Object` for downstream code compatibility
- Update `parse_command` to dispatch OP_PUSH and OP_PUSH_ASYNC through the new binary decoder
- Keep `read_json_payload` for SET / REGISTER (unchanged)

**`src/server/tcp.rs`:**
- `handle_connection` dispatch: add match arms for `PushAsync` and `Flush`. PushAsync goes through `handle_sync_command` (shared logic) but the caller skips response write when the result is empty and mode is async.
- `handle_sync_command` PUSH arm: extract the current push logic into `handle_push_core(stream_name, payload, state) -> Result<FeatureMap, TallyError>`. Sync PUSH wraps it with `feature_map_to_json`; async PUSH discards the returned feature map and returns `Ok(vec![])`.
- `handle_connection` response write: if the command was PushAsync AND the response is empty (no error), skip `writer.write_all` entirely. This is where the "silent success" savings come from. On error, send the error frame normally so the client's drain picks it up.
- `Flush` dispatch: return `Ok(vec![])` immediately — no special state needed.
- Phase 10.2 latency recording: wrap both sync and async PUSH paths under `CommandKind::Push` (unchanged — both branches call record_push).

**`src/engine/pipeline.rs`:**
- No changes expected. `push_with_cascade` takes `payload: &serde_json::Value` which is what the binary decoder produces.

### Python SDK

**`python/tally/_protocol.py`:**
- Add `OP_PUSH_ASYNC = 0x07`, `OP_FLUSH = 0x08`
- New `encode_push_binary(stream_name: str, event: dict) -> bytes` — direct `bytearray` construction, type-dispatched field encoding
- Keep `encode_push` as JSON for test compat (delete later) OR replace immediately — planner decides based on test impact

**`python/tally/_client.py`:**
- Add `drain_errors_nonblock()` method: `select.select([sock], [], [], 0)` → read one frame if ready → raise if STATUS_ERROR → store error if pending for next call
- Add `send_frame_no_recv(opcode, payload)` for async push (pure send, no read)

**`python/tally/_app.py`:**
- `push(stream_class, event)` — now fire-and-forget:
  ```python
  def push(self, stream_class, event):
      self._client.drain_errors_nonblock()
      payload = encode_push_binary(stream_class._tally_stream_name, event)
      self._client.send_frame_no_recv(OP_PUSH_ASYNC, payload)
  ```
- `push_sync(stream_class, event)` — old `push` behavior:
  ```python
  def push_sync(self, stream_class, event):
      self._client.drain_errors_nonblock()
      payload = encode_push_binary(stream_class._tally_stream_name, event)
      resp = self._send(OP_PUSH, payload)
      return FeatureResult(json.loads(resp) if resp else {})
  ```
- `flush()` — new:
  ```python
  def flush(self):
      self._client.drain_errors_nonblock()
      self._send(OP_FLUSH, b"")
  ```

### Tests

**New tests (must exist before claiming done):**
- `tests/test_server.rs`: raw-TCP test that sends OP_PUSH_ASYNC + reads nothing + then OP_GET returns the updated features
- `tests/test_server.rs`: raw-TCP test that sends malformed OP_PUSH_ASYNC and receives an error frame
- `tests/test_server.rs`: raw-TCP test for OP_FLUSH round-trip
- `python/tests/test_app.py`: `test_push_returns_none`, `test_push_sync_returns_feature_result`, `test_flush_blocks_until_ack`, `test_error_on_next_push_after_bad_async`
- `benches/throughput.rs` OR `benchmark/tally-throughput/bench.py` updated for the new API (add `--mode async` flag)

**Existing tests to update:**
- Any raw-TCP test that crafts a PUSH frame with JSON payload must switch to the binary encoder. Expected affected files: `tests/test_server.rs`, `tests/test_pipeline.rs`, `tests/test_debug_ui.rs`.

### Benchmark gate

Before marking the phase complete, run:
```bash
# Start tally
cd /data/home/tally && ./target/release/tally &

# Measure single-client throughput in async mode
cd benchmark/tally-throughput
python3 bench.py --events 100000 --clients 1 --pipeline medium --mode async
```

Gate: **≥ 100k events/sec** (5.7x improvement over the v1.1 17.5k baseline). Stretch: 150k.

---

## Out of Scope (future phases)

Explicitly NOT in Phase 11:

- **Multi-threaded tokio runtime** — stays `current_thread`. Concurrent-client scaling is a separate phase (the SemVer-major one).
- **DashMap / per-entity locks** — state store unchanged.
- **HLL cache** — Phase 12 (independent, can land before or after).
- **Binary feature map response** — `push_sync()` and `get()` still return JSON. Next phase or later.
- **Rust SDK** — way later.
- **Pipelining with inline ack drains / batch ACK** — Phase 11's drain-on-next-call model is sufficient for the target throughput. Background thread model can come later if needed.
- **REGISTER binary format** — REGISTER is one-time startup, stays JSON.

---

## Risk Register

| Risk | Severity | Mitigation |
|---|---|---|
| Raw-TCP integration tests break in bulk | Medium | Do a test-file audit before coding. Budget 1 day to update raw-frame crafters. |
| Binary event decoder has off-by-one on field type tags | Medium | Unit tests for every type tag (null, bool, i64, f64, string) + property tests for roundtrip. |
| Error drain `select()` masks real pipeline errors under heavy load | Low | Document the "errors surface on next call" contract. Provide `flush()` as the explicit sync point. |
| Phase 10.2 latency histograms double-count if both sync and async record separately | Low | Decision 6 folds them into one kind. Single code path. |
| Async mode silently drops events if server's socket buffer fills and client doesn't flush | Low | `sendall` blocks on kernel back-pressure — this is TCP-level, not application-level. Document. |
| Benchmark gate not met (throughput stays below 100k) | High | Profile mid-phase. If Python SDK encode is still the bottleneck, fall back to ~50k gate and mark a follow-up "Rust FFI encode" phase. |

---

## Reference Documents

- `benchmark/tally-throughput/RESULTS.md` — v1.1 baseline measurements
- `benchmark/tally-throughput/PROFILE.md` — callgrind profile showing JSON cost
- `benchmark/tally-throughput/FINDINGS-VS-REALITY.md` — cross-check against earlier spike
- `benchmark/tally-throughput/PATH-TO-100K-1M.md` — lever analysis showing Phase 11 as the primary 100k path
- `.planning/research/FINDINGS-GAP-ANALYSIS.md` — gap analysis against FINDINGS
- `benchmark/FINDINGS.md` — original benchmark spike
- `.planning/phases/10.2-latency-debugger/10.2-CONTEXT.md` — the Phase 10.2 design this phase must preserve

---

## Open Questions for Research Phase

1. **Binary decoder target type:** return `serde_json::Value::Object` (simplest, preserves downstream code) or introduce a new `EventPayload` type throughout the push path (bigger refactor, tighter performance)? Research phase should answer based on how deeply the current code is coupled to `serde_json::Value`.

2. **Phase 10.2 slow-query key_preview under binary format:** the current slow-query capture extracts the entity key from the event for display. Under binary format, we need to find the key_field and decode it during the record-slow path. Is this free? Check `handle_sync_command` PUSH arm lines ~192-370.

3. **Python `struct.pack_into` vs `bytearray` append performance:** which Python idiom is actually fastest for the binary encoder hot path? Micro-benchmark before writing the encoder.

4. **Flush semantics under server crash mid-push:** if the server crashes after processing 50 of 100 pending async pushes and the client calls `flush()`, what happens? Likely: client gets `ConnectionError` on flush. Document this.

5. **Event log append timing vs Phase 10.2 latency recording:** should the async branch wait for event log `append` before "acking" to the client? Currently event log is a buffered write → fsync every second. Need to confirm this is preserved on the async path.
