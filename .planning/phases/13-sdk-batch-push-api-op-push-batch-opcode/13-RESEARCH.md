# Phase 13: SDK batch push API + OP_PUSH_BATCH opcode - Research

**Researched:** 2026-04-11
**Domain:** Wire protocol extension + Python SDK batch API + server decode dispatch
**Confidence:** HIGH

## Summary

Phase 13 is a focused protocol + SDK phase. The server hot-path work is minimal: decode a new `OP_PUSH_BATCH` (0x0A) frame into `Vec<PendingAsync>` and dispatch to Phase 12's existing `handle_push_batch`. The bulk of the work is: (1) defining and implementing the wire format, (2) adding `push_many` to the Python SDK using the existing `encode_push_binary` encoder, and (3) adding `--mode async-batch` to the bench harness.

All hot-path logic already exists in `handle_push_batch` (shipped in Phase 12, commit `33932af`). Phase 13 adds zero new hot-path code. The `PendingAsync` struct, `ConnAccumulator`, and the `handle_push_batch` function are all public and ready to be reused. The key technical question -- whether batch events should bypass the `ConnAccumulator` -- is answered: yes, `OP_PUSH_BATCH` events are pre-batched by the client and should be dispatched directly to `handle_push_batch`, bypassing the accumulator entirely (the accumulator exists to coalesce single-event `OP_PUSH_ASYNC` frames; a batch frame is already coalesced).

**Primary recommendation:** Treat as a 3-wave phase: (1) Rust decode + dispatch with isolation micro-bench (H-6), (2) Python SDK `push_many` + unit tests, (3) bench harness `--mode async-batch` + throughput gate.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Opcode `OP_PUSH_BATCH` = 0x0A
- **D-02:** Frame layout: `[u16 stream_name_len][stream_name_bytes][u32 batch_id][u32 count]` followed by `count` x `[u32 event_len][event_bytes]`
- **D-03:** Events encoded via existing `encode_push_binary_payload` -- zero new serialization code
- **D-04:** batch_id is a per-connection monotonic u32 assigned by the SDK client
- **D-05:** Decode into `Vec<DecodedEvent>` pre-sized with `Vec::with_capacity(count.min(16_384))`
- **D-06:** Dispatch to `handle_push_batch` from Phase 12 -- zero new hot-path logic
- **D-07:** Hard cap: `count > 16_384` -> STATUS_ERROR "batch too large", close connection (H-7)
- **D-08:** Raw-TCP test: send frame with `count = 10_000_000_000` -> clean reject, no OOM, no crash
- **D-09:** Batch errors surface via `drain_errors_nonblock` with `(batch_id, event_index)` payload
- **D-10:** Reuses Phase 12's per-connection seq ordering -- batch events get consecutive seq numbers
- **D-11:** `app.push_many(stream_cls, events: Iterable[dict])` -- main API
- **D-12:** Pure Python only -- no C extension (M-5)
- **D-13:** SDK sends one `OP_PUSH_BATCH` frame per `push_many` call (no client-side chunking below 16,384)
- **D-14:** `app.push()` unchanged, still emits `OP_PUSH_ASYNC` (0x07). Both opcodes coexist.
- **D-15:** `bench.py --mode async-batch` exercises `push_many`
- **D-16:** Matrix: small / medium / large pipeline sizes (Phase 11-class coverage)
- **D-17:** Target: >=300k eps single-client async on medium (2x v1.2 baseline)
- **D-18:** Decode path benchmarked in isolation BEFORE wiring into server (H-6)

### Claude's Discretion
- `DecodedEvent` struct layout -- planner decides
- batch_id width (u32 vs u64) -- ROADMAP says u32, that's locked
- Whether to add `push_many_sync` -- out of scope for Phase 13 unless trivial
- Test file names -- executor decides
- Bench harness `--batch-size` flag default -- executor decides

### Deferred Ideas (OUT OF SCOPE)
- `push_many_sync` (synchronous batch with feature response)
- Client-side auto-batching (transparent batching in `push()`)
- Batch compression on the wire
- Multi-stream batch (events for different streams in one frame)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-04 | Client-side batch push API -- `app.push_many(stream, events)` wraps N events into one `OP_PUSH_BATCH` (0x0A) wire frame, reducing Python per-event loop overhead. Single-client async throughput via `push_many` >= 300k eps on medium pipeline. Error attribution surfaces `(batch_id, event_index)` via existing drain semantic; `app.push()` single-event API continues to work unchanged | Wire format design (Standard Stack), Python SDK patterns (Architecture Patterns), `handle_push_batch` reuse (Code Examples), H-6/H-7/M-5 mitigations (Pitfalls) |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| (no new Rust crates) | -- | -- | Phase 13 adds zero new dependencies per ROADMAP |
| `struct` (Python stdlib) | -- | Wire frame encoding | Already used by `_protocol.py` for all binary encoding |
| `bytearray` (Python stdlib) | -- | Mutable buffer for batch frame assembly | Already the pattern in `encode_push_binary` |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `serde_json` | (existing) | JSON decode of batch events on server | Already used by `decode_event_binary` for OP_PUSH/OP_PUSH_ASYNC |
| `tokio` | (existing) | Async I/O for TCP read loop | Already wired in `handle_connection` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Hand-rolled binary decode (Rust) | `nom` parser combinator | Overkill for ~30 lines of decode; nom adds a dependency for no benefit |
| `encode_push_binary` per-event (Python) | Custom batch-level binary encoder | D-03 locks reuse of existing encoder; zero new serialization code |
| C extension for Python batch encoding | Pure Python `struct.pack` + `bytearray` | M-5 explicitly prohibits C extensions; pure Python is sufficient for the 300k target |

**Installation:**
```bash
# No new packages needed -- zero new crates (Rust), zero new pip deps (Python)
```

## Architecture Patterns

### Integration Points in Existing Code

#### 1. Opcode Constants (`src/server/protocol.rs` lines 9-16)
Opcode constants are defined as `pub const` at the top of `protocol.rs`. Current highest: `OP_FLUSH = 0x08`. Phase 13 adds `OP_PUSH_BATCH = 0x0A` (skipping 0x09, consistent with D-01). [VERIFIED: src/server/protocol.rs lines 9-16]

#### 2. Command Enum (`src/server/protocol.rs` lines 31-47)
The `Command` enum has variants for each opcode. Phase 13 needs a new variant, either:
- **Option A:** `PushBatch { stream_name: String, batch_id: u32, events: Vec<PendingAsync> }` -- decode directly into `PendingAsync` structs that `handle_push_batch` already expects.
- **Option B:** `PushBatch { stream_name: String, batch_id: u32, events: Vec<(serde_json::Value, Vec<u8>)> }` -- decode into (payload, raw_payload) pairs, convert to `PendingAsync` at dispatch site.

**Recommendation:** Option A. The `PendingAsync` struct is already `pub` (Phase 12, `tcp.rs` line 711) with a public `new()` constructor. Decoding directly into `PendingAsync` avoids an intermediate allocation and conversion step. The `seq` field can be assigned at decode time from the connection's accumulator sequence space. [VERIFIED: PendingAsync is pub with pub new() at tcp.rs:711-731]

#### 3. `parse_command` Dispatch (`src/server/protocol.rs` lines 235-309)
The `parse_command` function matches on opcode. A new `OP_PUSH_BATCH` arm needs to:
1. Read `stream_name` via existing `read_string` (u16-prefixed)
2. Read `batch_id` (u32 BE)
3. Read `count` (u32 BE)
4. Validate `count <= 16_384` (H-7)
5. Pre-allocate `Vec::with_capacity(count.min(16_384))` (D-05)
6. For each event: read `event_len` (u32 BE), slice `event_len` bytes, call `decode_event_binary` to get `serde_json::Value`, capture raw bytes

**Important detail:** The existing `OP_PUSH_ASYNC` decode (protocol.rs:247-251) calls `read_string` for stream_name, then captures `raw_payload = buf.to_vec()` (full remaining bytes), then `decode_event_binary`. For `OP_PUSH_BATCH`, each event's raw_payload is the per-event binary slice (not the full remaining buffer). The `[u32 event_len]` prefix before each event provides the slice boundary. [VERIFIED: protocol.rs:247-251]

#### 4. `handle_connection` Dispatch (`src/server/tcp.rs` lines 287-300)
The connection loop dispatches on `Command` variant. Currently:
- `Command::PushAsync` -> accumulate into `ConnAccumulator`
- All other commands -> force-flush accumulator, then dispatch sync

For `OP_PUSH_BATCH`: the batch is **already coalesced by the client**. It should NOT go through the `ConnAccumulator`. Instead:
1. Force-flush any pending accumulator entries (same as sync commands -- H-2 consistency)
2. Convert batch events to `Vec<PendingAsync>` (assign consecutive seq numbers from accumulator)
3. Call `handle_push_batch` directly
4. Collect errors into `pending_drain`
5. Continue loop (no response frame -- fire-and-forget like `OP_PUSH_ASYNC`)

**Key insight:** The accumulator's `next_seq` counter must be advanced by `count` events even though the batch bypasses the accumulator buffer. This preserves the monotonic per-connection ordering (D-10, C-2). Call `accumulator.next_seq_peek()` to get the base seq, then manually increment. [VERIFIED: ConnAccumulator.next_seq_peek() at tcp.rs:772-774]

**Problem:** `ConnAccumulator` currently only exposes `next_seq_peek()` (read-only) and `push()` (adds to buffer). Phase 13 needs a way to advance the seq counter without buffering. Two options:
- **A:** Add `advance_seq(&mut self, n: u64) -> u64` method that returns the base seq and advances by n.
- **B:** Track a separate seq counter for batch frames outside the accumulator.

**Recommendation:** Option A. Keeps the seq space unified per-connection. Small, surgical addition to `ConnAccumulator`. [VERIFIED: ConnAccumulator struct at tcp.rs:738-743]

#### 5. Python SDK Encoding (`python/tally/_protocol.py`)
The `encode_push_binary(stream_name, event)` function (lines 97-166) builds a single event payload:
```
[u16 stream_name_len][stream_name][u16 field_count][fields...]
```
For `push_many`, the SDK needs to:
1. Encode the stream_name ONCE in the outer envelope
2. For each event, encode ONLY the event body (field_count + fields) -- NOT the stream_name prefix
3. Wrap in the batch frame: `[u16 stream_len][stream][u32 batch_id][u32 count][for each: [u32 event_len][event_bytes]]`

**Important:** `encode_push_binary` includes the stream_name as a prefix. For batch encoding, we need to split this: encode stream_name once in the envelope, and per-event encode only the binary event payload (starting from field_count). This means either:
- **A:** Extract the event-body encoding into a separate helper `_encode_event_body(event) -> bytes` and call it per-event
- **B:** Call `encode_push_binary` per-event and strip the stream_name prefix

**Recommendation:** Option A. Extracting `_encode_event_body` is cleaner and avoids encoding+stripping the stream_name N times. The extracted function is ~40 lines (the body of `encode_push_binary` after the stream_name write). This is a refactor of existing code, not new serialization logic, consistent with D-03's intent. [VERIFIED: encode_push_binary at _protocol.py:97-166]

#### 6. Python SDK `App.push_many` (`python/tally/_app.py`)
Follows the pattern of `App.push()` (lines 85-105):
```python
def push_many(self, stream_class, events):
    self._client.drain_errors_nonblock()
    stream_name = stream_class._tally_stream_name
    payload = encode_push_batch(stream_name, events, self._next_batch_id())
    self._client.send_frame_no_recv(OP_PUSH_BATCH, payload)
```
The `_next_batch_id` is a monotonic u32 counter on the `App` instance. [VERIFIED: App.push() pattern at _app.py:85-105]

#### 7. Python SDK `TallyClient.send_frame_no_recv` (`python/tally/_client.py` lines 242-270)
Already exists and handles fire-and-forget with at-least-once retry on broken pipe. Reusable as-is for `push_many`. [VERIFIED: send_frame_no_recv at _client.py:242-270]

### Recommended File Changes

```
src/server/protocol.rs   # +OP_PUSH_BATCH const, +PushBatch Command variant, +parse_command arm (~40 lines)
src/server/tcp.rs         # +OP_PUSH_BATCH dispatch in handle_connection (~20 lines), +advance_seq on ConnAccumulator (~5 lines)
python/tally/_protocol.py # +OP_PUSH_BATCH const, +_encode_event_body helper, +encode_push_batch function (~50 lines)
python/tally/_app.py      # +push_many method, +_next_batch_id counter, +OP_PUSH_BATCH import (~20 lines)
python/tally/__init__.py  # (may need re-export if push_many exposed at package level)
tests/test_push_batch.rs  # Decode roundtrip, oversized reject, mixed valid/invalid (~200 lines)
benchmark/tally-throughput/bench.py  # +run_single_client_async_batch, +--mode async-batch (~80 lines)
```

### Anti-Patterns to Avoid
- **Routing batch through ConnAccumulator:** The accumulator is for coalescing single-event frames. A pre-batched frame should dispatch directly. Routing through the accumulator would double-batch (64 events in an accumulator slot that is itself a batch of 16k events).
- **Encoding stream_name per-event in the batch:** Wastes bytes. Stream name goes once in the envelope, event bodies repeat.
- **Decoding batch events as JSON then re-encoding to binary:** Use `decode_event_binary` which already handles the binary format. Never touch JSON on the hot path.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Per-event binary encoding | Custom batch encoder | Existing `encode_push_binary` / extracted `_encode_event_body` | D-03 locks reuse; zero new serialization code |
| Batch error attribution | Custom error tracking | Phase 12's per-connection seq + `pending_drain` | D-09/D-10 lock reuse of existing drain mechanism |
| Batch dispatch logic | New batch handler | Phase 12's `handle_push_batch` | D-06 locks zero new hot-path logic |

## Common Pitfalls

### Pitfall 1: H-6 -- Batch decode cost exceeds per-event dispatch gain
**What goes wrong:** New decode path has its own bottleneck (same shape as Phase 11's HLL-on-async bug). The decode loop calls `decode_event_binary` per event inside the batch, plus allocates `Vec<PendingAsync>` with N elements.
**Why it happens:** Batch decode is on the hot path even though batch dispatch reuses existing code.
**How to avoid:** Benchmark the decode path in isolation BEFORE wiring it into the server. Write a Rust benchmark that decodes a 1000-event batch frame and measures time per event. Target: < 1us per event decode (at 300k eps, 1us/event decode budget = 300ms/s = 30% of one core).
**Warning signs:** Per-event decode time > 2us in isolation bench; `Vec::with_capacity` allocation showing up in flamegraph.

### Pitfall 2: H-7 -- Batch max size unbounded -> OOM attack
**What goes wrong:** Malicious client sends `count = 10,000,000,000` in the frame header. Server tries to allocate `Vec::with_capacity(10B)` -> OOM crash.
**Why it happens:** The count field is u32 = max 4.2 billion.
**How to avoid:** Hard cap at 16,384. Reject BEFORE allocation. Use `count.min(16_384)` for `with_capacity` but reject `count > 16_384` with STATUS_ERROR and connection close.
**Warning signs:** Missing validation before `Vec::with_capacity`; test missing for oversized count.

### Pitfall 3: M-5 -- Python GIL temptation for C extension
**What goes wrong:** Encoding 16k events in pure Python is "slow" (~7us/event), tempting a C extension.
**Why it happens:** The Python hot loop is `for event in events: buf += encode_event_body(event)`.
**How to avoid:** Stay pure Python. Use `bytearray` (mutable, avoids copies), pre-compile `struct.Struct` instances (already done in `_protocol.py`), minimize per-event allocations. The 300k eps target requires < 3.3us total per event including network I/O. With batching, the per-event Python overhead drops from ~7us (individual `push()` call) to ~0.3-0.5us (just the encode body loop).
**Warning signs:** Reaching for cffi, ctypes, or Cython.

### Pitfall 4: M-3 -- Partial batch frame on connection drop
**What goes wrong:** Client disconnects mid-batch. Server has read part of the batch payload.
**Why it happens:** `read_exact` fails partway through.
**How to avoid:** Frame is read atomically by `read_one_frame` (reads full `payload_len` bytes via `read_exact`). If `read_exact` fails, the entire connection closes. No partial state possible because `parse_command` only runs on a complete payload buffer. [VERIFIED: read_one_frame at tcp.rs:181-201]

### Pitfall 5: Seq counter drift between accumulator and batch
**What goes wrong:** Batch events get seq numbers that overlap with accumulator events on the same connection, corrupting drain ordering.
**Why it happens:** Batch bypasses accumulator but both need consecutive seqs from the same counter.
**How to avoid:** Add `advance_seq(n)` to `ConnAccumulator` that atomically reserves n seq numbers. Batch events get seqs `[base, base+1, ..., base+n-1]`. Accumulator's `push()` continues from `base+n`.

## Code Examples

### Rust: OP_PUSH_BATCH decode in parse_command
```rust
// Source: derived from existing OP_PUSH_ASYNC decode (protocol.rs:247-251)
// and OP_MSET decode (protocol.rs:263-292)
OP_PUSH_BATCH => {
    let stream_name = read_string(&mut buf)?;
    if buf.len() < 8 {
        return Err(TallyError::Protocol(
            "PUSH_BATCH header truncated: need 8 bytes for batch_id + count".into()
        ));
    }
    let batch_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    buf = &buf[8..];

    if count > 16_384 {
        return Err(TallyError::Protocol(
            "batch too large".into()
        ));
    }

    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.len() < 4 {
            return Err(TallyError::Protocol(
                "PUSH_BATCH event length truncated".into()
            ));
        }
        let event_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        buf = &buf[4..];
        if buf.len() < event_len {
            return Err(TallyError::Protocol(format!(
                "PUSH_BATCH event truncated: expected {} bytes, got {}",
                event_len, buf.len()
            )));
        }
        let event_bytes = &buf[..event_len];
        let raw_payload = event_bytes.to_vec();
        let mut event_buf: &[u8] = event_bytes;
        let payload = decode_event_binary(&mut event_buf)?;
        buf = &buf[event_len..];
        events.push((stream_name.clone(), batch_id, payload, raw_payload));
    }
    Ok(Command::PushBatch { stream_name, batch_id, events })
}
```

### Rust: handle_connection dispatch for OP_PUSH_BATCH
```rust
// Source: derived from existing PushAsync dispatch (tcp.rs:299-303)
Command::PushBatch { stream_name, batch_id, events } => {
    // Force-flush any pending single-event accumulator (H-2 consistency)
    if !accumulator.is_empty() {
        flush_batch_to_drain(&state, &mut accumulator, &mut pending_drain);
    }

    // Reserve seq numbers from the connection-level counter
    let base_seq = accumulator.advance_seq(events.len() as u64);

    // Convert to PendingAsync for handle_push_batch
    let now = SystemTime::now();
    let batch: Vec<PendingAsync> = events.into_iter().enumerate().map(|(i, (_sn, _bid, payload, raw))| {
        PendingAsync::new(base_seq + i as u64, stream_name.clone(), payload, raw, now)
    }).collect();

    let results = handle_push_batch(&state, &batch);
    for (ev, res) in batch.iter().zip(results.iter()) {
        if let Err(err) = res {
            pending_drain.push((ev.seq, err.to_string()));
        }
    }
    // Fire-and-forget: no response frame written.
    continue;
}
```

### Python: encode_push_batch
```python
# Source: derived from existing encode_push_binary (_protocol.py:97-166)
def encode_push_batch(stream_name: str, events, batch_id: int) -> bytes:
    """Encode an OP_PUSH_BATCH payload.

    Wire format: [u16 stream_len][stream][u32 batch_id][u32 count]
                 [for each: [u32 event_len][event_bytes]]
    """
    buf = bytearray()
    name_bytes = stream_name.encode("utf-8")
    _check_u16_len("stream_name", name_bytes)
    buf += _U16.pack(len(name_bytes))
    buf += name_bytes
    buf += struct.pack(">II", batch_id, 0)  # placeholder count
    count = 0
    for event in events:
        event_bytes = _encode_event_body(event)
        buf += struct.pack(">I", len(event_bytes))
        buf += event_bytes
        count += 1
    # Patch count
    count_offset = 2 + len(name_bytes) + 4  # after stream_name + batch_id
    struct.pack_into(">I", buf, count_offset, count)
    return bytes(buf)
```

### Python: App.push_many
```python
# Source: follows App.push() pattern (_app.py:85-105)
def push_many(self, stream_class: type, events) -> None:
    """Push a batch of events in one wire frame (fire-and-forget).

    Args:
        stream_class: The @tally.stream-decorated class.
        events: Iterable of event dicts.
    """
    self._client.drain_errors_nonblock()
    stream_name = stream_class._tally_stream_name
    batch_id = self._next_batch_id()
    payload = encode_push_batch(stream_name, events, batch_id)
    self._client.send_frame_no_recv(OP_PUSH_BATCH, payload)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Per-event `push()` loop (Phase 11) | `push_many()` batch frame (Phase 13) | This phase | Reduces Python per-event overhead from ~7us to ~0.3-0.5us |
| JSON event encoding (pre-Phase 11) | Binary `encode_push_binary` (Phase 11) | Phase 11 | ~5x encoding speedup; Phase 13 reuses this |
| Single-event server decode | Batch decode + `handle_push_batch` (Phase 12+13) | Phase 12/13 | Amortizes lock + event_log + dirty_mark over N events |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Per-event Python encode overhead drops from ~7us to ~0.3-0.5us with batching | Summary / Code Examples | Throughput target (300k) may not be achievable in pure Python; would need profiling |
| A2 | `advance_seq(n)` on ConnAccumulator is the cleanest way to unify seq spaces | Architecture Patterns | Could use a separate counter, but risks seq overlap bugs |
| A3 | `_encode_event_body` extraction from `encode_push_binary` is ~40 lines | Architecture Patterns | Slightly more or fewer; refactor scope is small either way |

## Open Questions

1. **Command::PushBatch variant design**
   - What we know: Need to carry stream_name, batch_id, and per-event (payload, raw_payload) pairs
   - What's unclear: Whether to decode into `Vec<PendingAsync>` in `parse_command` (requires seq assignment there, but seq is connection-local) or decode into an intermediate struct and convert at dispatch
   - Recommendation: Decode into an intermediate `Vec<(serde_json::Value, Vec<u8>)>` in `parse_command` (which has no connection context), then convert to `Vec<PendingAsync>` at the dispatch site in `handle_connection` where the seq counter lives. This is cleaner than threading connection state into the parser.

2. **Batch-level error vs per-event error in STATUS_ERROR frame**
   - What we know: D-09 says errors surface via `drain_errors_nonblock` with `(batch_id, event_index)` payload. Phase 12's drain uses `Vec<(u64, String)>` where the u64 is the seq.
   - What's unclear: Whether to embed `batch_id` and `event_index` in the error string, or restructure the drain tuple
   - Recommendation: Embed in the error string: `"[batch:{batch_id} event:{idx}] {error_message}"`. This avoids changing the drain infrastructure while providing attribution. The Python SDK can parse this prefix if needed.

3. **Whether `OP_PUSH_BATCH` should trigger the tight inner loop**
   - What we know: Phase 12 added a tight inner loop (tcp.rs:313-400) that reads more async frames from BufReader without going through select!
   - What's unclear: Whether a batch frame should be followed by more tight-loop reads
   - Recommendation: No. A batch frame is already a complete unit. After dispatching it, fall through to the normal loop. The tight inner loop optimization is for single-event async frames where BufReader has many queued.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust) + pytest-compatible raw assertions (Python) |
| Config file | Cargo.toml `[[test]]` sections + existing test infrastructure |
| Quick run command | `cargo test test_push_batch -- --test-threads=1` |
| Full suite command | `cargo test -- --test-threads=1` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERF-04.1 | OP_PUSH_BATCH decode roundtrip | unit | `cargo test test_push_batch::decode_roundtrip --test-threads=1` | Wave 0 |
| PERF-04.2 | Hard cap 16,384 reject | unit | `cargo test test_push_batch::oversized_batch_reject --test-threads=1` | Wave 0 |
| PERF-04.3 | Oversized count (10B) no OOM | integration | `cargo test test_push_batch::giant_count_clean_reject --test-threads=1` | Wave 0 |
| PERF-04.4 | Mixed valid/invalid events in batch | integration | `cargo test test_push_batch::partial_failure --test-threads=1` | Wave 0 |
| PERF-04.5 | Batch dispatch to handle_push_batch | integration | `cargo test test_push_batch::e2e_batch_dispatch --test-threads=1` | Wave 0 |
| PERF-04.6 | push_many Python roundtrip | integration | Raw TCP test in Rust test file | Wave 0 |
| PERF-04.7 | Backward compat (push still works) | integration | Existing test_server tests | Exists |
| PERF-04.8 | 300k eps gate | bench | `python3 bench.py --mode async-batch --pipeline medium` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test test_push_batch -- --test-threads=1`
- **Per wave merge:** `cargo test -- --test-threads=1` (full 632+ tests)
- **Phase gate:** Full suite green + bench gate (300k eps medium async-batch)

### Wave 0 Gaps
- [ ] `tests/test_push_batch.rs` -- covers PERF-04.1 through PERF-04.6
- [ ] bench.py `--mode async-batch` -- covers PERF-04.8
- [ ] No framework install needed -- cargo test infrastructure already exists

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | -- |
| V3 Session Management | no | -- |
| V4 Access Control | no | -- |
| V5 Input Validation | yes | Hard cap 16,384 (H-7), u32 count validation before allocation, per-event length validation |
| V6 Cryptography | no | -- |

### Known Threat Patterns for batch protocol

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| OOM via oversized batch count | Denial of Service | Hard cap 16,384 + reject before allocation (D-07) |
| Malformed event bytes within batch | Tampering | `decode_event_binary` validates each event independently; errors are per-event, not batch-fatal |
| Partial frame injection | Denial of Service | `read_exact` atomicity -- connection closes on incomplete read (M-3) |

## Sources

### Primary (HIGH confidence)
- `src/server/tcp.rs` -- `handle_push_batch`, `PendingAsync`, `ConnAccumulator`, `handle_connection` dispatch loop
- `src/server/protocol.rs` -- opcode constants (lines 9-16), `Command` enum (lines 31-47), `parse_command` (lines 235-309), `decode_event_binary` (line 157)
- `python/tally/_protocol.py` -- `encode_push_binary` (lines 97-166), opcode constants (lines 22-29), frame encoding utilities
- `python/tally/_app.py` -- `App.push()` pattern (lines 85-105), `App.push_sync()` pattern (lines 107-119)
- `python/tally/_client.py` -- `send_frame_no_recv` (lines 242-270), `drain_errors_nonblock` (lines 112-240)
- `.planning/phases/12-server-side-async-push-coalescing/12-02-SUMMARY.md` -- handle_push_batch details, ConnAccumulator API, pending_drain mechanism
- `benchmark/tally-throughput/bench.py` -- existing harness with `--mode async`, `--matrix`, `--mode mixed` patterns

### Secondary (MEDIUM confidence)
- `.planning/research/PITFALLS.md` -- H-6, H-7, M-3, M-5 pitfall descriptions and mitigations
- `.planning/research/SUMMARY.md` -- build order rationale, Phase 13 characterization as "wire-format + SDK only"
- `.planning/ROADMAP.md` Phase 13 section -- 9 success criteria, stack additions (none)
- `.planning/REQUIREMENTS.md` -- PERF-04 acceptance criteria
- `13-CONTEXT.md` -- D-01 through D-18 locked decisions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- zero new dependencies, all integration points verified in source
- Architecture: HIGH -- all code paths read and verified, `handle_push_batch` API confirmed public
- Pitfalls: HIGH -- H-6/H-7/M-3/M-5 grounded in existing codebase patterns and Phase 11 lessons

**Research date:** 2026-04-11
**Valid until:** 2026-05-11 (stable -- no external dependencies, all code is project-internal)
