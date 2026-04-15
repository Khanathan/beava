# Phase 11: Fire-and-Forget PUSH + Binary Wire Protocol — Research

**Researched:** 2026-04-11
**Domain:** TCP wire protocols, Rust binary decoding, Python socket pipelining, client-side message framing
**Confidence:** HIGH (codebase-verified), MEDIUM (Python perf micro-claims from training)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D1 — Two first-class APIs.** `app.push()` is fire-and-forget (returns `None`). `app.push_sync()` preserves v1.1 behavior (returns `FeatureResult`). `app.flush()` is a barrier that blocks until all prior async pushes are acknowledged and raises any surfaced error.
- **D2 — Opcode additions, not repurposing.** `OP_PUSH = 0x01` keeps exact v1.1 semantics. New opcodes: `OP_PUSH_ASYNC = 0x07`, `OP_FLUSH = 0x08`. The Python SDK routes `push()` through `OP_PUSH_ASYNC` and `push_sync()` through `OP_PUSH`.
- **D3 — Binary event payload on PUSH (both sync and async).** Replace `serde_json::from_slice` on PUSH paths with a typed binary encoder. Wire format:
  ```
  [u16 stream_name_len][stream_name][u16 field_count]
    field := [u16 key_len][key_bytes][type_tag: u8][value]
    type_tag:
      0x00 = null/missing (0 bytes)
      0x01 = bool (1 byte)
      0x02 = i64 (8 bytes, big-endian)
      0x03 = f64 (8 bytes, big-endian IEEE 754)
      0x04 = string ([u16 len][utf-8 bytes])
  ```
  Only PUSH event payloads become binary. Response feature maps, GET responses, SET/MSET/REGISTER payloads stay JSON.
- **D4 — Error surfacing via non-blocking drain.** Before every `push*` / `flush` / `get`, the Python SDK calls `select.select([sock], [], [], 0)` to drain pending error frames. No background thread.
- **D5 — `OP_FLUSH` is a trivial server-side no-op.** Server reads the `OP_FLUSH` frame, immediately sends `STATUS_OK` (empty payload). In-order TCP + sequential `handle_connection` dispatch means all prior async pushes are already processed.
- **D6 — Latency tracker folds async+sync PUSH into `CommandKind::Push`.** No new `CommandKind` variant. Same histogram, same slow-query capture.

### Claude's Discretion

- **Binary decoder target type.** Open question #1 in CONTEXT.md. Whether `decode_event_binary` returns `serde_json::Value::Object` (minimal refactor, downstream untouched) or a new `EventPayload` type (deeper refactor, potentially faster). Researched below.
- **Exact `Command` enum shape.** Either reuse `Command::Push { stream_name, payload, mode: PushMode }` with `enum PushMode { Sync, Async }`, or add a separate `Command::PushAsync` variant. Researched below.
- **Python binary encoder idiom.** `struct.pack` vs `struct.pack_into` vs manual `bytearray.extend`. Micro-benchmark during implementation.
- **Whether to delete `encode_push` JSON helper.** Depends on whether any external tooling (benchmarks, test_debug_ui) currently uses it.

### Deferred Ideas (OUT OF SCOPE)

- Multi-threaded tokio runtime / DashMap / per-entity locks — future SemVer-major phase.
- HLL cache — Phase 12 (independent).
- Binary feature map on response side (`push_sync()` and `get()` returns) — future phase.
- Rust SDK — later.
- Pipelining with inline ACK drain / background reader thread — drain-on-next-call is sufficient for 100k target.
- REGISTER binary format — REGISTER is startup-only.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-01 | Fire-and-forget ingest: `app.push()` returns None and does not wait for a feature response; `app.push_sync()` preserves v1.1 behavior; `app.flush()` blocks until all in-flight async pushes are processed and surfaces any prior error. | `select.select([sock],[],[],0)` drain pattern is the standard Python way to poll a blocking socket without a background thread (Python docs: `select` module). TCP in-order delivery over a single connection guarantees the server processes OP_FLUSH strictly after all prior OP_PUSH_ASYNC frames on the same connection, so `flush` needs no server-side queue. |
| PERF-02 | Binary event payload: PUSH event payload decoded from a typed binary format instead of `serde_json::from_slice`; JSON parse hotspots gone from PUSH path in callgrind. | Existing Rust wire format already uses length-prefixed strings (`write_string` / `read_string` at protocol.rs:78-112) and length-prefixed frames — the new field encoder reuses the same idiom. Rust has zero-cost byte-slice reads for fixed-width integers via `u16::from_be_bytes` / `u64::from_be_bytes`. No serde, no allocator traffic beyond the returned `Value::Object` (or `EventPayload`). |

</phase_requirements>

## Summary

Phase 11 is a performance + API phase with a clear shape: the CONTEXT.md already locks every meaningful decision. Research here is mostly (1) confirming the codebase hooks are where CONTEXT.md says they are, (2) resolving the two Claude-discretion questions with codebase-grounded recommendations, and (3) cataloguing the raw-TCP tests that must be updated.

Verified codebase facts:

- `parse_command` dispatches by opcode at `src/server/protocol.rs:131-197`. Adding `OP_PUSH_ASYNC = 0x07` and `OP_FLUSH = 0x08` is a two-line constant addition plus a match-arm addition.
- `handle_sync_command` is the canonical PUSH pipeline at `src/server/tcp.rs:198-375`, and its PUSH arm already has a clean separation between "produce features" (`engine.push_with_cascade` → `feature_map_to_json`) and "post-push side effects" (event log append, cascade/fan-out, dirty-marking, throughput, latency). The async variant reuses everything except the `feature_map_to_json` step.
- `handle_connection` (`src/server/tcp.rs:124-195`) writes a response for every command via `writer.write_all(&resp_bytes)`. The async path needs to either (a) skip the write entirely on empty-OK, or (b) let `Command::PushAsync` return `None` (i.e. change the response type from `Result<Vec<u8>>` to `Result<Option<Vec<u8>>>`). Option (b) is cleaner and affects one call site. Recommended.
- `CommandKind::Push` at `src/server/latency.rs:64` and `record_push` at `src/server/latency.rs:306` already exist and take `stream_name + micros`. Both branches will call the same recorder — CONTEXT.md D6 is a one-liner.
- Raw-TCP PUSH frames exist only in `tests/test_server.rs` (9 call sites) and `tests/test_pipeline.rs` uses engine-level `push()` not the wire — so the wire test surface is small. `tests/test_debug_ui.rs` does NOT craft PUSH frames; it uses the SDK. Effective test-file audit: only `test_server.rs` needs raw-wire updates, and the helper is already factored at `test_server.rs:104-105`.
- `MAX_FRAME_SIZE = 64 MB` in both Rust (`tcp.rs:140`) and Python (`_protocol.py:33`). Binary PUSH payloads are vastly smaller than the current JSON ones for typical events — no frame-size concerns.

**Primary recommendation:** Use `serde_json::Value::Object` as the decoder target (Open Q #1 → "simplest"), because downstream code (`engine.push_with_cascade`, event log, fan-out, dirty-marking, slow-query key_preview) all read the payload as `serde_json::Value` — introducing `EventPayload` would touch 8+ files for negligible win. The hot cost is `from_slice` (parser + allocator), not the `Value` type itself; the new decoder still produces `Value::Object` but with direct `serde_json::Map::insert` calls (no parsing, no intermediate string tokens).

**Command enum shape:** Add `Command::PushAsync { stream_name, payload }` as a separate variant (not a `mode:` field on `Push`). Reason: `handle_connection` dispatch uses `match cmd { Command::Mset {..} => ..., other => ... }` and cleanly extending this to `Command::PushAsync {..} => handle_push_async(...)` is simpler than pattern-matching on a nested enum. Also keeps the PUSH arm in `handle_sync_command` unchanged (lower regression risk on the sync path).

## Standard Stack

Not applicable — all dependencies are already in `Cargo.toml` / `python/pyproject.toml`:

- **Rust server:** `tokio`, `serde_json`, `std::io::{BufReader, BufWriter}`. No new crates.
- **Python SDK:** `socket`, `struct`, `select` (all stdlib). No new packages.

## Architecture Patterns

### Pattern 1 — Fast binary decode by field streaming (Rust)

**What:** Walk the byte buffer with a moving `&[u8]` slice, decoding each field in place. Use the same pattern as `read_string` at `protocol.rs:78-96`.

**Example (target shape):**

```rust
// src/server/protocol.rs
pub const TYPE_NULL: u8 = 0x00;
pub const TYPE_BOOL: u8 = 0x01;
pub const TYPE_I64: u8  = 0x02;
pub const TYPE_F64: u8  = 0x03;
pub const TYPE_STR: u8  = 0x04;

pub fn decode_event_binary(buf: &mut &[u8]) -> Result<serde_json::Value, TallyError> {
    if buf.len() < 2 {
        return Err(TallyError::Protocol("event payload: missing field_count".into()));
    }
    let field_count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];

    let mut map = serde_json::Map::with_capacity(field_count);
    for _ in 0..field_count {
        let key = read_string(buf)?;
        if buf.is_empty() {
            return Err(TallyError::Protocol("event payload: missing type tag".into()));
        }
        let tag = buf[0];
        *buf = &buf[1..];
        let value = match tag {
            TYPE_NULL => serde_json::Value::Null,
            TYPE_BOOL => {
                if buf.is_empty() { return Err(TallyError::Protocol("bool truncated".into())); }
                let v = buf[0] != 0;
                *buf = &buf[1..];
                serde_json::Value::Bool(v)
            }
            TYPE_I64 => {
                if buf.len() < 8 { return Err(TallyError::Protocol("i64 truncated".into())); }
                let n = i64::from_be_bytes(buf[..8].try_into().unwrap());
                *buf = &buf[8..];
                serde_json::Value::Number(n.into())
            }
            TYPE_F64 => {
                if buf.len() < 8 { return Err(TallyError::Protocol("f64 truncated".into())); }
                let n = f64::from_be_bytes(buf[..8].try_into().unwrap());
                *buf = &buf[8..];
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            TYPE_STR => serde_json::Value::String(read_string(buf)?),
            _ => return Err(TallyError::Protocol(format!("unknown type tag 0x{:02x}", tag))),
        };
        map.insert(key, value);
    }
    Ok(serde_json::Value::Object(map))
}
```

- **Zero parser allocations.** `from_be_bytes` reads 2/8-byte primitives from a stack array.
- **Matches existing wire style.** Same `&mut &[u8]` walking pattern used by `read_string`.
- **Returns `Value::Object`.** Downstream code (`engine.push_with_cascade`, event log `serde_json::to_vec(&payload)`, `payload.get(kf)` in dirty-marking and fan-out) works unchanged. The ONE downside is the event-log append still serializes the `Value` back to JSON (`serde_json::to_vec(&payload)` at `tcp.rs:231`), which we accept in this phase (Deferred: binary event log).

### Pattern 2 — Fire-and-forget dispatch in `handle_connection`

**What:** Change the per-command response type to `Result<Option<Vec<u8>>, TallyError>`. `None` = send nothing; `Some(bytes)` = wrap in `STATUS_OK` and write.

**Example:**

```rust
// src/server/tcp.rs handle_connection (new shape)
let response: Result<Option<Vec<u8>>, TallyError> = match cmd {
    Command::Mset { entries } => handle_mset(entries, &state).await.map(Some),
    Command::PushAsync { stream_name, payload } => {
        handle_push_async(stream_name, payload, &state)  // returns Ok(None) on success
    }
    Command::Flush => Ok(Some(vec![])),  // empty OK ack
    other => handle_sync_command(other, &state).map(Some),
};

match response {
    Ok(None) => {
        // Fire-and-forget success: write nothing, do NOT flush.
    }
    Ok(Some(payload)) => {
        let resp = protocol::encode_response(STATUS_OK, &payload);
        writer.write_all(&resp).await?;
        writer.flush().await?;
    }
    Err(e) => {
        // Error path ALWAYS writes, even on async — this is how clients learn.
        let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
        writer.write_all(&resp).await?;
        writer.flush().await?;
    }
}
```

Critical: error frames always fly, even for async pushes. That's how the Python drain picks them up.

**Perf note:** not calling `writer.flush().await?` on the success-async path is where the pipelining win lives. `BufWriter` will batch frames in its internal buffer; they get flushed on the NEXT sync response or on explicit flush. This is exactly the "kernel batches writes" behavior CONTEXT.md D1 describes.

### Pattern 3 — Non-blocking Python drain

**What:** Use `select.select([sock], [], [], 0)` to check for readable bytes without blocking. If ready, read one frame and surface any error on the current call.

**Example:**

```python
# python/tally/_client.py
import select

class TallyClient:
    def __init__(self, ...):
        ...
        self._pending_error: ProtocolError | None = None

    def drain_errors_nonblock(self) -> None:
        """Raise or store any pending error frames from the server.

        Must be called before every user-facing operation. Reads at most one
        frame per call (the one that's currently readable); subsequent frames
        will be picked up by subsequent drains.
        """
        if self._pending_error is not None:
            err, self._pending_error = self._pending_error, None
            raise err

        if self._sock is None:
            return

        # Non-blocking readability probe: 0 timeout = immediate return.
        ready, _, _ = select.select([self._sock], [], [], 0)
        if not ready:
            return

        # A frame is waiting. Read exactly one complete frame.
        status, payload = self._recv_frame()
        if status == STATUS_ERROR:
            raise ProtocolError(payload.decode("utf-8", errors="replace"))
        # If status == OK, it's a stray ACK (e.g., from OP_FLUSH that we don't
        # track as pending). Safe to discard — not a correctness issue.
```

- **`select` with `timeout=0`** is the canonical Python way to poll readiness without blocking. Documented in the `select` module docs.
- **No background thread.** No locking. No join on teardown.
- **One frame per drain call.** This is fine: drains happen before every push, so multiple queued error frames will be picked up over at most N pushes.

### Pattern 4 — Fast Python binary encoder

**What:** Single `bytearray`, `struct.pack` for the header, manual byte extends for keys and values. `struct.pack("!H{n}s", n, key_bytes)` is faster than separate `bytes([(n >> 8) & 0xff, n & 0xff])` + concat.

**Example:**

```python
# python/tally/_protocol.py
OP_PUSH_ASYNC: int = 0x07
OP_FLUSH: int = 0x08

TYPE_NULL: int = 0x00
TYPE_BOOL: int = 0x01
TYPE_I64:  int = 0x02
TYPE_F64:  int = 0x03
TYPE_STR:  int = 0x04

_H = struct.Struct(">H")       # u16 BE
_Hb = struct.Struct(">HB")     # u16 BE + u8
_HBq = struct.Struct(">HBq")   # u16 BE + u8 + i64 BE
_HBd = struct.Struct(">HBd")   # u16 BE + u8 + f64 BE

def encode_push_binary(stream_name: str, event: dict) -> bytes:
    buf = bytearray()
    name_bytes = stream_name.encode("utf-8")
    buf += _H.pack(len(name_bytes))
    buf += name_bytes
    buf += _H.pack(len(event))  # field_count

    for key, value in event.items():
        key_bytes = key.encode("utf-8")
        klen = len(key_bytes)
        if value is None:
            buf += _Hb.pack(klen, TYPE_NULL)
            buf += key_bytes
        elif isinstance(value, bool):
            # IMPORTANT: bool check must come BEFORE int (bool is int subclass)
            buf += _Hb.pack(klen, TYPE_BOOL)
            buf += key_bytes
            buf += b"\x01" if value else b"\x00"
        elif isinstance(value, int):
            buf += _Hb.pack(klen, TYPE_I64)
            buf += key_bytes
            buf += struct.pack(">q", value)
        elif isinstance(value, float):
            buf += _Hb.pack(klen, TYPE_F64)
            buf += key_bytes
            buf += struct.pack(">d", value)
        elif isinstance(value, str):
            v_bytes = value.encode("utf-8")
            buf += _Hb.pack(klen, TYPE_STR)
            buf += key_bytes
            buf += _H.pack(len(v_bytes))
            buf += v_bytes
        else:
            raise ProtocolError(
                f"unsupported event field type for key {key!r}: {type(value).__name__}"
            )

    return bytes(buf)
```

- **Pre-compiled `struct.Struct`** instances — avoid re-parsing the format string per call.
- **`bytearray` + `+=`** is fast in CPython (amortized O(1) for extends).
- **`bool` check before `int`** — `isinstance(True, int)` is `True` in Python; handle bools first.
- **No dict traversal** beyond `.items()`.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Integer endian conversion (Rust) | Bit-shifting loops | `u16::from_be_bytes([b0,b1])` / `i64::from_be_bytes(arr.try_into().unwrap())` | Zero-cost stdlib, bounds-checked. |
| Python struct packing | Manual `bytes([x])` + shift | `struct.Struct(">HBq").pack(...)` | Pre-compiled format string is 2-3x faster than `struct.pack` with format each call. |
| Python readability poll | Background thread + queue | `select.select([sock], [], [], 0)` | Stdlib, zero thread overhead, documented pattern. |
| Socket framing | Custom buffering | Existing `_recv_exact` / `_recv_frame` in `python/tally/_client.py` | Already handles truncation and reconnect. |
| Rust match dispatch | Nested enum flags | Add `Command::PushAsync` + `Command::Flush` variants | One new match arm per variant; minimal coupling. |

**Key insight:** Every component in this phase has a direct stdlib/existing equivalent. Hand-rolling would add cost (maintenance, correctness, test surface) for zero performance win.

## Common Pitfalls

### Pitfall 1: `bool` before `int` dispatch in Python encoder

**What goes wrong:** `True` gets encoded as `TYPE_I64` because `isinstance(True, int) == True` in Python.
**Why it happens:** `bool` is a subclass of `int`.
**How to avoid:** Check `isinstance(value, bool)` BEFORE `isinstance(value, int)` in the encoder dispatch.
**Warning sign:** A roundtrip unit test with `{"flag": True}` asserts the decoded value is `bool(True)`, not `1`.

### Pitfall 2: Forgetting to flush on sync-response path

**What goes wrong:** A sync `push_sync()` or `get()` response sits in `BufWriter`'s internal buffer indefinitely because the async path no longer flushes.
**Why it happens:** The shared `handle_connection` writer is `BufWriter`-wrapped; without an explicit `writer.flush().await?` after writing, bytes can be held back.
**How to avoid:** Any `Ok(Some(bytes))` arm in `handle_connection` MUST flush after writing. Any `Err(...)` arm MUST flush. Only the `Ok(None)` (async success) arm skips flush.
**Warning sign:** A raw-TCP sync test hangs waiting for the response.

### Pitfall 3: Drain reads too many frames and consumes the next sync response

**What goes wrong:** The drain on `push_sync()` reads a stray OK frame ahead of the sync-response frame, and the sync call then reads the wrong frame.
**Why it happens:** Multiple frames queued on the socket; drain and sync read overlap.
**How to avoid:** `drain_errors_nonblock()` reads AT MOST ONE frame per call. Sync `_send` reads the very next frame unconditionally. Any OK frame seen by the drain is an ack for a prior async push's error-path (there shouldn't be one) or an explicit FLUSH ack — both discardable.
**Warning sign:** Sync push returns garbled feature data, or a feature result from the wrong event.

### Pitfall 4: Raw-TCP tests craft JSON PUSH frames

**What goes wrong:** `tests/test_server.rs` uses `protocol::write_string(stream_name) + serde_json::to_vec(&event)` (line 104-105) to craft PUSH frames. After the binary switch, `parse_command` will reject these frames.
**Why it happens:** The test helper was written against the v1.1 JSON format.
**How to avoid:** Refactor the helper to call a new `encode_push_binary` test fixture that mirrors `decode_event_binary`. Add both sync and async test variants. **All PUSH call sites in `test_server.rs` must go through the new helper** — grep count is 9.
**Warning sign:** Early `cargo test` run shows 9 PUSH tests failing with "invalid JSON payload" or "unknown type tag".

### Pitfall 5: Dirty-marking / fan-out / slow-query key extraction still expects `payload.get(kf).as_str()`

**What goes wrong:** The binary decoder produces `Value::Object`, so `payload.get(key_field)` returns `Option<&Value>`, which is still compatible with the existing `.as_str()` calls at `tcp.rs:221,243,285,338,360`. No code change needed — but if you change the return type to `EventPayload`, ALL of these break.
**Why it happens:** Coupling of downstream code to the serde_json::Value API.
**How to avoid:** Stick with the CONTEXT.md Open Q #1 recommendation: return `Value::Object`. Do NOT introduce `EventPayload`.
**Warning sign:** After changing decoder return type, `cargo check` produces 5+ type errors in `handle_sync_command`.

### Pitfall 6: `f64::from_be_bytes` on NaN/Infinity → `serde_json::Number::from_f64` returns `None`

**What goes wrong:** `serde_json::Number` cannot represent NaN or Infinity. An incoming f64 with those values decodes silently to `Value::Null`.
**Why it happens:** JSON spec does not allow NaN/Infinity; serde_json enforces this.
**How to avoid:** Decide on the contract: either reject (return `Err`) or map to Null (as shown in the decoder sketch above). CONTEXT.md doesn't specify; the planner should pick "return Err" since that's the strict behavior and Tally's v1.1 JSON code would have errored too (`serde_json::from_slice` refuses `NaN`).
**Warning sign:** A unit test that pushes an event with `{"amount": float("nan")}` silently drops the amount.

### Pitfall 7: `BufReader` doesn't know about the new opcode — dispatch is opcode-dispatch only

**What goes wrong:** None — but worth documenting. `handle_connection` reads length + opcode + payload and hands the opcode byte to `parse_command`. Adding opcodes 0x07 and 0x08 is purely a `parse_command` match extension; the length framing is unchanged.
**How to avoid:** Just add the opcodes to `parse_command`. Do NOT touch the framing code.

## Runtime State Inventory

Not applicable — this phase is a pure code + wire-format change. There is no rename, data migration, or stored-state rewrite. The only "runtime state" touched is:

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | None. State store is unchanged; operator state and snapshots are untouched. | None |
| Live service config | None. No external config carries the JSON-event-format assumption. | None |
| OS-registered state | None. | None |
| Secrets/env vars | None. | None |
| Build artifacts | `target/release/tally` must be rebuilt with the new protocol. Python SDK bytecode cache in `python/tally/__pycache__` is regenerated automatically. | Rebuild binary; delete `__pycache__` if stale. |

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rust toolchain | Server compile | (assumed — builds today) | — | — |
| Python 3.x | SDK + tests | (assumed — runs today) | — | — |
| `select` stdlib | Python drain | ✓ (stdlib, POSIX + Windows) | — | On Windows, `select.select` on sockets works fine — it's only file descriptors other than sockets that break on Windows. |
| `struct` stdlib | Python encoder | ✓ (stdlib) | — | — |
| callgrind + valgrind | Perf gate (Success Criterion 5) | (assumed — used in Phase 10 profiling) | — | If missing, accept the throughput benchmark as proxy evidence. |

No new dependencies.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Rust framework | `cargo test` (stdlib + tokio test harness) |
| Python framework | `pytest` |
| Config file | `Cargo.toml` for Rust; `python/pyproject.toml` for Python |
| Quick run commands | `cargo test --lib protocol`, `pytest python/tests/test_app.py -x` |
| Full suite commands | `cargo test` (all 569 tests), `pytest python/tests` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERF-01 | `app.push()` returns None | unit (python) | `pytest python/tests/test_app.py::test_push_returns_none -x` | ❌ Wave 0 |
| PERF-01 | `app.push_sync()` returns `FeatureResult` | unit (python) | `pytest python/tests/test_app.py::test_push_sync_returns_feature_result -x` | ❌ Wave 0 |
| PERF-01 | `app.flush()` blocks and ACKs | integration (python) | `pytest python/tests/test_app.py::test_flush_blocks_until_ack -x` | ❌ Wave 0 |
| PERF-01 | Error from bad async push surfaces on next call | integration (python) | `pytest python/tests/test_app.py::test_error_on_next_push_after_bad_async -x` | ❌ Wave 0 |
| PERF-01 | Server raw-TCP: OP_PUSH_ASYNC + OP_GET roundtrip | integration (rust) | `cargo test --test test_server test_push_async_roundtrip` | ❌ Wave 0 |
| PERF-01 | Server raw-TCP: OP_FLUSH roundtrip | integration (rust) | `cargo test --test test_server test_flush_roundtrip` | ❌ Wave 0 |
| PERF-01 | Server raw-TCP: malformed OP_PUSH_ASYNC returns STATUS_ERROR | integration (rust) | `cargo test --test test_server test_push_async_malformed` | ❌ Wave 0 |
| PERF-02 | `decode_event_binary` roundtrips all 5 type tags | unit (rust) | `cargo test --lib protocol::tests::decode_event_binary` | ❌ Wave 0 |
| PERF-02 | `encode_push_binary` (python) roundtrips via decoder | unit (python) | `pytest python/tests/test_protocol.py::test_encode_push_binary_roundtrip -x` | ❌ Wave 0 |
| PERF-02 | Throughput gate — medium pipeline ≥ 100k events/sec | bench | `python3 benchmark/tally-throughput/bench.py --events 100000 --clients 1 --pipeline medium --mode async` | ❌ Wave 0 (bench.py needs `--mode` flag) |
| PERF-02 | No regression on p99 PUSH < 100us (sync arm) | bench | `python3 benchmark/tally-throughput/bench.py --events 20000 --clients 1 --pipeline medium --mode sync` | existing |
| PERF-01 | All 569 existing tests still pass | regression | `cargo test && pytest python/tests` | existing |

### Sampling Rate

- **Per task commit:** corresponding `cargo test --lib protocol` or targeted pytest invocation.
- **Per plan merge:** `cargo test` for that crate's test files + `pytest python/tests/test_app.py python/tests/test_protocol.py`.
- **Phase gate:** Full `cargo test` green, full `pytest python/tests` green, benchmark gate ≥ 100k events/sec on medium pipeline in async mode.

### Wave 0 Gaps

- [ ] `tests/test_server.rs` — update `send_frame` PUSH helper to use binary encoder; add `test_push_async_roundtrip`, `test_flush_roundtrip`, `test_push_async_malformed`
- [ ] `tests/test_pipeline.rs` — no raw-TCP PUSH frames, BUT verify no indirect coupling to the JSON event payload format (grep check)
- [ ] `python/tests/test_app.py` — add `test_push_returns_none`, `test_push_sync_returns_feature_result`, `test_flush_blocks_until_ack`, `test_error_on_next_push_after_bad_async`
- [ ] `python/tests/test_protocol.py` — add roundtrip unit tests for `encode_push_binary` covering null/bool/i64/f64/string + the bool-before-int pitfall
- [ ] `src/server/protocol.rs` unit tests — add `decode_event_binary` coverage for each type tag + truncation errors + unknown tag
- [ ] `benchmark/tally-throughput/bench.py` — add `--mode {sync,async}` flag, add warmup-then-flush in async mode
- [ ] No framework install needed.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | Tally is a localhost/private-network server; auth out of scope for v1.x. |
| V3 Session Management | no | No sessions. |
| V4 Access Control | no | No multi-tenant access control. |
| V5 Input Validation | **yes** | The new binary decoder is a fresh attack surface: malformed frames, type-tag fuzzing, length overflows, UTF-8 validity, NaN/Infinity floats. |
| V6 Cryptography | no | No new crypto. |
| V7 Error Handling | **yes** | Errors from bad async pushes must be surfaced (not swallowed) and must not leak internal state beyond what v1.1 already leaks. |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Length-prefixed buffer over-read | Tampering / DoS | All reads are bounds-checked in the decoder (already shown in Pattern 1); reuse `read_string` which bounds-checks. `MAX_FRAME_SIZE = 64 MB` framing already rejects oversize frames upstream at `tcp.rs:140`. |
| Malicious type tag | Tampering | `match tag { ... _ => Err(...) }` rejects unknown tags with `TallyError::Protocol`. |
| Integer overflow on `field_count` | DoS | `field_count` is `u16`, max 65535 — bounded memory per frame. `serde_json::Map::with_capacity(field_count)` is safe at this size. |
| UTF-8 smuggling | Tampering | `read_string` already uses `str::from_utf8` which rejects invalid sequences. |
| Error-frame leak / stall | Information disclosure / DoS | Error frames propagate the existing `TallyError::Protocol` strings — same information the v1.1 JSON path exposed. No new leak. |
| Async push flooding without drain | DoS | Client controls its own socket; if it never drains, TCP backpressure engages via `sendall` blocking on kernel send buffer full. This is documented behavior from D4. |
| Resurrection of invalid `NaN`/`Inf` floats | Tampering | Decoder returns `Err(Protocol)` on NaN/Inf (see Pitfall 6). Planner must choose "strict" behavior. |

## Code Examples

See Patterns 1-4 above for full code-level illustration. All examples are grounded in the actual tally codebase (file:line references given).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| JSON event payload over TCP | Typed binary field-list | Phase 11 (this) | -4-5% server CPU on PUSH path; Python encode drops from ~5us to ~1-2us (unverified — micro-bench during implementation) |
| Sync `push()` returning FeatureResult | Fire-and-forget `push()` + explicit `push_sync()` / `flush()` | Phase 11 | Removes ~9us round-trip per push; enables kernel-level batching |
| No latency insight into PUSH | Phase 10.2 histograms folded across sync+async | Phase 10.2 + Phase 11 | One histogram covers both modes |

**Deprecated/outdated:**

- `encode_push` (JSON version in `python/tally/_protocol.py`). Can be deleted or kept for one release as a compat shim. CONTEXT.md leaves this to the planner — recommendation: **delete in this phase** since no external tool depends on it and the only test-side user is `tests/test_server.rs` which we're already updating.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `struct.Struct(">HBq").pack(...)` is 2-3x faster than `struct.pack(">HBq", ...)` each call in CPython | Pattern 4 | LOW — even if equal, no correctness impact; just a micro-perf nit. |
| A2 | `select.select([sock], [], [], 0)` costs ~1us per call on Linux | Pattern 3 / CONTEXT.md D4 | LOW — even at 5us, total drain overhead is << round-trip savings. |
| A3 | `BufWriter` flush elision on async-success path is enough for kernel TCP batching | Pattern 2 | **MEDIUM** — if BufWriter flushes aggressively, the client won't see batching and throughput will hover around ~30-50k. Mitigation: profile mid-phase (CONTEXT.md risk register already tracks this). The fallback is to set `TCP_NODELAY = false` on the server socket OR explicitly track dirty flag in the per-connection write path. |
| A4 | The Python GIL is not a bottleneck for single-client async push at 100k events/sec | Implicit | MEDIUM — if GIL becomes the bottleneck, we hit the 50k fallback gate from CONTEXT.md risk register. Profile before declaring done. |
| A5 | `serde_json::to_vec(&payload)` for the event log append (at `tcp.rs:231`) does NOT dominate the PUSH hot path | Summary | LOW-MEDIUM — callgrind in Phase 10 should confirm. If it does dominate, Phase 11 still hits 100k from the Python-side win alone; binary event log becomes the next phase. |

**None of these assumptions block planning.** All are flagged for mid-phase empirical confirmation.

## Open Questions (for Implementation)

1. **Binary decoder target type (CONTEXT.md Q1).**
   - **Research recommendation:** Return `serde_json::Value::Object`. All downstream code (5+ call sites in `handle_sync_command`) is coupled to `serde_json::Value::get(field).as_str()` etc. Introducing `EventPayload` is out-of-scope churn. The hot cost is `from_slice` (the parser), not the `Value` type. Eliminating the parser + intermediate string allocs is the win; the final `Value::Object` layout is identical.

2. **Slow-query `key_preview` under binary format (CONTEXT.md Q2).**
   - **Research finding:** The current code at `tcp.rs:358-372` calls `payload.get(&kf).and_then(|v| v.as_str()).map(|s| ...)`. Because the decoder returns `Value::Object`, this code works unchanged. **No extra decode step needed on the slow-query path.** Cost is identical to v1.1. Confirmed by reading tcp.rs:358-372.

3. **Python struct idiom micro-bench (CONTEXT.md Q3).**
   - **Research recommendation:** Use pre-compiled `struct.Struct` instances (see Pattern 4). Micro-bench during Plan 03 execution if curious; the structural decision to pre-compile is enough.

4. **Flush semantics under server crash (CONTEXT.md Q4).**
   - **Research finding:** If the server crashes mid-async-push, the client's next operation (drain OR flush OR push) will hit `_recv_exact` which returns 0 bytes → raises `ConnectionError("server closed connection")`. The client's `_sock` is cleared, and auto-reconnect will fire on the NEXT call (see `_client.py:107-117`). **The in-flight async pushes are lost** — this is explicit fire-and-forget semantics. Document in the SDK docstring for `push()`.

5. **Event log append timing vs latency recording (CONTEXT.md Q5).**
   - **Research finding:** Event log is fsync'd periodically, not per-push (see `tcp.rs:230-234` — just `log.append(&stream_name, &event_bytes, now)`, no fsync). The existing sync PUSH path already returns BEFORE the event log has hit disk; async PUSH has identical durability semantics. No change needed.

## Sources

### Primary (HIGH confidence — codebase verified)

- `/data/home/tally/src/server/protocol.rs` — parse_command:131, read_string:78, read_json_payload:116, Command enum:22, OP_* constants:9-14
- `/data/home/tally/src/server/tcp.rs` — handle_connection:124, handle_sync_command:198, Push arm:201-375, event log append:230-232, fan-out:268-298, latency:350-372
- `/data/home/tally/src/server/latency.rs` — CommandKind:64, record_push:306, slow_queries_would_accept:317, record_command:322
- `/data/home/tally/python/tally/_protocol.py` — OP_* constants, encode_push:61, parse_response:110
- `/data/home/tally/python/tally/_client.py` — TallyClient.send_command:102, _recv_exact:63, _recv_frame:83, auto-reconnect:107-117
- `/data/home/tally/python/tally/_app.py` — push:82, _send:61
- `/data/home/tally/tests/test_server.rs` — send_frame:81, push frame builder:104-105, OP_PUSH test sites (9)
- `/data/home/tally/benchmark/tally-throughput/bench.py` — current benchmark harness; needs --mode flag
- `.planning/phases/11-fire-and-forget-push/11-CONTEXT.md` — all Decisions, Implementation Hooks, Open Questions

### Secondary (MEDIUM confidence — stdlib knowledge)

- Python `select` module documentation — `select.select([rlist], [], [], 0)` for non-blocking readability probe. Behavior on Windows: **works for sockets**, does not work for other file handles.
- Rust `i64::from_be_bytes` / `u16::from_be_bytes` — stable since 1.32.
- CPython `struct.Struct` — pre-compiled format pattern; documented.

### Tertiary (LOW confidence — marked for validation)

- A1 — pre-compiled struct perf claim. Micro-bench during implementation.
- A3 — BufWriter flush elision enabling kernel batching. Must be verified by the throughput benchmark (the phase gate).
- A4 — GIL not a bottleneck at 100k eps for a single Python thread. Verified by benchmark.

## Metadata

**Confidence breakdown:**
- Codebase hooks (file:line): HIGH — all verified via Read/Grep
- Wire format design: HIGH — specified verbatim in CONTEXT.md D3
- Python encoder idiom: MEDIUM — general pattern is sound, micro-perf claim is assumption A1
- Fire-and-forget BufWriter batching: MEDIUM — this is the single biggest unknown (assumption A3), and the benchmark gate is the acceptance test
- Security: HIGH — surface is small, threats bounded by existing framing limits

**Research date:** 2026-04-11
**Valid until:** 2026-05-11 (30 days — tally is stable, no upstream protocol work expected)

---

*Phase: 11-fire-and-forget-push*
*Research completed: 2026-04-11*
*Ready for planning: yes*
