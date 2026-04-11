---
phase: 11-fire-and-forget-push
status: findings
date: 2026-04-11
depth: standard
files_reviewed: 11
findings_total: 10
findings_by_severity:
  critical: 0
  high: 2
  medium: 2
  low: 3
  info: 3
---

# Phase 11 — Code Review

**Scope:** 11 source files changed in Phase 11 (Fire-and-Forget PUSH + Binary Wire Protocol).

**Files reviewed:**
- `benchmark/tally-throughput/bench.py`
- `python/tally/_app.py`
- `python/tally/_client.py`
- `python/tally/_protocol.py`
- `python/tests/test_app.py`
- `python/tests/test_client.py`
- `python/tests/test_integration.py`
- `python/tests/test_protocol.py`
- `src/server/protocol.rs`
- `src/server/tcp.rs`
- `tests/test_server.rs`

**Summary:** No blocking critical issues. The 166k eps benchmark is safe. However, two **High-severity correctness bugs** in the Python SDK drain model are latent until the server starts sending error frames under load — they must be fixed before Phase 11 is used in production with failing pushes.

---

## Findings

### H-1 — `drain_errors_nonblock` is not actually non-blocking

**Severity:** High
**File:** `python/tally/_client.py:126-143`
**Category:** Correctness / performance regression

`select.select([sock], [], [], 0)` only indicates that *some* bytes are readable, not that a complete frame is available. The subsequent `self._recv_frame()` call does blocking `_recv_exact(4)` + `_recv_exact(length)` reads that can stall up to the socket timeout (5s) if a partial frame header/body is in flight from the kernel.

Under heavy pipelined async load with occasional server error frames, the "drain" on every `push()` can introduce unexpected multi-millisecond stalls on the hot path, directly undermining PERF-01.

**Fix options:**
- Switch the socket to non-blocking for the duration of the drain, falling back to `_pending_error` storage if a frame is only partially available.
- Add `MSG_PEEK` check for a full 4-byte header before committing to `_recv_frame`, then loop `select(..., 0)` between header and body reads.
- Simpler: set `sock.setblocking(False)`, read until `BlockingIOError`, buffer partial frames, then restore blocking mode.

### H-2 — Multiple pending async error frames corrupt sync request/response pairing

**Severity:** High
**Files:** `python/tally/_client.py:106-144`, `python/tally/_app.py:63-71`
**Category:** Correctness

`drain_errors_nonblock` reads **at most one** frame per call, but the server can queue multiple `STATUS_ERROR` frames (e.g., N bad async pushes in a row before a drain). When the user then calls `push_sync`/`get`/`mget`, `TallyClient.send_command` calls `_recv_frame()` unconditionally after `_send_frame()` and will read a **stale error frame** as its response.

Result: the sync call raises a `ProtocolError` that actually belongs to a prior async push, and the genuine sync response is still in the buffer to become the next call's response — **persistent off-by-one frame desync**.

**Fix options:**
- Have `drain_errors_nonblock` loop until `select` reports not-ready, draining all pending frames. Collect errors into `_pending_error` (or a queue) and raise the first.
- Track an in-flight-response counter so `send_command` knows how many stray frames to skip.

---

### M-1 — `self._pending_error` is dead code

**Severity:** Medium
**File:** `python/tally/_client.py:47, 119-121`
**Category:** Incomplete implementation / maintainability

The field is initialized to `None`, read at the top of `drain_errors_nonblock`, and never assigned anywhere else in the SDK. Plan 11-02 specifies a deferred-error pattern for when draining needs to postpone raising, but the actual implementation raises inline. This is a latent bug surface: the fix for H-2 likely requires populating `_pending_error`, but right now the scaffolding is half-wired and will mislead future readers.

**Fix:** Either remove `_pending_error` entirely, or wire it up as part of the H-2 fix.

### M-2 — `send_frame_no_recv` auto-reconnect can duplicate async pushes

**Severity:** Medium
**File:** `python/tally/_client.py:146-159`
**Category:** Data integrity

Auto-reconnect on `OSError` in the async send path can **duplicate or silently drop** an async push: if `sendall` partially wrote before raising, the retry on a fresh connection re-sends the full frame (duplicate event) or the original bytes reach the old server and are lost on reconnect.

T-11-12 "accepts" silent loss, but the **duplicate-event case is not documented** and violates idempotency assumptions for counters/sums (a single duplicate push doubles the count contribution).

**Fix:** Document the at-least-once semantic explicitly in the SDK docstring, OR use a sequence number / retry-token to let the server dedupe.

---

### L-1 — Missing u16::MAX length validation in `encode_push_binary`

**Severity:** Low
**File:** `python/tally/_protocol.py:104-141`
**Category:** Error contract

`encode_push_binary` does not validate `stream_name`, `key`, or string-value byte-length against `u16::MAX` (65535) before `_U16.pack(len(...))`. Oversized inputs raise a raw `struct.error` instead of a typed `ProtocolError`, breaking the SDK's error contract.

**Fix:** Check `len(bytes) <= 65535` and raise `ProtocolError(f"{field} exceeds 65535 bytes")` otherwise.

### L-2 — `decode_event_binary` unbounded `Map::with_capacity` allocation

**Severity:** Low
**File:** `src/server/protocol.rs:156`
**Category:** Defense in depth

Allocates `Map::with_capacity(field_count)` from an attacker-controlled `u16`. Bounded at ~65535 entries (a few MB), so not a practical DoS vector, but worth gating against `buf.len() / 4` as a cheap upper bound since each field needs ≥4 bytes (1 tag + 2 length + 1 key).

**Fix:** `let cap = (field_count as usize).min(buf.len() / 4); Map::with_capacity(cap);`

### L-3 — Partial elimination of JSON on PUSH hot path

**Severity:** Low
**File:** `src/server/tcp.rs:260, 267, 313`
**Category:** Performance observation

`handle_push_core` re-serializes the binary-decoded payload to JSON (`serde_json::to_vec(payload)`) for the event log on every push. PERF-02 removed the parse cost but left the serialize cost on the hot path — partial rather than total elimination of JSON from PUSH.

**Fix:** Either write the binary wire bytes directly to the event log (requires log format change) or defer to a later phase. Note for v1.3 perf work.

---

### I-1 — Duplicate field keys silently last-wins

**Severity:** Info
**File:** `src/server/protocol.rs:203`

Duplicate field keys in the binary event payload are silently last-wins via `map.insert`; no warning, no error. Matches JSON object semantics but worth documenting on the wire format.

### I-2 — Defensive `unreachable!` arm in `handle_sync_command`

**Severity:** Info
**File:** `src/server/tcp.rs:548`

`Command::PushAsync | Command::Flush => unreachable!()` is correct defensive code and stays green as long as `handle_connection` intercepts first. No issue, noted for future refactors — add a comment explicitly tying it to the `handle_connection` dispatch invariant.

### I-3 — Flush invariant for BufWriter on disconnect

**Severity:** Info
**File:** `src/server/tcp.rs:194-212`

Response dispatch correctly flushes on sync OK and on all errors, and elides flush only on async `Ok(None)`. BufWriter on clean disconnect will not flush pending bytes, but since sync Ok and Err always flush immediately, and async Ok writes nothing, there are no unflushed event acks at drop time. Behavior is correct; the invariant deserves an explicit comment block.

---

## Recommendations

1. **Fix H-1 and H-2 together** — both are drain-path correctness bugs and share scaffolding. Suggested approach: loop the drain until the socket has no more readable bytes, use non-blocking I/O for partial-frame handling, and populate `_pending_error` as the first-error sink.
2. **Then M-1 falls out naturally** — fixing H-2 wires `_pending_error` up.
3. **M-2, L-1, L-2** are safe cleanups that can ship together.
4. **L-3** is a note for v1.3 performance work, not a phase-11 fix.
