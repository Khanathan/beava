---
phase: 02-tcp-server-and-binary-protocol
reviewed: 2026-04-09T00:00:00Z
depth: standard
files_reviewed: 7
files_reviewed_list:
  - src/main.rs
  - src/server/http.rs
  - src/server/mod.rs
  - src/server/protocol.rs
  - src/server/tcp.rs
  - src/types.rs
  - tests/test_server.rs
findings:
  critical: 1
  warning: 3
  info: 3
  total: 7
status: issues_found
---

# Phase 02: Code Review Report

**Reviewed:** 2026-04-09T00:00:00Z
**Depth:** standard
**Files Reviewed:** 7
**Status:** issues_found

## Summary

Reviewed the TCP server, binary protocol, HTTP management stub, types module, and integration tests for Phase 2. The overall implementation is well-structured: the frame parsing is correct, cooperative MSET yielding is properly implemented, the Mutex poisoning recovery path is thoughtful, and test coverage is thorough.

One critical issue exists: `write_string` panics on oversized input instead of returning an error, which could crash a Tokio task if called with externally-controlled data. Three warnings cover silent error suppression in connection handling, an integer overflow risk in frame encoding, and a technically unsound `u128`-to-`u64` truncation. Three info items cover a memory leak in tests, an unused parameter in the HTTP API, and an undocumented `.unwrap()` on the hot path.

## Critical Issues

### CR-01: `write_string` panics instead of returning an error on oversized input

**File:** `src/server/protocol.rs:97-104`
**Issue:** `write_string` uses `assert!` which panics if the input string exceeds `u16::MAX` bytes. While the function is currently only called from client-side test helpers (not the server's receive path), it is a public API function. Any future server-side use — such as serializing a stream name from a REGISTER payload back to a response — would allow a crafted request to panic the Tokio task handling that connection. The existing `#[should_panic]` test at line 819 validates the panic behavior, which would also need updating.

**Fix:**
```rust
// Before (panics):
pub fn write_string(s: &str) -> Vec<u8> {
    assert!(
        s.len() <= u16::MAX as usize,
        "string too long for protocol: {} bytes (max {})",
        s.len(),
        u16::MAX
    );
    let len = s.len() as u16;
    let mut buf = Vec::with_capacity(2 + s.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

// After (returns Result):
pub fn write_string(s: &str) -> Result<Vec<u8>, TallyError> {
    if s.len() > u16::MAX as usize {
        return Err(TallyError::Protocol(format!(
            "string too long for protocol: {} bytes (max {})",
            s.len(),
            u16::MAX
        )));
    }
    let len = s.len() as u16;
    let mut buf = Vec::with_capacity(2 + s.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
    Ok(buf)
}
```
Update the `#[should_panic]` test at `tests/test_server.rs:819` to assert `result.is_err()` instead.

## Warnings

### WR-01: All connection errors are silently swallowed — no logging

**File:** `src/server/tcp.rs:42-46`
**Issue:** The spawned connection handler discards all errors with an empty comment:
```rust
if let Err(_e) = handle_connection(stream, state).await {
    // Connection closed or error -- debug log only
}
```
The comment says "debug log only" but there is no logging. Non-EOF I/O errors, protocol parse failures that propagate out, and any panics-turned-errors from the engine will all be silently dropped. This makes diagnosing production failures very difficult.

**Fix:**
```rust
tokio::spawn(async move {
    if let Err(e) = handle_connection(stream, state).await {
        // Suppress UnexpectedEof (clean client disconnect) to avoid log spam.
        // Log everything else so operator can diagnose failures.
        let is_clean_disconnect = e
            .downcast_ref::<std::io::Error>()
            .map(|ioe| ioe.kind() == std::io::ErrorKind::UnexpectedEof)
            .unwrap_or(false);
        if !is_clean_disconnect {
            eprintln!("[tcp] connection error: {}", e);
        }
    }
});
```

### WR-02: Integer overflow in `encode_frame` and `encode_response` for large payloads

**File:** `src/server/protocol.rs:31` and `src/server/protocol.rs:64`
**Issue:** Both functions compute the frame length as:
```rust
let length = 1u32 + payload.len() as u32;
```
On 64-bit systems, `payload.len()` is a `usize`. Casting to `u32` truncates silently if `payload.len() > u32::MAX`. Additionally, the `+ 1u32` wraps silently in release mode if `payload.len() as u32 == u32::MAX`. The receive path rejects frames over 64 MB, but response encoding has no such guard. A very large engine response (e.g., a key with thousands of features) could silently produce a malformed frame.

**Fix:**
```rust
pub fn encode_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let payload_len = u32::try_from(payload.len())
        .expect("encode_frame: payload exceeds u32::MAX");
    let length = payload_len.checked_add(1)
        .expect("encode_frame: length overflow");
    let mut buf = Vec::with_capacity(4 + length as usize);
    buf.extend_from_slice(&length.to_be_bytes());
    buf.push(opcode);
    buf.extend_from_slice(payload);
    buf
}
```
Apply the same pattern to `encode_response`.

### WR-03: Implicit `u128`-to-`u64` truncation in `default_bucket`

**File:** `src/server/protocol.rs:262`
**Issue:** `Duration::as_nanos()` returns `u128`. After dividing by 30, the result is cast silently to `u64`:
```rust
let bucket_nanos = window.as_nanos() / 30;  // u128
let bucket = std::time::Duration::from_nanos(bucket_nanos as u64); // silent truncation
```
In practice, this only overflows for windows exceeding ~17,520 years (since `u64::MAX` nanoseconds is ~584 years and the window must be 30x that to overflow after the division). This is an impossible input in practice, but the code is technically unsound. A `u64::try_from` would make the intent explicit and catch the impossible case cleanly.

**Fix:**
```rust
let bucket_nanos: u64 = u64::try_from(window.as_nanos() / 30)
    .unwrap_or(u64::MAX); // saturating fallback; only possible for absurd windows
let bucket = std::time::Duration::from_nanos(bucket_nanos);
```

## Info

### IN-01: `Box::leak` in integration test causes intentional memory leak

**File:** `tests/test_server.rs:302-304`
**Issue:** The MSET bulk write test leaks 2048 strings to work around the `&str` lifetime requirement:
```rust
let key: &str = Box::leak(format!("k{}", i).into_boxed_str());
```
This is unnecessary — the `build_mset_payload` function could accept `&[(impl AsRef<str>, serde_json::Value)]` or `&[(String, serde_json::Value)]` instead of `&[(&str, serde_json::Value)]`.

**Fix:** Change the test helper signature:
```rust
fn build_mset_payload(entries: &[(String, serde_json::Value)]) -> Vec<u8> { ... }

// Test becomes:
let entries: Vec<(String, serde_json::Value)> = (0..2048)
    .map(|i| (format!("k{}", i), serde_json::json!({"score": i})))
    .collect();
let mset_payload = build_mset_payload(&entries);
```

### IN-02: `_state` parameter is accepted but not used in HTTP server functions

**File:** `src/server/http.rs:16` and `src/server/http.rs:26`
**Issue:** Both `run_http_server` and `run_http_server_with_listener` accept `_state: SharedState` but do not pass it to any handler. The `_` prefix acknowledges this, but callers may assume the state is wired up. A comment clarifying that this is intentionally deferred to Phase 4 would prevent confusion.

**Fix:** Add an explicit scaffolding comment:
```rust
// _state: shared with TCP server; not used until Phase 4 (pipeline CRUD endpoints).
pub async fn run_http_server(addr: &str, _state: SharedState) -> Result<(), std::io::Error> {
```

### IN-03: Undocumented `.unwrap()` in hot-path serialization function

**File:** `src/types.rs:57`
**Issue:**
```rust
serde_json::to_vec(&serde_json::Value::Object(map)).unwrap()
```
Serializing a `serde_json::Value::Object` constructed from known-good types is infallible at runtime, but using `.unwrap()` without a comment makes it look like a potential panic site during code review and future refactoring.

**Fix:** Replace with `expect` and a documenting comment:
```rust
serde_json::to_vec(&serde_json::Value::Object(map))
    .expect("serializing a serde_json::Value::Object with scalar values is infallible")
```

---

_Reviewed: 2026-04-09T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
