---
phase: 08-backfill-schema-evolution
reviewed: 2026-04-09T00:00:00Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - python/tally/_operators.py
  - src/engine/pipeline.rs
  - src/main.rs
  - src/server/http.rs
  - src/server/protocol.rs
  - src/server/tcp.rs
  - src/state/snapshot.rs
  - src/state/store.rs
  - tests/test_pipeline.rs
findings:
  critical: 1
  warning: 4
  info: 3
  total: 8
status: issues_found
---

# Phase 08: Code Review Report

**Reviewed:** 2026-04-09
**Depth:** standard
**Files Reviewed:** 9
**Status:** issues_found

## Summary

This phase introduces backfill (event-log replay for new features added to existing streams) and schema evolution (diff-based registration detecting added/removed/unchanged features). The implementation is solid overall — the snapshot versioning, cooperative yielding in the backfill loop, and crash-recovery re-spawn logic are well thought out. However, one critical integer overflow hazard was found in the binary protocol frame encoder, and several logic gaps exist in the backfill and schema-evolution paths that could cause incorrect state or silent data loss in edge cases.

## Critical Issues

### CR-01: Integer overflow in `encode_frame` / `encode_response` truncates large payloads silently

**File:** `src/server/protocol.rs:34,67`
**Issue:** Both `encode_frame` and `encode_response` compute `length = 1u32 + payload.len() as u32`. In Rust debug builds this panics on overflow, but in release builds it wraps silently. If a PUSH response or MGET response for many keys exceeds ~4 GB, the length header will be wrong and the client will read a truncated or corrupted frame. More practically, a MGET response for a very large number of keys, or a debug key response with large operator state, can plausibly exceed 4 GB if the feature map is large or operator Debug output is verbose. The cast `payload.len() as u32` alone overflows for payloads above 4,294,967,295 bytes, and the `+ 1` adds one more opportunity.

**Fix:**
```rust
pub fn encode_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let payload_len = u32::try_from(payload.len())
        .expect("payload exceeds u32::MAX; frame too large for protocol");
    let length = 1u32.checked_add(payload_len)
        .expect("frame length overflow");
    let mut buf = Vec::with_capacity(4 + length as usize);
    buf.extend_from_slice(&length.to_be_bytes());
    buf.push(opcode);
    buf.extend_from_slice(payload);
    buf
}
```
Apply the same pattern to `encode_response`. In production you may prefer returning `Result<Vec<u8>, TallyError>` and propagating the error instead of panicking.

---

## Warnings

### WR-01: `diff_features` does not detect backfill flag on re-registered unchanged features

**File:** `src/engine/pipeline.rs:127-143`
**Issue:** When a stream is re-registered, `diff_features` classifies features that exist in both old and new definitions as `unchanged`. The `backfilling` list is only populated for *new* (added) features. However, a user may re-register an existing feature with `backfill: true` added (e.g., to trigger a retroactive replay after discovering the event log was populated). In that case, the feature ends up in `unchanged`, not `backfilling`, and no backfill is spawned. This is a silent no-op — the user gets a diff response showing `backfilling: []` even though they explicitly set `backfill: true`. The correct behavior should either populate `backfilling` for unchanged features that now have `backfill: true`, or explicitly document and reject this usage.

**Fix:**
```rust
// In the unchanged branch of diff_features:
unchanged.push(name.to_string());
// Also check backfill flag on re-registration
if get_backfill_flag(new_def) && !get_backfill_flag(old_def) {
    backfilling.push(name.to_string());
}
```

### WR-02: Backfill spawned inside the mutex lock in `handle_sync_command`

**File:** `src/server/tcp.rs:316-324`
**Issue:** `tokio::spawn(run_backfill(...))` is called while `app` (the mutex guard) is still live in scope. Although `tokio::spawn` itself returns immediately without blocking, the guard is dropped only at the end of the `else` block — after `tokio::spawn` is called. On a current-thread executor, this is safe in practice because spawned tasks cannot run until the current task yields. However it is a correctness anti-pattern: if the executor changes or if the code is refactored to use a multi-thread executor, the backfill task could acquire the lock before the registration path finishes writing `backfill_complete` to the snapshot. This is the same issue that `main.rs` correctly handles (drops `app` before spawning).

**Fix:** Drop the mutex guard explicitly before calling `tokio::spawn`:
```rust
// Inside the backfill spawning block, after building `status`:
let state_clone = state.clone();
let backfill_stream = def_name.clone();
let backfill_features = diff.backfilling.clone();
drop(app); // Release lock before spawn
tokio::spawn(run_backfill(
    state_clone,
    backfill_stream,
    backfill_features,
    entries,
    status,
));
```

### WR-03: `Vec::with_capacity(count)` with untrusted `count` may cause excessive allocation

**File:** `src/server/protocol.rs:154, 189`
**Issue:** Both MSET and MGET parse a `count` from the wire as `u32::from_be_bytes(...)`, then immediately call `Vec::with_capacity(count)` where `count` can be up to `u32::MAX` (~4 billion). A malformed or malicious client can send a 4-byte count of `0xFFFFFFFF` followed by no data, causing the server to attempt to allocate ~32 GB (for MSET) or ~8 GB (for MGET) of memory — effectively a denial-of-service crash. The payload length is not validated against `count` before allocation.

**Fix:** Cap the pre-allocation at a reasonable upper bound, and rely on lazy growth for legitimate large inputs:
```rust
let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
buf = &buf[4..];
// Pre-allocate at most 4096 entries; Vec will grow as needed
let mut entries = Vec::with_capacity(count.min(4096));
```

### WR-04: `create_pipeline` in `http.rs` ignores the `SchemaDiff` (no backfill via HTTP)

**File:** `src/server/http.rs:147`
**Issue:** The HTTP `POST /pipelines` handler calls `app.engine.register(stream_def).map(|_diff| ())` — it discards the `SchemaDiff` and never spawns a backfill task. This means backfill is silently skipped for pipelines registered via the HTTP management API. Clients registering pipelines via HTTP with `backfill: true` features will receive `{"status": "ok"}` but no replay will occur. The TCP REGISTER handler correctly uses the diff to spawn backfill.

**Fix:** Mirror the TCP handler's backfill logic in `create_pipeline`, or explicitly return a 400 if any feature has `backfill: true` when registering via HTTP (to tell users to use the TCP REGISTER command for backfill-capable registrations):
```rust
// After convert_register_request:
let diff = app.engine.register(stream_def)?;
// Register event log
if let Some(ref mut log) = app.event_log {
    let history_ttl = app.engine.get_stream(&def_name).and_then(|s| s.history_ttl);
    let _ = log.register_stream(&def_name, history_ttl);
}
app.engine.store_raw_register_json(&def_name, body);
// Spawn backfill if needed (same logic as tcp.rs handle_sync_command)
if !diff.backfilling.is_empty() {
    // ... spawn backfill task
}
```

---

## Info

### IN-01: `clone_for_snapshot` (without GC) is dead code

**File:** `src/state/store.rs:235-252`
**Issue:** `StateStore::clone_for_snapshot` exists alongside `clone_for_snapshot_with_gc`. Every call site in `main.rs` and `http.rs` uses `clone_for_snapshot_with_gc`. The non-GC variant is never called in production paths — only a test exercises it indirectly. This is unused code that will confuse future maintainers about which method to use.

**Fix:** Remove `clone_for_snapshot` and update the one test that calls it to use `clone_for_snapshot_with_gc` with an appropriate `valid_features` map, or add a `#[cfg(test)]` gate to the function.

### IN-02: Magic memory estimate `2048` appears in two places

**File:** `src/server/http.rs:204, 301`
**Issue:** The memory estimate `keys_total * 2048` is duplicated in `metrics_endpoint` and `debug_memory`. If the estimate is revised, it must be updated in two places. The comment says "Rough estimate: ~2KB per entity with operators."

**Fix:** Extract to a named constant at the top of `http.rs`:
```rust
const ESTIMATED_BYTES_PER_ENTITY: usize = 2048;
```

### IN-03: `to_visit.contains(stream_in_order)` is O(n) linear scan in the cascade loop

**File:** `src/engine/pipeline.rs:591`
**Issue:** In `push_with_cascade`, the topological-order loop calls `to_visit.contains(stream_in_order)` on a `Vec`. Since `to_visit` holds all reachable downstream streams, this is an O(n) scan repeated for every stream in the topological order. For wide DAGs this is quadratic in the number of streams. The `visited` `AHashSet` already tracks the same information but is only used during BFS construction, not during execution.

**Fix:** Reuse `visited` (which already holds all reachable downstream streams) as the membership check during the topological execution loop:
```rust
// Replace:
if !to_visit.contains(stream_in_order) {
    continue;
}
// With:
if !visited.contains(stream_in_order.as_str()) {
    continue;
}
```
Note `visited` also contains `stream_name` (the origin), which is correctly skipped because `push_with_cascade` calls `self.push` for the origin before the loop.

---

_Reviewed: 2026-04-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
