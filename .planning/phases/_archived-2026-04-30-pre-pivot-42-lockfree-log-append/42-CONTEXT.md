# Phase 42: Lock-free event-log append (O_APPEND atomic) - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Direct fix — scope limited to log append path

<domain>
## Phase Boundary

Remove the per-stream `Mutex<BufWriter<File>>` from the event-log append path. Replace with `O_APPEND` + atomic `write()` syscalls — relying on Linux's kernel-level atomic append guarantee for concurrent writers to the same file.

Cascade + operator state mutation is already lock-free per key (via DashMap). The log append is the last serialization point. Measured: single-stream 8-process batched pushes cap at ~540k eps entirely due to this Mutex.

**In scope:**
- Replace `writers: DashMap<String, PLMutex<BufWriter<File>>>` with `DashMap<String, LockFreeStreamLog>`.
- `LockFreeStreamLog` wraps an O_APPEND file descriptor; `append_raw(&self, bytes)` is one atomic `write()` syscall.
- Handle partial-write edge case with a per-stream fallback Mutex (cold path; 99.99% of calls skip).
- Preserve batch-atomic semantics: `append_many` concatenates all events into ONE buffer, one `write()` call.
- `fsync_all` remains; now calls `fsync(fd)` per stream FD, still correct.

**Out of scope:**
- Operator cascade restructure (still runs synchronously on push thread; already parallel per key via DashMap).
- Per-operator shard executor (deferred — not needed per measurement; operator cascade is fast).
- Windows support (not a target; Hetzner = Linux).
- Event ordering semantics changes (event-time + watermark already handle this; Phase 24).
- BufWriter-style explicit buffering (`O_APPEND` + one syscall per append is simpler; Linux kernel coalesces writes to page cache anyway).

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
Kernel atomicity gives us lock-freeness for free. Don't reinvent it in userspace.

### Type
```rust
pub struct LockFreeStreamLog {
    fd: OwnedFd,
    stream_name: String,  // for error messages
    // Partial-write fallback — rarely used
    partial_write_lock: PLMutex<()>,
}
```

Created via `File::options().create(true).write(true).append(true).open(path)` then `.into()` to OwnedFd.

### `append_raw` hot path
```rust
fn append_raw(&self, bytes: &[u8]) -> io::Result<()> {
    let n = unsafe { libc::write(self.fd.as_raw_fd(), bytes.as_ptr() as *const _, bytes.len()) };
    if n < 0 { return Err(io::Error::last_os_error()); }
    if (n as usize) == bytes.len() { return Ok(()); }
    // Partial write — rare. Take fallback mutex + write remainder.
    self.append_raw_partial_fallback(bytes, n as usize)
}
```

### `append` semantics preserved
Existing signature:
```rust
pub fn append(&self, stream_name: &str, event_bytes: &[u8], now: SystemTime) -> io::Result<bool>
```
Body:
1. DashMap lookup — lock-free.
2. Encode `LogEntry { timestamp, payload }` via postcard.
3. Build single contiguous frame buffer `[u32 BE len][postcard bytes]`.
4. Call `log.append_raw(&frame)` — one syscall, lock-free.
5. Return `Ok(true)`.

### `append_many` batch-atomic
1. Concatenate ALL events' frames into one buffer.
2. One `write()` syscall for the whole batch.
3. Either whole batch lands atomically at end of file, OR partial-write fallback triggers — readers see complete frames via the length-prefix protocol either way.

### `fsync_all` — per-fd fsync
`self.writers.iter().for_each(|entry| unsafe { libc::fdatasync(entry.value().fd.as_raw_fd()); })`. No lock. Each stream's fd is independently fsyncable.

### Partial-write behavior
Linux `write()` on `O_APPEND` normally completes fully. Partial writes can occur under:
- Signal interrupt during the syscall (rare).
- Disk quota hit mid-write (user misconfig).
- EINTR (handled by loop).

Fallback: take `partial_write_lock`, write remainder in a loop. Frames never interleave with other threads because the partial-write case is caught before another thread's write completes (Linux guarantees `i_mutex` held during the full write syscall).

### Large frames (> ~1 MB)
POSIX atomicity for `O_APPEND` is only guaranteed for ≤ PIPE_BUF (4 KB). Linux extends this to much larger in practice via `i_mutex`, but to be safe:
- Typical Tally event: 100-500 B postcard-encoded → always atomic.
- Typical batch (1000 events × ~300 B avg): ~300 KB per batch → atomic on Linux.
- Batches > 1 MB: fall back to partial_write_lock for the whole write. Easy size check at call site.

### Ordering semantics unchanged
- Per-connection sequential order: preserved (connection handler issues writes sequentially).
- Cross-connection same-stream: was arbitrary (lock race), remains arbitrary (kernel scheduling).
- Operator correctness: event-time + watermark (Phase 24) handles this; log arrival order irrelevant.

### Tests
- Unit test: two threads spawn, each append 10k frames to the same stream concurrently. Verify file decodes cleanly (no torn frames) and has exactly 20k entries.
- Regression: existing `event_log` tests continue to pass.
- Bench: re-run 8-proc 1-stream batched bench. Expect > 1.5M eps aggregate (up from 540k).

</decisions>

<code_context>
- `src/state/event_log.rs` — contains `EventLog` + `append` + `append_many` + `fsync_all`.
- `DashMap<String, PLMutex<BufWriter<File>>>` — today's value type. Replace Mutex+BufWriter with new struct.
- Phase 6 log entry format: `[u32 BE length prefix][postcard LogEntry bytes]` — unchanged.
- Phase 35 LOG_FETCH reader: reads sequentially, decodes via length prefix — unchanged.
- `libc` is already a transitive dep via tokio/nix. Confirm in Cargo.toml; add if missing.
- Existing `OwnedFd` / `AsRawFd` from std.

</code_context>

<specifics>
- `File::options().append(true)` opens with `O_APPEND` implicitly; `write()` on the resulting fd always appends.
- Can't use Rust `std::fs::File::write_all` because it uses a loop that could interleave on partial writes. Use `libc::write` directly, one syscall per call.
- Cargo feature gate: none. Lock-free is the default; old behavior not kept.
- Snapshot interaction: snapshot save calls `fsync_all` via AppState ref. Still works — we just sync each fd independently.
- `debug_assert!(bytes.len() < 1_048_576)` gate for "frame too large, reconsider batch size" — not hit in current bench but good invariant to surface if it ever is.

</specifics>

<deferred>
- Per-operator shard executor (Phase 43 if cascade becomes the next bottleneck after this).
- io_uring for batched appends (marginal; sub-MB writes fit in one syscall cheaply).
- Windows support (never in scope for v0).
- File-system-specific tuning (O_DIRECT, fadvise) — defer until prod pressure demands.

</deferred>

---

*Phase: 42-lockfree-log-append*
*Source: user directive 2026-04-15 — "yes please [plan+implement]"; measured bench ceiling of 540k/stream due to per-stream log Mutex; event-time+watermark obviates arrival-order concerns*
