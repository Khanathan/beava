# Phase 40: Per-stream event-log write lock - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Direct fix (user directive: "please fix it")

<domain>
## Phase Boundary

Replace the single global mutex around `EventLog` with per-stream writer locks. Today every PUSH serializes through `state.event_log: PLMutex<Option<EventLog>>` regardless of stream count or worker thread count — the actual scaling bottleneck (verified in Phase 35-37 benchmarks: 16 worker threads gave zero speedup over 4 because workers all queue on the same mutex).

After this phase, two PUSH operations targeting different streams proceed in parallel; only same-stream concurrent pushes contend.

**In scope:**
- Refactor `EventLog` internal storage to per-stream interior-mutability: `writers: DashMap<String, PLMutex<BufWriter<File>>>`.
- Drop the outer `PLMutex<Option<EventLog>>` wrapper — replace with `Option<Arc<EventLog>>` (event log is set once at startup, never replaced).
- Update all `state.event_log.lock()` call sites in `src/server/tcp.rs` (and elsewhere) to use the new lock-free outer / locked-per-writer pattern.
- Snapshot/flush coordination: iterate DashMap, briefly lock each writer to flush.
- Add a multi-stream benchmark or unit test that proves parallel write scaling.

**Out of scope:**
- Per-key sharding inside a single stream (would need a different key→file mapping; defer to v0.2).
- Change to disk format or wire protocol — purely an in-memory locking refactor.
- Async/await on writer locks — keep `parking_lot::Mutex` blocking semantics; PUSH path already calls into blocking sections.
- Remove the snapshot-time "fsync_all" Phase 35 added — keep, just adapt to iterating writers.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Type changes
- `EventLog.writers`: `AHashMap<String, BufWriter<File>>` → `DashMap<String, parking_lot::Mutex<BufWriter<File>>>`.
- `EventLog::append`, `append_many`, `read_entries`, `fsync_all`, etc.: take `&self` (interior mutability) instead of `&mut self`.
- `ConcurrentAppState.event_log`: `PLMutex<Option<EventLog>>` → `Option<Arc<EventLog>>`. Set once during construction; never `None`-flipped at runtime.
- Snapshot save path that today does `state.event_log.lock()` and reads from inside: now does `if let Some(ref log) = state.event_log { log.fsync_all(); /* iterate writers, drain */ }`.

### Hot path
```rust
// Today (every PUSH):
let mut event_log = state.event_log.lock();             // GLOBAL contention
if let Some(ref mut log) = *event_log {
    log.append(stream_name, ...)?;
}

// After:
if let Some(ref log) = state.event_log {
    log.append(stream_name, ...)?;                      // per-stream contention
}
// Inside append:
//   let writer = self.writers.entry(stream).or_insert_with(...);
//   let mut guard = writer.lock();    // ONLY this stream's writer locked
//   guard.write_all(...)?;
```

### Snapshot/flush semantics
- Snapshot save (Phase 9) and replica `fsync_all` (Phase 35): iterate `writers.iter()`, briefly lock each, flush. Total wall time = sum of per-stream flush latencies (small) instead of one big serialized lock.
- Brief windows where a writer is mid-append while snapshot tries to flush: snapshot waits ~µs for that one writer's lock — invisible.

### Stream creation race
- First PUSH to a new stream may race to insert the writer in DashMap. Use `entry().or_insert_with(open_writer)` with idempotent file-open semantics. Two threads racing both produce the same file path → DashMap dedupes.
- If file open fails (disk full, permissions): return error, don't poison the entry. Subsequent pushes retry.

### Scope guarantees
- Same-stream concurrent pushes still serialize (one writer lock). That's correct — the per-stream log file format requires append ordering.
- Different-stream concurrent pushes parallelize. That's the win.

### Validation
- Existing `cargo test` suite stays green (1170+ tests).
- New unit test: spawn two threads pushing to streams A and B; verify wall time ~= max(A_time, B_time) not A_time + B_time. Time-based assertion has tolerance.
- Re-run `bench_v0.py --matrix --events 30000` — expect SAME numbers as today (single stream → single writer → no parallelism gain on this specific benchmark).
- New mini-benchmark `bench_multistream.py`: push to 4 streams across 8 clients; expect ~3-4x throughput vs single-stream + same client count, with `TALLY_WORKER_THREADS=8`.

### Plan split
- One plan (40-01), three tasks:
  1. EventLog refactor (`src/state/event_log.rs`): interior-mutability + DashMap. Update internal tests.
  2. ConcurrentAppState + tcp.rs call sites: drop outer mutex, update call sites.
  3. Multi-stream benchmark + unit test proving parallel writes scale.

</decisions>

<code_context>
- `src/state/event_log.rs` — EventLog struct + append/append_many/read_entries/fsync_all.
- `src/server/tcp.rs:97` — `pub event_log: PLMutex<Option<EventLog>>` field.
- `src/server/tcp.rs` lines 1058, 1064, 1417, 1418, 1493, 1494, 1829, 1830, 1863, 1864, 1874 — call sites that lock + unwrap.
- `src/server/replica_client.rs` (Phase 36) and `src/server/replica.rs` LOG_FETCH handler (Phase 35) — also use event_log; need updating.
- `src/state/snapshot.rs` — snapshot-save path may interact with event_log for HWM tracking.
- Phase 35 added an `fsync_all` call before LOG_FETCH reads — keep semantically; adapt to per-writer iteration.

</code_context>

<specifics>
- `parking_lot::Mutex` is faster than `std::sync::Mutex` and used elsewhere in the codebase already — use it.
- DashMap shard count default is 16 — fine for stream-name-keyed lookups.
- Snapshot file format: no change. The HWM tracking in `SnapshotHeader.sequence` stays as-is (snapshot file counter, not event-log seq).
- `EventLog::take()` (used in some shutdown paths if any): becomes a no-op or removed; with `Option<Arc<EventLog>>` you just drop the Arc.
- Tests that explicitly mock or replace the event log mid-test: rewrite to set up the desired state at construction.

</specifics>

<deferred>
- Per-key sharding within a single stream (needed only if a single stream becomes hot; v0.2 if anyone asks).
- Async-friendly writer locks (would need `tokio::sync::Mutex`, more complex; defer until benchmark says it's needed).
- `read_entries` parallelism (rare — it's a cold path called by LOG_FETCH).
- Lock-free SPSC queues per stream (huge complexity for marginal gain).

</deferred>

---

*Phase: 40-per-stream-write-lock*
*Source: user directive 2026-04-15 — "please fix it" after benchmark showed 16 workers = no speedup due to global event-log mutex*
