# Phase 15: Snapshot I/O off main thread - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning
**Mode:** Auto — adapted from ROADMAP (per-shard references updated to match actual Phase 14 architecture)

<domain>
## Phase Boundary

Move snapshot serialization and disk I/O off the main event-loop thread using `tokio::task::spawn_blocking`. Currently, the periodic snapshot holds the `StateStore` lock for the entire serialize + write + fsync duration, stalling all PUSH/GET/SET operations. After this phase, the lock is held only during a fast clone of dirty state, then released — serialization and disk I/O happen on a blocking thread pool without holding any locks.

**Architecture adaptation:** ROADMAP Phase 15 assumed Phase 14 delivered per-shard state with shard-local snapshot files. Phase 14 actually delivered `ConcurrentAppState` with a single `PLMutex<StateStore>`. This simplifies Phase 15: instead of coordinating N shard snapshot writes, we do a single clone-and-spawn pattern.

**In scope:**
- `spawn_blocking` for snapshot serialization + write + fsync
- Clone dirty state under lock, release lock, then serialize off-thread
- Snapshot cycle serialization (never start new cycle while previous is writing — H-4)
- Crash recovery for partially-written snapshots
- PUSH throughput regression ≤5% during snapshot write (vs 15-25% on v1.2)
- `POST /snapshot?wait=true&timeout_ms=N` HTTP endpoint
- Cleanup old snapshot files
- Stress test: sustained PUSH load DURING snapshot write

**Out of scope:**
- Per-shard parallel snapshot writes (requires full sharding from future milestone)
- Manifest-based multi-file commit protocol (single file snapshot, not sharded)
- Changes to snapshot format (still bincode + base/delta)

</domain>

<decisions>
## Implementation Decisions

### Off-Thread Pattern
- **D-01:** Acquire `StateStore` lock → clone dirty entities + snapshot metadata → release lock → `spawn_blocking(|| serialize_and_write(cloned_state))`
- **D-02:** The clone step should be O(dirty_keys) not O(total_keys) — clone only entities in the dirty set
- **D-03:** Use existing `tokio::task::spawn_blocking` — no new crates, no new thread pool
- **D-04:** `max_blocking_threads` configuration: default to 2 (one for active snapshot, one spare for manual trigger)

### Cycle Serialization (H-4)
- **D-05:** Never start a new snapshot cycle while the previous one is still writing
- **D-06:** Track in-flight snapshot via `AtomicBool` or `tokio::sync::Semaphore(1)` — cheap, no lock
- **D-07:** If snapshot interval fires while previous cycle is active: skip, increment `snapshots_skipped` metric
- **D-08:** `/metrics` exposes `snapshots_skipped` counter — alert if > 0

### Crash Recovery
- **D-09:** Write to `.tmp` file → fsync → rename to final → fsync parent dir
- **D-10:** On startup: if `.tmp` file exists without corresponding final file → incomplete write → ignore (roll back to previous snapshot)
- **D-11:** Preserve existing base + delta snapshot format and recovery logic

### HTTP Endpoint
- **D-12:** `POST /snapshot?wait=true&timeout_ms=N` — synchronous snapshot trigger
- **D-13:** Returns `200 {bytes, duration_ms}` on success, `408` on timeout, `409` if cycle already in progress

### Benchmarking
- **D-14:** Stress test: sustained PUSH load that DELIBERATELY overlaps a snapshot cycle
- **D-15:** PUSH throughput during snapshot must regress ≤5% (was 15-25% in v1.2)
- **D-16:** Snapshot write time budget: <1 second per 100k entities

### Claude's Discretion
- Exact clone strategy (deep clone vs Arc-swap)
- Whether to use `oneshot` channel for completion notification
- Cleanup file patterns
- Test file organization

</decisions>

<canonical_refs>
## Canonical References

### Current Snapshot Code
- `src/state/snapshot.rs` — current serialize/deserialize, base + delta logic
- `src/main.rs` — periodic snapshot timer, `save_incremental_snapshot` call site
- `src/server/http.rs` — existing `POST /snapshot` handler (if any)

### Phase 14 Architecture
- `src/server/tcp.rs` — `ConcurrentAppState`, `SharedState = Arc<ConcurrentAppState>`
- `src/state/store.rs` — `StateStore` with `PLMutex` wrapper, dirty_keys tracking

### Requirements
- `.planning/REQUIREMENTS.md` — OPS-05 (non-blocking snapshot write)
- `.planning/ROADMAP.md` §"Phase 15"
- `.planning/research/PITFALLS.md` — H-4 (dirty-set backpressure), H-5 (fsync ordering), C-3 (partial write)

</canonical_refs>

<code_context>
## Existing Code Insights

### Current Snapshot Path
- Periodic timer in main.rs calls snapshot function while holding the global state lock
- Serialization uses bincode/serde — can be expensive for large state
- Base + delta incremental snapshots (Phase 9)
- Dirty-key tracking determines which entities go into delta snapshots

### Integration Points
- `main.rs` snapshot timer — replace blocking snapshot with clone + spawn_blocking
- `snapshot.rs` serialize functions — adapt to accept cloned state (owned, not borrowed)
- `http.rs` POST /snapshot — add `wait=true&timeout_ms=N` variant
- `ConcurrentAppState.store` — PLMutex lock for clone step

</code_context>

<specifics>
## Specific Ideas

The simplest implementation: the snapshot timer acquires the store lock, calls `dirty_keys.iter().map(|k| (k.clone(), entities[k].clone())).collect()`, releases the lock, then `spawn_blocking` with the cloned data. The lock hold time is proportional to dirty_keys.len() × clone cost, NOT to serialization time.

For the stress test: run `bench.py --mode async --duration 10` and trigger `POST /snapshot` mid-run. Measure throughput during the snapshot window vs before/after.

</specifics>

<deferred>
## Deferred Ideas

- Per-shard parallel snapshot writes — requires full sharding (future milestone)
- Async compression (zstd) during snapshot write — future optimization
- Snapshot to S3 Files — see HORIZON-S3-FILES.md research

</deferred>

---

*Phase: 15-snapshot-io-off-main-thread*
*Context gathered: 2026-04-12 (auto mode — adapted from ROADMAP for actual Phase 14 architecture)*
