# Valkey-style I/O architecture rework (v0.1+)

**Status:** Proposed — not in v0 critical path. Captured 2026-05-02 after benchmark session on r8g.4xlarge revealed cross-thread channel overhead between IO workers and apply thread.

**Decision needed before promoting to phase:** is the current per-worker `mio::Poll` design measurably worse than Valkey-style for our workload, OR is it just architectural debt that costs nothing measurable?

---

## TL;DR

Beava's IO architecture (Plan 18-05/18-06) was claimed to mirror "Valkey 8 model" but it actually diverges:

- **Valkey**: 1 epoll, on main thread; IO threads are pure SPSC/SPMC worker queues that never call `epoll_wait`.
- **Beava**: N+1 mio::Poll instances; each IO worker independently polls its assigned client subset and sends parsed RingItems to apply via crossbeam channel.

The beava design is **simpler-looking per-worker** but adds:
1. Cross-thread channel hop (`worker → read_rx → apply`) on every push event
2. N+1 syscalls per tick across the cluster (vs. 1 for Valkey)
3. Apply thread `try_recv()` busy-loop with 50µs `recv_timeout` fallback (visible in trace gaps)

For workloads with **few hot connections at high pipeline depth** (our typical bench shape), this is overhead Valkey doesn't pay.

---

## Current beava architecture

### Threading model (from `crates/beava-server/src/server.rs::serve_with_dirs`)

```
┌──────────────────────────────────────────────────┐
│ Apply thread (1)                                 │
│ ─────────────────                                │
│  - Owns mio::Poll registered with:              │
│      TOKEN_HTTP_LISTENER (mio::Token(0))        │
│      TOKEN_TCP_LISTENER  (mio::Token(1))        │
│      TOKEN_APPLY_WAKER   (Token(usize::MAX))    │
│  - tick() returns ready listener tokens         │
│  - On accept: hands client to worker[w]         │
│    via crossbeam channel `new_client_tx[w]`     │
│  - Drains crossbeam channel `read_rx`           │
│    (busy-spin → recv_timeout(50µs))             │
│  - dispatch_one() for each RingItem             │
│  - Builds WriteEncoder closures, sends to       │
│    `write_tx[w]` for the same worker            │
└──────────────────────────────────────────────────┘
                      │ accept_clients_to_workers
                      ▼
┌──────────────────────────────────────────────────┐
│ IoPool worker × N (N = max(2, ncpu/4))          │
│ ──────────────────                               │
│  Each worker owns:                               │
│   - own mio::Poll + Waker (independent epoll!)  │
│   - own clients: HashMap<u64, WorkerClient>     │
│   - own BytesMutPool (cap=256 buffers × 4KB)    │
│                                                  │
│  Loop:                                           │
│   1. Drain new_client_rx → register on own poll │
│   2. Drain write_rx → invoke encoder, write     │
│   3. Poll own clients (mio::Poll::poll)         │
│   4. Read ready FDs → parse frames              │
│   5. Send RingItems via read_tx (crossbeam)     │
│   6. Wake apply via TOKEN_APPLY_WAKER           │
└──────────────────────────────────────────────────┘
```

### What this costs per push

Per the `BEAVA_TRACE_APPLY_TIMING=1` per-stage trace (r8g, fraud-team / cardinality=100k uniform / pd=1024):

| Stage | r8g ns | M4 ns |
|---|---|---|
| parse | 130 | 63 |
| lookup | 72 | 31 |
| validate | 410 | 337 |
| wal_build | 78 | 79 |
| wal_append | 96 | 51 |
| **agg** | **6555** | **3430** |
| bookkeeping | 123 | 65 |
| **TOTAL** | **7466** | **4060** |

`parse` runs on the IO worker (off-apply per Plan 18-04.7). The `wal_append` is a memcpy into the WAL ring buffer — not synchronous fsync.

**Not visible in this trace:** the channel-hop time. Apply thread spins on `read_rx.try_recv()` waiting for the next RingItem. Between push events, that gap can be 0–50µs depending on whether the busy-spin caught it.

---

## Valkey architecture (verified from source)

### Threading model (from `valkey-io/valkey/src/io_threads.c::IOThreadMain`)

```c
// Pure worker queue — IO threads NEVER call epoll_wait or aeWait.
void *IOThreadMain(void *arg) {
    while (1) {
        pthread_testcancel();

        // PRIORITY 1: Drain private SPSC queue
        while ((batch_count = spscDequeueBatch(&io_private_inbox[id],
                                              batch_jobs, BATCH_SIZE)) > 0) {
            // Execute batch of read+parse / write+encode jobs
        }

        // PRIORITY 2: Drain shared SPMC queue
        void *tagged_job = spmcDequeue(&io_shared_inbox);
        if (tagged_job) { ... }

        // If both empty, sleep on mutex (kernel signal from main)
        if (processed == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
        }
    }
}
```

### Main thread (the only event loop)

The main thread runs `aeMain()` (one event loop using epoll/kqueue/evport). It:

1. Accepts new client connections.
2. Polls ALL clients via one `epoll_wait` call.
3. For each client with pending read: adds it to `clients_pending_read` list.
4. Before continuing the loop: calls `handleClientsWithPendingReadsUsingThreads`:
   - Distributes the pending-read clients **round-robin** to io-threads.
   - io-threads pull jobs from queue, do `read()` + parse, return parsed query.
   - Main thread executes the command **serially** (single-threaded data plane).
5. For each client with pending write: same flow via `handleClientsWithPendingWritesUsingThreads`.

**Key invariant:** main thread is the **only** thread that calls `epoll_wait`. IO threads are **pure SPSC/SPMC consumers** — no kernel polling.

### Why Valkey did it this way

- **Cache locality:** main thread touches each client during command exec. Pre-warming L1/L2 by polling on the same thread reduces cross-CPU cache traffic.
- **Atomic dispatch:** parse → execute → encode is consecutive ops on the same data. No serialization through a channel.
- **Simpler synchronization:** SPSC/SPMC queues are lock-free. No cross-thread locking on per-client state.
- **Backpressure:** if io-threads are slow, main can refuse to schedule more work; client read events stay buffered in epoll until next iteration.

---

## Gap analysis

| Concern | Beava (today) | Valkey | Why beava costs more |
|---|---|---|---|
| epoll instances | N+1 | 1 | N extra `epoll_wait` syscalls per tick |
| Cross-thread channel | crossbeam `read_rx`, `write_tx[w]`, `new_client_tx[w]` | SPSC/SPMC inbox | crossbeam is fast but still ~30-100ns send/recv overhead |
| Apply thread idle behavior | Busy-spin `try_recv()` then `recv_timeout(50µs)` | Inline in epoll loop | Wastes apply core on idle |
| Worker idle behavior | mio::Waker fired on new work | mutex sleep | Roughly equivalent |
| Listener accept | Apply thread (mio::Poll on listeners) | Main thread (aeMain) | Equivalent |
| HTTP keep-alive support | Yes (`parse_http_request` returns keep_alive) | N/A (RESP only) | Beava has additional surface area |
| Multi-client polling | Distributed across N workers | All on main | Beava's N small epolls vs Valkey's 1 big epoll |
| `maxclients` cap | **None (leak)** | Configurable, default 10000 | Beava can grow unbounded |
| Slow client backpressure | None | `client-output-buffer-limit` | Beava can OOM from one slow client |

### Trace-confirmed bottleneck distribution (per push)

```
agg                  6,555 ns  ← apply thread CPU (operator chain) — DOMINANT
parse                  130 ns  ← worker thread (off-apply)
crossbeam send         ~50 ns  ← worker → apply channel hop
apply spin/recv        ~?? ns  ← not directly traced; gaps between pushes
write encode           ~?? ns  ← worker thread (off-apply, post-apply)
crossbeam send         ~50 ns  ← apply → worker write_tx
```

Apply CPU dominates at **88%** of total. The cross-thread channel overhead is small (~100ns total per push) — but at high concurrency or low per-event work (e.g. small pipeline workload), the proportion grows.

---

## Migration plan

### Phase A — measure the actual cost (1 day)

Before any rework, **prove the channel overhead matters**:

1. **Add a "single-thread" baseline mode** to bench-v2 server: run apply + IO inline (no workers, no channels). Compare per-push time on a tiny workload (small pipeline / 1 client / pd=1).
2. **Profile apply-thread idle gaps** with samply during a 100% load run. Measure time spent in `recv_timeout` vs in command apply.
3. **Profile cross-CPU traffic** via `perf stat -e cache-misses,cache-references` between worker and apply core. Compare to single-thread baseline.

If channel overhead is < 5% of total, **abandon the rework** — the design is fine.

If channel overhead is > 10%, proceed.

### Phase B — consolidate poll on apply thread (3-5 days)

1. **Remove per-worker `mio::Poll`.** Each worker becomes a pure consumer.
2. **Apply thread polls all clients** on a single `mio::Poll`. New tokens scheme:
   - `TOKEN_HTTP_LISTENER`, `TOKEN_TCP_LISTENER`
   - Per-client tokens reused: `slot_idx + TOKEN_CLIENT_BASE` (already defined as dead code, revive it)
3. **Pending-reads list:** apply thread maintains `Vec<u64>` of slot_idx values with pending read events.
4. **Batch dispatch to workers:** before each command-execution loop, send the pending-read batch to workers via SPMC queue. Workers parse in parallel.
5. **Replace crossbeam → SPMC.** crossbeam is fine for now but the Valkey-style pattern uses tagged SPSC/SPMC. Keep crossbeam if it benchmarks within 10% of SPSC.
6. **Apply waits on a barrier** until all batch parses complete. Then dispatches commands serially. Then enqueues encoded writes back to workers.

### Phase C — add `maxclients` + slow-client backpressure (1-2 days)

1. **`BEAVA_MAX_CLIENTS` env / config field**, default 10000 (match Valkey).
2. **Per-client write buffer cap.** If `write_buf.len()` exceeds threshold (default 16 MiB), close client and log.
3. **Slot reuse on disconnect.** When worker detects a closed client, send a `ClientClosed { slot_idx }` message back to apply via the existing channel; apply removes from `slot_proto` map (currently leaks).

### Phase D — validate (2 days)

1. **Re-run bench-v2 sweep** on r8g.4xlarge (3 shards, fraud-team + medium pipelines, pd=1024 × workers={1,2,4,8}).
2. **Re-capture per-stage trace**, look for reduced channel overhead.
3. **High-connection stress test:** 10k idle TCP connections + 100 active. Measure per-event tail latency. Valkey-style should NOT regress here despite single-poll.
4. **Update CLAUDE.md `§ mio-only Hot-Path Invariant`** to reflect new architecture (still mio-only, but consolidated).
5. **Add architectural test** asserting only ONE `mio::Poll` exists in the data plane (currently architectural test exists for "no axum on data plane" but not for poll count).

### Risk register

| Risk | Mitigation |
|---|---|
| Single epoll becomes the bottleneck at >50k connections | Phase A measurements should show this isn't an issue; v0 doesn't target that scale anyway. If hit, can fall back to per-worker poll for connection handling but consolidated for command dispatch. |
| Workers spending too much time idle waiting for batch | SPMC + adaptive batch size (small batches when low load, larger when busy) — Valkey already does this |
| Cache locality regression — apply thread now touches all clients | Use thread-local read buffers, not shared. Apply thread already touches all client state during command exec. |
| Migration breaks existing tests | Phase B is feature-flagged behind `BEAVA_VALKEY_IO=1` env var; both paths coexist for 1 release before old code removed. |

### Out of scope

- io_uring backend changes (Phase 18-12 added io_uring support; we'd reuse it under the new poll model)
- HTTP/2 — separate v0.1+ phase, not coupled to this rework
- WAL writer thread changes — already correct
- Apply thread CPU optimization — separate concerns (Phase 19.x)

---

## Open questions

1. **Does the trace overhead actually justify the rework?** Until Phase A measurements are done, this is speculative. If apply-CPU is 88% and channel overhead is 2%, the win is at most 2%.
2. **Do we lose anything by removing per-worker mio::Poll?** Possibly: per-worker independent backpressure (one slow client doesn't slow other workers). Valkey handles this with `client-output-buffer-limit`.
3. **HTTP server-side connection pooling.** Already present (BytesMutPool, keep-alive support). Not affected by this rework.
4. **Bench client side: HTTP keep-alive pooling.** Bench-v2 is TCP-only currently. Adding `--transport http` would test HTTP path — separate from this rework but informs the workload assumption.

---

## References

- **Valkey IO threads source:** [valkey-io/valkey/src/io_threads.c](https://github.com/valkey-io/valkey/blob/unstable/src/io_threads.c) — `IOThreadMain()`, SPSC/SPMC queue infrastructure
- **Valkey networking source:** [valkey-io/valkey/src/networking.c](https://github.com/valkey-io/valkey/blob/unstable/src/networking.c) — `handleClientsWithPendingReadsUsingThreads`, `handleClientsWithPendingWritesUsingThreads`
- **Beava IO worker source:** `crates/beava-runtime-core/src/io_thread_worker.rs` — current per-worker continuous loop
- **Beava IO pool source:** `crates/beava-runtime-core/src/io_pool.rs` — claims to "mirror `handleClientsWithPendingReadsUsingThreads`" but adds independent polling
- **Beava server entrypoint:** `crates/beava-server/src/server.rs::serve_with_dirs` — apply thread mio::Poll on listeners only; per-worker mio::Poll on clients
- **Plan 18-05:** original "continuous worker" design (Valkey 8 model claim)
- **Plan 18-06:** worker-side mio::Poll added (the divergence point)

## Provenance of this doc

Written 2026-05-02 after a benchmark session on AWS r8g.4xlarge (Graviton 4, 16 vCPU, 128 GiB, ARM64) and local Apple M4 comparison. Trace data was captured with `BEAVA_TRACE_APPLY_TIMING=1` showing per-stage push timing. The architectural difference was discovered when investigating why beava's per-shard EPS on r8g (~140k for fraud-team) was significantly lower than M4 (~195k for the same workload), and the user noticed beava's "Valkey 8 model" comment didn't match how Valkey actually structures its IO threads.
