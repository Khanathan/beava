---
phase: 18-redis-hand-roll
artifact: rust-translation
date: 2026-04-24
companion: 18-redis-research.md
---

# Phase 18 — Redis pattern → Rust translation

Pattern-by-pattern map. For each: Redis idiom (C), Rust equivalent, pitfalls
specific to the Rust borrow / Send-Sync model, and where it lives in the
Beava codebase post-Phase-18.

## Translation table

| # | Redis pattern (C) | Rust equivalent | Beava location | Pitfall |
|---|---|---|---|---|
| 1 | `aeMain` event loop | `mio::Poll` + manual `loop { poll.poll(); for event in events { ... } }` | `crates/beava-redis-core/src/event_loop.rs` | Don't use tokio::select; raw `Poll::poll(&mut events, timeout)` is the equivalent. |
| 2 | `aeApiPoll` (kqueue/epoll) | `mio::Poll::poll(&mut Events, timeout)` | same | mio handles the OS dispatch. We do NOT call epoll/kqueue directly. |
| 3 | `aeCreateFileEvent(fd, mask, handler)` | `poll.registry().register(&mut source, Token, Interest)` + dispatch table `HashMap<Token, Box<dyn Handler>>` | same | mio uses `Token(usize)` not raw FDs; we own the fd→Token map. |
| 4 | `beforeSleep` hook | inline call in main loop body before `Poll::poll`; named `before_sleep()` method on EventLoop | `crates/beava-redis-core/src/event_loop.rs` | This is where WAL flush, distribute-to-IO-threads, etc happen. |
| 5 | `IOThreadMain` | `std::thread::spawn(move \|\| io_worker_loop(rx, work_slot, done_counter))` | `crates/beava-redis-core/src/io_thread.rs` | Each worker holds an `Arc<IoSlot>` containing the input bucket + atomic done flag. |
| 6 | Spin-wait `getIOPendingCount() != 0` | `loop { if slot.pending.load(Acquire) > 0 { break; } std::hint::spin_loop(); if iter > 1024 { thread::yield_now(); } }` | same | std::hint::spin_loop is critical (issues `pause` / `yield` instruction); plain busy-loop pessimizes other hyperthreads. Add exponential backoff to park after N idle ticks (D-04). |
| 7 | `pthread_mutex_lock` long-wait fallback | `parking_lot::Mutex` or just `std::thread::park_timeout(Duration::from_micros(N))` | same | Use `park_timeout` not full `park` — main needs to be able to wake threads that have backed off. |
| 8 | Atomic counter `setIOPendingCount(i, n)` | `AtomicUsize::store(n, Release)` from main; `AtomicUsize::load(Acquire)` from worker; main joins by spinning on `load(Acquire) == 0` | same | Release/Acquire ordering matters. Don't use Relaxed — main needs to see the worker's writes to the client buffer. |
| 9 | `client::querybuf` (sds) | `BytesMut` (owned, growable, contiguous) | `crates/beava-redis-core/src/client.rs` | bytes::BytesMut allows `split_to(n)` to advance the read cursor with zero copy. |
| 10 | `client::argv` (parsed command pointers into querybuf) | Either `Vec<Bytes>` (parser borrows from querybuf via `BytesMut::freeze()` slices) OR per-frame `ParsedFrame { op: u16, payload: Bytes }`. Latter is simpler for Beava's framed-TCP wire | same | Don't try to use `&[u8]` references — borrow checker fights you across the parse → execute boundary. Use `Bytes` (Arc-counted slice) for cheap clones. |
| 11 | `client::reply` (chunk list) | `VecDeque<Bytes>` per client; small responses go into a stack `[u8; 64]` first | same | Beava response sizes are tiny (~50-200 bytes for a push ack); a single `Bytes` per response is fine. |
| 12 | `addReply(c, bytes)` | `client.pending_responses.push_back(bytes)` | same | No formatting cost — pre-encoded response bytes. For HTTP responses we'd need formatters; TCP we encode inline. |
| 13 | `feedAppendOnlyFile(...)` | `wal_writer.borrow_mut().append_inline(payload)` — direct `BufWriter::write_all` into in-memory buffer | `crates/beava-persistence/src/writer.rs` (modify) | Replace existing `append_tx.send().await` with synchronous `RefCell::borrow_mut`. Already lockless on the apply thread (it's the only borrower). |
| 14 | `flushAppendOnlyFile` in `beforeSleep` | `before_sleep()` calls `wal_writer.borrow_mut().flush_to_kernel()` — single `write()` syscall with all buffered bytes | `crates/beava-redis-core/src/event_loop.rs` | Once per tick. Even if 1000 events came in this tick, ONE write() syscall. |
| 15 | `bio.c` background fsync thread | `std::thread::spawn(move \|\| fsync_worker_loop(fsync_rx, fd_clone))` | `crates/beava-persistence/src/fsync_worker.rs` (rewrite) | Existing fsync_worker is tokio task; rewrite as `std::thread`. fsync_rx is `std::sync::mpsc::Receiver<FsyncRequest>`. Apply thread sends LSN watermark; fsync worker fsyncs and updates an AtomicU64 (durable_lsn). For PerEvent durability mode, apply thread spins on durable_lsn ≥ N before responding. |
| 16 | Time events (`processTimeEvents`) | `BinaryHeap<TimeEvent>` of `(deadline_ms, callback)`; `before_sleep` checks if next deadline < now, fires callback | `crates/beava-redis-core/src/event_loop.rs` | Use `Instant` + `coarsetime::Clock` to avoid syscall-per-tick. Snapshot rotation timer + idle-cache cleanup go here. |
| 17 | `processCommand` (table lookup + dispatch) | `match frame.op { OP_PUSH => apply_push(...), OP_GET => apply_get(...), ... }` | `crates/beava-server/src/wire_dispatch.rs` (refactor) | Direct match on u16 op. Beava already has this; Redis-style hash lookup is unnecessary. |
| 18 | `accept` handler registers new fd | accept thread: `let (stream, _) = listener.accept()?; round_robin_to_io_thread(stream)` | `crates/beava-redis-core/src/accept_thread.rs` | accept on its own std::thread (cheap; sleeps in syscall most of the time). Send streams to I/O threads via per-thread `mpsc::SyncSender<TcpStream>`. |
| 19 | `setsockopt(TCP_NODELAY)` | `stream.set_nodelay(true)?` | same | Set on every accepted connection. Default off in std; we want it on. |
| 20 | `epoll_ctl(EPOLL_CTL_ADD, fd, EPOLLIN)` | `poll.registry().register(&mut stream, token, Interest::READABLE)` | I/O thread accepts streams; registers them with that thread's local Poll | Each I/O thread has its OWN `mio::Poll`. Connections are sticky to a thread. |

## Send/Sync rules — pitfalls

The reason Phase 13.3 ended up at `Arc<LocalState<RefCell<AppState>>>` was tokio's `current_thread` runtime + `LocalSet` requiring everything spawned on the local task set to be `'static` but tolerating non-`Send` types. Phase 18 escapes tokio entirely on the apply thread, so the constraints simplify:

- **Apply thread** owns `Rc<RefCell<AppState>>` directly. No `Arc` wrapper. No `LocalState`. No await points = no fairness invariant to enforce.
- **I/O threads** never touch `AppState`. They only see per-client `BytesMut` query buffers and pre-encoded `Bytes` responses. Both are `Send`. No issue.
- **Cross-thread handoff** (apply → I/O for response, I/O → apply for parsed frames) goes through `Arc<Mutex<VecDeque<...>>>` per client OR lock-free SPSC channel (`crossbeam::channel::Receiver`) per direction. **TBD in 18-03 detailed design.** Decision criterion: measure.
- **HTTP cross-runtime** (axum tokio task → apply std::thread) uses a bounded `std::sync::mpsc::SyncSender<PushRequest>` (D-13). Tokio task does `tx.try_send(...)` non-blocking; if full, returns 503 to client. No tokio task ever blocks waiting on the apply thread.

## Allocations — Redis vs Rust

Redis uses sds (single contiguous heap allocation per string) heavily. Rust equivalents:

| Redis | Rust |
|---|---|
| `sds querybuf` (grow with `sdsMakeRoomFor`) | `BytesMut` (grow with `reserve(n)` — same realloc pattern) |
| `sdsempty()` | `BytesMut::new()` |
| `sdscatlen(s, buf, n)` | `bytes_mut.extend_from_slice(buf)` |
| `sdsfree(s)` | `drop(bytes_mut)` |
| `robj *createStringObject(buf, n)` | `Bytes::copy_from_slice(buf)` (or `BytesMut::freeze()` for zero-copy where possible) |

Beava's existing wire codec (`crates/beava-server/src/tcp/codec.rs` per Phase 2.5) already operates on `BytesMut` / `Bytes`. Phase 18 reuses it.

## Atomics — ordering rules

Phase 18 uses several atomic counters. Ordering matters:

- **`io_pending[i]: AtomicUsize`** — main writes with `Release` (publishes the bucket contents to the worker), worker reads with `Acquire`. Worker writes 0 with `Release` to signal done; main reads with `Acquire` to consume worker's writes back to the response queue.
- **`durable_lsn: AtomicU64`** — fsync worker writes with `Release` after each fsync; apply thread reads with `Acquire` only when handling PerEvent durability.
- **`stop_flag: AtomicBool`** — graceful shutdown. Writers use `Release`, readers `Acquire`.
- **`event_id_counter: AtomicU64`** — already exists; uses `Relaxed` (only correctness invariant is monotonicity, no synchronization with other state).

**Rule of thumb:** if atomic A's value implies a particular memory state B is published, use `Release` on the write of A and `Acquire` on the read. If A is just a counter with no memory ordering implications, `Relaxed` is fine. Phase 18-01 plan checker should reject `Ordering::SeqCst` on hot-path atomics — too expensive on ARM, no benefit over Acq/Rel for our use cases.

## Cross-runtime handoff (D-13)

```
[axum HTTP request task on tokio]
   ↓ tx.try_send(PushRequest { event_name, payload, ack_tx })
[std::sync::mpsc::SyncSender, capacity = configurable, default 1024]
   ↓
[apply thread, in main event loop, drains rx in before_sleep]
   ↓ apply each PushRequest, send response via ack_tx (oneshot)
[axum task awaits ack_tx.recv()]
   ↓ HTTP response
```

Note: we use `std::sync::mpsc` (not tokio mpsc) for the apply-thread side because the apply thread is NOT a tokio runtime. The `try_send` call from the tokio task is non-blocking — if the channel is full, return 503 with a `Retry-After` header. ack_tx can be `tokio::sync::oneshot::Sender` because the receiving side is tokio.

## What we do NOT translate

- **Cluster bus** — Beava is single-node v0.
- **Replication** (master-replica protocol) — out of v0 scope.
- **Lua / Modules** — out of scope; Beava uses declarative feature pipelines.
- **Pub/Sub** — out of scope.
- **MULTI/EXEC transactions** — Beava operates per-event; no transaction boundary.
- **AOF rewrite** (background AOF compaction) — Beava's snapshot mechanism (Phase 7) replaces this entirely.
- **Slow log** — could add later; not Phase 18.
- **CLIENT KILL / CLIENT LIST** — admin endpoints stay on tokio HTTP per D-01.

## Reference

- `.planning/phases/18-redis-hand-roll/18-redis-research.md` — pattern source.
- `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` — locked decisions.
