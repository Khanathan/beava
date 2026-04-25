---
phase: 18-redis-hand-roll
artifact: redis-research
date: 2026-04-24
license-note: "Redis 7.x is BSD-3-Clause. This document quotes function names + 1-2 line snippets for architectural explanation. No verbatim copy of Redis source into Beava codebase. Derivative Rust implementation of the architectural pattern is standard practice."
upstream-source: "github.com/redis/redis @ branch 7.2 (read for this research)"
---

# Phase 18 — Redis 7.x architecture summary

**Goal:** distill the Redis hot-path architecture so Phase 18 can translate it
to Rust without re-deriving from scratch. This is a one-page summary, not a
transcription. Implementers can re-read the upstream files directly when they
need detail.

## TL;DR

Redis 7.x is a **single-thread command-execution server** with a **multi-thread
I/O frontend**. The design's core insight: command execution is cheap (in-RAM,
typically ≤1 µs) but socket I/O (`read` / `write` syscalls + RESP parse) is
not. So Redis serializes the command-execution stage on one OS thread (no
locks needed) and parallelizes I/O across N threads guarded by a tight atomic
spin barrier. This is the architecture Phase 18 ports to Rust for the Beava
TCP hot path.

## Key files in Redis source (paths relative to redis repo root)

| Concern | File | Functions |
|---|---|---|
| Event loop core | `src/ae.c` | `aeMain`, `aeProcessEvents`, `aeCreateFileEvent`, `aeCreateTimeEvent` |
| epoll backend (Linux) | `src/ae_epoll.c` | `aeApiPoll`, `aeApiAddEvent`, `aeApiDelEvent` |
| kqueue backend (BSD/macOS) | `src/ae_kqueue.c` | same surface as ae_epoll |
| Main server loop | `src/server.c` | `main`, `initServer`, `beforeSleep`, `afterSleep`, `processCommand` |
| Networking + I/O threads | `src/networking.c` | `IOThreadMain`, `handleClientsWithPendingReadsUsingThreads`, `handleClientsWithPendingWritesUsingThreads`, `readQueryFromClient`, `writeToClient`, `processInputBuffer` |
| Client struct | `src/server.h` | `struct client` (~280 fields; key ones: `qb_pos`, `querybuf`, `argv`, `reply`, `bufpos`, `flags`) |
| AOF | `src/aof.c` | `feedAppendOnlyFile`, `flushAppendOnlyFile`, `aof_fsync_mode` |
| RESP protocol | `src/networking.c` | `processInlineBuffer`, `processMultibulkBuffer` |

## 1. Event loop (`ae.c`)

```c
// Skeleton — aeMain in src/ae.c
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP);
    }
}
```

Each iteration of `aeProcessEvents`:

1. Compute `tvp` — timeout for the next time event firing (`processTimeEvents`).
2. Call `beforeSleep(eventLoop)` — **this is where flushAppendOnlyFile runs**, where pending writes get distributed to I/O threads, and where pending reads are also drained via I/O threads.
3. Call `aeApiPoll(eventLoop, tvp)` — block on epoll/kqueue until I/O ready or timer fires.
4. Call `afterSleep(eventLoop)` — minor housekeeping (latency tracking).
5. Process fired file events: for each ready FD, call its registered handler (`readQueryFromClient` for clients, `acceptTcpHandler` for the listener).
6. Process fired time events.

**Crucial fact:** the listener fd, all client fds, the AOF child-pipe fd, and even the cluster-bus fd are all registered with the same `aeEventLoop`. The main thread does ~all work; I/O threads are spawned only for offloading recv/send/parse on a per-tick basis.

## 2. I/O threads (`networking.c`)

Spawned at `initThreadedIO`:

```c
// Skeleton — IOThreadMain in src/networking.c
void *IOThreadMain(void *myid) {
    long id = (long)myid;
    while (1) {
        // Spin until main thread sets pending count
        for (int j = 0; j < 1000000; j++) {
            if (getIOPendingCount(id) != 0) break;
        }
        // Long-wait fallback: take the mutex
        if (getIOPendingCount(id) == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
            continue;
        }

        // Process every client in our list
        listIter li; listNode *ln;
        listRewind(io_threads_list[id], &li);
        while ((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            if (io_threads_op == IO_THREADS_OP_WRITE) {
                writeToClient(c, 0);
            } else if (io_threads_op == IO_THREADS_OP_READ) {
                readQueryFromClient(c->conn);
            }
        }
        listEmpty(io_threads_list[id]);
        setIOPendingCount(id, 0);  // signal "done"
    }
}
```

**Coordination protocol:**

1. Main thread builds per-thread `io_threads_list[i]` (round-robin clients into N buckets).
2. Main thread sets `io_threads_op = IO_THREADS_OP_READ` (or WRITE) and updates `io_threads_pending[i]` to the bucket length.
3. **Atomic barrier:** main thread spins on `getIOPendingCount(0) != 0` (treats thread 0's slot as the "all done" flag for the join). Actually it processes its own slot 0 inline while threads 1..N work in parallel, then joins by spin-waiting on each thread's pending counter.
4. After all threads return their pending count to 0, main proceeds.

Two stages per event-loop tick:

- `handleClientsWithPendingReadsUsingThreads`: for every client in `clients_pending_read`, distribute → I/O threads do `readQueryFromClient` (which only fills the query buffer + does RESP parse, does NOT execute commands) → join → main executes commands inline via `processCommandAndResetClient`.
- `handleClientsWithPendingWritesUsingThreads`: for every client with pending output bytes (`clients_pending_write`), distribute → I/O threads do `writeToClient` → join → main installs writable handler if any client still has pending bytes.

**Crucial: command execution stays on the main thread.** Only socket recv/send + RESP byte-level parse run on I/O threads. This is what keeps the data structures lock-free.

## 3. Client state machine (`server.h` + `networking.c`)

`struct client` is the per-connection state. The hot-path fields:

- `int fd` — socket file descriptor.
- `sds querybuf` — SDS string (Redis's owned `char*` + len) holding incoming bytes; recv appends to the tail.
- `size_t qb_pos` — read cursor inside querybuf; parser advances as it consumes bytes.
- `int argc; robj **argv` — parsed command (after RESP parse). argv entries are objects pointing INTO querybuf (zero-copy where possible).
- `int reqtype` — INLINE or MULTIBULK protocol marker.
- `list *reply` + `char buf[PROTO_REPLY_CHUNK_BYTES]; size_t bufpos` — response buffer (small responses go in `buf`, larger ones spill to `reply` linked list).
- `uint64_t flags` — state flags (CLIENT_PENDING_READ, CLIENT_PENDING_WRITE, CLIENT_BLOCKED, ...).

State transitions:

```
[new connection accepted]
   → fd registered with aeApiAddEvent(AE_READABLE, readQueryFromClient)
[recv ready]
   → readQueryFromClient: append to querybuf
   → processInputBuffer: parse RESP frames into argv (loop until incomplete)
   → for each complete command: processCommand → execute → addReply(client, response_bytes)
[response written into buf or reply list]
   → if not registered for AE_WRITABLE: register handler for next loop tick
[write ready]
   → writeToClient: write buf + reply list to socket; mark CLIENT_PENDING_WRITE if more
```

In **threaded I/O mode** the parse/recv and write/serialize stages get offloaded; everything else stays inline on main.

## 4. AOF (`aof.c`)

The AOF write path is **fully inline** on the main thread, AND fsync is a separate decision:

```c
// Inline append on every write command
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = catAppendOnlyGenericCommand(...);  // serialize command into SDS
    server.aof_buf = sdscatlen(server.aof_buf, buf, sdslen(buf));
    sdsfree(buf);
}

// Flush + (maybe) fsync — runs in beforeSleep, NOT inline with command
void flushAppendOnlyFile(int force) {
    write(server.aof_fd, server.aof_buf, sdslen(server.aof_buf));  // sys write to page cache
    sdsclear(server.aof_buf);
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        redis_fsync(server.aof_fd);  // BLOCKS main thread — only used when user picks max-durability
    } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
        // Defer fsync to background thread (bio_pending_job); poll on next tick
        if (!sync_in_progress()) bioCreateFsyncJob(server.aof_fd);
    }
    // AOF_FSYNC_NO: never fsync; rely on OS
}
```

**Key insight for Beava:** `aof_buf` is appended inline on every write command (no syscall, just memcpy into SDS). Then **once per event-loop tick** in `beforeSleep`, the buffer is `write()`'d to the page cache (1 syscall regardless of how many commands were appended). Group commit is "natural" — N commands per tick = 1 write per tick. fsync is a separate concern, handled by the `bio.c` background thread pool. Main thread NEVER blocks on fsync in `everysec` mode (the default). This is exactly the model Beava already uses on `Periodic` — but Beava currently goes through an mpsc to a tokio task, which adds task-switch overhead. Phase 13.3 bottleneck investigation confirmed this is the macroscopic cost.

## 5. RESP parsing (`networking.c::processInputBuffer`)

Two protocols:
- **Inline:** `SET foo bar\r\n` — line-based, splitsds-style.
- **Multibulk (RESP):** `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` — array prefix + per-element bulk-string with explicit length. This is what production clients use.

```c
int processMultibulkBuffer(client *c) {
    if (c->multibulklen == 0) {
        // Read array header *N\r\n
        char *newline = strchr(c->querybuf + c->qb_pos, '\r');
        if (!newline) return C_ERR;  // incomplete
        long ll = string2ll(c->querybuf + c->qb_pos + 1, newline - (c->querybuf + c->qb_pos + 1), &ll);
        c->multibulklen = ll;
        c->qb_pos = newline - c->querybuf + 2;
    }
    while (c->multibulklen) {
        // Read $N\r\nDATA\r\n
        // ... parse length, then point argv[i] into querybuf at the data start
        c->argv[c->argc++] = createStringObject(c->querybuf + c->qb_pos, ll);
        c->qb_pos += ll + 2;
        c->multibulklen--;
    }
    if (c->multibulklen == 0) return C_OK;  // command complete
    return C_ERR;  // incomplete; resume on next read
}
```

Argv entries are `robj*` (Redis object) — sometimes the parser allocates new sds (copy from querybuf into argv), sometimes it shares (when argv outlives querybuf). For Beava's purposes, the takeaway is: **a single recv() can yield 0, 1, or many complete frames**, and the parser is a state machine that resumes from `qb_pos` when more bytes arrive. Beava's existing Phase 2.5 framed-TCP codec is simpler (length-prefixed) so it doesn't need the multibulk state machine — just `[u32 length][u16 op][u8 content_type][payload]`.

## 6. Command dispatch (`server.c::processCommand`)

```c
int processCommand(client *c) {
    struct redisCommand *cmd = lookupCommand(c->argv, c->argc);
    if (!cmd) { addReplyErrorFormat(...); return C_OK; }
    // ACL check, max-memory check, cluster-redirect, etc.
    cmd->proc(c);  // execute command inline
    if (server.aof_state == AOF_ON && cmd_writes) feedAppendOnlyFile(cmd, ...);
    return C_OK;
}
```

The command table is a hash table keyed on command name (case-insensitive). Each entry has a function pointer to a C function that takes `client *c` and reads from `c->argv` / writes to `c->reply` (via `addReply*` family).

## What this means for Beava Phase 18

1. **Mirror `aeMain`** with a Rust event loop on `mio` (cross-platform poll abstraction with epoll on Linux + kqueue on macOS) running on a dedicated OS thread. This thread owns `Rc<RefCell<AppState>>` (no `Arc` needed; never crosses a thread boundary).

2. **Mirror the I/O thread protocol** with N std::thread workers, atomic spin-barrier on `AtomicUsize` per thread, exponential backoff after K idle iterations to avoid burning idle cores.

3. **Inline the WAL** the same way Redis inlines `feedAppendOnlyFile` — append to a `Rc<RefCell<WalWriter>>` per command, flush in `beforeSleep`-equivalent (once per tick), fsync on a separate `std::thread` (mirror of Redis's `bio.c`).

4. **Mirror the client state machine** with a `Client` struct holding `query_buf: BytesMut`, `qb_pos: usize`, `pending_responses: VecDeque<Bytes>`. Reuse existing Phase 2.5 framed-TCP codec for parsing — simpler than RESP multibulk, same incremental-parse principle.

5. **Skip command-table dispatch hash lookup** — Beava's TCP wire op-code is a `u16` already; direct match on opcode is faster than RESP command name lookup. Already a Beava advantage over Redis.

6. **HTTP stays on tokio/axum** (D-01 in 18-CONTEXT.md). HTTP is not on the perf hot path — it exists for curl/admin/debug ergonomics. JSON parse cost (~3 µs per event per Phase 13.3 bottleneck doc) makes it impossible to clear 3M EPS/core regardless of runtime. Cross-runtime handoff via `std::sync::mpsc::SyncSender` (bounded) feeds HTTP-pushed events into the apply thread; backpressure when full.

## Cross-references

- `.planning/phases/18-redis-hand-roll/18-rust-translation.md` — pattern-by-pattern Rust translation table.
- `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` — locked decisions D-01..D-15.
- `.planning/phases/18-redis-hand-roll/18-risks.md` — 8 known risks + mitigations.
- `.planning/phases/13.3-lockless-apply/13.3-bottleneck-investigation.md` — confirms tokio reactor + cooperative-scheduling overhead is the macroscopic bottleneck (43% of server thread = `kevent` calls).
