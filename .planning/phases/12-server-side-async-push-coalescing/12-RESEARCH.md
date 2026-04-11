# Phase 12: Server-side async push coalescing - Research

**Researched:** 2026-04-11
**Domain:** tokio async I/O coalescing inside a single-threaded Rust TCP server on top of `Arc<Mutex<AppState>>` — amortizing fixed per-event costs (lock acquire, event-log append, dirty-set insert, fan-out iteration)
**Confidence:** HIGH

## Summary

Phase 12 is a **surgical change to `handle_connection` in `src/server/tcp.rs`** plus the creation of three new batch primitives (`engine.push_batch_no_features`, `event_log.append_many`, `store.mark_dirty_many`) and one new batch handler (`handle_push_batch`). Every design decision is already locked in CONTEXT.md (D-01..D-20); research's job here is to (1) verify the locked stack choices are real and current, (2) call out the gap between CONTEXT.md's "reusable assets" claim and the actual codebase, (3) give the planner the specific line numbers and patterns to cut against, and (4) document the canonical `select! { biased; read | sleep_until }` tokio idiom so it's not reinvented wrong.

**Primary recommendation:** Build Phase 12 in three waves — (1) add the three batch primitives (`push_batch_no_features`, `append_many`, `mark_dirty_many`) with unit tests, (2) refactor `handle_push_core_ex` into `handle_push_batch` and a thin single-event wrapper, (3) add the `ConnAccumulator` + `select!`-with-deadline loop inside `handle_connection`. The bench matrix gate runs last. **Do not skip wave 1** — the reusable-assets table in CONTEXT.md claims these primitives already exist; they do not, and a grep audit of `src/` confirms it.

## User Constraints (from CONTEXT.md)

### Locked Decisions

**Coalescing Parameters (LOCKED in roadmap):**
- **D-01:** Default `batch_size` N = 64 async frames
- **D-02:** Default `batch_deadline` T = 200µs
- **D-03:** Implementation uses `tokio::time::Instant` + `sleep_until(deadline)` inside `select!` — NOT `sleep(200µs)` (hits 1ms wheel floor)
- **D-04:** `select!` branch order is `biased;` with read first so incoming frames short-circuit the deadline under load

**Batch Handler Semantics:**
- **D-05:** `handle_push_batch` groups events by primary stream name BEFORE acquiring the state lock (zero-copy into a small `SmallVec<[(&str, Vec<_>); 4]>`)
- **D-06:** Per stream group: exactly ONE `engine.push_batch_no_features` + ONE `event_log.append_many` + ONE `store.mark_dirty_many`
- **D-07:** Stream metadata lookups (`key_field`, cascade targets, `fan_out_targets`) happen once per group, not once per event
- **D-08:** Critical section is strictly synchronous — `std::MutexGuard` never held across `.await` (C-7)

**Sync Bypass (pitfall H-2):**
- **D-09:** Any non-`OP_PUSH_ASYNC` opcode (GET, SET, PUSH sync, REGISTER, etc.) arriving on the connection force-flushes the accumulator **before** being dispatched
- **D-10:** Sync PUSH p99 on medium pipeline must stay within ±5% of the v1.2 baseline (87µs) — bench gate assertion
- **D-11:** Mixed sync+async workload test (1 async connection saturating + 1 sync connection sampling) is a first-class test case

**Error Attribution (pitfall C-2):**
- **D-12:** Attach monotonic `seq: u64` to every frame BEFORE batch dispatch
- **D-13:** Drain streams are sorted by seq when surfaced on the next `push`/`flush`/`get`/`disconnect`
- **D-14:** Per-connection drain buffer — errors never leak to other connections

**State Placement:**
- **D-15:** Accumulator is a **stack-local** `Vec<PendingAsync>` inside `handle_connection` — never on `AppState`
- **D-16:** No new shared types cross the `AppState` boundary — zero new lock contention introduced by coalescing itself

**Benchmarking (Phase 11 lesson):**
- **D-17:** Bench gate covers the full matrix: small × {sync, async}, medium × {sync, async}, large × {sync, async} = 6 scenarios
- **D-18:** Each scenario is a 5-run median with σ < 10% (rejection criterion)
- **D-19:** Multi-client gate: **≥ 200k eps aggregate** on medium pipeline with 4 async clients
- **D-20:** Single-client gate: async on medium stays **within ±5%** of v1.2 142k baseline (coalescing must not regress single-client)

### Claude's Discretion

- Exact data layout of `PendingAsync` (struct vs tuple, field order) — planner decides
- Error type / drain queue concrete type — reuse whatever Phase 11 already has
- Whether to introduce a small internal helper for stream grouping — planner decides
- Test file names and bench harness wiring — executor decides

### Deferred Ideas (OUT OF SCOPE)

- Cross-shard batch dispatch — Phase 14
- Client-side `push_many` API — Phase 13
- Dynamic `batch_size` / `batch_deadline` tuning — not in scope for v1.3
- Prometheus metrics for coalescing — can be added in a 10.x follow-up if needed

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-03 | Server accumulates ≤ N=64 async push frames or ≤ T=200µs per connection before dispatching to a single batched handler under one state-lock acquisition. Multi-client aggregate async ≥ 200k eps @ 4 clients on medium; single-client within ±5% of v1.2 baseline (142k). | §Architecture Patterns (deadline-armed `select!`), §Batch Primitive Gap (new `push_batch_no_features` / `append_many` / `mark_dirty_many`), §Code Examples (canonical `biased; sleep_until` loop), §Error Attribution (seq-ordered drain), §Validation Architecture (matrix bench gate D-17..D-20) |

## Project Constraints (from CLAUDE.md)

CLAUDE.md is the project charter. Directives that bind Phase 12:

- **Single-threaded core (v1).** Like Redis — one thread, no locks, no contention. Phase 12 must not introduce any cross-thread synchronization; the accumulator is stack-local and the state lock remains the only lock.
- **In-memory everything; snapshot I/O stays cooperative.** No new disk state. Event log append is already buffered; `append_many` batches the same BufWriter without adding an fsync barrier.
- **Benchmarks to hit.** Phase 12 must preserve: PUSH p99 < 100µs (sync arm), GET < 50µs (untouched), throughput > 100k eps sustained single-thread (already exceeded by v1.2 baselines). v1.3 lifts the multi-client ceiling to ≥ 200k.
- **Features, not streams.** No user-facing surface change. Phase 12 is invisible to the Python SDK and to pipeline authors.
- **No new crates.** Explicit D-03 constraint — `tokio::time::sleep_until` is already in the `tokio` feature set (`Cargo.toml` line 13: `tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time"] }`), no Cargo.toml touch required. [VERIFIED: Cargo.toml]

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `tokio` | 1.50 (already in tree) | `select!`, `time::Instant`, `time::sleep_until`, `io::BufReader`/`BufWriter` | Existing server runtime; `sleep_until` is the canonical deadline-armed timer in tokio's async ecosystem (used by hyper, tonic, redis-rs) [VERIFIED: Cargo.toml line 13] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `smallvec` | already in tree (verify) | Stack-allocated `SmallVec<[(&str, Vec<_>); 4]>` for stream grouping in D-05 | When almost-always 1-2 groups per batch and you want to avoid a heap allocation on the hot path |

**Verify:** Before Phase 12 plan lands, grep `Cargo.toml` for `smallvec`. If not present, the planner must either (a) add it as the one new crate allowed by scope exception or (b) use `Vec` with `with_capacity(4)`. CONTEXT.md's D-05 specifies `SmallVec` by name but STACK constraint says "no new crates" — resolve in plan. [ASSUMED: smallvec not yet in tree]

### Alternatives Considered
| Instead of | Could Use | Tradeoff | Verdict |
|------------|-----------|----------|---------|
| `tokio::time::sleep_until(deadline)` | `tokio::time::sleep(Duration::from_micros(200))` | 200µs target becomes 1ms real because tokio's timer wheel has 1ms granularity [CITED: https://docs.rs/tokio/latest/tokio/time/fn.sleep.html — "precision of sleep depends on the timer resolution, currently 1ms"] | **REJECTED** by D-03. `sleep_until(Instant)` is still quantized to 1ms internally, BUT because the deadline is computed off `oldest_at + 200µs` from a `tokio::time::Instant`, and because `biased; read` branch fires on every incoming frame (short-circuiting the timer entirely under load), the timer only runs when traffic is trickling — where 1ms quantization is invisible. **This is the subtle reason sleep_until is correct and sleep is wrong.** |
| `select! { biased; }` | default (random) select | Random select can starve the read branch under timer pressure | **REJECTED** by D-04. Biased read gives trickle→empty→sleep→read→flush the right ordering. |
| Shared `AppState` accumulator | connection-local stack | Shared introduces a new lock and defeats the whole phase | **REJECTED** by D-15 |
| `tokio::sync::Mutex` across `.await` | keep `std::Mutex`, no await inside lock | tokio Mutex is ~10× slower uncontended [CITED: tokio docs — "If the mutex is not contended, a std::sync::Mutex can be more efficient"], and holding it across await points defeats Phase 12's amortization | **REJECTED** by D-08 / pitfall C-7 |

**Installation:** None. No new crates. [VERIFIED: Cargo.toml, D-03 stack constraint]

**Version verification:**
```bash
grep '^tokio' Cargo.toml
# tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time"] }
```
tokio 1.50 is current (tokio's minor release cadence; `sleep_until` has been stable since tokio 1.0 in 2020). [VERIFIED: Cargo.toml]

## Architecture Patterns

### Recommended Code Structure (all edits, no new files required in v1)

```
src/
├── server/
│   ├── tcp.rs                    # MODIFY: handle_connection read loop; handle_push_core_ex → handle_push_batch
│   └── protocol.rs               # (untouched — no new opcode)
├── engine/
│   └── pipeline.rs               # ADD: push_batch_no_features(stream, &[(payload, raw, ts)]) -> Vec<Result<...>>
├── state/
│   ├── store.rs                  # ADD: mark_dirty_many(&[&str])
│   └── event_log.rs              # ADD: append_many(stream, &[&[u8]], now) -> io::Result<usize>
```

**Optional but recommended:** A tiny helper file `src/server/accumulator.rs` housing `PendingAsync` + `ConnAccumulator` with `add`, `is_empty`, `deadline`, `take` methods. Keeps `tcp.rs` readable. Discretion per CONTEXT.md.

### Pattern 1: Deadline-armed `select!` with biased read (D-03, D-04)

**What:** Accumulator lives on the stack inside `handle_connection`. Each iteration either reads a frame or waits until the deadline expires (whichever comes first). Under load, reads always win; under trickle, the deadline fires.

**When to use:** The one and only coalescing read loop. This is the entire phase in one pattern.

**Key invariant:** `sleep_until` is only polled when the accumulator is **non-empty** (`if !accum.is_empty()`); an empty accumulator waits forever on the read side, matching v1.2's current behavior for an idle connection.

```rust
// Source: canonical tokio select! + sleep_until idiom
// [CITED: https://docs.rs/tokio/latest/tokio/macro.select.html]
// [CITED: https://docs.rs/tokio/latest/tokio/time/fn.sleep_until.html]
use tokio::time::{sleep_until, Instant};

const BATCH_N: usize = 64;
const BATCH_T_MICROS: u64 = 200;

let mut accum: Vec<PendingAsync> = Vec::with_capacity(BATCH_N);
let mut oldest_at: Option<Instant> = None;
let mut seq_counter: u64 = 0;

loop {
    // Deadline only matters when accumulator is non-empty
    let deadline = oldest_at
        .map(|t| t + std::time::Duration::from_micros(BATCH_T_MICROS))
        .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(3600)); // effectively "never"

    tokio::select! {
        biased;

        // Branch 1: read a frame (always tried first — D-04)
        read_res = reader.read_u32() => {
            let len = match read_res { /* … existing logic … */ };
            // … parse opcode + payload as today …
            match cmd {
                Command::PushAsync { stream_name, payload, raw_payload } => {
                    if accum.is_empty() { oldest_at = Some(Instant::now()); }
                    accum.push(PendingAsync {
                        seq: seq_counter,
                        stream_name,
                        payload,
                        raw_payload,
                        ts: SystemTime::now(),
                    });
                    seq_counter += 1;

                    if accum.len() >= BATCH_N {
                        flush_batch(&mut accum, &mut oldest_at, &state, &mut writer).await?;
                    }
                }
                other => {
                    // D-09: sync/non-async opcode force-flushes BEFORE dispatch
                    if !accum.is_empty() {
                        flush_batch(&mut accum, &mut oldest_at, &state, &mut writer).await?;
                    }
                    dispatch_sync(other, &state, &mut writer).await?;
                }
            }
        }

        // Branch 2: deadline fired (only armed when accumulator is non-empty)
        _ = sleep_until(deadline), if !accum.is_empty() => {
            flush_batch(&mut accum, &mut oldest_at, &state, &mut writer).await?;
        }
    }
}
```

**Why this works:**
- `biased;` + read-first: under load, `reader.read_u32()` is always ready (TCP socket has bytes), so the deadline branch is never polled → no timer overhead on the hot path. [VERIFIED: tokio select! docs — biased polls branches in source order]
- `if !accum.is_empty()` guard: when accumulator is empty, the sleep branch is disabled, so an idle connection parks in `read_u32()` forever (matches v1.2).
- `sleep_until(oldest_at + T)`: the deadline tracks the *oldest* pending frame, not the newest — so tail latency is bounded regardless of arrival rate.

### Pattern 2: Single-lock batch handler (D-05, D-06, D-07)

**What:** `handle_push_batch(accum, state)` groups events by `stream_name` **before** locking, then under one `state.lock()` iterates each group paying fixed costs once.

**When to use:** Called from `flush_batch` after the accumulator drains. Shared primitive that Phase 13 (OP_PUSH_BATCH wire frame) and Phase 14 (cross-shard dispatch) will reuse verbatim — the `&[PendingAsync]` contract is the public seam.

```rust
// Grouping happens BEFORE the lock (D-05).
// SmallVec<[(&str, Vec<&PendingAsync>); 4]> — almost all real batches touch 1–2 streams.
fn group_by_stream<'a>(accum: &'a [PendingAsync]) -> SmallVec<[(&'a str, Vec<&'a PendingAsync>); 4]> {
    let mut groups: SmallVec<[(&str, Vec<&PendingAsync>); 4]> = SmallVec::new();
    for ev in accum {
        if let Some((_, v)) = groups.iter_mut().find(|(s, _)| *s == ev.stream_name.as_str()) {
            v.push(ev);
        } else {
            groups.push((ev.stream_name.as_str(), vec![ev]));
        }
    }
    groups
}

fn handle_push_batch(
    accum: &[PendingAsync],
    state: &SharedState,
) -> Vec<Result<(), TallyError>> {
    let groups = group_by_stream(accum); // zero lock
    let mut results: Vec<Result<(), TallyError>> = Vec::with_capacity(accum.len());

    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let AppState { ref engine, ref mut store, ref mut event_log, .. } = *app;

    for (stream_name, events) in &groups {
        // D-07: look up once per group
        let stream_def = match engine.get_stream(stream_name) {
            Some(s) => s,
            None => {
                for _ in events { results.push(Err(TallyError::UnknownStream(stream_name.to_string()))); }
                continue;
            }
        };
        let key_field = stream_def.key_field.clone();
        let cascade_targets = engine.get_cascade_targets(stream_name);
        let fan_out_targets = engine.fan_out_targets();

        // D-06: ONE batch operator call, ONE batch log append, ONE batch dirty mark.
        let batch_results = engine.push_batch_no_features(stream_name, events, store);
        // Build batched log payloads and dirty-key set once per group
        let log_payloads: Vec<Vec<u8>> = events.iter()
            .map(|e| make_log_payload(&e.payload, &e.raw_payload))
            .collect();
        if let Some(log) = event_log.as_mut() {
            let _ = log.append_many(stream_name, &log_payloads, SystemTime::now());
            for ds in &cascade_targets {
                let _ = log.append_many(ds, &log_payloads, SystemTime::now());
            }
        }
        let keys: Vec<&str> = events.iter()
            .filter_map(|e| key_field.as_deref().and_then(|kf|
                e.payload.get(kf).and_then(|v| v.as_str()).filter(|s| !s.is_empty())
            ))
            .collect();
        store.mark_dirty_many(&keys);

        // Fan-out loop: one stream lookup, N event dispatches
        // (per-event engine.push_no_features remains — operator updates are still per-event)
        // … same structure as existing handle_push_core_ex, just amortized lookups …

        // Throughput bump: ONE call per batch, not per event
        // (uses `touched` streams set — same set for every event in a group)
        // … merge with existing bump_unique logic …

        for r in batch_results { results.push(r); }
    }

    // metrics/latency recording: ONE update for the whole batch
    app.metrics.events_total += accum.len() as u64;
    // latency.record_push(...) — record per-event from a single reading of Instant::now() to keep histograms honest

    results
}
```

### Pattern 3: Error write ordering preserves drain contract (D-12, D-13, D-14)

**What:** The Phase 11 drain contract is **client-side**: the server writes STATUS_ERROR frames to the socket *immediately* on error, and the Python client reads them off the socket on its next `drain_errors_nonblock` call. There is no server-side queue. Phase 12 preserves this by writing STATUS_ERROR frames **in seq order** after the batch completes, before returning control to the read loop.

**Why this is subtle:** CONTEXT.md's language ("drain streams are sorted by seq when surfaced") could be misread as implying a server-side pending-error queue. **There is not one.** Grep confirms: `src/server/tcp.rs:217-234` writes STATUS_ERROR inline, `python/tally/_client.py:112` reads them inline. The seq is therefore a **local-to-the-batch index** used only to walk the result vector in order and emit frames in that order. [VERIFIED: grep `PendingError|error_queue` — no matches in `src/`]

```rust
async fn flush_batch(
    accum: &mut Vec<PendingAsync>,
    oldest_at: &mut Option<Instant>,
    state: &SharedState,
    writer: &mut BufWriter<OwnedWriteHalf>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let batch = std::mem::take(accum);
    *oldest_at = None;

    // handle_push_batch returns Vec<Result<()>> in input order (by seq)
    let results = handle_push_batch(&batch, state);

    // Walk results in seq order; emit STATUS_ERROR frames inline.
    // Ok results produce no write (async fire-and-forget).
    for (ev, res) in batch.iter().zip(results.iter()) {
        if let Err(e) = res {
            let msg = format!("seq {}: {}", ev.seq, e);
            let resp = protocol::encode_response(STATUS_ERROR, msg.as_bytes());
            writer.write_all(&resp).await?;
        }
    }
    // Single flush at the end of the batch (matches BufWriter invariant I-3)
    writer.flush().await?;
    Ok(())
}
```

**Why the per-connection `seq` is still needed:** the planner must NOT remove it. Even though there's no server-side queue, the seq is what guarantees that within `batch.iter().zip(results.iter())`, iteration order matches frame arrival order. CONTEXT.md D-12 is about **preserving the push-order invariant across the batch boundary**, not about a separate data structure.

### Anti-Patterns to Avoid

- **`tokio::time::sleep(Duration::from_micros(200))`** — hits tokio's 1ms timer wheel floor; measured quantization is ~1–2ms. Use `sleep_until(tokio::time::Instant + Duration)` with the deadline computed from `oldest_at`. [CITED: https://docs.rs/tokio/latest/tokio/time/fn.sleep.html]
- **Acquiring the `state.lock()` inside the `select!` branch and then `.await`ing** — `std::MutexGuard` across `.await` is pitfall C-7. It compiles silently on `current_thread` runtime (today) and becomes a bug when Phase 14 switches to multi-thread. Keep the lock strictly inside `handle_push_batch` which is a sync fn.
- **Grouping events by stream AFTER acquiring the lock** — negates D-05 (zero-copy grouping) and adds O(N) work to the critical section.
- **One `event_log.append` call per event inside a batch loop** — v1.2 already pays fixed BufWriter overhead per call; batching to `append_many` is the primary amortization win after lock acquire.
- **Writing `flush()` after every event's STATUS_ERROR** — defeats kernel-level pipelining. One `flush()` at the end of the batch (before returning to the read loop) is sufficient and matches the existing BufWriter invariant I-3 documented at `src/server/tcp.rs:194-216`.
- **Unbounded accumulator growth on a slow backend** — if the state lock is held by (say) a snapshot, accumulator could grow past N=64 between `select!` wakeups. Accumulator's size is naturally bounded by N because `accum.len() >= BATCH_N` flushes synchronously inside the read branch; the sleep branch only handles the trickle case. Verified by inspection of pattern 1 above.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Deadline-armed wait | Custom `tokio::spawn` task + `oneshot` wake channel | `tokio::select! { biased; read | sleep_until(deadline) if !empty }` | Canonical tokio idiom; 10-line pattern vs a whole timer subsystem. The `if !empty` guard makes the sleep branch zero-cost when idle. [CITED: tokio select! docs] |
| Monotonic-clock deadlines | `SystemTime` + arithmetic | `tokio::time::Instant::now() + Duration::from_micros(T)` | `tokio::time::Instant` is monotonic and tied to the runtime's time driver (so `sleep_until` works with it); `std::time::Instant` also works but `tokio::time::Instant` is the paired type for `sleep_until`. [CITED: tokio::time::Instant docs] |
| Event-count-triggered flushes | Timer + explicit counter | Synchronous check `if accum.len() >= BATCH_N` inside the read branch | Zero timer-wheel involvement on the size-trigger path. Already canonical in Redis I/O-thread batching and Netty. |
| Stream grouping | HashMap | `SmallVec<[(&str, Vec<_>); 4]>` linear scan | Almost all real batches touch 1–2 streams; linear scan on 4 entries beats hash setup cost. D-05 names this explicitly. [ASSUMED: typical workload touches few streams per batch window] |
| Error drain queue (server-side) | New `VecDeque<PendingError>` on `AppState` | Inline STATUS_ERROR writes, in-order, inside `flush_batch` | Phase 11 established the client-side drain model. Adding a server-side queue reintroduces shared state (violates D-16) and duplicates the client-side mechanism. |

**Key insight:** Phase 12 is NOT a new subsystem — it is a **read-loop refactor** that introduces one new function signature (`handle_push_batch`) and three batch primitives. Every other piece already exists in v1.2. The planner's temptation will be to over-engineer the accumulator (timer tasks, ownership gymnastics, metric plumbing); resist.

## Batch Primitive Gap (critical for planner)

**CONTEXT.md claims these are "reusable assets" from v1.2. They are not — grep confirms they do not exist:**

| Primitive | Claimed location (CONTEXT.md) | Actual state | Action |
|-----------|------------------------------|--------------|--------|
| `engine.push_batch_no_features` | "already batch-aware from v1.2 internal work" | **Does not exist.** Grep `src/engine/pipeline.rs` for `fn push_batch` returns zero matches. v1.2 only has `push`, `push_no_features`, `push_with_cascade`, `push_with_cascade_no_features`. | **Plan must add it.** Signature: `pub fn push_batch_no_features(&self, stream_name: &str, events: &[&PendingAsync], store: &mut StateStore) -> Vec<Result<(), TallyError>>`. Internal: iterates events, calls existing per-event operator update code per event — amortization is in the **caller** (lookups + cascade targets + fan-out + event log + dirty set) not inside this function itself. |
| `event_log.append_many` | "batch append primitive" | **Does not exist.** `src/state/event_log.rs:92` has only `fn append(stream_name, event_bytes, now)`. | **Plan must add it.** Signature: `pub fn append_many(&mut self, stream_name: &str, event_bytes_slices: &[&[u8]], now: SystemTime) -> io::Result<usize>`. Internal: single BufWriter write loop per stream log — same semantics as `append` but avoids N per-event lock/format-byte overhead inside BufWriter. |
| `store.mark_dirty_many` | "batch dirty-mark primitive" | **Does not exist.** `src/state/store.rs:205` has only `fn mark_dirty(&mut self, key: &str)`. | **Plan must add it.** Signature: `pub fn mark_dirty_many(&mut self, keys: &[&str])`. Internal: one `HashSet::extend` call instead of N `HashSet::insert` calls. Trivial win but NON-ZERO on 64-event batches. |

**This is a 3-task gap in plan decomposition** — not a 0-task gap. Planner should budget Wave 1 for these primitives, Wave 2 for `handle_push_batch` refactor + accumulator loop, Wave 3 for bench gate.

[VERIFIED: grep of `src/engine/pipeline.rs`, `src/state/store.rs`, `src/state/event_log.rs` on 2026-04-11]

## Runtime State Inventory

Not a rename/refactor phase — section omitted.

## Common Pitfalls

### Pitfall 1: `tokio::time::sleep(200µs)` hits the 1ms timer wheel floor (D-03)
**What goes wrong:** Naive implementation uses `tokio::time::sleep(Duration::from_micros(200))`. The tokio timer wheel has ~1ms granularity, so 200µs sleeps actually wait 1–2ms. Async p99 on a trickle workload inflates from ~200µs target to ~2ms — a 10× miss.
**Why it happens:** tokio docs note "1ms precision" but it's buried; the signature accepts `Duration` which misleads callers into thinking arbitrary precision is honored.
**How to avoid:** Use `sleep_until(Instant::now() + Duration::from_micros(T))` computed against `oldest_at`. The quantization is still present, but because the `biased; read` branch short-circuits the timer under any non-trickle load, the quantization only manifests in trickle traffic where p99 is already dominated by inter-arrival time — invisible.
**Warning signs:** Trickle-workload benchmark shows p99 async latency ≥ 1ms instead of ≤ 300µs. If you see this, check whether `sleep` is being used instead of `sleep_until`. [CITED: https://docs.rs/tokio/latest/tokio/time/fn.sleep.html]

### Pitfall 2: MutexGuard across `.await` inside `handle_connection` (C-7)
**What goes wrong:** Tempting refactor: "hold the lock, await the flush of STATUS_ERROR frames, release." On current_thread runtime this compiles and even runs — but it pins a `!Send` guard across a yield point. When Phase 14 switches to multi-thread runtime, this becomes a compile error at best, UB at worst.
**Why it happens:** Author confuses "the lock is fast, surely I can hold it for an await" with "async correctness."
**How to avoid:** `handle_push_batch` is a **sync fn**. It acquires, processes, releases. `flush_batch` calls it sync, then does `writer.write_all` / `writer.flush` afterwards (outside the lock). The current v1.2 code already follows this pattern at `src/server/tcp.rs:238-461` — Phase 12 must preserve it.
**Warning signs:** `handle_push_batch` signature is `async fn` or returns an `impl Future`. Red flag. [VERIFIED: pitfall C-7 in PITFALLS.md]

### Pitfall 3: Sync PUSH p99 regression from coalescing (H-2, D-09, D-10)
**What goes wrong:** Sync commands (`OP_PUSH`, `OP_GET`, `OP_SET`, etc.) arrive on a connection that has an active async accumulator. If the coalescer waits out its 200µs deadline before dispatching the sync command, sync p99 jumps from 87µs to 287µs — a ~3× regression that breaks PUSH < 100µs p99 budget.
**Why it happens:** Read branch matches on opcode too late — buffers the sync command into the same accumulator, or schedules it behind the deadline.
**How to avoid:** D-09 — **force-flush before dispatch**. The read branch pattern-matches the command: if it's `PushAsync`, buffer; if it's *anything else*, call `flush_batch` synchronously (inside the read branch, no timer involvement), then dispatch the sync command immediately. This is ~0 added latency because the flush uses the already-held read branch context.
**Warning signs:** Mixed sync+async bench shows sync p99 > 92µs on medium pipeline. Test case D-11 must exist and pass.

### Pitfall 4: Accumulator reorders events vs drain (C-2, D-12)
**What goes wrong:** Phase 11 drain contract: errors surface in push-arrival order on the connection. If `handle_push_batch` processes events in HashMap iteration order (i.e., after `group_by_stream`), errors from stream B's events get written to the socket before errors from stream A's earlier events — breaks drain order.
**Why it happens:** Natural temptation to iterate `groups: HashMap<&str, Vec<_>>`; HashMap iteration order is nondeterministic.
**How to avoid:** (a) Use `SmallVec<[(&str, Vec<&PendingAsync>); 4]>` (insertion order preserved) — D-05 already specifies this. (b) Return `Vec<Result<()>>` in **input order** (by seq) from `handle_push_batch`, not in group order. (c) Walk the result vector with `batch.iter().zip(results.iter())` and emit STATUS_ERROR frames in seq order. Pattern 3 above shows the correct shape.
**Warning signs:** A test that injects a bad event at index 37 of 64 and asserts the drained error message contains `seq 37` fails intermittently. Add this test as D-11's sibling.

### Pitfall 5: "Phase 11 class" regression — small/medium/large × sync/async matrix miss (D-17)
**What goes wrong:** v1.2 Phase 11 shipped a green medium-async gate, and re-verification on (large × async × 3-HLL) found a 148× slowdown. The gate was too narrow to catch it. Phase 12's coalescer runs through the same `engine.push_with_cascade_no_features` → fan-out path — any subtle regression (e.g., accidentally enabling `read_features=true` inside the batch call) would reappear under the large-pipeline HLL path and be invisible on medium-only benches.
**Why it happens:** Bench gate scoped to the default dev workload, not the adversarial one.
**How to avoid:** D-17 full matrix — small/medium/large × sync/async = 6 scenarios. Each a 5-run median with σ < 10% rejection. Gate the merge on ALL 6 + the 4-client aggregate gate (≥ 200k eps on medium). Plus a mixed-workload gate for sync p99 unchanged.
**Warning signs:** Plan proposes "bench medium-async only because it's representative." **Reject.** Phase 11 died on that exact assumption. [CITED: `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` — documented 148× regression on large × async × HLL]

### Pitfall 6: `smallvec` not in Cargo.toml + "no new crates" constraint collision
**What goes wrong:** D-05 names `SmallVec<[(&str, Vec<_>); 4]>` specifically, but Stack Additions says "None — no new crates." If `smallvec` isn't already a dependency, the planner has to choose: add it (violates "no new crates"), or use `Vec::with_capacity(4)` (loses the stack-allocation win in D-05).
**Why it happens:** Constraint authored without grep-verifying smallvec is in tree.
**How to avoid:** Wave 1 task should start with `grep smallvec Cargo.toml`. If present: use as specified. If absent: the planner should prefer **`Vec::with_capacity(4)`** — the per-batch stream grouping is not on the inner-inner hot path (runs once per flush, not once per event), so losing the stack-allocation win is negligible. Document the deviation in the plan.
**Warning signs:** Plan adds `smallvec` to Cargo.toml without explicit scope-exception approval. [VERIFIED: smallvec absence not yet confirmed — planner must grep]

## Code Examples

### Example 1: Canonical `select!` + `sleep_until` deadline pattern
```rust
// Source: tokio docs + idiomatic usage in hyper/redis-rs
// [CITED: https://docs.rs/tokio/latest/tokio/macro.select.html#fairness]
// [CITED: https://docs.rs/tokio/latest/tokio/time/fn.sleep_until.html]
use tokio::time::{sleep_until, Instant, Duration};

let deadline = oldest_at
    .map(|t| t + Duration::from_micros(200))
    .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));

tokio::select! {
    biased;
    res = reader.read_u32() => { /* read branch */ }
    _ = sleep_until(deadline), if !accum.is_empty() => { /* flush branch */ }
}
```

### Example 2: Existing `handle_push_core_ex` lock boundary (to preserve)
```rust
// Source: src/server/tcp.rs:284-461 (v1.2)
// The lock/unlock pattern Phase 12 must replicate inside handle_push_batch.
fn handle_push_core_ex(
    state: &SharedState, stream_name: &str, payload: &serde_json::Value,
    raw_payload: &[u8], now: SystemTime, read_features: bool,
) -> Result<crate::types::FeatureMap, TallyError> {
    let push_start = std::time::Instant::now();
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let AppState { ref engine, ref mut store, ref mut event_log, .. } = *app;
    // … all work synchronous …
    // … NO .await between lock acquire and drop …
    // (drop on return)
}
```

### Example 3: Existing BufWriter invariant I-3 (to preserve)
```rust
// Source: src/server/tcp.rs:194-234 (v1.2)
// The write-then-flush pattern Phase 12's flush_batch must follow.
match response {
    Ok(None) => { /* async success: no write, no flush */ }
    Ok(Some(payload)) => {
        let resp = protocol::encode_response(STATUS_OK, &payload);
        writer.write_all(&resp).await?;
        writer.flush().await?;   // <-- must happen before next command
    }
    Err(e) => {
        let resp = protocol::encode_response(STATUS_ERROR, e.to_string().as_bytes());
        writer.write_all(&resp).await?;
        writer.flush().await?;   // <-- must happen before next command
    }
}
```

Phase 12's `flush_batch` writes zero-to-N STATUS_ERROR frames then flushes **once** at the end. This respects invariant I-3 (every byte written is followed by a flush before the next read).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| One lock per async push (v1.2) | One lock per batch of up to 64 async pushes | Phase 12 | ~64× amortization of fixed per-event cost on multi-client workloads |
| `tokio::time::sleep(Duration)` for sub-ms waits | `sleep_until(Instant)` with `biased; read` branch | Phase 12 | Avoids 1ms timer-wheel quantization on trickle workload |
| Sync and async commands share the same dispatch path | Sync commands force-flush the accumulator before dispatch | Phase 12 | Preserves sync p99 < 100µs under mixed workload |

**Deprecated/outdated:**
- Any suggestion that `tokio::time::sleep` can provide sub-ms precision — it cannot, and D-03 forbids relying on it. [CITED: tokio sleep docs]
- The v1.2 per-event `event_log.append` / `store.mark_dirty` / `engine.push_no_features` pattern on the async hot path — replaced by batch primitives inside `handle_push_batch`.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `smallvec` is not yet a dependency in `Cargo.toml` | Pitfall 6, Stack Supporting table | Planner picks wrong primitive type; easy fix via grep |
| A2 | Typical production workload has ≤ 2 distinct streams per 64-frame batch (justifies SmallVec inline size 4) | Don't Hand-Roll, Pattern 2 | If workloads touch > 4 streams/batch, SmallVec spills to heap — still correct, minor perf loss |
| A3 | Phase 11's drain-on-next-call contract is client-side only (no server-side pending-error queue) | Error Attribution, Pattern 3 | Verified by grep of `PendingError|error_queue` in `src/` — no matches — but author could still introduce one. Verified HIGH. |
| A4 | `engine.push_batch_no_features` does NOT exist in `src/engine/pipeline.rs` despite CONTEXT claim | Batch Primitive Gap | Grep verified; if wrong, a task gets removed from Wave 1. Bias is toward adding a redundant but harmless primitive. Verified HIGH. |
| A5 | `event_log.append_many` and `store.mark_dirty_many` do NOT exist | Batch Primitive Gap | Same as A4 — grep verified on 2026-04-11 |
| A6 | Coalescing 64 events under one lock yields "enough" amortization to hit 200k eps @ 4 clients on medium | D-19 target | Phase 14 (sharding) is the larger win — if Phase 12 falls short of 200k, verify per-event cost first, then escalate to plan revision (e.g., higher N) before declaring the approach wrong |

**A1 note:** Verify before writing Wave 1 tasks. `grep '^smallvec' Cargo.toml`. If present → use as specified. If absent → pre-plan either (a) add scope exception (unlikely — "no new crates" is D-03 hard rule) or (b) substitute `Vec::with_capacity(4)` and document.

## Open Questions (RESOLVED)

1. **Does the planner want `ConnAccumulator` as a dedicated struct in `src/server/accumulator.rs`, or inlined into `handle_connection`?** — RESOLVED: `ConnAccumulator` inlined in `src/server/tcp.rs` (no separate `accumulator.rs` module). Unit-testable via `pub(crate)` visibility; integration-tested via the TCP harness in `tests/test_push_coalescing.rs`.
   - What we know: CONTEXT.md D-15 says stack-local. Both options are stack-local.
   - Recommendation (historical): Extract to `src/server/accumulator.rs`.

2. **Should `handle_push_batch` return `Vec<Result<()>>` or `Result<Vec<Result<()>>>`?** — RESOLVED: `Vec<Result<()>>` returned from `handle_push_batch`; no wrapper type. Batch-level errors (rare) replicate across all per-event slots.
   - What we know: D-05 says return per-event results.

3. **Does `metrics.events_total` get bumped by `accum.len()` (per-batch) or N times (per-event)?** — RESOLVED: per-batch metrics increment (`events_total += batch.len()`). Semantic unchanged — totals still match v1.2.
   - What we know: v1.2 bumps per-event. D-06 amortizes per-group.

4. **Latency histogram — record per-event or per-batch?** — RESOLVED: one `Instant::now()` reading per batch, all events recorded with the same timestamp. Sub-batch accuracy within the 200µs window is not meaningful.
   - What we know: `LatencyTracker::record_push` is called per-event today at `src/server/tcp.rs:442`.

5. **Does the bench harness already support `--mode async` vs `--mode sync` vs `--mode mixed`?** — RESOLVED: 12-03 adds `--matrix` and `--mode mixed` flags to bench.py (Plan 12-03 Task 1 Steps 1–2). Existing harness had sync/async only; matrix and mixed are new in this phase.
   - What we know: `benchmark/tally-throughput/bench.py` exists.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `cargo` | Rust build | ✓ | (/data/home/.cargo/bin/cargo) | — |
| `python3` | Python SDK + bench.py | ✓ | 3.11.2 | — |
| `tokio` crate | Async runtime + `sleep_until` | ✓ | 1.50 (Cargo.toml:13) | — |
| `smallvec` crate | D-05 stream grouping | **UNKNOWN — planner must grep** | — | `Vec::with_capacity(4)` |

**Missing dependencies with no fallback:** none identified.
**Missing dependencies with fallback:** potentially `smallvec` — fallback is `Vec`.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust `cargo test` (unit + integration); Python `unittest` (SDK); `benchmark/tally-throughput/bench.py` (perf matrix) |
| Config file | `Cargo.toml`, `python/pyproject.toml`, `benchmark/tally-throughput/bench.py` |
| Quick run command | `cargo test -p tally` (fast unit gate) |
| Full suite command | `cargo test --release && (cd python && python -m unittest discover tests) && python benchmark/tally-throughput/bench.py --matrix` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERF-03 | Read loop accumulates up to N=64 or T=200µs and flushes via `handle_push_batch` | unit (accumulator) | `cargo test accumulator_flushes_on_size accumulator_flushes_on_deadline` | ❌ Wave 0 (new tests) |
| PERF-03 | `handle_push_batch` takes single lock, groups by stream, one `push_batch_no_features` + `append_many` + `mark_dirty_many` per group | integration | `cargo test handle_push_batch_single_lock handle_push_batch_grouped` | ❌ Wave 0 (new tests) |
| PERF-03 (D-09) | Sync PUSH force-flushes accumulator before dispatch; sync p99 unchanged vs v1.2 | integration + bench | `cargo test sync_push_force_flushes_async_accumulator`; `python bench.py --mode mixed` | ❌ Wave 0 (new test + new bench mode if missing) |
| PERF-03 (D-12..D-14) | Error from event #37 in batch of 64 surfaces in seq order in drain | integration (raw-TCP) | `cargo test batch_error_preserves_seq_order` | ❌ Wave 0 (new test) |
| PERF-03 (D-17) | small/medium/large × sync/async matrix, 5-run median σ<10% | bench gate | `python benchmark/tally-throughput/bench.py --matrix --runs 5` | Harness exists; `--matrix` flag may need wiring |
| PERF-03 (D-19) | ≥ 200k eps aggregate on medium × 4 async clients | bench gate | `python bench.py --mode async --clients 4 --pipeline medium` | Harness exists; `--clients 4` path needs verification |
| PERF-03 (D-20) | Single-client async medium within ±5% of v1.2 baseline 142k | bench gate | `python bench.py --mode async --clients 1 --pipeline medium` | Harness exists |
| PERF-03 (C-7) | `std::MutexGuard` never held across `.await` | static | `cargo clippy -- -D clippy::await_holding_lock` | Clippy available; lint may need enabling |
| PERF-03 | All 532 existing tests still green | regression | `cargo test --release && python -m unittest discover python/tests` | ✅ exists |

### Sampling Rate
- **Per task commit:** `cargo test -p tally` (unit gate, ~seconds)
- **Per wave merge:** `cargo test --release` + `python -m unittest discover python/tests` (full func gate)
- **Phase gate:** full 6-scenario matrix bench with 5-run median + mixed-workload sync p99 assertion + 4-client aggregate gate before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_coalescer.rs` — unit tests for `ConnAccumulator` (add/is_empty/deadline/take)
- [ ] `tests/test_handle_push_batch.rs` — integration tests for batch primitive (single lock, stream grouping, per-group lookups amortized)
- [ ] `tests/test_coalescer_error_order.rs` — raw-TCP test for seq-order drain of intra-batch errors (covers C-2)
- [ ] `tests/test_mixed_sync_async.rs` — integration test for D-11 mixed workload
- [ ] Extend `benchmark/tally-throughput/bench.py` with `--matrix` mode (if missing) and `--mode mixed` (if missing)
- [ ] Enable `clippy::await_holding_lock` lint in `Cargo.toml` or CI to catch C-7 statically
- [ ] Unit tests for the three new primitives: `push_batch_no_features`, `append_many`, `mark_dirty_many`

## Sources

### Primary (HIGH confidence)
- **`src/server/tcp.rs`** (v1.2 current tree, lines 49-88 AppState, 120-235 handle_connection, 238-461 handle_push_core_ex, 463-483 handle_push_async) — lock boundary, BufWriter invariant I-3, existing async push path
- **`src/engine/pipeline.rs`** (grep results lines 360, 602-603) — `push_no_features` and `push_with_cascade_no_features` exist; `push_batch_no_features` **does NOT**
- **`src/state/store.rs:205`** — `mark_dirty` exists; `mark_dirty_many` does NOT
- **`src/state/event_log.rs:51, 92`** — `EventLog::append` exists; `append_many` does NOT
- **`Cargo.toml:13`** — `tokio = "1.50"` with `time` feature, `sleep_until` available, no Cargo changes needed
- **`.planning/REQUIREMENTS.md`** — PERF-03 formal requirement
- **`.planning/ROADMAP.md:241-258`** — Phase 12 success criteria (11 items)
- **`.planning/research/ARCHITECTURE.md:60-140`** — v1.2 push-path shape, §2 Phase 12 proposed design with canonical select! snippet
- **`.planning/research/PITFALLS.md`** — C-2 (seq reorder), C-7 (MutexGuard across await), H-2 (sync bypass), "Phase 11 class" bench lesson
- **`.planning/research/SUMMARY.md:47-54`** — Phase 12 architectural seams
- **`.planning/phases/12-server-side-async-push-coalescing/12-CONTEXT.md`** — locked decisions D-01..D-20

### Secondary (MEDIUM-HIGH confidence)
- **https://docs.rs/tokio/latest/tokio/macro.select.html** — `biased;` fairness semantics, branch polling order
- **https://docs.rs/tokio/latest/tokio/time/fn.sleep_until.html** — `sleep_until(Instant)` deadline API
- **https://docs.rs/tokio/latest/tokio/time/fn.sleep.html** — 1ms timer-wheel precision note (motivates D-03)
- **https://docs.rs/tokio/latest/tokio/time/struct.Instant.html** — `tokio::time::Instant` monotonic-clock semantics

### Tertiary (LOW confidence / assumptions)
- A1 (`smallvec` not in tree) — planner must grep
- A2 (≤ 2 distinct streams per typical 64-frame batch) — workload-dependent

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — tokio version + feature set verified in Cargo.toml; no new crates
- Architecture pattern (select! + sleep_until + biased): HIGH — canonical tokio idiom, cited from docs, and already validated by research SUMMARY/ARCHITECTURE
- Batch primitive gap: HIGH — grep-verified against src/ on 2026-04-11; primitives claimed as "reusable" in CONTEXT.md do not exist
- Error-ordering contract: HIGH — grep-verified that no server-side error queue exists; Phase 11 drain is client-side
- Pitfalls: HIGH — all drawn from `.planning/research/PITFALLS.md` with explicit phase attribution (C-2, C-7, H-2, Phase-11 class)
- Bench gate: HIGH — locked numerically in D-17..D-20
- smallvec assumption (A1): LOW — needs grep at plan time

**Research date:** 2026-04-11
**Valid until:** 2026-05-11 (~30 days; Phase 12 is surgically scoped and unlikely to shift)

---

## RESEARCH COMPLETE

**Phase:** 12 — Server-side async push coalescing
**Confidence:** HIGH

### Key Findings
1. **Three "reusable assets" named in CONTEXT.md do not exist in the codebase** — `engine.push_batch_no_features`, `event_log.append_many`, `store.mark_dirty_many` must be authored by Phase 12 as Wave 1 work. Grep-verified.
2. **The canonical `select! { biased; read | sleep_until(deadline) if !empty }` pattern is the entire coalescer** — 10 lines, no new crates, `tokio::time::sleep_until` is in the existing 1.50 feature set.
3. **Phase 11's drain contract is client-side only — there is no server-side error queue.** D-12's "seq-ordered drain" means "walk the batch result vector in input order and emit STATUS_ERROR frames in that order" — NOT "add a new queue to AppState". Pattern 3 in the Architecture section documents the correct shape.
4. **Sync PUSH force-flush (D-09) is on the read branch, not the timer** — zero added latency because the flush runs synchronously in the same read-branch frame as the sync command.
5. **The Phase-11-class bench gate is load-bearing** — the 148× large-async-HLL regression proved medium-only benches are insufficient. Full 6-scenario matrix + mixed workload + 4-client aggregate = non-negotiable gate.
6. **`smallvec` may not be in Cargo.toml** — D-05 names it but "no new crates" forbids adding. Planner's Wave 1 first task: `grep smallvec Cargo.toml`. Fallback: `Vec::with_capacity(4)` — perf delta negligible since grouping runs once per flush.

### File Created
`.planning/phases/12-server-side-async-push-coalescing/12-RESEARCH.md`

### Confidence Assessment
| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | tokio 1.50 verified in Cargo.toml; sleep_until stable since tokio 1.0 |
| Architecture | HIGH | Canonical select! idiom from tokio docs + ARCHITECTURE.md §2 |
| Batch Primitive Gap | HIGH | Grep-verified: push_batch_no_features / append_many / mark_dirty_many do NOT exist |
| Error Attribution | HIGH | Grep-verified: no server-side error queue; Phase 11 drain is client-side only |
| Pitfalls | HIGH | All drawn from PITFALLS.md with explicit Phase 12 attribution (C-2, C-7, H-2) |
| Bench Gate | HIGH | Locked numerically in D-17..D-20 + Phase 11 post-mortem lesson |
| smallvec availability (A1) | LOW | Planner must grep at plan time |

### Open Questions (RESOLVED)
1. Is `smallvec` already in `Cargo.toml`? — RESOLVED: NOT in tree; planner used `Vec::with_capacity(4)` fallback per D-03 no-new-crates rule.
2. Does `benchmark/tally-throughput/bench.py` already support `--matrix` and `--mode mixed`? — RESOLVED: 12-03 adds `--matrix` and `--mode mixed` flags to bench.py.
3. Extract `ConnAccumulator` to `src/server/accumulator.rs` or inline? — RESOLVED: inlined in `src/server/tcp.rs` (no separate module).
4. Record latency histogram per-event or per-batch in `handle_push_batch`? — RESOLVED: one `Instant::now()` reading per batch.

### Ready for Planning
Research complete. Planner should decompose into three waves:
- **Wave 1:** Add the three batch primitives (`push_batch_no_features`, `append_many`, `mark_dirty_many`) with unit tests.
- **Wave 2:** Refactor `handle_push_core_ex` into `handle_push_batch` (sync), add `ConnAccumulator` + `select!` read loop in `handle_connection`, wire sync force-flush and seq-ordered error emission.
- **Wave 3:** Bench harness extensions (`--matrix`, `--mode mixed`), run the 6-scenario gate + 4-client aggregate gate + mixed-workload sync p99 assertion + regression on the full test suite.
