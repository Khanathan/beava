# Phase 58: 58-tokio-connection-handling-rewrite - Context

**Gathered:** 2026-04-21
**Status:** Ready for planning
**Mode:** Auto (generated inside `/gsd-autonomous --auto` chain)

<domain>
## Phase Boundary

Eliminate per-connection Tokio task spawn/drop on the PUSH hot path. Replace the
existing `accept → tokio::spawn per-connection` pattern with long-lived per-shard
accept loops that inline `handle_push_batch` directly — no spawn, no drop churn.

**Linux path:** SO_REUSEPORT socket per shard; each shard thread owns its own
`TcpListener` + accept loop. Kernel distributes connections across sockets via
4-tuple hash. No tokio per-connection task.

**macOS path:** SO_REUSEPORT is unreliable; use dedicated `std::thread` per shard
running an `accept()` loop against a single listener, hand-off to shard via the
existing SPSC inbox. Tokio runtime used only for HTTP ingest (keeps axum).

**Scope:** TCP PUSH path only. HTTP PUSH stays on tokio/axum (Phase 59 handles
wire-format optimization, not runtime). Replica ingest also switches to the same
per-shard accept pattern since it's TCP.

**Out of scope (explicit):**
- JSON → binary wire format — Phase 59.
- Hot-key salting — Phase 60.
- Metrics hot-path hoist — Phase 61.
- Allocator/pooling — Phase 62.
- Fjall tuning — Phase 63.
- Rust bench client — Phase 64.

</domain>

<decisions>
## Implementation Decisions

### Area A — Per-Shard Accept Loop (Linux)

- **D-A1 (socket setup):** Each shard thread opens its own `TcpListener` with `SO_REUSEPORT` + `SO_REUSEADDR` bound to the public listen port. Kernel 4-tuple hash distributes connections across the N sockets roughly uniformly (same default behavior Phase 50 used for the HTTP path).
- **D-A2 (runtime):** Shard thread runs a `tokio::runtime::Builder::new_current_thread().enable_io().build()` local runtime per shard. The accept loop is `runtime.block_on(async move { listener.accept().await ... })` — keeps Tokio I/O but eliminates cross-task dispatch.
- **D-A3 (inline handler):** Per-connection work runs INLINE in the shard's local runtime — no `tokio::spawn`. Each connection's lifecycle: `accept → read framed OP_PUSH → handle_push_batch inline → write ack → loop`. Connection stays on the same shard thread until close/drop by client.
- **D-A4 (connection concurrency per shard):** Shard thread runs connections concurrently via `FuturesUnordered` (not spawn). Cap per shard: `BEAVA_MAX_CONNS_PER_SHARD=256` (env, default 256). On cap: back-pressure via kernel-level listen backlog (`listen(128)` per shard socket).

### Area B — macOS Fallback

- **D-B1 (thread model):** Each shard thread owns a dedicated `std::thread` running a blocking `TcpListener::accept` loop. Accepted connections get a `BufReader<TcpStream>` + `BufWriter<TcpStream>` wrapper, then execute `handle_push_batch` in BLOCKING mode (not async). Read: framed opcode → decode body → dispatch to shard inbox as today. Write: blocking ack.
- **D-B2 (single listener option):** Fallback fallback — if BEAVA_SHARDS_SINGLE_LISTENER=1 or on macOS <13 without dup3, run a single accept thread that round-robins dispatches to shard inboxes. Preserves existing 50.5 semantics. Pick D-B1 (dedicated thread per shard) as default on macOS.
- **D-B3 (HTTP path untouched):** HTTP PUSH continues on axum/tokio with per-connection task spawn. Its overhead is smaller (middleware + JSON parse dominate) and axum's architecture assumes per-task. Phase 59's wire work will revisit.

### Area C — Test Scope + Perf Gate

- **D-C1 (RED-first TDD):** Wave 0 plants a small set:
  - `tests/tokio_spawn_absence_smoke.rs` — asserts samply-like grep (via `std::backtrace` probe) that the push path does NOT hit `tokio::runtime::task::harness` — RED because today's code does.
  - `tests/per_shard_listener_smoke.rs` — on Linux, verifies N sockets listen on the same port under SO_REUSEPORT; on macOS, verifies N accept threads.
  - `tests/http_push_still_works.rs` — regression guard that HTTP ingest unchanged.
  - All `#[ignore = "58-W{1..3}"]`.
- **D-C2 (perf gate):** ≥ +25% EPS vs Phase 57 baseline on complex N=8. Phase 57 baseline 1,297,293 EPS → **floor 1,621,616 EPS**.
- **D-C3 (p99 latency guard):** p99 per-event push latency must NOT regress vs Phase 57. Measured by the harness's existing latency histogram.
- **D-C4 (pprof guard):** `tokio::runtime::task::*` symbols combined ≤ 15% of leaf samples in a re-run samply profile. Currently ~60% per Phase 54 notes.

### Claude's Discretion

- Exact socket/buf tuning (`TCP_NODELAY`, `SO_RCVBUF`, `SO_SNDBUF`) — pick consistent with existing Phase 50 settings; adjust if perf misses gate.
- `FuturesUnordered` vs a hand-rolled poll loop for per-shard concurrency — pick whichever yields cleaner code; both allocate.
- Whether `BEAVA_MAX_CONNS_PER_SHARD` default should be 256 or 1024 — tune in W4 if gate misses.
- macOS kernel version detection for the "prefer D-B1 / fall back to D-B2" branch.

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

### Phase 58 Source of Truth
- `.planning/ROADMAP.md` § Phase 58 — goal, success criteria, TPC-PERF-08.
- `.planning/STATE.md` — Phase 57 closed (1,297,293 EPS baseline).
- `.planning/REQUIREMENTS.md` — add TPC-PERF-08 row.

### Architecture
- `.planning/arch/TPC-SHARD-DESIGN.md` — TPC baseline; Phase 50 SO_REUSEPORT established for HTTP.
- `.planning/phases/54-legacy-engine-removal/54-05-SUMMARY.md` — identifies tokio task spawn/drop as 25-40% CPU.

### Phase 50 / 50.5 Primitives Reused
- `.planning/phases/50-multi-shard-routing/50-05-SUMMARY.md` (or equivalent) — Linux SO_REUSEPORT TCP accept pattern already in production for HTTP ingest.
- `.planning/phases/50.5-shard-thread-completion/50.5-02-SUMMARY.md` — shard thread ownership pattern.

### Phase 57 Perf Baseline
- `.planning/phases/57-retraction-across-crossshard-joins/57-PERF-GATE.md` — 1,297,293 EPS. This is the new floor denominator.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/server/tcp.rs::listen_tcp_task` + per-connection `tokio::spawn(handle_conn)` — this is the pattern to delete.
- `src/shard/thread.rs::spawn_shard_threads` — shard thread factory; extend to optionally own a per-shard listener.
- Phase 50's `bind_reuseport_tcp` helper (src/server/reuseport.rs or similar) — reuse verbatim for D-A1.

### Established Patterns
- Shard threads own per-shard state and event loops (Phase 49-54). Adding per-shard listener ownership is additive to this pattern.
- SO_REUSEPORT per-shard HTTP listener is already in Phase 50 — same primitive applied to TCP.

### Integration Points
- `src/server/tcp.rs::run_tcp_server` — replace top-level accept loop with per-shard delegation.
- `src/shard/thread.rs::spawn_shard_threads` — extend to take an `accept_cfg: Option<PerShardAcceptCfg>` parameter.
- `BEAVA_MAX_CONNS_PER_SHARD` — new env var, parsed at startup, default 256.

</code_context>

<specifics>
## Specific Ideas

- Samply reproducibility: keep the `scripts/profile-ingest.sh` harness used for Phase 54 pprof so the before/after SC-1 and SC-4 checks are one-command.
- Connection ownership: once a conn lands on shard-K, it stays there. This means a client that targets events for multiple shards must open N connections (or the existing tuple-routing path reroutes internally). Document in SUMMARY.
- Replica ingest is also TCP — include it in scope (same listener pattern).

</specifics>

<deferred>
## Deferred Ideas

- HTTP ingest rewrite — stays on axum. Phase 59 handles wire-format (JSON → binary) for TCP; HTTP wire stays JSON.
- io_uring for Linux — too experimental; SO_REUSEPORT + blocking+FuturesUnordered is the Rust 2026-era stable choice.
- connection-level rate limiting — exists at the application layer; no new primitives this phase.

</deferred>

---

*Phase: 58-tokio-connection-handling-rewrite*
*Context gathered: 2026-04-21*
