---
phase: 18-redis-hand-roll
gathered: 2026-04-24
status: ready-for-planning
mode: locked-decisions
---

# Phase 18 — Context

Replace tokio on the apply + wire hot path with a hand-rolled event loop matching Redis 7.x architecture. Spec target: ≥3M EPS/core simple-fraud TCP on Linux Xeon. Reference: `18-redis-research.md`, `18-rust-translation.md` (sibling files).

## Locked decisions

### D-01 Scope: hand-rolled hot path includes BOTH HTTP/1.1 + framed TCP

Hand-rolled event loop handles BOTH HTTP/1.1 + framed TCP for data-plane endpoints (`/push`, `/push-sync`, `/push-batch`, `/get`, `/upsert`, `/delete`, `/retract`). One apply thread serves both protocols uniformly. I/O threads handle wire I/O for both. Cross-protocol cost differences come from JSON parse vs MessagePack only — runtime is identical.

**Admin endpoints stay on tokio/axum** on a separate port (default `8081`): `/metrics`, `/health`, `/ready`, `/registry`. Cold path; perf doesn't matter; keep tower middleware ergonomics.

### D-02 Apply thread

Single OS thread, hand-rolled event loop via `mio` (or `io-uring` on Linux post-Stage-18.5). Owns `Rc<RefCell<AppState>>` directly (no `Arc<LocalState>` workaround — we are out of tokio so the `!Send` restriction is natural).

### D-03 I/O thread count

Configurable via `IoConfig::io_threads`. Default `num_cpus() - 1` reserving one core for the apply thread.

### D-04 Coordination

Per-I/O-thread atomic counter (`AtomicU64`). Spin-wait with `std::hint::spin_loop()`. Exponential backoff after N idle cycles, then `std::thread::park` to yield CPU when truly idle.

### D-05 WAL integration

Inline `write()` to `WalBufferedFile` from the apply thread. fsync on a dedicated `std::thread` (NOT one of the I/O threads). `Periodic` mode: apply does NOT await fsync; `PerEvent` mode (`/push-sync`): apply queues a oneshot to fsync thread, completes the request only after fsync acks.

### D-06 fsync semantics under Periodic mode

No await on the apply thread. fsync worker runs an independent loop with `std::thread::sleep(tick)`. fsync syscall via `File::sync_data()` directly.

### D-07 TCP accept

Dedicated thread using blocking `TcpListener::accept` (or non-blocking on the main event loop, equivalent). Distributes new connections round-robin to I/O threads.

### D-08 Connection load balancing

Each event-loop tick, main thread scans all clients via `mio::Events`, finds ready ones, distributes them round-robin to I/O threads in chunks. Each I/O thread services its assigned subset for that tick.

### D-09 Wire parsing

Hand-rolled framed TCP parser (matches Phase 2.5 wire format `[u32 length][u16 op][u8 content_type][payload]`). Hand-rolled HTTP/1.1 parser via `httparse` crate (zero-copy, used by hyper itself). Both produce zero-copy argv pointing into the per-client `BytesMut` read buffer.

### D-10 Apply → response

Apply thread processes all parsed commands inline after the read phase. Buffers responses into per-client output buffer queues (`Vec<Bytes>`). I/O threads write responses out in their next write phase.

### D-11 Error handling

- Parse error → close connection (Redis pattern; assumes corrupted client)
- Execute error → inline error response (HTTP 4xx/5xx with structured body, or TCP `op=0xFFFF` error frame per Phase 2.5)
- Apply panic → log + close connection + continue (do not crash the apply thread)

### D-12 Shutdown

Graceful drain: accept stops new connections, in-flight commands drain, apply thread finishes current tick, I/O threads exit on signal, fsync flushes WAL final time.

### D-13 HTTP cross-runtime for ADMIN endpoints

Admin endpoints (`/metrics`, `/health`, `/ready`, `/registry`) on separate tokio runtime + axum on port `8081`. Cross-runtime read-only access via shared atomics (`/metrics`, `/health`) or `crossbeam::channel` for `/registry` snapshot. No write-back from admin to apply thread (admin endpoints are read-only).

### D-14 Perf gates at each stage

Each stage MUST bench before proceeding to the next. Apple-M4 numbers are INFORMATIONAL through Stage 18.4. Linux Xeon is the HARD-GATE platform from Stage 18.5 onward. Threshold: 25% regression vs prior stage = BLOCKER, 10% = WARNING.

### D-15 Crate structure

New crate `beava-runtime-core` housing the reusable event loop + I/O threads primitives. NOT `beava-redis-core` (does not bind us to upstream comparison).

`beava-server` retains its current shape but the TCP path swaps to use `beava-runtime-core`. Behind feature flag `--features hand-rolled-runtime` until cutover lands. Admin tokio/axum stays in `beava-server`.

### D-16 Hard-gate platform

Apple-M4 / Darwin 24.3.0 / 10 cores: INFORMATIONAL gates only. Phase 18.1-18.4 bench targets must be checked but not BLOCK plan progression on M4 alone.

Linux Xeon (specific hw-class set up in Stage 18.4.5): HARD-GATE platform from Stage 18.5 onward. Stage 18.5 SC1 (`≥3M EPS/core simple-fraud TCP`) MUST pass on Linux Xeon — this is the Phase 13 ship-gate target.

## Success criteria (SC1-SC8)

1. Hand-rolled event loop replaces tokio on data-plane TCP and HTTP paths
2. Admin endpoints continue working on tokio/axum (separate port)
3. Read-your-writes preserved (existing `/push-sync` smokes pass)
4. ≥3M EPS/core simple-fraud TCP on Linux Xeon (Stage 18.5 hard gate)
5. ≥150k EPS/core HTTP under same conditions (JSON-bound floor)
6. Existing test suite green; integration tests rewritten as sync where needed
7. Clippy + fmt clean throughout
8. Phase 18.5/18.6 perf-baseline + throughput-baseline rows committed for Linux Xeon

## File inventory (per stage)

- `crates/beava-runtime-core/` — NEW crate (event loop, I/O threads, accept, WAL inline, fsync worker)
- `crates/beava-server/src/runtime_core_glue.rs` — NEW; bridges runtime-core to existing AppState + apply path
- `crates/beava-server/src/http_admin.rs` — NEW; tokio/axum admin endpoints on port 8081
- `crates/beava-server/src/server.rs` — modified: feature-flagged switch between hand-rolled and tokio runtimes
- Removed eventually (Stage 18.6): `crates/beava-server/src/serve_local.rs`, `crates/beava-server/src/local_state.rs`, `push_legacy.rs` if any remains

## Plan structure

| Plan | Stage | Scope | Hard gate platform |
|---|---|---|---|
| 18-00 | Research + design | ✅ Done | — (markdown only) |
| 18-01 | Hand-rolled event loop + HTTP + TCP listeners | M4 informational | — |
| 18-02 | Inline WAL + pthread fsync | M4 informational | — |
| 18-03 | I/O threads for reads | M4 informational | — |
| 18-04 | I/O threads for writes | M4 informational | — |
| 18-04.5 | Linux bench infrastructure | infra setup | — |
| 18-05 | io_uring on Linux | **Linux HARD: ≥3M EPS/core** | ship-gate |
| 18-06 | Wire polish + VERIFICATION | **Linux full matrix** | ship-gate verification |

## Grey areas / open questions

1. **`httparse` chunked transfer encoding for `/push-batch` large bodies** — verify in Stage 18.1; mitigated by `httparse` being mature (used by hyper). If issues, fall back to non-chunked-only initially.
2. **HTTP keep-alive state-machine reset** — pipelined requests on the same connection. Stage 18.1 must include a pipelined-keepalive integration test.
3. **Admin → core data sharing** — `/metrics` and `/health` can read shared atomics directly. `/registry` needs a snapshot — design in Stage 18.1 (likely `Arc<RwLock<RegistrySnapshot>>` updated on register).
4. **Linux Xeon machine availability** — confirmed in Stage 18.4.5 setup. If no machine accessible, GitHub Actions self-hosted runner is the fallback.
5. **TDD exemptions:** Plan 18-00 (research, already complete) and Plan 18-04.5 (infrastructure) are markdown/setup; CLAUDE.md §Conventions TDD red-green does NOT apply. All other plans (18-01..18-04, 18-05, 18-06) follow strict red-green per task.

## Spec readings

The "≥3M events/sec/core" target in CLAUDE.md applies to TCP+MessagePack on Linux Xeon. Two interpretations both satisfied by Phase 18:

- **Per-core, single-thread apply**: Stage 18.5 hard-gate target.
- **Per-core average across N I/O threads**: implicitly satisfied by I/O thread parallelism multiplying TCP throughput.

HTTP throughput is JSON-parse-bound at ~150-300k EPS/core regardless of runtime. Spec acknowledges HTTP as the "curl/dev" surface; SDK throughput targets are TCP.

## Rationale for Phase 18 over piecemeal optimization

The 9-item Phase 13.5 list (collapse mpsc hop, hand-roll responses, etc.) closes ~5× of the gap to Redis but leaves a 2-5× gap on top. Closing the remainder requires runtime removal — which is Phase 18 by definition. Path A (Phase 13.5 ship + Phase 18 v0.1): ships v0 sooner. Path B (Phase 18 before v0): hits 3M target on first ship.

Phase 18 as currently scoped is path-agnostic — it can ship pre-v0 (delays ship 5-6 weeks) or post-v0 (ships as v0.1 perf release).
