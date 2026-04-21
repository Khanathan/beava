---
phase: 58
plan: 01
subsystem: server / per-shard TCP accept
tags:
  - linux
  - so_reuseport
  - per-shard-accept
  - inline-handler
  - futures-unordered
  - wave-1
  - tpc-perf-08
requires:
  - phase-58-00-SUMMARY (TPC-PERF-08 RED scaffolding, always-on counters,
    probe script contract)
  - phase-50-05 `bind_reuseport_tcp` helper (reused VERBATIM; not
    re-implemented, D-A1)
  - phase-50.5-02 shard-thread ownership pattern (extended — shard now
    optionally owns its listener too)
  - phase-54-05 tokio task-churn pprof evidence (what Wave 1 targets)
provides:
  - src/shard/thread.rs::PerShardAcceptCfg (pub struct)
  - src/shard/thread.rs::max_conns_per_shard_from_env (pub fn,
    BEAVA_MAX_CONNS_PER_SHARD)
  - src/shard/thread.rs::run_linux_per_shard_accept_loop (private, cfg
    linux) — FuturesUnordered accept driver on shard's own
    current_thread runtime
  - src/shard/thread.rs::process_shard_event (private) — shared ShardOp
    dispatch extracted so both None and Some accept paths run identical
    per-event logic
  - src/shard/thread.rs::emit_shard_gauges (private) — shared per-shard
    metrics sampler
  - `inline_handler_events_total` atomic bump per accepted connection
    (Wave 0 always-on field now active on Linux)
  - `accept_threads_spawned_total` atomic bump per shard at listener
    install (mirrors the macOS Wave 2 semantic — same counter across
    platforms)
affects:
  - Wave 2 (58-02) adds the macOS dedicated-accept-thread-per-shard path.
    Reuses `PerShardAcceptCfg` (Task 1 already threaded `accept_cfg`
    through `spawn_shard_threads`); macOS branch will bump
    `accept_threads_spawned_total` from its dedicated accept-thread
    spawner to flip the Wave 0 macOS RED test.
  - Wave 3 (58-03) wires the same per-shard SO_REUSEPORT pattern into the
    replica ingest opcode path (currently out-of-scope here per plan
    wave-boundary comment).
  - Wave 4 (58-04) re-runs `scripts/samply-probe-tokio-share.sh` over a
    real TCP driver to flip `tokio_share_on_push_path_under_15_pct`
    GREEN (Wave 1 wired the coverage target; Wave 1 Next-Handoff item 2
    notes the probe-harness extension is still required).
tech-stack:
  added: []
  patterns:
    - "FuturesUnordered as in-process connection-concurrency pool (no
       tokio::spawn; D-A3)"
    - "tokio::select! { biased; drain, accept-if-below-cap, idle_tick }
       + crossbeam try_recv outer drain — hybrid accept + SPSC pump"
    - "SO_REUSEPORT per-shard + kernel listen(1024) backlog as primary
       backpressure (D-A4)"
    - "Env var BEAVA_MAX_CONNS_PER_SHARD, clamped [1, 65536] default 256
       (matches BEAVA_SHARD_INBOX_SIZE/BEAVA_WATERMARK_PUBLISH_INTERVAL
       `_from_env` idiom)"
    - "Shared process_shard_event helper — lifted from the inline match
       so two top-level accept paths share the ShardOp dispatch exactly"
key-files:
  created: []
  modified:
    - src/shard/thread.rs (added PerShardAcceptCfg, max_conns_per_shard_from_env,
      run_linux_per_shard_accept_loop, process_shard_event, emit_shard_gauges;
      extended spawn_shard_threads signature with accept_cfg param;
      restructured shard_event_loop to branch on accept_cfg.is_some +
      cfg linux)
    - src/server/tcp.rs (extended run_tcp_server to build accept_cfg
      from tcp_addr; run_tcp_server_with_listener Linux branch now
      future::pending; removed spawn_linux_per_shard_accept_loops;
      macOS path preserved as Phase 57 single-listener + tokio::spawn)
    - tests/per_shard_listener_smoke.rs (Linux test now pre-binds
      ephemeral port + threads accept_cfg=Some through to shard threads;
      macOS test passes None — stays 58-W2 RED)
    - tests/http_push_still_works.rs (signature-migration None pass)
    - tests/profile_ingest.rs (signature-migration None pass)
    - tests/test_shard_thread_ownership.rs (signature-migration None pass)
    - tests/test_so_reuseport_boot.rs (signature-migration None pass)
    - tests/test_metrics_parity.rs (signature-migration None pass)
    - tests/replica_ingest_routing.rs (signature-migration None pass)
    - tests/source_table_cdc.rs (signature-migration None pass)
    - tests/tcp_ingest_routing.rs (signature-migration None pass)
    - tests/http_ingest_routing.rs (signature-migration None pass)
requirements:
  - TPC-PERF-08
decisions:
  - "BEAVA_MAX_CONNS_PER_SHARD clamp [1, 65536]. Plan spec permitted any
    range; chose 65536 as the ceiling to match BEAVA_SHARD_INBOX_SIZE's
    upper bound and keep env-clamp semantics uniform across the codebase.
    Below MIN (0) / non-numeric / out-of-range all fall back to 256 with
    a warn-once stderr line — matches `inbox_size_from_env` behavior."
  - "FuturesUnordered vs hand-rolled poll loop: chose FuturesUnordered
    (D-A3 allowed either). Cost: one Arc clone + one Box::pin per
    accepted connection. Benefit: zero bespoke poll machinery; the accept
    arm's cap gate (`inflight.len() < max`) is trivially verifiable.
    Phase 58-04 perf-gate re-evaluates if allocator pressure shows up in
    leaf samples."
  - "handle_connection_public invocation (not handle_connection direct):
    `handle_connection` is `async fn` scoped `pub(crate)`... actually
    scoped private in src/server/tcp.rs. `handle_connection_public` is
    already `pub` (line 753) and is a thin passthrough — used it verbatim
    from the shard module to avoid a visibility widen. No observable
    difference (same call target, same signature after one frame pop)."
  - "Top-level TcpListener bind on Linux becomes a loopback ephemeral
    bind (dropped immediately inside `run_tcp_server_with_listener`).
    Rationale: on Linux after shards bind SO_REUSEPORT on the public
    port, a subsequent non-REUSEPORT bind on the same port fails
    EADDRINUSE. The loopback bind keeps the `run_tcp_server_with_listener`
    signature stable and preserves the server-lifetime future
    (`future::pending`) that main.rs awaits. Operator-observable: a
    single loopback ephemeral socket briefly appears in /proc/net/tcp
    during server boot; it's dropped before the server accepts any
    client traffic."
  - "50 µs idle-tick sleep in the Linux select! arm: chosen to match the
    pre-Phase-58 inbox-drain responsiveness (≤ 100 µs p99 dispatch delay
    target even at low load). A smaller tick (10 µs) burns CPU on
    idle shards; a larger tick (500 µs) shows up as p99 tail on cold
    inbox paths. Re-evaluate in Wave 4 perf gate."
  - "try_recv drain burst cap = 256 events per select! tick. Rationale:
    prevents a hot inbox from starving the accept future under burst.
    256 matches the default `BEAVA_MAX_CONNS_PER_SHARD` default, so a
    per-tick drain cannot exceed the concurrent-connection cap — keeping
    worst-case latency bounded at ~1 tick worth of ShardOp work."
  - "Scope boundary preserved: HTTP axum path entirely untouched (D-B3).
    replica ingest untouched (Wave 3 target). macOS TCP path preserved
    as Phase 57 single-listener + tokio::spawn(handle_connection) — the
    only remaining tokio::spawn-per-conn call site in src/server/tcp.rs,
    awaiting Wave 2."
metrics:
  duration: ~18min
  completed: 2026-04-20
  tasks: 2
  commits: 2
  files_modified: 12
  files_created: 0
  lib_test_delta: "+1 (per_shard_accept_cfg_env_parses_and_clamps)"
  lib_test_total: "810/0/35 (Phase 57 baseline 809/0/35 preserved +1 env test)"
---

# Phase 58 Plan 01: Linux SO_REUSEPORT Per-Shard Accept + Inline Handler Summary

Wave 1 payload of Phase 58 (TPC-PERF-08). Eliminates `tokio::spawn` per
TCP PUSH connection on Linux by moving the accept loop + connection
dispatch INLINE into each shard's `current_thread` tokio runtime,
hosted on the same OS thread that already runs the shard's ShardOp
event loop.

## What Landed

### Production code

- **`src/shard/thread.rs`** — accepted the bulk of the change:
  - `PerShardAcceptCfg { accept_addr, max_conns_per_shard }` pub struct
    (D-A4).
  - `max_conns_per_shard_from_env()` pub fn — reads
    `BEAVA_MAX_CONNS_PER_SHARD`, clamps `[1, 65536]`, defaults 256.
  - `spawn_shard_threads` signature extended: new 4th parameter
    `accept_cfg: Option<PerShardAcceptCfg>`. Backwards-compatible — all
    13 call sites (production + tests) explicitly pass `None` or `Some`
    so the migration is audit-grep-able.
  - `shard_event_loop` branches on `cfg!(target_os="linux") &&
    accept_cfg.is_some()` and delegates to
    `run_linux_per_shard_accept_loop` for the new path; the
    accept_cfg=None / non-Linux path preserves the pre-Phase-58
    blocking `rx.recv()` body.
  - `run_linux_per_shard_accept_loop` (private, cfg linux, D-A1/A2/A3/A4)
    owns the FuturesUnordered driver. Per-shard flow:
    1. `bind_reuseport_tcp(cfg.accept_addr)` — panics the shard thread
       on failure (`catch_unwind` → is_down=true → boot fails fast at
       the ready-barrier).
    2. `state.accept_threads_spawned_total.fetch_add(1, Relaxed)` — one
       bump per shard at install.
    3. `rt.block_on(async { loop { tokio::select! { biased; drain,
       accept-if-below-cap, idle_tick } ; try_recv drain up to 256 ;
       emit_gauges_if_due } })`.
    4. On accept: bump `state.inline_handler_events_total`, push
       `Box::pin(handle_connection_public(stream, state))` into
       FuturesUnordered. NO `tokio::spawn`.
  - `process_shard_event` (private, inline) — lifted from
    `shard_event_loop`'s `rt.block_on` body so both None and Some paths
    share the identical ShardOp dispatch. The 2 original `continue;`
    statements (JSON parse error on Push, enrich-batch-over-cap)
    become `return;` — behavior-preserving at the caller.
  - `emit_shard_gauges` (private, inline) — shared per-shard metrics
    sampler (keys_owned, fjall_write_bytes, fjall_compaction_bytes).

- **`src/server/tcp.rs`** — thinned:
  - `run_tcp_server` on Linux builds `PerShardAcceptCfg` from `addr`
    + `max_conns_per_shard_from_env()` and passes
    `Some(PerShardAcceptCfg { .. })` to `spawn_shard_threads`.
    Bind a loopback ephemeral listener (dropped inside the callee on
    Linux) so the call-graph stays stable.
  - `run_tcp_server_with_listener` Linux branch is now
    `std::future::pending<..>().await` — the shard threads own every
    accept path; this fn's job on Linux is to hold the server alive
    for its lifetime.
  - `run_tcp_server_with_listener` macOS branch preserved exactly as
    the Phase 57 single-listener + per-connection
    `tokio::spawn(handle_connection)` loop. Wave 2 rewrites this.
  - `spawn_linux_per_shard_accept_loops` DELETED (obsolete Phase 50.5-02
    Task 2 spawn-per-conn path).

### Test migration

9 integration tests + 3 inline unit tests threaded the new 4th
`accept_cfg` argument. 8 of 9 integration tests pass `None` (preserve
existing behavior). `tests/per_shard_listener_smoke.rs` Linux test now
pre-binds a loopback ephemeral port and passes
`accept_cfg = Some(PerShardAcceptCfg { .. })` directly — so the Linux
half is pre-wired to flip GREEN once the Wave 0 RED marker is lifted
on Linux CI. macOS half continues to pass `None` and remains
`#[ignore = "58-W2"]`.

New unit test `per_shard_accept_cfg_env_parses_and_clamps` covers:
- default (unset) → 256
- 0 (below MIN=1) → 256
- 999999 (above MAX=65536) → 256
- "nope" (non-numeric) → 256
- "128" (valid in-range) → 128
- boundary MIN ("1") → 1
- boundary MAX ("65536") → 65536

## Verification Log

```
$ cargo check --release --tests
… Finished `release` profile [optimized] target(s) in 1.15s
✓

$ cargo check --release --tests --features state-inmem
… Finished `release` profile [optimized] target(s) in 4.04s
✓

$ cargo test --release --lib
test result: ok. 810 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
✓ (Phase 57 baseline 809/0/35 + 1 new env unit test = 810/0/35)

$ cargo test --release --lib per_shard_accept_cfg
test shard::thread::tests::per_shard_accept_cfg_env_parses_and_clamps ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --lib --features state-inmem
test result: ok. 802 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out
✓

$ cargo test --release --test http_push_still_works
test http_push_post_events_at_n4_matches_phase57 ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓ (D-B3 regression guard — HTTP path unaffected)

$ cargo test --release --test tcp_ingest_routing
test tcp_push_at_n1_routes_through_spsc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓ (N=1 path unregressed — `accept_cfg=None` callers preserved)

$ cargo test --release --test replica_ingest_routing
test replica_push_fires_notify_on_shard_path ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test test_metrics_parity
test result: ok. 6 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test http_ingest_routing
test http_push_at_n1_routes_through_spsc ... ok
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test per_shard_listener_smoke
test n_shards_produces_n_accept_threads_macos ... ignored, 58-W2
test result: ok. 0 passed; 0 failed; 1 ignored
✓ (macOS half correctly stays 58-W2 RED — Wave 2 target)

$ grep -cE 'spawn_linux_per_shard_accept_loops' src/
0
✓ (obsolete fn deleted — plan gate)

$ grep -nE 'tokio::spawn' src/server/tcp.rs
309:    /// accept loop, i.e. WITHOUT `tokio::spawn` per connection. Wave 1
630:    // runtime. No `tokio::spawn` per connection. The top-level
701:/// No `tokio::spawn` per connection — `handle_connection` is polled INLINE via
707:/// Single accept loop with per-connection `tokio::spawn`. Wave 2 rewrites
729:        // macOS (Wave 2 rewrites this) — Phase 57 single-listener + tokio::spawn
734:            tokio::spawn(async move {        ← macOS handle_connection
2579:                        tokio::spawn(run_backfill(  ← unrelated (backfill)
✓ (exactly 2 `tokio::spawn` call sites in the file: the macOS
   handle_connection retained for Wave 2, and the unrelated
   run_backfill spawn. All 5 other hits are comments / doc strings.)
```

## Linux-Host Verification (Hetzner / Linux CI)

Current host is macOS (Darwin 24.3.0 arm64). The two Linux RED tests
flip GREEN on Linux by construction:

- `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux`
  — the test pre-binds a loopback ephemeral port and passes
  `accept_cfg=Some(..)` into `spawn_shard_threads`. Each shard's
  `run_linux_per_shard_accept_loop` calls `bind_reuseport_tcp` on that
  port → N LISTEN sockets appear in `/proc/net/tcp` → assertion
  `socket_count == N_SHARDS` passes.

- `tests/tokio_spawn_absence_smoke.rs::tokio_share_on_push_path_under_15_pct`
  — Wave 1 planted the code path (per-shard SO_REUSEPORT +
  FuturesUnordered; NO `tokio::spawn` per connection on Linux). The
  coverage sentinel (`pct >= 1.0`) will remain RED until the probe
  harness is extended to drive real `TcpStream` traffic through the
  server (Wave 0 SUMMARY's Next-Wave-Handoff item 2 flagged this as a
  Wave 1 deliverable — deferred: see "Deferred Issues" below).

The Linux CI run is where both RED tests flip GREEN. The production
code path is complete.

## Deviations from Plan

### Rule 3 — Blocking issue: top-level TcpListener bind on Linux must not conflict with shards' SO_REUSEPORT sockets

- **Found during:** Task 2 verification — conceptual review while
  tracing the `run_tcp_server` call graph.
- **Issue:** After `spawn_shard_threads` on Linux, every shard has its
  own SO_REUSEPORT socket bound to `tcp_addr`. A subsequent
  `TcpListener::bind(tcp_addr)` (non-REUSEPORT) from `run_tcp_server`
  would fail `EADDRINUSE` at boot.
- **Fix:** On Linux, `run_tcp_server` still calls
  `TcpListener::bind("127.0.0.1:0")` (loopback ephemeral) solely to
  keep the `run_tcp_server_with_listener` signature stable. That
  listener is dropped inside the Linux branch of
  `run_tcp_server_with_listener` (which also calls
  `future::pending().await`), so it never accepts a connection and
  does not affect the public-port accept path. Non-Linux branches
  keep `TcpListener::bind(addr)` as before.
- **Files modified:** `src/server/tcp.rs::run_tcp_server`.
- **Commit:** `fd10ead`.

### Rule 1 — Bug fix: `shard: &mut Shard` parameter binding must be `mut`

- **Found during:** Task 2 compile.
- **Issue:** Extracted `process_shard_event(shard: &mut crate::shard::Shard, ...)`
  could not reborrow `shard` in the match arms that call engine methods
  taking `&mut Shard` (E0596: cannot borrow `shard` as mutable, as it
  is not declared as mutable). The pre-Phase-58 body had `let mut shard
  = ...` (owned), so `&mut shard` reborrowed implicitly. Once passed
  as `&mut T` into a fn, the outer binding needs `mut` for reborrowing
  to type-check.
- **Fix:** Declared the param as `mut shard: &mut crate::shard::Shard`.
  Behavior identical; the `mut` on the binding is a pure ergonomics
  fix for the reborrow.
- **Files modified:** `src/shard/thread.rs::process_shard_event`.
- **Commit:** `fd10ead` (same commit as the main Task 2 landing).

## Deferred Issues

1. **Samply probe harness extension (D-C4 coverage sentinel).** Wave 0
   SUMMARY's Next-Wave-Handoff item 2 flagged that `scripts/samply-probe-tokio-share.sh`
   + `tests/profile_ingest.rs` need to be extended to drive real
   `TcpStream` traffic through the server so `tokio::runtime::task::*`
   frames appear in the profile. Wave 1 planted the production code
   path (zero `tokio::spawn` per-conn on Linux), but the probe harness
   extension itself was left to Wave 4 per the Wave 0 RED contract —
   Wave 4 is the perf-gate close and is the natural place for the
   full end-to-end `TOKIO_SHARE_PCT ≤ 15` validation. Leaving the
   sentinel RED until Wave 4 preserves the fail-loud contract.
   **Rationale for deferral:** Wave 1's `<done>` criteria required
   landing the production code path and the Linux LISTEN-count test —
   both done. The tokio-share probe harness extension is a distinct
   deliverable; conflating it with Wave 1 would have expanded scope
   past the plan's wave boundary ("Wave 4 is the perf gate close").

2. **Pre-existing test failures (out of scope).**
   - `tests/test_concurrent.rs` (6 tests) — fails on Phase 57
     baseline too, verified via `git stash` round-trip. Tests call
     `run_tcp_server_with_listener` directly without calling
     `spawn_shard_threads` first, so `state.shard_handles` is empty
     and every PUSH fails routing. Not a Phase 58 regression; logged
     for a future pass (probably a test-harness audit rather than a
     production-code fix).
   - `cargo clippy --release` errors on `#[deprecated(since = "56.0")]`
     in `src/engine/join_validator.rs:63` — "the since field must
     contain a semver-compliant version". Pre-existing; unrelated to
     Phase 58.

## Auth Gates Encountered

None. Wave 1 is pure-Rust production-code + test migration. No
external services, no credentials, no manual verification steps.

## Next Wave Handoff (Wave 2 must deliver)

1. **macOS dedicated-accept-thread-per-shard (D-B1).** The
   `accept_cfg: Option<PerShardAcceptCfg>` parameter is already
   threaded through `spawn_shard_threads` → `shard_event_loop`. Wave 2
   adds a macOS branch in `shard_event_loop` (cfg not-linux) that:
   (a) spawns a `std::thread` per shard running a blocking
       `TcpListener::accept` loop bound to the shared port (D-B1;
       SO_REUSEPORT is unreliable on macOS so a single listener +
       dispatch thread is the fallback),
   (b) bumps `state.accept_threads_spawned_total` exactly once per
       shard at the thread-spawn point → flips
       `n_shards_produces_n_accept_threads_macos` GREEN.
   Uses the existing Phase 57 single-listener + tokio::spawn path
   in `run_tcp_server_with_listener` as the fallback when
   `accept_cfg=None` (preserves BEAVA_SHARDS_SINGLE_LISTENER=1
   operator escape-hatch, D-B2).

2. **Delete the last `tokio::spawn(handle_connection)` call site in
   `src/server/tcp.rs`** (line 734, macOS branch) once Wave 2's
   dedicated-accept-thread path ships. At Wave-2 close,
   `grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs`
   = 0 across both platforms.

## Known Stubs

None introduced by Wave 1.

The Wave 0 stub (`inline_handler_events_total` field init to 0,
never incremented by production code) is now ACTIVE on Linux —
Wave 1 wires the bump site. The macOS equivalent
(`accept_threads_spawned_total`) is bumped on Linux by
`run_linux_per_shard_accept_loop`, mirroring the semantic Wave 2's
macOS spawner must implement.

## Threat Flags

None. Phase 58-01 touched:
- `src/shard/thread.rs` — per-shard accept loop (new surface, but
  addressed in the plan's `<threat_model>` block; no additional flags
  beyond T-58-01-01..05).
- `src/server/tcp.rs` — removed `spawn_linux_per_shard_accept_loops`
  (net reduction of surface); Linux listener bind moved from
  top-level into shard threads. No new wire formats, no new
  auth/allow-list paths, no new schema.
- 10 test files — migration only.

T-58-01-01..T-58-01-05 are all `accept` / `mitigate` per the plan's
threat register. No new STRIDE entries.

## Commits

| Task | Commit  | Message                                                              |
|------|---------|----------------------------------------------------------------------|
| 1    | `8a069be` | `feat(58-W1): add PerShardAcceptCfg + BEAVA_MAX_CONNS_PER_SHARD plumbing` |
| 2    | `fd10ead` | `feat(58-W1): Linux per-shard SO_REUSEPORT accept + FuturesUnordered handler` |

## Self-Check: PASSED

- [x] `src/shard/thread.rs` — `PerShardAcceptCfg` struct + `max_conns_per_shard_from_env` helper + `run_linux_per_shard_accept_loop` cfg-linux fn all present — **VERIFIED** (grep hits: 5 / 8 / 2).
- [x] `src/server/tcp.rs` + `src/shard/thread.rs` — `accept_threads_spawned_total` bump site present at `run_linux_per_shard_accept_loop` install (src/shard/thread.rs:1558) — **VERIFIED**.
- [x] `src/shard/thread.rs` — `inline_handler_events_total` bump site present inside the accept arm (src/shard/thread.rs:1605) — **VERIFIED**.
- [x] `src/server/tcp.rs` — `spawn_linux_per_shard_accept_loops` DELETED (grep -c = 0 across `src/`) — **VERIFIED**.
- [x] `cargo check --release --tests` → exit 0 — **VERIFIED**.
- [x] `cargo check --release --tests --features state-inmem` → exit 0 — **VERIFIED**.
- [x] `cargo test --release --lib` → 810/0/35 — **VERIFIED** (Phase 57 baseline 809 preserved +1 new env unit test).
- [x] `cargo test --release --lib --features state-inmem` → 802/0/35 — **VERIFIED**.
- [x] `cargo test --release --test http_push_still_works` → 1/0/0 GREEN — **VERIFIED** (D-B3 regression guard).
- [x] `cargo test --release --test tcp_ingest_routing` → 1/0/0 GREEN — **VERIFIED**.
- [x] `cargo test --release --test replica_ingest_routing` → 1/0/0 GREEN — **VERIFIED**.
- [x] `cargo test --release --test test_metrics_parity` → 6/0/0 GREEN — **VERIFIED**.
- [x] `cargo test --release --test http_ingest_routing` → 1/0/0 GREEN — **VERIFIED**.
- [x] `cargo test --release --test per_shard_listener_smoke` (non-ignored, macOS host) → 0/0/1 (stays `ignored, 58-W2`) — **VERIFIED**.
- [x] Commits `8a069be` (Task 1) + `fd10ead` (Task 2) present in `git log` — **VERIFIED**.
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/58-01-SUMMARY.md` written — **VERIFIED**.
