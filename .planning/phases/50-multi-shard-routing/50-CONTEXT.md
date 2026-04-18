# Phase 50: multi-shard-routing - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

This is the phase where `BEAVA_SHARDS > 1` actually starts routing. Pinned shard threads come online (core_affinity on Linux, best-effort on macOS); listeners hand off events to shards via `crossbeam-channel::bounded` SPSC queues; on Linux each shard binds its own TCP+HTTP accept socket via `SO_REUSEPORT` (kernel 4-tuple-hashes incoming connections); on macOS, a single listener thread dispatches inline. Backpressure is non-blocking: when a shard's inbox is full, the listener drops the event, increments `beava_shard_inbox_full_total{shard}`, and returns HTTP 503 (or `SHARD_OVERLOAD` on TCP) — the listener never blocks on a hot shard. Per-shard labeled Prometheus metrics go live via the `metrics` + `metrics-exporter-prometheus` crates (running in parallel with the existing hand-rolled `/metrics` for one release cycle, to preserve operator alert rules).

Covers 9 requirements (TPC-INFRA-03, 04, 07 · TPC-PERF-02, 03, 04 · TPC-CORR-01, 03 · TPC-DX-02) — the largest phase in v1.2.

Ship-gate: ≥3× baseline on `complex-c8-x8` at `N=CPU_COUNT`; `shard_probe` cross_shard_fraction <40% on the release benchmark workload.

</domain>

<decisions>
## Implementation Decisions

### Shard thread lifecycle

- **D-01:** **Spawn-all-at-boot + ready-gate.** At server startup, main spawns all N shard threads *before* binding any listener socket. Each shard initializes its per-shard state and signals ready via a barrier. Listeners bind only when all shards report ready. `/ready` returns 503 until the barrier passes; `/health` stays 200 throughout. Matches Iggy's topology. No lazy-spawn surprises, no accept-before-shard-ready race.

### Shard panic / OOM recovery policy

- **D-02:** **Quarantine + 503 per shard.** Each shard loop runs inside `std::panic::catch_unwind` (or an equivalent try/catch harness appropriate to the tokio `current_thread` runtime). On panic, the shard is marked DOWN in an `Arc<AtomicBool>` per shard. The listener routes incoming events for a DOWN shard to HTTP 503 + `SHARD_DOWN` error code; increment `beava_shard_down_total{shard}`; `/ready` flips to 503 while any shard is down. Other shards keep serving their keys. Operator-initiated restart only (via server restart). No auto-restart — state handoff to a fresh thread would lose the panicked shard's per-shard HashMap, which is unacceptable without a snapshot save.
- **D-03:** The Iggy RefCell-across-await lesson means we explicitly ban `RefCell` borrows across `.await` on the shard hot path. Audit the Wave 1 `Shard` struct code paths; add clippy lint if one exists.

### macOS N>1 dispatcher shape

- **D-04:** **Single listener thread + inline dispatch.** On macOS (no `SO_REUSEPORT_LB`), one accept thread owns the accept socket. For each accepted connection: compute `shard_hint`, `try_send` event to target shard's inbox. No intermediate dispatcher thread — dispatch runs inline on the accept thread. Latency penalty is the SPSC send cost (~1–2 μs). Matches the design-doc language for macOS fallback; sufficient for dev-mode throughput. Users needing prod throughput run Linux.
- **D-05:** On macOS the accept thread is NOT pinned (best-effort via `core_affinity::set_for_current` but the kernel silently ignores per-TPC-RESEARCH.md §Q2; that's fine, it's a dev-mode path).

### Metrics migration cutover

- **D-06:** **Parallel period.** In Wave 2, emit BOTH the existing hand-rolled `/metrics` series AND the new `metrics` + `metrics-exporter-prometheus` series (labeled per-shard). The hand-rolled path stays deprecated-but-functional through Wave 3; removal lands in Wave 4 alongside DashMap/ArcSwap cleanup. This preserves every existing operator dashboard/alert rule with zero change required during v1.2.
- **D-07:** New per-shard labeled series, via `metrics` crate: `beava_shard_reactor_utilization{shard}`, `beava_shard_inbox_depth{shard}`, `beava_shard_events_total{shard,outcome}`, `beava_shard_keys_owned{shard}`, `beava_shard_watermark_lag_seconds{shard}`, `beava_shard_inbox_full_total{shard}`, `beava_shard_down_total{shard}` (added per D-02) — plus unlabeled `beava_events_dropped_total{reason}` and `beava_cross_shard_fanout_total{op}` (scatter-gather counter lands in Wave 3 but the crate goes live here).

### SPSC channel + backpressure

- **D-08:** `crossbeam-channel::bounded(BEAVA_SHARD_INBOX_SIZE)` — one pair per listener→shard. Default capacity 65536 events. Configurable via env; 1024..=1_000_000 clamp. Use `try_send`; on full, drop + increment `beava_shard_inbox_full_total` + return HTTP 503 / TCP `SHARD_OVERLOAD`. Zero-copy payload via `bytes::Bytes`. Listener thread never blocks.

### SO_REUSEPORT on Linux

- **D-09:** On Linux, each shard binds its own TCP accept socket to the shared `0.0.0.0:BEAVA_PORT` with `SO_REUSEPORT` set. The kernel distributes new connections via its 4-tuple hash. HTTP path: each shard runs its own axum `Server` bound to the same port via `SO_REUSEPORT`. Preserve the existing `/public/*` public-surface gating via an axum middleware layered before the route tables on every shard — identical middleware stack per shard.

### Tuple shard_key missing-field reject (TPC-CORR-03)

- **D-10:** At ingest, if the stream has a declared tuple `shard_key=("a","b",...)` and any field is absent from the event payload, reject the event. Return HTTP 400 with body `{"error":"shard_key_missing","missing":["a"]}` on HTTP; return TCP error code `SHARD_KEY_MISSING` (TBD discriminant) on TCP. Increment `beava_events_dropped_total{reason="shard_key_missing"}`. The rejection happens BEFORE shard routing so no shard thread ever sees the malformed event (preempts the Iggy RefCell-in-extraction-path panic pattern).

### ShardKeyMissingWarning (TPC-DX-02)

- **D-11:** At `BEAVA_SHARDS > 1`, every stream registered with `shard_key = None` (i.e., no declared `shard_key=` in the Python SDK) emits a single warning to `/debug/warnings`: `ShardKeyMissingWarning: stream "<name>" has no shard_key; all events routing to shard 0. Declare @bv.stream(shard_key="<fieldname>") to distribute.` The stream still runs; Wave 2 does not reject. Warning fires once per stream registration, not per event.
- **D-12:** At `BEAVA_SHARDS == 1`, no warning fires — users who never increase N above 1 should never see this warning.

### BEAVA_ENTITIES_SHARDS deprecation (TPC-INFRA-07)

- **D-13:** The legacy `BEAVA_ENTITIES_SHARDS` env (a DashMap tuning knob in `src/state/store.rs:256`) is **soft-deprecated** in Wave 2. On startup if set, log warn-once: `BEAVA_ENTITIES_SHARDS is deprecated; see BEAVA_SHARDS for the TPC shard count. This var will be removed in v1.3.` Continue to honor its effect on DashMap internal bucketing (DashMap still exists as a Wave 4-deletion compat shim). Removal happens in Wave 4 along with DashMap deletion. Hard-error deprecation is rejected: too user-hostile for an env var that works today.

### Pinning

- **D-14:** On Linux, every shard thread calls `core_affinity::set_for_current(CoreId { id: shard_index })` at the top of its run loop. Pin to physical core `shard_index` (compute via `core_affinity::get_core_ids()` filtered to physical cores if possible; else best-effort CPU index). If pinning fails (non-Linux, or Linux in a restricted cgroup/container without CAP_SYS_NICE), log warn-once and proceed — pinning is an optimization, not a correctness requirement.

### Claude's Discretion

- Exact TCP error code for `SHARD_OVERLOAD` and `SHARD_KEY_MISSING` (pick unused byte values in the TCP opcode error range).
- How shard threads are spawned (std::thread::Builder + name "beava-shard-N" vs tokio `Builder::new_current_thread()` per thread — the latter is mandated by runtime but the spawn shell is planner's call).
- The exact metric-crate wiring pattern (metrics-util Recorder vs direct metrics::counter! / gauge! macros).
- The shape of the ready-barrier (Tokio `Notify` vs `parking_lot::Condvar` vs crossbeam `WaitGroup`).

</decisions>

<canonical_refs>
## Canonical References

### Design + research
- `.planning/arch/TPC-SHARD-DESIGN.md` §"Target architecture" (topology + SPSC), §6 "HTTP/TCP listener → shard routing" + **locked 2026-04-18 backpressure contract**, §Q3 SO_REUSEPORT + macOS fallback, §Q2 macOS pinning.
- `.planning/arch/TPC-RESEARCH.md` §Q3 (SO_REUSEPORT Linux 4-tuple hash vs macOS BSD semantics), §1.1 LocalRuntime pattern, §4.3 channel choice (crossbeam default).
- `.planning/research/SUMMARY.md` §"Wave 2: Multi-shard routing" (ship list) + Decisions locked section (backpressure contract, tuple missing-field).
- `.planning/research/PITFALLS.md` §3 (hot-shard footguns — `cross_shard_fraction <40%` gate), §1 (RefCell-across-await ban).
- `.planning/research/ARCHITECTURE.md` §1 (file impact: `src/server/tcp.rs`, `src/server/http.rs`, `src/server/shard_probe.rs`).
- `.planning/research/STACK.md` §1 (pins: `core_affinity 0.8.3`, `crossbeam-channel 0.5.15`, `metrics 0.24`, `metrics-exporter-prometheus 0.16`).

### Requirements
- `.planning/REQUIREMENTS.md` — TPC-INFRA-03, 04, 07, TPC-PERF-02, 03, 04, TPC-CORR-01, 03, TPC-DX-02.

### Upstream phases
- `.planning/phases/48-shard-hint-scaffolding/48-CONTEXT.md` — D-01 shard_hint is routing function, not field.
- `.planning/phases/49-per-shard-state-store/49-CONTEXT.md` — D-01 ShardedStateStore trait; D-07/D-08 StreamDefinition.shard_key field; D-10/D-11 BEAVA_SHARDS parsed in Wave 1 (clamped to 1); Wave 2 is where it takes effect.

### Existing code
- `src/server/tcp.rs` — current listener topology (to be split into listener-accept vs shard-handle).
- `src/server/http.rs` — axum router (per-shard replication).
- `src/server/shard_probe.rs` — existing cross_shard_fraction instrumentation (extended with per-shard counters).
- `src/state/store.rs:256` — `BEAVA_ENTITIES_SHARDS` env parse site (add deprecation warn).
- `src/debug/warnings.rs` (or equivalent) — /debug/warnings emission point for ShardKeyMissingWarning.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `require_loopback_or_token` middleware — layered on every per-shard axum router, identically, to preserve auth semantics.
- `src/server/shard_probe.rs` cross_shard_fraction measurement — extend with per-shard counters; ship-gate reads from it.
- `src/debug/warnings.rs` emission helper — already in tree for HTTP drop warnings; ShardKeyMissingWarning follows the pattern.

### Established Patterns
- Existing handrolled `/metrics` is the prior-art pattern; metrics crate additions run parallel, not replacement, through Wave 3.
- `require_loopback_or_token` middleware pattern — reuse identically on every per-shard router.
- `std::thread::Builder::name("beava-…")` — existing threads in tree carry descriptive names; follow "beava-shard-N" convention.

### Integration Points
- `src/server/tcp.rs`: `ConcurrentAppState` gains `shard_router: Arc<ShardRouter>`; `handle_push_core_ex` routes through it; `run_tcp_server` splits into `run_accept_loop` (listener) + `run_shard_loop` (per shard, spawned at boot).
- `src/server/http.rs`: `build_router` becomes per-shard; an `AxumServerSet` struct owns N server instances, one per shard, each bound via SO_REUSEPORT on Linux / single-listener on macOS.
- `src/state/store.rs`: ShardedStateStoreV1 from Wave 1 is now actively multi-shard; `shard_count > 1` path is live.

</code_context>

<specifics>
## Specific Ideas

- Wave 2 is where the ≥3× gate lives. `complex-c8-x8` is the target cell; any of the 9 cells drifting below −5% is a merge-blocker.
- Wave 2 does NOT land: scatter-gather (`GET /streams`) — that's Wave 3 TPC-PERF-05. Cross-shard reads in Wave 2 return shard-0 results only (legacy behavior, single-source-of-truth at N=1).
- Wave 2 does NOT land: JoinShardKeyMismatch enforcement (Wave 3 TPC-CORR-04).
- Wave 2 does NOT land: /debug/shards endpoint — Wave 3 TPC-INFRA-05.

</specifics>

<deferred>
## Deferred Ideas

- `GET /streams` scatter-gather → Wave 3 (TPC-PERF-05).
- `/debug/shards` diagnostics endpoint → Wave 3 (TPC-INFRA-05).
- Lazy global-watermark publish across shards → Wave 3 (TPC-PERF-06).
- JoinShardKeyMismatch register-time fatal → Wave 3 (TPC-CORR-04).
- Per-shard event log directory + snapshot v8 + hard-fail boot guard → Wave 4.
- Fork/replica re-hash on ingest → Wave 4 (TPC-CORR-06).
- Reshard CLI tool → Wave 4 (TPC-DX-03).
- DashMap/ArcSwap deletion → Wave 4.
- Docs `docs/architecture-tpc.md` → Wave 5 (TPC-DX-04).
- N=1↔N=8 proptest parity → Wave 5 (TPC-CORR-05).

</deferred>

---

*Phase: 50-multi-shard-routing*
*Context gathered: 2026-04-18*
