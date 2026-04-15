# Phase 41: Remove hot-path mutex contention - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Direct fix (user directive after strace showed 88% syscall time in futex)

<domain>
## Phase Boundary

strace -c during an 8-process 552k-eps bench showed **88% of syscall time in futex waits**, 531 contended acquisitions over ~0.5s. Phase 40 already removed the global event-log mutex. The remaining hot-path locks are:

- `state.recent_events: PLMutex<RecentEventsRing>` — 100-event ring for the `/public/recent-events` demo endpoint.
- `state.metrics: PLMutex<Metrics>` — has `events_total: u64` (bumped every PUSH) + other counters.
- `state.throughput: PLMutex<ThroughputTracker>` — rolling-window EPS counter for `/metrics`.
- `state.latency: PLMutex<LatencyHistogram>` — push-latency histogram for p50/p99 in `/metrics`.

Every successful PUSH touches 3-4 of these. Phase 41 removes or defangs each.

**In scope:**
- `recent_events` → `#[cfg(feature = "demo")]` gate. Not built unless explicitly compiled for the launch demo UI.
- `metrics.events_total` → `AtomicU64`. Other Metrics fields stay behind the mutex (they're rarely written).
- `throughput` → replace with atomic-based windowed counter (fine-grained per-second bucket + atomic rotation).
- `latency` → keep HDR-style histogram but only sample 1-in-N pushes (eliminate per-push lock).

**Out of scope:**
- Replace engine `RwLock<PipelineEngine>` — register path is already off-hot; not in this phase.
- Refactor metric exposure format — `/metrics` output shape unchanged.
- Remove /public endpoints — Phase 20 demo surface stays intact when `--features demo` is on.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
Each removal must be invisible to observers (API-compatible) and must not regress single-stream single-client numbers. The payoff comes from multi-connection bench scenarios.

### R1 — `recent_events` → `#[cfg(feature = "demo")]`
- Field gated on `AppState`, removed entirely from non-demo builds.
- Push call at `src/server/tcp.rs:1554` gated.
- HTTP route `/public/recent-events` gated.
- `demo` feature added to `Cargo.toml`. `default = ["server"]` still; `demo = ["server"]`.
- When not compiled: endpoint returns 404 (not implemented). Documented.

### R2 — `metrics.events_total` → `AtomicU64`
- Extract `events_total` field out of `Metrics` struct. Replace with `pub events_total: AtomicU64` directly on `AppState` (or keep inside Metrics but as atomic).
- Per-push increment: `state.events_total.fetch_add(1, Relaxed)`. No lock.
- `/metrics` read: `state.events_total.load(Relaxed)`.
- Other fields in `Metrics` (that are rarely written) stay behind the mutex — no change.

### R3 — `throughput` rolling-window → atomic ring
- Current `ThroughputTracker` uses a `PLMutex<...>` with 5-second windowed counter and unique-key tracker.
- Replacement: per-second bucket array (60 slots for 60s window) indexed by `SystemTime::now().as_secs() % 60`, each slot an `AtomicU64`. PUSH bumps current second's slot. `/metrics` scan reads the last 5 slots to compute 5s-eps.
- Unique-key tracker: it was behind the mutex for de-dup within window. Under Phase 41: drop the unique-key tracking from hot path — approximate via HyperLogLog or just drop the field (evaluate current consumers first).
- If unique-key counter is load-bearing for `/metrics`, flag — may need a second atomic-sketch approach.

### R4 — `latency` sampled via atomic counter, lock rarely taken
- Current: every PUSH locks `state.latency`, records µs into histogram, unlocks.
- Replacement: `state.latency_sample_counter: AtomicU64` — every push bumps it. Only 1-in-N (N=16 default, configurable) actually enters the histogram lock. The histogram update is then still locked but runs once per 16 pushes = 94% reduction.
- Result: same p50/p99 approximations (16× sampling preserves distribution shape), 94% fewer lock acquires.

### Defer to follow-up if scope creeps
- Anything touching `state.engine.write()` — Phase 42 territory.
- Replacing the Metrics mutex entirely (vs just `events_total`) — can follow if it shows up post-Phase-41.
- Replacing parking_lot with std::sync — irrelevant, parking_lot is faster.

### Validation
- Re-run `bench_v0.py --matrix --events 30000` with server built default (no demo). Expect single-stream numbers unchanged (±5%).
- Re-run the 8-process 8-stream bench (`push_one_stream.py` loop). Expect aggregate eps UP from ~540k toward ~700k+ (removes 3-4 of the ~8 hot locks per push, so expect ~30-50% gain).
- Re-run strace -c on the server during the same bench. Expect futex % to drop from 88% to <60%.

### Plan split
One plan (41-01), four tasks aligned with R1-R4:
1. R1: feature-gate `recent_events`.
2. R2: atomic `events_total`.
3. R3: atomic-ring `throughput`.
4. R4: sampled `latency`.

</decisions>

<code_context>
- `src/server/tcp.rs:146` — `pub recent_events: PLMutex<RecentEventsRing>`
- `src/server/tcp.rs:307` — construction site.
- `src/server/tcp.rs:1554` — per-push insert.
- `src/server/http.rs:542,1468` — endpoint + route.
- `src/server/tcp.rs:1118` — `state.throughput.lock().bump_unique(touched, now_inst)` per push.
- `src/server/tcp.rs:1123` — `state.metrics.lock()` block bumping events_total.
- `src/server/tcp.rs:1501` — batch-path events_total bump.
- `src/server/tcp.rs:610,781,1135,1620,1698,2250` — latency/metrics lock acquires in various paths.
- Existing `parking_lot::Mutex`, `AtomicU64`, `Instant`, `SystemTime` already in use.
- Metrics exposition: `src/server/http.rs` `/metrics` endpoint reads all four; updates to fields need matching read-side changes.

</code_context>

<specifics>
- `AtomicU64::fetch_add(1, Ordering::Relaxed)` is the correct ordering for monotonic counters. No cross-counter ordering needed for `events_total` alone.
- `ThroughputTracker`'s current "unique-key" counter: check if `/metrics` actually exposes a unique-keys-per-window number. If yes and it's load-bearing, it needs an atomic sketch (HLL-like) or a coarser approach. If it's diagnostic-only, drop it and note in plan.
- Latency sampling stride: constant `const LATENCY_SAMPLE_STRIDE: u64 = 16`. Simple modulus check on the atomic counter.
- Feature flag `demo`: add to `Cargo.toml` `[features]` section. Include `"server"` as transitive dep. `check-feature-builds.sh` may want a third check `--features demo` to confirm it compiles.
- CI implications: default tests should still run; demo-feature tests should run under a separate matrix cell.

</specifics>

<deferred>
- Lock-free HDR histogram — post-v0.
- Full Metrics struct lock-free — if 41 results show it's still hot, follow up.
- Replace engine RwLock write path — Phase 42 if needed.
- Benchmark-time thread-pinning config — dev convenience, not MVP.

</deferred>

---

*Phase: 41-remove-hot-path-mutexes*
*Source: strace -c showed 88% futex during 8-proc bench; user directive "yes please do"*
