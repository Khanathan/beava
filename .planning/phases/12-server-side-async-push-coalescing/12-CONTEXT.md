# Phase 12: Server-side async push coalescing - Context

**Gathered:** 2026-04-11
**Status:** Ready for planning
**Mode:** Auto (discuss --auto) — decisions locked in ROADMAP.md + .planning/research/SUMMARY.md

<domain>
## Phase Boundary

Buffer incoming `OP_PUSH_ASYNC` frames **per connection** and process them in batches under a single `state.lock()` acquisition, amortizing fixed per-event costs (lock acquire, event log append, fan-out target iteration, dirty-mark set insert). The phase establishes `handle_push_batch` as the shared primitive that Phase 13 (wire format) and Phase 14 (cross-shard dispatch) will reuse verbatim.

**In scope:**
- Per-connection accumulator (stack-local inside `handle_connection`)
- `select! { biased; read | sleep_until(deadline) if !empty }` deadline-armed flush
- `handle_push_batch` (stream-grouped batch dispatch — one lock, one event_log append_many, one mark_dirty_many per stream group)
- Sync PUSH force-flush bypass (H-2)
- Per-connection monotonic `seq: u64` for error ordering (C-2)
- Small/medium/large × sync/async bench matrix gate (Phase 11 lesson)

**Out of scope (explicit):**
- Wire-format changes / client SDK batch API (Phase 13)
- Multi-thread sharding (Phase 14)
- Snapshot off-thread (Phase 15)
- New shared state on `AppState`

</domain>

<decisions>
## Implementation Decisions

### Coalescing Parameters (LOCKED in roadmap)
- **D-01:** Default `batch_size` N = 64 async frames
- **D-02:** Default `batch_deadline` T = 200µs
- **D-03:** Implementation uses `tokio::time::Instant` + `sleep_until(deadline)` inside `select!` — NOT `sleep(200µs)` (hits 1ms wheel floor)
- **D-04:** `select!` branch order is `biased;` with read first so incoming frames short-circuit the deadline under load

### Batch Handler Semantics
- **D-05:** `handle_push_batch` groups events by primary stream name BEFORE acquiring the state lock (zero-copy into a small `SmallVec<[(&str, Vec<_>); 4]>`)
- **D-06:** Per stream group: exactly ONE `engine.push_batch_no_features` + ONE `event_log.append_many` + ONE `store.mark_dirty_many`
- **D-07:** Stream metadata lookups (`key_field`, cascade targets, `fan_out_targets`) happen once per group, not once per event
- **D-08:** Critical section is strictly synchronous — `std::MutexGuard` never held across `.await` (C-7)

### Sync Bypass (pitfall H-2)
- **D-09:** Any non-`OP_PUSH_ASYNC` opcode (GET, SET, PUSH sync, REGISTER, etc.) arriving on the connection force-flushes the accumulator **before** being dispatched
- **D-10:** Sync PUSH p99 on medium pipeline must stay within ±5% of the v1.2 baseline (87µs) — bench gate assertion
- **D-11:** Mixed sync+async workload test (1 async connection saturating + 1 sync connection sampling) is a first-class test case

### Error Attribution (pitfall C-2)
- **D-12:** Attach monotonic `seq: u64` to every frame BEFORE batch dispatch
- **D-13:** Drain streams are sorted by seq when surfaced on the next `push`/`flush`/`get`/`disconnect`
- **D-14:** Per-connection drain buffer — errors never leak to other connections

### State Placement
- **D-15:** Accumulator is a **stack-local** `Vec<PendingAsync>` inside `handle_connection` — never on `AppState`
- **D-16:** No new shared types cross the `AppState` boundary — zero new lock contention introduced by coalescing itself

### Benchmarking (Phase 11 lesson)
- **D-17:** Bench gate covers the full matrix: small × {sync, async}, medium × {sync, async}, large × {sync, async} = 6 scenarios
- **D-18:** Each scenario is a 5-run median with σ < 10% (rejection criterion)
- **D-19:** Multi-client gate: **≥ 200k eps aggregate** on medium pipeline with 4 async clients (v1.2 baseline was ~30k due to per-event lock contention)
- **D-20:** Single-client gate: async on medium stays **within ±5%** of v1.2 142k baseline (coalescing must not regress single-client)

### Claude's Discretion
- Exact data layout of `PendingAsync` (struct vs tuple, field order) — planner decides
- Error type / drain queue concrete type — reuse whatever Phase 11 already has
- Whether to introduce a small internal helper for stream grouping — planner decides
- Test file names and bench harness wiring — executor decides

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` §"Phase 12: Server-side async push coalescing" — success criteria, stack additions, pitfall tags
- `.planning/REQUIREMENTS.md` — PERF-03 (async coalescing) acceptance criteria

### Project Research (v1.3)
- `.planning/research/SUMMARY.md` — build order rationale, critical risks per phase
- `.planning/research/PITFALLS.md` — pitfalls C-2 (seq drain), C-7 (MutexGuard across .await), H-2 (sync bypass), "Phase 11 class" bench gate lesson
- `.planning/research/ARCHITECTURE.md` — current push path, lock boundaries
- `.planning/research/STACK.md` — "no new crates" constraint for Phase 12

### Prior Phase Context (Phase 11 — v1.2)
- `.planning/milestones/v1.1-phases/11-fire-and-forget-push/11-SUMMARY.md` (or latest) — fire-and-forget protocol, `OP_PUSH_ASYNC` opcode, existing drain-on-next-call error plumbing
- `.planning/milestones/v1.1-phases/11-fire-and-forget-push/11-VERIFICATION.md` — v1.2 baseline numbers (small 138k / medium 142k / large 128k async; sync p99 87-90µs)

### Code (from Phase 11 summary)
- `src/server/tcp.rs` — `handle_connection` read loop (where accumulator lives)
- `src/engine/pipeline.rs` — `engine.push_batch_no_features`, fan-out target iteration
- `src/event_log/*.rs` — `append_many` primitive
- `src/state/store.rs` — `mark_dirty_many`, `AppState` lock boundary

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `OP_PUSH_ASYNC` opcode (0x09) and fire-and-forget wire path — Phase 11
- `engine.push_batch_no_features` — already batch-aware from v1.2 internal work
- `event_log.append_many` — batch append primitive
- `store.mark_dirty_many` — batch dirty-mark primitive
- Per-connection drain error buffer — Phase 11 surfaces errors on next client call

### Established Patterns
- Single `AppState` Mutex guards the whole engine; critical sections are strictly synchronous
- Tokio read loop per connection in `handle_connection`; one task per connection
- `select!` with `biased;` for read priority (used elsewhere for backpressure)
- Bench harness under `benchmark/` runs matrix scenarios with 5-run median

### Integration Points
- `handle_connection` read loop in `src/server/tcp.rs` — accumulator lives here
- Dispatch site that currently handles `OP_PUSH_ASYNC` → replace with flush-to-batch
- Bench harness — add `--coalesce` scenario matrix

</code_context>

<specifics>
## Specific Ideas

Success criteria in ROADMAP.md (11 items) are the spec — no additional user-specific requirements beyond what's there. Numeric thresholds (N=64, T=200µs, ±5%, 200k eps) are all locked.

</specifics>

<deferred>
## Deferred Ideas

- Cross-shard batch dispatch — Phase 14
- Client-side `push_many` API — Phase 13
- Dynamic `batch_size` / `batch_deadline` tuning — not in scope for v1.3 (Claude's Discretion rejects this as speculative)
- Prometheus metrics for coalescing — can be added in Phase 10.x follow-up if needed

</deferred>

---

*Phase: 12-server-side-async-push-coalescing*
*Context gathered: 2026-04-11 (auto mode — roadmap + research pre-locked decisions)*
