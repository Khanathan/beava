---
context: v0-ship-correctness-path
created: 2026-04-29
revised: 2026-04-30 — no-event-time architectural pivot
status: post-pivot
---

# Correctness path to v0 OSS ship — REVISED 2026-04-30

**MAJOR PIVOT 2026-04-30:** Architectural simplification — no event-time / no watermarks / no joins / no PIT, ever. Phases 14, 14.1, 15 archived. NEW Phase 12.6 inserted as the new v0 surface-reduction blocker. The original "Phase 14 streaming bug" P0 item is **DELETED** — the bug class disappears with event-time itself.

See `project_redis_shaped_no_event_time_ever` (memory) for the full architectural commitment.

## Priority tier (post-pivot)

### 🔴 P0 — v0 ship blockers

#### 1. Phase 12.6 — v0 surface reduction (NEW; the new biggest blocker)
- **Severity:** CRITICAL — v0 ship surface must match the locked architectural commitment before launch
- **Status:** 📋 PLANNED (inserted 2026-04-30); no CONTEXT or plans yet — needs `/gsd-discuss-phase 12.6` first
- **Scope:** Legacy axum kill (~3500 LOC + ~10 smoke test migrations) + event-time strip (wire schema bump) + windowed-op time-source swap (Path X) + join/union removal + dead-code/redundancy sweep + mio-only hot-path enforcement + REQUIREMENTS.md + docs sweep
- **Why now:** Locked architectural commitment — v0 must ship exactly the surface defined by the no-event-time pivot
- **Next action:** `/gsd-discuss-phase 12.6` to gather scope-specific decisions (TestServerV18 design, dead-code threshold, smoke-test fate)
- **Estimated:** 12-15 plans across 3-4 weeks

#### 2. ~~Phase 14 — Streaming silent-data-loss bug~~ — REMOVED 2026-04-30
- **Why removed:** No event-time → no event-time-bucketed `agg_windowed` → no bucket-epoch mismatch class of bug. The bug disappears as a side-effect of the architectural pivot. Phase 12.6 Path X (windowed ops use server-side `now_ms()`) makes the agg_windowed bucket arithmetic operate on monotonically-increasing arrival time, eliminating the late-event class entirely.

#### 3. `phase11_smoke::all_eleven_ops_round_trip_through_http` regression
- **Severity:** HIGH — was flaky pre-existing; now deterministic-fail (3/3 reruns this session)
- **Failure point:** `crates/beava-server/tests/phase11_smoke.rs:235` — `v["a"].as_f64().expect("a share")`
- **Hypothesis:** type_mix Map response missing key "a"; previously HashMap iteration nondeterminism, possibly made deterministic by a recent change
- **Documented as pre-existing in:** 12-07 SUMMARY + 12-08 SUMMARY
- **Why now:** Documented "pre-existing" but currently *deterministic* — needs root cause; could be a real Phase 11 op regression
- **Next action:** `/gsd-debug` to investigate
- **Estimated:** 1-2 hours

### 🟡 P1 — Functional completeness (v0-blocking)

#### 4. Plan 12-10 — push-and-get on mio HTTP+TCP
- **Status:** 📋 SCOPED, plan written (`12-10-PLAN.md` in phase 12 dir, 2158 lines, 23 tasks, 11 waves)
- **Unblocked by:** Plan 12-09's `GlueResponse::QueryResult { body, format }` shape ✓
- **Why now:** Atomic apply+query for fraud-decisioning; collapses 2 RTs to 1 (~500µs → ~250µs P50 latency target). Lands BEFORE Phase 12.6 surface reduction so 12.6 doesn't have to migrate a half-finished mio push-and-get path.
- **Triage notes** (carried in HANDOFF):
  - Python `OP_PUSH=0x0002` vs Rust `OP_PUSH=0x0010` inconsistency — document in 12-10 SUMMARY as v0.1 followup; do NOT fix in 12-10
  - Wave 6 may collapse into Wave 4.b — executor decides per RED/GREEN status of 6.a
- **Next action:** `/gsd-execute-phase 12` (will pick up 12-10 next)
- **Estimated:** 3-4 hours executor time (similar to 12-08/12-09)

#### 5. Phase 12-01..12-06 follow-up — DESCOPED post-pivot
- **Scope (post-pivot):** `push_sync`, `push_many`, `push_table`, `delete_table`, `set`, `mset`, `mget`, `get_multi`. Joins (event↔event / event↔table / table↔table) + `bv.union` + `as_of=` REMOVED 2026-04-30 per `project_redis_shaped_no_event_time_ever`.
- **Status:** 🟡 PARTIAL — on `.claude/worktrees/phase-12-followup` (off `phase-12-joins` @ d541971). The `phase-12-joins` branch contains plumbing that's now dead architecture; needs careful audit during merge.
- **Why now:** Redis-shaped multi-key ops; users expect these for the OSS surface
- **Next action:** Continue work on the existing worktree, but skip any join/union plans on the worktree
- **Estimated:** 3+ plans of work post-descope (was 5+; joins/union plans dropped)

#### 6. ~~Phase 14.1 — Streaming opt-in modifiability~~ — REMOVED 2026-04-30
- **Why removed:** Killed by no-event-time pivot. Stream modifiability is meaningless without event-time / out-of-order events.

### 🟡 P2 — Ship-readiness

#### 6. Phase 13 Hetzner sweep + shard-scaling validation
- **Scope:**
  - Hetzner Linux baseline + samply trace post-12-08+12-09
  - Multi-instance shard scaling test (1 / 2 / 4 / 8 instances on 16-vCPU box, key%N sharding)
  - Validates `project_no_sharded_apply` commitment ("scale via multi-instance Redis-cluster pattern")
- **Hardware note:** Use **CCX-class** Hetzner instance (dedicated cores), NOT basic CX (KVM-shared) — current Hetzner KVM box is poor representation of production hardware. Or move to AWS Graviton3 (c7g.4xlarge — the Valkey 1.19M test rig).
- **Why now:** Validates v0 perf claims for marketing/docs; surfaces any Linux-specific issues
- **Why also now:** If shard scaling is sub-linear, that's a real ship-blocker we'd want to catch before customers do
- **Next action:** Scope as `Plan 13.x` (hetzner-baseline + shard-scaling)
- **Estimated:** 1-2 hours implementation + 2-4 hours data collection

#### 7. Phase 13 docs + packaging (deferred from v0 ship per CONTEXT D-16, but at least minimum-viable for launch)
- PyPI package (`pip install beava`)
- Docker Hub image
- GitHub Releases binaries (Linux x86_64, Linux ARM64, macOS ARM64)
- Quickstart guide on beava.dev
- Architecture overview doc
- HTTP API reference
- Operator catalogue (already partially documented)
- **Why now:** Required for OSS launch; minimum-viable subset suffices for v0
- **Next action:** Incremental — start with PyPI + Docker + quickstart; iterate

#### 8. Phase 13 metric-counter wiring + cold-entity GC
- **Status:** 🟡 PARTIAL — on `.claude/worktrees/phase-13-followup` (off `phase-13-ship` @ 2ef5afc)
- **Why now:** Production observability — required for ops to monitor a running Beava instance
- **Next action:** Continue work on existing worktree

### 🟢 P3 — Post-v0 (do not block ship)

#### 9. Plan 12-11 — RecyclableBytes wrapper
- **Status:** Sketched (chat-only), conditional on post-12-08 samply showing residual memcpy worth harvesting
- **Estimated:** Post-Phase-13 sweep decision

#### 10. Plan 12-12 — Read-path Layers 1+2 (parked this session)
- **Layer 1:** reads bypass response_batch (~50-70 LOC)
- **Layer 2:** EVFILT_USER on Darwin + inline write before set_writable (~110 LOC)
- **Combined est. lift:** 1.6-2× on read throughput
- **Investigation reports preserved:** `/tmp/read-encode-overhead.md`, `/tmp/read-dispatch-loop.md`, `/tmp/read-transport-overhead.md`
- **Status:** PARKED 2026-04-29; revisit after Phase 13 Hetzner samply confirms which lifts deliver real value

#### 11. ~~Phase 15 — Event-time PIT temporal store~~ — REMOVED 2026-04-30
- **Why removed:** Killed by no-event-time pivot. Phase 11.5 LSN-keyed MVCC chain remains for `app.retract(event_id)`.

#### 12. Phase 18 wrap (housekeeping)
- Phase 18 SUMMARY.md
- Phase 18 verification (`/gsd-verify-work 18`)
- Worktree archival decision: `phase-13.3-lockless-apply` archived since Phase 13.3 REJECTED 2026-04-26
- Folded into Phase 12.6 dead-code/redundancy sweep
- **Estimated:** 30 min standalone, or absorbed into Phase 12.6

#### 13. Phase 25 — Session window operator family (NEW, v0.1+)
- **Status:** 📋 PLANNED (inserted 2026-04-30 from no-event-time pivot)
- **Why post-v0:** Not ship-blocker — users compose count/sum with processing-time windowed ops for v0 demos. Session windows are the v0.1 highlight feature.
- **Next action:** `/gsd-discuss-phase 25` after Phase 12.6 lands

## Recommended ordering for next session(s) — REVISED 2026-04-30

1. **Session 1 — phase11_smoke debug:** `/gsd-debug` to root-cause `tests/phase11_smoke.rs:235` deterministic failure. 1-2 hours. Standalone fix; not blocked by anything.
2. **Session 2 — Plan 12-10 execute:** `/gsd-execute-phase 12` picks up push-and-get. ~3-4 hours.
3. **Session 3+ — Phase 12.6 discuss → plan → execute:** The big v0 surface reduction (legacy axum kill + event-time strip + dead-code sweep + windowed-op time-source swap + join/union removal + REQUIREMENTS sweep). 12-15 plans, 3-4 weeks.
4. **Session N — Phase 13 ship work:** Hetzner+shard sweep + docs/packaging incrementally toward OSS launch.
5. **Post-v0 — Phase 25 session windows + Phase 14/14.1/15 reconsideration if/when needed (ADR required to revive event-time).**

## Out-of-scope (do not pursue without explicit user direction)

- **Event-time / watermarks / late-event correction / PIT temporal store / joins of any kind / `bv.union`** — LOCKED OUT permanently per `project_redis_shaped_no_event_time_ever` (2026-04-30). Reviving any of these requires explicit user override + new ADR.
- Multi-thread apply / sharding within a process (LOCKED OUT per `project_no_sharded_apply`)
- TLS support (deferred to v0.1+ per Phase 12-09 D-E)
- Tokio dual-runtime / dual hot-path entry (LOCKED OUT per `project_phase18_no_dual_runtime` + `project_redis_shaped_no_event_time_ever`)
- SSD overflow / persistence layer (architectural decision: in-memory only)
- Read-path performance work beyond Layers 1-2 (parked)

---

*Drafted by Claude Opus 4.7 on 2026-04-29. Revised 2026-04-30 for no-event-time architectural pivot.*
