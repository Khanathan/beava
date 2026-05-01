---
context: v0-ship-correctness-path
created: 2026-04-29
revised: 2026-04-30 — Phase 12.6 CLOSED (PASS-WITH-WARN); v0 SCOPE LOCKED to events-only per `project_v0_events_only_scope`; new Phase 12.7 (table strip) inserted as predecessor to reframed Phase 13 (SDK polish + benchmarks + ship); 27 orphan pre-pivot phase dirs archived
status: post-pivot-events-only
---

# Correctness path to v0 OSS ship — REVISED 2026-04-30 (Phase 12.6 closure)

**MAJOR PIVOT 2026-04-30:** Architectural simplification — no event-time / no watermarks / no joins / no PIT, ever. Phases 14, 14.1, 15 archived. **Phase 12.6 (NEW v0 surface-reduction blocker) CLOSED 2026-04-30 (PASS-WITH-WARN).** The original "Phase 14 streaming bug" P0 item is **DELETED** — the bug class disappears with event-time itself.

See `project_redis_shaped_no_event_time_ever` (memory) for the full architectural commitment.

## Priority tier (post-12.6-closure)

### 🟢 CLOSED — completed v0 ship-blockers

#### Phase 12.6 — v0 surface reduction — ✅ CLOSED 2026-04-30 (PASS-WITH-WARN)
- **Severity:** was CRITICAL — was v0 ship surface mismatch with the locked architectural commitment
- **Status:** ✅ CLOSED 2026-04-30 (PASS-WITH-WARN) — 15 plans landed (Plans 01-15 inclusive of Wave-1.5 gap closure 14+15); workspace 1067/0/3; HEAD `1e318b1`
- **What landed:** Legacy axum kill (~7475 LOC; plan estimated ~3500 — orphan tcp.rs + in-source test mods cascaded out) + event-time hard rip (push wire + register wire + EventDescriptor + DevAggState + WAL/snapshot v1→v2 + Python decorator) + Path X windowed-op time-source swap (event_time_ms → server now_ms()) + joins/unions removal (OpNode::Join/Union/JoinType deleted; structured-error rejection arms `feature_removed_no_*_v0`) + dead-code/redundancy sweep + mio-only hot-path enforcement (`phase12_6_mio_only_dataplane.rs` architectural test + CLAUDE.md `§Conventions § mio-only Hot-Path Invariant`) + REQUIREMENTS.md surgical sweep + Phase 12.5 / 13.3 archival banner sweep + microbench + throughput rebaseline
- **Verdict basis:** all 7 ROADMAP success criteria PASS or PASS-WITH-WARN; all 5 CONTEXT decisions D-01..D-05 honored verbatim; PASS-WITH-WARN on the deadcode buckets (planning-target overshoots categorized as strict-deny test fixtures + post-pivot doc-comments + out-of-plan-scope `tally/` legacy package; clippy-warning floor is 0 warnings)
- **Artifacts:** `.planning/phases/12.6-v0-surface-reduction/12.6-SUMMARY.md` (phase narrative) + `.planning/phases/12.6-v0-surface-reduction/12.6-VERIFICATION.md` (mechanical pass/fail)

### 🔴 P0 — v0 ship blockers (post-Phase-12.6 closure + v0-events-only commitment 2026-04-30)

#### 1. Phase 12.7 — Table strip (NEXT on v0 critical path) — NEW 2026-04-30
- **Severity:** CRITICAL — predecessor to final ship phase
- **Status:** 📋 PLANNED — CONTEXT.md stub at `.planning/phases/12.7-table-strip/12.7-CONTEXT.md`; not yet planned
- **Scope:** Strip `@bv.table` Python decorator + `POST /upsert/{table_name}` + `POST /delete` + `POST /retract` + `GET /table/{name}` mio handlers (added by Phase 12.6 Plan 14) + `temporal_http.rs` helpers + `TemporalStore` MVCC machinery + `app.retract(event_id)` SDK verb + ~14 table-related tests. Walks back Phase 11.5 + Phase 12.6 Plan 14's mio table surface. WAL/snapshot likely needs v2→v3 schema bump.
- **Why now:** Per `project_v0_events_only_scope` (locked 2026-04-30): v0 commits to events-only. Tables/joins/aggregation return together in v0.1+ if/when justified by demand. Phase 12.6 already deleted joins; tables have no v0 consumer; clean break is cheaper than maintaining a half-feature.
- **Next action:** `/gsd-discuss-phase 12.7` to resolve open product decisions (WAL schema bump policy, Phase 11.5 retro-banner shape, test deletion vs migration, migration message in WAL loader)
- **Estimated:** ~10 plans across 3-4 waves

#### 2. Phase 13 — SDK polish + benchmarks + ship (FINAL v0 phase) — REFRAMED 2026-04-30
- **Severity:** CRITICAL — final v0 ship gate
- **Status:** 🟡 PARTIAL (Plan 13-01 `/metrics` Prometheus + Plan 13-03 `env_var_overrides` hermetic fix shipped on `phase-13-ship` @ `2ef5afc`; remaining plans need rescoping post-Phase-12.7)
- **Scope (REFRAMED — drop bv.fork + playground + structured logs):** SDK polish on the events-only surface (`@bv.event` + 54-op catalogue + /push + /get + /register); perf gates on THREE pipelines (simple fraud / complex fraud / recommendation) ≥3M EPS, <10ms P99 batch-get; minimum-viable docs (quickstart → operators → http-api → architecture); `/metrics` Prometheus (already partially shipped); PyPI + Docker Hub image + GitHub Releases binaries (Linux x86_64, Linux ARM64, macOS ARM64); CI green; ship-ready tag. **DROPPED:** `bv.fork` subcommand, `playground.beava.dev`, structured logs.
- **Why this shape:** v0 = polish + benchmarks. User explicit framing 2026-04-30: "the last phase before open source is polishing sdk and crafting benchmarks."
- **Next action:** `/gsd-discuss-phase 13` AFTER Phase 12.7 closes (Phase 13 scope partly depends on what Phase 12.7 leaves behind)
- **Estimated:** ~10 plans (down from ~18; bv.fork + playground dropped)

#### 2. ~~Phase 14 — Streaming silent-data-loss bug~~ — REMOVED 2026-04-30
- **Why removed:** No event-time → no event-time-bucketed `agg_windowed` → no bucket-epoch mismatch class of bug. The bug disappears as a side-effect of the architectural pivot. Phase 12.6 Path X (windowed ops use server-side `now_ms()`) makes the agg_windowed bucket arithmetic operate on monotonically-increasing arrival time, eliminating the late-event class entirely.

#### 3. ~~`phase11_smoke::all_eleven_ops_round_trip_through_http` regression~~ — RESOLVED in Phase 12.6 Plan 01 / Plan 07
- **Resolution:** Plan 12.6-01 (D-02) rewrote line 235 from `v["a"].as_f64().expect("a share")` to `assert_type_mix_set_membership(v)` enforcing set-membership invariants on the type_mix Map response (5/5 stable reruns). Plan 12.6-07 subsequently DELETED `phase11_smoke.rs` because its async-router-based mechanism didn't survive the legacy axum kill; the set-membership invariant is now exercised end-to-end via the mio data-plane HTTP path during the rest of the workspace test suite (every TestServer-using test that pushes events to phase11_smoke fixtures runs the same invariant).
- **Status:** ✅ RESOLVED — workspace at 1067/0/3 with the invariant preserved across the file lifecycle.

### 🟡 P1 — Functional completeness (v0-blocking)

#### 4. ~~Plan 12-10 — push-and-get on mio HTTP+TCP~~ — DEFERRED entirely from v0 per Phase 12.6 D-04
- **Status:** 📋 DEFERRED — Plan 12-10 PLAN.md remains at `.planning/phases/12-server-side-async-push-coalescing/12-10-PLAN.md` for v0.0.x or v0.1+ revival. Phase 12.5 dir banner-stamped SUPERSEDED-AND-DEFERRED 2026-04-30 (Plan 12.6-09). Legacy `crates/beava-server/src/push_and_get.rs` (293 LOC) DELETED by Plan 12.6-07.
- **Why deferred:** Per Phase 12.6 CONTEXT D-04, v0 ships without push-and-get — users do 2 RTs (push then get). Future v0.0.x or v0.1+ revival requires explicit user decision.
- **Resolution:** v0 ship surface tightens; Phase 13 picks up the next ship-readiness items.

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

## Recommended ordering for next session(s) — REVISED 2026-04-30 (post Phase 12.6 closure)

1. ~~**Session 1 — phase11_smoke debug**~~ — RESOLVED in Phase 12.6 Plan 01 (D-02 set-membership rewrite + Plan 07 file deletion with invariant preservation).
2. ~~**Session 2 — Plan 12-10 execute**~~ — DEFERRED entirely from v0 per Phase 12.6 D-04.
3. ~~**Session 3+ — Phase 12.6 discuss → plan → execute**~~ — ✅ CLOSED 2026-04-30 (PASS-WITH-WARN). 15 plans landed across 8 waves.
4. **Session 4 (NEXT) — Phase 13 discuss → plan → execute:** `/gsd-discuss-phase 13` to capture remaining ship-readiness context (Hetzner Linux baseline + multi-instance shard-scaling validation per `project_no_sharded_apply`; PyPI / Docker / GitHub Releases packaging; quickstart docs; concept docs / operator docs / HTTP API docs sweep with no-event-time pivot — D-05 deferred work). Then plan + execute. 8+ plans across 2-3 weeks.
5. **Post-v0 — Phase 25 session windows + Phase 14/14.1/15 reconsideration if/when needed (ADR required to revive event-time).**

## Out-of-scope (do not pursue without explicit user direction)

- **Event-time / watermarks / late-event correction / PIT temporal store / joins of any kind / `bv.union`** — LOCKED OUT permanently per `project_redis_shaped_no_event_time_ever` (2026-04-30). Reviving any of these requires explicit user override + new ADR.
- **Second data-plane runtime / third caller of `apply_event_to_aggregations` / `axum::*` symbols outside `http_admin.rs`** — LOCKED OUT permanently per Phase 12.6 mio-only Hot-Path Invariant (CLAUDE.md `§Conventions § mio-only Hot-Path Invariant (locked Phase 12.6)`). Enforced by `phase12_6_mio_only_dataplane.rs` architectural test on every PR.
- **`event_time_ms` / `event_time_field` / `tolerate_delay_ms` / `bv.join` / `bv.union` / `OpNode::Join` / `OpNode::Union`** — LOCKED OUT permanently per Phase 12.6 D-03 hard rip. Wire schema rejects with structured 400 codes; Python decorator raises TypeError; OpNode variants deleted.
- Multi-thread apply / sharding within a process (LOCKED OUT per `project_no_sharded_apply`)
- TLS support (deferred to v0.1+ per Phase 12-09 D-E)
- Tokio dual-runtime / dual hot-path entry (LOCKED OUT per `project_phase18_no_dual_runtime` + `project_redis_shaped_no_event_time_ever`)
- SSD overflow / persistence layer (architectural decision: in-memory only)
- Read-path performance work beyond Layers 1-2 (parked)

---

*Drafted by Claude Opus 4.7 on 2026-04-29. Revised 2026-04-30 for no-event-time architectural pivot. Re-revised 2026-04-30 for Phase 12.6 closure (Plan 12.6-13).*
