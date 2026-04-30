---
context: v0-ship-correctness-path
created: 2026-04-29
status: drafted-pending-review
---

# Correctness path to v0 OSS ship

After this session's perf investigation work (Plans 12-08, 12-09 shipped; read-path Layers 1+2 parked), the remaining work to ship is mostly **correctness + functional completeness + ship-readiness**, not more performance.

## Priority tier

### 🔴 P0 — Correctness blockers (must fix before ship)

#### 1. Phase 14 — Streaming silent-data-loss bug in `agg_windowed`
- **Severity:** CRITICAL — silent data loss = product credibility blocker
- **Status:** 📋 PLANNED on roadmap, no plan written
- **Why now:** Any v0 launch with this unfixed is dangerous for fraud-decisioning use cases
- **Next action:** `/gsd-discuss-phase 14` to capture the failure mode + repro, then `/gsd-plan-phase 14`
- **Estimated:** 1-3 plans, depending on how deep the bug goes

#### 2. `phase11_smoke::all_eleven_ops_round_trip_through_http` regression
- **Severity:** HIGH — was flaky pre-existing; now deterministic-fail (3/3 reruns this session)
- **Failure point:** `crates/beava-server/tests/phase11_smoke.rs:235` — `v["a"].as_f64().expect("a share")`
- **Hypothesis:** type_mix Map response missing key "a"; previously HashMap iteration nondeterminism, possibly made deterministic by a recent change
- **Documented as pre-existing in:** 12-07 SUMMARY + 12-08 SUMMARY
- **Why now:** Documented "pre-existing" but currently *deterministic* — needs root cause; could be a real Phase 11 op regression
- **Next action:** `/gsd-debug` to investigate
- **Estimated:** 1-2 hours

### 🟡 P1 — Functional completeness (v0-blocking)

#### 3. Plan 12-10 — push-and-get on mio HTTP+TCP
- **Status:** 📋 SCOPED, plan written (`12-10-PLAN.md` in phase 12 dir, 2158 lines, 23 tasks, 11 waves)
- **Unblocked by:** Plan 12-09's `GlueResponse::QueryResult { body, format }` shape ✓
- **Why now:** Atomic apply+query for fraud-decisioning; collapses 2 RTs to 1 (~500µs → ~250µs P50 latency target)
- **Triage notes** (carried in HANDOFF):
  - Python `OP_PUSH=0x0002` vs Rust `OP_PUSH=0x0010` inconsistency — document in 12-10 SUMMARY as v0.1 followup; do NOT fix in 12-10
  - Wave 6 may collapse into Wave 4.b — executor decides per RED/GREEN status of 6.a
- **Next action:** `/gsd-execute-phase 12` (will pick up 12-10 next)
- **Estimated:** 3-4 hours executor time (similar to 12-08/12-09)

#### 4. Phase 12-01..12-06 follow-up
- **Scope:** event↔event windowed join, event↔table enrichment (with `as_of=` for temporal), table↔table join, `bv.union`, `push_sync`, `push_many`, `push_table`, `delete_table`, `set`, `mset`, `mget`, `get_multi`
- **Status:** 🟡 PARTIAL — on `.claude/worktrees/phase-12-followup` (off `phase-12-joins` @ d541971)
- **Why now:** Redis-shaped multi-key ops; users expect these for the OSS surface
- **Next action:** Continue work on the existing worktree
- **Estimated:** 5+ plans of work, multiple sessions

#### 5. Phase 14.1 — Streaming opt-in modifiability (Chunk B)
- **Status:** 📋 PLANNED
- **Why now:** Behavior correctness; downstream Phase 15 (event-time PIT) depends on this
- **Next action:** After Phase 14 lands

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

#### 11. Phase 15 — Event-time PIT temporal store
- **Status:** 📋 PLANNED
- **Depends on:** Phase 14.1 (streaming opt-in modifiability)

#### 12. Phase 18 wrap (housekeeping)
- Phase 18 SUMMARY.md
- Phase 18 verification (`/gsd-verify-work 18`)
- Worktree archival decision: `phase-13.3-lockless-apply` archived since Phase 13.3 REJECTED 2026-04-26
- **Estimated:** 30 min

## Recommended ordering for next session(s)

1. **Session 1 — investigation:** Phase 14 streaming bug discovery (`/gsd-discuss-phase 14`) + phase11_smoke regression debug (`/gsd-debug`). 1-2 hours total.
2. **Session 2 — fixes:** Land the Phase 14 fix + phase11_smoke fix. Time depends on what's found.
3. **Session 3 — Plan 12-10:** Execute push-and-get. ~3-4 hours.
4. **Session 4 — Phase 13 Hetzner+shard sweep:** Scope `Plan 13.x` with dedicated-core Hetzner + multi-instance scaling. ~4-6 hours including data collection.
5. **Sessions 5+:** Phase 12-01..06 follow-up + docs/packaging incrementally toward OSS launch.

## Out-of-scope (do not pursue without explicit user direction)

- Multi-thread apply / sharding within a process (LOCKED OUT per `project_no_sharded_apply`)
- TLS support (deferred to v0.1+ per Phase 12-09 D-E)
- Tokio dual-runtime (LOCKED OUT per `project_phase18_no_dual_runtime`)
- SSD overflow / persistence layer (architectural decision: in-memory only)
- Read-path performance work beyond Layers 1-2 (parked)

---

*Drafted by Claude Opus 4.7 on 2026-04-29. Pending user review next session.*
