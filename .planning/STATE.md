---
gsd_state_version: 1.0
milestone: v0.0
milestone_name: milestone
status: "Phase 13.0 CLOSED 2026-05-03 (PASS); v0 critical path = 4-way parallel 13.4 (engine) + 13.5 (Python+bench) + 13.6 (TS+Go) + 13.7 (docs site) → sequential 13.8 (packaging + GA tag)"
last_updated: "2026-05-03T22:30:00.000Z"
progress:
  total_phases: 45
  completed_phases: 21
  total_plans: 220
  completed_plans: 160
  percent: 73
---

<!-- Session continuity (resume) -->
<!-- Last session: 2026-05-01 — /gsd-execute-phase 12.7 Plan 10 landed (Wave 4: closure — STATE/ROADMAP/CORRECTNESS-PATH advance + CLAUDE.md § Events-Only Invariant block + Phase 12.7 SUMMARY/VERIFICATION). Phase 12.7 OFFICIALLY CLOSED 2026-05-01 (PASS) at HEAD 5645ead. 10 plans landed (Plans 01-10) across 4 waves. Workspace 1049/0/4 with cargo clippy + fmt clean. Architectural test pair (phase12_7_no_table_surface 3 tests + phase12_7_legacy_table_handlers_killed 6 tests) GREEN BY DEFAULT (no --include-ignored flag needed). All 4 CONTEXT decisions D-01..D-04 honored verbatim: D-01 RESET FORMAT_VERSION 2→1 across 3 schemas; D-02 forward-looking error framing 'not supported in v0' across 7 layers; D-03 two-file architectural-test pair locks events-only at CI level; D-04 comprehensive REQUIREMENTS sweep (8 REQ-IDs DESCOPED + V0-EVENTS-ONLY-01 anchor) + 11.5 retro-descope banner on 3 files. ~5,500 LOC removed cumulatively (Plans 03/04/06 dominant). Microbench: 3 cells SIGNIFICANTLY FASTER (-25 to -30%); throughput +7.3% on small/tcp regression-gate cell. Two PLANNER-SURFACED CONCERNs documented for user review in 12.7-SUMMARY: SDK-AGG-* operator-family REQ-IDs LEFT ACTIVE (Concern 1) and D-04 wildcard discrepancy (Concern 2). Section-ownership honored across Plans 07 (REQUIREMENTS) + 08 (11.5 banners) + 09 (perf/throughput baselines) + 10 (STATE/ROADMAP/CORRECTNESS-PATH/CLAUDE.md). Per CLAUDE.md TDD §Note 4, doc-only closure plan: chore: prefix for code housekeeping (commit 5645ead) + docs: prefix for SUMMARY/VERIFICATION/closure (commits 6db1881 + this final closure commit). -->
<!-- Stopped at: Phase 12.8 Plan 07 landed (Wave 3 — REQUIREMENTS.md positive anchors V0-MEM-GOV-01/02/03 added under § V0-INVARIANT, mirroring 12.7-07 V0-EVENTS-ONLY-01 placement); ready for /gsd-execute-phase 12.8 Plan 08 (Wave 4 — microbench + throughput rebaseline) per `.planning/phases/12.8-memory-governance/12.8-08-PLAN.md`. -->
<!-- Resume files: .planning/phases/12.7-table-strip/12.7-SUMMARY.md (phase narrative) + .planning/phases/12.7-table-strip/12.7-VERIFICATION.md (mechanical pass/fail) + .planning/perf-baselines.md::Phase 12.7 (regression-tripwire baseline for Phase 13) + .planning/throughput-baselines.md::Phase 12.7 (8-cell baseline for Phase 13). -->
<!-- Phase 12.8 progress (2026-05-01): Plans 01–07 LANDED at HEAD 1fe058e. Plan 01 (4th JSON-prelude shim env-gated) + Plan 02 (cold_after kwarg + EventDescriptor.cold_after_ms field) + Plan 03 (cold-entity TTL eviction on apply hot path with last_seen_ms sidecar) + Plan 04 (54-op lifetime bound table + BoundedByRequiredKwarg presence-check) + Plan 05 (architectural test phase12_8_lifetime_ops_have_bounds — single-file CI-tripwire, GREEN-by-default) + Plan 06 (5 Prometheus metric families on /metrics + env-gate OFF→ON flip + Test 21 escape-hatch) + Plan 07 (REQUIREMENTS.md V0-MEM-GOV-01/02/03 positive anchors under § V0-INVARIANT, mirroring 12.7-07 V0-EVENTS-ONLY-01 placement byte-for-byte). Workspace +31 new tests cumulative across 12.8 (15 Rust unit tests in op_lifetime_bounds.rs + 5 Python E2E tests + 3 architectural tests + 8 metric-endpoint tests). Plan 06 RED: 8295259; GREEN: 41b2f68. Plan 07 GREEN: 1fe058e (single docs: commit per CLAUDE.md TDD §Note 4). Wave 2 COMPLETE (Plans 03+04). Wave 3 COMPLETE (Plans 05+06+07 — last of Wave 3). Remaining waves: Wave 4 (Plan 08 microbench + throughput rebaseline), Wave 5 (Plan 09 closure — STATE/ROADMAP/CORRECTNESS-PATH/CLAUDE.md/12.8-SUMMARY/12.8-VERIFICATION). Plan 06 default-flipped BEAVA_MEMORY_GOV_ENFORCE OFF→ON; explicit escape hatch BEAVA_MEMORY_GOV_ENFORCE=0; Plan 06 also moved Test 21 from Plan 04 per wave-3 ownership shift. -->
<!-- Resume Phase 12.8: next plan 08 per .planning/phases/12.8-memory-governance/12.8-08-PLAN.md (Wave 4 — microbench + throughput rebaseline). -->

<!-- Phase 12.8 OFFICIALLY CLOSED 2026-05-01 (PASS-WITH-WARN). Plan 08 landed at HEAD 8986683 (microbench cold-TTL on/off -2.6% within ±5% gate; 8-cell throughput rebaseline; small/tcp regression-gate -2.5% PASS; fraud-team/tcp -21.3% + fraud-team/http -29.8% WARN flagged for Phase 13 — root cause: O(N_tables) entity_count_resident snapshot, fraud-team has 9 tables vs 1-4 on simpler shapes; not gating per CLAUDE.md). Plan 09 closure (this plan) lands SUMMARY + VERIFICATION + CLAUDE.md § Memory Governance Invariant block + STATE/ROADMAP/CORRECTNESS-PATH advance. Plan 09 was originally spawned via gsd-executor agent which died on socket error after 55 tool calls; closure work completed manually inline by orchestrator per `feedback_logistics_autonomy` (mechanical doc work, no spec drift risk). All 4 CONTEXT decisions D-01..D-04 honored verbatim. Workspace: 1095/0/4. SUMMARY: .planning/phases/12.8-memory-governance/12.8-SUMMARY.md. VERIFICATION: .planning/phases/12.8-memory-governance/12.8-VERIFICATION.md. -->

<!-- Resume next: /gsd-discuss-phase 13 (final v0 ship — SDK polish + perf benchmarks on three pipelines + minimum-viable docs + PyPI/Docker/GitHub Releases packaging). Phase 13 candidates from 12.8 follow-ups: (a) fraud-team throughput root-cause fix (entity_count_resident amortization); (b) top_k → BoundedByRequiredKwarg promotion; (c) bytes_per_entity_p99 dynamic sampling; (d) per-source metric labels. -->

<!-- Phase 12.9 INSERTED 2026-05-03 between 12.8 and 13 (gates Phase 13 ship-pitch numbers). Triggered by post-Phase-12.8 r8g maxcard bench investigation (this session): size_of::<AggOp>() = 600 bytes because of unboxed SeasonalDeviationState. Box 7 fat variants (SeasonalDeviation + HourOfDayHistogram + EventTypeMix + GeoVelocity + GeoSpread + GeoDistance + DistanceFromHome) → drops to ~72 bytes (8× shrink); fraud-team weighted-avg per-entity ~22 KB → ~6 KB (clears CLAUDE.md 7 KB budget). Investigation doc: .planning/ideas/per-entity-memory-budget.md. Tests: crates/beava-core/tests/per_entity_size_dump.rs (size_of dump + per-derivation projection). Estimated 3 plans across 2 waves (red/green/verify). Requires FORMAT_VERSION bump 1→2 (third schema change in 4 days — care with persistence test matrix). Phase 26 (Valkey IO rework) added as v0.1+ slot — NOT a v0 ship-blocker. New v0 critical path: /gsd-discuss-phase 12.9 → execute → /gsd-discuss-phase 13 → execute → ship. -->

<!-- Phase 12.9 OFFICIALLY CLOSED 2026-05-03 (PASS). Executed inline (no /gsd-discuss-phase / /gsd-plan-phase ceremony — design decisions were already locked in 12.9-CONTEXT.md from the investigation doc). 3 plans landed: Plan 01 boxing red+green (commits ee87d02 test, d3eed60 feat); Plan 02 perf gate (no code commit — 3-run fraud-team/tcp throughput verification: median 109,895 EPS, +6.9% vs Phase 19.4-04 quiescent baseline 102,800 EPS; PASS); Plan 03 closure (this commit — SUMMARY + VERIFICATION + perf-baselines + throughput-baselines + CLAUDE.md amendment + STATE/ROADMAP advance). Plus 1 orthogonal cleanup commit f5caba7 (pre-existing fmt issues in beava-bench-v2.rs + phase12_8_memory_gov_apply.rs). NO FORMAT_VERSION bump needed (D-03: serde Box<T> is transparent; bincode wire format unchanged; verified by 1097/0/4 incl. snapshot round-trip tests). size_of::<AggOp>() dropped 600 → 80 bytes (7.5× shrink). user_id entity inline-slot dropped 46.8 KB → 6.2 KB. Workspace 1097/0/4. Clippy + fmt clean. Phase 11 D-08 explicit-no-boxing comment overridden after empirical verification (fraud-team/tcp +6.9%, no regression — likely cache-locality lift). aggop_size_within_cap test promoted to permanent CI tripwire (cap = 80 B). Two PLANNER-SURFACED CONCERNs deferred to Phase 13: (1) dynamic bytes_per_entity_p99 sampling (D-04 deviation; ~30 LOC); (2) r8g maxcard end-to-end memory rebench (Phase 13 Hetzner work). SUMMARY: .planning/phases/12.9-aggop-memory-boxing/12.9-SUMMARY.md. VERIFICATION: .planning/phases/12.9-aggop-memory-boxing/12.9-VERIFICATION.md. -->

<!-- Resume next: /gsd-discuss-phase 13 (final v0 ship — SDK polish + perf benchmarks (3 pipelines) + minimum-viable docs + PyPI/Docker/GitHub Releases packaging). Phase 13 should fold the two Phase 12.9 follow-up concerns into its plan list: (a) dynamic bytes_per_entity_p99 sampling (~30 LOC in agg_state.rs); (b) r8g maxcard end-to-end memory rebench as part of the Hetzner/multi-instance shard-scaling validation. Also fold Phase 12.8's outstanding follow-ups: top_k → BoundedByRequiredKwarg promotion; per-source metric labels; fraud-team WARN root cause (entity_count_resident O(N_tables) snapshot — c9597c7 was reverted). Pre-pivot Plan 13-XX list at .planning/phases/13-sdk-batch-push-api-op-push-batch-opcode/13-CONTEXT.md is stale; Phase 13 needs context refresh. -->

<!-- Phase 13 RESTRUCTURED 2026-05-03 from the v0-launch design session. After 1-by-1 review of 17 design questions (Q1-Q17 + several sub-questions), 20 SDK design decisions locked + 3 SDK ports confirmed (Python + TypeScript + Go; Java deferred) + 2 v0.1 directions captured (historical extraction engine + Polars dataframe ergonomics). Phase 13 is now an UMBRELLA for 6 sub-phases: 13.0 (design contract + spec docs — THE BOTTLENECK, 5-7 days) → parallel 13.4 (engine prep, 4-5d) + 13.5 (Python SDK + beava bench CLI, 7-10d) + 13.6 (TS+Go SDKs, 5-7d) + 13.7 (docs site, 4-6d) → sequential 13.8 (packaging + GA tag, 5-7d). Total wall-clock: ~6-7 weeks solo / ~3-4 weeks with 3+ contributors. KEY ARCHITECTURAL OVERTURN (Q1): @bv.table decorator REVIVED for aggregation-output ONLY (no upsert/delete/retract; no MVCC; no joins) — partial overturn of project_v0_events_only_scope, ADR-001 lands in Phase 13.0. KEY WIRE CHANGE (Q4): rename ops avg→mean / variance→var / stddev→std / count_distinct→n_unique / percentile→quantile across server+SDK+fraud-team.json (mechanical, FORMAT_VERSION stays at 1). KEY API SHAPES: dict-style push.push("EventName", {fields}); row-shape get(table, key) → dict; heterogeneous batch_get([(table, key), ...]) → list-of-dicts; verb-style HTTP routes all-POST + JSON body; bv.App() no-URL = embed mode (in-memory default); schema evolution additive-default + force=True for destructive (with diff matrix) + dry_run=True flag; cold-start returns {} but schema/field errors raise + batch atomic. NEW CRITICAL ARTIFACTS: docs/wire-spec.md (canonical JSON contract for SDK porters), docs/sdk-api/{python,typescript,go,shared}.md, docs/pipeline-dsl/, docs/operators/ (53 ops), examples/wire/, examples/{python,typescript,go}/, ADR-001 + ADR-002. Bookkeeping committed pre-13.0: .planning/ideas/v0.1-deferrals.md (master index), .planning/ideas/v0.1-historical-extraction-engine.md (meaty SpeeDB+SlateDB v0.1 architectural spec). Resume next: /gsd-discuss-phase 13.0 to gather any remaining context for the design contract phase. -->

<!-- Phase 13.0 CONTEXT gathered 2026-05-03 via /gsd-discuss-phase 13.0 (default mode, 4 single-question turns; user picked the recommended option on each). 4 decisions locked: D-01 wire-spec rigor = Markdown + JSON Schema (per-opcode, machine-validatable) + per-endpoint examples in examples/wire/*.json (highest rigor for SDK porters in Phase 13.6); D-02 operator catalog = ONE PAGE PER OP (53 pages under docs/operators/<op>.md, each with signature/semantics/return-type/complexity/worked-example/edge-cases/wire-link — devex-first per project_v2_devex_first; matches DuckDB/Polars conventions; SEO-friendly per-op landing pages on beava.dev for Priya target user); D-03 examples = RUNNABLE against language-local mock during 13.0 + re-verified against real engine post-13.4 (9 vertical demo files: examples/{python,typescript,go}/{adtech,fraud,ecommerce}.{py,ts,go}; doubles as integration regression test); D-04 pipeline-DSL compilation rules = PER-CHAIN-OPERATOR worked examples + ambiguity matrix (~12-15 examples + ~20-row matrix with ALLOWED/FORBIDDEN/UNDEFINED rulings + fixture/error-code links; SDK porters in 13.6 don't need to reverse-engineer Python source). Defaults set autonomously per feedback_logistics_autonomy: stale docs/*.md = nuke-and-rewrite (cost-class.md kept); ADRs = lightweight Nygard pattern (~1 page each); plan/wave shape = planner's discretion. Estimated 10-12 plans across 3 waves (matches ROADMAP §13.0 estimate). CONTEXT: .planning/phases/13.0-design-contract-spec-docs/13.0-CONTEXT.md. DISCUSSION-LOG: .planning/phases/13.0-design-contract-spec-docs/13.0-DISCUSSION-LOG.md. Resume next: /gsd-plan-phase 13.0 to break into ~10-12 plans across 3 waves. -->

<!-- Phase 13.0 PLANNED 2026-05-03 via /gsd-plan-phase 13.0 (research → plan → 3-iteration check loop). RESEARCH (1550 lines, agent gsd-phase-researcher) at 13.0-RESEARCH.md surfaced 8 open questions; 2 user-locked (Q1 Path B = bv.sum(field: str) only / two-stage with_columns().agg() pattern + inline boolean-sum FORBIDDEN; Q7 OP_RESET = 0x0040), 6 logistics-autonomous (researcher-recommended defaults). Plan checker found 4 BLOCKERs + 5 WARNINGs in iter-1 (missing docs/quickstart.md; rate_of_change family-directory mismatch decay/→velocity/; Plan 04 bv.sum signature narrowing missing; Plan 14 MockApp.push no-op stub defeating D-03 integration-regression intent), all resolved at commit ee013b7. Iter-2 found 2 BLOCKERs (incomplete propagation of rate_of_change family swap into Plan 04 family-table + Plan 15 docs/index.md), resolved at commit 0a9b747. Iter-3 PASS — all 9 prior issues clean; 3 stale-pattern greps return 0 hits; family totals = 54 across all plans. 15 PLAN.md files committed: Wave 1 (Plans 01-04: NUKE+ADRs / wire-spec / http-api / sdk-api), Wave 2 (Plans 05-12: scaffold + 6 family-polish + DSL+schema-evolution+error-codes), Wave 3 (Plans 13-15: concepts/architecture / examples+quickstart / closure). Family layout: core(8) + sketch(5) + point-ordinal(5) + recency(10) + decay(6) + velocity(9) + buffer-geo(11) = 54 op pages (53 unique AggKind variants + ema alias inline in ewma.md). 14 of 15 plans use TDD §Note 4 doc-only-plan exemption (single docs(13.0-NN): commit); Plan 13.0-02 ships the JSON Schema validator test (red→green within plan). Doc-only phase exemption from §Performance Discipline microbench/throughput-run noted in closure plan VERIFICATION. RESEARCH/PLAN commits: b0ea0da (initial 15 plans) → ee013b7 (iter-1 9 fixes) → 0a9b747 (iter-2 2 fixes). Resume next: /gsd-execute-phase 13.0 (start with /clear for fresh context window). -->

<!-- Phase 13.0 OFFICIALLY CLOSED 2026-05-03 (PASS). 16 plans across 3 waves: Wave 1 (Plans 01-04: NUKE 14 stale docs + 2 ADRs + wire-spec + http-api + 4 SDK API specs), Wave 2 (Plans 05-12: 54 op page stubs + 6 family-polish waves + pipeline-DSL + schema-evolution + error-codes), Wave 3 (Plans 13-16: 9 concept+architecture + 9 vertical demos + closure + per-op test suite). Plan 16 (per-operator high-volume integration test suite) added mid-execution per user directive ("write test for each operator as if we are users testing in python"). ADR-003 added mid-execution per user directive ("ship both / do both" — global aggregation + bv.lit). Approximately 158 doc + script + fixture + test artifacts shipped: 14 NUKE deletions + 15 dir stubs + 3 ADRs (001 @bv.table partial overturn + 002 Polars op renames + 003 global-agg + bv.lit) + 1 wire spec + 13 JSON Schema 2020-12 contracts + 20 worked-example fixtures + 1 HTTP API spec + 4 SDK API specs + 54 op pages + 7 family indexes + 1 master catalog index + 5 pipeline-DSL+schema+errors docs + 9 concept+architecture docs (includes new docs/concepts/global-aggregation.md from Plan 15) + 9 vertical demos + 3 mock backends + 1 smoke test + 1 docs/index.md + 1 SUMMARY + 1 VERIFICATION + 13-file Python integration test suite (Plan 16: 68 tests across 13 files / ~3,784 LOC). All 4 CONTEXT decisions D-01..D-04 honored verbatim; all 7 locked Q answers honored. ADR-003 mid-execution scope amendment landed in single closure commit per CLAUDE.md TDD §Note 4 doc-only-plan exemption: ADR-003 file + REQUIREMENTS.md V0-GLOBAL-AGG-01/02 + V0-LIT-01 anchors + 13 doc patches across wire-spec.md / 4 sdk-api files / 3 pipeline-dsl files / operators/index.md / quickstart.md + 4 new wire fixtures + 1 schema description patch + 1 new concept doc (global-aggregation.md) + ROADMAP §13.4/13.5/13.6 amendments adding implementation deferrals. Mechanical implementation deferred to: Phase 13.4 (engine sentinel routing + server-side op rename + architectural-test allowlist update for OpNode::Table* on derivations per ADR-001) → Phase 13.5 (Python SDK rewrite + 21 missing op helpers + bv.lit export + global-agg surface) → Phase 13.6 (TS + Go SDKs + bv.lit + global-agg overloads / GetGlobal method) → Phase 13.7 (docs site rendering / publishing). All 3 final-gate scripts exit 0 at closure: examples/wire/_validate_examples.py (20 examples validate), scripts/check_op_page_coverage.py (54 op pages match catalogue), bash examples/test_examples.sh (9/9 demos pass). SUMMARY: .planning/phases/13.0-design-contract-spec-docs/13.0-SUMMARY.md. VERIFICATION: .planning/phases/13.0-design-contract-spec-docs/13.0-VERIFICATION.md. ADR-003: .planning/decisions/ADR-003-global-aggregation-and-bv-lit.md. Resume next: 4-way parallel /gsd-discuss-phase 13.4 + 13.5 + 13.6 + 13.7 (independent post-13.0). -->

<!-- Plan 12.8-06 closed 2026-05-01 at HEAD 41b2f68. 5 Prometheus metric families on /metrics admin sidecar (cold_entity_evictions_total counter, lifetime_op_cap_hit_total counter aggregating EntropyStateWrap::categories_capped_count, entity_count_resident gauge, bucket_reclaim_total counter, bytes_per_entity_p99 gauge static = 7000 per PROJECT.md). 3 process-static atomic counters in agg_state.rs (ColdEntityEvictionCounter, BucketReclaimCounter, EntityCountResidentSnapshot) mirroring the existing EntropyStateWrap pattern; AdminState plumbing skipped per Rule 3 deviation (process-static is consistent with sibling counters; per-source labels still v0.0.x deferred). Counter inc()-sites wired at agg_apply.rs Plan 03 eviction site + agg_windowed.rs::evict_oldest_bucket; gauge store-site wired in apply_shard.rs::dispatch_push_sync post-apply block (under existing table lock; O(N_tables) sum). Env-gate BEAVA_MEMORY_GOV_ENFORCE flipped OFF→ON (apply_shard.rs::memory_gov_enforce_enabled now `!= Some("0")`); per-call read preserved (no OnceLock per Plan 06 B-02 fix). Test fixture sweep: phase12_8_unbounded_op_in_lifetime_mode.rs::test_no_enforcement_when_env_unset → renamed test_default_enforcement_on_when_env_unset, semantic-flipped to assert default-ON behavior; Test 21 (test_env_var_zero_disables_enforcement) lives in phase12_8_metrics_endpoint.rs per Plan 06 wave-3 ownership shift. v0 ships UNLABELED counters (no `{source=...}` block); per-source labels v0.0.x deferred. Workspace 1095/0; clippy/fmt clean. RED commit 8295259 (526-LOC test file); GREEN commit 41b2f68 (8 substeps atomically). SUMMARY: .planning/phases/12.8-memory-governance/12.8-06-SUMMARY.md. -->

<!-- Plan 12.8-07 closed 2026-05-01 at HEAD 1fe058e. REQUIREMENTS.md V0-MEM-GOV-01/02/03 positive anchors added under existing § V0-INVARIANT subsection (line 200, created by 12.7-07 alongside V0-EVENTS-ONLY-01). 3 anchors land at lines 203–205 between V0-EVENTS-ONLY-01 (line 202) and § SRV-REG (line 207); blank-line discipline preserved. V0-MEM-GOV-01 cites phase12_8_cold_entity_eviction.rs + Plans 02/03/06 (cold-entity TTL opt-in + FRESH-on-resurrect Redis pattern); V0-MEM-GOV-02 cites phase12_8_lifetime_ops_have_bounds.rs + op_lifetime_bounds.rs + Plans 01/04/05 (lifetime ops declare bounds at register-time, 4th JSON-prelude shim, default-ON via BEAVA_MEMORY_GOV_ENFORCE); V0-MEM-GOV-03 cites BucketReclaimCounter + agg_windowed.rs test mod + Plan 06 (per-event bucket reclaim during update_at, no new mechanism, locks the existing contract). Section-ownership held: only `.planning/REQUIREMENTS.md` modified in commit 1fe058e (STATE.md / ROADMAP.md / CORRECTNESS-PATH.md / CLAUDE.md owned by closure Plan 09). +3 / -0 LOC. Single docs(12.8-07): commit per CLAUDE.md TDD §Note 4 doc-only-plan exemption. SUMMARY: .planning/phases/12.8-memory-governance/12.8-07-SUMMARY.md. Wave 3 COMPLETE — all 3 of Plans 05+06+07 landed. Remaining: Plan 08 (Wave 4 microbench + throughput rebaseline), Plan 09 (Wave 5 closure). -->

<!-- Session resumed 2026-05-03 — /gsd-resume-work read HANDOFF.json post-bench follow-ups; user picked Track 1 (per-entity memory investigation). Proceeding via /gsd-quick: write microbench that registers each AggOp variant individually, prints std::mem::size_of of the AggOp state + per-entity overhead summed for fraud-team's actual aggregation declarations vs measured ~22 KB, then write .planning/ideas/per-entity-memory-budget.md. Empirical r8g maxcard data: small ~1 KB / medium ~5.6 KB / large_phase9 ~18 KB / fraud-team ~22 KB vs CLAUDE.md 7 KB budget — 3× over. Likely heaviest contributors to investigate: HLL count_distinct, UDDSketch percentile, TopK SpaceSaving, histogram bounded-buffer ops, bv.entropy categorical buckets, Phase 9 EWMA + inter_arrival_stats ring buffers. Output decides v0 ship-pitch numbers (cut overhead 3× / reframe pitch / recommend bigger node / combo). HANDOFF.json renamed → HANDOFF.consumed-2026-05-03.json; .continue-here.md retained as durable narrative. Track 2 (commit bench-v2 + valkey doc) and Track 3 (open Phase 13) deferred until investigation lands. -->

# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md` (26 phases — see roadmap for the full inserted-phase note)
**Requirements:** `.planning/REQUIREMENTS.md`
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Last revised:** 2026-04-26 (Phase 19 CONTEXT.md captured at `.planning/phases/19-1m-bench/19-CONTEXT.md` — 4 areas locked: blast shape (4 modes side-by-side), pipelining (continuous + burst), Python harness via public app.push() multi-process, WIP stash receiver-flips-stop pattern. Phase 18 wrap items remaining: SUMMARY + verification + worktree archival decision)

**Session resumed:** 2026-04-27 — Phase 19.1 family fully complete (verdict PASS, HEAD `3e28b77`). Phase 19.2 consolidated from prior 19.2 + 19.3 + two opus audit findings.

**Phase 19.2 CONTEXT captured 2026-04-27** at `.planning/phases/19.2-big-apply-path-optimization/19.2-CONTEXT.md` (commit `666099b`) — 8 decisions locked across 7 questions:

- D-01: Field pre-extraction = indexed array (`Vec<&Value>`, register-time field-idx)
- D-02a/b: Process-static AHasher init + FxHasher for HLL ops
- D-03: EntityKey hybrid (`SingleU64`/`SingleStr`/`Multi`)
- D-04: Cluster shape = split-by-agg_id (shared EntityKey + lookup; per-agg `Vec<AggOp>`)
- D-05: **Remove `bv.unique_cells` + `bv.geo_entropy`** (catalogue 55→53; recipes: `count_distinct(quadkey)`, `entropy(quadkey)`)
- D-05a: Apply `max_categories` cap + drop-new + cap-hit metric to `bv.entropy`
- D-06: Cost-class metadata = hand-maintained `docs/operators/cost-class.md`
- D-07: `/debug/op-cost` endpoint feature-gated behind `BEAVA_DEV_ENDPOINTS=1`
- D-08: `apply_path_bench.rs` criterion microbench + Phase 19.2 rebaseline matrix

**Next:** `/gsd-plan-phase 19.2` to break into 6-8 plans across 3-4 waves.

---

**Phase 19.3 CLOSED 2026-04-28 at PASS-WITH-DEFICIT.** D-04 architectural fix landed (Plan 19.3-02: `WindowedOp::update_at`). Wrapper-bypass anti-pattern resolved. Performance lift +4.4% EPS / -1,526 ns agg-stage (within run-to-run noise band). Predicted lift was 60% overestimated due to cost-model conjecture in `19.2-INVESTIGATION.md §4`; flamegraph + cost-model investigation (`19.3-COST-MODEL.md`, `19.3-FLAMEGRAPH.md`) identified 5 NEW levers and superseded Plans 19.3-03/04/05. Memory `feedback_cost_model_from_flamegraph` saved.

**Phase 19.4 OPENED 2026-04-28** at `.planning/phases/19.4-final-100k-push/` — final v0 ship-gate optimization, flamegraph-derived scope. Goal: lift fraud-team K=10k zipfian from 73,743 EPS (post-19.3-A) to ≥100,000 EPS (PASS gate).

**Phase 19.4 sub-goals (all PASSED — final verdict 102,800 EPS at Plan 04 closure):**

1. **19.4-A** CountDistinct identity-hasher fix (`std::HashSet<u64>` rehashes via SipHash) → ~85k EPS (-1,180 ns/event, ~3h work) — **PASS** (79,367 EPS / 11,667 ns agg-stage)
2. **19.4-B** ExtractedFields SmallVec inline-cap 8→16 (TxnByUser cluster spills) → ~91k EPS (-530 ns/event, 1-line) — **PASS attempt #3** at quieter load (96,298 EPS / 10,329 ns agg-stage)
3. **19.4-C** Geo lat/lon pre-extraction (D-01 missed geo path) → ~94k EPS (-360 ns/event, ~4h) — **PASS** first attempt (94,733 EPS / 8,244 ns agg-stage; samply confirms `agg_geo::read_lat_lon` slow path eliminated, 0.000% self-time was 2.86%)
4. **19.4-D** ExtractedFields hoist above descriptor loop (carried from 19.3-04) → ~105k EPS (-1,200 ns/event predicted, -100 ns measured trace; hoist correctness confirmed by criterion -10.9%) — **PASS-on-EPS-goal** (102,800 EPS clears 100k Phase-19.4 PASS gate; trace-floor missed because cost model overstated post-Plan-02 cap-widening)
5. **19.4-E** Sanity flamegraph + throughput rebaseline + dual-measurement verification + Phase 19 closure — **PASS** (3 of 4 predicted hot-function shifts confirmed; 5-pipeline rebaseline no WARN/BLOCK; anti-pattern sweep 7/7 PASS; Phase 19 amended PASS-WITH-DEFICIT → PASS)

**Phase 19.4 CLOSED 2026-04-28 at PASS.** fraud-team K=10k zipfian sustained_eps cumulative trajectory:

- post-19.3 12,533 ns / 73,743 EPS → post-19.4-01 11,667 ns / 79,367 EPS → post-19.4-02 10,329 ns / 96,298 EPS → post-19.4-03 8,244 ns / 94,733 EPS → **post-19.4-04 8,344 ns / 102,800 EPS (Plan 04 closure measurement)** = +39% over the phase, **clears 100k v0 ship gate**.

**Phase 19 verdict amended PASS-WITH-DEFICIT → PASS** (cumulative path: Phase 19.1 bench wall-clock fix amendment + Phase 19.2/19.3/19.4 chained apply-path optimizations).

**Phase 19.5+ pivots to scale-out** (sharding deployment + multi-instance benchmarks per `project_no_sharded_apply`); vertical optimization stops here. **Phase 19.5 is OUT OF v0 ship critical path.**

**Next: v0 ship critical path:** Phase 14 → 15 → 12 followup → 12.5 → 16 → 13 followup → ship.

---

**Plan 12-07 closed 2026-04-29** (commit `9bb18c7`). Production binary on ServerV18; /get works HTTP+TCP without env-var workarounds; read_bench.py end-to-end ok=1000/1000 with p99=1.81 ms. Wave-by-wave: WireRequest TcpGet variants → TCP parser routing → apply_shard dispatch → real dispatch_get_batch (replaces stub) → OP_GET_RESPONSE = 0x0023 + TCP encoder → /health shim on mio HTTP listener → main.rs migrated to ServerV18 + Config::admin_addr → integration tests + read_bench.py validation → criterion microbench + throughput rebaseline. **Throughput regression-gate: small/tcp 694,144 EPS post-12-07 vs 642,760 EPS post-19.4 = +8.0% (PASS).** Plan 12-08 (push-and-get over mio HTTP+TCP) is unblocked. SUMMARY: `.planning/phases/12-server-side-async-push-coalescing/12-07-SUMMARY.md`.

**Verification artifacts (commit `ff5579a`):**

- `.planning/phases/19.4-final-100k-push/19.4-VERIFICATION.md` — OVERALL: PASS, full evidence
- `.planning/phases/19.4-final-100k-push/19.4-FLAMEGRAPH-POST.md` — sanity flamegraph + artifact analysis
- `.planning/phases/19.4-final-100k-push/19.4-05-SUMMARY.md` — plan summary
- `.planning/throughput-baselines.md` — ## 1M-event blast (rebaseline 19.4) section
- `.planning/perf-baselines.md` — ### Phase 19.4 — 19.4-E Final cumulative baseline section
- `.planning/phases/19-1m-bench/19-VERIFICATION.md` — Amendment 2026-04-28 (Phase 19.4 closure)
- `.planning/phases/19-1m-bench/19-SUMMARY.md` — verdict updated 2026-04-28

**Phase 19.1 OPENED 2026-04-27** as the consolidated umbrella for the post-Phase-19 follow-up work (rolls together what was originally proposed as 19.0.1 / 19.0.2 / 19.0.3 mini-phases). See ROADMAP.md → "Phase 19.1: Realistic-shape benchmark + bench/WAL fixes + complex-pipeline optimization" for the full goal/sub-goal/success-criteria block.

**Phase 19.1 scope:**

1. **Path B — fraud-team.json validation** (primary tuning benchmark; locked decision per memory `project_fraud_team_primary_bench`)
2. **Bench wall_clock fix** (1-line elapsed-move + tokio::select! per memory `project_phase19_bench_wallclock_fix`; flips Phase 19 verdict PASS-WITH-DEFICIT → PASS)
3. **WAL config bump** (4×32MiB tick=20ms middle-ground default candidate per memory `project_phase19_wal_experiment`; experimental 8×64MiB tick=100ms eliminated bimodal tail with +33% EPS but 512MB RSS)
4. **Re-baselined Phase 19 numbers** (re-run small/medium/large/large_phase9 + new fraud-team.json zipfian cell; amend 19-VERIFICATION verdict)
5. **Complex-pipeline apply-thread optimization** (≥1 of: WindowedOp lazy buckets / same-key batch sketch updates / OP_PUSH_MANY adoption — measured against fraud-team.json zipfian)

**Next:** `/gsd-discuss-phase 19.1` to capture context decisions (numbering, WAL default, histogram windowed semantics, stretch scope), then `/gsd-plan-phase 19.1` to break into 4–5 plans across 3 waves.

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged. Semantics: Redis-shaped, processing-time only (no event-time, no joins, no watermarks — locked 2026-04-30 per `project_redis_shaped_no_event_time_ever`).

## Architectural pivot 2026-04-30 — no event-time / no joins / no watermarks (PERMANENT)

**Locked.** State is `f(arrival-order events, query time)`. mio data plane is the only hot-path entry. Phases 14, 14.1, 15 archived. Phase 12 retitled "push/get API completion (joins/unions REMOVED)". Phase 17 reworked. Phase 12.5 archived (superseded by Plan 12-10). NEW Phase 12.6 inserted (v0 surface reduction — legacy axum kill + event-time strip + dead-code/redundancy sweep + windowed-op time-source swap + join/union removal + REQUIREMENTS sweep + mio-only enforcement). NEW Phase 25 inserted (session window operator family — v0.1+).

**v0 critical path post-pivot (refreshed 2026-05-03):** ~~Plan 12-10 (push-and-get on mio)~~ DEFERRED per Phase 12.6 D-04 → ~~Phase 12.6 (surface reduction)~~ ✅ **CLOSED 2026-04-30 (PASS-WITH-WARN)** → ~~Phase 12.7 (table strip)~~ ✅ **CLOSED 2026-05-01 (PASS)** → ~~Phase 12.8 (memory governance)~~ ✅ **CLOSED 2026-05-01 (PASS-WITH-WARN)** → ~~Phase 12.9 (AggOp memory boxing — fraud-team 22 KB → 6 KB)~~ ✅ **CLOSED 2026-05-03 (PASS)** → ~~Phase 13.0 (design contract + spec docs)~~ ✅ **CLOSED 2026-05-03 (PASS)** → **4-way parallel 13.4 (engine) + 13.5 (Python+bench) + 13.6 (TS+Go) + 13.7 (docs site) NEXT** → sequential 13.8 (packaging + GA tag). Phase 25 (session windows) and Phase 26 (Valkey IO rework) are v0.1+. Phases 14/14.1/15 are dead architecture — do not unarchive without explicit user override + new ADR.

## Current Focus

**Phase 13.0 OFFICIALLY CLOSED 2026-05-03 (PASS) — v0 design contract + spec documentation landed.** 16 plans across 3 waves (Wave 1: setup + spec contracts; Wave 2: operator catalog + DSL/schema/errors; Wave 3: concepts + examples + closure + per-op test suite). ~158 doc + script + fixture + test artifacts shipped — the largest doc-only phase to date. **3 ADRs** establish the project's decision-record convention and lock the architectural overturns: ADR-001 (`@bv.table` aggregation-output partial overturn of `project_v0_events_only_scope`); ADR-002 (Polars op renames `avg→mean / variance→var / stddev→std / count_distinct→n_unique / percentile→quantile`); **ADR-003 (mid-execution scope amendment 2026-05-03 per user directive "ship both / do both": first-class global aggregation `@bv.table` no `key=` / `events.agg(...)` no `group_by` + public `bv.lit(value)` export)**. All 4 CONTEXT decisions D-01..D-04 honored verbatim (D-01 wire-spec rigor with 13 JSON Schema 2020-12 contracts + 20 worked-example fixtures; D-02 53 per-op pages + 7 family overviews; D-03 9 runnable demos against language-local mocks; D-04 per-method H3 worked examples + 20-row ambiguity matrix). All 7 locked Q answers honored (Q1 Path B boolean-sum / Q2 MockApp shim / Q3 docs/blog/ NUKE / Q4 contributing.md NUKE / Q5 Rust validator deferred to 13.4 / Q6 docs/index.md ownership / Q7 OP_RESET=0x0040). 3 final-gate scripts exit 0: `examples/wire/_validate_examples.py` (20 examples validate), `scripts/check_op_page_coverage.py` (54 op pages match catalogue), `bash examples/test_examples.sh` (9/9 demos pass). Plan 16 added mid-execution per user directive ("write test for each operator as if we are users testing in python") — 13-file Python integration test suite under `python/tests/v0/` with 68 tests across 53 ops + 8 global-agg + 5 bv.lit + scaffolding (~3,784 LOC), gated by `_engine_available()` SKIP until Phase 13.4 + 13.5 land the engine + SDK rewrite. Plan 16 turns these tests GREEN as the acceptance gate for downstream phases. CLAUDE.md unchanged at closure (planner discretion: 13.0 is doc-only with no new code-level invariant; ADRs are referenced from spec docs already). Section-ownership held: closure plan owns SUMMARY/VERIFICATION/STATE/ROADMAP/REQUIREMENTS + ADR-003 + ADR-003-derived doc patches; spec docs (Plans 02-14) untouched in their existing sections (only additive global-agg + bv.lit patches per scope amendment). v0 critical path advances Phase 13.0 (CLOSED) → **4-way parallel 13.4 (engine) + 13.5 (Python+bench) + 13.6 (TS+Go) + 13.7 (docs site)** → sequential 13.8 (packaging + GA tag). Mechanical implementation deferred: Phase 13.4 owns engine sentinel routing for global tables + server-side op rename + architectural-test allowlist update for OpNode::Table* on derivations per ADR-001; Phase 13.5 owns Python SDK rewrite (21 missing op helpers + bv.lit export + global-agg surface ~110 LOC); Phase 13.6 owns TS + Go ports (bv.lit + global-agg overloads + Go GetGlobal helper ~150 LOC); Phase 13.7 owns docs-site rendering / publishing. SUMMARY: `.planning/phases/13.0-design-contract-spec-docs/13.0-SUMMARY.md`. VERIFICATION: `.planning/phases/13.0-design-contract-spec-docs/13.0-VERIFICATION.md`. ADR-003: `.planning/decisions/ADR-003-global-aggregation-and-bv-lit.md`. **Resume next:** `/gsd-discuss-phase 13.4` (or 13.5 / 13.6 / 13.7 — they're independent post-13.0).

### Phase 12.8 CLOSED 2026-05-01 (PASS-WITH-WARN) — memory governance landed. 9 plans across 5 waves. Workspace **1095 passed / 0 failed / 4 ignored**. Cargo clippy + fmt clean. Two-tier memory hygiene + lifetime aggregation contract + 5 Prometheus metric families + env-gate flipped ON (default enforcement). 4 architectural-test CI tripwires (`phase12_8_lifetime_ops_have_bounds.rs`, `phase12_8_cold_entity_eviction.rs`, `phase12_8_unbounded_op_in_lifetime_mode.rs`, `phase12_8_metrics_endpoint.rs`). Microbench: cold-TTL on/off **-2.6%** (within ±5% gate; TTL is essentially free per-event). Throughput regression-gate `small/tcp`: **-2.5%** vs Phase 12.7's 751,498 EPS (PASS within ±10%). **Two fraud-team cells WARN flagged for Phase 13:** `fraud-team/tcp` -21.3% + `fraud-team/http` -29.8% (root cause: O(N_tables) `entity_count_resident` snapshot; fraud-team has 9 tables vs 1-4 simpler shapes; NOT gating per CLAUDE.md §Performance Discipline). All 4 CONTEXT decisions D-01..D-04 honored verbatim. CLAUDE.md `§ Memory Governance Invariant (locked Phase 12.8)` block landed alongside existing 12.6 mio-only and 12.7 Events-Only invariants. REQUIREMENTS.md gained V0-MEM-GOV-01/02/03 positive anchors. Section-ownership held across Plans 07/08/09. Plan 04 PLANNER-SURFACED CONCERN: `top_k` classified as `BoundedByConfig("k", 10)` (soft default) for backward-compat. Plan 06 PLANNER-SURFACED CONCERNs: `bytes_per_entity_p99` static placeholder = 7000; v0 metrics ship UNLABELED. SUMMARY: `.planning/phases/12.8-memory-governance/12.8-SUMMARY.md`. VERIFICATION: `.planning/phases/12.8-memory-governance/12.8-VERIFICATION.md`.

### Phase 12.7 CLOSED 2026-05-01 (PASS) — v0 table strip landed. 10 plans across 4 waves (Plans 01-10). HEAD `5645ead` (`chore(12.7-10): remove #[ignore] from architectural test pair + delete dead temporal_throughput.rs`). 26 commits in the Phase 12.7 commit range. Workspace **1049 passed / 0 failed / 4 ignored** with `cargo clippy + cargo fmt` clean. Entire table / temporal / retraction surface DELETED (~5,500 LOC cumulative): `temporal_http.rs` (~756 LOC) + `temporal.rs` (~394 LOC) + `_tables.py` (~502 LOC) + `temporal_throughput.rs` (~238 LOC) + Plan 06 SDK strip + Plans 03/04 wire-router-dispatch surgery. **Persistence schema RESET 2→1** (D-01 hard rip RESET, more aggressive than 12.6's bump): `record.rs::FORMAT_VERSION = 1`, `snapshot_body.rs::SNAPSHOT_BODY_FORMAT_VERSION = 1`, `snapshot_header.rs::SNAPSHOT_FORMAT_VERSION = 1`. `RecordType::TableUpsert/TableDelete/Retract` variants deleted; `recovery.rs:380+` table-replay branch deleted. **Forward-looking error framing** (D-02): `unsupported_node_kind` JSON-prelude shim (NOT `feature_removed_no_tables_v0`); deleted mio HTTP routes return plain 404; Python `bv.table` / `App.upsert/delete/retract` raise `AttributeError` naturally; `GroupBy.agg()` is a method-shape stub raising `RuntimeError("...not supported in v0...")`. **Architectural test pair** (D-03): `phase12_7_no_table_surface.rs` (3 tests, symbol grep across 5 walk dirs × 18 forbidden patterns) + `phase12_7_legacy_table_handlers_killed.rs` (6 tests, file/symbol absence) GREEN BY DEFAULT post Plan 10 #[ignore] removal. **REQUIREMENTS.md comprehensive sweep** (D-04): 8 REQ-IDs DESCOPED with uniform banner + V0-EVENTS-ONLY-01 positive anchor added. **Phase 11.5 retro-descope banner** on 3 named files (SUMMARY / VERIFICATION / CONTEXT). **Microbench (Plan 09)**: 3 cells SIGNIFICANTLY FASTER than 12.6 baseline (-25.2% to -30.3%). **Throughput rebaseline (Plan 09)**: small/tcp regression-gate cell +7.3% above 12.6 baseline (751,498 EPS vs 700,571); 7/8 cells PASS within ±10%. CLAUDE.md `§ Events-Only Invariant (locked Phase 12.7)` block lands as sibling to existing `§ mio-only Hot-Path Invariant (locked Phase 12.6)`. All 4 CONTEXT decisions D-01..D-04 honored verbatim. Two PLANNER-SURFACED CONCERNs documented for user review in 12.7-SUMMARY: SDK-AGG-* operator-family REQ-IDs LEFT ACTIVE (Concern 1) and D-04 wildcard discrepancy (Concern 2). SUMMARY: `.planning/phases/12.7-table-strip/12.7-SUMMARY.md`. VERIFICATION: `.planning/phases/12.7-table-strip/12.7-VERIFICATION.md`.

**Phase 12.6 CLOSED 2026-04-30 (PASS-WITH-WARN) — v0 surface reduction landed.** 15 plans across 8 waves (Plans 01-15 inclusive of Wave-1.5 gap closure 14+15). HEAD `1e318b1`. 76 commits in the Phase 12.6 commit range. Legacy axum data plane DELETED (~7475 LOC across `push.rs` / `http.rs` / `push_and_get.rs` / `tcp.rs` / legacy `Server` struct). mio is the SOLE data-plane runtime per `project_phase18_no_dual_runtime` — enforced by `phase12_6_mio_only_dataplane.rs` architectural test. `event_time_ms` / `event_time_field` / `tolerate_delay_ms` HARD ripped from push wire + register wire + EventDescriptor + DevAggState + WAL/snapshot schema (v1→v2 — later RESET to v=1 by Phase 12.7) + Python SDK decorator. `OpNode::Join` / `OpNode::Union` / `JoinType` deleted. Path X swapped windowed-op time source from event_time_ms to server `now_ms()`. Microbench (Plan 11) captured 3 cells as first measurement; throughput rebaseline (Plan 12) at -0.94% on small/tcp gate cell vs post-12-08 baseline (PASS). All 5 CONTEXT decisions D-01..D-05 honored verbatim. SUMMARY: `.planning/phases/12.6-v0-surface-reduction/12.6-SUMMARY.md`. VERIFICATION: `.planning/phases/12.6-v0-surface-reduction/12.6-VERIFICATION.md`.

**Plan 12-07/08/09 closed 2026-04-29 (Phase 12 sequence).** main.rs migrated to ServerV18 (mio data plane); `/get` on mio HTTP+TCP via apply_shard; apply-loop overhead reduction 1095→75 ns/event (14.6×); TCP /get msgpack default. Legacy push.rs / push_and_get.rs / tcp.rs subsequently deleted by Phase 12.6 Plan 07.

**Phase 18 wrap items folded into Phase 12.6 closure** (worktree archival recorded by Plan 09; SUMMARY/verification work absorbed into Phase 12.6 SUMMARY's plan-by-plan TOC and per-plan SUMMARY references).

**Next: v0 critical path → Phase 13 (final v0 ship).** `/gsd-discuss-phase 13` for ship-readiness scope (SDK polish on the events-only surface; perf gates on THREE pipelines (simple fraud / complex fraud / recommendations) ≥3M EPS, P99 < 10ms batch-get; minimum-viable docs (quickstart → operators → http-api → architecture); `/metrics` Prometheus (already partially shipped on `phase-13-ship`); PyPI + Docker Hub image + GitHub Releases binaries (Linux x86_64, Linux ARM64, macOS ARM64); CI green; ship-ready tag. Hetzner Linux baseline + multi-instance shard-scaling validation per `project_no_sharded_apply`; concept docs / operator docs / HTTP API docs sweep with no-event-time pivot — D-05 deferred work from 12.6). **DROPPED:** `bv.fork`, `playground.beava.dev`, structured logs. Plan 12-10 (push-and-get) DEFERRED entirely from v0 per Phase 12.6 D-04.

---

### Legacy: Phase 18 — Redis-shaped hand-rolled hot path landed + continuous pipelining landed; only Phase 18 wrap (SUMMARY + verification + worktree archival decision) remains. main.rs migration closed by Plan 12-07.

### Landed and merged on `v2/greenfield` (HEAD `a809d04`):

- **Plan 18-09** — msgpack-on-TCP (CT_MSGPACK), Row::Deserialize impl, WAL v=2 binary records
- **Plan 18-10** — hand-rolled envelope parsers: `parse_msgpack_envelope` (33 ns / 57× faster), `parse_json_envelope` (77 ns / 7.6× faster), `BeavaValueVisitor` direct Row deserialize
- **Plan 18-04.7** — IoPool wiring into `serve_with_dirs`: parse + encode moved off apply thread, per-tick lifecycle [poll → distribute_reads → join → apply → distribute_writes → join]
- **Plan 18-04.8** — body→Row deserialize moved off apply onto IoPool worker; apply parse stage 193 → 77 ns; IoPool runtime timing trace under same `BEAVA_TRACE_APPLY_TIMING` env var
- **Plan 18-11** — hot-path optimization: Row.0 → SmallVec<[(CompactString, Value); 8]>; Value::Str(CompactString); AggStateTable → hashbrown::HashMap+FxBuildHasher with raw_entry_mut; EntityKey SmallVec; Arc<EventDescriptor>; per-source aggregation index. agg stage 5× faster (3,191 → 529 ns), parse 6× faster (911 → 150 ns)
- **Plan 18-12** — `Arc<str>` event_name in EventIdEntry::Stream + EventDescriptor.name_arc pre-allocated at registration; bookkeeping site refcount-bumps registry-resident Arc<str> (no per-push String alloc). EPS at p=16/pd=256 json **346k → 462k (+33.5%)**, msgpack **357k → 487k (+36.4%)**. Trace per-stage mean held flat (mutex+insert dominates the bookkeeping stage); the EPS lift came from removed allocator pressure / cache pollution that the in-window trace doesn't capture
- **env::var caching** for trace flags (OnceLock per process — saves ~200 ns/event when trace OFF)
- **`TRACE_AGG_TIMING` env var split** so outer trace doesn't include inner eprintln cost
- **bench-v18 `--pipeline-depth N` flag** — burst pipelining baseline; 6-8× EPS lift on M4 loopback at p=16/pd=256

### Phase 18 wrap (still TBD):

- **Phase 18 SUMMARY.md** — overall phase wrap covering 18-09, 18-10, 18-11, 18-04.7, 18-04.8, 18-12, plus continuous pipelining
- **Phase 18 verification** — `/gsd-verify-work 18` against the phase goal
- **`phase-13.3-lockless-apply` worktree archival decision** — delete vs rename to `archived/phase-13.3-rejected` (Phase 13.3 REJECTED 2026-04-26 per architectural decision)
- **main.rs migration to ServerV18 completed in Plan 12-07 (commit `2ede08f`)** — production binary now boots ServerV18 (mio data plane) per memory `project_phase18_no_dual_runtime`. Legacy `Server` retained for `phase6_crash_probe` + `TestServer`.

### Architectural decision LOCKED 2026-04-26:

**Phase 13.3 (in-process apply sharding via lockless RefCell + LocalSet) is REJECTED.** Beava commits to single-threaded data plane forever. Per-instance throughput ceiling = single apply thread (~1M EPS for simple counters, ~400k for medium aggregations on Linux Xeon post-current optimizations). For higher aggregate throughput, users run **multiple Beava instances** sharded at the entity-key level (Redis-cluster pattern). Cross-shard queries within a process are explicitly avoided.

### Headline numbers (M4 loopback, post-merge of 18-12 + continuous pipelining, commit `a809d04`):

- `parse_msgpack_envelope` microbench: **33.4 ns**
- `parse_json_envelope` microbench: **77.1 ns**
- agg stage (clean trace): **500 ns** (was 3,191 ns at start of phase)
- TOTAL push (clean trace, p=4/pd=64): **888 ns** (was 5,154 ns at start of phase) — **5.8× faster**
- Apply-thread theoretical max at p50 cycle: ~1.13M EPS single-thread
- Best-of-3 EPS at p=16/pd=256 (continuous pipelining mode): **mean 375k json / 400k msgpack** with 3-7× tighter variance than burst mode. Burst-mode upper-tail EPS (462k/487k) still observed but with much wider variance band; continuous is the new default

## Shipped & Merged to `v2/greenfield`

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Foundation (workspace, axum, /health, /ready, logging, test harness) | ✅ merged |
| 2 | Sources + registry + version bumps + additive-only enforcement | ✅ merged |
| 2.5 | TCP wire listener + framing + full opcode table | ✅ merged |
| 3 | Python SDK skeleton + decorators + expression DSL | ✅ merged |
| 4 | Stateless ops + expression evaluator (server-side) | ✅ merged |
| 5 | Aggregation framework + 8 core operators | ✅ merged |
| 5.5 | Perf harness + retroactive baselines + 10%/25% regression gate | ✅ merged |
| 6 | WAL + idempotency | ✅ merged |
| 6.1 | Async durability — `SyncMode::{Periodic,PerEvent}` + `/push-sync` (Kafka-style acks=1 default, ~15× EPS lift; acks=all via push-sync) | ✅ merged |
| 7 | Snapshot + recovery + schema evolution | ✅ merged |
| 7.5 | End-to-end throughput harness + first baseline + per-phase throughput-run convention | ✅ merged |
| 8 | Point / ordinal / recency operators (15 ops) + TCP `OP_PUSH` | ✅ merged |
| 9 | Decay + velocity operators (16 ops + Python helpers) | ✅ merged |
| 10 | Sketch operators — HLL / CMS / TopK / UDDSketch / Bloom (5 ops) | ✅ merged |
| 11 | Bounded-buffer + geo operators (13 ops + `Value::{List,Map}`) | ✅ merged |
| 11.5 | Temporal MVCC tables + retraction primitive | ✅ merged |
| 13.1 | Perf regression fix — `spawn_blocking` for fsync (17k EPS restored at parallel=64) | ✅ merged |

**v2/greenfield HEAD:** `1495054` (docs session abandon 13.2). Test count: **850 tests green**.

## Shipped Partial — Awaiting Merge + Follow-up

| Branch / worktree | HEAD | What landed | What's left |
|---|---|---|---|
| `phase-12-joins` | `d541971` | Plan 12-02 (WAL replay for `TableUpsert/Delete/Retract`) + path-rewrites for 01/03/04/05/06 | **ABANDONED 2026-04-30 per Phase 12.6-09** — joins removed permanently per `project_redis_shaped_no_event_time_ever`. Plan 12-02 (TableUpsert/Delete/Retract WAL replay) is non-join work; if revived, cherry-pick onto `v2/greenfield`, do NOT merge from this branch. Non-join survivors (Plans 12-01/03/04/05/06) tracked separately on `phase-12-followup`. |
| `phase-13-ship` | `2ef5afc` | Plan 13-01 (`/metrics` Prometheus + middleware), Plan 13-03 (`env_var_overrides` hermetic fix) | Plans 13-02, 13-04, metric-counter wiring on `phase-13-followup` worktree |

## Remaining Work (priority order)

### Phase 18 (all data-plane items landed):

| # | Task | Where | Status |
|---|------|-------|--------|
| 1 | **Plan 18-04.8** — body→Row migration from apply thread to IoPool worker + IoPool runtime timing trace | DONE 2026-04-26 (commits 9a1daec/6ed8b97/677d3ea on v2/greenfield). Apply parse 193 → 77 ns (-60%); apply TOTAL 974 → 941 ns; IoPool parse_body=4,265 ns mean; EPS p=16/pd=256 json 346k / msgpack 357k; new TRACE_APPLY io trace lives under same BEAVA_TRACE_APPLY_TIMING var | ✅ done |
| 2 | **Plan 18-12** — `Arc<str>` event_name to kill bookkeeping String alloc | DONE 2026-04-26 (commits e96c59b → adaa66e on v2/greenfield). EPS at p=16/pd=256 json 346k → 462k (+33.5%), msgpack 357k → 487k (+36.4%); EPS at p=4/pd=64 json 165k → 239k (+44.5%); apply TOTAL 941 → 888 ns. Trace per-stage mean held flat (mutex+insert dominates bookkeeping stage; ~50-100 ns alloc savings absorbed by ±25 ns variance band); EPS lift came from removed allocator pressure / cache pollution that in-window trace doesn't capture | ✅ done |
| 3 | **Continuous pipelining for bench-v18** — split sender/receiver + Semaphore; replaces burst pattern | DONE 2026-04-26 (commit a809d04 on v2/greenfield). `--continuous-pipeline` flag (default true); tokio::io::split + Semaphore + mpsc<Instant> for FIFO ack-pairing + latency batching to mirror burst's lock amortization. Best-of-3: json 322k → 375k mean (+16%, **7× tighter variance**), msgpack 374k → 400k mean (+7%, **3× tighter variance**). Continuous reports REAL per-event wall-clock latency vs burst's amortized batch_total/N | ✅ done |

### Phase 18 cleanup (after the above land):

- Run combined post-everything EPS sweep + agg sub-stage trace; append to `throughput-baselines.md`
- Phase 18 SUMMARY.md (overall phase wrap)
- Phase 18 verification

### Other phase follow-ups (not Phase 18):

| # | Task | Where | Status |
|---|------|-------|--------|
| A | Phase 12 follow-up — Plans 12-01/03/04/05/06 (joins + `push_sync`/`push_many`/`push_table`/`delete_table`/`set`/`mset`/`mget`/`get_multi`) | `.claude/worktrees/phase-12-followup` (off `phase-12-joins`) | ⏳ pending |
| B | Phase 13 follow-up — Plans 13-02 (cold-entity GC sweep), 13-04 (perf gate), metric-counter wiring | `.claude/worktrees/phase-13-followup` (off `phase-13-ship`) | ⏳ pending |
| C | Merge sequence: 12-joins + 12-followup → 13-ship + 13-followup → v2/greenfield | Mainline | ⏳ after A & B |
| D | Final bench + ledger update (`beava-bench` at parallel=64 × small/medium/large × HTTP/TCP × BATCH_MS=0/1/5/20) | `.planning/throughput-baselines.md` | ⏳ after merges |
| E | Milestone audit → complete → cleanup (`gsd-audit-milestone` → `gsd-complete-milestone v0.0` → `gsd-cleanup`) | Lifecycle | ⏳ final |

### REJECTED (do not propose as future plans):

- ~~**Phase 13.3** — lockless apply (RefCell + LocalSet)~~ — single-threaded data plane LOCKED 2026-04-26; users scale out via multi-instance Redis-cluster pattern instead. Worktree `.claude/worktrees/phase-13.3-lockless-apply` archived for historical reference.

**Deferred to v0.0.x point releases** (per Phase 13 CONTEXT D-16):

- Plan 13-05 docs site (quickstart/operators/concepts/http-api/architecture)
- Plan 13-06 `bv.fork()` local scoped replica subcommand
- Plan 13-07 `pip install beava` + Docker Hub + GitHub Releases packaging
- Plan 13-08 `playground.beava.dev` hosted tutorial

## Performance Snapshot

- **Post-merge ceiling** (macOS Apple-M4, `v2/greenfield` HEAD `adaa66e` — post-18-12): **462k EPS (json) / 487k EPS (msgpack)** at p=16/pd=256, bench-side bursty load is the next wall (continuous pipelining is the queued unlock).
- **Apply-thread per-event work (clean trace):** 888 ns mean (was 941 ns post-18-04.8); theoretical ~1.13M EPS single-thread at p50 cycle.
- **Per-stage breakdown (mean ns post-18-12, n=67k):** parse 67, lookup 28, validate 29, wal_build 30, wal_append 36, agg 500, bookkeeping 194 (mutex + HashMap::insert; the 50-100 ns String alloc removal is absorbed in stage variance — see 18-12-SUMMARY.md for analysis).
- **Phase 13 ship-gate target:** ≥3M EPS/core single-instance on simple-fraud (medium pipeline) shape — REFRAMED (post-13.3-rejection) as **per-instance peak achievable on Linux Xeon with all 18-04.7 + 18-04.8 + 18-12 + future 18-05 io_uring + OP_PUSH_MANY**. For aggregate >1 instance ceiling: scale out (multiple Beava instances).
- **Baselines:** `.planning/perf-baselines.md` (criterion rows, phases 2.5..18-11); `.planning/throughput-baselines.md` (end-to-end EPS + latency ledger across 18-09, 18-10, 18-11, 18-04.7, 18-04.8, 18-12).

## Accumulated Context

### Architectural decisions (locked)

- Python SDK is the canonical authoring UX; curl is the language-agnostic escape hatch
- Dual wire: HTTP/JSON + custom-framed TCP `[u32 len][u16 op][u8 content_type][payload]`; Redis-style strict-FIFO correlation (no request_id); `content_type` 0x01 JSON, 0x02 MessagePack reserved; `op=0xFFFF` error_response
- **beava-core WASM-portability invariant:** `beava-core` (expression, registry, ops, aggregations, sketches) stays syscall-free; only `beava-server` + WAL/snapshot crates touch fs/net. Unlocks v0.1+ browser-WASM + edge deployment without refactor
- `@bv.event` (immutable append-only) and `@bv.table(key=..., ttl=...)` (upsertable, with tombstone delete); temporal tables use MVCC (Phase 11.5)
- Aggregations via `Event.group_by(keys).agg(name=bv.<op>(...), ...)` produce Tables
- Stateless ops chain: `filter / select / drop / rename / with_columns / map / cast / fillna`
- Expression DSL: `bv.col("x")` with arithmetic, comparison, `& | ~`, `.isnull()`, `.cast()`
- Joins: event↔event windowed, event↔table enrichment (uses `as_of=` for temporal), table↔table key-matched; `bv.union(*events)` with schema-identity enforcement
- Single Rust process, single apply-loop thread (auxiliary threads for WAL fsync via `spawn_blocking`, HTTP accept, snapshot writer)
- In-memory state only; no RocksDB / fjall / SSD tiering
- Uniform event-time bucketing, cap 64 buckets per windowed operator
- Schema evolution: additive-only registry changes with monotonic version bumps
- Commercial tier (HA, replicas, cross-region) explicitly out of v0 OSS

### Operator catalogue shipped (55 ops)

- Core (8): count, sum, avg, min, max, variance, stddev, ratio — Phase 5
- Sketch (5): count_distinct (HLL), percentile (UDDSketch), top_k (SpaceSaving), bloom_member, entropy — Phase 10
- Point/ordinal (11) + recency (4): first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n, streak, max_streak, negative_streak, first_seen_in_window — Phase 8
- Decay (7) + velocity (8) + z_score (1): ewma (alias ema), ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count, z_score — Phase 9
- Bounded-buffer (7) + geo (6): histogram, hour_of_day_histogram, dow_hour_histogram, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home — Phase 11

### Pre-created worktrees (resume points)

```
.claude/worktrees/phase-12-followup     (base: phase-12-joins   @ d541971)
.claude/worktrees/phase-13-followup     (base: phase-13-ship    @ 2ef5afc)
.claude/worktrees/phase-13.2-followup   (base: phase-13.2-coalesce — ABANDONED; do not merge)
```

### Worktree map post-Phase-12.6 (2026-04-30, recorded by Plan 12.6-09)

Post-architectural-pivot worktree fates. Plan 12.6-09 audits and records each branch's status:

| Worktree / branch | Status | Rationale |
|---|---|---|
| `phase-12-joins` (HEAD `d541971`) | **ABANDONED 2026-04-30 per Phase 12.6-09** | Joins removed permanently per `project_redis_shaped_no_event_time_ever`. Phase 12.6-04 deletes joins as architecture. Plan 12-02 (WAL replay for `TableUpsert/Delete/Retract`) is non-join work; if revived, cherry-pick onto `v2/greenfield` directly, do NOT merge from this branch. |
| `phase-12-followup` (off `phase-12-joins`) | **REBASE PENDING** | Off-branch dependency on `phase-12-joins` is now stale (parent ABANDONED). Either rebase onto `v2/greenfield` (preferred — preserves Plans 12-01/03/04/05/06 survivors) or abandon and recreate the followup branch fresh off `v2/greenfield`. |
| `phase-13-followup` (off `phase-13-ship` @ `2ef5afc`) | **KEEP** | Plans 13-02 (cold-entity GC sweep), 13-04 (perf gate), and metric-counter wiring still active per Phase 13 critical path. |
| `phase-13.1-perf-fix` | **KEEP** (already merged) | Phase 13.1 fsync regression fix landed; worktree may be cleaned up by Phase 13 lifecycle pass — not Phase 12.6's scope. |
| `phase-13-ship` | **KEEP** | Base branch for `phase-13-followup`; Plans 13-01 / 13-03 already merged to `v2/greenfield`. |
| `phase-13.2-followup` (off `phase-13.2-coalesce`) | **ABANDONED** (already noted line 261; Phase 12.6-09 confirms) | Phase 13.2 superseded by Phase 13.3 (which itself was rejected); branch is dead. |
| `phase-13.3-lockless-apply` | **ARCHIVED-REJECTED 2026-04-26** (already noted line 213; Phase 12.6-09 confirms + adds banners to `.planning/phases/13.3-lockless-apply/*.md`) | Single-threaded data plane locked per `project_no_sharded_apply`. Worktree was deleted 2026-04-26; planning files retained for historical reference and now banner-stamped. |
| `phase-15-event-time-pit` | **ARCHIVED 2026-04-30** (per no-event-time pivot) | Event-time gone permanently per `project_redis_shaped_no_event_time_ever`. Worktree may stay on disk for historical reference; do not check out for new work. Phase dir already moved to `.planning/phases/_archived-15-event-time-pit-killed-no-event-time/` per ROADMAP line 57. |
| `phase-16-sdk-source-annotation` | **NEEDS REASSESSMENT** | Phase 16 reworked 2026-04-30 (`tolerate_delay_ms` + `modifiable=True` references removed by no-event-time pivot; remaining `@bv.source` + `app.upsert/delete` scope is intact). Worktree status pending Phase 13 sweep — defer revisit. |

**Section ownership note:** Plan 12.6-09 owns this worktree-status sub-block. Plan 12.6-13 (Wave 8) owns the phase-progress block + Current Focus line. The Phase 12.5 dir banners + this map were added together in `docs(12.6-09)` GREEN commit.

## Blockers

None active. Quota-wall blockers from the 2026-04-24 06:12 session have reset.

## Historical session notes

- `.planning/SESSION-STATE-2026-04-23.md` — Phase 2.5 → operator-family dispatch
- `.planning/SESSION-STATE-2026-04-24-0612.md` — post-quota-wall handoff with full branch-level detail

---
*State last rewritten: 2026-04-24 — reconciled with actual shipped state after parallel merges (6.1..11.5), Phase 12/13 partial landings, and Phase 13.1 fsync fix merge.*
