# Beava v2 — v0 OSS Launch Roadmap

**Milestone:** v0 (first public OSS cut on `beava.dev`)
**Granularity:** fine (19 phases; 3–8 plans per phase)
**Mode:** yolo (auto-approved; written to hold up unrevised)
**Parallelization:** enabled where indicated
**Created:** 2026-04-22
**Revised:** 2026-04-24 (added sub-phases 6.1 async-durability, 13.1 perf-regression-fix, 13.3 lockless-apply; abandoned 13.2 coalesce; marked all shipped phases ✅ COMPLETE)
**Source:** `.planning/PROJECT.md`, `.planning/REQUIREMENTS.md`

## North Star

Feature authoring as composable Python code that ships to production unchanged. v0 ships a streamlined Python SDK shape (`@bv.event` / `@bv.table` / `bv.col` / `.filter / .select / ... / .group_by().agg()` / `app.register` / `app.push` / `app.get` / `bv.fork`) on a hand-rolled mio data plane with a 55-operator catalogue. Semantics are Redis-shaped: processing-time only, no event-time anywhere, no joins. Session windows replace event-time grouping for activity-based aggregation (v0.1+).

## Architecture (locked, do not revisit in phases)

- **Runtime:** Single Rust process, single OS thread for the apply loop via hand-rolled mio data plane (ServerV18). Tokio sidecar for admin endpoints only on a separate port. Auxiliary threads for WAL fsync (single writer+fsync thread), HTTP accept, snapshot writer.
- **Hot-path entry:** **mio is the only data-plane entry point.** All push/get/upsert/delete traffic dispatches through `apply_shard.rs::dispatch_*_sync`. Legacy axum (`Server`, `push.rs`, `http.rs`, `http_admin.rs`) scheduled for removal in Phase 12.6.
- **Semantics:** Redis-shaped, processing-time only. No event-time anywhere. No watermarks. No joins (event↔event, event↔table, table↔table all removed permanently). No `bv.union` in v0. State is `f(arrival-order events, query time)`. Late-event question is undefined — there are no late events. Locked 2026-04-30; see `project_redis_shaped_no_event_time_ever`.
- **State:** In-memory only (no RocksDB, no fjall, no tiered storage)
- **Durability:** WAL file per instance with 1–5ms group-commit fsync; periodic snapshots (default 30s) of in-memory state
- **Recovery:** Load latest snapshot + replay WAL from snapshot LSN
- **Wire:** HTTP/1.1 + JSON + framed-TCP (`[u32 len][u16 op][u8 content_type][payload]`, Redis-style strict-FIFO correlation, no `request_id`); endpoints `POST /register`, `POST /push/{event}`, `POST /push-sync/{event}`, `POST /push-batch/{event}`, `POST /push-and-get/{event}`, `POST /push-table/{table}`, `POST /delete-table/{table}`, `POST /get`, `GET /get/{feature}/{key}`, `POST /set`, `POST /mset`, `/metrics`, `/health`, `/ready`. **`event_time_ms` removed from wire in Phase 12.6** (was: optional field on push payload; now: server-side `now_ms()` is the only time source).
- **Authoring UX:** Python SDK with streamlined decorator DSL, expression DSL, stateless ops, aggregation framework, session windows (v0.1+). No join API. No union API.
- **Registration:** Additive-only with monotonic `registry_version` bumps; removals/changes return 409 with structured diff
- **Operator catalogue:** 55 built-in aggregation operators spanning core, sketch, point, decay, velocity, recency, bounded-buffer, and geo families. Windowed ops index by server-side `now_ms()` (Path X) — preserved through the no-event-time pivot.

## Phase Overview

| # | Phase | Goal | Reqs | Success criteria |
|---|-------|------|------|------------------|
| 1 | Foundation | Rust workspace, axum HTTP scaffolding, config, logging, test harness | 0 (infrastructure) | 4 ✅ **COMPLETE** |
| 2 | Sources + registry + version bumps | `/register` accepts DAG of event/table/derivation nodes; additive-only; monotonic version; registry persists in-memory | 12 | 5 ✅ **COMPLETE** |
| 2.5 | TCP wire listener + framing + full opcode table | Custom-framed TCP listener alongside HTTP; full v0 opcode table designed; `register` + `ping` handlers wired; rest return `op_not_implemented` placeholder | ~8 | 8 ✅ **COMPLETE** |
| 3 | Python SDK skeleton + decorators + expression DSL | `@bv.event`, `@bv.table`, `bv.col`, `bv.App(url)` (HTTP + TCP), register + validate, REGISTER JSON compiler | 20 | ✅ **COMPLETE** |
| 4 | Stateless ops + expression evaluator (server-side) | `filter`/`select`/`drop`/`rename`/`with_columns`/`map`/`cast`/`fillna` + `bv.col` evaluator | 13 | ✅ **COMPLETE** |
| 5 | Aggregation framework + core operators (8) | `group_by().agg()` + 8 core ops + `Windowed<Op>` 64-bucket infra | 15 | ✅ **COMPLETE** |
| 5.5 | Perf harness + retroactive baselines | `criterion` workspace + `.planning/perf-baselines.md` + 10%/25% regression gate | ~10 | ✅ **COMPLETE** |
| 6 | WAL + idempotency | Group-commit fsync ACK + dedupe-key replay | 5 | ✅ **COMPLETE** |
| 6.1 | Async durability (SyncMode + /push-sync) | Adds Kafka-style acks=1 default to `/push` (~15× EPS lift) while preserving acks=all path via `/push-sync` | ~6 | ✅ **COMPLETE** |
| 7 | Snapshot + recovery | Periodic full-state snapshot; restart replays snapshot + WAL; schema evolution survives restart | 6 | ✅ **COMPLETE** |
| 7.5 | End-to-end throughput harness + first baseline | Reusable harness measuring sustained EPS + push/get latency through the live HTTP+TCP server. Tiered pipelines (small=1 / medium=5 / large=15 features). 60s wall-time time-bounded runs. Baselines committed to `.planning/throughput-baselines.md` keyed by hw-class. Establishes the per-phase throughput-run convention every operator phase (8–12) must honor. | 6 | ✅ **COMPLETE** |
| 8 | Point / ordinal / recency operators | first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n, streak, max_streak, negative_streak, first_seen_in_window + TCP `OP_PUSH` | 15 | ✅ **COMPLETE** |
| 9 | Decay + velocity operators | ewma, ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count, z_score | 16 | ✅ **COMPLETE** |
| 10 | Sketch operators | count_distinct (HLL), percentile (UDDSketch), top_k (SpaceSaving), bloom_member, entropy | 5 | ✅ **COMPLETE** |
| 11 | Bounded-buffer + geo operators | histogram, hour_of_day/dow_hour histograms, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home | 13 | ✅ **COMPLETE** |
| ~~11.5~~ | ~~Temporal tables + retraction primitive~~ | ~~MVCC storage for `@bv.table(temporal=True, retention=...)`; `app.retract(event_id)` scoped to table upserts/deletes~~ | ~~~10~~ | ❌ **RETROACTIVELY-DESCOPED 2026-04-30** — Phase 12.7 will strip `@bv.table` + TemporalStore + `/upsert/delete/retract` endpoints since v0 is events-only (tables return in v0.1+ alongside joins/aggregation per `project_v0_events_only_scope`). Original landed work preserved in git history; Phase 12.7 deletes the surface. |
| 12 | push/get API completion (joins/unions REMOVED) | `push_sync` + `push_many` + `push_table` + `delete_table` + `set` + `mset` + `mget` + `get_multi` + `push_and_get` (Plan 12-10) wired end-to-end on the mio data plane. **Joins + unions removed permanently 2026-04-30** per `project_redis_shaped_no_event_time_ever`. | 8 | 🟡 **PARTIAL** — Plans 12-07/08/09 SHIPPED; Plan 12-10 written-not-executed; multi-key + table ops on `phase-12-followup` worktree |
| 12.5 | ~~`push_and_get` combined endpoint~~ | Superseded by Plan 12-10 in Phase 12; Plan 12-10 itself DEFERRED entirely from v0 per Phase 12.6 D-04. v0 ships without push-and-get. | — | ❌ **ARCHIVED-AND-DEFERRED 2026-04-30** — superseded by Plan 12-10 (push-and-get on mio HTTP+TCP); Plan 12-10 deferred from v0 per Phase 12.6 D-04; axum-shaped 12.5 plans are dead code. Phase 12.6-09 added SUPERSEDED-AND-DEFERRED banners to `.planning/phases/12.5-push-and-get/*.md`. |
| 12.6 | v0 surface reduction — legacy axum kill + event-time strip + dead-code/redundancy sweep + mio-only enforcement | Remove legacy axum (`Server`, `push.rs`, `http.rs`, `push_and_get.rs`, `tcp.rs` ~7475 LOC actual vs 3500 estimated — orphan tcp.rs + in-source test mods cascaded out) + TestServer drop-in rewrite to ServerV18 (D-01) + phase11_smoke set-membership fix (D-02); strip `event_time_ms` from wire (push payload + WAL record schema bump v1→v2 + snapshot v1→v2) per D-03 hard rip; remove `tolerate_delay_ms` + `event_time_field` decorator + `DEFAULT_TOLERATE_DELAY_MS` + `AppState.max_event_time_ms` (renamed `query_time_ms`); switch 14+ windowed operators to `now_ms()` server clock (Path X — preserves 55-op catalogue); delete `OpNode::Join` + `JoinType` + `OpNode::Union` + Python SDK helpers; register-time validator rejects join/union payloads + legacy event-time keys with structured codes `feature_removed_no_*_v0` / `unknown_field_*_v0`; mio-only architectural test (`phase12_6_mio_only_dataplane.rs`) + CLAUDE.md `§Conventions § mio-only Hot-Path Invariant` documentation; REQUIREMENTS.md surgical sweep; Phase 12.5 + 13.3 archival banner sweep. Single hot-path entry through `apply_shard.rs::dispatch_*_sync` enforced. | 15 | ✅ **COMPLETED 2026-04-30 (PASS-WITH-WARN)** — 15 plans across 8 waves (inclusive of Wave-1.5 gap closure 14+15); workspace 1067/0/3; clippy + fmt clean; microbench (Plan 11) + throughput rebaseline (Plan 12) PASS; small/tcp regression-gate -0.94% vs post-12-08 baseline. SUMMARY + VERIFICATION at `.planning/phases/12.6-v0-surface-reduction/`. |
| 12.7 | v0 table strip — events-only commitment | Strip `@bv.table` decorator + `POST /upsert/{table_name}` + `POST /delete` + `POST /retract` + `GET /table/{name}` mio handlers (added by Phase 12.6 Plan 14) + `temporal_http.rs` helpers + `TemporalStore` MVCC machinery + `app.retract(event_id)` SDK verb + ~12 table-related tests (`phase11_5_temporal_smoke`, `phase18_07_upsert_delete_rename_test`, `phase12_6_14_mio_temporal`, plus python/tests table-related). Walks back Phase 11.5 + Phase 12.6 Plan 14's mio table surface. v0 commits to events-only per `project_v0_events_only_scope`. Tables return in v0.1+ alongside joins/aggregation. | 10 | ✅ **COMPLETED 2026-05-01 (PASS)** — 10 plans across 4 waves; workspace 1049/0/4; cargo clippy + fmt clean; architectural test pair (phase12_7_no_table_surface 3 tests + phase12_7_legacy_table_handlers_killed 6 tests) GREEN BY DEFAULT; ~5,500 LOC removed (temporal_http.rs / temporal.rs / _tables.py + temporal_throughput.rs + Plans 03/04/06 surgery); FORMAT_VERSION RESET 2→1 across 3 schemas (D-01); all 4 CONTEXT decisions D-01..D-04 honored verbatim; microbench -25 to -30% (3 cells SIGNIFICANTLY FASTER vs 12.6); throughput +7.3% on small/tcp regression-gate cell. SUMMARY + VERIFICATION at `.planning/phases/12.7-table-strip/`. |
| 12.8 | Memory governance — cold-entity TTL + bucket-level reclaim + lifetime aggregation contract | Two-tier memory hygiene before final ship. **Tier 1 (entity-level):** opt-in cold-entity TTL eviction via per-source `@bv.event(cold_after='<dur>')` decorator (default OFF; range [1s, 365d]); FRESH state on resurrect (Redis TTL pattern, locked permanent). **Tier 2 (bucket-level — always on):** existing `update_at(now_ms)` per-event reclaim; `BucketReclaimCounter::inc` on bucket trim. **Lifetime aggregation contract:** implicit via `windowed=` omission (D-02 — no new SDK kwarg); each of 53 AggKind variants classifies as O1 / BoundedSketchN / BoundedByRequiredKwarg / BoundedByConfig at register-time; 4th JSON-prelude shim `pre_check_unbounded_op_in_lifetime_mode` rejects Unbounded ops with structured error code (forward-looking framing — NOT `feature_removed_*`). `BEAVA_MEMORY_GOV_ENFORCE=0` is the explicit escape hatch (default ON post-Plan-06). 5 Prometheus metric families on `/metrics` (`cold_entity_evictions_total`, `lifetime_op_cap_hit_total`, `entity_count_resident`, `bytes_per_entity_p99` static placeholder = 7000, `bucket_reclaim_total`). 4 architectural-test CI tripwires. CLAUDE.md `§ Memory Governance Invariant (locked Phase 12.8)` block. | 9 | ✅ **COMPLETED 2026-05-01 (PASS-WITH-WARN)** — 9 plans across 5 waves; workspace 1095/0/4; cargo clippy + fmt clean; all 4 CONTEXT decisions D-01..D-04 honored verbatim; microbench cold-TTL on/off -2.6% (within ±5% gate); throughput regression-gate `small/tcp` -2.5% PASS; **fraud-team WARN flagged for Phase 13** (-21.3% TCP / -29.8% HTTP — root cause: O(N_tables) entity_count_resident snapshot, fraud-team has 9 tables vs 1-4 simpler shapes; NOT gating). REQUIREMENTS V0-MEM-GOV-01/02/03 anchors. SUMMARY + VERIFICATION at `.planning/phases/12.8-memory-governance/`. |
| 12.9 | **AggOp memory boxing — fraud-team 22 KB → 6 KB budget fix** | Inserted 2026-05-03 from post-Phase-12.8 r8g maxcard bench. `size_of::<AggOp>() = 600 bytes` because of unboxed `SeasonalDeviationState`; every `Vec<AggOp>` slot consumes 600 B regardless of variant. Box the 7 fat-payload variants (SeasonalDeviation, HourOfDayHistogram, EventTypeMix, GeoVelocity, GeoSpread, GeoDistance, DistanceFromHome — same pattern Phase 10 sketches and `WindowedOp` already use) → `size_of::<AggOp>()` drops 600 → 80 bytes (7.5× shrink); fraud-team `user_id` entity inline cost drops 46.8 KB → 6.2 KB; weighted-avg per-entity drops ~22 KB → ~6 KB (clears CLAUDE.md 7 KB budget with headroom). Free side effect: `WindowedOp` bucket inner ops also shrink (same enum). Mechanical change (~10 LOC, no match-arm derefs needed — `Box::DerefMut` auto-deref); NO `FORMAT_VERSION` bump (D-03: serde Box<T> is transparent, bincode wire format unchanged). Investigation doc: `.planning/ideas/per-entity-memory-budget.md`. | 3 | ✅ **COMPLETED 2026-05-03 (PASS)** — 3 plans (boxing red+green / perf gate / closure); workspace 1097/0/4; clippy + fmt clean; size_of cap test promoted to permanent CI tripwire (≤ 80 B); fraud-team/tcp +6.9% median (no regression — possibly cache-locality lift); Phase 11 D-08 explicit-no-boxing comment empirically overridden. SUMMARY + VERIFICATION at `.planning/phases/12.9-aggop-memory-boxing/`. |
| 13 | **v0 Launch — UMBRELLA** (RESTRUCTURED 2026-05-03) | Umbrella for 6 sub-phases (13.0 + 13.4–13.8) implementing the v0 launch from a 2026-05-03 design session. Sub-phase 13.0 (design contract + spec docs) is the bottleneck; after it lands, 4 implementation phases (13.4 engine / 13.5 Python+bench / 13.6 TS+Go SDKs / 13.7 docs site) run in parallel; 13.8 (packaging + GA tag) is the sequential ship phase. 20 design decisions locked in the session (see Phase 13 detail block). 3 SDK ports: Python + TypeScript (npm) + Go. | ~30 across 6 sub-phases | 📋 **RESTRUCTURED 2026-05-03** — kicks off with `/gsd-discuss-phase 13.0` for design contract |
| 13.0 | **Design contract + spec documentation** | Produce ship-quality specs for every contract (wire / SDK API per language / pipeline DSL / schema evolution / error codes / 54-op catalog). These docs ARE the v0 documentation rendered into beava.dev. ADR-001 (`@bv.table` partial overturn) + ADR-002 (Polars op rename) + ADR-003 (mid-execution: global-agg + bv.lit). Memory updates: `project_v0_events_only_scope` partial overturn pointer. | 16 | ✅ **COMPLETED 2026-05-03 (PASS)** — 16 plans / ~158 artifacts; 4-way parallel 13.4/13.5/13.6/13.7 unblocked |
| 13.4 | **Engine prep** (Rust server changes against the wire spec) | Op renames (`avg→mean` etc.); GET response → row-shape; new `OP_BATCH_GET` opcode; verb-style HTTP routes; `force=True` + `dry_run=True` register flags; in-memory persistence backend; `OP_RESET` + `POST /reset`; `phase12_7_no_table_surface.rs` test update. | ~8 | ✅ **COMPLETED 2026-05-04 (PASS-WITH-WARN)** — 10 plans across 5 waves (parallel-execute via gsd-executor + wave-1.5 cleanup); 41 commits f229755 → 0af054a; +60 new tests; microbench apply_path/cold_key -32.7% vs 19.2 (faster); throughput regression-gate small/tcp -0.4% PASS; HTTP-transport small/medium/large -24% to -32% WARN flagged for v0.0.x (non-gating); fraud-team/http +35.5%. SUMMARY + VERIFICATION at `.planning/phases/13.4-engine-prep-wire-spec/`. |
| 13.5 | **Python SDK rewrite + `beava bench` CLI** | Delete ~2000 LOC of over-engineered SDK; new ~600 LOC core+pipeline-DSL+demo-loader+test-fixtures. Polish bench-v18/v2 into `beava bench` subcommand (4 modes: throughput/mixed/memory/fsync). Bundled `bv.demo("adtech"|"fraud"|"ecommerce")` datasets. PEP 563 fix. Module structure: core flat + `beava.test` + `beava.cli` submodules. | ~12 | 🟡 **DONE-WITH-DEFICIT 2026-05-04** — 12 plans / 32 commits across base + 2 continuations. Workspace GREEN (clippy/fmt/cargo test/mypy --strict/145 internal tests). Throughput small/tcp -7.1% PASS, fraud-team/tcp -2.4% PASS. **DEFICIT**: 0/68 v0 acceptance tests passing — `Http/Tcp/EmbedTransport.send_*` stubbed to NotImplementedError (mypy passed via MagicMock). Plus `@bv.table` empty-annotation AttributeError. **Phase 13.7.5 fix needed BEFORE 13.8 GA.** SUMMARY at `.planning/phases/13.5-python-sdk-and-bench-cli/`. |
| 13.6 | **TypeScript + Go SDKs** (communicate-only, post-rescope) | `@beava/sdk` ESM-only npm package (TS, ~600 LOC) + `github.com/beava-dev/beava/sdk/go` Go module (~600 LOC). Both communicate-only: push events + register schema (JSON pass-through, no DSL) + get/batch_get features. Pipeline DSL stays Python-only. Cross-SDK conformance via single Python orchestrator. | ~8 | ✅ **COMPLETED 2026-05-04 (PASS)** — 8 plans / 35 commits / TS 19/19 + 1 skip + Go all + 1 skip. Conformance harness scaffolded but currently SKIPS (payload sends top-level `kind:"table"` instead of derivation `output_kind=table` — test-design bug, non-blocking, v0.0.x). SUMMARY at `.planning/phases/13.6-typescript-go-sdks/`. |
| 13.7 | **Docs site (beava.dev)** | Integrate Phase 13.0 spec docs into existing `beava-website/` (NOT new MkDocs spin-up) via Markdown→HTML converter. Reuse existing `beava-design-system/` tokens. Pagefind search. Cloudflare Pages deploy. Quickstart polish + operator catalog + wire spec reference. Vertical guides DEFERRED to user-authored interactive follow-up (v0.1+). | ~4 | ✅ **COMPLETED 2026-05-04 (PASS)** — 4 plans / 4 commits / 86 docs pages / 93 Pagefind fragments / 0 broken links (4570 OK). Plan-vs-reality mismatch: PLANs referenced React infra that didn't exist; agent built static-HTML + Pagefind from scratch. **MANUAL**: Cloudflare Pages dashboard auth + DNS for beava.dev (one-time; documented in beava-website/README.md § Deploy). SUMMARY at `.planning/phases/13.7-docs-site-beava-dev/`. |
| 13.5.1 | **Phase 13.5 fix-up: finish Transport impl + decorator hardening** (NEW — inserted 2026-05-04) | Surfaced by Phase 13.5 Plan 11. `HttpTransport`/`TcpTransport`/`EmbedTransport` `send_push`/`send_get`/`send_batch_get`/`send_reset` are stubbed to `NotImplementedError` at the `Transport` base class — Plans 02-07 shipped types + decorators but transport impl was TODO. mypy passed via MagicMock so 0/68 v0 acceptance tests didn't surface during 13.5 execution. Phase 13.5.1 wires up real transport impls against the Phase 13.4 wire surface (verb-style POST routes, OP_PUSH/OP_GET/OP_BATCH_GET/OP_RESET, force/dry_run register flags, flat-dict GET response, sentinel routing for global tables). Also hardens `@bv.table` empty-parameter-annotation (currently raises AttributeError). Greens up the 68 v0 acceptance tests. **BLOCKS 13.8 GA.** | ~3 | 📋 **PROPOSED 2026-05-04** — ~2-3 days estimate; needs `/gsd-discuss-phase 13.5.1` to lock scope |
| 13.7.5 | **Pre-OSS polish — comment audit + test coverage** | Two workstreams: (A) component-by-component comment audit removing AI-slop / restating-the-obvious comments per CLAUDE.md heuristic ("default to no comments; only add when WHY is non-obvious"); (B) test coverage audit producing a feature × test-status matrix, classifying each gap MUST-FIX vs DEFER, and filling MUST-FIX gaps. Outcome: code that reads as engineered (not generated) and a comprehensive test inventory before the v0 GA tag. | ~13 | 📋 **PLANNED 2026-05-03** — captured at `.planning/ideas/phase-13.7.5-pre-oss-polish.md`; 13 plans across 2 workstreams; ~1-2 weeks |
| 13.7.6 | **Pre-OSS security + commit-path sanitization + public-facing files** (NEW — inserted 2026-05-04) | Four workstreams: (C) security audit + lint sweep — `cargo clippy -D warnings`, `cargo audit`, `cargo deny`, `mypy --strict`, `tsc --noEmit`, `go vet`, OWASP Top-10 review via `/cso` skill, threat-model ASVS-L1 narrative, secrets sweep (`git secrets`, `trufflehog`); (D) commit-path sanitization — strip `Co-Authored-By: Claude` trailers from ~30+ historical commits, strip `🤖 Generated with Claude Code` markers, decide history shape (squash vs filter-repo `.planning/`), strip stale worktree branches, branch rename `v2/greenfield → main`, optional repo rename `tally → beava`; (E) public-facing files — LICENSE (Apache-2.0), README, CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, CHANGELOG, .gitignore audit, GitHub issue/PR templates, optional public CI workflow; (F) closure. **BLOCKS 13.8 GA.** | ~24 | 📋 **PROPOSED 2026-05-04** — captured at `.planning/ideas/phase-13.7.6-pre-oss-security-and-commit-path.md`; ~3-5 days; needs `/gsd-discuss-phase 13.7.6` to lock D-01 (history shape) + D-04 detail (subject rewrite scope) + repo rename + author email |
| 13.8 | **Packaging + GA tag** (SHIP) | PyPI multi-arch wheels (Linux x86_64, Linux ARM64, macOS ARM64) with bundled binary + npm `@beava/sdk` + Go module + Docker Hub multi-arch manifest + GitHub Releases × 3 platforms. CI green on all 4 repos. v0.0.0 tag everywhere. Marketing assets (README hero, HN/Twitter posts). | ~6 | 📋 **PLANNED 2026-05-03** — 5-7 days; sequential after 13.5.1 + 13.7.5 + 13.7.6 |
| 13.1 | Perf regression fix — fsync off the runtime thread | `spawn_blocking` for WAL fsync; restored 17k EPS at parallel=64 on macOS | 1 | ✅ **COMPLETE** |
| ~~13.2~~ | ~~Batch coalescing~~ | ~~ApplyConfig 6-knob + ApplyBuffer skeleton~~ | — | ❌ **ABANDONED** — superseded by Phase 13.3 (RefCell + LocalSet, simpler/faster Redis-shaped approach). Branch `phase-13.2-coalesce` is not to be merged; ApplyBuffer primitive is not reused. |
| ~~13.3~~ | ~~Lockless apply via RefCell + LocalSet~~ | ~~Replace apply-state Mutex with single-thread `RefCell` + `LocalSet`~~ | ~~~4~~ | ❌ **REJECTED 2026-04-26** — locked architectural decision: Beava commits to a single-threaded data plane forever (Redis-cluster pattern). Per-instance ceiling = single apply thread; users scale out via multiple Beava instances sharded at entity-key level. Worktree `phase-13.3-lockless-apply` archived (deleted 2026-04-26). Plans 13.3-01..04 in `.planning/phases/13.3-lockless-apply/` retained for historical reference. |
| ~~14~~ | ~~Streaming semantics — Chunk A (correctness)~~ | Watermark + drop + bucket widening | — | ❌ **ARCHIVED 2026-04-30** — killed by no-event-time pivot per `project_redis_shaped_no_event_time_ever`. Dir: `.planning/phases/_archived-14-streaming-correctness-killed-no-event-time/` |
| ~~14.1~~ | ~~Streaming semantics — Chunk B (opt-in modifiability)~~ | Modifiable streams + retraction-impact analyzer | — | ❌ **ARCHIVED 2026-04-30** — depended on Phase 14 watermark; dead. Dir: `.planning/phases/_archived-14.1-streaming-modifiability-killed-no-event-time/` |
| ~~15~~ | ~~Event-time PIT temporal store~~ | `(event_time_ms, lsn)` composite chain | — | ❌ **ARCHIVED 2026-04-30** — event-time gone permanently. Dir: `.planning/phases/_archived-15-event-time-pit-killed-no-event-time/` |
| ~~16~~ | ~~SDK surface v0 ergonomics — explicit `@bv.source` + `app.upsert/delete`~~ | ~~Explicit `@bv.source` annotation on class-form `@bv.event` / `@bv.table`; `app.upsert(T, {...})` + `app.delete(T, key={...})` verbs~~ | — | ❌ **ARCHIVED 2026-04-30** — `app.upsert/delete` verbs are dead with the table strip (Phase 12.7); `@bv.source` was for disambiguating class-form events from class-form tables, but with tables out v0 has no class-form ambiguity. Returns alongside tables in v0.1+ if/when joins land. |
| ~~17~~ | ~~Table aggregation tiered modifiability (v0.1)~~ | ~~`@bv.table.group_by(...).agg(...)` Tier B/C scope~~ | — | ❌ **ARCHIVED-INDEFINITELY 2026-04-30** — depends on `@bv.table` which is being stripped in Phase 12.7 (v0 is events-only). Returns alongside tables in v0.1+ if/when joins land per `project_v0_events_only_scope`. |
| 18 | Redis-shaped hand-rolled hot path | 2/2 | Complete   | 2026-04-26 |
| ~~25~~ | ~~Session window operator family (v0.1+)~~ | ~~`bv.session(gap_ms=..., inner=bv.<op>(...))` activity-based grouping~~ | — | ❌ **ARCHIVED-INDEFINITELY 2026-04-30** — out of v0 scope per `project_v0_events_only_scope`. Users can compose count/sum with processing-time windowed ops for v0 demos. Returns in a future minor release if demand arises. |
| 26 | **Valkey-style I/O architecture rework** (v0.1+) | Inserted 2026-05-03 from post-Phase-12.8 bench session. Beava's IO worker design diverges from the "Valkey 8 model" comments claim: each worker owns a `mio::Poll` and independently polls its assigned client subset (N+1 epoll instances), sending parsed RingItems to apply via crossbeam channel. Valkey IO threads are pure SPSC/SPMC consumers (verified `valkey-io/valkey/src/io_threads.c::IOThreadMain`). 4-phase migration (A measure → B consolidate → C maxclients/backpressure → D validate); Phase A is the **GATE** — abandon if cross-thread channel overhead < 5% of total push CPU. Apply-CPU is 88% of total push time per session's per-stage trace, so the upside is bounded. Plan doc: `.planning/ideas/valkey-io-architecture-rework.md`. **NOT v0 ship-blocker** — pure architecture-debt cleanup if Phase A confirms the cost is real. | ~5 | 💡 **PROPOSED 2026-05-03 (v0.1+)** — gated on Phase A measurement |

**Total active phases:** 27 (Phase 13 RESTRUCTURED 2026-05-03 from a single ship phase into 6 sub-phases — 13.0 + 13.4–13.8 — for the v0-launch design-session deliverables). v0 ship critical path = **Phase 13.0 (design contract + spec docs) ✅ CLOSED 2026-05-03 (PASS)** → **4-way parallel 13.4 (engine) + 13.5 (Python+bench) + 13.6 (TS+Go) + 13.7 (docs site) NEXT** → sequential 13.8 (packaging + GA tag). Phase 12.7 (table strip) ✅ CLOSED 2026-05-01. Phase 12.8 (memory governance) ✅ CLOSED 2026-05-01 (PASS-WITH-WARN). Phase 12.9 (AggOp boxing) ✅ CLOSED 2026-05-03 (PASS). Phase 25 (session windows) and Phase 26 (Valkey IO rework) are v0.1+, not ship-blockers.

**Insertion / archive history:**
- Phase 2.5 inserted 2026-04-23 (dual HTTP+TCP wire); Phase 5.5 inserted 2026-04-23 (perf harness + retroactive baselines + regression gates); Phase 7.5 inserted 2026-04-23 (end-to-end throughput harness + per-phase ledger convention)
- Phase 11.5 inserted 2026-04-23 (temporal tables + retraction primitive) — **RETROACTIVELY-DESCOPED 2026-04-30 per `project_v0_events_only_scope`; surface stripped in Phase 12.7**
- Phase 6.1 inserted 2026-04-24 (async-durability split); Phase 13.1 inserted 2026-04-24 (fsync regression fix); Phase 13.3 inserted 2026-04-24 (canonical apply-lock removal — REJECTED 2026-04-26 per `project_no_sharded_apply`)
- Phase 12.5 inserted 2026-04-24 — ARCHIVED 2026-04-30 (superseded by Plan 12-10 in Phase 12)
- Phases 14 / 14.1 / 15 added 2026-04-24 (streaming-correctness watermark + opt-in modifiability + event-time PIT) — **ALL ARCHIVED 2026-04-30 per no-event-time architectural pivot**
- Phase 16 added 2026-04-24 (SDK surface v0 ergonomics) — **ARCHIVED 2026-04-30 per v0-events-only commitment** (no class-form ambiguity without tables)
- Phase 17 added 2026-04-24 (v0.1 table aggregation) — **ARCHIVED-INDEFINITELY 2026-04-30** (depends on tables; v0 events-only)
- Phase 18 added 2026-04-24 (Redis-shaped hand-rolled hot path — landed 2026-04-26)
- **Phase 12.6 inserted 2026-04-30** (v0 surface reduction — legacy axum kill + event-time strip + dead-code/redundancy sweep + windowed-op time-source swap + join/union removal + REQUIREMENTS sweep + mio-only enforcement) — CLOSED 2026-04-30 PASS-WITH-WARN
- **Phase 12.7 inserted 2026-04-30, CLOSED 2026-05-01 PASS** (table strip — `@bv.table` + temporal store + retraction stripped per `project_v0_events_only_scope`; 10 plans across 4 waves; ~5,500 LOC removed; FORMAT_VERSION RESET 2→1; all 4 CONTEXT decisions honored verbatim; microbench -25 to -30% lift; throughput +7.3% on regression-gate cell)
- **Phase 12.8 inserted 2026-05-01, CLOSED 2026-05-01 PASS-WITH-WARN** (memory governance — opt-in cold-entity TTL via `@bv.event(cold_after=)` + always-on bucket-level reclaim during `update_at()` + lifetime aggregation contract via 4th JSON-prelude shim `unbounded_op_in_lifetime_mode`; 5 Prometheus metric families; `BEAVA_MEMORY_GOV_ENFORCE` env-gate default ON; 9 plans across 5 waves; all 4 CONTEXT decisions honored verbatim; microbench -2.6%, throughput regression-gate -2.5% PASS; fraud-team WARN flagged for Phase 13 — root cause O(N_tables) entity_count_resident snapshot)
- **Phase 12.9 inserted 2026-05-03** (AggOp memory boxing — fraud-team 22 KB/entity → 7 KB CLAUDE.md budget fix). Triggered by post-Phase-12.8 r8g maxcard bench: `size_of::<AggOp>() = 600` because of unboxed `SeasonalDeviationState`. Box 7 fat variants → 600 → ~72 B (8× shrink); fraud-team weighted-avg per-entity 22 KB → ~6 KB. Investigation: `.planning/ideas/per-entity-memory-budget.md`. Tests: `crates/beava-core/tests/per_entity_size_dump.rs`. Gates Phase 13 ship-pitch numbers.
- Phase 25 added 2026-04-30 (session window operator family) — **ARCHIVED-INDEFINITELY 2026-04-30 per v0-events-only commitment**
- **Phase 26 added 2026-05-03 (Valkey-style I/O architecture rework — v0.1+)** — Beava IO workers diverge from Valkey's pure-consumer pattern (verified `valkey-io/valkey/src/io_threads.c::IOThreadMain`). 4-phase migration; Phase A is the GATE (abandon if channel overhead < 5% of push CPU). Apply-CPU is 88% per session's trace so upside is bounded. NOT a v0 ship-blocker. Plan doc: `.planning/ideas/valkey-io-architecture-rework.md`.
- **Phase 13 reframed 2026-04-30** to slim ship scope: SDK polish (events surface) + perf benchmarks + minimum-viable docs + packaging. **Dropped:** `bv.fork`, `playground.beava.dev`, structured logs.
- **Phase 13 RESTRUCTURED 2026-05-03** from the v0-launch design session. Phase 13 is now an umbrella for 6 sub-phases (13.0 / 13.4 / 13.5 / 13.6 / 13.7 / 13.8). 20 SDK design decisions locked. 3 SDK ports confirmed (Python + TypeScript + Go; Java deferred). Phase 13.0 is the bottleneck (design contract + spec docs); 13.4–13.7 run in parallel after 13.0 lands; 13.8 (packaging + GA) is sequential. Q1 partial overturn of `project_v0_events_only_scope` to revive `@bv.table` decorator (aggregation-output only — NOT user-mutable; no upsert/delete/retract). ADRs 001 + 002 documented in 13.0. Total wall-clock estimate: ~6-7 weeks solo / ~3-4 weeks with 3+ contributors.
- **`@bv.table` partial overturn 2026-05-03** of `project_v0_events_only_scope`. Phase 12.7's strip of `@bv.table` was over-broad — it killed both the `app.upsert/delete/retract` user-mutable surface AND the aggregation-output decorator. v0 launch revives ONLY the aggregation-output decorator (function-form `@bv.table(key=...) def Name(ev) -> bv.Table: return ev.group_by(...).agg(...)`). MVCC, TemporalStore, temporal_http.rs, and user-mutable verbs STAY KILLED. ADR-001 (lands in Phase 13.0) is the authoritative record.

**Architectural commitments locked 2026-04-30** (see `project_redis_shaped_no_event_time_ever` + `project_v0_events_only_scope`): no event-time anywhere; no watermarks; no joins; no PIT; no tables (`@bv.table` stripped Phase 12.7); no aggregation beyond the existing 54-op catalogue; no session windows; processing-time-only semantics; mio-only data-plane entry; `event_time_ms` removed from wire; windowed operators on server-side `now_ms()`. v0 = pure events: `@bv.event` + 54 ops + /push + /get + /register + WAL/snapshot durability + Python SDK. Tables/joins/aggregation return together in v0.1+ if/when justified by demand.

**Phase 1 status:** ✅ **COMPLETE** on commits `b100e51`..`c21b6b7`. Cargo workspace, axum HTTP server, `/health` + `/ready` stubs, graceful shutdown, integration TestServer harness — all gates green. See `.planning/phases/01-foundation/01-SUMMARY.md`, `.planning/phases/01-foundation/01-VERIFICATION.md`.

## Parallelization

- **Phases 1 → 2 → 3 → 4 → 5 → 6 → 7 → 7.5** are strictly sequential — each depends on the one before. Phase 5 is where the apply loop first runs real aggregations; Phases 6–7 harden durability around it; Phase 7.5 builds the throughput harness on top of stable durability so EPS numbers reflect production shape (WAL fsync + snapshot/recovery in the path).
- **Phases 8 / 9 / 10 / 11** can run in parallel after Phase 7.5 — each operator family attaches to the existing apply loop + registry + window infra, touching independent operator modules. Recommended: sequence 8 → 9 → 10 → 11 unless explicitly running parallel worktrees. Each must include a "throughput run" task that re-runs the Phase 7.5 harness with that family's operators added to the medium/large pipelines and appends the result to `.planning/throughput-baselines.md`.
- **Phase 11.5** (temporal tables + retraction) depends on 7 (needs WAL + snapshot); can run parallel with 8–11 since it touches its own table-storage module. MUST ship before Phase 12 because joins consume the `as_of=...` kwarg. Throughput run measures upsert/retract path against the temporal-table workload variant.
- **Phase 12** (push/get API completion — joins/unions REMOVED 2026-04-30) depends on 7. Originally also depended on 11.5 for "temporal join resolution" — that dependency is dead. Throughput run adds multi-key push/get + push-and-get pipeline shapes to the harness.
- **Phase 13** waits on everything for the final three-shape perf gate (simple fraud / complex fraud / recommendations ≥ 3M EPS) + `/metrics` + docs sign-off. By Phase 13 the throughput-baselines ledger has ~6 rows showing how EPS evolved phase-by-phase.

## Dependency graph

```
  Phase 1 (Foundation) ✅
       │
       ▼
  Phase 2 (Sources + registry + version bumps)
       │
       ▼
  Phase 2.5 (TCP wire listener + framing + full opcode table)
       │
       ▼
  Phase 3 (Python SDK + decorators + expression DSL, HTTP + TCP)
       │
       ▼
  Phase 4 (Stateless ops + expression evaluator server-side)
       │
       ▼
  Phase 5 (Aggregation framework + 8 core operators)
       │
       ▼
  Phase 6 (WAL + idempotency)
       │
       ▼
  Phase 7 (Snapshot + recovery + schema evolution)
       │
       ▼
  Phase 7.5 (End-to-end throughput harness + first baseline)
       │
       ├────────────┬────────────┬────────────┬────────────┐
       ▼            ▼            ▼            ▼            ▼
  Phase 8       Phase 9      Phase 10     Phase 11     Phase 12
  (recency/     (decay/      (sketches)   (buffer+geo) (joins +
  point ops)    velocity)                              unions + API
                                                        completion)
  ↓ each phase 8-12 ships a "throughput run" task using the 7.5 harness
       └────────────┴────────────┴────────────┴────────────┘
                                   │
                                   ▼
                     Phase 13 (obs + perf + docs + pkg + fork — ship)
```

## Phase details

### Phase 1: Foundation ✅ COMPLETE

**Goal:** A `beava` binary that boots from config, exposes an HTTP server with `/health` and `/ready` stubs, writes structured JSON logs, and runs under an integration test harness.

**Status:** Shipped. See `.planning/phases/01-foundation/01-SUMMARY.md` + `01-VERIFICATION.md`.

**Depends on:** Nothing.

**Requirements:** none (infrastructure phase).

**Success criteria:** (all ✅)
1. `cargo build --release` produces stripped binary; `./beava --config ./beava.yaml` starts HTTP listener, logs JSON
2. `curl localhost:$PORT/health` → 200; `/ready` returns 503 until flag flips
3. axum wired; graceful shutdown on SIGTERM
4. Integration-test harness (`TestServer::spawn()`) exists and tested

### Phase 2: Sources + registry + version bumps

**Goal:** `POST /register` accepts a JSON DAG of events, tables, and derivations; validates; persists in-memory; assigns monotonic `registry_version`. Additive-only — removals/changes return 409 with structured diff. No aggregations execute yet.

**Depends on:** Phase 1.

**Requirements:** SRV-API-01, SRV-API-02, SRV-API-11, SRV-API-12, SRV-REG-01, SRV-REG-02, SRV-REG-03, SRV-REG-05, SRV-REG-06, SDK-DEC-06, SDK-DEC-08, SDK-DEC-09 — 12 REQ-IDs.

**Success criteria:**
1. `POST /register` with a valid JSON DAG (1+ events, 0+ tables) returns 200 with `registry_version: 1` and `registered_descriptors` listing
2. Re-posting an identical DAG is a no-op; version unchanged
3. Posting an additive DAG (new event or table) returns 200 and bumps version
4. Posting a DAG that removes or changes an existing descriptor returns 409 with `{error: {code: "registration_conflict", diff: {added, removed, changed}}}` naming each change
5. Malformed payload (missing required fields, unknown node type) returns 400 with `{error: {code, path, reason}}` pointing to the offending path

### Phase 2.5: TCP wire listener + framing + full opcode table

**Goal:** Ship the server-side TCP fast-path alongside the existing HTTP listener. Custom-framed binary wire `[u32 length][u16 op][u32 request_id][payload bytes]` with the full v0 opcode table designed up front; `register` + `ping` handlers wired; every other opcode (push/push_sync/push_many/get/mget/set/mset) reserved and returns a structured `op_not_implemented` error so later phases just fill in handlers without touching the codec.

**Depends on:** Phase 2.

**Requirements:** SRV-API-NEW (TCP listener), SRV-WIRE-01 through SRV-WIRE-06 (framing), SRV-WIRE-REG-01 (register over TCP). New REQ-IDs added to REQUIREMENTS.md at plan-phase time.

**Success criteria:**
1. Server binds two listeners when configured: HTTP on `http_port`, TCP on `tcp_port` (both configurable via YAML/env); binary starts with both bound by default
2. Frame codec round-trips via proptest: arbitrary `(op, request_id, payload)` → bytes → parsed frame byte-identical
3. `op=ping` returns a pong frame with server's `registry_version` + build-version string
4. `op=register` over TCP delivers the same JSON DAG semantics as `POST /register` (200/400/409 equivalents returned as response frames with matching error shapes) — shares validation + diff engine with HTTP path (no duplicated logic)
5. Unknown / unimplemented opcode returns a `op_not_implemented` response frame; server does NOT close the connection (clients can retry other ops)
6. Connection lifecycle: client opens TCP, issues N requests on one connection (request_id disambiguates responses), closes cleanly; server-side graceful shutdown drains in-flight requests
7. Max frame size bounded (default 4 MiB, configurable); oversized frames produce `frame_too_large` error and connection reset
8. Integration smoke: `phase2_5_smoke.rs` — spin server, TCP-client sends ping + register + unknown-op; assert expected responses

### Phase 3: Python SDK skeleton + decorators + expression DSL

**Goal:** Ship the user-facing Python SDK that compiles decorators + expression DSL into the REGISTER JSON the server accepts. SDK supports both transports via URL scheme (`http://` for HTTP/JSON, `tcp://` for framed TCP) — Phase 3 exercises both against the Phase 2.5 server. Dogfood the DSL from Phase 3 onwards; curl remains the language-agnostic escape hatch.

**Depends on:** Phase 2.5.

**Requirements:** SDK-DEC-01 through SDK-DEC-09, SDK-COL-01 through SDK-COL-06, SDK-COL-08, SDK-APP-01, SDK-APP-02, SDK-APP-03, SDK-APP-15, SDK-WIRE-01 (HTTP transport), SDK-WIRE-02 (TCP transport), SDK-WIRE-03 (URL-scheme dispatch) — 20 REQ-IDs. SDK-COL-07 (schema-reference resolution) moved to Phase 4 because it requires the server-side expression evaluator.

**Success criteria:**
1. `@bv.event` class form extracts schema and registers event descriptor; function form resolves upstreams
2. `@bv.table(key=..., ttl=...)` class + function forms work; key validation at decoration time
3. `bv.col("x") > 100` expression produces expected `to_expr_string()` canonical form
4. `app.register(*descriptors)` topologically sorts the DAG, detects cycles, validates schemas, dispatches to HTTP or TCP based on URL scheme, receives `registry_version`
5. `app.validate(*descriptors)` runs zero-network-IO validation returning `list[ValidationError]`
6. End-to-end smoke: spawn TestServer (with both ports), register 2 events + 1 table from Python twice — once via `bv.App('http://...')` and once via `bv.App('tcp://...')` — identical registry state verifiable via `curl /registry`
7. SDK TCP client round-trips `ping` successfully; connection reuse across multiple `register`/`validate` calls on one App instance

### Phase 4: Stateless ops + expression evaluator (server-side)

**Goal:** Server-side expression parser + evaluator for the `bv.col(...)` canonical form. Stateless per-event op chain (`filter`/`select`/`drop`/`rename`/`with_columns`/`map`/`cast`/`fillna`) executes before aggregations see events. SDK clients register chained ops in their DAG nodes.

**Depends on:** Phase 3.

**Requirements:** SDK-OPS-01 through SDK-OPS-10, SDK-COL-07 (schema-reference resolution, moved from Phase 3 because the expression evaluator lands here), SRV-APPLY-06, SRV-APPLY-07 — 13 REQ-IDs.

**Success criteria:**
1. `Event.filter(bv.col("amount") > 100)` registered via SDK; server rejects events failing the predicate
2. `Event.with_columns(is_big=bv.col("amount") > 500)` adds a derived column visible to downstream nodes
3. Chained ops (`filter → select → with_columns → cast`) compose correctly; schema propagates through every step
4. Proptest-covered: random predicate + random event → truth-table equivalence between client-side eval and server-side eval
5. Malformed predicate in registration returns 400 with path pointing to the offending expression

**Plans:** 7/7 plans complete
- [x] 04-01-PLAN.md — Row + Value + SQL three-valued null logic (beava-core foundation)
- [x] 04-02-PLAN.md — Recursive-descent expression parser with Span tracking + column-pointing errors
- [x] 04-03-PLAN.md — Expression evaluator + cast/isnull builtins + determinism proptest
- [x] 04-04-PLAN.md — Op-chain executor + register-time schema propagator (8 ops + SDK-OPS-01..10 mechanics)
- [x] 04-05-PLAN.md — Register integration: HTTP/TCP parity for invalid_expression errors; OpChain caching
- [x] 04-06-PLAN.md — Phase 4 Rust acceptance: /dev/apply_ops endpoint (gated) + Rust SC1/SC2/SC3/SC5 smokes over HTTP + TCP (completed 2026-04-23)
- [x] 04-07-PLAN.md — Phase 4 Python acceptance: 8 SDK op methods + Python reference evaluator + SC1/SC2/SC3/SC5 Python smokes + SC4 hypothesis proptest (256 cases, client/server eval equivalence)

### Phase 5: Aggregation framework + core operators

**Goal:** `group_by(keys).agg(name=bv.<op>(...), ...)` produces a Table in the DAG; server's apply loop updates per-entity aggregation state for every registered feature touching the event's source. Core 8 operators land (count, sum, avg, min, max, variance, stddev, ratio). `Windowed<Op>` bucket infra.

**Depends on:** Phase 4.

**Requirements:** SDK-AGG-01 through SDK-AGG-06, AGG-CORE-01 through AGG-CORE-09 — 15 REQ-IDs.

**Success criteria:**
1. `Event.group_by("user_id").agg(cnt=bv.count(window="5m"))` registered via SDK produces a Table with `cnt` feature
2. Push to the event updates the aggregation; `/get` returns current value
3. All 8 core operators pass table-driven correctness tests
4. Uniform event-time bucketing cap 64 proven replay-deterministic: replaying the same event stream produces byte-identical state
5. Lifetime/windowless mode works when `window` omitted on compatible operators (ratio, count)
6. Validation: unknown field in `op.field` rejected at registration

**Plans:** 7/8 plans executed
- [x] 05-01-PLAN.md — AggOp enum + per-op state structs (Count/Sum/Avg/Min/Max/Variance/StdDev/Ratio) + Windowed<Op> 64-bucket tumbling (AGG-CORE-01..09, SDK-AGG-03)
- [x] 05-02-PLAN.md — `where=` predicate threading through apply path (SDK-AGG-04)
- [x] 05-03-PLAN.md — AggregationDescriptor + propagate_aggregation_schema (SDK-AGG-01, SDK-AGG-03)
- [x] 05-04-PLAN.md — Register-time Rule 11 + compiled_aggregations cache + HTTP/TCP wire errors (SDK-AGG-05, SDK-AGG-06)
- [x] 05-05-PLAN.md — Apply loop hook + per-entity AggStateTable + /dev/apply_events (SDK-AGG-02, AGG-CORE-09)
- [x] 05-06-PLAN.md — Feature query endpoints GET /get/{feature}/{key} + POST /get + cross-agg collision rule (SDK-AGG-02)
- [x] 05-07-PLAN.md — Python SDK group_by + 8 bv.<op> helpers + REGISTER JSON serialization (SDK-AGG-01..06)
- [x] 05-08-PLAN.md — Phase 5 Rust + Python acceptance smokes (SC1..SC6 coverage)

### Phase 5.5: Perf harness + retroactive baselines

**Goal:** Set up `criterion` bench harness workspace-wide. Write retroactive microbenches for every prior phase's hot path. Establish baseline numbers committed to `.planning/perf-baselines.md`. Establish the regression-gate convention (10% slower = warn; 25% slower = block) that every subsequent phase must honor. This is NOT about optimizing — just measuring, so regressions in later phases (8–13) surface incrementally rather than landing as a surprise in the Phase 13 perf gate.

**Depends on:** Phase 5 (all prior phases must exist to bench).

**Requirements:** PERF-HARNESS-01 (criterion workspace setup), PERF-HARNESS-02 (baselines file + hw-class tagging), PERF-HARNESS-03 (regression thresholds 10%/25%), PERF-BENCH-WIRE-01 (Phase 2.5 frame codec encode/decode throughput), PERF-BENCH-SDK-01 (Phase 3 REGISTER JSON compile throughput), PERF-BENCH-EXPR-01 (Phase 4 parse + eval per op), PERF-BENCH-OPCHAIN-01 (Phase 4 OpChain apply for 4-op chain), PERF-BENCH-AGG-01 (Phase 5 AggOp::update per op), PERF-BENCH-WINDOWED-01 (Phase 5 WindowedOp fold 64-bucket), PERF-BENCH-APPLY-01 (Phase 5 apply_event_to_aggregations per event) — ~10 REQ-IDs to be defined at plan-time.

**Success criteria:**
1. `cargo bench --workspace` runs all benches and produces baseline numbers per hw class
2. `.planning/perf-baselines.md` committed with machine-class-tagged results (e.g., `hw: apple-m1-pro; os: darwin; cpu-count: 10`)
3. Regression gate documented in CLAUDE.md §Conventions and plan-checker contract: every phase from here forward MUST ship at least one microbench
4. Retroactive baselines prove ≥1 bench per phase 2.5/3/4/5
5. Phase 13 end-to-end perf gate (≥3M EPS/core, P99 <10ms batch-get) still the final ship gate — Phase 5.5 does NOT replace it, just surfaces regressions early

**Plans:** 5/6 plans executed

### Phase 6: WAL + idempotency

**Goal:** `/push` ACK returns only after event's LSN has been fsynced. Stream-level `dedupe_key` + window enforced: duplicate requests return the cached response byte-identical.

**Depends on:** Phase 5.

**Requirements:** SRV-DUR-01, SRV-DUR-02, SRV-DUR-03, SRV-DUR-04, SRV-DUR-05 — 5 REQ-IDs.

**Success criteria:**
1. Push event, kill process before fsync, restart → event NOT present. Push event, wait for ACK, kill → event IS present.
2. Duplicate push with same dedupe key within window returns byte-identical response; state unchanged between first and duplicate
3. Group-commit fsync adds P50 < 2ms to push-ACK latency at default config
4. WAL rotation: segments ≤ snapshot-covered LSN truncated; disk usage bounded

**Plans:** 2/4 plans executed
- [x] 06-01-PLAN.md — beava-persistence crate + WAL record frame + WalWriter/WalReader (no fsync)
- [x] 06-02-PLAN.md — Group-commit fsync worker + segment rotation + truncate_up_to
- [ ] 06-03-PLAN.md — IdemCache + /push HTTP endpoint wiring (durable ACK + byte-identical dedupe replay)
- [ ] 06-04-PLAN.md — Crash UAT subprocess tests + criterion perf baselines + phase smoke + PHASE-SUMMARY

### Phase 7: Snapshot + recovery + schema evolution

**Goal:** Periodic snapshot serializes in-memory state + registry; restart loads snapshot + replays WAL-past-snapshot-LSN and resumes. Schema evolution preserved across restart.

**Depends on:** Phase 6.

**Requirements:** SRV-REG-04, SRV-RECOV-01, SRV-RECOV-02, SRV-RECOV-03, SRV-RECOV-04, SRV-RECOV-05 — 6 REQ-IDs.

**Success criteria:**
1. Run 1M events through the server, snapshot fires, restart → all features replayable; values match pre-restart
2. Add a new feature (additive registration + version bump), snapshot, restart → new feature still present
3. RTO: 10GB state snapshot + 1GB WAL tail → server online within 30s on NVMe
4. Corrupt snapshot (flipped byte) detected + logged; operator can fall back to previous

### Phase 7.5: End-to-end throughput harness + first baseline

**Goal:** Build a reusable, hardware-tagged throughput harness that drives a live `beava` server (HTTP + TCP) end-to-end and produces sustained EPS + push/get latency numbers. Capture the first baseline using only Phase 5 operators (count/sum/avg/min/max/variance/stddev/ratio) over Phase 6 WAL durability + Phase 7 snapshot/recovery in the path. Establish the per-phase "throughput run" convention so every operator phase from 8 onward appends a row to `.planning/throughput-baselines.md`. This is NOT about hitting 3M EPS — it is about starting the line and having a stable, comparable measurement system before the operator catalog grows.

**Depends on:** Phase 7 (needs durable + recoverable server in the loop so numbers are production-shaped, not toy-mode).

**Requirements:** THROUGHPUT-HARNESS-01 (harness crate + result schema), THROUGHPUT-HARNESS-02 (`.planning/throughput-baselines.md` ledger format + hw-class tagging matching perf-baselines.md convention), THROUGHPUT-HARNESS-03 (per-phase regression thresholds 10% warn / 25% block on the simple-fraud shape), THROUGHPUT-PIPELINES-01 (small/medium/large pipeline configs: 1 / 5 / 15 features, 1 entity type, 1 window), THROUGHPUT-WORKLOAD-01 (60s wall-time time-bounded run; record EPS, P50/P95/P99 push latency, P99 batch-get, RSS at peak), THROUGHPUT-FIRST-BASELINE-01 (Phase 5-operators-only baseline committed for all 3 sizes on at least one hw-class) — 6 REQ-IDs.

**Success criteria:**
1. `cargo bench --bench throughput` (or equivalent CLI) drives a real server over HTTP + TCP and returns structured results for the small / medium / large pipelines
2. `.planning/throughput-baselines.md` exists with hw-class-tagged rows for the first baseline (small/medium/large × HTTP/TCP) on at least one machine class
3. Plan-checker contract: every phase from 8 onward MUST include a "throughput run" task that re-runs the harness, appends a row, and asserts no > 25% regression on the simple-fraud shape
4. Harness output schema documented and stable across phases (numeric comparisons across phases must work mechanically)

**Plans:** to be written at plan-time (estimated 4 plans: harness crate + result schema, pipeline configs, baseline capture + ledger, smoke + SUMMARY).

### Phase 8: Point / ordinal / recency operators

**Goal:** The point-shaped operator family lands — values, sequences, streaks, recency markers.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with Phases 9, 10, 11, 12.**

**Requirements:** AGG-POINT-01 through AGG-POINT-11, AGG-RECENCY-01 through AGG-RECENCY-04 — 15 REQ-IDs.

**Success criteria:**
1. All 15 operators pass table-driven correctness tests with deterministic replay
2. Operators round-trip through WAL + snapshot + recovery
3. Docs entry per operator in `docs/operators.md`
4. SDK descriptor constructors match v1 API (same parameter names)
5. Throughput run: harness re-run with this phase's operators added to medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape vs Phase 7.5 baseline

### Phase 9: Decay + velocity operators

**Goal:** Exponentially-decayed and velocity-shaped operators land.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 10, 11, 12.**

**Requirements:** AGG-DECAY-01 through AGG-DECAY-07, AGG-VEL-01 through AGG-VEL-08, AGG-Z-01 — 16 REQ-IDs.

**Success criteria:**
1. All 15 operators pass correctness + determinism tests
2. `bv.ema()` alias resolves to `bv.ewma()` in the SDK
3. Half-life parameter validation at decoration time (duration string format)
4. Operators replay byte-identically after restart
5. Throughput run: harness re-run with decay/velocity ops in the medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape

### Phase 10: Sketch operators

**Goal:** Approximate-algorithm operators land with documented error bounds.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 9, 11, 12.**

**Requirements:** AGG-SKETCH-01 through AGG-SKETCH-05 — 5 REQ-IDs.

**Success criteria:**
1. `count_distinct`, `percentile`, `top_k` pass error-bound checks (within documented tolerances on reference datasets)
2. Sketch serialization round-trips through snapshot + WAL replay; deterministic under sketched inputs
3. `bloom_member` and `entropy` pass table-driven tests
4. Memory bounded per-entity by operator configuration
5. Throughput run: harness re-run with sketch ops in the medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape (note: sketches add memory + CPU per insert — large-pipeline regression most likely here)

### Phase 11: Bounded-buffer + geo operators

**Goal:** Histograms, per-user baselines, and geo-shaped operators land.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 9, 10, 12.**

**Requirements:** AGG-BUFFER-01 through AGG-BUFFER-07, AGG-GEO-01 through AGG-GEO-06 — 13 REQ-IDs.

**Success criteria:**
1. All 13 operators pass correctness tests
2. Geo math verified against a reference implementation (`haversine` crate)
3. Structured outputs (histograms, reservoir samples) round-trip through `GET /get/{feature}/{key}` with `{value, meta?}` shape
4. Replay determinism preserved
5. Throughput run: harness re-run with buffer/geo ops in the medium/large pipelines + a recommendation-shape pipeline variant exercising geo-velocity; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape

### Phase 11.5: Temporal tables + retraction primitive

**Goal:** Server-side MVCC storage for `@bv.table(temporal=True, retention=...)` tables, plus an `app.retract(event_id)` primitive scoped to tables in v0. Wires the `as_of=...` kwarg the SDK already ships so Phase 12 joins can resolve event-time PIT lookups. Stream retraction is intentionally deferred to v1 — but the WAL + aggregation format land with stable event-IDs so stream retraction is additive later, not a breaking change.

**Depends on:** Phase 7 (needs WAL + snapshot; temporal versions ride on LSN). **Must ship before Phase 12** (joins consume `as_of=...`).

**Requirements:** SRV-TBL-TEMPORAL-01 through SRV-TBL-TEMPORAL-06 (MVCC storage, retention enforcement, version-at-lsn lookup, tombstone semantics, snapshot of historical versions, memory budget cap), SRV-RETRACT-01 through SRV-RETRACT-03 (retract API wire + idempotency + error shape for non-temporal targets), SDK-TBL-TEMPORAL-01 (already landed — decorator flag), SDK-APP-RETRACT-01 (Python client `app.retract(event_id)`). New REQ-IDs to be defined at plan-time.

**Success criteria:**
1. `@bv.table(temporal=True, retention="7d")` registered via SDK — server stores every version keyed by `(entity_key, lsn)`; evicts versions older than retention window
2. `GET /registry` reports temporal vs non-temporal tables; `as_of=<lsn>` query param on GET returns the version-at-lsn for temporal tables; 400 for non-temporal
3. `POST /retract` with `{event_id}` undoes a table upsert/delete (restores prior version); returns 404 for unknown event_id; returns 409 for events outside retention window
4. Stream retraction is explicitly rejected in v0: `POST /retract` against a stream event_id returns 501 with message pointing at the forward-compat plan
5. Acceptance smoke: register a temporal table, upsert value at t=0, upsert at t=1, retract the t=1 event, assert GET returns t=0 value; assert `GET /table?as_of=t=0` returns t=0 value regardless of retraction state
6. Memory budget: temporal storage ≤ N× non-temporal equivalent for retention window R; measured in Phase 13 perf gate
7. Throughput run: harness re-run with a temporal-table workload variant (upsert-heavy + occasional retract) appended; row added to `.planning/throughput-baselines.md`; baseline table-write throughput captured for the first time

### Phase 12: push/get API completion (joins/unions REMOVED 2026-04-30) — 🟡 PARTIAL

**Status:** Plan 12-02 shipped on branch `phase-12-joins` @ `d541971` (WAL replay for `TableUpsert/Delete/Retract`). Plans 12-01, 12-03, 12-04, 12-05, 12-06 pending on worktree `.claude/worktrees/phase-12-followup`. **Plan 12-07 SHIPPED 2026-04-29** on `v2/greenfield` (production-ready /get on HTTP+TCP through mio apply_shard + main.rs migration to ServerV18 + `dispatch_get_batch` real impl replacing the Plan 18-01 stub + `OP_GET_RESPONSE = 0x0023` allocated + `/health` shim on mio HTTP listener; 22 TDD-paired tasks, 35 new tests; simple-fraud TCP +8.0% vs 19.4 baseline; `python/benches/read_bench.py` runs end-to-end at 1000/1000 OK / p99=1.81 ms; closes the Phase 18 main.rs-migration deferral). **Plan 12-08 SHIPPED 2026-04-29** on `v2/greenfield` @ `c6471bd` (apply-loop overhead reduction; 11 TDD-paired tasks across 6 waves; orchestration 1095→75 ns/event = 14.6×; fraud-team/tcp +10.9%, fraud-team/http +82%; small/tcp regression-gate +1.9% PASS; pool design landed as simpler `acquire/encode/extend/release` rather than RecyclableBytes wrapper per plan's escape hatch; Hetzner Linux baseline + samply trace pending Phase 13 sweep). **Plan 12-09 SHIPPED 2026-04-29** on `v2/greenfield` @ `98e305b` (TCP /get msgpack body+response; HTTP unchanged; 14 TDD-paired tasks across 8 waves; D-A..D-E locked decisions honored; Python SDK `App.get` on tcp:// defaults to msgpack; STRETCH miss documented — ~2-3% codec lift on Apple-M4 microbench vs predicted 40%; integer-leaf fixture not representative of heavy-sketch case; cost-model gap documented per `feedback_cost_model_from_flamegraph`). **Plan 12-10 SCOPED** — push-and-get over mio HTTP+TCP (supersedes Phase 12.5 axum plans; now unblocked by 12-09's GlueResponse shape). **Plan 12-11 SKETCHED 2026-04-29** (chat-only — RecyclableBytes wrapper follow-up; CONDITIONAL on post-12-08 samply showing residual memcpy worth harvesting; user routed to formalize via /gsd-plan-phase after Phase 13 Hetzner sweep). Recommended ordering next: 12-10; 12-11 optional after if samply justifies.

**PARKED 2026-04-29 — Read-path optimization Layers 1 & 2** (deferred until post-v0 ship). Investigated 2026-04-29 via 3 parallel agents (reports at `/tmp/read-encode-overhead.md`, `/tmp/read-dispatch-loop.md`, `/tmp/read-transport-overhead.md`). Combined ~150-200 LOC, est. 1.6-2× lift on read throughput. Layer 1 = reads bypass response_batch (per-client direct write, ~50-70 LOC). Layer 2 = inline write before set_writable + EVFILT_USER on Darwin (~110 LOC). User decision: park for now; pivot to correctness + ship-readiness. Re-evaluate after Phase 13 Hetzner samply confirms which lifts deliver real value on production hardware. Best routed via `/gsd-plan-phase 12-12` post-Hetzner-baseline. (NOTE 2026-04-30: original "pivot to correctness (Phase 14 streaming bug)" rationale is dead — Phase 14 archived by no-event-time pivot. Phase 12.6 v0 surface reduction CLOSED 2026-04-30 (PASS-WITH-WARN); Plan 12-10 push-and-get DEFERRED from v0 per Phase 12.6 D-04. Current critical path is **Phase 13 (NEXT)** → ship.)

**Goal (post-2026-04-30 pivot):** Push/get API completion. `push_sync`, `push_many`, `push_table`, `delete_table`, `set`, `mset`, `mget`, `get_multi`, `push_and_get` (Plan 12-10) wired end-to-end on the mio data plane. **Joins + unions REMOVED permanently** per `project_redis_shaped_no_event_time_ever` — original goal of "Joins (event↔event/event↔table/table↔table) + `bv.union` implemented end-to-end" is dead architecture. `as_of=...` PIT join syntax is dead.

**Depends on:** Phase 7. (Phase 11.5 dependency for "temporal join resolution" is dead — joins removed.) **Parallelizable with 8, 9, 10, 11.**

**Requirements (post-pivot):** SDK-APP-04 through SDK-APP-14, SRV-API-03 through SRV-API-10. SDK-JOIN-01..05 + SRV-APPLY-08 REMOVED 2026-04-30.

**Success criteria (post-pivot):**
1. ~~Event↔event windowed join~~ — REMOVED
2. ~~Event↔table join~~ — REMOVED
3. ~~Table↔table join~~ — REMOVED
4. ~~`bv.union` schema-strict~~ — REMOVED (deferred v0.1+)
5. All push/get API variants pass end-to-end Python SDK tests against a real server
6. Throughput run: harness re-run with the post-pivot pipeline shapes (multi-key push/get, push-and-get) appended; row added to `.planning/throughput-baselines.md`

### Phase 12.5: `push_and_get` combined endpoint — ❌ ARCHIVED-AND-DEFERRED 2026-04-30

**Status:** ARCHIVED-AND-DEFERRED — Phase 12.6-09 (2026-04-30) retired this directory. Originally superseded by **Plan 12-10** in Phase 12 (push-and-get on the mio HTTP+TCP data plane); Plan 12-10 itself is now **DEFERRED entirely from v0** per Phase 12.6 CONTEXT D-04. v0 ships without push-and-get; users do 2 RTs (push then get).

The original axum-shaped 12.5 plans (`12.5-CONTEXT.md` + `12.5-01/02/03-PLAN.md` in `.planning/phases/12.5-push-and-get/`) are dead code; do not execute. Phase 12.6-09 added SUPERSEDED-AND-DEFERRED banners to all four files. The directory is retained for historical reference only — see `.planning/phases/12.6-v0-surface-reduction/12.6-CONTEXT.md` D-04 for the architectural decision.

**Why ARCHIVED-AND-DEFERRED:** Original 12.5 plans assumed legacy axum hot path. Plan 12-07 migrated production to ServerV18 (mio data plane). Plan 12-10 was scoped on top of 12-09's `GlueResponse::QueryResult { body, format }` shape and would have superseded 12.5 entirely — but Plan 12-10 itself was deferred from v0 per Phase 12.6 D-04 to keep the v0 surface tight (users can do push then get in two round-trips for v0). Plan 12-10's PLAN.md stays in place at `.planning/phases/12-server-side-async-push-coalescing/12-10-PLAN.md` for v0.0.x or v0.1+ revisit.

### Phase 12.6: v0 surface reduction — legacy axum kill + event-time strip + dead-code/redundancy sweep + mio-only enforcement — ✅ COMPLETED 2026-04-30 (PASS-WITH-WARN)

**Status:** ✅ COMPLETED 2026-04-30 (PASS-WITH-WARN). 15 plans landed (Plans 01-15 inclusive of Wave-1.5 gap closure 14+15) at HEAD `1e318b1`. Workspace 1067/0/3 with `cargo clippy + cargo fmt` clean. Legacy axum data plane DELETED (~7475 LOC); `event_time_ms` / `event_time_field` / `tolerate_delay_ms` HARD ripped from wire/WAL/snapshot/SDK; joins/unions DELETED; Path X swapped windowed-op time source to server `now_ms()`; mio-only architectural test enforces the locked invariant via `phase12_6_mio_only_dataplane.rs`. Plan 11 microbench captured 3 cells as first measurement; Plan 12 throughput rebaseline at -0.94% on small/tcp gate cell vs post-12-08 baseline (PASS) + +0.5% on fraud-team primary tuning bench. All 5 CONTEXT decisions D-01..D-05 honored verbatim. PASS-WITH-WARN on Plan 02's deadcode buckets (planning-target overshoots categorized as strict-deny test fixtures + post-pivot doc-comments + out-of-plan-scope `tally/` legacy package; clippy-warning floor is 0). SUMMARY: `.planning/phases/12.6-v0-surface-reduction/12.6-SUMMARY.md`. VERIFICATION: `.planning/phases/12.6-v0-surface-reduction/12.6-VERIFICATION.md`.

**Goal:** Collapse the v0 surface to exactly what `project_redis_shaped_no_event_time_ever` defines as the architecture, and sweep all dead code that supported the now-removed event-time / join / legacy-axum paths.

**Depends on:** Plan 12-10 (push-and-get on mio) ideally landed first so the legacy axum kill doesn't have to migrate the push-and-get path through a half-finished mio impl.

**Scope:**

1. **Legacy axum kill** (~3500 LOC):
   - Delete `crates/beava-server/src/server.rs` (2540 LOC — legacy `Server` struct + axum router)
   - Delete `crates/beava-server/src/push.rs` (legacy axum push handler — `apply_event_to_aggregations` call site at :302)
   - Delete `crates/beava-server/src/http.rs`, `crates/beava-server/src/http_admin.rs`
   - Delete `crates/beava-server/src/runtime_core_glue::dispatch_wire_request` (legacy async path retained "for tests and admin callers" — gone with the tests)
   - Delete `BEAVA_DEV_ENDPOINTS` env-var paths
   - Migrate `phase6_crash_probe`, `TestServer`, ~10 smoke tests (`phase6_smoke`, `phase6_1_crash`, `phase7_smoke`, `phase7_restart_cycle`, `phase10_sketch_smoke`, `phase10_sketch_recovery`, `phase11_smoke`, `phase18_07_*`, `phase12_07_main_uses_v18_test`) to a new `TestServerV18` harness; or remove tests whose coverage is replicated elsewhere

2. **Event-time strip** (wire schema bump):
   - Remove `event_time_ms` from push payload schema (HTTP JSON + framed-TCP) + Python SDK + curl recipes + tests
   - Remove `EventDescriptor.tolerate_delay_ms` field + `DEFAULT_TOLERATE_DELAY_MS` constant + `@bv.event(event_time_field=...)` decorator + `@bv.event(tolerate_delay=...)` decorator
   - Remove `AppState.max_event_time_ms` global atomic (no consumer once event-time gone)
   - WAL record format bumps schema version to drop `event_time` field; recovery handles old vs new schema

3. **Windowed-op time-source swap (Path X)** — switch all 14+ windowed operators (`agg_windowed.rs` + decay + velocity + recency + bounded-buffer + geo) from reading event_time_ms to reading server-side `now_ms()`. "Rolling 60s sum" still means 60s of arrival-time. Catalogue stays at 55 ops.

4. **Join + union removal**:
   - Delete `OpNode::Join { other, on, within_ms, join_type }` from `crates/beava-core/src/op_node.rs:71-78`
   - Delete `JoinType { Inner, Left }` enum (`op_node.rs:25-28`, `schema_propagate.rs:1296` reference)
   - Delete `OpNode::Union { others }` (`op_node.rs:81`)
   - Delete Python SDK join/union helpers (`python/beava/_*.py`)
   - Delete `schema_propagate.rs` join/union branches
   - Register-time validator: reject any DAG containing residual join/union references with error code `feature_removed_no_joins_v0` / `feature_removed_no_unions_v0`

5. **Dead code + redundancy sweep**:
   - `cargo-deadcode` (or equivalent) scan across workspace
   - Manual audit for redundant code paths (legacy push.rs vs apply_shard.rs::dispatch_push_sync; multiple WAL replay paths in recovery.rs; bench harness `beava-bench` vs `bench-v18` consolidation)
   - Sweep `phase-13.3-lockless-apply` worktree archival (already noted in STATE.md as TBD)
   - Sweep dead `phase-{N}-followup` worktrees if their work merged or got abandoned

6. **mio-only hot-path enforcement**:
   - Architectural test: assert `apply_shard.rs::dispatch_*_sync` is the only file that calls `apply_event_to_aggregations` post-axum-kill (recovery.rs replay still calls it directly — that's fine, replay is not a hot path)
   - Document in CLAUDE.md as a locked invariant
   - Tokio sidecar restricted to admin endpoints on a separate port

7. **REQUIREMENTS.md sweep** + documentation sweep (beava.dev guide pages, recipes, API docs all need event-time + join references stripped)

**Success criteria:**
1. `cargo test --workspace --all-features` passes with the legacy axum files deleted
2. `cargo build --workspace` clean with zero references to `event_time_ms` (per push payload), `tolerate_delay_ms`, `event_time_field`, `OpNode::Join`, `OpNode::Union`, `JoinType`
3. `grep -rn "axum::" crates/beava-server/src/` returns zero matches (or only on the admin sidecar)
4. cargo-deadcode reports < N% dead code (set N during plan-phase based on baseline)
5. Wire schema version bumped + WAL record schema version bumped; recovery handles both v(N-1) (with event_time) and v(N) (without) for one release cycle, then drops compat
6. Throughput rebaseline: simple-fraud + fraud-team.json zipfian shapes still PASS (no regression > 5% from no-event-time path simplification — actually expect a small lift from removed code in hot path)
7. SUMMARY + VERIFICATION docs land in `.planning/phases/12.6-v0-surface-reduction/`

**Plans:** 15 plans landed (originally 13 + Wave-1.5 gap closure 14+15). Per CLAUDE.md every task is a TDD red→green pair (Phase 3+).

Plans:
- [x] 12.6-01-PLAN.md — Wave 1 — TestServer drop-in rewrite to ServerV18 + phase11_smoke type_mix fix (D-01, D-02) — ✅ landed
- [x] 12.6-02-PLAN.md — Wave 1 — Dead-code baseline scan + threshold setting (DEADCODE-REPORT.md) — ✅ landed
- [x] 12.6-03-PLAN.md — Wave 1 — REQUIREMENTS.md + PROJECT.md surgical sweep (depth: surgical) — ✅ landed
- [x] 12.6-04-PLAN.md — Wave 2 — OpNode::Join / OpNode::Union / JoinType deletion + structured error codes (feature_removed_no_joins_v0 / no_unions_v0) — ✅ landed
- [x] 12.6-05-PLAN.md — Wave 2 — Path X — windowed-op time-source swap from event_time_ms to server now_ms() — ✅ landed
- [x] 12.6-06-PLAN.md — Wave 2 — Event-time hard rip per D-03 — push wire + EventDescriptor + AppState + WAL/snapshot schema bump v1→v2 — ✅ landed
- [x] 12.6-07-PLAN.md — Wave 3 — Legacy axum kill (~7475 LOC actual) — push.rs/http.rs/push_and_get.rs/tcp.rs delete + legacy Server struct delete — ✅ landed
- [x] 12.6-08-PLAN.md — Wave 4 — Python SDK strip — event_time / tolerate_delay / bv.join / bv.union helpers; decorator-time TypeError — ✅ landed
- [x] 12.6-09-PLAN.md — Wave 4 — Worktree + Phase 12.5 + 13.3 archive sweep (SUPERSEDED-AND-DEFERRED banners; STATE.md worktree map) — ✅ landed
- [x] 12.6-10-PLAN.md — Wave 5 — mio-only architectural enforcement test + CLAUDE.md invariant doc — ✅ landed
- [x] 12.6-11-PLAN.md — Wave 6 — Criterion microbench (post-axum-kill apply hot path) + perf-baselines.md row — ✅ landed
- [x] 12.6-12-PLAN.md — Wave 7 — Throughput rebaseline (small/medium/large + fraud-team-zipfian × http+tcp) + throughput-baselines.md row — ✅ landed
- [x] 12.6-13-PLAN.md — Wave 8 — Phase 12.6 SUMMARY.md + VERIFICATION.md + STATE/CORRECTNESS-PATH/ROADMAP closure — ✅ landed (this plan)
- [x] 12.6-14-PLAN.md — Wave 1.5 (gap closure) — Mio data-plane HTTP gap (upsert/delete/retract/table + dev_endpoints + Content-Type 415) — ✅ landed
- [x] 12.6-15-PLAN.md — Wave 1.5 (gap closure) — Test-side migrations + per-test residuals + 4 source-tree fixes (Buckets A/B/C) — ✅ landed

### Phase 12.7: v0 table strip — events-only commitment — ✅ COMPLETED 2026-05-01 (PASS)

**Status:** ✅ COMPLETED 2026-05-01 (PASS). 10 plans across 4 waves landed. HEAD `5645ead`. 26 commits in the Phase 12.7 commit range. Workspace **1049 passed / 0 failed / 4 ignored**; `cargo clippy + cargo fmt` clean. Architectural test pair (`phase12_7_no_table_surface.rs` 3 tests + `phase12_7_legacy_table_handlers_killed.rs` 6 tests) GREEN BY DEFAULT post Plan 10 #[ignore] removal. ~5,500 LOC removed cumulatively (temporal_http.rs ~756 + temporal.rs ~394 + _tables.py ~502 + temporal_throughput.rs ~238 + Plans 03/04/06 surgery). All 4 CONTEXT decisions D-01..D-04 honored verbatim. Microbench (Plan 09): 3 cells SIGNIFICANTLY FASTER (-25.2% to -30.3% vs 12.6 baseline). Throughput rebaseline (Plan 09): small/tcp regression-gate cell **+7.3% above 12.6 baseline** (751,498 EPS vs 700,571); 7/8 cells PASS within ±10%. CLAUDE.md `§ Events-Only Invariant (locked Phase 12.7)` block lands as sibling to existing `§ mio-only Hot-Path Invariant`. SUMMARY: `.planning/phases/12.7-table-strip/12.7-SUMMARY.md`. VERIFICATION: `.planning/phases/12.7-table-strip/12.7-VERIFICATION.md`.

**Goal:** Strip the entire table / temporal / retraction surface from v0 so Beava ships as events-only per `project_v0_events_only_scope`. Walks back Phase 11.5 (temporal MVCC + `app.retract`) and Phase 12.6 Plan 14's mio table handlers.

**Depends on:** Phase 12.6 closed (✅ 2026-04-30 PASS-WITH-WARN).

**Scope:**

1. **Server (Rust) deletes:** `temporal_http.rs` (~756 LOC), `temporal.rs` (~394 LOC, deletes `TemporalStore` / `MvccVersion` / `RetractError`), mio table handlers in `apply_shard.rs:400-459` + `runtime_core_glue.rs`, wire-request variants `WireRequest::HttpUpsert/HttpDelete/HttpRetract/HttpTableGet`, router branches `Route::Upsert/Delete/Retract/TableGet`, http_listener routing for deleted paths, AppState per-table `TemporalStore` map + `event_id_index`.
2. **WAL/persistence reset to FORMAT_VERSION = 1 (D-01 RESET, not bump):** `record.rs::FORMAT_VERSION 2→1`; delete `RecordType::TableUpsert/TableDelete/Retract` variants; `from_u8(0x03|0x04|0x05) → UnknownRecordType`. `snapshot_body.rs::SNAPSHOT_BODY_FORMAT_VERSION 2→1`. `snapshot_header.rs::SNAPSHOT_FORMAT_VERSION 2→1`. `recovery.rs:380+` table-replay branch deleted.
3. **Python SDK deletes:** `python/beava/_tables.py` (~502 LOC), `bv.table` re-export, `App.upsert/App.delete` methods, `from ._tables import TableDerivation` in `_agg.py`. `GroupBy.agg()` rewrites to raise `RuntimeError("Aggregation is not supported in v0; ...")`.
4. **Tests delete + retain selectively:** DELETE `phase11_5_temporal_smoke.rs`, `phase18_07_upsert_delete_rename_test.rs`, `phase12_6_14_mio_temporal.rs`, 6 Python table tests. KEEP `phase18_07_no_tokio_dataplane_test.rs` (non-table 7.1+7.2 architectural assertions). ADD `phase12_7_no_table_surface.rs` + `phase12_7_legacy_table_handlers_killed.rs` (D-03).
5. **REQUIREMENTS.md sweep (D-04 first half):** mark TABLE-* / RETRACT-* / SDK-DEC-04/05 / SDK-AGG-02 / SRV-API-06|07 / SDK-APP-07/08 / V0.1-TABLE-01 as `DESCOPED 2026-04-30` with uniform reason banner. Add positive `V0-EVENTS-ONLY-01` anchor; architectural-test pair satisfies it. Operator-family REQ-IDs (AGG-CORE-* etc.) left ACTIVE.
6. **Phase 11.5 retro-descope (D-04 second half):** banner stamps on `11.5-SUMMARY.md` + `11.5-VERIFICATION.md` + `11.5-CONTEXT.md`. Phase 11.5 dir stays in place per `feedback_logistics_autonomy`.
7. **Microbench + throughput rebaseline (Phase 8+ contract):** rerun `phase12_6_post_axum_kill_apply.rs` 3 cells; append row to `.planning/perf-baselines.md`. Rerun `crates/beava-bench` 8 cells (small/medium/large/fraud-team × http+tcp); regression-gate cell `small/tcp` measured against post-12.6 baseline 700,571 EPS. 10% warn / 25% block thresholds.

**Success criteria:**
1. `cargo test --workspace` returns 0 failures (target ~1059 = 1067 pre-12.7 - 12 deleted + 4 new architectural test fns)
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean
3. `cargo fmt --all --check` clean
4. `pytest python/tests/` clean
5. `phase12_7_no_table_surface.rs` + `phase12_7_legacy_table_handlers_killed.rs` both GREEN at HEAD
6. CLAUDE.md `§ Events-Only Invariant (locked Phase 12.7)` block intact (Plan 10 lands it; sibling to existing `§ mio-only Hot-Path Invariant`)
7. Microbench: no cell ≥ 25% slower than 12.6 baseline (BLOCK gate); ≤ 10% slower (PASS)
8. Throughput regression-gate (small/tcp): ≥ 90% of 700,571 EPS (PASS)
9. SUMMARY + VERIFICATION docs land in `.planning/phases/12.7-table-strip/`

**Plans:** 10 plans across 4 waves. Per CLAUDE.md TDD discipline (Phase 3+) every code task pairs `test:` (red) → `feat:` (green) commits.

Plans:
- [x] 12.7-01-PLAN.md — Wave 1 — JSON-prelude `unsupported_node_kind` shim in `register_validate.rs` (D-02; lands FIRST so subsequent variant deletions are safe per 12.6 Plan 04 lesson) — ✅ CLOSED
- [x] 12.7-02-PLAN.md — Wave 1 — Architectural-test pair (`phase12_7_no_table_surface.rs` + `phase12_7_legacy_table_handlers_killed.rs`); RED at end of Wave 1 → GREEN as Waves 2-3 land deletions (D-03) — ✅ CLOSED
- [x] 12.7-03-PLAN.md — Wave 2 — Mio table dispatch + wire-request + router + http_listener strip (delete `WireRequest::Http*` table variants + `Route::*` table variants + dispatch arms in `apply_shard.rs:400-459`); deleted routes return plain 404 (D-02) — ✅ CLOSED
- [x] 12.7-04-PLAN.md — Wave 2 — `temporal_http.rs` + `temporal.rs` whole-module delete + AppState `temporal_stores`/`event_id_index` field strip + orphan apply_shard.rs:816 cleanup — ✅ CLOSED 2026-05-01 (commit `4d0fabd`; -1,358 LOC; 1 architectural sub-test GREEN, 1 partial; phase12_6_legacy_axum_killed::temporal_http_axum_handlers_deleted repointed to file-absence)
- [x] 12.7-05-PLAN.md — Wave 2 — Persistence schema RESET (D-01): `record.rs::FORMAT_VERSION 2→1` + `snapshot_body.rs::SNAPSHOT_BODY_FORMAT_VERSION 2→1` + `snapshot_header.rs::SNAPSHOT_FORMAT_VERSION 2→1` + `RecordType` variant deletions + recovery.rs branch deletion — ✅ CLOSED 2026-05-01 (commits `5394d2a` test + `9a2012b` feat; +93/-71 across 6 production files + 112-line new test file; 5-test RED→GREEN gate at `phase12_7_format_version_reset.rs`; architectural sub-test `temporal_record_type_variants_deleted` GREEN; Plan 02 RED inventory dropped 8 → 2; workspace 101 files / 1045 cases all pass)
- [x] 12.7-06-PLAN.md — Wave 3 — Python SDK strip: `_tables.py` delete + namespace cleanup + `App.upsert/delete` delete + `_agg.py:GroupBy.agg()` raises v0 error + 6 Python test deletes + 5 surgical strips + 3 Rust test deletes; architectural-test pair turns FULLY GREEN here — ✅ CLOSED 2026-05-01 (commit `8e51539`)
- [x] 12.7-07-PLAN.md — Wave 3 — REQUIREMENTS.md comprehensive sweep (D-04 first half): 8 REQ-IDs DESCOPED with uniform banner + V0-EVENTS-ONLY-01 positive anchor added; AGG-CORE-* / AGG-SKETCH-* etc. operator-family REQ-IDs LEFT ACTIVE — ✅ CLOSED 2026-05-01 (commit `ace00b8`)
- [x] 12.7-08-PLAN.md — Wave 3 — Phase 11.5 retroactive-descope banner stamps on 3 files (D-04 second half); pattern from 12.6 Plan 09's 12.5 banner-stamp — ✅ CLOSED 2026-05-01 (commit `ee81fa2`)
- [x] 12.7-09-PLAN.md — Wave 4 — Microbench rerun (3 cells) + throughput rebaseline (8 cells) + verdict logging (Phase 8+ contract) — ✅ CLOSED 2026-05-01 (commit `358de7a`); microbench -25 to -30% lift; throughput +7.3% on small/tcp gate cell
- [x] 12.7-10-PLAN.md — Wave 4 — Phase 12.7 SUMMARY + VERIFICATION + CLAUDE.md `§ Events-Only Invariant` block + STATE/ROADMAP/CORRECTNESS-PATH closure (advance v0 critical-path → Phase 13); section-ownership contract per 12.6 Plans 09 + 13 — ✅ CLOSED 2026-05-01 (this plan)

### Phase 12.8: Memory governance — cold-entity TTL + lifetime aggregation contract — 📋 PLANNED 2026-05-01

**Status:** PLAN.md files created 2026-05-01. 9 plans across 5 waves; per CLAUDE.md TDD discipline (Phase 3+) every code task pairs `test:` (red) → `feat:` (green) commits.

**Goal:** Two-tier memory hygiene before final ship. (1) Tier 1 entity-level cold-entity TTL via opt-in `@bv.event(cold_after='<duration>')`. (2) Tier 2 bucket-level reclaim within active entities (existing `update_at(now_ms)` mechanism, verified + metric-tracked). (3) Lifetime aggregation contract: every operator declares finite per-entity memory ceiling at register-time; 4th JSON-prelude shim rejects unbounded ops in lifetime mode. Default-ON via `BEAVA_MEMORY_GOV_ENFORCE` env-gate (escape hatch: `=0`). 5 Prometheus metric families on `/metrics`. Architectural test locks the contract.

**Depends on:** Phase 12.7 (events-only commitment locked).

**Locked CONTEXT decisions (`.planning/phases/12.8-memory-governance/12.8-CONTEXT.md`):**
- D-01 — Per-source `@bv.event(cold_after='<dur>')` decorator only (no env-var, no global override; range [1s, 365d])
- D-02 — Implicit lifetime semantics via `windowed=` omission (no new SDK kwarg)
- D-03 — Hard reject at register-time via 4th JSON-prelude shim `pre_check_unbounded_op_in_lifetime_mode` (alongside Plan 12.6-04 / 12.6-06 / 12.7-01 shims)
- D-04 — Per-event reclaim via existing `update_at(now_ms)` mechanism; FRESH-state-on-resurrect locked permanent (Redis TTL pattern)

**Plans:** 9 plans across 5 waves.

Plans:
- [x] 12.8-01-PLAN.md — Wave 1 — 4th JSON-prelude shim `pre_check_unbounded_op_in_lifetime_mode` in `register_validate.rs` + env-gate `BEAVA_MEMORY_GOV_ENFORCE` (Wave 1 default OFF; Plan 04 flips to ON) — placeholder helper returns Unbounded for every input — ✅ LANDED 2026-05-01 (commits `ceac213` test + `9803272` feat)
- [x] 12.8-02-PLAN.md — Wave 1 — `cold_after` kwarg on Python `@bv.event` decorator + `cold_after_ms: Option<u64>` field on Rust `EventDescriptor` (range [1s, 365d] enforced at decoration; round-trips through serde + wire JSON) — ✅ LANDED 2026-05-01 (commits `e4d5e73` test + `72e4d1f` feat)
- [x] 12.8-03-PLAN.md — Wave 2 — Cold-entity TTL eviction on apply hot path (`apply_shard.rs::dispatch_push_sync` reads `descriptor.cold_after_ms`; FRESH-state-on-resurrect via `evict_entity_by_shape_if_cold` in `agg_state_table.rs` + last_seen_ms sidecar HashMaps) — depends on Plans 01 + 02 — ✅ LANDED 2026-05-01 (commits `27a50e0` test + `aa90198` feat; 7 integration tests in `phase12_8_cold_entity_eviction.rs`; workspace 1069/0)
- [x] 12.8-04-PLAN.md — Wave 2 — 54-row classification table populates `lifetime_bound_for_op_str` (`O1` / `BoundedSketch` / `BoundedByRequiredKwarg(<kwarg>)` / `BoundedByConfig(<kwarg>, <default>)`) + `agg_compile.rs` histogram cap-where-missing (env-gate flip moved to Plan 06 for wave-ownership) — depends on Plan 01 — ✅ LANDED 2026-05-01 (commits `dac7150` test + `b096f7c` feat; 15 Rust unit tests + 5 Python E2E tests; workspace +20 tests; clippy/fmt clean; histogram canonical kwarg = `buckets` Vec<f64> per existing wire convention; top_k → `BoundedByConfig("k", 10)` for backward compat with ~10 existing tests; agg_compile.rs cap-where-missing implemented as doc-comment per `feedback_logistics_autonomy` to preserve workspace-stays-green under default-OFF gate; 4 Plan 01 fixtures updated count→histogram-no-buckets as canonical post-Plan-04 rejection example)
- [x] 12.8-05-PLAN.md — Wave 3 — Architectural test `phase12_8_lifetime_ops_have_bounds.rs` (single-file, GREEN-by-default; coverage check that every op-string in `agg_compile::parse_agg_kind` has a classification in `register_validate::lifetime_bound_for_op_str`; sister to 12.6/12.7 architectural-test pairs but single-file because 12.8 has no analogous deletion target) — depends on Plans 01 + 04 — ✅ LANDED 2026-05-01 (commit `e39314d` test; 321 LOC; 3 tests = 1 coverage check + 2 sanity drift guards; GREEN-by-default no `#[ignore]`; `cargo test --workspace` 83 buckets pass; clippy/fmt clean; single `test:` prefix commit per CLAUDE.md TDD §Note 4 doc/test-only-plan exemption — Plan 04 already provides the GREEN behaviour, this test locks the lockstep into CI; Plan 05 bumped Wave 2→Wave 3 per checker W-01 fix so it executes strictly AFTER Plan 04's classification table is populated; match-arm extractor uses look-ahead-for-`=>`-or-`|` rule to distinguish pattern strings from RHS expression strings like `BoundedByRequiredKwarg("buckets")`)
- [x] 12.8-06-PLAN.md — Wave 3 — 5 Prometheus metric families on `/metrics` admin sidecar (`beava_cold_entity_evictions_total` / `beava_lifetime_op_cap_hit_total` / `beava_entity_count_resident` / `beava_bucket_reclaim_total` / `beava_bytes_per_entity_p99`; UNLABELED v0; per-source labels deferred to v0.0.x) + flip `BEAVA_MEMORY_GOV_ENFORCE` env-gate default ON in `apply_shard.rs::memory_gov_enforce_enabled` (escape hatch: `=0`; moved here from Plan 04 for wave-2 file-ownership) — depends on Plans 03 + 04 — ✅ LANDED 2026-05-01 (commits `8295259` test + `41b2f68` feat; 8 metric-endpoint integration tests in `phase12_8_metrics_endpoint.rs` 526 LOC; 3 process-static atomic counters in `agg_state.rs` mirror existing `EntropyStateWrap::categories_capped_count` pattern; `AdminState` plumbing for `entity_count_resident` SKIPPED per Rule 3 deviation — process-static `AtomicU64` instead, identical observable behavior with smaller surface; v0 ships UNLABELED counters per `Claude's Discretion`; `bytes_per_entity_p99 = 7000` static placeholder per PROJECT.md memory budget; 1 legacy fixture inline-fixed: `phase12_8_unbounded_op_in_lifetime_mode.rs::test_no_enforcement_when_env_unset` renamed `test_default_enforcement_on_when_env_unset` and inverted to assert post-flip default-ON; Test 21 escape-hatch test moved to `phase12_8_metrics_endpoint.rs::test_env_var_zero_disables_enforcement` per Plan 06 wave-3 ownership shift; workspace 1095/0; clippy/fmt clean)
- [ ] 12.8-07-PLAN.md — Wave 3 — REQUIREMENTS.md positive anchors `V0-MEM-GOV-01/02/03` under existing § V0-INVARIANT subsection; section-ownership: REQUIREMENTS-only edits
- [ ] 12.8-08-PLAN.md — Wave 4 — Criterion microbench (2 cells: cold_ttl_disabled vs Phase 12.7 baseline; cold_ttl_enabled vs disabled — <5% target) + 8-cell throughput rebaseline (small/medium/large/fraud-team × http+tcp); regression-gate cell small/tcp vs Phase 12.7 baseline (751,498 EPS); 10% warn / 25% block per CLAUDE.md §Performance Discipline — depends on Plans 03+04+06
- [ ] 12.8-09-PLAN.md — Wave 5 — Phase 12.8 SUMMARY + VERIFICATION + CLAUDE.md `§ Memory Governance Invariant (locked Phase 12.8)` block + STATE/ROADMAP/CORRECTNESS-PATH closure (advance v0 critical-path → Phase 13); section-ownership contract per 12.7 Plan 10 — depends on Plans 01-08


### Phase 12.9: AggOp memory boxing — fraud-team 22 KB → 6 KB budget fix — ✅ COMPLETED 2026-05-03 (PASS)

**Status:** Closed 2026-05-03 at HEAD post-`d3eed60` + closure commits. 3 plans landed: Plan 01 boxing (red `ee87d02` + green `d3eed60`); Plan 02 perf gate (no code, 3-run fraud-team/tcp throughput verification); Plan 03 closure (this commit set — SUMMARY + VERIFICATION + perf-baselines + throughput-baselines + CLAUDE.md amendment + STATE/ROADMAP advance). `size_of::<AggOp>()` dropped 600 → 80 bytes (7.5× shrink). Workspace **1097 passed / 0 failed / 4 ignored**. Clippy + fmt clean. Throughput regression-gate `fraud-team/tcp` median **+6.9%** vs Phase 19.4-04 quiescent baseline (102,800 EPS) — boxing did NOT regress; Phase 11 D-08 explicit-no-boxing comment empirically overridden. NO `FORMAT_VERSION` bump (D-03: serde Box<T> transparent). 2 PLANNER-SURFACED CONCERNs deferred to Phase 13: dynamic `bytes_per_entity_p99` sampling + r8g maxcard end-to-end memory rebench. SUMMARY: `.planning/phases/12.9-aggop-memory-boxing/12.9-SUMMARY.md`. VERIFICATION: `.planning/phases/12.9-aggop-memory-boxing/12.9-VERIFICATION.md`.

**Goal:** Close the 22 KB → 7 KB fraud-team per-entity memory gap measured on r8g.4xlarge. Box 7 fat-payload AggOp variants so `size_of::<AggOp>()` drops from 600 B → ~72 B (8× shrink), bringing weighted-avg fraud-team per-entity from ~22 KB to ~6 KB and clearing the CLAUDE.md `~7 KB per entity for a rich 30-feature pack` budget with headroom.

**Why it gates Phase 13:** Phase 13 ships the v0 perf-pitch numbers. Without this fix, the marketing claim "~7 KB / entity → 700 GB for 100M entities" doesn't survive contact with the fraud-team workload — users on heavy sketch pipelines see 3× the budget. Fixing it pre-ship lets the docs and PROJECT.md numbers hold up under scrutiny.

**Depends on:** Phase 12.7 (FORMAT_VERSION just RESET to 1; this phase bumps to 2). Phase 12.8 (memory governance baseline; metrics already in place to verify the lift).

**Locked context (from `.planning/ideas/per-entity-memory-budget.md`):**
- Fix: box `SeasonalDeviation`, `HourOfDayHistogram`, `EventTypeMix`, `GeoVelocity`, `GeoSpread`, `GeoDistance`, `DistanceFromHome` in `crates/beava-core/src/agg_op.rs` (same pattern Phase 10 sketches and `WindowedOp` already use)
- Match-arm derefs in `agg_apply.rs` + `agg_compile.rs` (~10 LOC mechanical)
- WAL `FORMAT_VERSION = 1 → 2`, snapshot `SNAPSHOT_BODY_FORMAT_VERSION = 1 → 2`, `SNAPSHOT_FORMAT_VERSION = 1 → 2` (mirrors Phase 12.6's bump and Phase 12.7's RESET — third schema change in 4 days, needs care with the persistence-test matrix)
- Phase 12.8's static `bytes_per_entity_p99 = 7000` placeholder → dynamic-sampled in same plan (Phase 12.8 follow-up)
- TrendResidual (72 B) and BurstCount (64 B) are borderline; deferred until primary fix lands and the next-largest unboxed variant becomes the visible bottleneck

**Plans (estimated, 3 plans across 2 waves):**
- 12.9-01 — Wave 1 (red) — extend `crates/beava-core/tests/per_entity_size_dump.rs` to assert `size_of::<AggOp>() <= 80`. Confirms RED.
- 12.9-02 — Wave 1 (green) — box the 7 fat variants in `agg_op.rs` + match-arm derefs in `agg_apply.rs` / `agg_compile.rs` + `FORMAT_VERSION` bump 1→2 across WAL/snapshot/snapshot_body + recovery test for old-format rejection (Phase 12.7's pattern) + dynamic-sample `bytes_per_entity_p99` from process-static placeholder. Workspace + clippy + fmt clean.
- 12.9-03 — Wave 2 — re-run maxcard bench on r8g.4xlarge (or whatever EKS cluster is up); confirm fraud-team weighted-avg per-entity ≤ 7 KB; update `.planning/throughput-baselines.md` post-fix row; amend CLAUDE.md `Memory:` line with `(verified Phase 12.9, 2026-05-XX)` footnote. Phase SUMMARY + VERIFICATION.

**Success criteria:**
1. `cargo test -p beava-core --test per_entity_size_dump` asserts `size_of::<AggOp>() <= 80`
2. Maxcard bench on r8g (or equivalent 120 GiB-class node) shows fraud-team weighted-avg per-entity ≤ 7 KB
3. `phase12_8_metrics_endpoint.rs` updated to verify `bytes_per_entity_p99` is dynamically sampled (not 7000 static)
4. WAL/snapshot recovery test confirms `FORMAT_VERSION = 1` files are rejected with structured error (Phase 12.7 pattern)
5. Throughput regression-gate `small/tcp` within ±10% vs Phase 12.8 baseline (no perf surprise from extra heap allocations)
6. Workspace + clippy + fmt clean


### Phase 13: v0 Launch — UMBRELLA — 📋 RESTRUCTURED 2026-05-03

**Status:** RESTRUCTURED 2026-05-03 from the v0-launch design session. Phase 13 is now an **umbrella** for 6 sub-phases (13.0 + 13.4 through 13.8). The legacy plan list (13-01..13-08) is superseded by the new structure.

The user's 4 launch dimensions (benchmark / SDK shape / UI/UX / first-1-min magic) collapse into 6 sub-phases with one foundational bottleneck phase that locks all design contracts in writing, after which 4 implementation phases run in parallel.

**Sub-phases:**

```
Phase 13.0 (design contract + spec docs)  ✅ CLOSED 2026-05-03 (PASS)
                              ┃
                              ┃ ALL specs locked + docs drafted (16 plans, ~158 artifacts, 3 ADRs)
                              ▼
              ┏━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━━┓
              ┃               ┃               ┃
   13.4 engine     13.5 Python+bench   13.6 TS+Go SDKs   13.7 docs site
   ━━━━ 4-5d      ━━━━━━━━━━ 7-10d   ━━━━━ 5-7d         ━━━━ 4-6d
   📋 NEXT         📋 NEXT             📋 NEXT             📋 NEXT
              ┗━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━━┛
                              ┃ all converge
                              ▼
                          Phase 13.8 (packaging + GA tag)  ━━━━━━━ 5-7d
```

**Total wall-clock (1 person): ~6-7 weeks. Total wall-clock (3+ people parallelized): ~3-4 weeks.**

**Locked decisions from the design session (20 total + ADR-003 mid-execution 2026-05-03):**
- `@bv.event` + `@bv.table` decorators (table revival as aggregation-output only, no upsert/delete/retract)
- **First-class global aggregation per ADR-003 (mid-execution 2026-05-03):** `@bv.table` no `key=` form / `events.agg(...)` no `group_by` / `App.get(table_name)` 1-arg / wire-level `key: []` + sentinel `key: ""`. Public `bv.lit(value)` factory exposed (per ADR-003). Implementation deferred: 13.4 (~30 LOC engine sentinel routing) + 13.5 (~110 LOC Python SDK) + 13.6 (~150 LOC TS+Go).
- Dict-style push, single push/get, Redis-shaped client
- Row-shape get + heterogeneous batch_get
- Cross-language: JSON wire is contract, SDKs are thin compilers
- Polars-style chained syntax (chains compile to existing wire)
- Wire ops renamed to Polars conventions (`avg→mean`, `variance→var`, `stddev→std`, `count_distinct→n_unique`, `percentile→quantile`)
- Full HTTP data plane parity, verb-style routes (all POST + JSON body)
- 53-op coverage hand-written, full Python SDK
- Cold-start `{}`, schema/field errors raise, batch atomic
- `bv.App()` no-URL = embed mode (in-memory default); URL connects to remote
- Schema evolution: additive default, `force=True` for destructive (with diff matrix)
- Configurable retry, `max_retries=0` default, single TCP conn + auto-reconnect
- 3 SDK ports: Python + TypeScript (npm) + Go (Java deferred to v0.1+)
- Tiered demos: `bv.demo("adtech" | "fraud" | "ecommerce")`
- `app.reset()` + `bv.App(persist_dir=...)` both in v0
- `dry_run=True` flag on register
- Single `beava` binary with subcommands (Redis-style)
- Module structure: core flat + advanced submodules (`beava.test`, `beava.cli`)
- `beava bench` CLI with 3 modes (throughput / mixed read-write / memory)

**Success criteria (umbrella):**
1. All sub-phases 13.0–13.8 ✅ closed
2. `pip install beava && python -c "import beava as bv; bv.demo('adtech')"` works on a fresh machine
3. `docker run -p 7380:7380 beava/beava` works (curl-only quickstart succeeds)
4. `npm install @beava/sdk` works in TypeScript
5. `go get github.com/beava-io/beava-go` works in Go
6. CI green on `v2/greenfield` + 3 SDK repos
7. `v0.0.0` tag cut on all 4 repos
8. `beava.dev` live with 3 vertical guides + operator catalog + wire spec

**Explicitly DROPPED from v0** (deferred to v0.1+ — see `.planning/ideas/v0.1-deferrals.md`):
- `bv.fork(...)` local scoped replica subcommand
- `playground.beava.dev` hosted interactive tutorial
- Stream-stream / event↔table joins (forever-rejected per `project_redis_shaped_no_event_time_ever`)
- Async / `AsyncApp`
- `app.deregister()` / `app.migrate()`
- Java SDK
- Lifecycle hooks
- Structured `ErrorCode` enum
- Schema introspection (`app.list_descriptors()`, `app.get_schema(name)`)
- `OP_PUSH_BATCH` / `OP_PUSH_SYNC`
- Historical extraction engine (SpeeDB local + SlateDB S3) — see `.planning/ideas/v0.1-historical-extraction-engine.md`

---

### Phase 13.0: Design contract + spec documentation — ✅ COMPLETED 2026-05-03 (PASS)

**Status:** ✅ **CLOSED 2026-05-03 (PASS)**. **16 plans across 3 waves; ~158 doc + script + fixture + test artifacts shipped; ADR-001 + ADR-002 + ADR-003 lock the design contract; v0 critical path advances to 4-way parallel 13.4 + 13.5 + 13.6 + 13.7.** SUMMARY: `.planning/phases/13.0-design-contract-spec-docs/13.0-SUMMARY.md`. VERIFICATION: `.planning/phases/13.0-design-contract-spec-docs/13.0-VERIFICATION.md`.

**Goal:** Produce ship-quality specs for every contract (wire, SDK API per language, pipeline DSL, schema evolution, error codes, operator catalog) — these documents BOTH lock the design AND become the rendered content of beava.dev. No "what's the API shape?" Slack messages during implementation.

**Depends on:** Phase 12.9 ✅ (closed 2026-05-03).

**Documents produced:**

```
docs/
├── wire-spec.md                  ← frame format + opcode table + JSON payloads
├── http-api.md                   ← REST routes, request/response shapes, errors
├── sdk-api/
│   ├── python.md                 ← Python signatures, decorators, exceptions
│   ├── typescript.md             ← TS interfaces, class signatures
│   ├── go.md                     ← Go signatures (context-aware), structs
│   └── shared.md                 ← cross-language semantics
├── pipeline-dsl/
│   ├── overview.md               ← @bv.event, @bv.table, group_by, agg
│   ├── expressions.md            ← bv.col, .filter, .over, .alias, boolean-sum trick
│   └── compilation-rules.md      ← polars chain → JSON wire (worked examples)
├── operators/                    ← all 53 ops with signatures + examples
├── schema-evolution.md           ← additive vs destructive matrix; force=True; dry_run=True
├── error-codes.md                ← exception types, structured codes, recovery
├── concepts/                     ← events vs tables, embed mode, lifetime aggregation
├── quickstart.md                 ← pip install → bv.demo() → first feature query
└── architecture/                 ← single-thread apply, mio data plane, WAL+snapshot

examples/
├── wire/                         ← sample JSON requests/responses (per command)
├── python/                       ← 3 verticals (adtech/fraud/ecommerce)
├── typescript/                   ← same 3 demos in TS
└── go/                           ← same 3 demos in Go

.planning/decisions/
├── ADR-001-bv-table-partial-overturn.md          ← Q1 architectural overturn
├── ADR-002-polars-op-rename.md                   ← Q4 wire op renames
└── ADR-003-global-aggregation-and-bv-lit.md      ← mid-execution 2026-05-03 scope amendment
```

**Plans:** 15 plans across 3 waves

Plans:
- [ ] 13.0-01-PLAN.md — Setup: NUKE 13 stale docs/*.md + create new dir tree + ADR-001 + ADR-002 + memory pointer
- [ ] 13.0-02-PLAN.md — docs/wire-spec.md + 13 JSON Schema 2020-12 contracts + 16+ example fixtures + validator script
- [ ] 13.0-03-PLAN.md — docs/http-api.md (verb-style POST routes for 6 v0 endpoints + admin sidecar)
- [ ] 13.0-04-PLAN.md — 4 SDK API specs (shared / python / typescript / go)
- [ ] 13.0-05-PLAN.md — Operator catalog scaffold: 54 op page stubs + master index + 2 catalog scripts
- [ ] 13.0-06-PLAN.md — Polish 13 op pages: core (8) + sketch (5) + 2 family index pages
- [ ] 13.0-07-PLAN.md — Polish 15 op pages: point-ordinal (5) + recency (10) + 2 family index pages
- [ ] 13.0-08-PLAN.md — Polish 7 decay-family op pages + family index page
- [ ] 13.0-09-PLAN.md — Polish 8 velocity-family op pages + family index page
- [ ] 13.0-10-PLAN.md — Polish 7 bounded-buffer op pages
- [ ] 13.0-11-PLAN.md — Polish 4 geo op pages + buffer-geo/ family index (all 11 ops)
- [ ] 13.0-12-PLAN.md — pipeline-DSL (3 docs) + schema-evolution.md + error-codes.md
- [ ] 13.0-13-PLAN.md — 9 concept + architecture docs (4 concepts + 5 architecture)
- [ ] 13.0-14-PLAN.md — 9 vertical demos (Python+TS+Go × adtech+fraud+ecommerce) + 3 mock backends + smoke test
- [ ] 13.0-15-PLAN.md — Closure: SUMMARY + VERIFICATION + docs/index.md + STATE/ROADMAP advance

Wave structure:
- Wave 1 (parallel after 01 lands): 13.0-02, 13.0-03, 13.0-04 (depends_on: [01])
- Wave 2 (after 05 scaffold + Wave 1 specs): 13.0-06, 13.0-07, 13.0-08, 13.0-09, 13.0-10, 13.0-11, 13.0-12
- Wave 3 (final): 13.0-13, 13.0-14 parallel; 13.0-15 closure last

**Memory + ADR housekeeping (Wave 1):**
- ADR-001 documents the partial overturn of `project_v0_events_only_scope` for `@bv.table` aggregation-output decorator
- ADR-002 documents the Phase 12.7-RESET → Phase 13.0-rename op-name churn rationale
- Update `project_v0_events_only_scope` memory with the partial overturn pointer to ADR-001
- Update `phase12_7_no_table_surface.rs` test plan (actual code update lands in Phase 13.4)
- Update CLAUDE.md if any constraints shift (likely the Memory line + a new Polars-naming footnote)

**Success criteria:**
1. A senior engineer who has never seen beava can read `docs/sdk-api/python.md` + `docs/wire-spec.md` and implement a working Python SDK without asking questions
2. Same for `docs/sdk-api/typescript.md` and `docs/sdk-api/go.md` (peer language SDKs)
3. `examples/wire/` has copy-pasteable JSON for every command (request + response + every error code)
4. `examples/python/adtech.py` is a runnable file (against a mocked engine — engine impl lands in 13.4)
5. ADR-001 and ADR-002 reviewed + signed off
6. `project_v0_events_only_scope` memory updated with partial overturn

---

### Phase 13.4: Engine prep — server-side implementation against the wire spec — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03. Implements the server-side contract from Phase 13.0. Parallel with 13.5/13.6/13.7 (all read from the same spec).

**Goal:** Update the Rust server to honor the v0 wire contract. ~800 LOC of mostly mechanical changes against locked spec docs.

**Depends on:** Phase 13.0 (wire-spec.md + ADRs locked).

**Plans (estimated, ~8 plans across 3 waves):**

- Wave 1 (parallel, ~4 plans):
  - Op renames: `avg→mean`, `variance→var`, `stddev→std`, `count_distinct→n_unique`, `percentile→quantile` (Rust + tests + fraud-team.json + small/medium/large configs)
  - GET response → row-shape (`OP_GET` payload + response shape change)
  - NEW `OP_BATCH_GET (0x0024)` opcode + parser + dispatch + response
  - HTTP route additions/redesigns: `POST /register / /push / /get / /batch_get / /reset / /ping` with consistent verb-style + JSON body
- Wave 2 (parallel, ~3 plans, depends on Wave 1):
  - `force=True` flag on register + diff logic + structural-pipeline-change detection (~150 LOC in `register_validate.rs`)
  - `dry_run=True` flag on register (~30 LOC; uses the diff logic from above)
  - In-memory persistence backend: `Persistence::Memory` enum + bounded-ring WAL (~200 LOC in `crates/beava-persistence/`)
- Wave 3 (~1 plan):
  - `OP_RESET` + `POST /reset` route (~30 LOC)
  - Update `phase12_7_no_table_surface.rs` test to permit `output_kind=table` for derivations
  - **Global-table sentinel routing per ADR-003 (~30 LOC)** — accept `key: []` at register-time in `register_validate.rs`; sentinel `entity_id = ""` routes through the existing `&str` key path in `apply_shard.rs::dispatch_*_sync` (mostly the absence of a special-case rejection — the existing hashmap machinery handles `""` natively). Acceptance gate: `python/tests/v0/test_global.py` (Plan 13.0-16, 8 tests).
  - SUMMARY + VERIFICATION + STATE/ROADMAP advance

**Success criteria:**
1. All wire-spec endpoints return the documented response shape (matched against `examples/wire/*.json` from Phase 13.0)
2. `cargo test --workspace` passes; clippy + fmt clean
3. Throughput regression-gate `small/tcp` within ±10% of Phase 12.9 baseline (renamed-op rename should be free)
4. New tests for: row-shape GET, batch_get heterogeneous, force=True diff (additive vs destructive), dry_run=True, in-memory mode boot, reset, **global-table register + GET round-trip per ADR-003**

---

### Phase 13.5: Python SDK rewrite + `beava bench` CLI — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03. Implements the Python SDK + `beava bench` CLI against the wire-spec + sdk-api specs from Phase 13.0. Parallel with 13.4/13.6/13.7.

**Goal:** Ship the canonical Python client + benchmark tool. ~4200 LOC across two tracks.

**Depends on:** Phase 13.0 (wire-spec.md + sdk-api/python.md locked). CAN start before Phase 13.4 lands by mocking the wire — integration tests run when 13.4 is ready.

**Two tracks:**

**Python SDK (~2200 LOC, ~7-10 days):**
- DELETE: `_events.py`, `_agg.py`, `_schema.py`, `_validate.py`, `_col.py` (~2000 LOC)
- KEEP + bug-fix: `_wire.py` (fix `OP_PUSH = 0x0010` opcode bug), `_transport.py`, `_errors.py`, `_embed.py`
- NEW core client (~200 LOC): `App.register/push/get/batch_get/ping/reset/close` + URL-scheme dispatch + `bv.App()` no-URL embed mode
- NEW pipeline DSL (~600 LOC): `@bv.event` class form + `@bv.table` function form + `bv.col` chained expressions + 53 operator methods + Polars-style `.filter().over().alias()` + `bv.lit()`
- NEW demo loader (~300 LOC): `bv.demo("adtech"|"fraud"|"ecommerce")` with bundled datasets
- NEW test fixtures (~150 LOC in `beava.test`): `fixture`, `replay`, `assert_features_eq`
- PEP 563 fix (`get_type_hints()` instead of raw `param.annotation`)
- Module structure: core flat (`bv.App`, `bv.event`, etc.) + submodules (`beava.test`, `beava.cli`)
- **Public `bv.lit` export per ADR-003 (~5 LOC in `python/beava/__init__.py`)** — promote internal `_Literal` AST node to public namespace as `bv.lit(value)` factory.
- **Global aggregation surface per ADR-003 (~110 LOC across `_events.py` + `_app.py` + decorator factory)** — flip `events.group_by()` empty rejection at `python/beava/_events.py:170-172` to acceptance (~10 LOC); add `events.agg(**aggs)` direct shorthand on EventSource/EventDerivation (~30 LOC); accept `@bv.table` decorator without `key=` kwarg → declares global table (~15 LOC); add `App.get(table_name)` 1-arg overload (~30 LOC). Acceptance gate: `python/tests/v0/test_global.py` (Plan 13.0-16, 8 tests) + `python/tests/v0/test_lit.py` (Plan 13.0-16, 5 tests).

**`beava bench` CLI (~2000 LOC Rust, ~5-7 days):**
- Promote `crates/beava-bench/src/bin/beava-bench-v18.rs` and `beava-bench-v2.rs` to a polished `beava bench` subcommand
- 3 modes: throughput / mixed read-write / memory-only
- Interactive walkthrough via `inquire` crate; `--yes` for non-interactive
- Pre-run memory estimator (uses Phase 12.9 size_of math + per-derivation feature counts)
- Output formats: human (default) / `--json` / `--markdown` / `--append=ledger.jsonl`
- 3 bundled dataset generators (adtech/fraud/ecommerce, each ~300 LOC)

**Success criteria:**
1. Python SDK: `pip install -e python/` then `python examples/sdk_showcase.py` runs end-to-end (no GAP markers remaining)
2. `beava bench --workload=adtech --size=medium --mode=throughput --yes` completes successfully
3. All 3 demos (`bv.demo("adtech"|"fraud"|"ecommerce")`) replay cleanly
4. Pytest fixtures from `beava.test` work (boot embed once + reset per test)
5. mypy clean (or stated incompatibilities)

---

### Phase 13.5.1: Transport impl + decorator hardening (Phase 13.5 fix-up) — 📋 PLANNED 2026-05-04

**Status:** Inserted 2026-05-04 mid-batch-discuss session covering 13.5.1 + 13.7.5 + 13.7.6 + 13.8. Surfaces from Phase 13.5 Plan 11 deficit (0/68 v0 acceptance tests passing — `Http/Tcp/EmbedTransport.send_*` stubbed to `NotImplementedError`; mypy passed via MagicMock).

**Goal:** Wire `HttpTransport` / `TcpTransport` / `EmbedTransport` `send_push` / `send_get` / `send_batch_get` / `send_reset` against the locked Phase 13.4 wire surface (verb-style POST routes; `OP_PUSH=0x10` / `OP_GET=0x20` / `OP_BATCH_GET=0x24` / `OP_RESET=0x40`). Harden `@bv.table` decorator empty-parameter-annotation. Greens up the 68 v0 acceptance tests.

**Depends on:** Phase 13.4 CLOSED + Phase 13.5 CLOSED.

**Detail capture:** `.planning/phases/13.5.1-transport-impl-decorator-hardening/13.5.1-CONTEXT.md` (5 user-locked decisions: D-01 strict TypeError on `@bv.table` empty annotation; D-02 JSON default for TCP read-path wire format; D-03 `features` filter on `send_get`; D-04 rename `*_get_single` private + remove v0.0.x; D-05 embed-only 68-test harness + ~6-test cross-transport equivalence smoke).

**Plans (estimated, ~3-5 plans across 2 waves):**

- Wave 1 (red, parallel): decorator-hardening test (D-01); transport-impl scaffolding + 68-test pytest fixture (`bv.App(test_mode=True)`); transport-equivalence smoke (~6 tests).
- Wave 2 (green, parallel): `_table.py::_resolve_upstream_proxies` strict-TypeError fix; `Http/Tcp/EmbedTransport.send_push/get/batch_get/reset` impls; rename `tcp_get_single`/`http_get_single` → private; amend 68 acceptance test decorators with type annotations.
- Closure: SUMMARY + VERIFICATION + perf-baselines/throughput-baselines append.

**Success criteria:**
1. 68 / 68 v0 acceptance tests GREEN against `bv.App(test_mode=True)` embed engine.
2. ~6 cross-transport equivalence tests assert HTTP / TCP / Embed produce identical results on a canonical fraud-team flow.
3. NO `MagicMock` in any integration test under `python/tests/v0/` or transport-equivalence smoke (anti-pattern enforced by plan-checker contract).
4. `cargo test --workspace --features testing` + `cargo clippy --workspace --all-targets --all-features -- -D warnings` + `cargo fmt --all --check` + `mypy --strict python/beava` GREEN.
5. `@bv.table(key="...")\ndef Fn(events):` raises `TypeError` with helpful message; `@bv.table(key="...")\ndef Fn(events: Click):` works.

**Estimated wall-clock:** 2-3 days.

**Blocks:** Phase 13.8 GA.

---

### Phase 13.6: TypeScript + Go SDKs (communicate-only) — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03. **RESCOPED 2026-05-03** to communicate-only per user directive: pipeline authoring is Python-only; TS+Go ship wire-thin clients (~600 LOC each, down from ~1800). Parallel with 13.4/13.5/13.7.

**Goal:** Ship `@beava/sdk` (npm, ESM-only) and `github.com/beava-dev/beava/sdk/go`. Both implement the cross-language **communicate** surface from `docs/sdk-api/shared.md` — `App` constructor + URL-scheme dispatch + register (pre-compiled JSON pass-through) + push/pushSync + get/batchGet (per-entity + global) + reset + ping + close. NO pipeline DSL.

**Depends on:** Phase 13.0 (wire-spec.md + sdk-api/{typescript,go,shared}.md locked). Conformance test in Plan 13.6-07 needs Phase 13.4 engine accepting the wire register payload (`kind: "table"` revival per ADR-001 partial-overturn).

**Repo layout (USER-LOCKED D-02):**
- Monorepo at `github.com/beava-dev/beava/` (renamed from codename `tally` at v0 ship; see Phase 13.8)
- TS SDK source: `sdk/typescript/` — published to npm as `@beava/sdk`
- Go SDK source: `sdk/go/` — module path `github.com/beava-dev/beava/sdk/go`

**TypeScript SDK (~600 LOC TS, USER-LOCKED D-01 ESM-only):**
- `BeavaApp` class with all 8 wire methods (constructor / register / push / pushSync / get / batchGet / reset / ping / close)
- HTTP transport via `fetch` (Node 18+ baseline)
- TCP transport via `node:net` (Redis-style strict-FIFO correlation)
- Embed mode via `node:child_process` spawn (mirrors `python/beava/_embed.py` 4-step discovery; per-instance temp CWD for WAL/snapshot isolation)
- ESM-only output (`tsc --module esnext --target es2022 --strict`)
- vitest suite

**Go SDK (~600 LOC Go):**
- `App` struct with all wire methods
- HTTP transport via `net/http`
- TCP transport via `net.Conn` (Redis-style FIFO; one writer + one reader goroutine)
- Embed mode via `os/exec` (4-step binary discovery matching python; per-instance temp CWD)
- Functional options (`WithForce`, `WithDryRun`, `WithTimeout`, `WithBinaryPath`, `WithTestMode`)
- `App.GetGlobal(ctx, table)` separate method per ADR-003 Go convention
- standard `testing` + `httptest`

**Cross-SDK conformance (~250 LOC Python + ~50 LOC each adapter, USER-LOCKED D-03):**
- Single Python orchestrator at `python/tests/conformance/test_cross_sdk.py`
- Drives all 3 SDKs against the same `scenario.json`
- Asserts identical outputs across Python+TS+Go
- Runs in CI on every PR (skipped if `beava` binary or `node`/`go` toolchain missing; engine-alignment-error skip when Phase 13.4 lags docs/wire-spec.md)

**Doc patches (USER-LOCKED D-04):** `docs/sdk-api/typescript.md` + `docs/sdk-api/go.md` rewritten for communicate-only scope; `docs/sdk-api/shared.md` clarifies authoring=Python-only / communicate=universal; ROADMAP §13.6 (this entry) + `docs/quickstart.md` audited.

**Success criteria:**
1. `npm install --prefix sdk/typescript && npm test && npm run build` clean
2. `go vet ./sdk/go/... && go test ./sdk/go/...` clean
3. Cross-SDK conformance test PASSES (or skips cleanly with documented engine-alignment caveat)
4. Doc patches landed; ROADMAP/quickstart consistent

---

### Phase 13.7: Docs site (beava.dev) — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03. Renders the Phase 13.0 spec docs into a published docs site. Parallel with 13.4/13.5/13.6.

**Goal:** beava.dev live with quickstart + operator catalog + 3 vertical guides + wire spec + SDK references.

**Depends on:** Phase 13.0 (specs are the source content). Can polish/screenshot/integrate working examples once 13.4/13.5 lands but the structural site work starts immediately after 13.0.

**Plans (estimated, ~5 plans):**

- MkDocs Material (or similar) site scaffold + navigation + theme matching `feedback_beava_website_voice` (DuckDB/bun/Linear pattern, not enterprise-y)
- Render `docs/` from 13.0 into the site (mostly mechanical)
- Quickstart polish with screenshots / animated GIF of `bv.demo("adtech")` running
- 3 vertical guides (adtech / fraud / ecommerce) — written prose tutorials wrapping the demo datasets
- Home page hero per Q17 recommendation: combined latency + throughput + memory headlines (`<10ms P99 / 100K+ EPS / ~6KB per entity`)
- Search index + cross-linking + copy-to-clipboard buttons on code blocks
- Existing `project_beava_website_ia` memory (Priya target user; home/guide/docs/community/cloud-banner IA) already locked — follow it

**Success criteria:**
1. beava.dev live (deployed via Cloudflare Pages or Netlify)
2. All 53 operators have a docs page with example
3. 3 vertical guides each have a working pipeline + sample data + expected output
4. Quickstart copy-pastes successfully on a fresh machine
5. Wire spec reference is published (load-bearing for SDK contributors)

---

### Phase 13.7.5: Pre-OSS repo polish — comment audit + test coverage audit — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03 mid-Phase-13.0 per user directive ("we need a plan to review our repo to prepare for OSS, ... right now code are overloaded with comments. Also we need to check if our test coverage for every features and ops are good enough"). Slot: between Phase 13.7 (docs site) and Phase 13.8 (packaging+GA) — code in final form before public eyeballs.

**Goal:** Two workstreams: (A) component-by-component comment audit removing AI-slop / restating-the-obvious comments per CLAUDE.md heuristic ("default to no comments; only add when WHY is non-obvious"); (B) test coverage audit producing a feature × test-status matrix, classifying each gap MUST-FIX vs DEFER, and filling MUST-FIX gaps. Outcome: code that reads as engineered (not generated) and a comprehensive test inventory before the v0 GA tag.

**Depends on:** Phase 13.4 + 13.5 + 13.6 + 13.7 ALL CLOSED. Polishing earlier would clean code that's about to be rewritten.

**Detail capture:** `.planning/ideas/phase-13.7.5-pre-oss-polish.md` (179 lines) — original capture; `.planning/phases/13.7.5-pre-oss-code-polish/13.7.5-CONTEXT.md` — locked decisions from 2026-05-04 batch-discuss (3 decisions: D-01 idea-doc heuristic verbatim; D-02 8 component plans with Wave-2 parallelism; D-03 MUST-FIX = v0 ship-pitch surface only).

**Plans (locked at 12 plans across 3 waves; cross-SDK conformance plan dropped to v0.1+ deferrals per D-03):**

- Wave 1 — comment-audit conventions + 8 per-component scrubs (parallelizable via worktrees):
  - 13.7.5-01 conventions doc + heuristic checklist
  - 13.7.5-02 `crates/beava-core/`
  - 13.7.5-03 `crates/beava-server/`
  - 13.7.5-04 `crates/beava-runtime-core/`
  - 13.7.5-05 `crates/beava-persistence/`
  - 13.7.5-06 `crates/beava-bench/` + `beava-bench-v2/`
  - 13.7.5-07 `python/beava/` (post-13.5-rewrite scrub)
  - 13.7.5-08 `examples/{python,typescript,go}/`
- Wave 2 — coverage matrix + gap fill:
  - 13.7.5-09 coverage matrix CSV (feature × test-file × test-status); gap classification MUST-FIX vs DEFER
  - 13.7.5-10 fill Rust gaps (per-crate)
  - 13.7.5-11 fill Python gaps (extend `python/tests/v0/`)
- Wave 3 — closure:
  - 13.7.5-13 SUMMARY + VERIFICATION + STATE/ROADMAP advance to Phase 13.7.6

**Success criteria:**
1. ~3000-8000 LOC of redundant comments removed across the codebase (net-negative LOC)
2. `COVERAGE-MATRIX.md` exists at `.planning/phases/13.7.5-pre-oss-polish/` with one row per operator (53), wire endpoint (6), schema-evolution flag, CRUD verb, architectural invariant
3. Every MUST-FIX gap has a corresponding test commit
4. Cross-SDK conformance harness asserts Python/TS/Go produce identical per-entity outputs for ≥1 representative scenario
5. `cargo test --workspace` + `cargo clippy --workspace --all-targets --all-features -- -D warnings` + `cargo fmt --all --check` all pass
6. `cd python && python -m pytest tests/v0` passes ≥80% of tests once engine is up (Phase 13.4) + SDK is up (Phase 13.5)

**Estimated wall-clock:** 1-2 weeks with parallelism.

---

### Phase 13.7.6: Pre-OSS repo polish — security + commit-path + public-facing files — 📋 PLANNED 2026-05-04

**Status:** Inserted 2026-05-04 mid-batch-discuss session covering 13.5.1 + 13.7.5 + 13.7.6 + 13.8. Captured per user directive ("we need to also run clippy on our repo. ... For commit please remove claude code from all commit and only place me as commit. Dont show AI in all commits"). Companion to 13.7.5: 13.7.5 cleans the **code surface**; 13.7.6 cleans the **repo surface** (history, public files, dependencies).

**Goal:** Ship a public-facing GitHub repo that's professional, free of AI-tooling artifacts, free of secrets / private business reasoning, free of CVE'd dependencies. Three workstreams: (C) security audit + lint sweep; (D) commit-path sanitization (history rewrite + branch rename + repo rename); (E) public-facing files audit/refresh.

**Depends on:** Phase 13.7.5 CLOSED (clippy / lint / mypy run cleaner after comment audit; running them on code about to be edited is wasted work).

**Detail capture:** `.planning/ideas/phase-13.7.6-pre-oss-security-and-commit-path.md` — original capture; `.planning/phases/13.7.6-pre-oss-repo-polish/13.7.6-CONTEXT.md` — locked decisions from 2026-05-04 batch-discuss (6 decisions: D-01 keep history + strip `.planning/` + `CLAUDE.md` + `.claude/` via `git filter-repo`; D-02 trailer-only surgical AI-attribution scrub; D-03 repo rename `tally → beava` under `beava-dev` GitHub org; D-04 author email `hoang@beava.dev`; D-05 `CLAUDE.md` stripped from public repo entirely [USER OVERRIDE]; D-06 SKIP public docs of architectural invariants — CONTRIBUTING.md = test/lint/workflow basics only; tripwires educate breakers organically [USER OVERRIDE]).

**Plans (locked at 24 plans across 4 workstreams):**

- **Workstream C — security + lint sweep (8 plans):** clippy `-D warnings` sweep + `cargo audit` + `cargo deny` + Python ruff + mypy --strict re-verify + tsc --noEmit + go vet + `/cso` OWASP review + ASVS-L1 threat model + secrets sweep on full history.
- **Workstream D — commit-path sanitization (6 plans):** bare-clone filter-repo rehearsal + trailer-strip callback + path strips (`.planning/` + `CLAUDE.md` + `.claude/`) + worktree cleanup + branch rename `v2/greenfield → main` + repo rename `tally → beava`.
- **Workstream E — public-facing files (9 plans):** LICENSE audit + README rewrite + CONTRIBUTING (per D-06 minimal) + SECURITY + CODE_OF_CONDUCT + CHANGELOG synthesis + `.gitignore` audit + `.github/ISSUE_TEMPLATE` + `.github/workflows` audit.
- **Workstream F — closure (1 plan):** SUMMARY + VERIFICATION + STATE/ROADMAP advance to Phase 13.8.

**Success criteria:**
1. `git log --all --pretty=format:"%H %ae %s" | grep -iE 'claude|🤖'` returns 0 lines (trailer-strip complete).
2. `git log --all --pretty=format:"%H" -- .planning/ CLAUDE.md .claude/` returns 0 lines (path strips complete).
3. Every commit's author = `Hoang Phan <hoang@beava.dev>` (mailmap rewrite complete).
4. `cargo clippy --workspace --all-targets --all-features -- -D warnings` GREEN with zero `#[allow(...)]` debt unjustified.
5. `cargo audit` + `cargo deny` GREEN.
6. Repo lives at `github.com/beava-dev/beava` with default branch `main`.
7. Clean clone of public repo → `cargo test --workspace` GREEN; `pytest python/tests` GREEN.
8. LICENSE / README / CONTRIBUTING / SECURITY / CODE_OF_CONDUCT / CHANGELOG / `.gitignore` / `.github/` all audited and refreshed.

**Pre-launch user actions (parallelizable with execution):**
1. Verify or create the `beava-dev` GitHub org.
2. Add + verify `hoang@beava.dev` on the GitHub account that owns `beava-dev/beava`.

**Estimated wall-clock:** 3-5 days.

**Blocks:** Phase 13.8 GA.

---

### Phase 13.8: Packaging + GA tag — 📋 PLANNED 2026-05-03

**Status:** Inserted 2026-05-03. Final phase — sequential after 13.4/13.5/13.6/13.7/13.7.5 all land.

**Goal:** Cut v0.0.0 across all 4 repos and ship.

**Depends on:** Phases 13.4 + 13.5 + 13.6 + 13.7 all closed.

**Plans (estimated, ~6 plans):**

- PyPI multi-arch wheels: Linux x86_64 / Linux ARM64 / macOS ARM64 (3 wheels with bundled binary, ~10-20 MB each)
- npm: `@beava/sdk` (single package, all platforms — no native binary, just TS)
- Go module: `github.com/beava-io/beava-go` (single module)
- Docker Hub: `beava/beava:v0.0.0` (multi-arch manifest)
- GitHub Releases: 3 platform binaries (no bundled SDK — for direct download)
- CI green on all 4 repos
- v0.0.0 tag cut on all 4 repos
- Marketing assets: README hero, HN/Twitter/Reddit posts drafted, demo video
- `examples/quickstart.sh` curl-only path tested manually
- Brew formula (optional, can ship in v0.0.x if not done in 13.8)

**Success criteria:**
1. `pip install beava && python -c "import beava as bv; print(bv.demo('adtech').replay())"` works on a fresh machine (Linux x86_64, Linux ARM64, macOS ARM64)
2. `docker run -p 7380:7380 beava/beava:v0.0.0` works
3. `npm install @beava/sdk` + sample TS code runs
4. `go get github.com/beava-io/beava-go` + sample Go code runs
5. v0.0.0 GitHub Release published with binaries + changelog
6. v0 LAUNCH 🚀

### Phase 13.1: Perf regression fix — fsync off the runtime thread — ✅ COMPLETE

**Status:** Merged to `v2/greenfield` as `5b60bdc` (merge) / `2f3a092` (impl) / `a03730e` (regression test). Restored ~17k EPS at parallel=64 on macOS Apple-M4.

**Goal:** Move the WAL fsync syscall off the Tokio `current_thread` runtime via `spawn_blocking` so long fsyncs don't starve the apply loop.

**Depends on:** Phase 6.1 (WAL async dispatch path).

**Success criteria:**
1. WAL fsync never runs on the runtime thread (verified by regression test `test_fsync_does_not_stall_runtime`)
2. 10× throughput regression observed pre-fix is closed (measured in `.planning/throughput-baselines.md`)
3. No new flakes; 850 tests green on `v2/greenfield`

### ~~Phase 13.2: Batch coalescing~~ — ❌ ABANDONED

**Status:** Spike shipped on branch `phase-13.2-coalesce` @ `2122a16` (Plan 01 — ApplyConfig 6-knob + ApplyBuffer skeleton + 20 tests; RYW default preserved). **Do not merge.** Follow-up plans 02–05 are cancelled.

**Why abandoned:** Phase 13.3 (RefCell + LocalSet) is simpler, faster, and Redis-shaped. The ApplyBuffer primitive from 13.2 is not reused — 13.3 removes the Mutex outright rather than amortizing contention across it.

### Phase 13.3: Lockless apply via RefCell + LocalSet (Option 0) — ❌ REJECTED 2026-04-26

**Status:** REJECTED. Worktree `.claude/worktrees/phase-13.3-lockless-apply` archived (deleted 2026-04-26 during repo cleanup). Plans 13.3-01..04 retained in `.planning/phases/13.3-lockless-apply/` for historical reference.

**Why rejected:** Locked architectural decision 2026-04-26 — Beava commits to a single-threaded data plane forever (Redis-cluster pattern). For aggregate throughput beyond the per-instance ceiling, users run multiple Beava instances sharded at entity-key level. In-process apply sharding (RefCell + LocalSet, Option 0) was rejected because:

1. Phase 18's hand-rolled hot path already achieves the apply-loop performance the rejected refactor targeted (~1 µs/event end-to-end at saturated load post-Plan-18-06; ~600 ns inside agg).
2. Cross-shard query semantics within a process add complexity without commensurate user value vs. the multi-instance scale-out path.
3. The Plan 18-12 + state_tables-Vec[agg_id] + encode-off-apply chain already removed the dominant Mutex contention paths; further apply-thread parallelism gives diminishing returns.

Per-instance throughput ceiling at v0 ship time: ~470–520k EPS for simple-fraud msgpack TCP on M4 (saturated, p=16/pd=256). For higher aggregate, scale-out via multiple Beava instances.

### Phase 14.1: Streaming semantics — Chunk B — ❌ ARCHIVED 2026-04-30

**Status:** Killed by no-event-time architectural pivot (`project_redis_shaped_no_event_time_ever`). Depended on Phase 14 watermark; both archived together.

**Original goal (now dead):** opt-in `@bv.event(modifiable=True)` + per-(entity,feature) modification log + retraction-impact analyzer + Tier 3 operator state redesign. With event-time gone, streams have no out-of-order events to replay; modifiability becomes meaningless. Table retraction via explicit `app.retract(event_id)` survives (per `project_stateful_architecture` Decision 1).

**Original artifacts preserved at:** `.planning/phases/_archived-14.1-streaming-modifiability-killed-no-event-time/` (CONTEXT + 6 plans). Do not execute.

### ~~Phase 14: Streaming semantics — Chunk A~~ — ❌ ARCHIVED 2026-04-30

**Status:** Killed by no-event-time architectural pivot. Watermark + late-event drop + `agg_windowed` bucket-widening machinery all dead. The bucket-reset silent-data-loss bug class disappears with event-time itself.

**Original artifacts preserved at:** `.planning/phases/_archived-14-streaming-correctness-killed-no-event-time/` (CONTEXT + 4 plans). Do not execute.

### Phase 15: Event-time PIT temporal store — ❌ ARCHIVED 2026-04-30

**Status:** Killed by no-event-time architectural pivot. The `(event_time_ms, lsn)` composite chain, watermark-derived retention sweep, and `GET /table?as_of=...` dev gate are all dead. PIT joins are dead (joins themselves removed).

**Phase 11.5's LSN-keyed MVCC chain remains** — table retraction via explicit `app.retract(event_id)` still uses LSN-based MVCC. Retention is arrival-LSN-age based, not event-time-age based.

**Original artifacts preserved at:** `.planning/phases/_archived-15-event-time-pit-killed-no-event-time/` (CONTEXT + 3 plans). Do not execute.

### Phase 25: Session window operator family (v0.1+) — 📋 PLANNED

**Status:** Inserted 2026-04-30 from no-event-time architectural pivot. Replaces event-time-grouped windowed activity aggregation that was eliminated. **Not v0 ship-blocker** (users can compose count/sum with processing-time windowed ops for v0 demos).

**Goal:** Add a session-window aggregation primitive — activity-based grouping, processing-time only, no event-time. Per (entity, feature): open session on first event, increment inner per event within `gap_ms`, close on `now_ms() - last_event_ms > gap_ms`. New AggKind variant + per-entity state machine + WAL replay.

**Locked decisions (from 2026-04-30 discussion):**
- D-01: SDK shape — `bv.session(gap_ms=..., inner=bv.<op>(...))`. Inner ops cover the full op set (count/sum/avg/sketch/decay/etc. — same surface as windowed ops).
- D-02: Close semantics — **both** lazy-on-query (`now_ms() - last_event_ms > gap_ms` reads as closed) AND flip-on-next-event-after-gap (next event explicitly closes the previous session and opens a new one). Deterministic state for downstream consumers + correct read-side semantics.
- D-03: Retention — **latest closed session only** per (entity, feature). Fixed memory. Users wanting history compose with `count(session(...))` etc.
- D-04: WAL replay — deterministic in arrival order; session state replays correctly because gap-based close is purely a function of `now_ms()` advance + arrival sequence.
- D-05: No event-time — uses server-side `now_ms()` exclusively (consistent with `project_redis_shaped_no_event_time_ever`).

**Depends on:** Phase 12.6 (event-time strip needs to land first; session windows assume `now_ms()` time source already in place for the rest of the operator catalogue).

**Plans (estimated):** 5-7 plans
- 25-01: AggKind::Session variant + per-entity SessionState struct + open/closed flag plumbing
- 25-02: SDK decorator (`bv.session(...)`) + register-time validation
- 25-03: Close-on-next-event-after-gap state machine + lazy-on-query computation
- 25-04: WAL replay determinism test + recovery correctness
- 25-05: Criterion microbench (open/close/increment paths < 50 ns/event) + throughput rebaseline row
- 25-06: SUMMARY + VERIFICATION + docs

**Success criteria:**
1. `bv.session(gap_ms=N, inner=bv.count())` returns the count of events in the latest closed session per entity
2. Both close paths (lazy-on-query, flip-on-event) produce the same observable behavior in tests
3. WAL replay reproduces session state byte-identically
4. Per-event session-state update cost < 50 ns on Apple-M4
5. Throughput on simple-fraud × session-window pipeline within 5% of pre-session baseline

### Phase 26: Valkey-style I/O architecture rework — 💡 PROPOSED 2026-05-03 (v0.1+)

**Status:** Inserted 2026-05-03 from post-Phase-12.8 bench session on r8g.4xlarge. **NOT v0 ship-blocker.** Architecture-debt cleanup with bounded upside; gated on a Phase A measurement that may abandon the rework.

**Goal:** Reconcile beava's IO architecture with the "Valkey 8 model" comments in `crates/beava-runtime-core/src/io_thread_worker.rs` claim. Beava currently diverges:

- **Valkey** (verified `valkey-io/valkey/src/io_threads.c::IOThreadMain`): ONE `epoll_wait` on main thread; IO threads are pure SPSC/SPMC consumers that never call `epoll_wait`.
- **Beava current**: N+1 `mio::Poll` instances; each IO worker independently polls its assigned client subset and sends parsed RingItems to apply via crossbeam channel; apply thread spin-loops on `try_recv` then falls to 50 µs `recv_timeout`.

For workloads with few hot connections at high pipeline depth (the typical bench shape), this overhead is wasted. But apply-CPU is **88%** of total push time per session's per-stage trace, so the upside is bounded.

**Why post-v0:** The fix is a wire-stack rewrite (not a correctness fix). v0 ships the events-only Redis-shaped surface; this is the next-tier architectural cleanup if/when the bench shape shifts toward many-cold-connections workloads.

**Depends on:** Phase 13 ship (need v0 stable baseline before measuring lift).

**Plan doc:** `.planning/ideas/valkey-io-architecture-rework.md` — 4-phase migration:

1. **Phase A — measure (1 day, GATE):** instrument the existing channel hop. Compute `t_in_channel_recv / t_total_push` over fraud-team zipfian + cold-connection workloads. **Abandon if < 5%.**
2. **Phase B — consolidate poll on apply thread (3-5 days):** apply thread owns the single `mio::Poll`; IO workers become pure SPSC consumers reading from per-worker `crossbeam_queue::ArrayQueue<ConnId>` filled by apply.
3. **Phase C — maxclients + slow-client backpressure (1-2 days):** Valkey-style; required to make consolidated poll safe under load.
4. **Phase D — validate at scale (2 days):** rerun maxcard + worker-sweep benches on r8g; confirm Phase A's measured overhead is recovered.

**Success criteria:**
1. `t_in_channel_recv / t_total_push` measured before-and-after with concrete numbers (must be ≥ 5% pre-rework or the phase is abandoned at A)
2. Functional parity with v0 wire stack (HTTP/1.1 + framed TCP, all 6 data-plane endpoints, all op codes)
3. EPS lift on the workload shape that motivated the rework (likely few-hot-connections / high pipeline depth)
4. No regression on simple-fraud / fraud-team / recommendations baselines
5. Architectural test pair (similar to `phase12_6_mio_only_dataplane.rs`) locks the consolidated-poll invariant in CI

**NOT a v0 ship-blocker.** If Phase A confirms < 5% overhead, abandon and remove the Valkey-model claim from the io_thread_worker.rs comments instead.

### Phase 18: Redis-shaped hand-rolled hot path — 🔄 IN PROGRESS

**Status:** Plan 18-01 COMPLETE (2026-04-25). `beava-runtime-core` crate scaffold, HTTP/1.1 + framed TCP parsers, WireRequest dispatch to AppState via `runtime_core_glue.rs`, `ServerV18::bind_v18` with tokio/axum admin sidecar, samply profiling procedure. Next: Plan 18-02 (inline WAL + pthread fsync).

**Goal:** Replace tokio on the apply + wire hot path with a hand-rolled event loop matching Redis 7.x architecture. Spec target: ≥3M EPS/core simple-fraud TCP on Linux Xeon. The hand-rolled hot path handles BOTH HTTP/1.1 + framed TCP for data-plane endpoints (`/push`, `/push-sync`, `/push-batch`, `/get`, `/upsert`, `/delete`, `/retract`); admin endpoints (`/metrics`, `/health`, `/ready`, `/registry`) stay on tokio/axum on a separate port (`8081`).

**Depends on:** Phase 13 ship-gate baseline (need to know the floor `tokio` produces before measuring lift). Phase 13.3 lockless-apply landing (apply thread already owns `RefCell<AppState>` directly — Phase 18 is a wire-stack rewrite, not a state-ownership rewrite). All Phase 8–11 + 12 + 12.5 operators must be on `v2/greenfield` so the wire rewrite measures the real workload.

**Stages and perf gates:**
1. **18.0** — research + translation (complete)
2. **18.1** — hand-rolled event loop + HTTP/1.1 + framed TCP (~2200 LoC, 6 tasks). Apple-M4 INFORMATIONAL gate. ✅ COMPLETE
3. **18.2** — inline WAL + pthread fsync (~300 LoC, 3 tasks). Apple-M4 INFORMATIONAL gate.
4. **18.3** — I/O threads for reads (Redis 6.0 pattern, ~500 LoC, 5 tasks). Apple-M4 INFORMATIONAL gate: 1–1.5M EPS/core aggregate with 4 I/O threads.
5. **18.4** — I/O threads for writes (~250 LoC, 3 tasks). Apple-M4 INFORMATIONAL gate: 2–2.5M EPS/core aggregate; tail p99 <5ms.
6. **18.4.5** — Linux Xeon bench infra setup (markdown/setup only, no code, no TDD).
7. **18.5** — `io_uring` on Linux (~600 LoC, 5 tasks). **HARD GATE on Linux Xeon: ≥3M EPS/core simple-fraud TCP.** Stretch ≥4M.
8. **18.6** — wire polish + VERIFICATION + SUMMARY (~400 LoC, 6 tasks). PERF GATE 6.1: full Phase 13 spec target on Linux for simple-fraud + complex-fraud + recommendation pipelines. PERF GATE 6.2: each micro-opt shows 5–10% individual uplift.

**Success criteria:** see `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` (D-01..D-16 locked decisions) and per-stage plan documents (18-01..18-06). The phase-wide risks register lives at `18-risks.md` (8 risks with mitigations).

**Why this matters:** Phase 18 closes the throughput gap between Phase 13.3's apply-loop ceiling (~16k EPS/core measured) and the 3M EPS/core ship-gate target. Each stage has a clear perf gate, so regressions surface incrementally instead of at the end.

### Phase 19: 1M-EPS bench harness — Python + Rust × multiple workload sizes — 📋 PLANNED

**Status:** Planned post Phase 18 wrap (added 2026-04-26). Follows up on the WIP `--total-events` + pre-encoded-frame work stashed during the Plan 18-06 perf push (`git stash list` → "wip: --total-events + pre-encoded-frame bench").

**Goal:** Ship a saturation bench that pushes a fixed number of events (default 1,000,000) at the server as fast as possible, isolated from per-event encoding cost on the bench side, and reports wall-clock time + server-side EPS. Run the bench from BOTH the existing Rust harness and a new Python SDK harness so the published "Beava processes 1M events in <Xs" number reflects the realistic Python-client path users will hit.

**Sub-goals:**

1. **Rust bench finalization** — finish the WIP `--total-events N` flag in `crates/beava-bench/src/bin/beava-bench-v18.rs`. Pre-encode ONE event frame at sender startup, blast that buffer N times across many TCP connections, drain acks, report `wall_clock`, `send_drain_time` (last byte left bench), and `ack_lag` (server queueing past send-drain). Debug the WIP stall (probably the watcher polling logic).
2. **Python bench** — equivalent harness using the existing Python SDK (`beava` package) over both HTTP/JSON and TCP/MessagePack transports. Pre-build once, blast N times. Report the same three metrics. Compare to Rust harness to surface SDK-side overhead.
3. **Multi-size workload matrix** — run each harness against `small`, `medium`, `large`, `large_phase9` (15-feature) configs from `crates/beava-bench/configs/`. Tabulate results per `(size, transport, format)` tuple in `.planning/throughput-baselines.md` under a new "1M-event blast" section. Threshold goals (M4): small ≤2 s, medium ≤4 s, large ≤8 s, large_phase9 ≤12 s.
4. **`--isolation-mode` flag** — split the timing into "bench-bound" (last byte sent) vs "server-bound" (last ack received). Helps users (and us) tell when their workload is rate-limited by bench/SDK encoding cost vs by Beava itself.
5. **Saturation bench architectural notes** — document the design decisions (pre-encoded blast vs varied-key, no inflight semaphore vs continuous pipelining, multiple TCP connections vs single conn) in `.planning/phases/19-1m-bench/19-CONTEXT.md` so future bench changes don't accidentally regress measurement honesty.

**Depends on:** Phase 18 wrap (SUMMARY + verification). The 1M-event ceiling is only meaningful once the hand-rolled hot path is the data-plane runtime — measuring against the legacy `IoPool` would give a misleadingly lower number.

**Success criteria:**
- `--total-events N` works end-to-end, no stall (debug WIP)
- Python harness produces matching `(size, transport, format)` rows in `throughput-baselines.md`
- Server-side EPS for `small + msgpack + TCP` clears 1M EPS on M4 OR documented gap reports concretely why (SDK overhead, syscall cost, …)
- Per-size thresholds met or recorded as known-deficits

**Why this matters:** "Can Beava handle 1M events per second" is the most common ship-readiness question users ask. We need a reproducible, defensible answer for both the curl/Rust path AND the Python path — not just the apply-thread microbench number from `criterion`. Without the Python harness the marketed number is misleading because most users will go through the SDK.

**Plans:** 6/5 plans complete

Plans:
- [x] 19-01-PLAN.md — blast_shape module: 4-shape Pool=N builder + Zipfian sampler + 10 unit/property tests (Wave 1)
- [x] 19-02-PLAN.md — bench-v18 integration: --total-events / --blast-shape / --isolation-mode + receiver-flips-stop in continuous AND burst paths; cherry-pick stash@{0} (Wave 2)
- [x] 19-03-PLAN.md — Python multi-process harness at python/benches/blast.py + wheel exclude (Wave 1)
- [x] 19-04-PLAN.md — criterion microbench for blast_shape + 6 baseline rows in perf-baselines.md (Wave 2)
- [x] 19-05-PLAN.md — throughput run + ledger ## 1M-event blast section + VERIFICATION + SUMMARY (Wave 3)

### Phase 19.1: Realistic-shape benchmark + bench/WAL fixes + complex-pipeline optimization — 📋 PLANNED

**Status:** Planned 2026-04-27 as the consolidated follow-up to Phase 19's PASS-WITH-DEFICIT verdict. Rolls together what was originally proposed as three separate Phase 19.0.x mini-phases (19.0.1 wall-clock + WAL, 19.0.2 lazy buckets, 19.0.3 batch sketch updates) into one phase, scoped around making `crates/beava-bench/configs/fraud-team.json` the primary tuning benchmark and using it to drive complex-pipeline apply-thread optimizations.

**Goal:** Re-baseline Phase 19's published EPS numbers with three corrections in place — (1) bench wall_clock measurement bug fixed, (2) WAL config bumped to a sensible middle-ground default, (3) realistic 14-node fraud-team config validated and added to the canonical bench matrix — and then drive at least one complex-pipeline apply-thread optimization landing measurably on the new fraud-team zipfian cell. Outcome: Phase 19 verdict flips from PASS-WITH-DEFICIT → PASS, and the published per-instance ceiling for realistic complex shapes is honest and known.

**Sub-goals:**

1. **Path B — fraud-team.json validation** — read `AggOpDescriptor` parsing in `crates/beava-core/src/agg_op.rs` for each `AggKind`; write a quick validator that audits `crates/beava-bench/configs/fraud-team.json` against the canonical param schemas; fix the 14 known/suspected items (histogram `bins` → `buckets`, geo lat/lon field names, unique_cells precision, burst_count param names, reservoir_sample n→size, first_n/last_n/lag, amount_to_count_ratio degeneracy, cb_streak field check, ssn_reuse 7d/30d naming, etc. — see `.planning/phases/19-1m-bench/.continue-here.md` Path B for the full list). Commit fraud-team.json + the supporting `.planning/research/fraud-feature-catalogue.md` (1054 lines, 110 features, 14 sources, anti-feature list).

2. **Bench wall_clock fix** — `crates/beava-bench/src/bin/beava-bench-v18.rs:660-672`: move `let elapsed = start.elapsed();` before the `for w in workers { ... }; let _ = get_task.await; let _ = rss_task.await;` block; convert `get_task` and `rss_task` from raw sleep loops to `tokio::select!` with stop signal. Re-run canonical cell; confirm `wall_clock_ms < 1000ms` and EPS > 500k for N=100k zipfian small. (See memory `project_phase19_bench_wallclock_fix` for the full recipe.)

3. **WAL config bump** — pick the middle-ground default (4×32 MiB tick_ms=20 is the proposed candidate; 8×64 MiB tick_ms=100 was the experimental upper bound that eliminated the bimodal tail with +33% EPS but at 512 MB RSS). Edit `crates/beava-server/src/server.rs:577,588` area; add tunables. Land with TDD per phase 3+ rule. Trace at N=500k zipfian to confirm bimodal `wal_append > 1ms` tail collapses. (See memory `project_phase19_wal_experiment` for experimental data.)

4. **Re-baselined Phase 19 numbers** — re-run the canonical small/medium/large/large_phase9 matrix AND a new fraud-team.json zipfian cell after corrections (1)–(3) land. Append a new section to `.planning/throughput-baselines.md`. Amend `.planning/phases/19-1m-bench/19-VERIFICATION.md` verdict from PASS-WITH-DEFICIT → PASS with a footnote explaining the deficit was a measurement artifact. Update Phase 19 SUMMARY.md headlines.

5. **Complex-pipeline apply-thread optimization (at least one of)** — measured against fraud-team.json zipfian as the primary cell:
   - **WindowedOp lazy buckets** (highest-leverage pick): replace `[Option<Box<AggOp>>; 64]` preallocation with `SmallVec<[(i64, Box<AggOp>); 4]>` so cold-key entity init doesn't pay the 4×512B zero-init cost. Predicted savings: ~1500 ns of the 2576 ns cold `entity_row_init` (~60%); +50% EPS for cold-key complex shapes.
   - **Same-key batch sketch updates** in apply dispatch — batch up consecutive events with the same entity-key for sketch ops (HLL/UDDSketch/SpaceSaving/Entropy) to amortize per-call overhead. Sketches consume 76% of features-time on `large-with-sketches` (Percentile_UDDSketch=257ns, Entropy=224ns, TopK=221ns, HLL=138ns); batching lets one update touch state once instead of N times for hot keys.
   - **OP_PUSH_MANY adoption in bench** — alternative path that lifts the wire-stack ceiling above 1M EPS instead of optimizing apply.

   Phase 19.1 lands at minimum the lazy-buckets optimization (concrete win, well-scoped); same-key batching and OP_PUSH_MANY are scoped as stretch.

**Depends on:** Phase 19 SUMMARY/VERIFICATION (already shipped at commit `98a3f8c`). Reads `crates/beava-core/src/agg_op.rs` AggOpDescriptor schema, `crates/beava-server/src/server.rs` WAL wiring, `crates/beava-bench/src/bin/beava-bench-v18.rs` (sub-goal 2), and `crates/beava-core/src/agg_apply.rs` WindowedOp (sub-goal 5).

**Success criteria:**
- `fraud-team.json` validates clean against `AggOpDescriptor` schemas (zero param-shape errors); committed alongside `fraud-feature-catalogue.md`.
- Bench `wall_clock_ms` reports honest elapsed time for N≥100k (no background-task contamination).
- WAL `wal_append > 1ms` tail collapses to 0 events under sustained 500k EPS at N=500k zipfian; default RSS ≤ 200MB.
- Phase 19 VERIFICATION verdict: PASS-WITH-DEFICIT → PASS.
- At least one complex-pipeline apply-thread optimization lands with measurable EPS lift on fraud-team.json zipfian (≥20% over re-baselined number).
- Per-instance ceiling for realistic complex shapes documented in `throughput-baselines.md` with `(pipeline, transport, format, blast_shape)` keying.

**Why this matters:** Phase 19's PASS-WITH-DEFICIT was based on bug-contaminated `wall_clock_ms`; the real number clears the M4 threshold by 2.5×. Fixing the bench restores honesty in the published number. Establishing fraud-team.json as the primary tuning benchmark grounds future perf work in a realistic shape (not synthetic configs that mask apply-bound work). Landing at least the WindowedOp lazy-buckets win demonstrates that the new bench actually drives optimization decisions — the loop closes.

**Key decisions to lock in `19.1-CONTEXT.md` during discuss:**
- Numbering: Phase 19.1 (single umbrella) vs three separate 19.0.1/19.0.2/19.0.3 phases — leaning umbrella for momentum.
- WAL default: 4×32 MiB tick=20ms (middle ground) vs 3×16 MiB + just-fix-wall_clock (cheap default, accept the bimodal tail).
- Histogram windowed semantics: add `windowed_histogram` op family vs document `percentile (UDDSketch)` as the windowed-distribution path.
- Stretch scope: cap at lazy buckets, OR also include same-key batching, OR also include OP_PUSH_MANY adoption.

**Plans:** 5/5 plans complete

Plans:
- [x] 19.1-01-PLAN.md — bench wall_clock measurement fix (Wave 1; verdict-flip pre-condition)
- [x] 19.1-02-PLAN.md — fraud-team.json validation + catalogue commit (Wave 1; primary tuning bench)
- [x] 19.1-03-PLAN.md — WAL config bump 4×32 MiB tick=20ms + env-tunables (Wave 2; depends on 01)
- [x] 19.1-04-PLAN.md — WindowedOp lazy buckets via SmallVec (Wave 2; depends on 01)
- [x] 19.1-05-PLAN.md — re-baseline matrix + Phase 19 verdict-flip + Phase 19.1 verification (Wave 3; depends on 01-04) (completed 2026-04-27)

### Phase 19.1.1: HTTP buffer-cap hotfix — split MAX_HEADER_BYTES into header-only vs body-via-Content-Length — 📋 PLANNED

**Status:** Inserted 2026-04-27 as a hotfix mini-phase between Phase 19.1 Wave 1 and Wave 2. Phase 19.1 Wave 1 (plans 19.1-01 + 19.1-02) discovered a pre-existing bug in `crates/beava-runtime-core/src/http_listener.rs:69-74` that blocks running fraud-team.json (~15 KiB register body) against the live bench server — 8 KiB `MAX_HEADER_BYTES` check fires on the entire buffer (headers + body), not just headers. Phase 19.1 Wave 2 (lazy buckets + WAL bump) and Wave 3 (rebaseline) need fraud-team.json to actually register, so 19.1.1 unblocks the critical path.

**Goal:** Fix `parse_http_request` so the 8 KiB cap applies only to header bytes (up to `\r\n\r\n` boundary) while bodies up to `MAX_BODY_BYTES` (4 MiB) parse cleanly via `Content-Length` header. Acceptance: a 15 KiB register POST against bench-v18 server completes successfully.

**Sub-goals:**

1. **Fix the buffer-cap split** in `crates/beava-runtime-core/src/http_listener.rs:69-74` — track header-end position, accept up to `MAX_HEADER_BYTES` of header bytes, then read up to `MAX_BODY_BYTES` body bytes via `Content-Length`. Existing line 143 Content-Length check stays; the early-return at line 69-74 stops gating bodies.

2. **TDD red test** asserting that a 15 KiB POST body succeeds. Test fails on current code (`ParseError::TooLarge`); passes after the fix. Lives at `crates/beava-runtime-core/tests/http_body_cap.rs`.

**Depends on:** None (orthogonal hotfix). Phase 19.1's existing 5 plans don't touch `http_listener.rs`.

**Success criteria:**
- `cargo test --workspace http_body_cap` passes
- 15 KiB register POST against bench-v18 succeeds (`./target/release/beava-bench-v18 --pipeline crates/beava-bench/configs/fraud-team.json --transport tcp --total-events 50` runs without `connection closed before message completed`)
- No regression on smaller POSTs (1 KiB, 4 KiB, 8 KiB still work)

**Why this matters:** fraud-team.json is locked as the primary tuning benchmark per memory `project_fraud_team_primary_bench`. Without this fix, Phase 19.1's primary deliverable (re-baseline against fraud-team) cannot run. Fix is small (1-file change) and orthogonal to Phase 19.1's WAL + bench + lazy-bucket work.

**Plans:**
1/1 plans complete

### Phase 19.1.2: geo_spread O(n) → O(1) Welford RMS dispersion — 📋 PLANNED

**Status:** Inserted 2026-04-27 as a second hotfix mini-phase between Phase 19.1 Wave 2 (19.1-03 + 19.1-04, completed) and Wave 3 (19.1-05 rebaseline). Phase 19.1's traced bench on fraud-team.json zipfian K=10k revealed `geo_spread` is O(n) per push (recomputes max-distance-from-running-mean by walking all stored samples on every event), reaching ~5-25 µs/push on hot keys with several thousand event history. The current implementation comment at `crates/beava-core/src/agg_geo.rs:152-154` deliberately deferred this to v0.1. Pulling forward because (a) it dominates fraud-team's hot-key apply path (~50% of features-stage time), (b) the current `max-distance-from-moving-mean` semantic is non-standard and counter-intuitive (a SCATTERED user with two clusters reports a LOWER value than a user with one outlier), and (c) the fix is small.

**Goal:** Replace `GeoSpreadState`'s `samples: Vec<(f64, f64)>` + per-update walk with Welford-style online second-moment accumulators. Returns RMS dispersion (km) instead of max-distance-from-mean (km). O(1) per push (~50 ns vs current 5-25,000 ns on hot keys). v0 spec change: the value `bv.geo_spread` returns has different units/semantics (RMS scatter vs single-point max). v0 is not publicly shipped (per memory `project_beava_product`), so no external consumers exist yet.

**Sub-goals:**

1. **Replace `GeoSpreadState` shape** — drop `samples: Vec<(f64, f64)>` and `max_km: f64`; add `m2_lat: f64`, `m2_lon: f64` (Welford squared-deviation accumulators). Keep `n`, `mean_lat`, `mean_lon`, `lat_field`, `lon_field`. Snapshot format bumps because struct shape changes.

2. **`update()` becomes O(1)** — Welford online algorithm:
   ```
   prev_mean_lat = self.mean_lat
   prev_mean_lon = self.mean_lon
   self.mean_lat += (lat - prev_mean_lat) * inv_n
   self.mean_lon += (lon - prev_mean_lon) * inv_n
   self.m2_lat += (lat - prev_mean_lat) * (lat - self.mean_lat)
   self.m2_lon += (lon - prev_mean_lon) * (lon - self.mean_lon)
   ```

3. **`query()` returns RMS-km** — convert variance from degree² to km² using local-mean-latitude cos-correction, then return `sqrt(rms_km_lat² + rms_km_lon²)`. Returns `Null` for `n < 2` (variance undefined).

4. **TDD red-green** — RED test asserts new query returns RMS dispersion (NOT max distance) for known input set; test fails on current code. GREEN: apply the Welford rewrite. Add at least one property test: variance is monotone-increasing as scatter grows; equal across permutations.

5. **Snapshot compat note** — document in SUMMARY.md that v0-internal snapshots taken pre-fix won't restore (struct shape changed); since v0 isn't shipped publicly, no migration path needed. WAL replay is unaffected (WAL stores raw events, agg state is rebuilt by replay).

**Depends on:** None. Orthogonal to Phase 19.1's existing 5 plans. Phase 19.1-04 (lazy buckets) and 19.1-03 (WAL config) don't touch agg_geo.rs.

**Success criteria:**
- `crates/beava-core/src/agg_geo.rs::GeoSpreadState` has `m2_lat: f64`, `m2_lon: f64` fields; no `samples: Vec<...>`, no `max_km` field
- `update()` body has no for-loop over samples
- New unit/property tests pass
- `cargo test -p beava-core` exits 0 (no regression)
- Smoke run of `beava-bench-v18 --pipeline fraud-team.json --total-events 100k --blast-shape zipfian --cardinality 10000` shows GeoSpread per-call cost in TRACE_AGG `per_kind=GeoSpread=...` < 200 ns (vs current 5,000-25,000 ns)
- Phase 19.1.2-01-SUMMARY.md documents the snapshot-format change

**Why this matters:** fraud-team.json's traced zipfian K=10k bench showed `geo_spread` consuming 30-50% of the warm-key features-stage time and contributing the lion's share of the hot-key slowdown observed in the K=10k vs K=1M comparison. Fixing this restores fraud-team's apply throughput on realistic warm-key shapes. Also makes `bv.geo_spread` semantically aligned with what fraud teams expect (spatial dispersion as stddev), instead of the confusing mean-drift-dependent max-distance metric.

**Plans:**
1/1 plans complete

### Phase 19.2: Big apply-path optimization — wrapping reduction + EntityKey cluster + sketch tuning + observability — 📋 PLANNED

**Status:** Planned 2026-04-27. Consolidates the prior Phase 19.2 (EntityKey work) + Phase 19.3 (wrapping reduction) + the two opus-research-agent audit findings (`operator-update-efficiency-audit.md` + `operator-update-uniformity-audit.md`) into one umbrella optimization phase. The goal: get fraud-team K=10k zipfian apply-path from 77k EPS (post-19.1) to **~125-150k EPS**, with ~67% of the 55-op catalogue hitting Tier 1 (≤30 ns/call) post-fix, and the remaining 33% (sketches, BTreeMap-walk ops) at their algorithmic floor with documented per-op cost class. Same-key batching is FORBIDDEN per memory `project_no_same_key_batching`.

**Goal:** Drop fraud-team K=10k zipfian per-event apply work from 13.4 µs → 6-8 µs across a coordinated set of seven sub-goals: wrapping reduction (field pre-extraction, hasher optimization), apply-loop refactor (EntityKey cluster + single-u64 fast path), op-specific tuning (UDDSketch BTreeMap → sorted Vec, EventTypeMix allowlist + str_from_row Cow), unbounded-state caps (UniqueCells/GeoEntropy max_cells), and cost-class observability. Stacks cleanly on Phase 19.1's WindowedOp lazy-bucket + GeoSpread Welford lifts.

**Sub-goals (in order of measured leverage from the audit + traces):**

1. **Field pre-extraction at apply entry** (HIGHEST single-lever — ~800-2,500 ns/event saved) — `crates/beava-core/src/agg_state.rs:867-876` and similar wrappers all do `row.get(field_name)` per agg call. fraud-team's 88-features-per-event × ~10-15 row fields = 88 redundant linear scans of the same `SmallVec<[(CompactString, Value); 8]>`. Hoist to apply-loop entry: pre-extract distinct field names ONCE into an indexed array; aggs reference fields by `field_idx` instead of `field_name`. Per-call cost: 100-300 ns linear scan → 5 ns array index. Affects ALL 55 ops.

2. **AHasher caching + FxHasher for HLL inputs** (~270-1,020 ns/event saved) — Two cooperating fixes:
   - Replace `ahash::AHasher::default()` per-call (which reads thread-local random seed) with a process-static AHasher initialized at registry-init time. Saves ~10-20 ns per hash op (HLL/Entropy/BloomMember).
   - For HLL specifically, switch from AHasher to FxHasher. HLL's `mix64` post-processes the input hash for distribution, so FxHasher's weaker statistical properties are repaired. FxHasher is ~3-5 ns vs AHasher's ~30-50 ns for short strings. ~9 HLL ops on fraud-team × ~30-80 ns saved each.

3. **EntityKey cluster + single-u64 fast path** (was old Phase 19.2; ~600-1,500 ns/event saved):
   - **EntityKey cache across aggs sharing `group_keys`** — `apply_event_to_aggregations` (`crates/beava-core/src/agg_apply.rs:97-103`) currently calls `EntityKey::from_row(&desc.group_keys, row)` once per `desc`. Cache by `group_keys` signature so M aggs sharing `["user_id"]` build the EntityKey once. Saves ~30 ns × (M-1) per event.
   - **Cluster aggs by `group_keys`** + single hashmap lookup per unique signature — Cluster `descs` by group_keys signature at register time so one `state_tables[agg_idx].get_or_init(...)` lookup serves all aggs sharing that key set. For fraud-team with 14 aggs over ~3 unique key signatures: ~308 ns lift.
   - **EntityKey single-u64 fast path (Approach C hybrid)** — `enum EntityKeyShape { SingleU64(u64), SingleStr(u64), Multi(SmallVec<...>) }` with two storage maps per AggStateTable. Zero collision for numeric, birthday-paradox for strings, zero for multi. ~150 ns lift per agg with single-key (~7 single-key aggs × 150 ns = 1,050 ns).

4. **UDDSketch `BTreeMap<i32, u64>` → flat sorted `Vec<(i32, u64)>`** (NEW from uniformity audit; ~5-15% fraud-team lift) — UDDSketch is fraud-team's 2nd most-expensive op (after HLL). The 2,048-bucket cap means an 11-level binary search on the Vec vs BTreeMap's node-pointer chase. Same α=0.01 accuracy contract, same retraction support. Per-call algorithm floor: 130 ns → ~75 ns (~30-50% faster). Source: `crates/beava-core/src/sketches/uddsketch.rs`. The wrapping fixes (sub-goals 1+2) and this data-structure swap together drop UDDSketch per-call cost from 963 ns → ~80 ns (~12× speedup).

5. **EventTypeMix allowlist + `str_from_row` Cow refactor** (NEW from efficiency audit; ~5-8% fraud-team lift) — Two-part fix:
   - `EventTypeMixState`: swap `Vec<String>` allowlist for `AHashSet<String>` at `EventTypeMixState::new`. `allowed.contains(&cat)` is O(allowed_len) today (`agg_buffer.rs:312-314`); becomes O(1).
   - `agg_state.rs:830-843`: refactor `str_from_row` and `value_to_key_string` to return `Cow<'_, str>` instead of allocating new `String` for every `Value::Str(Arc<str>)`. Skips ~50 ns/call across Bloom, Entropy, EventTypeMix.
   - Combined: EventTypeMix per-call drops from 1,127 ns → ~50-100 ns (10-20× speedup).

6. **Unbounded-state caps (UniqueCells / GeoEntropy)** (NEW from efficiency audit; memory bug) — Both ops grow `BTreeMap<(i32, i32), u64>` unbounded. Per-call cost stays O(log n_distinct) which is fine, but memory is uncapped — could blow CLAUDE.md's "~7 KB/entity for 30-feature pack" budget for high-mobility entities (millions of distinct geo-cells per entity). Add `max_cells` register-time cap mirroring `EventTypeMix.max_categories` pattern. When the cap is hit, fall back to "approximate count" mode (HLL-style) or evict least-frequent. Choose during discuss-phase.

7. **Cost-class observability + per-op cost documentation** (NEW from uniformity audit; product-shaping):
   - Add cost-class column to op docs (`bv.count` ≈ Tier 1 ≤30 ns, `bv.percentile` ≈ Tier 3 ~75-150 ns post-fix, etc.). Each AggKind tagged in source with a `#[doc(cost = "tier1|tier2|tier3")]` attribute or similar.
   - Add `/debug/op-cost` HTTP endpoint exposing the latest TRACE_AGG per_kind output. Lets users budget realistically without forcing API surface differences.
   - Preserves unified devex (per memory `project_v2_devex_first`) — NOT an API split into "fast" vs "premium" SDK surfaces. Just clearer expectations.

8. **Per-phase microbench + throughput rebaseline** (CLAUDE.md Phase 6+ rule + Phase 8+ rule):
   - criterion microbench at `crates/beava-core/benches/apply_path_bench.rs`: cold-key 14-agg apply with old vs full-fix path; warm-key apply same comparison; UDDSketch BTreeMap vs sorted-Vec; EventTypeMix allowlist Vec vs Set
   - Re-run Phase 19.1 targeted matrix (`small/medium/large/large_phase9/fraud-team` × zipfian × tcp × msgpack); append to `.planning/throughput-baselines.md` under `## 1M-event blast (rebaseline 19.2)` section
   - Update PHASE-19.2-VERIFICATION.md with verdict + measured EPS lift

**Combined predicted lift on fraud-team K=10k zipfian (current: 77k EPS, 13.4 µs/event):**

| Sub-goal | Mechanism | Predicted lift |
|---|---|---|
| 1. Field pre-extraction | row.get linear-scan elimination | -800 to -2,500 ns/event |
| 2. AHasher cache + FxHasher | per-call hasher init removal | -270 to -1,020 ns/event |
| 3. EntityKey cluster + u64 | per-agg-loop dedup + numeric fast path | -600 to -1,500 ns/event |
| 4. UDDSketch sorted Vec | algorithmic floor reduction | -200 to -350 ns/event (×4 UDDSketch ops) |
| 5. EventTypeMix Cow + Set | allowlist + alloc fix | -800 to -1,000 ns/event (1 op) |
| 6. UniqueCells/GeoEntropy cap | memory safety, no perf cost | RSS bound; not EPS lift |
| 7. Cost-class docs/observability | informational | not measurable |
| **Stacked total** | | **~3,000-6,500 ns/event saved** |
| **Predicted apply ceiling** | 13.4 µs → 7-10 µs | **~100-150k EPS** (was 77k) |

Tier classification post-fix per uniformity audit:
- **Tier 1 (38 ops, 67%)**: counters/sums/Welford/Phase 8/9 — ~25-40 ns/call
- **Tier 2 (8 ops, 14%)**: HLL, BloomMember, simple-mode sketches, OutlierCount — ~30-100 ns/call
- **Tier 3 (9 ops, 16%)**: UDDSketch (post-fix), TopK Hybrid, Entropy, EventTypeMix (post-fix), BTreeMap-key-walk family — at algorithmic floor with documented cost

**Depends on:** Phase 19.1 (DONE — bench fix + WAL bump + lazy buckets + GeoSpread Welford + HTTP fix all merged; verdict PASS). Reads `crates/beava-core/src/agg_apply.rs` + `agg_state.rs` + `agg_state_table.rs` + `registry.rs` + `agg_buffer.rs` + `sketches/uddsketch.rs` + `sketches/count_distinct.rs`. Cross-references both audit docs at `.planning/research/operator-update-{efficiency,uniformity}-audit.md`.

**Success criteria:**
- Apply-time `row.get()` calls per event drops from ~88 → ≤ 10 on fraud-team-shape pipelines
- HLL per-call cost drops from ~952 ns traced → ≤ 50 ns traced (untraced equivalent ~25 ns)
- UDDSketch per-call cost drops from 963 ns traced → ≤ 100 ns traced (untraced equivalent ~75 ns)
- EventTypeMix per-call cost drops from 1,127 ns traced → ≤ 100 ns traced
- AHasher initialization happens once per process (verifiable via cargo-bench 0-allocation profile)
- `EntityKey::from_row` called once per unique `group_keys` signature, not once per agg
- fraud-team.json K=10k zipfian N=1M shows ≥ 100k EPS in `## 1M-event blast (rebaseline 19.2)` ledger section (vs 77k post-19.1)
- Tier 1 ops measured at ≤ 40 ns/call traced (Count, Sum, Avg, Min, Max — verifiable via TRACE_AGG per_kind)
- UniqueCells/GeoEntropy register-time validation enforces `max_cells` cap; OOM-safety regression test exists
- Cost-class column appears in op docs; `/debug/op-cost` endpoint returns last-observed TRACE_AGG per-kind data
- All sub-goals' threat models are minimal (apply-path internals; no new attack surface; same trusted input fields)

**Why this matters:** Phase 19.1 closed the verdict-flip gap (637k EPS canonical small zipfian @ N=1M). Phase 19.2 closes the COMPLEX-pipeline gap — where fraud-team-shape realistic workloads actually run. Pushing fraud-team K=10k zipfian to ~100-150k EPS is the marketing-defensible benchmark for "single-instance fraud-feature-server with sub-ms decisions." Tier-classification observability gives users honest cost expectations per op without splitting the catalogue surface. The two op-specific fixes (UDDSketch + EventTypeMix) close real algorithmic + alloc bugs the audit found.

**Key decisions to lock in `19.2-CONTEXT.md` during discuss:**
- Field pre-extraction storage shape: indexed array (`Vec<&Value>` with field-idx-into-row) vs hashmap (`HashMap<&str, &Value>`). Leaning indexed array.
- HLL hasher choice: FxHasher (fastest, non-keyed) vs AHasher-cached (more HashDoS-resistant). Internal fraud workloads are operator-controlled so HashDoS isn't a real concern; lean FxHasher.
- EntityKey single-u64: Approach A (numeric only, zero collision) vs C (hybrid, RECOMMENDED).
- Cluster storage shape: shared `Vec<AggOp>` across clustered aggs (split by agg_id via secondary indirection at update time) vs separate Vec<AggOp> per agg with shared row lookup. Shared-Vec saves more memory; split-by-agg_id is simpler.
- UniqueCells/GeoEntropy cap behavior at threshold: drop new cells (truncate) vs fall back to HLL approximation vs LRU eviction.
- Cost-class doc surface: source attribute (`#[doc(cost = "tier1")]`) vs separate markdown table in operator catalogue docs site.
- Observability endpoint: `/debug/op-cost` always-on vs feature-gated behind `BEAVA_DEV_ENDPOINTS=1`.

**Plans:** 8/8 plans complete
- [x] 19.2-01-PLAN.md — D-01 field pre-extraction (apply-loop one-pass row scan + register-time field-idx + missing-field reject) — Wave 1
- [x] 19.2-02-PLAN.md — D-02a process-static AHasher RandomState + D-02b FxHasher for HLL input — Wave 2
- [x] 19.2-03-PLAN.md — D-03 EntityKeyShape hybrid (SingleU64/SingleStr/Multi) + D-04 cluster signature dispatch + register-time NaN-float reject — Wave 2
- [x] 19.2-04-PLAN.md — D-04a UDDSketch BTreeMap → flat sorted Vec with binary-search insert — Wave 1
- [x] 19.2-05-PLAN.md — D-04b EventTypeMix AHashSet allowlist + str_from_row/value_to_key_string Cow refactor (Bloom/Entropy/EventTypeMix consumers) — Wave 3
- [x] 19.2-06-PLAN.md — D-05 remove unique_cells/geo_entropy from catalogue (55 → 53) + add quadkey() builtin + D-05a bv.entropy max_categories cap + Prometheus counter — Wave 4
- [x] 19.2-07-PLAN.md — D-06 cost-class catalogue at docs/operators/cost-class.md + D-07 /debug/op-cost endpoint feature-gated by BEAVA_DEV_ENDPOINTS=1 — Wave 5
- [x] 19.2-08-PLAN.md — D-08 criterion microbench (apply_path_bench.rs, 4 groups) + Phase 19.2 throughput rebaseline matrix + verification verdict — Wave 6 (completed 2026-04-27)

Wave structure:
- Wave 1: 19.2-01 (foundation), 19.2-04 (independent UDDSketch)
- Wave 2: 19.2-02 (hashers, depends on 01 — agg_state.rs file overlap), 19.2-03 (cluster dispatch, depends on 01)
- Wave 3: 19.2-05 (EventTypeMix Set+Cow, depends on 01+02 — agg_state.rs file overlap)
- Wave 4: 19.2-06 (op removal + entropy cap, depends on 01+03+05 — many file overlaps)
- Wave 5: 19.2-07 (cost-class + /debug/op-cost, depends on 06)
- Wave 6: 19.2-08 (microbench + rebaseline + verification, depends on all)

**Anti-pattern preserved (per Phase 19.3 design notes):**
- Same-key sketch batching is FORBIDDEN per memory `project_no_same_key_batching` — read-after-write semantic risk + Redis-shaped positioning. Do NOT propose batching as a sub-goal during discuss-phase.
- Cross-event aggregation reordering is FORBIDDEN — preserves arrival-order semantics for ewma/streak/lag.
- Multi-thread apply parallelism is FORBIDDEN per memory `project_no_sharded_apply` — single-threaded data plane forever; horizontal scaling via multi-instance Redis-cluster pattern only.

### Phase 19.3: Extend pre-extraction across WindowedOp wrapper — close fraud-team apply-stage WindowedOp dispatch tax — 📋 PLANNED

**Status:** Planned 2026-04-28 as the direct follow-up to Phase 19.2's PASS-WITH-DEFICIT. Live-trace investigation (`.planning/phases/19.2-big-apply-path-optimization/19.2-INVESTIGATION.md`) identified that 60 of 88 fraud-team feature updates pay a ~100 ns wrapping tax = ~9000 ns/event of the 14 µs agg-stage budget. WindowedOp dispatch bypasses Plan 19.2-01's field pre-extraction protocol — every windowed op re-does `row.get(fname)` linear scan + double-dispatch inside each bucket update. Phase 19.3 extends D-01's `update_at(extracted, field_idx, …)` protocol across the WindowedOp wrapper layer.

**Goal:** Drop fraud-team K=10k zipfian per-event agg-stage from 14,059 ns → ~8,450 ns across three stacked sub-goals, lifting end-to-end EPS from ~70k → ~125k on the primary tuning bench. Closes the gap to Phase 19.2 CONTEXT's original 100k+ EPS aspiration with the same conceptual model Phase 19.2 already validated for non-windowed ops.

**Sub-goals (in order of measured leverage from `19.2-INVESTIGATION.md` §4):**

1. **19.3-A — `WindowedOp::update_at(extracted, field_idx, lat_idx, lon_idx, event_time_ms, where_matched)` fast-path** (HIGHEST single-lever — ~3,900 ns/event saved) — Mirrors the non-windowed pre-extracted path. Per-bucket inner op dispatches via `AggOp::update_with_extracted` (already exists) instead of `AggOp::update_with_row`. Eliminates the inner `row.get(fname)` linear scan AND the inner `evaluate_where_predicate` re-evaluation. Files: `crates/beava-core/src/agg_windowed.rs:191-211` (new fast-path method), `crates/beava-core/src/agg_op.rs:867` (Windowed arm dispatches new method), `crates/beava-core/src/agg_apply.rs:225-235` (pass through). Predicted lift: 14,059 ns → ~10,150 ns agg → ~95k EPS.

2. **19.3-B — Specialize windowed Count/Sum dispatch** (~1,100 ns/event saved) — Count and Sum are the most-called windowed kinds (11 + 3 = 14 calls/event in fraud-team). Inner state update is trivial (`n += 1` / `total += v`). Bypass the full `AggOp::update_with_row → AggOp::update → CountState::update(row, …)` chain by inlining: `WindowedOp::update_with_row` matches on `inner_kind` for Count/Sum and writes to inner state's `n` / `(total, n)` directly. Saves ~80 ns/call dispatch tax for the highest-frequency kinds. Stacks cleanly on 19.3-A. Files: `crates/beava-core/src/agg_windowed.rs:160-211`. Predicted lift: ~9,050 ns agg → ~107k EPS.

3. **19.3-C — Hoist event-level `ExtractedFields` above the descriptor loop** (~600 ns/event saved) — Currently `extracted` is rebuilt per-desc at `agg_apply.rs:201-205`. Across 5 descs × ~6 distinct fields × ~25 ns/find = ~750 ns wasted on overlapping field reads. Hoist a single per-event ExtractedFields keyed by `(source_event_schema, field)` — registry knows the union of all fields any agg on this source needs, so the apply loop builds one ExtractedFields per event and indexes it via per-agg `field_idx` arrays. Files: `crates/beava-core/src/registry.rs` (precompute per-source `apply_field_names` union — already exists, just unused), `crates/beava-core/src/agg_apply.rs:201-205` (build one extracted per event). Predicted lift: ~8,450 ns agg → ~115-125k EPS.

4. **Per-phase microbench amendment + throughput rebaseline** (CLAUDE.md Phase 6+ rule + Phase 8+ rule):
   - Add `apply_path/warm_key/14_aggs_windowed` group to `crates/beava-core/benches/apply_path_bench.rs` whose registry has the same 14 features wrapped in `WindowedOp(window_ms = 24h)`. Should currently show ~5 µs (the live trace's per-feature × 14 ratio); after 19.3-A drops to ~1 µs. Without this bench, Phase 19.3 repeats Phase 19.2's measurement gap (microbench improves while live workload stays flat).
   - Re-run Phase 19.2's targeted matrix (`small/medium/large/large_phase9/fraud-team` × zipfian × tcp × msgpack); append to `.planning/throughput-baselines.md` under `## 1M-event blast (rebaseline 19.3)` section.
   - Phase 19.3 verification MUST include a live `BEAVA_TRACE_APPLY_TIMING` + `BEAVA_TRACE_AGG_TIMING` run (not just criterion bench) — verifier conjecture without measurement was the root cause of Phase 19.2's misdirected diagnosis. See memory `feedback_dispatch_refactor_enumerate_wrappers`.

**Stacked predicted lift on fraud-team K=10k zipfian (Phase 19.2 baseline: 70,639 EPS, 14,059 ns agg):**

| Sub-goal | Mechanism | Saved ns/event | Cumulative agg-stage | Cumulative EPS |
|---|---|---:|---:|---:|
| 19.2 baseline | — | — | 14,059 | 70,639 |
| + 19.3-A | windowed update_at fast-path | -3,900 | ~10,150 | ~95,000 |
| + 19.3-B | windowed Count/Sum specialize | -1,100 | ~9,050 | ~107,000 |
| + 19.3-C | event-level ExtractedFields | -600 | ~8,450 | ~115-125k |

**Depends on:** Phase 19.2 (DONE — D-01 field pre-extraction, D-04 cluster dispatch, D-04a UDDSketch sorted Vec, D-04b EventTypeMix Cow all merged; verdict PASS-WITH-DEFICIT). Reads `crates/beava-core/src/agg_apply.rs` + `agg_op.rs` + `agg_windowed.rs` + `agg_state.rs` + `registry.rs`. Cross-references `19.2-INVESTIGATION.md` (per-AggKind breakdown, 100k traced events) + memory `feedback_dispatch_refactor_enumerate_wrappers` (anti-pattern this phase remediates).

**Success criteria:**
- `WindowedOp::update_at(extracted, field_idx, …)` exists and is dispatched from `AggOp::update_with_extracted::Windowed(…)` arm
- `grep -c 'row.get(' crates/beava-core/src/agg_state.rs` ≤ 5 on apply-time hot paths (was ~30 callsites pre-19.3); any remaining call has explicit grandfathering rationale in code comment
- `apply_path/warm_key/14_aggs_windowed` criterion bench exists and shows ≥ 4× speedup vs pre-19.3 baseline
- Live `BEAVA_TRACE_AGG_TIMING` run on fraud-team K=10k zipfian shows agg-stage mean ≤ 9,000 ns (vs 14,059 ns post-19.2)
- Per-AggKind windowed Count/Sum cost drops from ~180 ns/call → ≤ 30 ns/call
- fraud-team.json K=10k zipfian N=1M shows ≥ 100k EPS in `## 1M-event blast (rebaseline 19.3)` ledger section (vs 70,639 post-19.2) — flips Phase 19.2 PASS-WITH-DEFICIT remediation pointer to closed
- No regression > 10% on any non-fraud-team ladder cell (small/medium/large/large_phase9)
- Phase 19.3 verification MUST include both criterion microbench AND live trace measurements (not conjecture)
- All sub-goals' threat models are minimal (apply-path internals; no new attack surface; no API surface change)

**Why this matters:** Phase 19.2 proved field pre-extraction works for non-windowed ops; Phase 19.3 extends the same architectural pattern across the WindowedOp wrapper that 60% of fraud-team's per-event work flows through. This is the single largest remaining apply-stage lever before WAL group-commit batching (which would be the next phase if 19.3 leaves a gap). Phase 19.3 is also the structural fix for the dispatch-protocol-bypass anti-pattern memorialized in `feedback_dispatch_refactor_enumerate_wrappers` — explicitly enumerating ALL dispatch entry points (top-level + WindowedOp wrapper) up-front.

**Key decisions to lock in `19.3-CONTEXT.md` during discuss:**
- `WindowedOp::update_at` signature shape: forward `extracted: &ExtractedFields` + per-bucket inner `update_at` (cleanest) vs forward extracted-by-ref + bucket-local `field_idx` recompute (more memory-friendly).
- Specialized arms scope: just Count/Sum (19.3-B as scoped) vs also include EventTypeMix-windowed (currently non-windowed, but pattern extension may unlock future) vs none (skip 19.3-B, rely on 19.3-A's general path).
- `ExtractedFields` hoist storage: per-event arena allocation vs reuse a single `Vec<Value>` across events vs per-source-schema cached buffer. Affects allocator pressure on cold-key paths.
- Whether to land 19.3-A alone first, gate 19.3-B/C on measured 19.3-A lift (sequential proof) vs land all three in one wave (parallel speed at risk of correlated bugs).
- Whether the new `apply_path/warm_key/14_aggs_windowed` criterion group also gets a cold-key sibling for completeness vs warm-key only (matches the Phase 19.2 bench shape).

**Anti-patterns preserved (carried from Phase 19.2):**
- Same-key sketch batching is FORBIDDEN per memory `project_no_same_key_batching`.
- Cross-event aggregation reordering is FORBIDDEN — preserves arrival-order semantics.
- Multi-thread apply parallelism is FORBIDDEN per memory `project_no_sharded_apply`.

### Phase 19.4: Final 100k EPS push — flamegraph-derived levers — 🚧 IN PROGRESS (3/5 plans complete: 19.4-01 PASS, 19.4-02 PASS, 19.4-03 PASS)

**Status:** Phase opened 2026-04-28; 3 of 5 plans complete as of 2026-04-28 evening. Plan 19.4-01 PASSED (CountDistinct identity hasher → 79,367 EPS / 11,667 ns agg-stage). Plan 19.4-02 PASSED via re-measurement attempt #3 on quiet system (SmallVec cap 8→16 → 96,298 EPS / 10,329 ns agg-stage). Plan 19.4-03 PASSED on first attempt (Geo lat_idx/lon_idx register-time resolution → 94,733 EPS / 8,244 ns agg-stage; samply confirms `agg_geo::read_lat_lon` slow path eliminated, 0.000% self-time was 2.86%). Cumulative trajectory: post-19.3 12,533 ns → **post-19.4-03 8,244 ns** (-3,423 ns / -29% on apply CPU across 3 plans).

Phase 19.4 was planned 2026-04-28 as the closure phase for the v0 Phase-19 100k EPS ship gate. Phase 19.3 closed at PASS-WITH-DEFICIT — D-04 architectural fix landed (WindowedOp::update_at) and is shippable, but predicted lift was 60% overestimated by cost-model conjecture. samply flamegraph + per-AggKind drill-down on the post-19.3-A binary identified 5 NEW optimization levers the original investigation missed; this phase picks up the top-3 cheapest + carries forward the deferred 19.3-D ExtractedFields hoist. All cost-model predictions cite `19.3-FLAMEGRAPH.md` directly (per memory `feedback_cost_model_from_flamegraph`).

**Goal:** Lift fraud-team K=10k zipfian from 73,743 EPS (post-19.3-A) to **≥100k EPS** (PASS gate; 75% floor 75k EPS PASS-WITH-DEFICIT). Apply-thread agg-stage drops from 12,533 ns → ≤9,500 ns via 4 surgical optimizations + dual-measurement verification. After Phase 19.4 closes, optimization shifts from per-instance throughput to scale-out (Phase 19.5: sharding deployment patterns + multi-instance benchmarks).

**Sub-goals (in flamegraph-priority order):**

1. **19.4-A — CountDistinct identity-hasher fix** (HIGHEST single-lever — predicted ~1,180 ns/event, ~+13k EPS) — `crates/beava-core/src/sketches/count_distinct.rs:24-27` defines `CountDistinctState::HashSet { hashes: std::collections::HashSet<u64> }`. `std::HashSet` runs SipHash on every probe — but the values are ALREADY 64-bit cryptographic hashes from the upstream HLL preprocessing. Replace with `hashbrown::HashSet<u64, BuildHasherDefault<NoOpHasher>>` (or flat sorted `Vec<u64>` until promote-to-HLL threshold). Snapshot wire-format unchanged (the hash values themselves serialize the same); replay code may need a small adjustment to instantiate the new collection type. Per `19.3-FLAMEGRAPH.md` §2: this is 9.36% of apply-thread CPU (1,234 ns/event). Effort: ~3 hours. Risk: snapshot replay format compat.

2. **19.4-B — ExtractedFields SmallVec inline-cap 8→16** (predicted ~530 ns/event, ~+5k EPS) — `crates/beava-core/src/agg_op.rs:232` defines `pub type ExtractedFields = SmallVec<[Option<&'static Value>; 8]>`. fraud-team's TxnByUser cluster has **10 distinct fields**, so SmallVec spills to heap on every Txn event — `RawVecInner::reserve` + `RawVec::with_capacity_in` together appear at 4.0% inclusive in the flamegraph. Widening inline cap to 16 covers all known v0 cluster shapes. **One-line change.** Effort: 1 hour incl. test. Risk: none.

3. **19.4-C — Geo lat/lon pre-extraction** (predicted ~360 ns/event, ~+3-5k EPS) — `crates/beava-core/src/agg_geo.rs:24-35` (`read_lat_lon`) does linear `row.get(lat_field)` + `row.get(lon_field)` on every geo feature update. Phase 19.2-01's D-01 protocol missed the geo path (per `19.3-FLAMEGRAPH.md` §2 row #8: 2.86% self-time). Extend `extracted: &ExtractedFields` indexing to geo ops; resolve `lat_idx`/`lon_idx` at register time (already partially scoped in RESEARCH.md but unimplemented). Effort: ~4 hours. Risk: low — matches Phase 19.2 D-01 pattern.

4. **19.4-D — ExtractedFields hoist above descriptor loop** (predicted ~900-1,500 ns/event, ~+10-15k EPS — flamegraph realistic, was Phase 19.3's superseded Plan 19.3-04) — Currently `extracted` rebuilds per-descriptor at `agg_apply.rs:201-205` (5 descs × ~500 ns = ~2,500 ns/event scaffolding tax per `19.3-COST-MODEL.md` §2). Hoist to per-event: build one `ExtractedFields` keyed by `(source_event_schema, field_idx_union)`; aggs index via per-agg `field_idx` arrays. Sub-tasks (per `19.3-RESEARCH.md` amendment R5/R6, carried forward):
   - **D.1:** Populate `EventDescriptor.apply_field_names` at registration time (currently `vec![]` at 15+ construction sites in `registry.rs`).
   - **D.2:** Re-resolve `field_idx` against the per-event union after the apply-loop hoist.
   - **D.3:** Hoist `ExtractedFields` build above the descriptor loop in `apply_event_to_aggregations` — single `Vec<Option<&Value>>` local variable alongside `shape_cache` at `agg_apply.rs:152` (per RESEARCH.md Q1).
   - **D.4:** SmallVec inline-cap widening already done in 19.4-B (verify still correct under hoist).
   Effort: ~1 week. Risk: cross-cutting through registry + apply.

5. **19.4-E — Throughput rebaseline + dual-measurement verification + Phase 19 closure** — Append `## 1M-event blast (rebaseline 19.4)` to `.planning/throughput-baselines.md`. Run BOTH criterion bench AND live `BEAVA_TRACE_APPLY_TIMING=1 BEAVA_TRACE_AGG_TIMING=1` trace per `19.3-FLAMEGRAPH.md` reproduction commands. Side-by-side per-AggKind table comparing post-19.3-A vs post-19.4. Update Phase 19 SUMMARY/VERIFICATION via amendment if 100k EPS is achieved. PASS gate: ≥100k EPS on fraud-team K=10k zipfian (75% floor: ≥75k EPS PASS-WITH-DEFICIT).

**Sequential measurement gates carried forward from Phase 19.3:** Each plan has an explicit measurement gate. If measured lift < 75% of predicted, HALT — write DEVIATION.md and re-evaluate before proceeding to next sub-goal. Cost model is now flamegraph-derived (not arithmetic on microbench numbers); 75% floor is honest. If 19.4-A hits ≥75% of predicted, proceed to 19.4-B; if not, halt and re-investigate.

**Stacked predicted lift on fraud-team K=10k zipfian (post-19.3-A baseline: 73,743 EPS, 12,533 ns agg-stage):**

| Step | Saved ns/event (predicted) | Cumulative agg-stage | Cumulative EPS | Measured (Apple-M4) |
|---|---:|---:|---:|---|
| Post-19.3-A baseline | — | 12,533 | 73,743 | — |
| + 19.4-A (CountDistinct identity hasher) | -1,180 | ~11,353 | ~85,000 | **PASS:** 11,667 ns / 79,367 EPS (Plan 01, 73% realization on agg-stage) |
| + 19.4-B (SmallVec cap 8→16) | -530 | ~10,823 | ~91,000 | **PASS (attempt-3):** 10,329 ns / 96,298 EPS (Plan 02 quiet-system re-measurement) |
| + 19.4-C (geo lat_idx pre-extract) | -360 | ~10,463 | ~94,000 | **PASS:** 8,244 ns / 94,733 EPS (Plan 03 first-attempt; 580% realization — structural bypass elimination) |
| + 19.4-D (ExtractedFields hoist) | -1,200 (realistic per cost-model) | ~9,263 | **~105,000** | (next) |

**Predicted realistic ceiling: ~100-110k EPS.** Hits the original ship gate. Stop here for vertical optimization; pivot to sharding deployment story for further scale.

**Plan 19.4-03 over-delivered:** measured agg-stage drop was -2,085 ns vs predicted -360 ns (5.8× the prediction). The cost-model methodology under-predicted the lift because it treated `agg_geo::read_lat_lon`'s 2.86% self-time as the sole overhead; in practice, eliminating the slow-arm dispatch also removed Row::new() + 9-arg dispatch overhead the slow path inherited from `update()`. The Plan-03 SUMMARY (`19.4-03-SUMMARY.md`) documents this as "structural bypass elimination" pattern — distinct from "sub-step swap" patterns (Plans 01/02) which realize ~75% of predicted lift.

**Depends on:** Phase 19.3 (closed at PASS-WITH-DEFICIT — `WindowedOp::update_at` exists and is shippable; 19.3-CONTEXT.md decisions D-01..D-04 still valid). Reads `19.3-FLAMEGRAPH.md`, `19.3-COST-MODEL.md`, and source files: `crates/beava-core/src/{agg_apply.rs, agg_op.rs, agg_geo.rs, agg_state.rs, registry.rs, sketches/count_distinct.rs}` + `crates/beava-core/benches/apply_path_bench.rs`.

**Success criteria:**
- CountDistinct HashSet mode uses identity hasher; criterion bench shows CountDistinct windowed cost ≤200 ns/call (was 457 ns post-19.3-A)
- ExtractedFields inline cap = 16; instrumentation shows zero per-event SmallVec spill on fraud-team apply
- Geo features dispatch via `extracted` indexing; `grep -c 'row.get(' crates/beava-core/src/agg_geo.rs` ≤ 2 (was 3)
- ExtractedFields rebuilt 1× per event (not 5×); instrumentation `EXTRACTED_BUILD_COUNT == event_count`
- Live `BEAVA_TRACE_APPLY_TIMING` agg-stage mean ≤ 9,500 ns (was 12,533 ns post-19.3-A)
- fraud-team K=10k zipfian N=1M shows **≥ 100,000 EPS** (PASS gate) — flips Phase 19 verdict from PASS-WITH-DEFICIT → PASS
- No regression > 10% on small/medium/large/large_phase9 ladder cells
- Verification includes BOTH criterion microbench AND live BEAVA_TRACE_AGG_TIMING trace
- All sub-goals' threat models are minimal (apply-path internals; no API surface change)

**Why this matters:** Closes the v0 ship-gate set in Phase 19's CONTEXT (≥100k EPS on fraud-team K=10k zipfian). Beyond 19.4, optimization shifts from per-instance throughput (vertical) to scale-out (Phase 19.5+: sharding deployment patterns + multi-instance benchmarks). For workloads needing >130k EPS single-instance, customers run multiple Beava processes per memory `project_no_sharded_apply`. The 100k milestone is the marketing/positioning number — credible "Beava: 100k+ EPS per core, single-binary, sub-ms latency" alongside "linear scaling via Redis-cluster pattern."

**Key decisions to lock in `19.4-CONTEXT.md` during discuss:**
- 19.4-A storage: `hashbrown::HashSet<u64, BuildHasherDefault<NoOpHasher>>` vs flat sorted `Vec<u64>` for the < 1024-entries Exact mode. Vec wins on memory + binary search cost; HashSet wins on insert simplicity. Snapshot replay implications differ.
- 19.4-D buffer location: per-thread local var in `apply_event_to_aggregations` (RESEARCH Q1 recommendation, alongside `shape_cache` at `agg_apply.rs:152`) vs new struct field on apply context. RESEARCH.md already recommends local-var; confirm during discuss.
- 19.4-D field-union scope: union across ALL descriptors at register-time (one ExtractedFields per event, 88 fields max) vs per-source-schema union (separate ExtractedFields per source). Larger union = fewer branches but more wasted slots; cost-model needs to pick.
- Sequential vs parallel landing (carried from D-02): A → measure → B → measure → C → measure → D → measure → E. Or compress B (one-line) and C (4-hour) into a single wave after A's measurement gate.
- Whether to add a flamegraph re-run after D as a sanity check (similar to 19.3's investigation flow) or trust the cumulative trace measurements.

**Anti-patterns preserved (mandatory plan-checker rules):**
- **Cost-model predictions must cite `19.3-FLAMEGRAPH.md` file:line** (memory `feedback_cost_model_from_flamegraph`). No `per_call_ns × call_count` arithmetic without flamegraph reference.
- **Verifier MUST run live trace** (memory `feedback_verify_plan_decisions` + Phase 19.2 lesson). Criterion bench alone is insufficient.
- **Wrapper-bypass enumeration** (memory `feedback_dispatch_refactor_enumerate_wrappers`) — geo/19.4-C extends D-01 protocol; verify ALL geo callsites covered.
- **Same-key sketch batching FORBIDDEN** (memory `project_no_same_key_batching`).
- **Cross-event aggregation reordering FORBIDDEN.**
- **Multi-thread apply parallelism FORBIDDEN** (memory `project_no_sharded_apply`).
- **No `todo!()` / "deferred" / "if absent"** language in plans or commits.

**Out of scope / Deferred ideas (do not propose during discuss):**
- TopK Exact-mode BTreeMap → Vec (~500 ns/event, 6h, snapshot break) — Phase 19.5 candidate.
- EventTypeMix double-HashMap-lookup fix (~150 ns/event, 1h) — Phase 19.5 candidate.
- HLL Dense register-pack SIMD (AVX2/NEON) (~300-500 ns/event, 1 week + portable_simd) — Phase 19.5 candidate, only if customer demand pushes past 130k EPS single-instance.
- Codegen op-fusion at register-time (devex risk) — Phase 21+ if ever.
- Tier-1 windowed specialize (Count/Sum/Min/Max/Avg/Variance inline arms) — explicitly DROPPED per `19.3-FLAMEGRAPH.md`: Tier-1 ops are only ~10% of agg-stage; ROI is poor vs the four levers above.
- WAL group-commit batching — was Phase 19.2's wrong-but-still-real conjecture; per `19.3-FLAMEGRAPH.md` WAL is ~85 ns (0.6% of apply CPU), not the bottleneck. Phase 19.5+ candidate if 19.4 leaves a gap.

### Phase 20: Operator catalogue + streaming-semantics + push/get API audit — 📋 PLANNED

**Status:** Planned post Phase 19 (added 2026-04-26; push/get API audit scope folded in 2026-04-26).

**Goal:** Systematic review of every shipped aggregation operator (Phases 5/8/9/10/11/11.5: 55+ ops), every streaming-semantics decision (event-time, watermarks, retraction, MVCC, modifiability tiers, dedupe, idempotency), AND every push/get API surface (push variants, get variants, set/mset/mget, upsert, delete, retract, push-and-get) — for correctness, test coverage, and documented behavior. Treat this as the "before-public-launch QA" pass — every public surface must have a written contract that matches its implementation, and every edge case must have a test.

**Sub-goals:**

1. **Operator-by-operator audit** — for each of the 55+ ops shipped in Phases 5/8/9/10/11/11.5, write/refresh a one-page contract covering: numeric domain, NaN/null handling, window semantics (if windowed), retraction semantics (subtractive-OK / approx-modifiable / reject-modifiable / Tier-A/B/C), determinism guarantees, snapshot serialization shape, restart behavior. Cross-check the contract against the implementation; raise issues for divergence.

2. **Streaming-semantics audit** — re-derive the v0 contract for: event-time vs ingest-time, watermark behavior, out-of-order delivery, dedupe windows, idempotency cache, MVCC retention, retraction primitives. Check each against `register_validate.rs` warnings/errors and the existing tests.

3. **Push/get API audit** — every endpoint a user can hit on the data plane gets a one-page contract:
   - **Push variants**: `/push/{event}` (acks=1 default, `SyncMode::Periodic`), `/push-sync/{event}` (acks=all, `SyncMode::PerEvent`, fsync before ack), `/push-batch/{event}` (multi-event in one frame), `/push-many` (TCP `OP_PUSH_MANY` if landed in Phase 12 follow-up), `/push-table/{table}` (table upsert via push), `/push-and-get/{event}` (combined endpoint, Plan 18-07 / Phase 12.5), `/push-sync-and-get/{event}` (acks=all + query in one round-trip)
   - **Get variants**: `/get` (batch JSON body `{keys, features}`), `/get/{feature}/{key}` (single feature single key), `/get-multi` (Phase 12 follow-up batch over many features × many keys with cell-cap enforcement)
   - **Table verbs**: `/upsert/{table}`, `/delete/{table}`, `/retract` (event_id-routed), `/set` and `/mset` and `/mget` (key-value-style table verbs from Phase 12 follow-up)
   - **For each endpoint**, document: HTTP method + path, TCP opcode (where applicable), request body shape (JSON + msgpack), response codes (200/4xx/5xx + the `code:` enum), invariants (single-writer, atomic borrow scope, ordering guarantees), perf characteristics (sync-mode latency budget, batch caps), and the curl example a user would copy-paste.
   - Cross-check against `runtime_core_glue.rs` dispatch + `register.rs` + `feature_query.rs` + `temporal_http.rs` + `push_and_get.rs` + apply_shard.rs's TCP variant handling. Flag missing endpoints, undocumented status codes, or response-body shape drift.
   - Table the routes in `docs/http-api.md` and `docs/tcp-wire.md` (or refresh existing).

4. **Test-coverage matrix** — for each {operator OR push/get endpoint} × {happy path, null/missing field, NaN/Inf, schema-mismatch, dedupe-replay, retraction, restart from snapshot, restart with WAL replay past snapshot, batch-cap exceeded, malformed body, unknown event, unknown feature}, confirm a test exists. File `.planning/phases/20-op-audit/20-COVERAGE-MATRIX.md` listing all tests by surface, flag missing cells.

5. **Validity tests** — write the missing tests surfaced by the matrix audit. TDD red-green per task per CLAUDE.md §Conventions.

6. **Documented edge-cases** — produce or update:
   - `docs/operators/{op}.md` (one per op) with the per-op contract from sub-goal 1
   - `docs/streaming-semantics.md` with all event-time / watermark / retraction / dedupe / idempotency decisions
   - `docs/http-api.md` + `docs/tcp-wire.md` with every endpoint × variant × wire-format combo
   - All sourced from CONTEXT.md / locked architectural decisions / memory — in user-facing prose, with curl examples for HTTP and `nc` examples for TCP.

**Depends on:** Phase 19 (we want to know the throughput ceiling before adding more operator coverage tests, since some tests are slow). Phase 12 follow-up (for `/push-many`, `/get-multi`, `/set`, `/mset`, `/mget` to actually exist when audited). Phase 12.5 (for `/push-and-get` to be in scope). Optional dependency on Phase 14.1 (modifiability) and Phase 15 (event-time PIT) if those land first — otherwise the audit baselines against current behavior.

**Success criteria:**
- Every op has a one-page contract committed to `docs/operators/`
- Every push/get endpoint has a contract in `docs/http-api.md` + `docs/tcp-wire.md`, including curl + `nc` examples
- Streaming-semantics decisions audited; mismatches between contract and code closed
- Test-coverage matrix shows no missing cells in the {surface × edge-case} grid (operators AND push/get endpoints)
- All new tests green; cargo clippy + cargo fmt clean
- `register_validate.rs` warnings/errors all documented in user-facing docs

**Why this matters:** Beava's v0 ship gate is "users can declare a feature, push events, query it — in under 10 minutes, with curl alone." That promise breaks the moment a user hits an undocumented edge case — whether on the operator side (NaN behavior, retraction semantics, restart determinism) OR on the API side (which push variant gives which durability guarantee, what happens on dedupe replay, how /retract routes between stream and table writes). Phase 20 closes both gaps before public launch.

### Phase 21: Nexmark MVP slice (Bucket A) — Rust generator + 8 queries vs Flink — 📋 PLANNED

**Status:** Planned post Phase 20 (added 2026-04-26 from `.planning/research/nexmark-gap-analysis.md`). Implements the first tier of three-tier Nexmark coverage. Builds on Phase 19's `## 1M-event blast` ledger format.

**Goal:** Run 8 Nexmark queries (q0, q1, q2, q14, q15, q17, q21, q22 — Bucket A in the gap analysis) on Beava with the upstream Nexmark generator, baselining against Flink reference outputs. Produces the published "Beava vs Flink on Nexmark" credibility line that fraud/streaming buyers ask for. Settles the row-emission-vs-state-serve drain pattern as a locked architectural decision. Lands the bundled scalar-DSL extension PR that unblocks half of Bucket B as a side effect.

**Sub-goals:**

1. **Nexmark generator port** — `crates/nexmark-gen/` Rust crate ports the Beam Nexmark generator with deterministic seed control. Inputs: `events_per_second`, `total_events`, `seed`, ratio knobs (Beam defaults: 92% bid, 6% auction, 2% person). Output: a `Stream<NexmarkRecord>` that an adapter shim translates into Beava `/push` payloads (HTTP + framed TCP). Aim ~1KLOC. Avoids JVM dep in the bench harness.

2. **`crates/beava-bench --nexmark` mode** — wires the generator into the existing Phase 19 bench scaffolding. New flag `--nexmark-query=q0..q22` selects which adapter to register. Ledger rows append to `.planning/throughput-baselines.md` under a new `## nexmark` section (sibling to `## 1M-event blast`); same column shape but the `Pipeline` column carries the query id.

3. **Expression-DSL bundle PR** — bundle these scalar additions in one commit set: `isin`, `lower`, `regex_extract(pattern, group)`, `split_index(sep, n)`, `format(fmt_string)`, `hour()` (date-part), `when(cond).then(val).otherwise(val)`, `%` (modulo). Plus aggregation modifier extension: confirm/extend `count(filter=expr)` and `count_distinct(filter=expr)`. All small, batch as one PR for review economy.

4. **Drain-pattern decision lock-in** — discuss-phase MUST resolve the row-emission gap. Two candidates: (a) spec a `/tail?event=<name>` streaming endpoint (becomes a real Beava capability beyond Nexmark — useful for live debugging, dashboards), or (b) adapter-only drain-cadence contract (poll `/get-multi` every 1s, hash-and-compare buckets; Beava core unchanged; correctness is "approximate-row-equivalence within cadence window"). Decision goes in `21-CONTEXT.md` and propagates to Tier 2/3 phases.

5. **Correctness harness** — for each of the 8 queries, run both Beava and Flink against the same deterministic generator seed; hash output row sequences (sort-then-hash for keyed-aggregation queries; raw-then-hash for streaming-tail queries); assert equality. Sketch-based ops (`count_distinct`, `percentile`) get ±epsilon tolerance per Beava's documented error bounds.

6. **Per-query criterion microbench + ledger row** — at minimum one criterion microbench for the Nexmark hot path (e.g., `nexmark_q15_filtered_count`) appending to `.planning/perf-baselines.md`. Per-query rows in `## nexmark` section of `.planning/throughput-baselines.md`.

**Depends on:** Phase 20 (operator catalogue audit lands one-page contracts that we cite from the Nexmark adapter docs). Phase 19 (1M-EPS bench harness wiring is the foundation for the `--nexmark` mode). The 8 Bucket A queries do NOT depend on Phase 15 (event-time PIT) — they all run on tumble + scalar transforms.

**Success criteria:**
- `crates/nexmark-gen/` ships a deterministic generator matching Beam reference output byte-for-byte (with documented seed/ratio knobs)
- 8 queries (q0, q1, q2, q14, q15, q17, q21, q22) green via correctness harness vs Flink (within ±epsilon for sketches)
- `## nexmark` section in `.planning/throughput-baselines.md` has all 8 query rows × HTTP/TCP × json/msgpack
- Drain-pattern decision committed to `21-CONTEXT.md` and reflected in either `/tail?event=` endpoint code OR the adapter's drain-cadence implementation
- Bundled scalar-DSL extension PR landed: 8 new ops/modifiers cited in this phase's CONTEXT
- `count(filter=expr)` confirmed working (or extended to work) for q15, q17

**Why this matters:** "Can Beava run Nexmark?" is the second-most-asked question after "Can Beava handle 1M EPS?" (Phase 19 covers the latter). For fraud/streaming buyers comparing platforms, Nexmark numbers vs Flink are table-stakes credibility. Tier 1 establishes that Beava covers the easy half (stateless transforms + per-key feature aggs) competitively; Tiers 2/3 extend coverage. Tier 1 also forces the row-emission decision that affects every future Beava product surface.

### Phase 22: Nexmark winner-ops + windowing (Bucket B) — 8 more queries — 📋 PLANNED

**Status:** Planned post Phase 21 (added 2026-04-26 from gap analysis). Adds the operators that unlock the next 8 queries.

**Goal:** Land q3, q5, q7, q8, q11, q16, q18, q19 against Flink reference. Adds `top_n_by` + `arg_max` (the "winner" ops that show up across fraud/leaderboard recipes), session windows (huge value beyond Nexmark for engagement/fraud-session detection), HOP/sliding windows (rolling-velocity recipes), processing-time virtual column, tumble-aligned event-event joins, and `@bv.table(mode='row')` row-as-value table mode.

**Sub-goals:**

1. **`top_n_by(k, by, return=[fields])`** — exact heap-of-N op (distinct from existing `top_k` SpaceSaving sketch which is frequency-mode). Per-key memory bounded = N × row-size. TDD red-green per task.

2. **`arg_max(by, return=[fields])`** — the k=1 specialization; returns the row tied to max. Underpins all "winner" patterns in fraud/auction/leaderboard recipes.

3. **`@bv.table(mode='row')`** — store the entire event payload as the table value (not field-by-field). Cleaner primitive than `bv.last(field)` per column. Unblocks q18 dedupe pattern.

4. **Session-window kind** — new operator family: `session_count(gap=)`, `session_sum(field, gap=)`, `session_first_event_time(gap=)`, `session_last_event_time(gap=)`. Data-driven boundaries (vs uniform tumble) require a new state machine: per-key `(session_start, last_seen, accumulator)`. Reuses bounded-buffer + apply machinery; the windowing kind is a new code path.

5. **HOP/sliding-window iterator** — generalize the bucketing engine to report every step instead of every period. Adds `step=` parameter to existing windowed ops. Already partly enabled by 64-bucket-cap + uniform bucketing.

6. **`proc_time` virtual column** — inject `proc_time` at apply time; existing windowed ops take it as the time field. Tiny addition (S effort), unblocks q12.

7. **`align='tumble'` option on event↔event join** — both sides snap to identical window boundaries. Unblocks q8.

8. **Per-query benches + ledger rows** — append rows to `## nexmark` section for q3/q5/q7/q8/q11/q16/q18/q19.

**Depends on:** Phase 21 (Nexmark adapter + generator + drain pattern + DSL bundle must exist). Independent of Phase 15 PIT (none of the Bucket B queries need event-time PIT).

**Success criteria:**
- 8 queries (q3, q5, q7, q8, q11, q16, q18, q19) green via correctness harness vs Flink
- New operators landed with TDD red-green commits + unit + property tests
- Per-key memory bounds enforced and tested for `top_n_by` and session windows
- `## nexmark` section has 8 more query rows × HTTP/TCP × json/msgpack
- Phase 22 SUMMARY documents which new ops landed (the ops are first-class Beava primitives, not Nexmark-specific scaffolding)

**Why this matters:** The Bucket B operators are the highest-leverage gap closers BEYOND Nexmark — `top_n_by`, `arg_max`, sessions, and HOP unlock standard fraud/engagement/leaderboard recipes that Beava's first wave of users will demand even without Nexmark framing. Nexmark is the forcing function; the operators are the deliverable.

### Phase 23: Nexmark Bucket C — retraction-aware joins + Table.agg — 📋 PLANNED (gated on Phase 15)

**Status:** Planned post Phase 22 AND Phase 15 (added 2026-04-26 from gap analysis). The remaining 4 Nexmark queries plus a major architectural item.

**Goal:** Cover q4, q6, q9, q20 — the queries blocked on event-time PIT (Phase 15 prerequisite) and on table-level re-aggregation with retraction propagation. Lands `Table.agg()` as a first-class DAG primitive: aggregations layered on top of derived tables, with retractions propagating through stages. Documents the q10 (file-system sink) and q13 (CSV-side-input) skip rationale.

**Sub-goals:**

1. **`Table.agg()` table-level re-aggregation** — heavyweight architectural item. Make `.agg()` first-class on `Table`, not just `Event`. Stage-2 aggregations re-aggregate over a derived table's column distribution. Unlocks q4 directly and any "agg of agg" pattern (cohort statistics, leaderboards over leaderboards). Requires retraction propagation: when stage-1 max changes, stage-2 avg must recompute. Multi-week with new state semantics.

2. **`last_n_avg(field, n)` rolling op** — small once Phase 15 PIT lands (rolling sum + count over the last N rows per key). Unblocks q6.

3. **`arg_max` extension for PIT-bound joins** — auction.dateTime..auction.expires bounds become a real PIT constraint via Phase 15. Unblocks q9.

4. **Row-emission contract finalization** — q20-style "emit every joined row" queries formalize the `/tail?event=` endpoint OR the cadence-drain pattern (whichever Phase 21 locked). Documents the contract in `docs/streaming-semantics.md`.

5. **Per-query benches + ledger rows** — q4/q6/q9/q20 get rows in `## nexmark` section.

6. **Skip-rationale docs** — q10 (file-system sink) and q13 (CSV side-input file loader) get documented "why not" entries in the Nexmark adapter README. Beava is a feature server, not an ETL framework; q10 measures sink throughput which has no analog. q13's join half runs on Beava's existing event↔table enrichment; only the file-loading step is adapter responsibility.

**Depends on:** Phase 15 (event-time PIT join, watermark — hard dependency). Phase 22 (operators must exist). The order of Phase 15 vs Phase 23 is not flexible — Phase 15 must land first.

**Success criteria:**
- 4 queries (q4, q6, q9, q20) green via correctness harness vs Flink
- `Table.agg()` is a first-class primitive with retraction propagation tests
- Phase 23 SUMMARY closes the Nexmark coverage at 19 of 23 queries (q10/q13 documented skips; q12 covered in Phase 22 via proc_time virtual column)
- "Beava vs Flink on Nexmark" published comparison covers all queries Beava is intended to support
- Beava-native sister benchmark family scoped (per-entity P99 reads under load, batch-get fanout, fraud-shape feature packs) — defer the implementation to a stretch phase

**Why this matters:** Closes Beava's Nexmark story — every query that aligns with Beava's feature-server model is benchmarked; non-aligned queries are documented as "not what Beava does" rather than as gaps. The `Table.agg()` primitive becomes a flagship Beava capability that competitors don't expose cleanly. After Phase 23, the marketing line is "Beava runs the half of Nexmark that maps to feature serving — at >10× the throughput per core" (numbers TBD by actual run).

### Phase 24+ (stretch): Nexmark Plus — Beava-native sister benchmark — 💡 BACKLOG

**Status:** Stretch / backlog (added 2026-04-26 from gap analysis). NOT v0 scope.

**Goal:** Extend the Nexmark complement with queries that *only* Beava (or other feature servers) can run cleanly: per-entity P99 latency reads under load, batch-get fanout, fraud-shape feature packs (the kind documented in `crates/beava-bench/`'s small/medium/large pipelines). Position as the "Beava native" benchmark — Flink won't have an equivalent, which is the point.

**Status:** moved to v0.0.x point releases / 999.x backlog parking lot. Revisit after v0 ships with Nexmark Tiers 1/2/3 green.

---

## Traceability (preview)

Populated in `REQUIREMENTS.md` traceability section. Summary: every REQ-ID maps to exactly one phase; Phase 1 ships zero scope-shipping REQ-IDs (infrastructure).

## Notes

- ROADMAP.md may be revised as phases complete and new-requirement discoveries force rebalancing. Revisions are committed as explicit changes.
- The previous 10-phase roadmap (commit `ad5a3ef`) was re-planned on 2026-04-22 when we pivoted from a JSON-only aggregation DSL to the v1 Python SDK API shape. Phase 1 (Foundation) work carries over unchanged.
