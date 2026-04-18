# Beava

## What This Is

Beava is a real-time feature server for ML: a single-node Rust service that ingests
events over TCP (and soon HTTP), runs composable keyed/keyless stream pipelines with
event-time semantics, and serves the freshest per-entity feature values via low-latency
reads. It's built for engineers who need Flink-grade correctness without Flink's
operational surface area, and for data scientists who want to fork a scoped slice of
production CDC to a laptop and experiment against their own pipelines.

## Core Value

**A skeptical engineer evaluating Beava on github.com can go from landing on the repo
to correct, live feature values in under 60 seconds — from any language.**

If this fails, nothing else matters: no benchmarks, no blog posts, no fork story
recovers a failed 60-second evaluation.

## Requirements

### Validated

<!-- Shipped and confirmed valuable. -->

- ✓ Binary TCP ingest with `push`, `push_batch`, `push_many` — v1.x
- ✓ Event log (per-stream, durable) + crash recovery — v1.1
- ✓ Composable keyed/keyless pipelines with DAG execution — v2.0
- ✓ DataFrame-parity stream/table operators (filter, map, agg, join, fork) — v2.0
- ✓ Event-time watermarks with γ propagation, 5s fixed lateness — v2.0
- ✓ Hybrid sketches (UDDSketch, CMS+heap, HLL) — v2.0
- ✓ Per-Table row storage with 7d tombstone grace — v2.0
- ✓ Unified `/debug/warnings` + `tally suggest-config` — v2.0
- ✓ 9-cell benchmark matrix within −5% of v2.0 BASELINE — v2.0
- ✓ Public read-only `/public/*` HTTP surface + loopback-or-token admin middleware — v2.1
- ✓ Scoped `OP_LOG_FETCH` + `OP_SUBSCRIBE` (server replica opcodes) — v0 Phase 27
- ✓ Replica-mode server boot (`--replica-from`, `--replica-since`, `--replica-streams`) — v0 Phase 36
- ✓ `tally fork` CLI + data-scientist E2E demo — v0 Phase 37
- ✓ Per-stream write locks and hot-path mutex removal — Phases 40-41

### Active

<!-- Current scope — milestone v1.2 (Thread-Per-Core + Full Key-Shard) -->

## Current Milestone: v1.2 Thread-Per-Core + Full Key-Shard

**Goal:** Intra-node scaling via thread-per-core + full key-shard architecture — eliminate DashMap contention and cross-core cache-line bouncing to reach 1.5M–2.5M EPS on a 16-core box (5-6× current baseline), preserving correctness and migration-compat with today's single-shard state format.

**Source of truth:** `.planning/arch/TPC-SHARD-DESIGN.md` + `.planning/arch/TPC-RESEARCH.md` (all 6 original design questions resolved, hardened against 2026 research).

**Target features (five waves → five phases):**
- **Wave 0 — shard_hint scaffolding** — `EventSource::shard_hint()` trait method, wired through TCP + HTTP parsers; micro-benches for hash overhead (<100 ns) and SPSC roundtrip (<10 μs). Backward-compatible; always returns 0 for N_SHARDS=1.
- **Wave 1 — per-shard state store** — `Shard` struct encapsulating state (single-threaded HashMap, not DashMap), event log, watermark, dirty set. Runtime-configurable N_SHARDS from day 1, default 1.
- **Wave 2 — multi-shard routing** — SO_REUSEPORT-per-shard accept on Linux; single-listener fallback on macOS; `core_affinity` pinning; 9-cell matrix re-run with ≥3× gate on `complex-c8-x8` at N=CPU_COUNT.
- **Wave 3 — cross-shard queries + joins** — `GET /streams` scatter-gather; co-location constraint for joins (register-time shard_key agreement); lazy global-watermark publish across shards.
- **Wave 4 — per-shard event log + recovery + fork/replica** — `data/shard-N/streams/{name}/log.bin` layout; parallel recovery (one thread per shard); re-sharding tool for N=1→N=K migration; fork/replica always re-hashes on ingest by downstream N (no `--reshard-from` flag); production-readiness ship-gate (1M+ EPS load test, N=1↔N=8 property parity, failover, docs/architecture-tpc.md).

**Ship gate for merging to main:** (1) every 9-cell matrix cell within −5% of baseline at N=1; (2) ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT; (3) shard_probe cross_shard_fraction <40% on the release workload.

**Runtime choice:** tokio current_thread (`Builder::new_current_thread().build_local()`) per pinned shard for v1.2. compio is the v1.3 / Beava Cloud endpoint, not v1.2. glommio rejected (unmaintained as of 2026).

---

<!-- v1.0-launch (engineering complete 2026-04-17) — awaiting launch-day human-run items, tracked in STATE.md § Launch Day Checklist -->

**Block 1 — HTTP Ingest & Read API (unlock non-Python DX):**
- [ ] `POST /push/{stream}` single-event JSON
- [ ] `POST /push-batch/{stream}` JSON-array batch
- [ ] `POST /push/{stream}/ndjson` streaming body
- [ ] `GET /features/{key}` and `GET /features/{key}?table=X`
- [ ] `GET /streams`, `GET /streams/{name}`
- [ ] Reuse `BEAVA_ADMIN_TOKEN` + `require_loopback_or_token` middleware
- [ ] >100 K EPS on single HTTP batch stream
- [ ] Curl/Go/Node examples + HTTP API reference

**Block 2 — Correctness audit & fixes:**
- [ ] Fix batch-path shared-`now` bug (`push_batch_with_cascade_no_features`)
- [ ] Add `beava_ring_buffer_drops_total{stream,operator,reason}` metric
- [ ] Make `watermark_lateness` per-stream configurable (default 5s)
- [ ] Audit & resolve: backfill path correctness (2d.i)
- [ ] Audit & resolve: crash-recovery determinism uses payload `_event_time` (2d.ii)
- [ ] Audit & resolve: TTL evaluation vs event-time (2d.iii)
- [ ] Audit & resolve: fork watermark propagation (2d.iv)
- [ ] Audit & resolve: join idle-input documentation + deferral (2d.v)
- [ ] Audit & resolve: snapshot cross-entity consistency (2d.vi)
- [ ] Audit & resolve: dirty-gen race on snapshot rollover (2d.vii)
- [ ] `docs/event-time.md` page

**Block 3 — Repo polish & Docker:**
- [ ] README rewrite (HTTP-first, <60 lines)
- [ ] CONTRIBUTING / CHANGELOG / LICENSE / CODE_OF_CONDUCT / SECURITY / GOVERNANCE pass
- [ ] Directory READMEs (`src/server/`, `src/engine/`, `src/state/`, `benchmark/`, `deploy/`)
- [ ] TODO/FIXME sweep; dead-code audit; `#![warn(missing_docs)]` on crate root
- [ ] `cargo clippy --all-targets -- -D warnings` green
- [ ] `cargo fmt --check` green
- [ ] Docker Hub image (`beavadb/beava:latest` + tagged) + `docker compose up` example
- [ ] 8 docs pages (getting-started, concepts, http-api, python-sdk, event-time, operations, architecture, faq)
- [ ] Example projects: `fraud-scoring/`, `session-features/`, `curl-ingest/`
- [ ] GitHub Actions CI (test + clippy + fmt), CI badge on README
- [ ] Social preview PNG, GitHub topics/description

**Ship gate:**
- [ ] E2E smoke test on fresh VM: install → HTTP push → read → crash → recover → verify
- [ ] Outreach rewrite re-audited against AUDIT-V11.md for fabricated claims
- [ ] Benchmarks re-verified on current tree (ingest, recovery, fork-replay)

### Out of Scope

<!-- Explicit boundaries. Deferred to post-launch roadmap. -->

- **Multi-node via Kafka** — v1.3+ roadmap. Horizontal scaling; single-node posture is part of the launch story.
- **compio runtime migration** — v1.3 / Beava Cloud. Wave-0-through-Wave-4 TPC lands on tokio current_thread; compio swap comes after the architecture ships and measures a real io_uring ceiling worth chasing.
- **UDF / stateful scripting (Rhai / WASM)** — v1.2 roadmap. Powerful but not on the critical path to first-user success.
- **OpenAPI / Swagger UI** — nice, but a clean `docs/http-api.md` is enough for launch.
- **Deploy-button integrations (Fly.io / Railway / Render)** — nice, not launch-gating.
- **Web UI for `/debug/*`** — CLI + HTTP suffice; not launch-gating.
- **CLI subcommands (`beava push`, `beava get`, `beava tail`)** — DX polish, post-launch.
- **Stateless derive expressions via Python SDK** — partly built; finish post-launch if not already exposed.

## Context

**Prior milestones (engineering complete):**
- v1.0 Foundation (phases 1-5) — Apr 9
- v1.1–v1.3 Event Log / Composable Pipelines / Concurrency & Batching (phases 6-15) — Apr 12
- v2.0 New API & Engine (phases 16-19) — Apr 13
- v2.1 Launch prep (phase 20) — Apr 14, live-run ops calendar-gated
- v0 Restructure (phases 21-26) — Apr 14
- v0 Data-Scientist Fork, Option M (phases 27, 35-38; Option K at 28/30/31 superseded) — Apr 15

**Audit signal feeding this milestone:**
Release audit this week surfaced one real correctness bug (batch-path shared `now`
collapses event-time buckets for backfill / sparse-batch clients) and several
silent-failure modes (ring-buffer drops below the watermark late-drop metric,
hardcoded 5s `WATERMARK_LATENESS`, uninspected fork/replay/TTL/snapshot corners).
These are not launch-blocking in steady-state production but are unacceptable for
a publicly-launched feature server that backfill users will hit in minute one.

**DX signal feeding this milestone:**
Today's only write path is the Python SDK over binary TCP. Every non-Python engineer
evaluating Beava hits the SDK wall in the first 60 seconds. HTTP ingest + read is
the single highest-leverage DX investment — turns Beava into a curl-able,
language-agnostic server.

**Repository name:** directory is `tally/` (legacy name); public product name is
**Beava**. Cargo package `beava`. Python SDK `beava`. Old `tally` CLI binary
remains until Block 3 polish.

## Constraints

- **Tech stack**: Rust single-binary server, axum HTTP, tokio runtime, DashMap for
  per-stream state, UDDSketch / CMS / HLL for sketches. Python SDK.
  Apache 2.0 license.
- **Performance**: HTTP batch ingest must hit >100 K EPS on a single stream
  (TCP baseline is ~350 K EPS). Regression gate: 9-cell bench within −5% of
  v2.0 BASELINE.
- **Correctness**: Event-time semantics must be load-bearing — payload `_event_time`
  is the source of truth for bucketing, TTL, crash-replay, fork-replay. Wall-clock
  is not authoritative inside the engine.
- **Compatibility**: Existing Python SDK users must keep working. HTTP surface is
  additive. `BEAVA_ADMIN_TOKEN` / `require_loopback_or_token` middleware is reused
  — no new auth model.
- **Team**: 1–3 engineers. Target ship window: ~3 weeks with two engineers in
  parallel on blocks 1 and 2.
- **Public-launch bar**: a 10-second github.com visit must earn the "let me try the
  quickstart" click — README, badges, Docker image, examples must all read as
  production-quality.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| One milestone for all three blocks (not three separate milestones) | LAUNCH.md ship gate is unified; blocks are parallel workstreams, not sequential products. | — Pending |
| Continue phase numbering from 45 (no reset) | `phase_dir_count=32` with no completed-milestone archive target; reset is unsafe. | — Pending |
| HTTP ingest reuses `handle_push_core_ex` + existing auth middleware | Zero duplicated ingest logic; ships faster; keeps correctness guarantees from TCP path. | — Pending |
| Fix batch-path shared-`now` bug before public launch | Real correctness bug (backfill misbuckets); silently masked in live production because batches are sub-ms wide. | — Pending |
| Docker Hub + README as the 60-second onboarding path (Option A for docs) | Docs site (mkdocs/docusaurus) is 3-5 days for marginal launch win; markdown in `docs/` is enough for v1. | — Pending |
| Keep Python SDK TCP path unchanged | HTTP surface is additive; breaking the SDK would regress validated v2.0 users. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-18 after starting milestone v1.2 Thread-Per-Core + Full Key-Shard*
