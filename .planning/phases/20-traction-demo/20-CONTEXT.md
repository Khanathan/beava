# Phase 20: Traction Demo - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Auto-generated from user conversation (scope pre-locked)

<domain>
## Phase Boundary

Ship a public, read-only traction demo of Tally to support the v2.1 launch. Three deliverables + a 5-day live run:

1. **Historical replay benchmark** — a CLI script that ingests 30 days of historical events into a fresh Tally instance as fast as possible and records wall-clock time + events/sec. Also reusable as a standalone historical-backfill tool.
2. **Public web demo** — a hosted page where visitors can **read** the live Tally instance: query features by key, browse recent events, see computed feature values. **No public write access.** PUSH/SET/MSET must be authenticated or internal only.
3. **Live traction metrics** — the demo page surfaces aggregate metrics updated in near-real-time: total events processed since launch, uptime, p99 push latency, current events/sec.
4. **Blog integration** — the launch blog post (posts already exist under `docs/blog/`) embeds or links the live demo and prominently displays the 30-day replay wall-clock time as a headline number.
5. **Live operation** — deployed instance stays up ≥ 5 consecutive days with snapshot-based crash recovery verified.

</domain>

<decisions>
## Implementation Decisions

### Public Access Model (LOCKED by user 2026-04-14)
- Public URL is **read-only**: GET features for a key, browse recent events, view aggregate metrics.
- No public PUSH / SET / MSET / REGISTER exposure. Write ops either authenticated (admin only), bound to localhost, or driven by internal event generator.
- Implication: the web demo talks to an HTTP proxy/gateway that translates read requests to Tally's TCP GET (or uses HTTP management API's debug endpoints). Direct TCP exposure to the internet is out.

### Historical Data Source (to be chosen during research)
- "Last 30 days of events" — source data not yet fixed. Candidates: synthesized realistic transaction/login stream (reuse bench.py generators), public dataset (e.g. NYC taxi, GitHub events archive), or captured production-like trace. Planner chooses.
- Whatever the source, the replay script must be deterministic (same input → same timing benchmark is reproducible) and scale to a number that produces a headline-worthy eps figure.

### Replay Performance Goal
- Match or exceed the v1.3 benchmark baseline (1.1M eps sustained, per `19-05-PLAN.md`).
- Replay uses OP_PUSH_BATCH (Phase 13) and exploits the existing concurrency (Phase 14 DashMap per-stream locks) to hit max throughput.

### Live Metrics Source
- Reuse existing HTTP management API (port 6401): `/metrics` (Prometheus), `/health`, `/debug/memory`. If p99 push latency / current eps aren't exposed, extend the metrics endpoint — don't invent a new one.
- Frontend polls `/metrics` on an interval (e.g. 2s) and renders counters + simple sparklines. No backend WebSocket required in v1 unless trivial.

### Frontend Stack
- Keep it minimal. Existing project has a debug UI under Phase 10/10.1/10.2 — check if its frontend (likely vanilla JS or Preact) can be reused / extended for the public demo, rather than introducing a new framework. Planner decides.
- Single-page, static assets served by the same binary or a separate static host. Zero new infrastructure preferred.

### Deployment
- Single Tally binary + static frontend assets on a modest VM (1-2 vCPU). Snapshot directory on local disk. No Docker Compose orchestration required.
- 5-day uptime requirement means snapshot persistence (Phase 9) and crash recovery must be verified end-to-end before launch.

### Blog Integration
- Existing blog lives in `docs/blog/` (recent commits: Reddit posts, quickstart fixes).
- Add/update a launch post with: the replay headline number, an embedded live-metrics widget or screenshot + link to demo URL, and the backfill benchmark story.

### Test Plan (per user feedback preference — every phase includes tests)
- Unit: replay script parses inputs correctly, rate-limits handled, metrics endpoint returns expected shape.
- Integration: end-to-end 30-day replay in CI against a fresh Tally (smaller dataset to keep CI time reasonable, but same code path). Assertions on final eps floor and state size.
- Smoke: deployment script + health check verifies live demo URL returns 200 and metrics update.

### Claude's Discretion
- Exact frontend layout and visual polish (subject to UI review).
- Choice of historical data source (given constraints above).
- Whether to host the demo on fly.io, Render, a cheap VM, or similar — pick the simplest option that meets 5-day uptime.

</decisions>

<code_context>
## Existing Code Insights

Codebase context will be gathered during plan-phase research. Known entry points:
- `bench.py` — benchmark script migrated in Phase 19; reuse generators.
- HTTP management API — existing `/metrics`, `/health`, `/debug/*` endpoints in `src/server/http.rs`.
- Debug UI — `src/server/` + static assets from Phase 10/10.1/10.2.
- Event log — `.planning/phases/06-*` established SSD event log; may be reused as the 30-day replay source if trace data is captured from an internal run.
- OP_PUSH_BATCH — Phase 13 batch opcode for max ingestion throughput.

</code_context>

<specifics>
## Specific Ideas

- Headline number framing: "Tally replayed 30 days of events in X seconds." Must be a single, memorable, sub-minute-ideally number.
- Live demo should feel alive: event count incrementing visibly, recent-events feed scrolling, clear "tally is processing X events/sec right now" indicator.
- Reference look: inspired by gstack traction demo (user mentioned in original request — verify what surface area it exposes during research).

</specifics>

<deferred>
## Deferred Ideas

- Public write access / user-submitted events — **deferred**, out of scope for launch.
- Multi-region deployment, auto-scaling — deferred.
- Historical replay UI (scrub timeline, replay controls in browser) — deferred.
- Feature comparison dashboards vs competitor systems — deferred; belongs in blog copy, not the demo app.

</deferred>

---

*Phase: 20-traction-demo*
*Context gathered: 2026-04-14 (pre-locked from conversation with user)*
