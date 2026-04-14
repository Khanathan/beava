# Project State

**Current Milestone:** v0 Restructure
**Active Phase:** α — Type system & SDK skeleton (not yet planned)
**Last Updated:** 2026-04-14

## Milestone Status

| Milestone | Status | Completed |
|-----------|--------|-----------|
| v1.0 Foundation | Complete | 2026-04-09 |
| v1.1 Event Log & Composable Pipelines | Complete | 2026-04-10 |
| v1.2 Fire-and-Forget PUSH | Complete | 2026-04-11 |
| v1.3 Concurrency & Batching | Complete | 2026-04-12 |
| v2.0 New API & Engine | Complete | 2026-04-13 |
| v2.1 Launch | PAUSED (pending v0) | - |
| v0 Restructure | Active | - |

## Why the v0 restructure

Tally is pre-launch. Phase 20 (v2.1 Launch — traction demo + blog + Hetzner deploy) had code artifacts ready and was about to go public when a design conversation on 2026-04-14 surfaced that the current `@tl.source`/`@tl.dataset` + `EventSet`/`FeatureSet` API (Phase 16) had structural issues for the streaming semantics Tally wants long-term:

- Out-of-order handling was ad-hoc (no watermarks)
- Type system couldn't distinguish append-only logs (Stream) from keyed current-state (Table)
- Retraction/correction semantics weren't formalized
- Operator catalog lacked DataFrame parity, percentile used memory-expensive sorted-Vec-per-bucket, top_k / count_distinct had no hybrid exact-to-sketch transition

Rather than ship these issues into the public API and pay migration tax later, v0 blocks the launch to rebuild the API clean.

## Accumulated Context

### Roadmap Evolution
- Phase 20 added and then paused: Traction Demo code complete, awaiting v0 restructure before public deploy
- v2.1 Launch milestone paused; roadmap snapshot in `.planning/milestones/v2.1-PAUSED-ROADMAP.md`
- v0 Restructure milestone activated with 6 phases (α through ζ)

### v0 Milestone Goal
Replace the public API with the two-type (Stream + Table) model, DataFrame-parity operators, UDDSketch/CMS+heap-backed hybrid sketches, fixed 5-second event-time watermarks, and a forward-compatible retraction architecture (Table aggregation deferred to v0.1 to keep v0 minimal). Phase 20 then ports to the new API and ships to the public.

### Key design decisions (locked during 2026-04-14 conversation; full spec in `.planning/research/v0-restructure-spec.md`)
- Stream vs Table as sole public types
- `@tl.stream` / `@tl.table` decorators with class=source / function=derivation convention
- Table aggregation disabled in v0 (sidesteps Case 3 retraction complexity)
- UDDSketch for percentile, CMS+heap for top_k, HLL for count_distinct — all hybrid exact-first
- Fixed 5s watermark, tunable later; γ-model propagation
- `/debug/warnings` unified observability; `tally suggest-config` CLI for tuning

### Phase 20 artifacts preserved
- `.planning/phases/20-traction-demo/` intact with SUMMARIES and PLANs
- `deploy/` directory with tally.service, Caddyfile, provision.sh, smoke.sh ready
- `docs/blog/streaming-shouldnt-require-a-platform-team.md` has placeholder content
- Phase ζ explicitly rebuilds Phase 20 against new API before deploy resumes

## Phase History

See `.planning/milestones/v2.0-ROADMAP.md` and `.planning/milestones/v2.1-PAUSED-ROADMAP.md` for archived phase details (1-20).

### v0 Restructure progress

- Phase 21 (SDK surface + DAG + REGISTER serializer): Complete (2026-04-14)
- Phase 22 (Stream aggregation engine): Complete (2026-04-14)
  - 22-01: v0 REGISTER parser + build_operator dispatch — shipped
  - 22-02: linear + order-sensitive operator bodies (Welford, event-time First/Last, FirstN, ema, lag) — shipped
  - 22-03: hybrid sketch operators (UDDSketch / CMS+heap / HLL threshold 1024) + telemetry — shipped
  - 22-04: TCP REGISTER v0 wiring + BASELINE.json + criterion install + TopK optimization + 9-cell matrix (all 9 cells ≤5% baseline) — shipped
- Phase 23 (joins) — next, can proceed on the wired v0 TCP path
