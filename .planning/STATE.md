---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: unknown
last_updated: "2026-04-15T02:03:25.829Z"
progress:
  total_phases: 8
  completed_phases: 3
  total_plans: 13
  completed_plans: 11
  percent: 85
---

# Project State

**Current Milestone:** v0 — Data-Scientist Fork (Phases 27, 35-38; Option K phases 28/30/31 superseded)
**Active Phase:** 35 — OP_LOG_FETCH (Option M kickoff) — planned, not yet executed
**Last Updated:** 2026-04-15 (Option M adopted: scientist forks CDC via LOG_FETCH+SUBSCRIBE, replica IS a local Tally server. Option K mid-flight Phase 31-02 cancelled. Phases 35-38 planned.)

## Milestone Status

| Milestone | Status | Completed |
|-----------|--------|-----------|
| v1.0 Foundation | Complete | 2026-04-09 |
| v1.1 Event Log & Composable Pipelines | Complete | 2026-04-10 |
| v1.2 Fire-and-Forget PUSH | Complete | 2026-04-11 |
| v1.3 Concurrency & Batching | Complete | 2026-04-12 |
| v2.0 New API & Engine | Complete | 2026-04-13 |
| v2.1 Launch | Engineering complete; live-run ops pending | 2026-04-14 (eng) |
| v0 Restructure (21-26) | Complete | 2026-04-14 |
| v0 Local Replica (27-34) | Active — Phase 27 starting | — |

## Scope note (2026-04-14)

v2.1 Launch (Phase 20) is closed on the engineering side: all artifacts shipped, tests green, v0 ports landed, 9-cell bench passes. Remaining work is human ops only — Hetzner VM provision + 5-day live observation — running async per the 9-step runbook in `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md § Resuming v2.1 Launch`. It does not block v0 Local Replica.

Phases 27-34 (originally sketched as a separate v0.1 milestone) are folded into v0 as the Local Replica leg, continuing the same milestone cycle. The restructure leg (21-26) is archived at `.planning/milestones/v0-ROADMAP.md`.

## v0 Restructure — Closeout note (2026-04-14)

v0 Restructure shipped. All 6 phases (21–26), all 22 plans complete:

- Phase 21 (3/3), Phase 22 (4/4), Phase 23 (3/3), Phase 24 (5/5), Phase 25 (3/3), Phase 26 (4/4).
- Sign-off: `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md` — **11 / 11 criteria green**.
- Final tests: **1628 green** (1170 cargo + 451 pytest python + 10 pytest integration).
- 9-cell benchmark gate passed (MATRIX-V0-FINAL.json `gate_passed: true`, worst `small_1c −4.84%`).
- Criterion sketch micro all pass (UDDSketch 23.74 ns / CMS 14.34 ns / HLL 43.17 ns — all <200 ns).
- Zero old-API refs in `python/ tests/ benchmark/ docs/`.
- Launch blog rewritten honestly (237 lines, zero placeholders, `{{DEMO_URL}}` kept for post-deploy resolution).
- Phase 20 traction demo ported to v0 SDK; deploy artifacts (`tally.service` / `Caddyfile` / `provision.sh` / `README.md`) clean-diff.
- Archive: `.planning/milestones/v0-ROADMAP.md`.

## v2.1 Launch — Remaining ops (async)

Runbook lives in `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md § Resuming v2.1 Launch`:

1. `git pull && cargo build --release --bin tally`
2. Copy binary to Hetzner box per `deploy/README.md`
3. First-time only (new box): `sudo bash deploy/provision.sh`
4. `sudo systemctl restart tally`
5. `bash deploy/smoke.sh <host>` — expect 6/6 invariants pass
6. Register traction pipeline: `python benchmark/replay/generator.py --register-only --target <host>`
7. Warm the 30-day replay
8. Load demo page, confirm features render
9. Publish blog + link live demo

No re-provision of `deploy/*` needed; v0 ports are API-agnostic there.

## Key design decisions (locked, all still in force)

- Stream vs Table as sole public types
- `@tl.stream` / `@tl.table` decorators with class=source / function=derivation convention
- Table aggregation disabled in v0 restructure (sidesteps Case 3 retraction complexity; deferred)
- UDDSketch for percentile, CMS+heap for top_k, HLL for count_distinct — all hybrid exact-first
- Fixed 5s watermark, tunable later; γ-model propagation
- `/debug/warnings` unified observability; `tally suggest-config` CLI for tuning
- Local replica is scope-driven, not whole-cluster (see `.planning/research/local-replica-design.md`)

## Deferred (post-v0)

- Table-input aggregation + full retraction propagation through DAG
- Outer joins (right/full)
- Session windows
- CEP / `match_recognize` patterns
- Horizontal scale-out / key-partitioned multi-threading
- CI/CD integration for the regression gate
- Multi-platform testing (macOS / Linux / Windows)
- Predicate-level replica scoping (e.g., `balance > 1000`)

## Phase History

- v1.x phases: see `.planning/milestones/v1.0-ROADMAP.md`, `v2.0-ROADMAP.md`
- v2.0: see `.planning/milestones/v2.0-ROADMAP.md`
- v0 Restructure (Phases 21–26): see `.planning/milestones/v0-ROADMAP.md`
- v2.1 Launch (Phase 20): see `.planning/milestones/v2.1-ROADMAP.md`

## Blockers

None. v2.1 Launch live-run is gated only on human action (VM provision + 5-day calendar window), not on engineering. v0 Local Replica (Phase 27) ready to plan.
