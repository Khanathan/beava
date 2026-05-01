---
phase: 26-test-migration-bench-docs-demo
milestone: v0-restructure
status: complete
completed: 2026-04-14
plans: 4/4
signoff: 11/11 green
---

# Phase 26 Summary — Test migration, benchmarks, docs, demo rebuild

**Status:** Complete 2026-04-14. All 4 plans shipped. 11/11 sign-off criteria green. v0 Restructure milestone closed. v2.1 Launch resumable.

## Plans

| Plan | Focus | SUMMARY |
|------|-------|---------|
| 26-01 | Test migration + old-API deletion | [26-01-SUMMARY.md](26-01-SUMMARY.md) |
| 26-02 | Bench regression gate + launch-blog perf numbers | [26-02-SUMMARY.md](26-02-SUMMARY.md) |
| 26-03 | Traction demo port + blog narrative + v2.1 unpause | [26-03-SUMMARY.md](26-03-SUMMARY.md) |
| 26-04 | Sign-off + milestone close | [26-04-SUMMARY.md](26-04-SUMMARY.md) |

## Headline numbers

- **Total tests green:** 1628 (1170 cargo + 451 pytest python + 10 pytest integration) — well above the ≥744 baseline
- **Old-API refs in scoped tree:** 0 (was 115 across 17 files pre-26-01)
- **9-cell matrix:** `gate_passed: true`, worst cell `small_1c −4.84%` (threshold −5%)
- **Criterion sketch micro:** all pass — UDDSketch 23.74 ns, CMS 14.34 ns, HLL 43.17 ns
- **Launch blog:** 237 lines, 0 placeholders, honest headline from worst 1c cell
- **Deploy files:** clean-diff across `tally.service` / `Caddyfile` / `provision.sh` / `README.md`

## Artifacts

- `26-SIGNOFF.md` — 11-criteria checklist with live evidence
- `MATRIX-V0-FINAL.json` — 9-cell benchmark gate
- `MICRO-V0-FINAL.json` — criterion sketch micro gate
- `docs/blog/streaming-shouldnt-require-a-platform-team.md` — launch blog
- `.planning/milestones/v0-ROADMAP.md` — archived milestone roadmap

## Milestone sign-off

`.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md` — all 11 criteria green, no red gates, no escalation.

## What ships next

**v2.1 Launch** — Phase 20 resumes. Resume checklist: [26-04-SUMMARY.md § Resuming v2.1 Launch](26-04-SUMMARY.md).
