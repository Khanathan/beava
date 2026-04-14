# Project State

**Current Milestone:** v2.1 Launch (resuming post v0 closeout)
**Active Phase:** 20 — Traction Demo (deploy-ready; VM provision + 5-day live run pending)
**Last Updated:** 2026-04-14 (v0 Restructure milestone closed; 26-04 sign-off 11/11 green)

## Milestone Status

| Milestone | Status | Completed |
|-----------|--------|-----------|
| v1.0 Foundation | Complete | 2026-04-09 |
| v1.1 Event Log & Composable Pipelines | Complete | 2026-04-10 |
| v1.2 Fire-and-Forget PUSH | Complete | 2026-04-11 |
| v1.3 Concurrency & Batching | Complete | 2026-04-12 |
| v2.0 New API & Engine | Complete | 2026-04-13 |
| v0 Restructure | Complete | 2026-04-14 |
| v2.1 Launch | Active (resuming) | — |

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

## Resuming v2.1 Launch

The canonical roadmap is `.planning/milestones/v2.1-ROADMAP.md` (renamed from `v2.1-PAUSED-ROADMAP.md`; the paused file now redirects to the active file). Resume checklist lives in `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md`:

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

## Why the v0 restructure (history)

Tally is pre-launch. Phase 20 (v2.1 Launch — traction demo + blog + Hetzner deploy) had code artifacts ready and was about to go public when a design conversation on 2026-04-14 surfaced that the old `@tl.source`/`@tl.dataset` + `EventSet`/`FeatureSet` API (Phase 16) had structural issues for the streaming semantics Tally wants long-term (no watermarks, no Stream/Table split, no formal retraction, no DataFrame parity, no hybrid sketches). Rather than ship these issues into the public API and pay migration tax later, v0 blocked the launch to rebuild clean. v0 now complete; Phase 20 resumes.

## Key design decisions (locked, all still in force)

- Stream vs Table as sole public types
- `@tl.stream` / `@tl.table` decorators with class=source / function=derivation convention
- Table aggregation disabled in v0 (sidesteps Case 3 retraction complexity; deferred to v0.1)
- UDDSketch for percentile, CMS+heap for top_k, HLL for count_distinct — all hybrid exact-first
- Fixed 5s watermark, tunable later; γ-model propagation
- `/debug/warnings` unified observability; `tally suggest-config` CLI for tuning

## Deferred to v0.1 (post-launch)

- Table-input aggregation + full retraction propagation through DAG
- Outer joins (right/full)
- Session windows
- CEP / `match_recognize` patterns
- `SCAN` / `SUBSCRIBE` opcodes
- Horizontal scale-out / key-partitioned multi-threading
- CI/CD integration for the regression gate
- Multi-platform testing (macOS / Linux / Windows)

## Phase History

- v1.x phases: see `.planning/milestones/v1.0-ROADMAP.md`, `v2.0-ROADMAP.md`
- v2.0: see `.planning/milestones/v2.0-ROADMAP.md`
- v0 Restructure (Phases 21–26): see `.planning/milestones/v0-ROADMAP.md`
- v2.1 Launch (Phase 20): see `.planning/milestones/v2.1-ROADMAP.md`

## Blockers

None. v2.1 Launch resume is gated only on human action (VM provision + 5-day calendar window), not on engineering.
