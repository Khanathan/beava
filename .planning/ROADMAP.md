# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [x] **v0 -- Restructure** (Phases 21-26) -- Complete 2026-04-14 -- `.planning/milestones/v0-ROADMAP.md`
- [ ] **v2.1 -- Launch** (Phase 20) -- Active (resuming; v0 unblocks deploy) -- see `.planning/milestones/v2.1-PAUSED-ROADMAP.md` (being renamed to ACTIVE by 26-04) and `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md` for resume instructions

## Phases

### v2.1 Launch (Active — resuming post v0 sign-off)

**Milestone Goal:** Ship Tally publicly. Prove traction with a live, read-only demo and a headline benchmark number (30-day historical replay time) surfaced in the launch blog.

- [ ] **Phase 20: Traction Demo** — v0-ready, awaiting deploy post v0 sign-off. Read-only public web showcase + 30-day historical replay benchmark + blog integration, runs live 5 days post-launch. (Artifacts shipped 2026-04-14; 26-03 ported them to the v0 SDK; Hetzner VM provision + 5-day live sign-off resume here post-26-04 closeout. No re-provision of `deploy/*` artifacts needed — they are API-agnostic and clean-diff at sign-off.)

### v0 Restructure (Complete 2026-04-14)

**Outcome:** Two-type (Stream + Table) API, DataFrame-parity operators, hybrid sketches (UDDSketch / CMS+heap / HLL), 5-second fixed event-time watermarks with γ propagation, per-Table row storage with 7d tombstone grace, unified `/debug/warnings` + `tally suggest-config`, zero-old-API codebase, 9-cell benchmark matrix within −5% of v2.0 BASELINE (worst cell −4.84%), launch blog rewritten honestly. **All 11 sign-off criteria green** — see `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md`. Full phase list + plan history archived in `.planning/milestones/v0-ROADMAP.md`.

## Progress

**Execution Order:** v0 Restructure (Phases 21-26) complete. v2.1 Launch (Phase 20) resumes next.

Dependency graph (historical):
```
21 ─┬─► 22 ─┬─► 23 ─► 26
   │      │        ▲
   │      └─► 24 ───┤
   └────────► 25 ───┘
```

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 21. Type system & SDK skeleton | v0 | 3/3 | Complete | 2026-04-14 |
| 22. Stream aggregation engine | v0 | 4/4 | Complete | 2026-04-14 |
| 23. Joins | v0 | 3/3 | Complete | 2026-04-14 |
| 24. Table storage + Watermarks & event-time | v0 | 5/5 | Complete | 2026-04-14 |
| 25. Query surface, TTL, warnings | v0 | 3/3 | Complete | 2026-04-14 |
| 26. Test migration, bench, docs, demo | v0 | 4/4 | Complete | 2026-04-14 |
| 20. Traction Demo | v2.1 | 2.5/3 | Artifacts + v0 ports shipped; VM provision + 5-day live run pending | — |
