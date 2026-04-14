# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- see `.planning/milestones/v1.0-ROADMAP.md`
- [x] **v1.1 -- Event Log & Composable Pipelines** (Phases 6-10.2) -- Complete 2026-04-10 -- see `.planning/milestones/v1.1-ROADMAP.md` (archived within v2.0)
- [x] **v1.2 -- Fire-and-Forget PUSH** (Phase 11) -- Complete 2026-04-11
- [x] **v1.3 -- Concurrency & Batching** (Phases 12-15) -- Complete 2026-04-12
- [x] **v2.0 -- New API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- see `.planning/milestones/v2.0-ROADMAP.md`
- [ ] **v2.1 -- Launch** (Phase 20) -- Active

## Phases

### v2.1 Launch (Active)

**Milestone Goal:** Ship Tally publicly. Prove traction with a live, read-only demo and a headline benchmark number (30-day historical replay time) surfaced in the launch blog.

- [~] **Phase 20: Traction Demo** - Read-only public web showcase + 30-day historical replay benchmark + blog integration, runs live 5 days post-launch (artifacts shipped 2026-04-14; 20-03 awaiting VM provision + 5-day sign-off)

## Phase Details

### Phase 20: Traction Demo
**Goal**: Public-facing, **read-only** web demo showcasing Tally running live for 5 days post-launch (view features + query state, no public write access), with a historical-replay benchmark that ingests the last 30 days of data on startup and records wall-clock throughput -- surfaced as a headline number in the launch blog post.
**Depends on**: Phase 19 (v2.0 complete)
**Requirements**: TRAC-01, TRAC-02, TRAC-03, TRAC-04, TRAC-05, TRAC-06, TRAC-07, TRAC-08, TRAC-09, TRAC-10, TRAC-11
**Success Criteria** (what must be TRUE):
  1. A standalone benchmark script replays the last 30 days of historical events into a fresh Tally instance as fast as possible and prints total time, events/sec, and final state size -- reusable as a historical-backfill tool
  2. Visitors to a public URL can browse the live Tally instance read-only: query features for any key (GET) and inspect recent events / computed feature values -- no public PUSH/SET write access
  3. The live demo page surfaces aggregate traction metrics (total events processed since launch, uptime, p99 push latency, current events/sec) updated in near-real-time
  4. The launch blog post embeds or links the live demo and prominently displays the 30-day replay wall-clock time as a headline number
  5. The deployed demo stays up for at least 5 consecutive days post-launch without manual intervention, with crash-recovery from snapshot verified
**Plans:** 3/3 plans complete
- [x] 20-01-PLAN.md — Deterministic 30-day replay CLI (`benchmark/replay/replay_30d.py`) + multi-process `push_many(batch_size=1000)` driver + integration test at 100k-event scale
- [x] 20-02-PLAN.md — `/public/*` read-only HTTP surface + `require_loopback_or_token` admin middleware + extended `/metrics` + vanilla-JS demo.html (~150 LOC) via existing rust-embed
- [~] 20-03-PLAN.md — Hetzner CX22 deploy (systemd + Caddyfile + provision.sh) + smoke script (6 invariants incl. TCP 6400 closed publicly) + blog post update with headline replay time + 5-day live-run sign-off with mid-run crash recovery — artifacts + LIVE_SIGNOFF framework shipped 2026-04-14; VM provision (human action) + 5-day sign-off (calendar-gated) pending

## Progress

**Execution Order:**
Phase 20 is the only active phase in v2.1. Plans 20-01 and 20-02 are parallel (Wave 1); 20-03 depends on both (Wave 2).

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 20. Traction Demo | v2.1 | 2.5/3 | Artifacts shipped; 5-day live run pending | — |
