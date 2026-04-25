# Phase 18 — Resume instructions for next session

**Status as of 2026-04-24 (late evening):** All 10 planning documents written and committed. Phase 18 is **plan-complete** and ready for execution dispatch (after the upstream merge round).

## What's done (all 10 docs)

| File | Commit | Notes |
|---|---|---|
| `18-redis-research.md` | `a5edf82` | Redis 7.x architecture summary |
| `18-rust-translation.md` | `4798bd3` | Rust mapping table |
| `18-CONTEXT.md` | `050cb32` | D-01..D-16 locked |
| `18-00-PLAN.md` | `050cb32` | Research plan (artifacts already complete) |
| `18-01-PLAN.md` | `050cb32` | Stage 18.1 — hand-rolled event loop + HTTP + TCP (~2200 LoC, 6 tasks) |
| `18-02-PLAN.md` | `050cb32` | Stage 18.2 — inline WAL + pthread fsync (~300 LoC, 3 tasks) |
| `18-03-PLAN.md` | `c5a3be8` | Stage 18.3 — I/O threads for reads (~500 LoC, 5 tasks) |
| `18-04-PLAN.md` | `4717509` | Stage 18.4 — I/O threads for writes (~250 LoC, 3 tasks) |
| `18-04.5-PLAN.md` | `115370a` | Stage 18.4.5 — Linux bench infra (infra/markdown only) |
| `18-05-PLAN.md` | `4476d86` | Stage 18.5 — io_uring on Linux — **HARD GATE ≥3M EPS/core** (~600 LoC, 5 tasks) |
| `18-06-PLAN.md` | `ccbc876` | Stage 18.6 — wire polish + VERIFICATION + SUMMARY (~400 LoC, 6 tasks) |
| `18-risks.md` | `621f5ec` | 8 risks with mitigations + cross-cutting register |

## What's remaining (none — planning is done)

Nothing in `.planning/` is outstanding for Phase 18. Execution starts at Stage 18.1 once the upstream merge round is complete (see "Critical: when execution starts" below).

## How to resume

### Option A — Direct write (recommended for next session)

Have a fresh agent write the 6 remaining plans using `18-01-PLAN.md` and `18-02-PLAN.md` as templates. The structure is:
- frontmatter (phase, plan, type, wave, depends_on, files_modified, autonomous, must_haves)
- goal + scope
- tasks with red-green TDD splits per CLAUDE.md §Conventions (except 18-04.5 which is infra-only)
- perf gate (informational on M4 for 18.3-18.4; HARD on Linux for 18.5+)
- verification + risks

Read the existing 18-CONTEXT.md for locked decisions and the existing 18-01/18-02 plans for the template style. Don't re-derive anything.

### Option B — Fresh planner dispatch

```bash
# In a fresh /clear'd context:
Agent(
  subagent_type="gsd-planner",
  model="opus",
  prompt="""
Write the 6 remaining Phase 18 plan documents.

Project: /Users/petrpan26/work/tally
Phase dir: /Users/petrpan26/work/tally/.planning/phases/18-redis-hand-roll/

Read these for context (DO NOT re-derive — these are committed and authoritative):
- 18-CONTEXT.md (D-01..D-16 locked decisions)
- 18-redis-research.md (Redis 7.x architecture)
- 18-rust-translation.md (Rust mapping)
- 18-01-PLAN.md and 18-02-PLAN.md (template style + frontmatter format)

Write these 6 documents using the same structure:
1. 18-03-PLAN.md — I/O threads for reads (Redis 6.0 pattern: spin-wait atomic
   barrier per pthread, distribute ready clients per tick, parallel parse).
   ~500 LoC, 4-5 tasks. M4 informational gate: 1-1.5M EPS/core aggregate
   with 4 I/O threads. Test scaling curve on 2/4/8 threads.

2. 18-04-PLAN.md — I/O threads for writes (parallel response serialize +
   write). ~250 LoC, 2-3 tasks. M4 informational gate: 2-2.5M EPS/core
   aggregate; tail p99 <5ms.

3. 18-04.5-PLAN.md — Linux bench infrastructure setup. NO TDD (infra task).
   Set up Linux Xeon runner (GitHub Actions self-hosted OR cloud VM with
   isolcpus, nohz_full, transparent_hugepage=never). Re-run 18.3+18.4 benches
   on Linux to establish baseline. Output: 18-04.5-linux-baseline.md.

4. 18-05-PLAN.md — io_uring on Linux (HARD GATE). Abstract IoBackend trait
   (mio on macOS, io_uring on Linux). io-uring crate integration. Batched
   submit/reap. ~600 LoC, 4-5 tasks. HARD GATE on Linux Xeon: ≥3M EPS/core
   simple-fraud TCP. Stretch ≥4M.

5. 18-06-PLAN.md — Wire polish + VERIFICATION. Zero-copy argv (no Row
   materialization), hand-rolled response formatters, static response
   strings, criterion microbench post-refactor, full beava-bench matrix
   on M4 + Linux. ~400 LoC, 5-6 tasks. PERF GATE 6.1: full Phase 13
   spec target on Linux for simple-fraud + complex-fraud + recommendation
   pipelines. PERF GATE 6.2: each micro-opt shows 5-10% individual uplift.
   Final 18-VERIFICATION.md + 18-SUMMARY.md.

6. 18-risks.md — 8 risks with mitigations:
   - HTTP parsing edge cases (chunked, trailers) → use httparse + explicit tests
   - Integration test rewrites (~200 LoC) → keep tokio for admin tests
   - I/O threads spin-wait CPU burn idle → exponential backoff + park
   - fsync coordination across pthreads → AtomicU64 LSN watermark
   - Cross-runtime handoff for admin endpoints (cold path) → bounded mpsc
   - macOS kqueue no io_uring benefit → accept M4 ceiling; Linux is target
   - Axum dependency drag for admin → minimal cost, separate listener
   - Senior review before Stage 18.3 → atomic coordination correctness review

TDD red-green per task EXCEPT 18-04.5 (infra) which is markdown/setup only.
Use absolute paths in frontmatter files_modified.
Commit each document as docs(18-redis-hand-roll): <subject>.

Commit each one immediately so context can offload. If you hit 70%, stop
and update this RESUME.md with what's left.
"""
)
```

## Why this matters

Phase 18 closes the throughput gap between current 16k EPS/core (Phase 13.3 endpoint) and the 3M EPS/core ship-gate target. Each stage has a clear perf gate, so we catch regressions early instead of building everything and being surprised at the end.

## Key locked decisions to remember (from 18-CONTEXT.md)

- **HTTP + TCP both in same hand-rolled loop** (D-01) — admin endpoints on tokio/axum on port 8081
- **Crate: `beava-runtime-core`** (D-15)
- **Apple-M4 INFORMATIONAL gates** through Stage 18.4; **Linux Xeon HARD gates** from Stage 18.5 onward (D-14, D-16)
- **TDD exemptions**: Plan 18-00 (research) + Plan 18-04.5 (infrastructure) — markdown/setup only

## Critical: when execution starts

Phase 18.0 is already done (research + translation already shipped). Execution starts at **Stage 18.1** with a fresh worktree:

```bash
git worktree add -b phase-18-redis-hand-roll .claude/worktrees/phase-18-redis-hand-roll v2/greenfield
```

Then dispatch `gsd-executor` for Stage 18.1 pointed at the worktree. Each stage gets its own dispatch with its perf gate as the success criterion.
