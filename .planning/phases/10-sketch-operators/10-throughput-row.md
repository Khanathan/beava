# Phase 10 — Sketch throughput rows

**Captured:** 2026-04-24
**Format:** mirrors `.planning/throughput-baselines.md` — orchestrator merges into the canonical ledger post-PR.

**Notes:**
- HTTP-only transport — TCP push not yet wired (Phase 8 sibling will land it; per CONTEXT D-10).
- macOS `F_FULLSYNC` ceiling ~7.4 ms P50 → ~1k EPS plateau across all pipeline sizes regardless of CPU work per event. The phase10 sketch overhead is invisible at this hw-class because we're fsync-bound, not CPU-bound.
- Linux ext4 / Linux xfs / Linux io_uring runs (Phase 13 territory) will reveal the actual sketch CPU cost.

## hw-class: Darwin-24.3.0 / 10 cores (Apple Silicon M-series)

| Phase | Date | Pipeline | Transport | Sustained EPS | P50 push (µs) | P95 push (µs) | P99 push (µs) | P99 batch-get (µs) | Peak RSS (MB) | Commit SHA | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 2026-04-24 | medium-with-sketches | http | 982 | 7631 | 8943 | 10135 | 3217 | 95  | 13c60b9 | medium + count_distinct + percentile (5→7 features). HTTP only. fsync-bound on macOS. |
| 10 | 2026-04-24 | large-with-sketches  | http | 976 | 7619 | 9071 | 10375 | 2089 | 182 | 13c60b9 | large + 5 sketches (15→20 features). HTTP only. fsync-bound on macOS. |

**Regression vs Phase 7.5 baseline (same hw-class, simple-fraud small shape):**

- Phase 7.5 small/HTTP baseline ≈ 990 EPS (per CLAUDE.md hard constraints / Phase 5.5+ ledger).
- medium-with-sketches at 982 EPS: ~0.8% slower vs small baseline — well within the 25% blocker threshold (and within sample noise on the macOS fsync plateau).
- large-with-sketches at 976 EPS: ~1.4% slower vs small baseline — same conclusion.

**No regression gate fires.** Both rows are within 1-2% of the small-fraud Phase 7.5 baseline; the macOS `F_FULLSYNC` cap dominates wall time. The actual per-event CPU cost of the 5 sketch ops will surface once we run on Linux io_uring (Phase 13) or with `wal_fsync_interval_ms >= 8` to amortise fsyncs across larger groups.

**TCP-not-yet-wired follow-up:** re-run with `--transport tcp` once Phase 8 sibling lands the TCP push handler; append the resulting rows to the canonical ledger.
