---
phase: 47-repo-polish
plan: "08"
subsystem: docs
tags: [docs, architecture, comparison, faq, python-sdk, http-api]
dependency_graph:
  requires: []
  provides:
    - docs/architecture.md (CONTENT-06, D-28)
    - docs/comparison.md (D-30)
    - docs/faq.md (CONTENT-07, D-29)
    - docs/python-sdk.md (CONTENT-04, D-25)
    - docs/http-api.md polish (CONTENT-10 docs portion, D-24)
  affects:
    - README.md (links to all 5 pages)
    - docs/event-time.md (cross-linked from http-api.md and faq.md)
tech_stack:
  added: []
  patterns:
    - honest-voice documentation (no fabricated Nx claims)
    - cross-linked docs graph (each page links to 2+ related pages)
    - pip-installable SDK docs pattern
key_files:
  created:
    - docs/faq.md
  modified:
    - docs/architecture.md
    - docs/comparison.md
    - docs/python-sdk.md
    - docs/http-api.md
decisions:
  - "comparison.md rewritten as full multi-comparison page (was Flink-only); 430-510K EPS claim replaced with committed 315K EPS baseline"
  - "python-sdk.md: bv.fork() documented; bv.variance/top_k/first_n operators added; all v0 stub comments removed; pip install beava added as primary install"
  - "http-api.md: Error Codes Reference appendix added; Phase 45/46 internal notes removed; event-time.md cross-link verified present"
  - "architecture.md: Single-Node by Design + Scaling Posture + Fork Replica Model + Security Model sections added"
  - "faq.md: new file with honest answers; cites committed benchmarks; admits at-least-once, single-node ceiling, bus factor 1"
metrics:
  duration_minutes: ~45
  completed: "2026-04-18T01:16:33Z"
  tasks_completed: 4
  files_modified: 5
  files_created: 1
requirements_closed:
  - CONTENT-04
  - CONTENT-06
  - CONTENT-07
  - CONTENT-10 (docs-polish portion; full CONTENT-10 closes at 47-09 ship gate)
---

# Phase 47 Plan 08: Reference Docs + http-api.md Polish Summary

**One-liner:** Four reference docs audited/extended (architecture, comparison, faq new, python-sdk) + http-api.md polish with Error Codes appendix and voice cleanup.

## Commits

| Hash | Message | Files |
|------|---------|-------|
| `387e1f2` | docs(47-08): audit + extend docs/architecture.md (CONTENT-06, D-28) | docs/architecture.md |
| `ec90c65` | docs(47-08): audit + extend docs/comparison.md (D-30) | docs/comparison.md |
| `a0ad6ff` | docs(47-08): docs/faq.md — scale / Flink / prod-ready / Feast (CONTENT-07, D-29) | docs/faq.md |
| `3cd6db0` | docs(47-08): re-verify docs/python-sdk.md against live SDK (CONTENT-04, D-25) | docs/python-sdk.md |
| `f5fdeea` | docs(47-08): polish docs/http-api.md — voice, error codes, event-time.md cross-link (CONTENT-10, D-24) | docs/http-api.md |

## Line Counts (final)

| File | Lines | Min Required | Status |
|------|-------|-------------|--------|
| docs/architecture.md | 545 | 150 | PASS |
| docs/comparison.md | 222 | 120 | PASS |
| docs/faq.md | 184 | 80 | PASS |
| docs/python-sdk.md | 1136 | 150 | PASS |
| docs/http-api.md | 1137 | (polish only) | PASS |

## Cross-Links Verified

All cross-link targets exist as files in the repo at time of commit:

| From | To | Pattern |
|------|----|---------|
| docs/architecture.md | docs/comparison.md | `comparison.md` |
| docs/architecture.md | docs/event-time.md | `event-time.md#fork-watermark-propagation` |
| docs/architecture.md | benchmark/ | `benchmark/` |
| docs/architecture.md | docs/operators.md | `operators.md` |
| docs/comparison.md | docs/architecture.md | `architecture.md` |
| docs/comparison.md | docs/faq.md | `faq.md` |
| docs/comparison.md | benchmark/ | `benchmark/` |
| docs/faq.md | docs/comparison.md | `comparison.md` |
| docs/faq.md | docs/architecture.md | `architecture.md#scaling-posture` |
| docs/faq.md | docs/event-time.md | `event-time.md` |
| docs/faq.md | docs/python-sdk.md | `python-sdk.md` |
| docs/python-sdk.md | docs/http-api.md | `http-api.md` |
| docs/python-sdk.md | docs/architecture.md | `architecture.md#fork-replica-model` |
| docs/python-sdk.md | docs/concepts.md | `concepts.md` |
| docs/http-api.md | docs/event-time.md | `event-time.md#crash-replay-determinism` |
| docs/http-api.md | docs/operations.md | `operations.md` |

## SDK Example Fixes (Drift Caught)

The following discrepancies were found between the existing python-sdk.md and the live SDK in `python/beava/`:

1. **Installation method**: docs showed `pip install -e .` (dev only); live package is `pip install beava`. Fixed.
2. **Missing operators**: `bv.variance`, `bv.top_k`, `bv.first_n` all present in `_agg_ops.py` and `__init__.py` but absent from docs. Added.
3. **Missing `bv.fork`**: `bv.fork()`, `ForkedReplica`, and fork error types all present in `_fork.py` but absent from docs. Full section added.
4. **`v0:` stub comments**: 10+ `# v0:` comments in code examples referenced non-functional placeholder patterns. Replaced with working code:
   - `.with_columns(bv.derive(...))` pattern for derived features
   - `.filter(bv.col(...))` pattern for stream filtering
   - `where=` parameter syntax for per-operator filtering
5. **`Field()` usage in validation example**: Used `user_id: str = Field()` annotation style not present in live SDK surface. Replaced with plain `user_id: str` annotations.
6. **Old SDK name**: `/pipelines` endpoint docs referenced `tl.register_remote()` (pre-rename). Fixed to `app.register()`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `comparison.md` cited "430-510K events/sec on a 48-core Xeon"**
- **Found during:** Task 1 (comparison.md audit)
- **Issue:** Unverified benchmark claim inconsistent with committed baseline (315K EPS on 10-core M4 laptop in `benchmark/LAUNCH-VERIFY.md`)
- **Fix:** Replaced with committed 315K EPS TCP / 100K+ HTTP baseline throughout
- **Files modified:** docs/comparison.md

**2. [Rule 1 - Bug] `comparison.md` was scoped to Flink-only (title: "Beava vs Flink + Kafka + Redis")**
- **Found during:** Task 1 (audit)
- **Issue:** Plan requires Feast, Redpanda, and ksqlDB/Materialize pairwise sections; existing file had none
- **Fix:** Full rewrite as multi-comparison reference page
- **Files modified:** docs/comparison.md

**3. [Rule 1 - Bug] `http-api.md` referenced `tl.register_remote()` (pre-rename SDK name)**
- **Found during:** Task 4 (http-api.md voice pass)
- **Issue:** Python SDK was renamed; `tl.register_remote()` no longer exists
- **Fix:** Updated to `app.register()`
- **Files modified:** docs/http-api.md

**4. [Rule 1 - Bug] `http-api.md` body limit: plan spec said "2 MiB default" but server source constant is 16 MiB**
- **Found during:** Task 4 (error codes table authoring)
- **Issue:** Plan template said 2 MiB; actual `DEFAULT_MAX_BODY_BYTES = 16 * 1024 * 1024` confirmed in `src/server/http_ingest.rs`
- **Fix:** Error codes table correctly says 16 MiB; no change needed to existing http-api.md body limits section (already correct)
- **Files modified:** none (confirmed existing content correct)

## Known Stubs

None. All stub patterns removed from python-sdk.md. All docs reference live, committed code.

## Threat Flags

None. No new network endpoints, auth paths, or schema changes introduced. Documentation only.

## Self-Check: PASSED

Files exist:
- `docs/architecture.md` — FOUND (545 lines)
- `docs/comparison.md` — FOUND (222 lines)
- `docs/faq.md` — FOUND (184 lines)
- `docs/python-sdk.md` — FOUND (1136 lines)
- `docs/http-api.md` — FOUND (1137 lines)

Commits exist:
- `387e1f2` — FOUND (architecture.md)
- `ec90c65` — FOUND (comparison.md)
- `a0ad6ff` — FOUND (faq.md)
- `3cd6db0` — FOUND (python-sdk.md)
- `f5fdeea` — FOUND (http-api.md)
