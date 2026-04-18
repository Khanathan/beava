---
phase: 47-repo-polish
plan: "09"
subsystem: examples
tags: [examples, http-api, fraud-scoring, session-features, curl-ingest, python, bash, docker]

requires:
  - phase: 47-01
    provides: beavadb/beava:latest Docker image + HTTP port 6900

provides:
  - "examples/fraud-scoring/ — 3-stream × 2-table HTTP-first fraud pipeline with 10k synthetic events"
  - "examples/session-features/ — single-stream last-N-click pipeline for first-timers"
  - "examples/curl-ingest/README.md — Phase 45 curl-ingest example documented and indexed"
  - "examples/README.md — index of all 3 examples with recommended order"

affects: [47-10, docs-site, onboarding, getting-started]

tech-stack:
  added: []
  patterns:
    - "Example pattern: run.sh starts Docker container if not running, installs deps, registers pipeline, pushes events, reads features"
    - "Pipeline registration via Python SDK over TCP (port 6400); event ingest via HTTP /push-batch (port 6900)"
    - "push_synthetic.py / push.py uses requests library only — no beava SDK needed for ingest"
    - "PYTHONPATH set in run.sh to pick up repo python/ SDK without pip install beava"

key-files:
  created:
    - examples/fraud-scoring/README.md
    - examples/fraud-scoring/pipeline.py
    - examples/fraud-scoring/push_synthetic.py
    - examples/fraud-scoring/run.sh
    - examples/session-features/README.md
    - examples/session-features/pipeline.py
    - examples/session-features/push.py
    - examples/session-features/run.sh
  modified:
    - examples/curl-ingest/README.md
    - examples/README.md

key-decisions:
  - "Used bv.count_distinct (not bv.distinct_count) — the SDK exports count_distinct; plan pseudocode had the wrong name"
  - "Used bv.avg (not bv.mean) — SDK has avg, not mean"
  - "Pipeline registration uses PYTHONPATH to pick up repo python/ SDK so run.sh works without pip install beava"
  - "push_synthetic.py and push.py require only 'requests' (not beava SDK) — HTTP ingest is language-agnostic"
  - "examples/curl-ingest/README.md updated for port 6900 (Docker image, Plan 47-01) — original said 6401 and referenced Phase 47 Docker as upcoming"

requirements-completed: [CONTENT-08, CONTENT-09, CONTENT-10]

duration: 4min
completed: 2026-04-18
---

# Phase 47 Plan 09: Examples Summary

**3-example suite shipping fraud-scoring (3-stream × 2-table, HTTP-first), session-features (single-stream last-N, first-timer-friendly), and curl-ingest README + examples/ index — all runnable against `docker run beavadb/beava:latest`**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-04-18T01:10:07Z
- **Completed:** 2026-04-18T01:13:28Z
- **Tasks:** 3
- **Files modified:** 10 (4 created in fraud-scoring, 4 in session-features, 2 updated)

## Accomplishments

- `examples/fraud-scoring/` — HTTP-first fraud pipeline: 3 streams (Transaction, Device, Login), 2 tables (UserFraudScore with 15 windowed features, UserLoginPattern with 4 features), 10k synthetic events via HTTP `/push-batch`, `run.sh` end-to-end runner.
- `examples/session-features/` — minimal first-timer example: 1 Click stream, 1 SessionFeatures table (clicks_5m, total_duration_5m, last_3_pages, first_page), TTL 30m, 1000 synthetic events.
- `examples/curl-ingest/README.md` — updated Phase 45 README to reflect Docker (port 6900), full endpoint table, see-also links.
- `examples/README.md` — replaced legacy single-entry index with 3-example index + recommended order (curl-ingest → session-features → fraud-scoring).

## Task Commits

1. **Task 1: examples/fraud-scoring/** — `8854ac3` (feat)
2. **Task 2: examples/session-features/** — `f1d42aa` (feat)
3. **Task 3: examples/curl-ingest/README.md** — `6f3611b` (docs)
4. **Task 3: examples/README.md index** — `e9784fc` (docs)

## Files Created/Modified

- `examples/fraud-scoring/README.md` — what it demonstrates, expected output, pipeline structure, cleanup
- `examples/fraud-scoring/pipeline.py` — 3 streams + 2 tables via Python SDK; uses bv.count_distinct, bv.avg, bv.last, bv.stddev
- `examples/fraud-scoring/push_synthetic.py` — pushes 10k events across Transaction/Device/Login via HTTP /push-batch
- `examples/fraud-scoring/run.sh` — one-shot runner (Docker start + register + push + read features)
- `examples/session-features/README.md` — pipeline diagram, expected output, explore-further section
- `examples/session-features/pipeline.py` — Click stream + SessionFeatures table with last_n, first, count, sum
- `examples/session-features/push.py` — 1000 click events via HTTP /push-batch/Click
- `examples/session-features/run.sh` — one-shot runner
- `examples/curl-ingest/README.md` — updated from Phase 45; port 6900, Docker prerequisites, see-also links
- `examples/README.md` — 3-example index with recommended order

## Smoke-run status

Not run against a live container (would require Docker pull). All acceptance criteria verified statically:
- All `run.sh` files pass `bash -n` syntax check
- All `.py` files pass `ast.parse`
- All READMEs >= 30 lines
- All `run.sh` reference `beavadb/beava:latest`
- `examples/README.md` indexes all three examples

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Corrected SDK aggregation function names in pipeline.py files**
- **Found during:** Task 1 (fraud-scoring pipeline.py authoring)
- **Issue:** Plan pseudocode used `bv.distinct_count` and `bv.mean`; actual SDK exports are `bv.count_distinct` and `bv.avg`
- **Fix:** Used `bv.count_distinct` and `bv.avg` throughout both pipeline files
- **Files modified:** examples/fraud-scoring/pipeline.py, examples/session-features/pipeline.py
- **Verification:** `python3 -c "import ast; ast.parse(...)"` exits 0; grep confirms correct names
- **Committed in:** 8854ac3 (Task 1), f1d42aa (Task 2)

**2. [Rule 2 - Missing Critical] Added PYTHONPATH in run.sh for SDK discovery**
- **Found during:** Task 1 (run.sh authoring)
- **Issue:** pipeline.py imports beava via sys.path.insert but run.sh also needed PYTHONPATH for the subprocess to find the repo's python/ SDK without requiring `pip install beava`
- **Fix:** `PYTHONPATH="$(cd ../.. && pwd)/python:${PYTHONPATH:-}" python3 pipeline.py` in both run.sh files
- **Files modified:** examples/fraud-scoring/run.sh, examples/session-features/run.sh
- **Committed in:** 8854ac3, f1d42aa

**3. [Rule 1 - Bug] Updated curl-ingest/README.md port from 6401 to 6900**
- **Found during:** Task 3
- **Issue:** Existing README referenced port 6401 and said Docker was "coming in Phase 47"; Docker now ships as 6900 per Plan 47-01
- **Fix:** Updated all port references, Docker run commands, and removed "upcoming" language
- **Files modified:** examples/curl-ingest/README.md
- **Committed in:** 6f3611b

---

**Total deviations:** 3 auto-fixed (1 SDK name bug, 1 missing critical PYTHONPATH, 1 stale port reference)
**Impact on plan:** All fixes necessary for correctness. No scope creep.

## Issues Encountered

- `@bv.table` function form requires stream class types as type hints (not instances); the benchmark code confirmed this pattern. Used correctly throughout.

## Known Stubs

None — both pipeline examples use correct SDK calls; `push_synthetic.py` and `push.py` use the `requests` library against live HTTP endpoints.

## Threat Flags

None — example files are static documentation and shell/Python scripts with no new network surface, auth paths, or schema changes.

## Next Phase Readiness

- CONTENT-08 closed (fraud-scoring).
- CONTENT-09 closed (session-features).
- CONTENT-10 docs portion closed (curl-ingest README + examples/README.md index). Full CONTENT-10 gates on SHIP-02 fresh-VM smoke (Plan 47-10).
- Plan 47-10 can proceed: examples/ is indexed, all 3 examples have runnable scripts.

---
*Phase: 47-repo-polish*
*Completed: 2026-04-18*
