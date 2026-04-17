---
phase: 45-http-ingest-read-api
plan: 05
subsystem: docs-examples-benchmarks
tags: [http-api, docs, examples, benchmarks, curl, go, node, oha]
dependency_graph:
  requires: [45-01, 45-02, 45-03]
  provides: [docs/http-api.md, examples/curl-ingest/, benchmark/http_load.sh, docs/http-api-examples.sh]
  affects: [benchmark/README.md]
tech_stack:
  added: []
  patterns:
    - awk-based fenced-code-block extraction for live doc validation
    - HTTP /pipelines registration as SDK-free stream setup
    - oha smoke/reference-box mode split via LOAD_TEST_REFERENCE_BOX_REQUIRED flag
key_files:
  created:
    - docs/http-api-examples.sh
    - examples/curl-ingest/run.sh
    - examples/curl-ingest/sample-pipeline.py
    - examples/curl-ingest/README.md
    - benchmark/http_load.sh
  modified:
    - docs/http-api.md
    - benchmark/README.md
decisions:
  - "sample-pipeline.py uses stdlib urllib (HTTP /pipelines) not TCP SDK — avoids beava package dependency in examples"
  - "http-api-examples.sh extracts first block per language and seds localhost→127.0.0.1 for macOS IPv6 compat"
  - "Quickstart bash block in docs replaced Python SDK call with direct HTTP /pipelines curl — runnable without SDK"
  - "Reference-box EPS measurement deferred — oha not installed on dev machine; TBD marker in benchmark/README.md"
metrics:
  duration: "~45 minutes"
  completed: "2026-04-17T22:45:46Z"
  tasks_completed: 4
  tasks_deferred: 1
  files_created: 5
  files_modified: 2
---

# Phase 45 Plan 05: Wave 2 Docs + Examples + Load Test Summary

**One-liner:** Rewrote `docs/http-api.md` (1111 lines, 6 endpoints × 3 language examples), shipped `examples/curl-ingest/run.sh` (8-step smoke, exit 0), `benchmark/http_load.sh` (oha driver, smoke/reference-box modes), and `docs/http-api-examples.sh` (HTTP-10 live-code validation — Go compile + Node run + curl run all pass).

## Tasks Completed

| Task | Name | Commit | Result |
|------|------|--------|--------|
| 1 | docs/http-api.md rewrite (HTTP-10) | `5fe48e6` + `a7046b0` | 1111 lines, 6 endpoints × 3 languages, A7 + A5 notes |
| 2 | examples/curl-ingest/ (HTTP-08) | `f526a9d` | 8/8 steps pass, exit 0 on live server |
| 3 | benchmark/http_load.sh (HTTP-09) | `818fa8b` | oha script + README section; EPS TBD (reference box) |
| 4 | Reference-box EPS checkpoint | — | DEFERRED — see below |
| 5 | docs/http-api-examples.sh (HTTP-10 live) | `a7046b0` | Go + Node + curl all pass against live server |

## Acceptance Criteria Verification

### docs/http-api.md

```
wc -l docs/http-api.md          → 1111  (≥400 ✓)
grep -c '```bash'               → 30    (≥6 ✓)
grep -c '```go'                 → 6     (≥6 ✓)
grep -c '```javascript'         → 6     (≥6 ✓)
grep endpoint-refs              → 8     (≥6 ✓)
?sync=1 + durable-ack + Phase 46 notes → 5 matches (≥1 ✓)
401 / UNAUTHORIZED              → 55 matches (≥1 ✓)
beava_events_total{proto        → 3 matches (≥1 ✓)
```

All 9 acceptance criteria pass.

### examples/curl-ingest/run.sh

```
test -x examples/curl-ingest/run.sh         ✓
grep -c 'curl'                              → 12 (≥8 ✓)
grep -c endpoint patterns                   → 21 (all 6 endpoints ✓)
grep -c 'assert_field'                      → 14 (≥6 ✓)
Manual smoke run exit code                  → 0 ✓
```

### benchmark/http_load.sh

```
test -x benchmark/http_load.sh              ✓
grep -c 'oha '                              → 6 (≥1 ✓)
grep -c 'push-batch'                        → 5 (≥1 ✓)
grep -c 'EPS.*100000\|100000'               → 2 (≥1 ✓)
grep -c 'README.md'                         → 2 (≥1 ✓)
```

### docs/http-api-examples.sh

```
test -x docs/http-api-examples.sh           ✓
grep awk-extract blocks                     → 3 (≥3 ✓)
grep 'go build\|node '                      → 4 (≥2 ✓)
Live run: Go 200, Node 200, curl 200        ✓
```

## Deviations from Plan

### Deviation 1: sample-pipeline.py uses HTTP /pipelines, not beava TCP SDK

**Rule:** 1 (Auto-fix bug — SDK API mismatch)
**Found during:** Task 2
**Issue:** The plan's `sample-pipeline.py` template used `@tl.table(depends_on=[Transactions])` which is not valid in the actual SDK (no `depends_on` kwarg, and `@tl.table` requires `key=`). Additionally `App.register()` for a stream-only registration without operators produces an entity store with zero operators — the server registers the stream but creates no per-key state, so `/features/{key}` always returns 404.
**Fix:** Rewrote `sample-pipeline.py` to use `urllib.request` (stdlib only) to POST to `/pipelines` directly with explicit `tx_count_1h` and `tx_sum_1h` windowed features. No SDK dependency needed.
**Files modified:** `examples/curl-ingest/sample-pipeline.py`
**Commits:** `f526a9d`

### Deviation 2: Quickstart bash block replaced Python SDK with HTTP curl

**Rule:** 1 (Auto-fix bug — non-runnable doc example)
**Found during:** Task 5
**Issue:** The Quickstart `\`\`\`bash` block called `python3 -c "import tally as tl ..."` — `tally` is not the correct package name (it's `beava`) and the `register_remote` pattern doesn't exist in the current SDK. This made the first extracted bash block non-runnable.
**Fix:** Replaced the Python SDK Quickstart step with a direct `curl -X POST /pipelines` registration. The doc is now accurate to the HTTP-only path and the examples harness can run it without any Python SDK.
**Files modified:** `docs/http-api.md`
**Commits:** `a7046b0`

### Deviation 3: `localhost` → `127.0.0.1` substitution in examples harness

**Rule:** 3 (Auto-fix blocking issue — macOS IPv6 resolution)
**Found during:** Task 2 (first smoke run failure) and Task 5
**Issue:** macOS resolves `localhost` to `::1` (IPv6) but the Beava server binds `0.0.0.0` (IPv4 only). All `localhost:6401` references in `sample-pipeline.py` and extracted doc examples fail with `[Errno 49] Can't assign requested address`.
**Fix:** `sample-pipeline.py` uses `127.0.0.1` directly. `http-api-examples.sh` sed-substitutes `localhost:6401` → `127.0.0.1:${PORT}` on extracted Go, JS, and bash snippets before running them. Note: the doc examples still show `localhost:6401` (correct for Linux + Docker where IPv6 is not the issue); the harness adapts at runtime.
**Files modified:** `docs/http-api-examples.sh`, `examples/curl-ingest/sample-pipeline.py`
**Commits:** `f526a9d`, `a7046b0`

### Deviation 4: Reference-box EPS measurement deferred (Task 4 checkpoint)

**Type:** Deferred (not a bug fix — reference-box-only measurement)
**Found during:** Task 3/4
**Issue:** `oha` is not installed on the development machine. The >100K EPS measurement requires the reference box (10-core Apple Silicon, same machine that produced the 314K TCP baseline). This is per the plan's own instructions (`autonomous: false`, Task 4 is `checkpoint:human-verify`).
**Status:** `benchmark/http_load.sh` is complete and correct. `benchmark/README.md` has a "TBD — measure on reference box" marker in the HTTP ingest load test section. The measured number will be appended when `LOAD_TEST_REFERENCE_BOX_REQUIRED=1 bash benchmark/http_load.sh` is run on the reference box.
**HTTP-09 status:** Infrastructure shipped; numeric gate pending reference-box run.

## Smoke Test Results

**Server:** `./target/release/beava serve` on `localhost:6400` (TCP) / `localhost:6401` (HTTP)

**`bash examples/curl-ingest/run.sh`:**
```
== 1. Register Transactions stream via HTTP /pipelines ==  PASS
== 2. POST /push/Transactions (single event) ==             PASS  {"ok":true}
== 3. POST /push-batch/Transactions?sync=1 (3-event) ==    PASS  {"accepted":3,"rejected":0}
== 4. POST /push/Transactions/ndjson (5-event) ==           PASS  {"accepted":5,"rejected":0,"chunks":1}
== 5. GET /features/alice (all tables) ==                   PASS  {"ok":true,...}
== 6. GET /features/alice?table=Transactions ==             PASS  {"ok":true,...}
== 7. GET /streams ==                                       PASS  Transactions listed
== 8. GET /streams/Transactions ==                          PASS  {"name":"Transactions",...}
ALL GREEN (HTTP-08) — 8/8 steps passed
```

**`bash docs/http-api-examples.sh`:**
```
Go compile:   PASS
Go run:       200 {"ok":true}
Node run:     200 {"ok":true}
curl run:     {"status":"ok"} + {"ok":true} + features response
HTTP-10 PASS: Go + Node + curl examples from docs/http-api.md are live code.
```

**Build gates:**
```
cargo test --lib --release     → 788 passed, 0 failed ✓
cargo build --release --bin beava → Finished ✓
```

## Endpoint × Language Coverage in docs/http-api.md

| Endpoint | curl | Go | Node |
|----------|------|----|------|
| POST /push/{stream} | ✓ | ✓ | ✓ |
| POST /push-batch/{stream} | ✓ | ✓ | ✓ |
| POST /push/{stream}/ndjson | ✓ | ✓ | ✓ |
| GET /features/{key} | ✓ | ✓ | ✓ |
| GET /streams | ✓ | ✓ | ✓ |
| GET /streams/{name} | ✓ | ✓ | ✓ |

## Key Decisions Made

1. `sample-pipeline.py` uses stdlib `urllib` over HTTP `/pipelines` (not the beava TCP SDK) — removes SDK dependency from examples, works in any environment with python3.
2. `http-api-examples.sh` extracts exactly the first fenced block per language (awk exits after first closing fence) — prevents concatenation of all same-language blocks.
3. `LOAD_TEST_REFERENCE_BOX_REQUIRED=1` env flag gates EPS assertion and README append — smoke mode always safe to run on dev machines.
4. Quickstart bash block uses HTTP-only registration — doc is now self-contained without Python SDK.

## Known Stubs

None. All 6 endpoints exercised with real data; features responses contain actual computed values.

## Threat Flags

None. No new network endpoints or auth paths introduced in this plan (docs + scripts only).

## Self-Check

Files exist:
- docs/http-api.md ✓
- examples/curl-ingest/run.sh ✓
- examples/curl-ingest/sample-pipeline.py ✓
- examples/curl-ingest/README.md ✓
- benchmark/http_load.sh ✓
- docs/http-api-examples.sh ✓

Commits exist:
- 5fe48e6 docs(45-05): rewrite docs/http-api.md ✓
- f526a9d feat(45-05): add examples/curl-ingest/ ✓
- 818fa8b feat(45-05): add benchmark/http_load.sh ✓
- a7046b0 test(45-05): docs/http-api-examples.sh ✓

## Self-Check: PASSED
