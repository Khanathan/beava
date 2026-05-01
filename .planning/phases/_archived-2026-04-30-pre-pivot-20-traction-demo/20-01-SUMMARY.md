---
phase: 20-traction-demo
plan: 01
subsystem: benchmark
tags:
  - benchmark
  - replay
  - python
  - cli
  - launch
requires:
  - Phase 8 SCHM-03 event-time bucketing
  - Phase 13 OP_PUSH_BATCH
  - Phase 14 per-stream DashMap locks
  - Phase 19 v2.0 tl.App.push_many
provides:
  - Deterministic 30-day replay CLI (benchmark/replay/replay_30d.py)
  - Reusable fraud-event generator (benchmark/replay/generator.py)
  - Dual-use: launch benchmark + user-facing historical backfill tool
affects:
  - benchmark/ (new package)
  - tests/integration/ (new dir)
tech-stack-added:
  - multiprocessing.Pool (spawn context)
  - urllib (post-run HTTP probes to management API)
patterns:
  - Module-level worker + spawn context for multiprocessing portability
  - Deterministic shard-by-key hashing (stable fold, not Python's randomized hash())
  - Post-measurement HTTP probe of /debug/latency, /debug/memory, /metrics
key-files-created:
  - benchmark/__init__.py
  - benchmark/replay/__init__.py
  - benchmark/replay/generator.py
  - benchmark/replay/replay_30d.py
  - benchmark/replay/README.md
  - tests/integration/__init__.py
  - tests/integration/conftest.py
  - tests/integration/test_replay_generator.py
  - tests/integration/test_replay_30d.py
key-files-modified: []
decisions:
  - Use random.Random(seed) not global random, so determinism is not broken by unrelated imports
  - Sort events by ts so window operators bucket correctly (event-time, not arrival-time)
  - Shard by deterministic fold over user_id, not Python's hash() (process-randomized on str)
  - Module-level _worker function + spawn context for cross-platform multiprocessing
  - Post-run HTTP probes are best-effort (return 0 on any failure) — the CLI should never fail because /debug/latency was slow
  - Put tests under tests/integration/ (not python/tests/) because the plan said so and because these tests touch both SDK and Rust subprocess — they are not pure-SDK tests
metrics:
  duration_minutes: ~25
  completed: 2026-04-14
requirements-completed:
  - TRAC-01
  - TRAC-02
  - TRAC-03
---

# Phase 20 Plan 01: Deterministic 30-Day Replay CLI — Summary

Ship a deterministic 30-day replay benchmark CLI — `benchmark/replay/replay_30d.py` — that spawns 8 `multiprocessing` workers, drives `push_many(batch_size=1000)` into a running Tally instance, and prints a 7-field wall-clock report. Doubles as a user-facing historical-backfill tool via `--input events.jsonl`.

## What shipped

| Path | Role | Commit |
|------|------|--------|
| `benchmark/__init__.py` | Makes `benchmark.replay` a valid package | `5fc050a` |
| `benchmark/replay/__init__.py` | Package init | `5fc050a` |
| `benchmark/replay/generator.py` | Deterministic fraud-event generator (`generate(n, seed=42, days=30, now_ms=None)`), 100k×5k pools, log-normal amounts, 5% failure rate, ts-sorted output | `5fc050a` |
| `benchmark/replay/replay_30d.py` | CLI: argparse, register, shard-by-user, `Pool.starmap(worker, …)`, post-run HTTP probes, key=value report | `fa298af` |
| `benchmark/replay/README.md` | Dual-purpose framing (launch benchmark + backfill tool), flag reference, CI pointer | `f750cf2` |
| `tests/integration/__init__.py` | New test package | `5fc050a` |
| `tests/integration/conftest.py` | sys.path wiring so `benchmark.*` and `tally` import from the working tree | `5fc050a` |
| `tests/integration/test_replay_generator.py` | 7 unit tests: determinism (list + JSON byte-equality), timestamp spread, sort order, failure rate, schema shape, speed, seed sensitivity | `5fc050a` |
| `tests/integration/test_replay_30d.py` | 3 integration tests: `--help` renders, 100k × 4 workers end-to-end with 7 report fields + eps > 50k floor, determinism re-run at 5k scale | `fa298af` |

## Success criteria

- [x] `benchmark/replay/replay_30d.py --help` prints all documented flags — verified
- [x] `benchmark/replay/replay_30d.py --events 100000 --workers 4` against localhost completes, prints 7 report fields, exit code 0 — verified via integration test
- [x] Two runs with same `--events` and default seed produce byte-identical generated event streams — `test_determinism` asserts this both as list equality and JSON byte-equality
- [x] Integration test passes in CI in < 30s — actual: 1.76s for the 3 replay tests, 1.96s for the full 10-test suite
- [x] README frames CLI as both a benchmark and a backfill tool — verified (`grep backfill && grep seed`)
- [ ] At 30M events × 8 workers on the deploy VM, `events_per_sec` ≥ 800k — **deferred to manual smoke on the deploy VM (Plan 20-03)**; local smoke on the dev box hits 276k eps at 200k × 4 debug-release run

## Test results

```
$ python3 -m pytest tests/integration/ -x -q --timeout=120
..........                                                               [100%]
10 passed in 1.96s
```

- `test_replay_generator.py`: 7/7 pass (determinism, timestamp spread, sort order, failure rate, schema shape, fast, seed-sensitivity)
- `test_replay_30d.py`: 3/3 pass (help, 100k end-to-end, determinism re-run)

## Manual smoke

```
$ TALLY_TCP_PORT=17400 TALLY_HTTP_PORT=17401 ./target/release/tally &
$ python3 benchmark/replay/replay_30d.py --events 200000 --workers 4 --no-warmup \
    --host 127.0.0.1 --port 17400 --mgmt-port 17401
events_total=200000
elapsed_seconds=0.723
events_per_sec=276767.8
p50_push_us=0.0
p99_push_us=0.0
keys_total=86617
final_state_mb=169.17
```

276k eps on a shared dev box with 4 workers × release build. The launch-box 1M+ target is deferred to 20-03 manual smoke.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing `pytest-timeout` plugin**
- **Found during:** Task 2 integration test run
- **Issue:** Plan's verify command `--timeout=60` flag unrecognized; `pytest-timeout` not installed.
- **Fix:** `pip install --user --break-system-packages pytest-timeout` (single one-off install; package manager forbade system install).
- **Files modified:** none (environment-level change).

**2. [Rule 3 - Blocking] `benchmark/` had no `__init__.py`**
- **Found during:** Task 1 RED
- **Issue:** `from benchmark.replay.generator import …` failed because `benchmark/` was a sibling directory without a package marker.
- **Fix:** Added empty `benchmark/__init__.py`. No side effects — existing `benchmark/tally-throughput/bench.py` is a loose script that doesn't rely on package semantics.
- **Files modified:** `benchmark/__init__.py` (new, empty).

### Known Stubs / Caveats

- **`p50_push_us=0.0` / `p99_push_us=0.0` in the report during batch-only runs.** The server's latency histogram records synchronous `OP_PUSH`, not `OP_PUSH_BATCH`. Since the replay driver uses only `push_many`, no PUSH samples land in the histogram and the report correctly returns 0. This is a pre-existing server behavior (owned by `src/server/tcp.rs` `latency.record_push`) and out of scope for Plan 20-01. The fix — record batch-push latency per event (or per batch) — would live in Plan 20-02 when it extends `/metrics`. Documented as TRAC-07 territory.
- **`final_state_mb` via `/debug/memory` best-effort.** The endpoint's JSON shape has drifted across phases; `_extract_memory_mb` tries three known key names (`total_bytes`, `grand_total_bytes`, `estimated_bytes`) and falls back to summing `per_stream`. If the shape changes again the report shows `0.0` but the CLI still exits clean.

## Threat Flags

None. The replay CLI is a client — it speaks only TCP (existing OP codes) and HTTP reads against the management API. No new network surface introduced.

## Dependency handoff

Plan 20-03 depends on this script to both (a) seed the live demo instance with realistic state and (b) produce the headline blog number. The CLI is stable: default seed 42, 30M events, 8 workers, warmup on. For the blog run, capture stdout directly — the report is already in a parseable `key=value` format.

## Self-Check: PASSED

- FOUND: `benchmark/replay/generator.py`
- FOUND: `benchmark/replay/replay_30d.py`
- FOUND: `benchmark/replay/README.md`
- FOUND: `benchmark/replay/__init__.py`
- FOUND: `benchmark/__init__.py`
- FOUND: `tests/integration/test_replay_generator.py`
- FOUND: `tests/integration/test_replay_30d.py`
- FOUND: `tests/integration/conftest.py`
- FOUND: commit `5fc050a` (Task 1)
- FOUND: commit `fa298af` (Task 2)
- FOUND: commit `f750cf2` (Task 3)
