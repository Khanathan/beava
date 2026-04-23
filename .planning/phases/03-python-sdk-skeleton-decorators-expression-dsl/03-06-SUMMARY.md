---
phase: "03"
plan: "03-06"
subsystem: python-sdk
tags: [python-sdk, acceptance, smoke, verification, phase-gate, tdd]
completed: "2026-04-23T12:00:00Z"
duration_minutes: 20

dependency_graph:
  requires:
    - python/beava package (Plans 03-01..03-05)
    - beava._events: EventSource, EventDerivation (from 03-03)
    - beava._tables: TableSource, TableDerivation (from 03-03)
    - beava._col: col, Col (from 03-02)
    - beava._app: App (from 03-05)
    - beava._embed: spawn_embedded_server, teardown_process (from 03-04)
    - python/tests/conftest.py: beava_binary + beava_server fixtures (from 03-04)
    - Phase 2.5 binary with HTTP + TCP listeners
  provides:
    - python/tests/test_phase3_smoke.py (8 smoke tests, SC1..SC7 + embed)
    - python/README.md (SDK quickstart: HTTP, TCP, embed, validate, expression DSL)
    - .planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-VERIFICATION.md
  affects:
    - Phase 4 (stateless ops + expression evaluator — depends on bv.col canonical grammar)
    - Phase 6 (push — depends on bv.App architecture)

tech_stack:
  added:
    - httpx (used in test_c5_validate_no_io and test_c6 for GET /registry queries)
  patterns:
    - TDD red-then-green commit pair (test: 4e28fd7 → feat: 913be3d)
    - pytest.fail("red stub") as Pattern A for all 8 test bodies (exit 1, all FAILED)
    - spawn_embedded_server() for SC6 two-server comparison (independent processes)
    - id(app._transport._socket) identity check for connection-reuse assertion (SC7)
    - _dev_only sentinel stripped before cross-transport JSON comparison (SC6)

key_files:
  created:
    - python/tests/test_phase3_smoke.py
    - python/README.md
    - .planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-VERIFICATION.md
  modified: []

decisions:
  - "Omit from __future__ import annotations in test file — stringifies annotations and breaks @bv.event function-form parameter inspection (documented in 03-05 deviations, applied consistently here)"
  - "SC6 star test spawns two independent subprocesses via spawn_embedded_server() to get fresh registry state; shares no state between HTTP and TCP registration rounds"
  - "SC7 connection reuse verified via id(app._transport._socket) — same Python object identity across ping/register/ping sequence means no reconnect happened"
  - "SC5 zero-network-IO proven by snapshotting GET /registry version before and after validate(); version must be identical"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 8
  tests_passing: 113
  files_created: 3
  files_modified: 0
---

# Phase 03 Plan 06: Phase 3 Acceptance Gate

**One-liner:** 8-test smoke suite (`test_phase3_smoke.py`) proves all 7 ROADMAP Phase 3 success criteria end-to-end against a live Rust `beava` binary, plus SDK quickstart README and VERIFICATION.md closing the phase.

## What Was Built

### `python/tests/test_phase3_smoke.py` (320 lines)

Eight criterion tests covering all 7 ROADMAP Phase 3 success criteria plus embed mode:

| Test | Criterion | Key assertion |
|------|-----------|---------------|
| `test_c1_event_decorator_both_forms` | SC1 | `isinstance(TxEvent, EventSource)` + schema fields + `isinstance(CheckoutDerivation, EventDerivation)` + `_upstreams == ["TxEvent"]` |
| `test_c2_table_decorator_both_forms` | SC2 | `isinstance(UserProfileTable, TableSource)` + `_primary_key`; TTL → ms; function form → `TableDerivation`; bad key raises `TypeError` at decoration |
| `test_c3_col_canonical_form` | SC3 | `(bv.col("amount") > 100).to_expr_string() == "(amount > 100)"`; compound `&` → `"((a > 0) and (b < 5))"` |
| `test_c4_register_both_transports` | SC4 | HTTP register → `registry_version=1`; TCP register → `registry_version=2`; both via live `beava_server` fixture |
| `test_c5_validate_no_io` | SC5 | Missing upstream → `ValidationError(kind="missing_upstream")`; `GET /registry` version unchanged before/after `validate()` |
| `test_c6_identical_registry_state_across_transports` | SC6 | Two fresh subprocesses; HTTP registers SC6EventA+SC6EventB+SC6Table; TCP registers same DAG; `GET /registry` JSON bodies equal after stripping `_dev_only` |
| `test_c7_tcp_ping_and_connection_reuse` | SC7 | Three pings return `server_version`+`registry_version`; `id(app._transport._socket)` stable across ping→ping→register→ping |
| `test_extra_embed_mode_end_to_end` | bonus | `with bv.App() as app: resp = app.register(TxEvent)` → `registry_version=1`, status=ok |

Module-level decorators (`TxEvent`, `LoginEvent`, `UserProfileTable`, `CheckoutDerivation`, `SC6EventA`, `SC6EventB`, `SC6Table`) defined at module scope — decoration is pure Python, no server required.

### `python/README.md` (73 lines)

SDK quickstart covering:
- Install: `pip install beava`
- HTTP transport: `with bv.App("http://localhost:7379") as app: app.register(...)`
- TCP transport: `with bv.App("tcp://localhost:7380") as app: ...`
- Embed mode: `with bv.App() as app: ...` — auto-spawns binary
- Validate: `bv.App(...).validate(...)` → `list[ValidationError]`
- Expression DSL: `bv.col("amount") > 100` → `"(amount > 100)"`

### `03-VERIFICATION.md`

Frontmatter: `gate: phase-3-acceptance`, `pass_fail: PASS`. Maps all 7 ROADMAP criteria to specific test functions with the assertion that proves each. Records all gate outputs (pytest, ruff, mypy, cargo test). Documents red-green commit pair with SHAs.

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `4e28fd7` | RED | `test(03-06): Phase 3 smoke tests + README placeholder + VERIFICATION stub pending` |
| `913be3d` | GREEN | `feat(03-06): Phase 3 acceptance gate — all 7 ROADMAP criteria proven end-to-end + README + VERIFICATION` |

Red commit: all 8 test bodies are `pytest.fail("red stub — implement in Plan 03-06 Task 1.b")`; `grep -c` returns 8; `pytest` exits 1 with all 8 FAILED. Zero PASSED, zero SKIPPED — strict TDD gate satisfied.

Green commit: stubs replaced with real assertions; `grep -c` returns 0; all 8 PASSED; full 113-test suite green; ruff + mypy clean.

## Verification Results

```
cd python && python -m pytest tests/test_phase3_smoke.py -v
  → 8 passed in 0.45s

cd python && python -m pytest tests/ -q
  → 113 passed in 1.97s

cd python && python -m ruff check beava/ tests/
  → All checks passed!

cd python && python -m mypy beava/
  → Success: no issues found in 12 source files

cargo test --workspace
  → 256 passed, 0 failed, 0 ignored
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Import ordering violation (ruff I001)**
- **Found during:** Task 1.b, ruff gate
- **Issue:** `import pytest` / `import beava as bv` / `import httpx` in wrong alphabetical order — ruff I001.
- **Fix:** `ruff check --fix` auto-resolved; `httpx` moved before `pytest`.
- **Files modified:** `python/tests/test_phase3_smoke.py`
- **Commit:** included in `913be3d`

**2. [Rule 1 - Bug] `from __future__ import annotations` breaks function-form decorator**
- **Found during:** Task 1.a, first collection run
- **Issue:** Module-level `@bv.event def CheckoutDerivation(src: TxEvent)` failed with `TypeError: parameter 'src' must be annotated with a descriptor (got 'TxEvent')`. The `from __future__ import annotations` import stringified all type annotations, so `param.annotation` returned the string `"TxEvent"` instead of the live `EventSource` object.
- **Fix:** Removed `from __future__ import annotations` from the test file. This is the documented pattern from Plan 03-05 (same issue, same fix).
- **Files modified:** `python/tests/test_phase3_smoke.py`
- **Commit:** included in `4e28fd7` (caught before red commit)

## Known Stubs

None. All 8 tests exercise real functionality against a live binary. The expression DSL is fully wired; server-side evaluation (Phase 4) is correctly noted in the README as shipping in Phase 4.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. Tests use existing `beava_server` fixture and `spawn_embedded_server()` — both covered by prior threat models (T-03-04-03, T-03-04-04). SC6 two-subprocess pattern is covered by T-03-06-02 (port 0 = OS-assigned, no hardcoded ports).

## Self-Check: PASSED

Files created:
- `/Users/petrpan26/work/tally/python/tests/test_phase3_smoke.py` — FOUND
- `/Users/petrpan26/work/tally/python/README.md` — FOUND, contains `import beava as bv` and `with bv.App(`
- `/Users/petrpan26/work/tally/.planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-VERIFICATION.md` — FOUND

Commits:
- `4e28fd7` (red) — FOUND
- `913be3d` (green) — FOUND

Gates:
- `pytest tests/test_phase3_smoke.py -v` → 8 passed — VERIFIED
- `pytest tests/ -q` → 113 passed — VERIFIED
- `ruff check beava/ tests/` → clean — VERIFIED
- `mypy beava/` → clean — VERIFIED
- `cargo test --workspace` → 256 passed — VERIFIED
- `grep -c 'pytest.fail("red stub'` → 0 — VERIFIED
- README does NOT contain `TODO: Plan 03-06 green task` — VERIFIED
