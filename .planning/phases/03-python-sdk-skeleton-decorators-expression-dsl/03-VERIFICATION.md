---
phase: "03"
gate: phase-3-acceptance
pass_fail: PASS
verified_date: "2026-04-23"
verification_type: acceptance-gate
test_counts:
  phase3_smoke: 8
  phase3_full_suite: 113
  rust_workspace: 256
criteria_mapping:
  total: 7
  proven: 7
  gaps: 0
commits_verified:
  red: 4e28fd7
  green: (see below — committed after VERIFICATION authoring)
---

# Phase 3 Verification

## Status: PASS

All 7 ROADMAP Phase 3 success criteria proven end-to-end via `python/tests/test_phase3_smoke.py` against a real Rust `beava` binary (via the `beava_server` + `beava_binary` fixtures from Plan 03-04). Python suite 113 passed; Rust workspace 256 passed; ruff clean; mypy strict clean.

## Criterion-to-test mapping

| # | ROADMAP criterion (verbatim) | Test function | What assertion proves it | Status |
|---|------------------------------|---------------|--------------------------|--------|
| 1 | `@bv.event` class form extracts schema and registers event descriptor; function form resolves upstreams | `test_c1_event_decorator_both_forms` | `isinstance(TxEvent, EventSource)`, schema field py_types correct, `event_time_field` detected; `isinstance(CheckoutDerivation, EventDerivation)`, `_upstreams == ["TxEvent"]`; `_to_register_json()` shapes verified | pass |
| 2 | `@bv.table(key=..., ttl=...)` class + function forms work; key validation at decoration | `test_c2_table_decorator_both_forms` | `isinstance(UserProfileTable, TableSource)`, `_primary_key == ["user_id"]`; TTL converts to ms; function form yields `TableDerivation`; `@bv.table(key="missing")` raises `TypeError` at decoration; bare `@bv.table` raises `TypeError` | pass |
| 3 | `bv.col("x") > 100` expression produces expected `to_expr_string()` canonical form | `test_c3_col_canonical_form` | `(bv.col("amount") > 100).to_expr_string() == "(amount > 100)"`; compound `& `→ `"((a > 0) and (b < 5))"`; arithmetic, NOT, `isinstance(bv.col("x"), bv.Col)` | pass |
| 4 | `app.register(*descriptors)` topologically sorts the DAG, detects cycles, validates schemas, dispatches to HTTP or TCP based on URL scheme, receives `registry_version` | `test_c4_register_both_transports` | HTTP register → `status="ok"`, `registry_version=1`; TCP register on same server → `registry_version=2`; both via `beava_server` fixture (live binary) | pass |
| 5 | `app.validate(*descriptors)` runs zero-network-IO validation returning `list[ValidationError]` | `test_c5_validate_no_io` | Missing upstream → `list[ValidationError]` with `kind="missing_upstream"`; `GET /registry` version unchanged before and after `validate()`; valid batch → `[]` | pass |
| 6 | End-to-end smoke: register 2 events + 1 table once via `bv.App('http://...')` and once via `bv.App('tcp://...')` — identical registry state verifiable via `curl /registry` | `test_c6_identical_registry_state_across_transports` | Two independent embedded servers; HTTP registers SC6EventA+SC6EventB+SC6Table; TCP registers same DAG; `GET /registry` JSON bodies equal after stripping `_dev_only` | pass |
| 7 | SDK TCP client round-trips `ping` successfully; connection reuse across multiple `register`/`validate` calls on one App instance | `test_c7_tcp_ping_and_connection_reuse` | Three pings return `server_version` + `registry_version`; `id(app._transport._socket)` stable across ping→ping→register→ping sequence | pass |

## Bonus coverage

| Area | Test | Status |
|------|------|--------|
| Embed mode (`bv.App()` no URL) spawns subprocess, registers, subprocess reaped | `test_extra_embed_mode_end_to_end` | pass |

## Gate outputs

```text
cd python && python -m pytest tests/test_phase3_smoke.py -v
  → 8 passed, 0 failed

cd python && python -m pytest tests/
  → 113 passed, 0 failed   (Plans 01..06 all green together)

cd python && python -m ruff check beava/ tests/
  → All checks passed!

cd python && python -m mypy beava/
  → Success: no issues found in 12 source files

cargo test --workspace
  → 256 passed, 0 failed
```

## Red-green commit trace

| Commit | Type | Message |
|--------|------|---------|
| `4e28fd7` | RED | `test(03-06): Phase 3 smoke tests + README placeholder + VERIFICATION stub pending` |
| (green — after this VERIFICATION.md) | GREEN | `feat(03-06): Phase 3 acceptance gate — all 7 ROADMAP criteria proven end-to-end + README + VERIFICATION` |

Red commit: all 8 test bodies are `pytest.fail("red stub — implement in Plan 03-06 Task 1.b")`; `pytest` exits 1; `grep -c` returns 8.

Green commit: every stub replaced with real assertions; `grep -c` returns 0; all 8 tests PASSED.

## Deviations from plan

None. All tests passed on the first green run.

The only minor adjustment: `from __future__ import annotations` was intentionally omitted from `test_phase3_smoke.py`. That import stringifies all annotations, which breaks `@bv.event` function-form decorators that read `param.annotation` directly (same issue documented in Plan 03-05). Not a deviation — the plan notes are consistent with the fix.

Import ordering was auto-fixed by `ruff --fix` (I001 violation; `httpx` alphabetically before `pytest`).

## Files created / modified

| File | Action | Role |
|------|--------|------|
| `python/tests/test_phase3_smoke.py` | created | 8 smoke tests covering SC1..SC7 + embed |
| `python/README.md` | created | SDK quickstart: HTTP, TCP, embed, validate, expression DSL |
| `.planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-VERIFICATION.md` | created | This file |

## Gaps

None.

## Human-action items

None. Phase 3 closes with all 7 ROADMAP criteria proven. Phase 4 (stateless ops + expression evaluator server-side) can proceed; it depends on the `bv.col` canonical grammar and `@bv.event`/`@bv.table`/`bv.App` SDK surface that Phase 3 provides.
