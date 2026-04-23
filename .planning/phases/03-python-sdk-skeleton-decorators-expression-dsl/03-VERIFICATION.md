---
status: passed
phase: 03-python-sdk-skeleton-decorators-expression-dsl
verified: 2026-04-23T00:00:00Z
must_haves_total: 7
must_haves_verified: 7
human_verification: []
gaps: []
overrides_applied: 0
re_verification: false
---

# Phase 3: Python SDK Skeleton + Decorators + Expression DSL — Verification Report

**Phase Goal:** Ship the user-facing Python SDK that compiles decorators + expression DSL into the REGISTER JSON the server accepts. SDK supports both transports via URL scheme (`http://` for HTTP/JSON, `tcp://` for framed TCP) — Phase 3 exercises both against the Phase 2.5 server. Dogfood the DSL from Phase 3 onwards; curl remains the language-agnostic escape hatch.

**Verified:** 2026-04-23
**Status:** PASSED
**Re-verification:** No — initial independent verification

---

## Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| SC1 | `@bv.event` class + function forms extracts schema and registers event descriptor | VERIFIED | `isinstance(TxEvent, EventSource)`, schema field py_types correct, `event_time_field` detected. Function form: `isinstance(CheckoutDerivation, EventDerivation)`, `_upstreams == ["TxEvent"]`. `test_c1_event_decorator_both_forms` passes. |
| SC2 | `@bv.table(key=..., ttl=...)` class + function forms work; key validation at decoration time | VERIFIED | `UserProfileTable._primary_key == ["user_id"]`; TTL `7d` converts to `604_800_000` ms; function form yields `TableDerivation`; bare `@bv.table` raises `TypeError`. `test_c2_table_decorator_both_forms` passes. |
| SC3 | `bv.col("x") > 100` expression produces expected `to_expr_string()` canonical form | VERIFIED | `(bv.col("amount") > 100).to_expr_string() == "(amount > 100)"`. `((a > 0) & (b < 5)).to_expr_string() == "((a > 0) and (b < 5))"`. All operator forms tested in `test_c3_col_canonical_form` and 14 unit tests in `test_col.py`. |
| SC4 | `app.register(*descriptors)` topo-sorts DAG, detects cycles, validates schemas, dispatches to HTTP or TCP based on URL scheme, receives `registry_version` | VERIFIED | HTTP → `status="ok"`, `registry_version=1`; TCP → `registry_version=2` on same server. Cycle + missing_upstream detected before wire I/O. `test_c4_register_both_transports` passes against live Rust binary. |
| SC5 | `app.validate(*descriptors)` runs zero-network-IO validation returning `list[ValidationError]` | VERIFIED | Missing upstream → `ValidationError(kind="missing_upstream")`; `GET /registry` version unchanged before/after `validate()`; valid batch → `[]`. `test_c5_validate_no_io` passes. |
| SC6 | End-to-end smoke: spawn TestServer (both ports), register 2 events + 1 table via `bv.App('http://...')` AND via `bv.App('tcp://...')` — identical registry state verifiable via `GET /registry` | VERIFIED | Two independent embedded servers; HTTP registers SC6EventA+SC6EventB+SC6Table; TCP registers same DAG; `GET /registry` JSON bodies equal after stripping `_dev_only`. `test_c6_identical_registry_state_across_transports` passes. |
| SC7 | SDK TCP client round-trips `ping` successfully; connection reuse across multiple `register`/`validate` calls on one App instance | VERIFIED | Three pings return `server_version` + `registry_version`; `id(app._transport._socket)` stable across ping→ping→register→ping. `test_c7_tcp_ping_and_connection_reuse` passes. |

**Score:** 7/7 truths verified

---

## Test Gates (independently re-run)

```
cd /Users/petrpan26/work/tally/python && python -m pytest -q
  → 114 passed in 0.81s

cd /Users/petrpan26/work/tally/python && python -m pytest tests/test_phase3_smoke.py -v
  → 8 passed in 0.43s (test_c1..test_c7 + test_extra_embed_mode_end_to_end)

cd /Users/petrpan26/work/tally/python && python -m ruff check beava/ tests/
  → All checks passed!

cd /Users/petrpan26/work/tally/python && python -m mypy beava/
  → Success: no issues found in 12 source files
```

---

## Required Artifacts

| Artifact | Min Lines | Actual | Status | Wired | Data Flows |
|----------|-----------|--------|--------|-------|-----------|
| `python/beava/__init__.py` | — | 42 lines | VERIFIED | Imports all submodules | N/A |
| `python/beava/_types.py` | — | exists | VERIFIED | Re-exported from `__init__` | N/A |
| `python/beava/_errors.py` | — | exists | VERIFIED | Re-exported from `__init__` | N/A |
| `python/beava/_col.py` | 180 | 404 lines | VERIFIED | `from ._col import col, Col` in `__init__` | N/A |
| `python/beava/_schema.py` | 120 | 145 lines | VERIFIED | Used by `_events.py`, `_tables.py` | N/A |
| `python/beava/_events.py` | 180 | 336 lines | VERIFIED | `from ._events import event` in `__init__` | N/A |
| `python/beava/_tables.py` | 130 | 325 lines | VERIFIED | `from ._tables import table` in `__init__` | N/A |
| `python/beava/_wire.py` | 130 | 241 lines | VERIFIED | Used by `_transport.py` | N/A |
| `python/beava/_transport.py` | 220 | 338 lines | VERIFIED | `from ._transport import parse_url_to_transport` in `__init__` | N/A |
| `python/beava/_embed.py` | 130 | 196 lines | VERIFIED | Used by `_transport.py` for embed mode | N/A |
| `python/beava/_validate.py` | 160 | 430 lines | VERIFIED | Used by `_app.py` | N/A |
| `python/beava/_app.py` | 160 | 214 lines | VERIFIED | `from ._app import App` in `__init__` | N/A |
| `python/tests/test_phase3_smoke.py` | 200 | 400 lines | VERIFIED | Imports `beava as bv` + `beava_server` fixture | All 8 tests exercise live paths |
| `python/README.md` | ~60 | 73 lines | VERIFIED | Contains `import beava as bv` and `with bv.App(` | N/A |
| `.planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-VERIFICATION.md` | — | this file | VERIFIED | Maps all 7 criteria to test functions | N/A |

---

## Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `python/beava/__init__.py` | `_types.py`, `_errors.py` | `from ._types import Optional, Field` + `from ._errors import ...` | WIRED |
| `python/beava/__init__.py` | `_col.py` | `from ._col import col, Col` | WIRED |
| `python/beava/__init__.py` | `_events.py`, `_tables.py` | `from ._events import event` + `from ._tables import table` | WIRED |
| `python/beava/__init__.py` | `_transport.py` | `from ._transport import parse_url_to_transport` | WIRED |
| `python/beava/__init__.py` | `_app.py` | `from ._app import App` | WIRED |
| `python/beava/_app.py` | `_validate.py`, `_transport.py` | `from beava._validate import topo_sort, validate_descriptors` + `from beava._transport import Transport, parse_url_to_transport` | WIRED |
| `python/beava/_transport.py` | `_wire.py` | `from beava._wire import CT_JSON, MAX_FRAME_BYTES, OP_PING, OP_REGISTER, encode_frame, parse_register_response, read_frame` | WIRED |
| `python/beava/_transport.py` | `_embed.py` | `from beava._embed import spawn_embedded_server` (in `parse_url_to_transport`) + `from beava._embed import teardown_process` (in `EmbedTransport.close`) | WIRED |
| `python/tests/test_phase3_smoke.py` | `beava_server` fixture | `def test_c4_register_both_transports(beava_server: tuple[str, str])` | WIRED |

---

## Data-Flow Trace (Level 4)

All dynamic artifacts (smoke tests) verified as passing against a live Rust binary. Registration data flows:

- `@bv.event class TxEvent` → `TxEvent._to_register_json()` produces valid JSON → `App.register(TxEvent)` → `transport.send_register(payload_bytes)` → server 200 → `registry_version=1` returned
- Both HTTP and TCP paths independently verified in `test_c6_identical_registry_state_across_transports` with `GET /registry` comparison confirming identical state

---

## Behavioral Spot-Checks

| Behavior | Result | Status |
|----------|--------|--------|
| `python -m pytest tests/test_phase3_smoke.py -v` → 8 passed | 8/8 passed in 0.43s | PASS |
| `python -m pytest -q` (full suite) | 114 passed in 0.81s | PASS |
| `bv.col("x") > 100).to_expr_string()` | `(x > 100)` | PASS |
| `ValidationError.__str__` | `[cycle] A.b: m` | PASS |
| `encode_frame(OP_PING, CT_JSON, b'')` | `b'\x00\x00\x00\x03\x00\x00\x01'` | PASS |
| `parse_url_to_transport('tcp://localhost:7380').port` | `7380` | PASS |
| `bv.App('http://placeholder').validate(T, U) == []` | `[]` | PASS |

---

## Requirements Coverage

| Requirement | Plan | Description | Status |
|-------------|------|-------------|--------|
| SDK-DEC-01 | 03-03 | `@bv.event` class form extracts schema | SATISFIED — `test_event_class_form_basic`, `test_c1_event_decorator_both_forms` |
| SDK-DEC-02 | 03-03 | `@bv.event` accepts `keep_events_for` / `tolerate_delay` duration strings | SATISFIED — `test_event_duration_options` |
| SDK-DEC-03 | 03-03 | `@bv.event` function form resolves upstreams | SATISFIED — `test_event_function_form`, `test_c1_event_decorator_both_forms` |
| SDK-DEC-04 | 03-03 | `@bv.table(key=...)` validates key at decoration | SATISFIED — `test_table_key_required`, `test_c2_table_decorator_both_forms` |
| SDK-DEC-05 | 03-03 | `@bv.table` function form | SATISFIED — `test_table_function_form`, `test_c2_table_decorator_both_forms` |
| SDK-DEC-06 | 03-01 | Schema extraction supports 6 field types, rejects unsupported at decoration | SATISFIED — `test_field_type_mapping`, `test_unsupported_field_type_errors_at_decoration` |
| SDK-DEC-07 | 03-01 | `bv.Optional[T]` + `bv.Field(desc=..., default=...)` | SATISFIED — `test_optional_produces_marker`, `test_field_stores_metadata` |
| SDK-DEC-08 | 03-03 | `event_time` optional; if declared must be `int` or `datetime` | SATISFIED — `test_event_time_type_invalid`, `test_event_without_event_time_field`, devex-first per CONTEXT D-07 |
| SDK-DEC-09 | 03-03 | `@bv.event` accepts `dedupe_key` + `dedupe_window` | SATISFIED — `test_event_dedupe_options`, `test_event_dedupe_key_must_be_in_schema` |
| SDK-COL-01 | 03-02 | `bv.col("field")` + arithmetic ops | SATISFIED — `test_arithmetic_parenthesized` |
| SDK-COL-02 | 03-02 | Comparison operators | SATISFIED — `test_comparison_parenthesized` |
| SDK-COL-03 | 03-02 | Boolean combinators `& | ~` | SATISFIED — `test_boolean_combinators_emit_keywords` |
| SDK-COL-04 | 03-02 | `.isnull()` → `(x == null)` | SATISFIED — `test_isnull` |
| SDK-COL-05 | 03-02 | `.cast("float")` → `cast(x, float)` | SATISFIED — `test_cast` |
| SDK-COL-06 | 03-02 | `.to_expr_string()` canonical parenthesized form | SATISFIED — 14 tests in `test_col.py`, `test_c3_col_canonical_form` |
| SDK-COL-07 | DEFERRED | Schema-reference resolution (needs server-side evaluator) | DEFERRED to Phase 4 per ROADMAP |
| SDK-COL-08 | 03-02 | `infer_output_type(lhs, rhs, op)` | SATISFIED — `test_infer_output_type_*` (3 tests) |
| SDK-APP-01 | 03-05 | `bv.App(url)` + context manager lifecycle | SATISFIED — `test_app_context_manager_http`, `test_app_context_manager_tcp` |
| SDK-APP-02 | 03-05 | `app.register(*descriptors)` topo-sort + dispatch + version | SATISFIED — `test_app_register_http_success`, `test_c4_register_both_transports` |
| SDK-APP-03 | 03-05 | `app.validate(*descriptors)` zero-network-IO | SATISFIED — `test_app_validate_returns_list_without_network_io`, `test_c5_validate_no_io` |
| SDK-APP-15 | 03-01 | `ValidationError` structure + `__str__` format | SATISFIED — `test_validation_error_str_repr`, `test_validation_error_is_frozen_dataclass` |
| SDK-WIRE-01 | 03-04 | HTTP transport via httpx | SATISFIED — `test_http_transport_register_success` |
| SDK-WIRE-02 | 03-04 | Framed TCP transport + strict-FIFO + connection reuse | SATISFIED — `test_tcp_transport_*` (6 tests), `test_c7_tcp_ping_and_connection_reuse` |
| SDK-WIRE-03 | 03-04 | URL-scheme dispatch + embed mode | SATISFIED — `test_parse_url_to_transport_*` (6 tests), `test_extra_embed_mode_end_to_end` |

**Deferred (1):** SDK-COL-07 explicitly deferred to Phase 4 per ROADMAP.md ("SDK-COL-07 (schema-reference resolution) moved to Phase 4 because it requires the server-side expression evaluator").

---

## Anti-Patterns Found

None found. Scanned key implementation files for TODO/FIXME/placeholder patterns, empty implementations, and hardcoded stub patterns.

Notable stub that is intentional and documented:
- `schema_mismatch` rule in `_validate.py` is a Phase 3 no-op placeholder; comment clearly states "Phase 4 will walk each derivation's _ops chain here." This is the correct behavior — ops are always empty in Phase 3.

---

## TDD Discipline Evidence

All 6 plans executed strict red-then-green discipline. Commit pairs (verified in git log):

| Plan | Red Commit | Green Commit |
|------|-----------|-------------|
| 03-01 | `7c59e00` `test(03-01): failing tests for errors, types, and package exports` | `d95d8c0` `feat(03-01): beava package skeleton...` |
| 03-02 | `e29b042` `test(03-02): failing tests for bv.col AST grammar + type inference` | `d67a34f` `feat(03-02): bv.col expression DSL...` |
| 03-03 | `878956c` `test(03-03): failing tests for @bv.event + @bv.table decorators...` | `32bd6f3` `feat(03-03): @bv.event + @bv.table decorators...` |
| 03-04 | `7ea5db4` `test(03-04): failing tests for wire framing, HTTP transport...` | `7a22148` `feat(03-04): wire codec + HTTP transport + TCP transport + embed mode` |
| 03-05 | `166c4c3` `test(03-05): failing tests for validate + App client...` | `6a04e74` `feat(03-05): bv.App client — register + validate + DAG topo-sort...` |
| 03-06 | `4e28fd7` `test(03-06): Phase 3 smoke tests + README placeholder...` | `913be3d` `feat(03-06): Phase 3 acceptance gate — all 7 ROADMAP criteria proven end-to-end` |

All red commits produce failing tests (ModuleNotFoundError or pytest.fail stubs). All green commits produce passing tests. Pattern is strict and unambiguous.

---

## CONTEXT.md Decisions Compliance

| Decision | Status |
|----------|--------|
| D-01: Clean-room implementation (no v1 copy) | COMPLIANT — `_events.py` not `_stream.py`; all modules are fresh implementations |
| D-02: `@bv.event` only, no `@bv.stream` alias | COMPLIANT — `hasattr(bv, 'stream') == False` confirmed |
| D-03: `httpx>=0.27,<1` + stdlib socket only | COMPLIANT — `pyproject.toml` has exactly `"httpx>=0.27,<1"` as only runtime dep |
| D-04: URL scheme dispatches transport | COMPLIANT — `http://`→`HttpTransport`, `tcp://`→`TcpTransport` confirmed |
| D-05: Sync-only `bv.App` in Phase 3 | COMPLIANT — `hasattr(bv, 'AsyncApp') == False` confirmed |
| D-06: validate-first register (zero wire I/O on local failure) | COMPLIANT — `App.register` calls `validate_descriptors` before any transport call; raises `RegistrationError` with `.errors` list |
| D-07: stdlib-only schema extraction | COMPLIANT — `_schema.py` uses `inspect.signature` + `typing.get_type_hints`; no pydantic/attrs |
| D-08: Canonical grammar locked (every binary op parenthesized) | COMPLIANT — `_BinOp.to_expr_string()` is the single code path: `f"({left} {op} {right})"` |
| D-09: subprocess fixture pattern | COMPLIANT — `beava_binary` (session) + `beava_server` (function) fixtures in `conftest.py` |
| D-10: embed mode binary discovery + spawn | COMPLIANT — 4-step order in `discover_binary()`; `spawn_embedded_server` reads stdout JSON for bind events |
| D-11: `ValidationError` frozen dataclass + `RegistrationError` with `.errors` | COMPLIANT — `ValidationError.__dataclass_params__.frozen == True`; `str(e) == "[kind] path: message"` |

---

## Deferred Items

| Item | Addressed In | Evidence |
|------|-------------|---------|
| SDK-COL-07: Expression validation at registration time (field references resolve to schema fields) | Phase 4 | ROADMAP.md Phase 3 requirements section: "SDK-COL-07 (schema-reference resolution) moved to Phase 4 because it requires the server-side expression evaluator." Phase 4 requirements include "SDK-COL-07 (schema-reference resolution, moved from Phase 3 because the expression evaluator lands here)." |

---

## Human Verification Required

None. All success criteria are verifiable programmatically and have been verified against a live Rust `beava` binary.

---

## Summary

Phase 3 goal achieved. All 7 ROADMAP Phase 3 success criteria are proven end-to-end:

1. The Python SDK is a complete, importable package with all public APIs wired
2. `@bv.event` (class + function form), `@bv.table` (class + function form) produce correct descriptor JSON matching Phase 2 wire contract
3. `bv.col(...)` expression DSL emits canonical parenthesized grammar (D-08 invariant enforced at a single code path)
4. `bv.App(url)` with HTTP and TCP URL schemes both register correctly against a live Rust server, receiving `registry_version`
5. `app.validate(...)` runs zero-network-IO local DAG checks returning `list[ValidationError]`
6. Two independent servers both end up in identical registry state after registering the same 2-event + 1-table DAG via HTTP and TCP respectively
7. TCP ping round-trips and socket identity is stable across ping/register/ping sequence (connection reuse)

**Bonus:** embed mode (`bv.App()` with no URL) spawns a local Rust subprocess, registers, and tears down cleanly.

**Gate outputs:**
- `python -m pytest -q` → 114 passed (0 failed, 0 skipped)
- `python -m ruff check beava/ tests/` → All checks passed
- `python -m mypy beava/` → Success: no issues found in 12 source files
- TDD discipline: 6 strict red-then-green commit pairs, all verified in git log

Phase 4 (stateless ops + expression evaluator) can proceed.

---

_Verified: 2026-04-23_
_Verifier: Claude (gsd-verifier) — independent check against codebase, not trusting SUMMARY.md claims_
