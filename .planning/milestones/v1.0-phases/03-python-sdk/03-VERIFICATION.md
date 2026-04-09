---
phase: 03-python-sdk
verified: 2026-04-09T17:10:00Z
status: passed
score: 5/5 must-haves verified
overrides_applied: 0
re_verification: false
---

# Phase 3: Python SDK Verification Report

**Phase Goal:** An ML engineer can define streams in Python using decorators, register them with the server, push events, and receive typed feature results — all without writing Rust or touching the wire protocol directly
**Verified:** 2026-04-09T17:10:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A Python script using @st.stream with count/sum/avg/derive operators can be registered and push events that return a typed FeatureResult object with named attribute access (features.tx_count_30m) | VERIFIED | 12 integration tests pass; `test_register_and_push` confirms attribute access returns correct values |
| 2 | @st.view with cross-stream derive expressions serializes correctly to JSON and registers successfully with the server | VERIFIED | JSON serialization confirmed programmatically; live server registration tested and passes |
| 3 | All operator classes (st.count, st.sum, st.avg, st.min, st.max, st.distinct_count, st.last, st.derive, st.lookup) serialize to valid JSON pipeline definitions | VERIFIED | All 9 operator `to_json()` outputs confirmed against expected schema; 42 operator tests pass |
| 4 | app.get(), app.set(), and app.mset() all work correctly against a running server with persistent connections | VERIFIED | Integration tests `test_get_features`, `test_set_features`, `test_mset_bulk` all pass. Connection is persistent with auto-reconnect. Note: implementation uses a single persistent connection (not a pool), which is the documented v1 design decision per 03-CONTEXT.md. |
| 5 | A conformance test verifies that the Python client's binary encoding matches the Rust server's expected wire format byte-for-byte | VERIFIED | `python/tests/test_protocol.py` has 26 byte-level conformance tests; `test_wire_conformance` in integration tests proves round-trip correctness |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `python/pyproject.toml` | Package config with hatchling build system | VERIFIED | Contains `hatchling`, `name = "tally"`, `requires-python = ">=3.10"` |
| `python/tally/__init__.py` | Full public API re-exports | VERIFIED | Exports all 9 operator aliases, FeatureResult, TallyError, ConnectionError, ProtocolError, stream, view, App, protocol constants |
| `python/tally/_types.py` | FeatureResult, TallyError hierarchy | VERIFIED | FeatureResult with __getattr__, __getitem__, to_dict, __contains__, __repr__; TallyError > ConnectionError, ProtocolError |
| `python/tally/_protocol.py` | Binary frame encoding/decoding for all 5 opcodes | VERIFIED | encode_frame, encode_string, encode_push, encode_get, encode_set, encode_mset, encode_register, parse_response, MAX_FRAME_SIZE (64MB) all present and correct |
| `python/tally/_operators.py` | All 9 operator descriptor classes | VERIFIED | Count, Sum, Avg, Min, Max, DistinctCount, Last, Derive, Lookup — all inherit OperatorBase, implement to_json() |
| `python/tally/_stream.py` | @stream decorator with StreamMeta metaclass | VERIFIED | StreamMeta collects operators from class body and bases (mixin support), sets _tally_features/_tally_key_field/_tally_stream_name/_tally_is_view, provides _to_register_json() |
| `python/tally/_view.py` | @view decorator restricted to derive/lookup | VERIFIED | Passes _is_view=True to StreamMeta; metaclass validates only Derive/Lookup allowed, raises TypeError at definition time |
| `python/tally/_client.py` | TallyClient TCP connection | VERIFIED | Lazy connect, auto-reconnect, frame I/O, configurable timeout, context manager, MAX_FRAME_SIZE enforcement |
| `python/tally/_app.py` | App class with all 5 command methods | VERIFIED | register, push, get, set, mset all present and wired to TallyClient + protocol encoding; returns FeatureResult from push/get |
| `python/tests/test_types.py` | Type tests | VERIFIED | 16 tests covering all FeatureResult behaviors and exception hierarchy |
| `python/tests/test_protocol.py` | Byte-level conformance tests | VERIFIED | 26 tests verifying exact byte output against Rust wire format |
| `python/tests/test_operators.py` | Operator JSON serialization tests | VERIFIED | 42 tests covering all 9 operators |
| `python/tests/test_stream.py` | Decorator and metaclass tests | VERIFIED | 17 tests covering @stream, mixin inheritance, validation |
| `python/tests/test_view.py` | View restriction tests | VERIFIED | 11 tests covering @view restrictions |
| `python/tests/test_client.py` | TCP client unit tests | VERIFIED | 9 tests with mock server |
| `python/tests/test_app.py` | App method tests | VERIFIED | 14 tests with mock server |
| `python/tests/conftest.py` | Server fixture | VERIFIED | Session-scoped fixture: cargo build, random ports, subprocess lifecycle, TCP readiness wait |
| `python/tests/test_integration.py` | End-to-end integration tests | VERIFIED | 12 tests covering all SDK-* requirements against live server |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `python/tally/_protocol.py` | `src/server/protocol.rs` | identical wire format (`struct.pack('>I')`, `struct.pack('>H')`) | VERIFIED | encode_frame uses struct.pack('>I'); encode_string uses struct.pack('>H'); byte-level tests confirm match |
| `python/tally/_operators.py` | `src/server/protocol.rs` RegisterRequest | `to_json()` output matches FeatureDefRequest fields | VERIFIED | All 9 operator to_json outputs produce dicts with name/type/field/window/expr keys matching Rust schema |
| `python/tally/_stream.py` | `python/tally/_operators.py` | isinstance check for OperatorBase | VERIFIED | `isinstance(attr_val, OperatorBase)` used in StreamMeta for feature collection |
| `python/tally/_app.py` | `python/tally/_client.py` | App uses TallyClient for TCP communication | VERIFIED | `self._client = TallyClient(host, port, timeout=timeout)` wired in App.__init__ |
| `python/tally/_app.py` | `python/tally/_stream.py` | register() calls cls._to_register_json() | VERIFIED | `definition = cls._to_register_json()` in App.register() |
| `python/tally/_app.py` | `python/tally/_types.py` | push/get return FeatureResult | VERIFIED | `return FeatureResult(data)` in both App.push() and App.get() |
| `python/tally/_client.py` | `python/tally/_protocol.py` | uses encode_frame and parse_response | VERIFIED | `from tally._protocol import encode_frame, MAX_FRAME_SIZE` in _client.py |
| `python/tests/conftest.py` | Tally server binary | subprocess.Popen starts cargo-built binary | VERIFIED | `subprocess.Popen([_BINARY_PATH], env=env, ...)` with TALLY_TCP_PORT/TALLY_HTTP_PORT |
| `python/tests/test_integration.py` | `python/tally/_app.py` | App class used in all tests | VERIFIED | All 12 integration tests use `app` fixture (st.App instance) |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `_app.py` App.push() | `data` dict from json.loads | `self._send(OP_PUSH, payload)` → TCP response from Rust server | Yes — Rust engine computes and returns feature map | FLOWING |
| `_app.py` App.get() | `data` dict from json.loads | `self._send(OP_GET, payload)` → TCP response from Rust server | Yes — Rust state store returns current features | FLOWING |
| `_client.py` send_command() | `(status, payload)` tuple | `self._recv_frame()` reads real bytes from socket | Yes — TCP recv from live server | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Package importable | `python3 -c "import tally as st; print(st.App)"` | `<class 'tally._app.App'>` | PASS |
| @stream decorator collects features | Python evaluation | stream_name, key_field, features dict all correctly set | PASS |
| All 9 operators serialize to JSON | Python evaluation | All 9 to_json() outputs match expected schema | PASS |
| Unit test suite | `cd python && python3 -m pytest tests/test_types.py ... -q` | 135 passed | PASS |
| Integration tests (all 12) | `cd python && python3 -m pytest tests/test_integration.py -x -v` | 12 passed in 0.41s | PASS |
| Full test suite | `cd python && python3 -m pytest tests/ -q` | 147 passed in 0.42s | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| SDK-01 | Plans 02, 04 | @st.stream decorator defines a stream with key field and feature declarations | SATISFIED | StreamMeta metaclass collects operators; 17 stream tests + integration tests |
| SDK-02 | Plans 02, 04 | @st.view decorator defines cross-stream views with derive expressions | SATISFIED | @view restricts to Derive/Lookup, validates at definition time; live server registration confirmed |
| SDK-03 | Plans 02, 04 | All operator classes serialize to JSON | SATISFIED | All 9 operators (count, sum, avg, min, max, distinct_count, last, derive, lookup) implement to_json() |
| SDK-04 | Plans 01, 03, 04 | TCP client with connection communicates via binary protocol | SATISFIED | TallyClient sends binary frames; 26 byte-level conformance tests; 12 integration tests pass |
| SDK-05 | Plans 03, 04 | app.push() sends event and returns typed feature results | SATISFIED | FeatureResult with attribute access; integration tests verify correct computed values |
| SDK-06 | Plans 03, 04 | app.get(), app.set(), app.mset() for read/write operations | SATISFIED | All three methods implemented and tested against live server |
| SDK-07 | Plans 03, 04 | app.register() sends pipeline definitions to server | SATISFIED | register() calls _to_register_json() and sends REGISTER command; server accepts and processes |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found | — | — | — | All modules are substantive implementations with no TODOs, stubs, or placeholders |

### Human Verification Required

None. All success criteria are verifiable programmatically via the 147-test suite (135 unit + 12 integration tests against a live Rust server).

### Gaps Summary

No gaps. All 5 ROADMAP success criteria are verified:

1. @stream with count/sum/avg/derive registers and pushes events returning typed FeatureResult — proven by 12 integration tests.
2. @view serializes to JSON and registers with server — serialization proven by unit tests; server registration confirmed by spot-check against live server.
3. All 9 operators serialize to valid JSON — 42 operator tests + programmatic verification.
4. get/set/mset work against a running server — integration tests prove all three. The connection is persistent (with auto-reconnect) per the v1 design decision in 03-CONTEXT.md ("single persistent TCP connection per App instance").
5. Wire format conformance tests exist and pass — 26 byte-level tests in test_protocol.py + test_wire_conformance integration test.

One notable design decision: the ROADMAP summary line mentions "connection pooling" but the CONTEXT.md explicitly documents a v1 decision to use a single persistent connection rather than a pool. This is intentional and acceptable for v1.

---

_Verified: 2026-04-09T17:10:00Z_
_Verifier: Claude (gsd-verifier)_
