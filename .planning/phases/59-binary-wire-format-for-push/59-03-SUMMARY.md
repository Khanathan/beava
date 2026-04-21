---
phase: 59
plan: 03
subsystem: python SDK / client handshake
tags:
  - tpc-perf-09
  - wave-3
  - python-sdk
  - op-negotiate-wire-format
  - backward-compat-fallback
  - version-bump
requires:
  - phase-59-wave-2 (OP_NEGOTIATE_WIRE_FORMAT server side; commit e64b85c)
  - 59-CONTEXT.md D-B1 (opcode surface) + D-B4 (opt-in env) + D-E4 (pre-59 fallback)
provides:
  - python/beava/_protocol.py: OP_NEGOTIATE_WIRE_FORMAT, WIRE_BINARY_PASSTHROUGH, WIRE_VERSION_TAG_CLIENT
  - python/beava/_client.py: BeavaClient.negotiate_wire_format() + server_capability_bits / server_version_tag attributes + BEAVA_WIRE_NEGOTIATE env opt-in
  - python/pyproject.toml version 0.1.0 → 0.2.0 (minor; net-additive)
  - python/tests/test_wire_negotiate.py (8 pytest cases — all GREEN)
  - tests/python_sdk_pre_59_server_fallback.rs (3 Rust integration tests — all GREEN)
affects:
  - Wave 4 (59-04) perf gate — unchanged (SDK negotiate is a connect-time
    cost, not a per-event cost; bench harness does not call it).
  - Future Python SDK minor bumps may flip BEAVA_WIRE_NEGOTIATE default
    from off to on once the ecosystem has updated to Phase-59+ servers.
  - REQUIREMENTS.md TPC-PERF-09 row marked progressively closer to
    complete (SC-5 ship-gate passes; see Wave 4 VERIFICATION).
tech-stack:
  added: []
  patterns:
    - "Graceful pre-server-version fallback via (0, 0) sentinel — same idiom as OCSP/TLS 'unknown extension' responses"
    - "os.environ env-opt-in trigger at __init__ end (Twelve-Factor config-via-env)"
    - "Lazy import inside method body to keep module-load cost low (established beava._client pattern)"
key-files:
  created:
    - python/tests/test_wire_negotiate.py
    - tests/python_sdk_pre_59_server_fallback.rs
    - .planning/phases/59-binary-wire-format-for-push/59-03-SUMMARY.md
  modified:
    - python/beava/_protocol.py (+3 constants, ~25 LOC)
    - python/beava/_client.py (+STATUS_OK import, +negotiate_wire_format method, +env opt-in, +server_capability_bits/server_version_tag attrs, ~60 LOC)
    - python/pyproject.toml (0.1.0 → 0.2.0)
decisions:
  - "Class name deviation: plan referenced `TallyClient.negotiate_wire_format()` — actual class is `BeavaClient` (module beava._client). The 'Tally' prefix is the repo codename; public product is Beava (per MEMORY.md project_beava_product). Renamed method location to BeavaClient; no behavior impact. Deviation Rule 3 (blocking naming mismatch)."
  - "Python unit tests live in python/tests/test_wire_negotiate.py using the existing pytest layout (plan said 'if the repo has a pytest suite'; it does — see pyproject.toml [tool.pytest.ini_options] testpaths=['tests']). 8 tests covering: 3 constant asserts + 3 mock-server scenarios (phase-59+, pre-59, truncated OK) + 2 env-flag cases (on triggers auto-negotiate, off does not). All 8/0/0 PASS on Python 3.13."
  - "Test 3 in python_sdk_pre_59_server_fallback.rs renamed + rewritten. Plan said 'after STATUS_ERROR a subsequent valid OP_PUSH on the SAME connection succeeds'; empirically Rust server tears down the connection after parse_command error (src/server/tcp.rs:1323 — 'send error response and close connection, matching the original inline error handling behavior'). This is a long-standing Phase-50+ invariant. Rewrote the test as status_error_is_framed_reconnect_restores_push_path: validates the STATUS_ERROR is fully framed (the Python SDK's _recv_frame depends on this to stay byte-aligned), then opens a fresh connection and demonstrates the valid push succeeds. This is exactly what Python's auto-reconnect in _client.py:302-305 does — the end-to-end D-E4 safety net works via reconnect, not same-connection persistence. Deviation Rule 1 (plan claim diverged from server behavior)."
  - "Rust-side STATUS_ERROR teardown policy left UNCHANGED. Changing it would be a Rule-4 architectural change (alters every opcode's error semantics, not just OP_NEGOTIATE_WIRE_FORMAT's). Left for future work; D-E4 contract holds via Python's auto-reconnect path."
  - "BEAVA_WIRE_NEGOTIATE default off per D-B4 — verified empirically: test_default_off_no_auto_negotiate opens a BeavaClient against port 1 (ECONNREFUSED) with the flag UNSET and asserts the constructor returns without raising and without any socket activity (server_capability_bits stays None). Flipped env on in test_env_opt_in_triggers_auto_negotiate_on_connect; constructor auto-calls negotiate and caches the bits."
  - "Python version bump to 0.2.0 (not 0.1.1) — matches semver minor-version rule for net-additive API surface (D-B4 'bump minor; no breaking change'). Existing 0.1.0 callers work unchanged; new constants + method appear in 0.2.0+."
metrics:
  duration: ~20min
  completed: 2026-04-21
  tasks: 2
  commits: 1  # single commit for both Python + Rust test work
  files_created: 3
  files_modified: 3
  python_tests_added: 8
  rust_tests_added: 3
  lib_tests_passing: "825/0/35"  # Wave 2 unchanged
  version_bump: "0.1.0 → 0.2.0"
---

# Phase 59 Plan 03: Python SDK Negotiate Handshake + Pre-59 Fallback Summary

Wave 3 wires the Python SDK (beava v0.2.0) to speak the Phase 59
OP_NEGOTIATE_WIRE_FORMAT handshake Wave 2 landed. The emit path is
unchanged (Python has emitted binary TYPE_* bodies since Phase 11) —
Wave 3 is purely net-additive surface for negotiation + a safety-net
fallback path for pre-Phase-59 servers.

## Handshake Round-Trip Behavior Table

| Server type              | `BeavaClient.negotiate_wire_format()` behavior                            | Cached attrs                                       |
|--------------------------|---------------------------------------------------------------------------|----------------------------------------------------|
| Phase 59+ server         | sends OP_NEGOTIATE (6-byte body) → STATUS_OK + server body → parses       | `server_capability_bits=1`, `server_version_tag=2` |
| Pre-59 server            | sends OP_NEGOTIATE → STATUS_ERROR "unknown opcode: 0x18" → fallback        | `server_capability_bits=0`, `server_version_tag=0` |
| Server returns truncated | STATUS_OK but body length < 6 → defensive fallback (mirrors pre-59 shape) | `server_capability_bits=0`, `server_version_tag=0` |
| Socket-level error       | OSError / ConnectionError propagates to caller                            | attrs stay at prior value (or None if never set)   |

## BEAVA_WIRE_NEGOTIATE Env Opt-in

```python
# Default (env unset or != "1"):
c = BeavaClient("host", 6400)
assert c.server_capability_bits is None  # NO connection attempt at construct time
assert c.server_version_tag is None

# Opt-in:
os.environ["BEAVA_WIRE_NEGOTIATE"] = "1"
c = BeavaClient("host", 6400)  # auto-calls negotiate in __init__
assert c.server_capability_bits == 1    # cached from server response
assert c.server_version_tag == 2
```

## Grep-Invariant Evidence

```
$ grep -c "OP_NEGOTIATE_WIRE_FORMAT" python/beava/_protocol.py
2  (const def + doc-comment reference)

$ grep -c "negotiate_wire_format" python/beava/_client.py
4  (method def + 1 call site in __init__ + 1 self.* doc + 1 _negotiate_wire_format reference)

$ grep -c "BEAVA_WIRE_NEGOTIATE" python/beava/_client.py
1  (env-opt-in check in __init__)

$ grep -cE '^version = "0\.' python/pyproject.toml
1  (= 0.2.0 exactly)

$ grep -c "server_capability_bits" python/beava/_client.py
6  (attr init + 4 assignment sites + 1 doc ref)

$ grep -c "WIRE_BINARY_PASSTHROUGH" python/beava/_protocol.py
1  (const def)

$ grep -c "WIRE_VERSION_TAG_CLIENT" python/beava/_protocol.py
1  (const def)
```

## Test Disposition Matrix

| Test                                                                   | Wave 2 status           | Wave 3 status                       |
|------------------------------------------------------------------------|-------------------------|-------------------------------------|
| `python/tests/test_wire_negotiate.py` (new, 8 cases)                   | N/A                     | **GREEN** 8/0/0                      |
| `tests/python_sdk_pre_59_server_fallback.rs` (new, 3 cases)            | N/A                     | **GREEN** 3/0/0                      |
| `tests/wire_negotiation_handshake` (Wave 2)                            | GREEN 1/0/0             | GREEN 1/0/0 preserved                |
| `tests/binary_push_bytes_passthrough` (Wave 1)                         | GREEN 1/0/0             | GREEN 1/0/0 preserved                |
| `tests/json_over_tcp_still_accepted` (Wave 1 D-B3 guard)               | GREEN 1/0/0             | GREEN 1/0/0 preserved                |
| `tests/http_push_still_works`                                          | GREEN                   | GREEN preserved                      |
| `tests/tcp_ingest_routing`                                             | GREEN                   | GREEN preserved                      |
| `tests/replica_ingest_routing`                                         | GREEN                   | GREEN preserved                      |
| `cargo test --release --lib`                                           | 825/0/35                | **825/0/35** unchanged (no src/ changes) |
| `scripts/verify-no-tcp-json-reserialize.sh`                            | exit 0                  | exit 0                              |
| `scripts/verify-no-{dashmap,statestore,legacy-push,retraction-metrics}.sh` | exit 0             | exit 0                              |

## Deviations from Plan

### Rule 3 — Class name mismatch: TallyClient → BeavaClient

- **Found during:** Task 1 method placement.
- **Issue:** Plan repeatedly referenced `TallyClient.negotiate_wire_format()`, `TallyClient(...)`, `TallyClient.push(...)`. The actual class in `python/beava/_client.py` is `BeavaClient`. The 'Tally' prefix is the repo codename; public product is Beava per MEMORY.md.
- **Fix:** Renamed method location + docstring + test references to `BeavaClient`. Zero behavior change.
- **Files modified:** python/beava/_client.py, python/tests/test_wire_negotiate.py.
- **Commit:** 921f04d.

### Rule 1 — Test 3 "same-connection persistence" not true of Rust server

- **Found during:** `cargo test --release --test python_sdk_pre_59_server_fallback` first run.
- **Issue:** Plan's Test 3 (`status_error_preserves_connection_for_subsequent_push`) asserted that after a STATUS_ERROR on an unknown opcode, a valid OP_PUSH on the SAME connection would succeed. Empirically the Rust server tears down the connection after parse_command error (src/server/tcp.rs:1319-1326 — "send error response and close connection, matching the original inline error handling behavior"). This is a Phase-50+ invariant that applies to EVERY opcode's parse errors, not Phase 59 specific.
- **Fix:** Rewrote test as `status_error_is_framed_reconnect_restores_push_path`. Asserts the STATUS_ERROR frame is fully-formed (length + status byte + non-empty body with "unknown opcode" keyword) — the Python SDK's `_recv_frame` depends on this framing to stay byte-aligned. Then opens a fresh TcpStream and demonstrates OP_PUSH succeeds there + events_total advances. This mirrors the Python SDK's actual behavior path: `_client.py:302-305` auto-reconnects on `ConnectionError`, so D-E4 holds via reconnect, not same-connection persistence.
- **Rationale not to flip server behavior:** Would be a Rule-4 architectural change (changes every opcode's error semantics, not just 0x18). Left for future work if a phase specifically needs it.
- **Files modified:** tests/python_sdk_pre_59_server_fallback.rs.
- **Commit:** 921f04d.

### Rule 2 — Pytest layout auto-discovered

- **Found during:** Task 1 §4 "check if repo has pytest suite".
- **Issue:** Plan said "if the repo has a pytest suite, add test_wire_negotiate.py". Repo DOES have one (python/pyproject.toml `[tool.pytest.ini_options] testpaths=["tests"]`).
- **Fix:** Added `python/tests/test_wire_negotiate.py` with 8 tests covering constants + 3 mock-server scenarios + 2 env-flag cases. All 8/0/0 PASS.
- **Files modified:** python/tests/test_wire_negotiate.py (new).
- **Commit:** 921f04d.

## Auth Gates Encountered

None — Wave 3 is SDK + integration tests. OP_NEGOTIATE is unauthenticated
(inherits OP_PUSH posture per Wave-2 T-59-02-04).

## Next Wave Handoff

### Wave 4 (plan 59-04) MUST

1. Extend `tests/profile_ingest.rs` (58-NEXT #1) to exercise a real TCP
   push path so `scripts/samply-probe-json-share.sh`'s coverage sentinel
   (floor 1.0%) passes.
2. Re-run `scripts/samply-probe-json-share.sh` — expect `JSON_SHARE_PCT ≤ 3.0`
   (D-D3) OR SENTINEL_FAILED → C3 human_needed escalation.
3. Run `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576 BEAVA_MAX_CONNS_PER_SHARD=1024 bash benchmark/fraud-pipeline/run_bench.sh`
   — expect aggregate EPS ≥ 1,514,095 (Phase 58 C1 × 1.10). Contingency
   ladder C0 → C1 (per-shard BytesMut pool) → C2 (inline decode) → C3
   (human_needed escalation mirroring Phase 56/57/58 precedent).
4. Verify p99 parity vs Phase 58's 30,632.5 µs median-of-p99 (±5%) for SC-4.
5. Write 59-PERF-GATE.md (mirror 58-PERF-GATE.md format) + 59-VERIFICATION.md
   (SC-1..SC-5 per-SC status) + update ROADMAP Phase 59 row to 5/5 +
   update STATE to advance to Phase 60 with accumulated-context entry.
6. Two close commits: `perf(59-W4): ...` + `docs(phase-59): ...`.

## Known Stubs

None — every surface Wave 3 introduced is fully wired:

- `BeavaClient.negotiate_wire_format()` sends a real TCP frame and parses
  the real response; caches real values. Verified against 3 mock-server
  shapes (Phase 59, pre-59, truncated OK).
- `BEAVA_WIRE_NEGOTIATE=1` env flag really triggers an auto-call at
  construct time; default-off really does not attempt any socket I/O
  during __init__.
- Rust `tests/python_sdk_pre_59_server_fallback.rs` boots a real server
  + shard threads and talks to it — not a mock.

## Threat Flags

None — plan `<threat_model>` T-59-03-01..T-59-03-04 covers all new surface:

- T-59-03-01 (pre-59 server spoofing server_bits) — accepted: attribute
  is advisory; Python emit is always binary post-Phase-11.
- T-59-03-02 (1-RTT negotiate latency) — accepted: once per connection.
- T-59-03-03 (server_capability_bits info disclosure) — accepted: non-secret.
- T-59-03-04 (MitM downgrade forcing no-negotiate mode) — accepted:
  server accepts binary either way; no amplification.

## Commits

| Task            | Commit    | Message                                                                     |
|-----------------|-----------|-----------------------------------------------------------------------------|
| Task 1 + Task 2 | `921f04d` | `feat(59-W3): Python SDK negotiate handshake + pre-59 fallback (TPC-PERF-09 D-B1/D-E4)` |

## Deferred Issues

1. Rust server's per-frame parse-error teardown policy — long-standing
   Phase-50+ invariant that prevents "same-connection persistence"
   (which plan Test 3 assumed). Filed informally here; D-E4 holds via
   Python's auto-reconnect. If a future phase needs per-frame
   error-tolerance on a specific opcode, that's a Rule-4 architectural
   decision and should be scoped separately.

## Self-Check

- [x] `python/beava/_protocol.py` contains `OP_NEGOTIATE_WIRE_FORMAT: int = 0x18` — **FOUND**
- [x] `python/beava/_protocol.py` contains `WIRE_BINARY_PASSTHROUGH: int = 1 << 0` — **FOUND**
- [x] `python/beava/_protocol.py` contains `WIRE_VERSION_TAG_CLIENT: int = 2` — **FOUND**
- [x] `python/beava/_client.py::BeavaClient.negotiate_wire_format` defined — **FOUND**
- [x] `python/beava/_client.py::BeavaClient.__init__` has BEAVA_WIRE_NEGOTIATE env check — **FOUND**
- [x] `python/pyproject.toml` version = "0.2.0" — **FOUND**
- [x] `python/tests/test_wire_negotiate.py` exists (8 tests) — **FOUND**
- [x] `tests/python_sdk_pre_59_server_fallback.rs` exists (3 tests) — **FOUND**
- [x] Commit `921f04d` present in git log — **FOUND**
- [x] `python3 -c 'from beava._protocol import OP_NEGOTIATE_WIRE_FORMAT, WIRE_BINARY_PASSTHROUGH, WIRE_VERSION_TAG_CLIENT'` succeeds — **VERIFIED**
- [x] `python3 -m pytest python/tests/test_wire_negotiate.py` → 8 passed — **VERIFIED**
- [x] `cargo test --release --test python_sdk_pre_59_server_fallback` → 3/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --lib` → 825/0/35 — **VERIFIED**
- [x] `cargo test --release --test wire_negotiation_handshake / binary_push_bytes_passthrough / json_over_tcp_still_accepted / http_push_still_works / tcp_ingest_routing / replica_ingest_routing` → all GREEN — **VERIFIED**
- [x] `bash scripts/verify-no-tcp-json-reserialize.sh` → exit 0 — **VERIFIED**
- [x] `bash scripts/verify-no-{dashmap,statestore,legacy-push,retraction-metrics}.sh` → all exit 0 — **VERIFIED**

## Self-Check: PASSED
