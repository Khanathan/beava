---
phase: 11-fire-and-forget-push
plan: 02
status: complete
date: 2026-04-11
---

# Plan 11-02 — Python SDK binary encoder + fire-and-forget

## Outcome

Delivered PERF-01 (fire-and-forget `push()`) and PERF-02 (binary event payload) on the client side. Every public `App` method now drains pending server error frames via a non-blocking `select()` probe before doing its own work, so any error from a prior async push surfaces on the next call.

## Key files modified

- `python/tally/_protocol.py`
  - New constants: `OP_PUSH_ASYNC = 0x07`, `OP_FLUSH = 0x08`, `TYPE_NULL..TYPE_STR`
  - New pre-compiled `_U16`, `_I64`, `_F64` struct instances
  - New function `encode_push_binary(stream_name, event) -> bytes`
  - **Critical bool-before-int check** — `isinstance(value, bool)` tested before `isinstance(value, int)` so `True` encodes as `TYPE_BOOL`, not `TYPE_I64`
  - Strict i64-range enforcement and finite-float rejection mirroring the server decoder

- `python/tally/_client.py`
  - New imports: `select`, `STATUS_ERROR`
  - New `self._pending_error: ProtocolError | None` in `__init__`
  - New `drain_errors_nonblock()` — reads at most one frame; raises pending + server STATUS_ERROR frames
  - New `send_frame_no_recv(opcode, payload)` — auto-reconnects once on broken pipe

- `python/tally/_app.py`
  - `push()` rewritten as fire-and-forget: `drain → encode_push_binary → send_frame_no_recv(OP_PUSH_ASYNC, ...)`, returns `None`
  - `push_sync()` added: v1.1 semantics, uses the binary encoder
  - `flush()` added: `drain → _send(OP_FLUSH, b"")`
  - `drain_errors_nonblock()` called at the top of `register`, `push`, `push_sync`, `flush`, `get`, `mget`, `set`, `mset`

- `python/tests/test_protocol.py`
  - New imports: `OP_PUSH_ASYNC`, `OP_FLUSH`, `TYPE_*`, `encode_push_binary`
  - New `TestEncodePushBinary` class with 14 tests including bool-before-int guard, NaN/Inf rejection, i64 range check, utf-8 roundtrip

- `python/tests/test_client.py`
  - New `TestPhase11ClientPrimitives` class with 7 tests covering drain with `sock=None`, pending_error, OK frame discard, STATUS_ERROR raise, send_frame_no_recv byte roundtrip, and non-blocking guarantee

## Deviations

- **Old `encode_push` retained.** Plan said deletion preferred, but `python/tests/test_protocol.py` still uses the v1.1 JSON `encode_push` helper in legacy tests. Per the plan's explicit fallback ("If a test still uses it, leave it for Plan 05 to update"), I kept the old function. Plan 11-05 can delete it once those tests are rewritten.

- **Pytest unavailable in the execution environment.** The runtime has no pytest installed and no pip/uv to install it. I instead verified every new behavior with direct `python3 -c` smoke tests using the `socket.socketpair()` pattern from the plan. All 8 checks pass:
  - `encode_push_binary` round-trips across all 5 type tags
  - bool-before-int guard (`True` encodes as `TYPE_BOOL` with value byte `0x01`)
  - NaN, Infinity, i64-overflow, list, and dict all raise `ProtocolError`
  - utf-8 keys and values (`café`/`résumé`) round-trip
  - `drain_errors_nonblock` handles sock=None, pending_error, OK discard, STATUS_ERROR raise
  - `send_frame_no_recv` writes the exact frame bytes and does not block on recv
  - `App.push` has return annotation `None`
  - `App.push_sync`, `App.flush` exist
  The pytest files themselves are committed and ready to run in any environment that has pytest.

## Tests expected to break in `python/tests/test_app.py` (Plan 05 fixes)

Because `App.push` no longer returns a FeatureResult, any existing test in `python/tests/test_app.py` that does `features = app.push(...)` will fail when executed. Plan 11-05 rewrites these with `app.push_sync` (or `app.push` + `app.get`). This deviation is explicitly covered in the plan.

## Self-Check: PASSED

- [x] `encode_push_binary` exists, byte layout verified, bool-before-int guarded
- [x] `TallyClient.drain_errors_nonblock` and `TallyClient.send_frame_no_recv` exist and behave correctly under socketpair smoke tests
- [x] `App.push` returns None, `App.push_sync` and `App.flush` exist
- [x] All 8 public App entrypoints call drain at the top
- [x] Test files committed (ready to run under any pytest-equipped environment)
- [x] Git commit: `1ec9ec6` feat(11-02)
