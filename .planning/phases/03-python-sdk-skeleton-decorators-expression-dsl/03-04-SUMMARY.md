---
phase: "03"
plan: "03-04"
subsystem: python-sdk
tags: [python-sdk, transport, http, tcp, framing, embed, subprocess, wire-codec]
completed: "2026-04-23T05:55:00Z"
duration_minutes: 25

dependency_graph:
  requires:
    - python/beava package (from 03-01)
    - beava._errors (RegistrationError, BinaryNotFoundError) (from 03-01)
    - Phase 2.5 TCP wire server (crates/beava-core/src/wire.rs frame format)
    - Phase 2 HTTP register endpoint (POST /register JSON contract)
  provides:
    - python/beava/_wire.py (encode_frame, decode_frame, read_frame, Frame dataclass,
      OP_PING/OP_REGISTER/OP_ERROR_RESPONSE/CT_JSON/CT_MSGPACK constants,
      FrameTooLarge/IncompleteFrame exceptions, parse_register_response)
    - python/beava/_transport.py (Transport base, HttpTransport, TcpTransport,
      EmbedTransport, parse_url_to_transport)
    - python/beava/_embed.py (discover_binary, spawn_embedded_server, teardown_process,
      4-step binary discovery order)
    - python/tests/conftest.py: beava_binary (session) + beava_server (function) fixtures
    - parse_url_to_transport exported from beava.__init__
  affects:
    - Plans 03-05, 03-06 (App class + acceptance smoke consume these transports + fixtures)
    - Phase 6 (push transport will extend TcpTransport)
    - Phase 12 (get/mget will add new opcodes to _wire.py constants)

tech_stack:
  added: []  # httpx was already added in 03-01; no new deps in this plan
  patterns:
    - TDD red-then-green commit pair (test: 7ea5db4 → feat: 7a22148)
    - Lazy TCP connect: socket opened on first use, reused for instance lifetime
    - Strict-FIFO: one in-flight request per connection (sendall + blocking read)
    - struct.pack(">IHB", length, op, ct) matches Rust's put_u32/put_u16/put_u8
    - threading.Event + daemon reader thread for subprocess stdout parsing
    - Binary logs to STDOUT (not stderr) — fixture reads proc.stdout, not proc.stderr
    - --config /dev/null to start beava with defaults when no beava.yaml in CWD

key_files:
  created:
    - python/beava/_wire.py
    - python/beava/_transport.py
    - python/beava/_embed.py
    - python/tests/test_wire_framing.py
    - python/tests/test_transport_http.py
    - python/tests/test_transport_tcp.py
    - python/tests/test_embed.py
  modified:
    - python/beava/__init__.py (replaced _AppStub block with parse_url_to_transport import)
    - python/tests/conftest.py (replaced placeholder with beava_binary + beava_server fixtures)

decisions:
  - "Binary JSON logs go to STDOUT not stderr — conftest and _embed.py must pipe stdout,
    not stderr; this was discovered during Task 1.b debugging"
  - "Pass --config /dev/null to binary so it starts with all-defaults regardless of CWD
    (default config path is ./beava.yaml which does not exist in python/)"
  - "parse_register_response uses typing.Any + cast for mypy strict compatibility;
    json.loads returns Any and mypy strict disallows returning Any from -> dict functions"
  - "EmbedTransport wraps TcpTransport + Popen; close() calls teardown_process after
    closing socket — clean lifecycle for embed mode"
  - "BEAVA_BINARY env var set to non-existent path raises BinaryNotFoundError immediately
    (no silent fallthrough); explicit set = explicit intent = must be valid"
  - "CT_MSGPACK constant defined in _wire.py as 0x02 (reserved Phase 6+) — SDK
    recognises it exists but never sends it in Phase 3"

metrics:
  tasks_completed: 2
  subtasks: "1.a (red) + 1.b (green)"
  tests_added: 31
  tests_passing: 82
  files_created: 7
  files_modified: 2
---

# Phase 03 Plan 04: Wire Layer — Frame Codec + HTTP/TCP Transports + Embed Mode

**One-liner:** Binary-framed TCP transport (stdlib socket + struct matching Phase 2.5's
`[u32 len][u16 op][u8 ct][payload]`), HTTP/JSON transport (httpx), URL-scheme dispatch,
and embed-mode subprocess launcher with 4-step binary discovery and stdout-based port
extraction from tracing JSON logs.

## What Was Built

### `python/beava/_wire.py` (220 lines)

- **Constants**: `OP_PING=0x0000`, `OP_REGISTER=0x0001`, `OP_ERROR_RESPONSE=0xFFFF`,
  `CT_JSON=0x01`, `CT_MSGPACK=0x02`; `MAX_FRAME_BYTES=4MiB`.
- **Exceptions**: `FrameTooLarge` (message contains "too_large"), `IncompleteFrame`.
- **`Frame` dataclass**: `op: int`, `ct: int`, `payload: bytes`.
- **`encode_frame(op, ct, payload) -> bytes`**: `struct.pack(">IHB", length, op, ct) + payload`
  where `length = 2 + 1 + len(payload)`. Byte-identical to Rust's `encode_frame`.
- **`decode_frame(buf, max_frame_bytes) -> Frame`**: Parses complete buffer; raises
  `IncompleteFrame` if buffer too short, `FrameTooLarge` if declared length exceeds limit.
- **`read_frame(sock, max_frame_bytes) -> Frame`**: Reads from a live socket with
  `_recv_exactly` loop; same validation as `decode_frame`.
- **`parse_register_response(frame) -> dict`**: Maps `OP_REGISTER` → success dict;
  `OP_ERROR_RESPONSE` → raises `RegistrationError`; unknown op → raises `RegistrationError`.

### `python/beava/_transport.py` (340 lines)

- **`Transport`**: Base class with `send_register`, `send_ping`, `close`, context-manager.
- **`HttpTransport`**: `httpx.Client` with `base_url`; `send_register` posts to
  `/register` with `application/json`, maps non-200 to `RegistrationError`; `send_ping`
  raises `NotImplementedError` (HTTP has no /ping in v0).
- **`TcpTransport`**: Lazy socket (`_socket=None` until first call); `_ensure_connected`
  opens on first use; `send_register` + `send_ping` use `encode_frame` + `read_frame`;
  `close()` sets `_socket = None`; context manager exits clean.
- **`EmbedTransport`**: Wraps `TcpTransport` + `Popen`; `close()` calls `teardown_process`.
- **`parse_url_to_transport(url)`**: `http://`/`https://` → `HttpTransport`; `tcp://` →
  `TcpTransport`; `None` → `EmbedTransport`; else → `ValueError`.

### `python/beava/_embed.py` (180 lines)

- **`discover_binary() -> Path`**: 4-step order: (1) `BEAVA_BINARY` env var (raises
  immediately if set but invalid); (2) `shutil.which("beava")`; (3) CWD walk upward
  for `target/debug/beava`; (4) `BinaryNotFoundError` with install guidance.
- **`spawn_embedded_server(startup_timeout=5.0)`**: Spawns binary with
  `BEAVA_LISTEN_ADDR=127.0.0.1:0`, `BEAVA_TCP_PORT=0`, `BEAVA_DEV_ENDPOINTS=1`,
  `--config /dev/null`; reads `proc.stdout` line-by-line in a daemon thread;
  JSON-parses each line looking for `kind=server.http_bound` + `kind=server.tcp_bound`;
  `threading.Event.wait(timeout=5.0)` — raises `TimeoutError` if not both received.
- **`teardown_process(proc, timeout=5.0)`**: SIGTERM → wait → SIGKILL on timeout.

### `python/tests/conftest.py`

- **`beava_binary` (session scope)**: `cargo build --bin beava --quiet` at repo root
  (`pytestconfig.rootpath.parent`); returns `Path`.
- **`beava_server` (function scope)**: Spawns binary with same env var overrides as
  embed mode; parses `proc.stdout` for bind log lines; yields `(http_url, tcp_url)`;
  SIGTERM on teardown.

### `python/beava/__init__.py`

Replaced `class _AppStub` block with `from ._transport import parse_url_to_transport`;
`App = _AppStub` stub remains for Plan 03-05.

## TDD Commit Trace

| Commit | Type | Message |
|--------|------|---------|
| `7ea5db4` | RED | `test(03-04): failing tests for wire framing, HTTP transport, TCP transport, embed mode` |
| `7a22148` | GREEN | `feat(03-04): wire codec + HTTP transport + TCP transport + embed mode` |

Red commit: all 4 test files fail with `ModuleNotFoundError: No module named 'beava._wire'`
(and `._transport`, `._embed`). No implementation files existed.

Green commit: 31 new tests + 51 prior tests = 82 total passing; ruff clean; mypy strict clean.

## Verification Results

```
pytest tests/test_wire_framing.py -v   → 9 passed
pytest tests/test_embed.py -v          → 12 passed
pytest tests/test_transport_http.py -v → 4 passed
pytest tests/test_transport_tcp.py -v  → 6 passed (includes strict-FIFO pipeline test)
pytest tests/ -q                       → 82 passed in 1.54s

ruff check beava/ tests/               → All checks passed!
mypy beava/                            → Success: no issues found in 10 source files

python -c "from beava._wire import encode_frame, OP_PING, CT_JSON; \
  f = encode_frame(OP_PING, CT_JSON, b''); \
  assert f == b'\\x00\\x00\\x00\\x03\\x00\\x00\\x01', f.hex()"  → OK

python -c "from beava._transport import parse_url_to_transport; \
  t = parse_url_to_transport('tcp://localhost:7380'); \
  assert t.host == 'localhost' and t.port == 7380"               → OK
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Binary writes JSON logs to stdout, not stderr**
- **Found during:** Task 1.b, first test run (10-second timeout, `http_addr=[], tcp_addr=[]`)
- **Issue:** Plan spec and CONTEXT.md §D-10 describe parsing stderr for bind log lines.
  In reality, the beava binary writes all tracing JSON (including `server.http_bound` /
  `server.tcp_bound` lines) to **stdout**. The banner line also goes to stdout.
  Setting `stdout=subprocess.DEVNULL` silently discarded all parseable output.
- **Fix:** Changed both `conftest.py::beava_server` and `_embed.py::spawn_embedded_server`
  to `stdout=subprocess.PIPE` + `stderr=subprocess.DEVNULL`; reader threads now iterate
  `proc.stdout` instead of `proc.stderr`; non-JSON lines (banner) are skipped silently.
- **Files modified:** `python/beava/_embed.py`, `python/tests/conftest.py`
- **Commit:** included in `7a22148`

**2. [Rule 1 - Bug] Binary requires config file — fails without ./beava.yaml**
- **Found during:** Task 1.b, embed test debugging
- **Issue:** The beava binary's default config path is `./beava.yaml`. When spawned from
  `python/` (pytest rootdir), no such file exists → binary exits with
  `Error: config file not found: ./beava.yaml` before emitting any log lines.
- **Fix:** Pass `--config /dev/null` in all subprocess spawns (both conftest fixture and
  `spawn_embedded_server`). `/dev/null` is treated as empty YAML → all server defaults
  apply. Port overrides via `BEAVA_LISTEN_ADDR` + `BEAVA_TCP_PORT` env vars still work.
- **Files modified:** `python/beava/_embed.py`, `python/tests/conftest.py`
- **Commit:** included in `7a22148`

**3. [Rule 1 - Bug] mypy strict: `json.loads` returns `Any`, cannot return from `-> dict`**
- **Found during:** Task 1.b mypy gate
- **Issue:** `parse_register_response` declared `-> dict` (with `type: ignore[type-arg]`).
  `json.loads` returns `Any`. Mypy strict raises `no-any-return` on the `return body` line.
- **Fix:** Added `from typing import Any, cast`; annotated `body: Any = json.loads(...)`
  and returned `cast(dict[str, Any], body)` on the success branch.
- **Files modified:** `python/beava/_wire.py`
- **Commit:** included in `7a22148`

**4. [Rule 1 - Bug] Ruff lint violations in test files**
- **Found during:** Task 1.b ruff gate
- **Issue:** `test_embed.py` had unused `os` + `Any` imports; `test_transport_tcp.py`
  had unused `struct` import; `test_wire_framing.py` had `f"payload mismatch"` (f-string
  without placeholders); `test_transport_http.py` had one E501 (102 char docstring line).
- **Fix:** `ruff --fix` auto-resolved 4 of 5; the E501 was fixed manually by splitting
  the docstring.
- **Files modified:** `python/tests/test_embed.py`, `python/tests/test_transport_tcp.py`,
  `python/tests/test_wire_framing.py`, `python/tests/test_transport_http.py`
- **Commit:** included in `7a22148`

## Known Stubs

| Stub | File | Line | Resolved By |
|------|------|------|-------------|
| `App = _AppStub` | `beava/__init__.py` | ~38 | Plan 03-05 (`bv.App` client) |

All transport-layer functionality is fully wired. `parse_url_to_transport` is real.

## Threat Surface Scan

Threat model items from plan addressed:

- **T-03-04-02 (FrameTooLarge)**: `TcpTransport` enforces `max_frame_bytes=4MiB` (matches
  server default) via `read_frame`; oversized responses raise `FrameTooLarge`.
- **T-03-04-03 (binary discovery)**: `discover_binary` only executes paths from the
  4-step order; no shell interpolation; `BEAVA_BINARY` set but invalid → `BinaryNotFoundError`
  (no arbitrary fallthrough that could execute unexpected binaries).
- **T-03-04-04 (subprocess env)**: `spawn_embedded_server` inherits `os.environ` and
  overlays only the 3 specific overrides (`BEAVA_LISTEN_ADDR`, `BEAVA_TCP_PORT`,
  `BEAVA_DEV_ENDPOINTS`). No shell-injected env vars are added.

No new network surface beyond what was planned (HTTP `POST /register` + TCP ping/register).

## Self-Check: PASSED

Files created/modified:
- `/Users/petrpan26/work/tally/python/beava/_wire.py` — FOUND
- `/Users/petrpan26/work/tally/python/beava/_transport.py` — FOUND
- `/Users/petrpan26/work/tally/python/beava/_embed.py` — FOUND
- `/Users/petrpan26/work/tally/python/beava/__init__.py` has `from ._transport import parse_url_to_transport` — VERIFIED
- `/Users/petrpan26/work/tally/python/tests/conftest.py` has `def beava_binary` and `def beava_server` — VERIFIED
- `/Users/petrpan26/work/tally/python/tests/test_wire_framing.py` — FOUND
- `/Users/petrpan26/work/tally/python/tests/test_transport_http.py` — FOUND
- `/Users/petrpan26/work/tally/python/tests/test_transport_tcp.py` — FOUND
- `/Users/petrpan26/work/tally/python/tests/test_embed.py` — FOUND

Commits:
- `7ea5db4` (red) — FOUND
- `7a22148` (green) — FOUND

Gates:
- `pytest tests/ -q` → 82 passed — VERIFIED
- `ruff check beava/ tests/` → clean — VERIFIED
- `mypy beava/` → clean (10 source files) — VERIFIED
- `encode_frame(OP_PING, CT_JSON, b'')` → `b'\x00\x00\x00\x03\x00\x00\x01'` — VERIFIED
- `parse_url_to_transport('tcp://localhost:7380').host == 'localhost'` — VERIFIED
