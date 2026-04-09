---
phase: 03-python-sdk
reviewed: 2026-04-09T00:00:00Z
depth: standard
files_reviewed: 19
files_reviewed_list:
  - python/pyproject.toml
  - python/tally/__init__.py
  - python/tally/_types.py
  - python/tally/_protocol.py
  - python/tally/_operators.py
  - python/tally/_stream.py
  - python/tally/_view.py
  - python/tally/_client.py
  - python/tally/_app.py
  - python/tests/test_types.py
  - python/tests/test_protocol.py
  - python/tests/test_operators.py
  - python/tests/test_stream.py
  - python/tests/test_view.py
  - python/tests/test_client.py
  - python/tests/test_app.py
  - python/tests/test_integration.py
  - python/tests/conftest.py
  - src/main.rs
findings:
  critical: 2
  warning: 5
  info: 4
  total: 11
status: issues_found
---

# Phase 03: Code Review Report

**Reviewed:** 2026-04-09
**Depth:** standard
**Files Reviewed:** 19
**Status:** issues_found

## Summary

The Python SDK is well-structured, cleanly separated into layers (types, protocol, operators, stream/view DSL, client, app), and the test suite has thorough unit and mock-server coverage. The wire protocol encoding is tight and well-tested.

Two critical issues stand out: a silent data-corruption bug in `_protocol.py`'s `parse_response` where the length-zero check arrives _after_ the buffer has already been read (and the zero-length frame case would allow an index-out-of-range crash on `body[0]`), and a port-reuse race in `conftest.py` that makes the test server startup non-deterministic under high load. Five warnings cover logic gaps: the `send_command` auto-reconnect silently swallows non-connection errors, the `stream` decorator drops `__doc__` and other dunder attributes that users legitimately set, `_parse_address` will crash on IPv6 literal addresses, `encode_string` silently truncates strings longer than 65535 UTF-8 bytes (no guard), and the integration test `test_get_features` has a hidden ordering dependency on `test_push_accumulates` that will fail if tests run in isolation. Four info-level items cover missing `__all__` in `pyproject.toml` dependencies, protocol opcodes being exported from `__init__.py` as part of the public API (an internal implementation detail), lack of `__setattr__` protection on `FeatureResult`, and a magic number for the default port.

---

## Critical Issues

### CR-01: `parse_response` length-zero check is ordered after buffer read — allows `body[0]` to panic

**File:** `python/tally/_protocol.py:126-129`

**Issue:** The guard `if length < 1` is checked _after_ `body = data[5 : 4 + length]` has already been computed (line 130 is not reached — this check is at line 126 but `data[4]` is accessed at line 129 with no guard). More precisely: when `length == 0`, `data[4]` is accessed at line 129 unconditionally if the caller has provided at least 5 bytes. If `data` has exactly 4 bytes the earlier `len(data) < 4 + length` check (which becomes `len(data) < 4`) passes when `length == 0`, so the code falls through to `status = data[4]`, which raises `IndexError` (not a `ProtocolError`) for a 4-byte input. This turns a protocol error into an unhandled exception that bypasses the error hierarchy. Additionally, `parse_response` is not used by `TallyClient` — the client has its own `_recv_frame` which handles this correctly — but `parse_response` is a public API and its contract is broken.

**Fix:** Move the `length < 1` check to immediately after the length is decoded, before any access into `data[4+...]`:

```python
def parse_response(data: bytes) -> tuple[int, bytes]:
    if len(data) < 4:
        raise ProtocolError("response too short: need at least 4 bytes for length header")

    length = struct.unpack(">I", data[:4])[0]

    # Guard must come before any buffer access
    if length < 1:
        raise ProtocolError("frame length must be at least 1 (status byte)")

    if length > MAX_FRAME_SIZE:
        raise ProtocolError(
            f"frame too large: {length} bytes exceeds limit of {MAX_FRAME_SIZE}"
        )

    if len(data) < 4 + length:
        raise ProtocolError(
            f"response truncated: expected {length} bytes after header, got {len(data) - 4}"
        )

    status = data[4]
    payload = data[5 : 4 + length]

    if status == STATUS_ERROR:
        raise ProtocolError(payload.decode("utf-8", errors="replace"))

    return status, payload
```

---

### CR-02: `conftest.py` port-reuse race — free port may be taken between `_find_free_port()` and server bind

**File:** `python/tests/conftest.py:34-38`

**Issue:** `_find_free_port()` opens a socket, reads the OS-assigned port, then _closes_ the socket before returning. The port is free at that point, but nothing prevents another process (or the OS) from reusing it before the Tally server subprocess binds to it. Under parallel test runners (pytest-xdist) or CI with many concurrent jobs this is a consistent source of flaky failures — the server silently starts on the wrong port (or fails to bind) and `_wait_for_tcp` times out after 10 seconds, causing the entire session to fail with an unhelpful message.

```python
def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]   # Port is released here — race window opens
```

**Fix:** Use `SO_REUSEPORT` (Linux) or pass `port=0` via environment and let the server report its actual bound port via stdout/stderr or a ready-file. A pragmatic alternative is to keep the socket open with `SO_REUSEADDR` until the subprocess has had time to bind, but the cleanest fix for the test harness is to tell the server to bind on port 0 and read the actual port back:

```python
# Simpler workaround: use a larger retry window and accept the race,
# OR use a fixed port range with collision detection in CI:
def _find_free_port() -> int:
    import random
    for _ in range(20):
        port = random.randint(30000, 50000)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
            return port  # If bind succeeds the port is available
        except OSError:
            continue
    raise RuntimeError("Could not find a free port")
```

The real fix is to have the server accept port 0 and print its bound port, but that requires a server-side change.

---

## Warnings

### WR-01: `send_command` auto-reconnect catches `ConnectionError` but the except clause also catches `tally.ConnectionError` (a subclass of `TallyError`) — OSError from `sendall` is NOT caught, leaving the socket in a dirty state

**File:** `python/tally/_client.py:109-117`

**Issue:** The reconnect logic catches `ConnectionError` (the tally custom exception), but `self._send_frame` calls `self._sock.sendall(frame)` which raises `OSError` (not `ConnectionError`) if the socket breaks during a write (broken pipe, reset). In that case the exception propagates up as a raw `OSError` uncaught, and `self._sock` is left pointing to the broken socket. The next call to `send_command` skips `_ensure_connected` (socket is not None) and tries to use the broken socket again.

```python
try:
    self._send_frame(opcode, payload)   # May raise OSError (not ConnectionError)
    return self._recv_frame()
except ConnectionError:                 # Does NOT catch OSError from sendall
    self._sock = None
    self._connect()
    ...
```

**Fix:** Catch `OSError` in addition to the custom `ConnectionError`, and always null out the socket before reconnecting:

```python
try:
    self._send_frame(opcode, payload)
    return self._recv_frame()
except (ConnectionError, OSError):
    self._sock = None
    self._connect()
    self._send_frame(opcode, payload)
    return self._recv_frame()
```

---

### WR-02: `stream` decorator silently drops `__doc__` and other non-dunder class attributes

**File:** `python/tally/_stream.py:107-111`

**Issue:** The decorator rebuilds the class via `StreamMeta` using a namespace filtered to exclude all `__`-prefixed attributes. This means a user-authored `__doc__` on their stream class is silently dropped. It also means any Python special method defined on the class body (e.g., a `__repr__` for debugging) is discarded. The filter `not k.startswith("__")` is overly broad.

```python
namespace = {
    k: v for k, v in cls.__dict__.items() if not k.startswith("__")
}
```

This means:
```python
@stream(key="user_id")
class Transactions:
    """My stream docstring."""   # LOST after decoration
    tx_count = st.count(window="30m")

print(Transactions.__doc__)  # None
```

**Fix:** Preserve `__doc__` explicitly:

```python
namespace = {
    k: v for k, v in cls.__dict__.items()
    if not k.startswith("__") or k == "__doc__"
}
```

The same issue exists in `_view.py:33-35`.

---

### WR-03: `_parse_address` crashes on IPv6 literal addresses (e.g. `"[::1]:6400"`)

**File:** `python/tally/_app.py:54-57`

**Issue:** The address parser uses `":" in address` to detect host:port, then `rsplit(":", 1)` to split. An IPv6 address like `"[::1]:6400"` contains multiple colons, and `rsplit(":", 1)` would yield `host="[::1]"` and `port_str="6400"` (accidentally correct), but `"::1"` (without brackets) would yield `host=":"` and `port_str="1"` — silently wrong. More critically, `"[::1]"` without a port hits the `else` branch and is treated as a hostname, then passed directly to the socket layer, which will likely fail with a confusing error rather than a clear address-parse error.

This is a reliability issue — users who try to connect to an IPv6 server will get an obscure error rather than a clear parse failure.

**Fix:** Use a more robust parse approach:

```python
@staticmethod
def _parse_address(address: str) -> tuple[str, int]:
    """Parse 'host:port' or 'host' (default port 6400). Supports IPv6 [::1]:port."""
    if address.startswith("["):
        # IPv6 literal: [host]:port or [host]
        bracket_end = address.find("]")
        if bracket_end == -1:
            raise ValueError(f"Invalid address (unmatched '['): {address!r}")
        host = address[1:bracket_end]
        rest = address[bracket_end + 1:]
        if rest.startswith(":"):
            return host, int(rest[1:])
        return host, 6400
    if ":" in address:
        host, port_str = address.rsplit(":", 1)
        return host, int(port_str)
    return address, 6400
```

---

### WR-04: `encode_string` silently truncates strings longer than 65535 UTF-8 bytes (u16 overflow)

**File:** `python/tally/_protocol.py:49-52`

**Issue:** The protocol encodes string length as a `u16` (max 65535). `struct.pack(">H", len(s_bytes))` will raise `struct.error` if `len(s_bytes) > 65535`, but only at call time with a confusing message (`'H' format requires 0 <= number <= 65535`). There is no validation that produces an actionable `ProtocolError`. A very long stream name or key would produce this error deep inside a network call with no context about which string violated the limit.

**Fix:** Add an explicit check with a descriptive error:

```python
def encode_string(s: str) -> bytes:
    """Encode a protocol string: [u16 BE length][UTF-8 bytes]."""
    s_bytes = s.encode("utf-8")
    if len(s_bytes) > 0xFFFF:
        raise ProtocolError(
            f"string too long for protocol: {len(s_bytes)} bytes (max 65535)"
        )
    return struct.pack(">H", len(s_bytes)) + s_bytes
```

---

### WR-05: `test_get_features` has a hidden ordering dependency on `test_push_accumulates`

**File:** `python/tests/test_integration.py:71-77`

**Issue:** The test `test_get_features` asserts that `app.get("u2")` returns `tx_count_1h == 2` and `tx_sum_1h == 80.0`, which are the exact values left by `test_push_accumulates`. Since the `app` fixture is session-scoped (shared server, shared state), this test _only passes_ if `test_push_accumulates` ran first in the same session. If pytest runs tests in a different order (e.g., alphabetical, randomized with `pytest-randomly`, or if only `test_get_features` is run in isolation with `-k`), it will fail with `AttributeError: no feature named 'tx_count_1h'` or an assertion error.

```python
def test_get_features(app):
    # Relies on state from test_push_accumulates having run first
    all_features = app.get("u2")
    assert all_features.tx_count_1h == 2   # Will fail if run in isolation
```

**Fix:** Either make each test own its setup (push the events it needs before asserting), or use pytest ordering markers:

```python
def test_get_features(app):
    """GET returns features previously written by push."""
    # Own our setup
    app.push(Transactions, {"user_id": "u2_get", "amount": 50.0})
    app.push(Transactions, {"user_id": "u2_get", "amount": 30.0})
    all_features = app.get("u2_get")
    assert all_features.tx_count_1h == 2
    assert all_features.tx_sum_1h == 80.0
```

---

## Info

### IN-01: Protocol opcode constants exported from `tally.__init__` — internal implementation detail in public API

**File:** `python/tally/__init__.py:16-44`

**Issue:** `OP_PUSH`, `OP_GET`, `OP_SET`, `OP_MSET`, `OP_REGISTER` are numeric wire protocol constants exported as part of the top-level `tally` package public API. These are low-level internals; users of the SDK never need them directly. Exporting them locks in the wire format values as a public API commitment.

**Fix:** Remove them from `__init__.py` exports. Users who need them can import from `tally._protocol` directly (accepting the private-module convention).

---

### IN-02: `FeatureResult` has no `__setattr__` protection — attributes can be shadowed silently

**File:** `python/tally/_types.py:33-56`

**Issue:** `FeatureResult` uses `__slots__ = ("_data",)` which prevents adding new instance attributes, but this raises `AttributeError: '_data' object has no attribute 'foo'` with a confusing message. More importantly, `_data` itself is settable: `result._data = {}` silently wipes all features. There is no protection against accidental mutation.

**Fix:** Add `__setattr__` to block all writes:

```python
def __setattr__(self, name: str, value: object) -> None:
    if name == "_data":
        object.__setattr__(self, name, value)
    else:
        raise AttributeError(f"FeatureResult is read-only; cannot set '{name}'")
```

(The `object.__setattr__` path in `__init__` still works because it bypasses `__setattr__`.)

---

### IN-03: `pyproject.toml` has no `dependencies` field — `tally` package will fail to install without `pip install` manually tracking implicit deps

**File:** `python/pyproject.toml:1-12`

**Issue:** The `[project]` section has no `dependencies` key. Even though the SDK uses only the Python standard library (no third-party packages), the absence of an explicit `dependencies = []` means static analysis tools and pip extras resolution may not handle the package correctly. It also makes the intent implicit.

**Fix:**
```toml
[project]
name = "tally"
version = "0.1.0"
requires-python = ">=3.10"
description = "Python SDK for Tally real-time feature server"
dependencies = []
```

---

### IN-04: Magic number `6400` appears in three places with no named constant

**File:** `python/tally/_app.py:57`, `python/tests/conftest.py` (implicitly via `App` default)

**Issue:** The default port `6400` is hardcoded as a magic number in `_app.py` and in test comments. If the default port changes, all three sites must be updated manually and the connection between them is not visible.

**Fix:** Define it as a module-level constant in `_protocol.py` or `_app.py`:

```python
DEFAULT_PORT: int = 6400
```

And reference it in `_parse_address`:
```python
return address, DEFAULT_PORT
```

---

_Reviewed: 2026-04-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
