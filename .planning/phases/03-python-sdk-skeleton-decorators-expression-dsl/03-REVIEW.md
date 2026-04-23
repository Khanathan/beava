---
phase: 03-python-sdk-skeleton-decorators-expression-dsl
reviewed: 2026-04-23T00:00:00Z
depth: standard
files_reviewed: 28
files_reviewed_list:
  - python/pyproject.toml
  - python/beava/__init__.py
  - python/beava/_app.py
  - python/beava/_col.py
  - python/beava/_embed.py
  - python/beava/_errors.py
  - python/beava/_events.py
  - python/beava/_schema.py
  - python/beava/_tables.py
  - python/beava/_transport.py
  - python/beava/_types.py
  - python/beava/_validate.py
  - python/beava/_wire.py
  - python/README.md
  - python/tests/__init__.py
  - python/tests/conftest.py
  - python/tests/test_app.py
  - python/tests/test_col.py
  - python/tests/test_embed.py
  - python/tests/test_errors.py
  - python/tests/test_events.py
  - python/tests/test_package_init.py
  - python/tests/test_phase3_smoke.py
  - python/tests/test_schema.py
  - python/tests/test_tables.py
  - python/tests/test_transport_http.py
  - python/tests/test_transport_tcp.py
  - python/tests/test_types.py
  - python/tests/test_validate.py
  - python/tests/test_wire_framing.py
findings:
  critical: 1
  warning: 7
  info: 5
  total: 13
status: issues_found
---

# Phase 03: Code Review Report

**Reviewed:** 2026-04-23
**Depth:** standard
**Files Reviewed:** 28 (source: 13, tests: 15)
**Status:** issues_found

## Summary

Phase 3 ships a clean, well-structured Python SDK: the decorator system, expression DSL, transport layer, frame codec, and DAG validation are all well-implemented and match the Rust wire protocol byte-for-byte. The TDD discipline is visible — tests are genuinely written to be red before green (the test files contain comments confirming the red-commit intent). The code quality is uniformly high.

Seven issues need attention before this phase is merged, in three categories:

1. **One critical resource leak**: the `EmbedTransport` subprocess stdout pipe is never drained after startup completes, causing potential deadlock on large server log output (`_embed.py`).
2. **Four correctness/reliability warnings**: a `topo_sort` that raises `ValueError` instead of `ValidationError` (breaking the contract documented in `_app.py`), a frame-underflow case unguarded by `decode_frame` (mirroring the Rust server's `LengthUnderflow` error), a conftest fixture leaking `_reader` threads and the subprocess stdout pipe on timeout, and a silent no-close path in `App` when embed mode is used without a context manager.
3. **Three minor warnings / five info items**: type-unsafe `_upstreams` mutation in tests, a TDD smell in one test class, and several informational items.

---

## Critical Issues

### CR-01: Subprocess stdout pipe not drained — deadlock risk in embed mode

**File:** `python/beava/_embed.py:133-155`

**Issue:** The `_reader` background thread reads from `proc.stdout` line-by-line until both bind events are received, then sets `ready`. After that, the `elif ready.is_set(): _log.debug(...)` branch is dead code because the `for raw in proc.stdout` loop continues draining but the only action in the loop body after `ready.is_set()` is a no-op path. More critically: once `ready` is set, the reader thread continues consuming stdout. However, when the outer caller calls `teardown_process(proc)` and then `proc.wait()`, if the stdout pipe buffer fills up before the reader thread has drained it (e.g., during a high-log-volume workload), the subprocess will block writing to stdout, preventing it from exiting, causing `proc.wait()` inside `teardown_process` to deadlock after `timeout` seconds and then issue `proc.kill()`.

There is a second, subtler bug: the `elif ready.is_set()` branch is unreachable because after `ready.set()` the loop continues reading and the *next* iteration will hit `if http_addr and tcp_addr:` which is already true and calls `ready.set()` again (no-op) — then falls through with no logging. The intended DEBUG logging of post-startup lines never runs.

**Fix:**
```python
def _reader() -> None:
    assert proc.stdout is not None
    for raw in proc.stdout:
        line = raw.decode("utf-8", errors="replace").rstrip()
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            _log.debug("non-json stdout: %s", line)
            if ready.is_set():
                continue
            continue

        kind = rec.get("kind", "")
        if kind == "server.http_bound":
            http_addr.append(rec.get("addr", ""))
        elif kind == "server.tcp_bound":
            tcp_addr.append(rec.get("addr", ""))

        if http_addr and tcp_addr:
            ready.set()
        elif ready.is_set():
            # Post-startup structured log line
            _log.debug("%s", line)
    # Stdout pipe closed (process exited) — signal readiness in case
    # teardown happened before bind events arrived.
    ready.set()
```

The critical addition is the `ready.set()` at the end of the loop body: when the process exits (pipe EOF) without having emitted both bind events, the `ready.wait()` call in the spawn path currently waits the full `startup_timeout` before killing the process. With the fix above the loop ends on EOF and `ready.set()` is called, allowing the caller to proceed to the timeout check immediately.

The broader fix for the deadlock is to ensure the thread always fully drains stdout, which the existing daemon thread already does (it reads to EOF). The daemon=True flag means Python will not wait for the thread on interpreter exit, so process cleanup is safe.

---

## Warnings

### WR-01: `topo_sort` raises `ValueError` instead of `ValidationError` — contract mismatch

**File:** `python/beava/_validate.py:214-222`

**Issue:** The docstring and `_app.py` comment (line 191: "raises ValidationError on cycle — caught above already") imply that `topo_sort` raises `ValidationError`. In practice it raises `ValueError(str(err))`. The `App.register()` call path calls `validate_descriptors` first (which correctly appends a `ValidationError` and returns it), then calls `topo_sort` only if validation passed. Because cyclic inputs are already caught, `topo_sort`'s cycle branch is normally unreachable in the register path — but the function is public and documented as raising `ValidationError`. Any caller that imports `topo_sort` directly and passes a cyclic graph gets `ValueError`, not the documented `ValidationError`.

**Fix:**
```python
# _validate.py line 214
if cycle_path:
    path_str = " -> ".join(cycle_path)
    raise ValidationError(
        kind="cycle",
        path=path_str,
        message=f"dependency cycle detected: {path_str}",
    )
# line 217 fallback:
raise ValidationError(
    kind="cycle",
    path="(unknown)",
    message="dependency cycle detected (could not reconstruct path)",
)
```

Also update the docstring to `Raises: ValidationError` (currently says `ValidationError` in the docstring on line 160-169 but raises `ValueError` in the body).

---

### WR-02: `decode_frame` does not guard against `length < 3` (LengthUnderflow)

**File:** `python/beava/_wire.py:111-136`

**Issue:** The Rust `decode_frame` in `crates/beava-core/src/wire.rs` (line 215) explicitly returns `FrameError::LengthUnderflow` when `declared_len < 3`. The Python `decode_frame` has no such check: if a malicious or buggy server sends a frame header with `length = 0`, `1`, or `2`, the Python code computes `payload = buf[7:total_needed]` where `total_needed = 4 + length`. For `length = 0`, `total_needed = 4` so `buf[7:4]` = `b""`, then `op = struct.unpack(">H", buf[4:6])[0]` and `ct = buf[6]` are still read, but the frame semantics are wrong (the length field claims the frame contains zero bytes for op+ct+payload, yet op and ct are read). For `length = 1` or `2`, the slice `buf[4:6]` or `buf[4:7]` would read beyond the declared frame boundary. The same issue exists in `read_frame` at line 173: after reading `length`, there is no `length < 3` check before reading `rest = _recv_exactly(sock, length)`.

**Fix:**
```python
# In decode_frame, after line 115:
(length,) = struct.unpack(">I", buf[:4])
if length < 3:
    raise IncompleteFrame(
        f"frame length {length} < 3: cannot cover op + content_type"
    )

# In read_frame, after line 173:
(length,) = struct.unpack(">I", len_bytes)
if length < 3:
    raise IncompleteFrame(
        f"frame length {length} < 3: cannot cover op + content_type"
    )
```

Note: using `IncompleteFrame` preserves the existing two-exception convention; a dedicated `MalformedFrame` would be more precise, but changing exception types is a larger contract change. An alternative is to use `FrameTooLarge` with a `too_small` message, but `IncompleteFrame` is the least-breaking choice. The Rust server uses a protocol-violation error and closes the connection; the Python client should also not attempt to parse further.

---

### WR-03: `conftest.py` fixture leaks `_reader` thread and stdout pipe on server timeout

**File:** `python/tests/conftest.py:114-134`

**Issue:** When `ready.wait(timeout=5.0)` returns `False` (server did not emit both bind lines in time), the code calls `proc.kill()` and `proc.wait()` — correct. However, the `_reader` thread is a daemon thread that is still blocking on `proc.stdout` iteration. After `proc.wait()` returns, the underlying subprocess is dead, but `proc.stdout` (the pipe fd) is not explicitly closed. The `_reader` thread will unblock when the OS closes the read end of the pipe after `proc.stdout` is garbage-collected, but the timing is non-deterministic, and on CPython with a live reference in the thread's frame the pipe fd may stay open. The `pytest.fail()` call exits the test, dropping the `proc` local — but in the thread the `proc.stdout` reference is held by the `for raw in proc.stdout` loop.

More concretely: the `_reader` thread holds `proc.stdout` open until the generator's destructor runs or an exception propagates. Until then, the OS-level file descriptor remains open. On systems with limited fd counts (CI containers), running many integration tests can exhaust fds. The recommended fix is to call `proc.stdout.close()` before `pytest.fail()`.

**Fix:**
```python
if not ready.wait(timeout=5.0):
    proc.kill()
    proc.wait()
    if proc.stdout:
        proc.stdout.close()  # unblocks the _reader thread
    error_detail = f"http_addr={http_addr}, tcp_addr={tcp_addr}"
    pytest.fail(
        f"beava server did not emit both bind log lines within 5s: {error_detail}"
    )
```

The same pattern applies symmetrically in `_embed.py`'s `spawn_embedded_server` (line 160-166), which already closes `proc.stdout` indirectly via `proc.kill()` + `proc.wait()` before raising, but should also explicitly close:
```python
if not ready.wait(timeout=startup_timeout):
    proc.kill()
    proc.wait()
    if proc.stdout:
        proc.stdout.close()
    raise TimeoutError(...)
```

---

### WR-04: `App` embed mode without context manager never closes the transport on user error

**File:** `python/beava/_app.py:57-108`

**Issue:** `App.__init__` with `url=None` (embed mode) defers transport creation to `__enter__`. But `_require_transport()` raises `RuntimeError` before any subprocess is spawned, so there is no resource leak. However, once a user does enter the context manager and assigns `self._transport = parse_url_to_transport(None)` (line 77), and then an exception is raised inside the `with` block that is caught *outside* the context manager (e.g., `try: with bv.App() as app: raise ValueError() except ValueError: pass`), `__exit__` is still called (correctly). This path is safe.

The actual gap is: if `parse_url_to_transport(None)` succeeds (subprocess spawned), but then `__enter__` itself raises an exception (theoretically impossible now but fragile), the subprocess would be orphaned because `self._transport` is assigned but `_closed` is False and `__exit__` is never called. This is currently not exploitable but is a latent risk as `__enter__` grows.

More concretely: when `url is not None`, the transport is created eagerly in `__init__` (line 67). If the caller creates `app = bv.App("tcp://...")` and never calls `app.close()` (no context manager), the socket is leaked. This is documented ("context manager optional"), but there is no `__del__` guard. For the TCP transport this means a leaked fd; for HTTP this means a leaked connection pool. This is a resource management concern, not a crash.

**Fix:** Add a `__del__` finalizer as a safety net:
```python
def __del__(self) -> None:
    # Safety net: close transport if the App is garbage-collected without
    # explicit close(). __del__ is not guaranteed to run, but it prevents
    # common forget-to-close bugs from leaking connections indefinitely.
    if not self._closed and self._transport is not None:
        try:
            self._transport.close()
        except Exception:
            pass
```

---

### WR-05: `_decorate_event_function` / `_decorate_table_function` silently accept non-descriptor callables via `hasattr(upstream_cls, "_name")`

**File:** `python/beava/_events.py:232` and `python/beava/_tables.py:225`

**Issue:** The function-form decorator checks `hasattr(upstream_cls, "_name")` to determine whether a parameter annotation is a valid upstream descriptor. Any object with a `_name` attribute passes this check, including arbitrary Python objects, named tuples, dataclasses, and even strings (which have no `_name` but any class with `__name__` manipulated by metaclasses could pass). The practical risk is a confusing `AttributeError` downstream when `upstream_cls._name` or `upstream_cls._schema` is accessed for a non-descriptor object. The error message will not point to the decorator call site.

The correct guard is to check for the `_beava_kind` attribute (all four descriptor classes define it as a class attribute), which is a Beava-specific sentinel:

**Fix:**
```python
# _events.py line 232, _tables.py line 225
upstream_cls = param.annotation
if (
    upstream_cls is inspect.Parameter.empty
    or not hasattr(upstream_cls, "_beava_kind")
    or not hasattr(upstream_cls, "_name")
):
    raise TypeError(
        f"@bv.event function form: parameter {param_name!r} must be annotated "
        f"with a @bv.event- or @bv.table-decorated descriptor "
        f"(got {upstream_cls!r})"
    )
```

---

### WR-06: `extract_schema` does not handle inherited annotations — class hierarchy causes silent field omission

**File:** `python/beava/_schema.py:148-149`

**Issue:** `extract_schema` builds the field order from `list(getattr(cls, "__annotations__", {}).keys())` (line 148). This reads `cls.__annotations__` which in Python only contains annotations **declared directly on `cls`**, not on parent classes. However, `typing.get_type_hints(cls)` (line 143) **does** include inherited annotations by default (it merges the MRO). The result: if a user subclasses another event class to add fields, `hints` contains all fields (from both `cls` and the parent), but `annotation_order` only contains the fields declared on `cls` itself. The `for name in annotation_order` loop (line 151) will then skip all parent-declared fields silently, producing an incomplete schema with no error.

Example:
```python
class Base:
    user_id: str

@bv.event
class Derived(Base):
    amount: float  # only this field appears in schema; user_id is silently dropped
```

**Fix:**
```python
# Collect annotation order from the full MRO (excluding object).
annotation_order: list[str] = []
seen: set[str] = set()
for klass in reversed(cls.__mro__):
    if klass is object:
        continue
    for field_name in getattr(klass, "__annotations__", {}):
        if field_name not in seen:
            annotation_order.append(field_name)
            seen.add(field_name)
```

This is consistent with how `typing.get_type_hints` already works and gives users an intuitive inheritance experience. Note: the `annotation_order` list filters to only names also present in `hints` (line 152 `if name not in hints: continue`), so forward-ref failures remain safely handled.

---

### WR-07: `_reader` thread logic in `_embed.py` has an unreachable logging branch

**File:** `python/beava/_embed.py:149-154`

**Issue:** Inside `_reader`, the structure is:
```python
if http_addr and tcp_addr:
    ready.set()
    # comment says "forward remaining lines at DEBUG"
elif ready.is_set():
    _log.debug("%s", line)
```

The `elif ready.is_set()` is structurally dead. Once `ready.set()` has been called, all subsequent iterations of the loop will re-enter the `if http_addr and tcp_addr` branch (both lists are non-empty), call `ready.set()` again (no-op), and never reach `elif`. The intended post-startup logging never fires.

**Fix:**
```python
if http_addr and tcp_addr:
    if not ready.is_set():
        ready.set()
    else:
        # Post-startup log line — forward at DEBUG
        _log.debug("%s", line)
```

Or more simply, set a local `started` bool that flips once and gates the DEBUG path.

---

## Info

### IN-01: Tests mutate `_upstreams` via `type: ignore[assignment]` — leaks internal API into test layer

**File:** `python/tests/test_app.py:35`, `python/tests/test_validate.py:32`

**Issue:** Both test files construct `EventSource` objects and then assign `src._upstreams = upstreams` with `# type: ignore[assignment]`. `_upstreams` is set in `__init__` to `[]` but typed as `list[str]`; re-assigning it is an internal-API violation, and the `type: ignore` comment acknowledges the type checker objection. If `EventSource` is later made a frozen dataclass or `_upstreams` gets a property, this pattern breaks silently.

The `_make_event` helper should instead construct an `EventDerivation` (which accepts `upstreams` as a constructor argument) when an upstream is needed, or `EventSource.__init__` should accept an optional `upstreams` parameter. The current workaround is acceptable for Phase 3 but should be tracked.

---

### IN-02: `test_tcp_transport_strict_fifo` tests pipelining but not ordering guarantees

**File:** `python/tests/test_transport_tcp.py:103-132`

**Issue:** The test sends two OP_PING frames in one `sendall` and reads two responses, asserting both are `OP_PING`. This correctly verifies that the server responds to both frames. However, it does not verify that response 1 corresponds to request 1 (i.e., FIFO ordering). Since both requests are identical pings, this is not distinguishable. The test name says "strict FIFO" but the assertion only checks opcode equality, not correlation. This is a TDD smell: the test name implies a stronger contract than it actually encodes.

This is acceptable for Phase 3 (strict-FIFO is enforced at the Rust server level and the Phase 2.5 TCP tests cover it), but the test comment should acknowledge this limitation.

---

### IN-03: `App.validate()` creates a new `App` object in `test_phase3_smoke.py` without closing it

**File:** `python/tests/test_phase3_smoke.py:271`, `python/tests/test_phase3_smoke.py:286`

**Issue:**
```python
errs = bv.App(http_url).validate(CheckoutDerivation)
valid_errs = bv.App(http_url).validate(TxEvent, UserProfileTable)
```
Both create `App` instances that are never closed. `App(http_url)` eagerly creates an `httpx.Client` inside `HttpTransport`. The client's connection pool is not closed, leaving OS-level TCP sockets in `TIME_WAIT` state. In the test suite this causes at most a handful of leaked connections, but it's inconsistent with the `App.close()` contract advertised in `_app.py`.

**Fix:** Use `bv.App(http_url).validate(...)` only after wrapping in `with`, or use a `try/finally`. Given `validate` does zero network I/O, the cleanest pattern is:
```python
app = bv.App(http_url)
try:
    errs = app.validate(CheckoutDerivation)
    valid_errs = app.validate(TxEvent, UserProfileTable)
finally:
    app.close()
```

---

### IN-04: `_DURATION_RE` does not reject zero-valued durations (`"0s"`, `"0ms"`)

**File:** `python/beava/_schema.py:52`

**Issue:** The regex `r"^\d+(ms|s|m|h|d)$"` accepts `"0s"`, `"0ms"`, `"0d"`, etc. A zero-duration `keep_events_for` or `dedupe_window` is almost certainly a user error and will produce confusing server behavior (e.g., events are retained for 0 milliseconds). The server likely ignores or rejects it, but the SDK provides no feedback at decoration time.

This is an info item rather than a warning because the server is the authoritative validator. However, a client-side guard against zero-valued durations would improve the devex-first contract.

---

### IN-05: `FieldSpec` dataclass is not frozen — mutability is unintentional

**File:** `python/beava/_schema.py:28-44`

**Issue:** `FieldSpec` is a plain `@dataclass` (mutable). Given it is produced once at decoration time and read-only afterward, it should be `@dataclass(frozen=True)`. Making it mutable allows test code (or SDK internals) to accidentally mutate `spec.optional` or `spec.py_type` after schema extraction, producing hard-to-debug inconsistencies. The `default: Any` field holding `MISSING` prevents `frozen=True` from being entirely free (you cannot store mutable containers in a frozen dataclass), but since `default` is `Any`, this is already an escape hatch. Making the class frozen at least catches accidental attribute assignments.

---

_Reviewed: 2026-04-23_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
