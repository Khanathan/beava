---
phase: 03-python-sdk-skeleton-decorators-expression-dsl
fixed_at: 2026-04-23T00:00:00Z
review_path: .planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-REVIEW.md
iteration: 1
findings_in_scope: 8
fixed: 7
skipped: 0
status: all_fixed
---

# Phase 03: Code Review Fix Report

**Fixed at:** 2026-04-23
**Source review:** `.planning/phases/03-python-sdk-skeleton-decorators-expression-dsl/03-REVIEW.md`
**Iteration:** 1

**Summary:**
- Findings in scope: 8 (1 Critical + 7 Warning; CR-01 and WR-07 fixed together)
- Fixed: 7 commits (8 findings — CR-01 and WR-07 addressed in one atomic commit)
- Skipped: 0

---

## Fixed Issues

### CR-01 + WR-07: Subprocess stdout pipe deadlock and dead elif branch in `_embed.py`

**Files modified:** `python/beava/_embed.py`
**Commit:** `2f1544f`
**Applied fix:**
- Rewrote `_reader()`: non-JSON lines now log at DEBUG post-startup instead of silently discarding; the dead `elif ready.is_set()` branch replaced with `if not ready.is_set(): ready.set() else: _log.debug(...)` so post-startup lines actually reach the logger.
- Added `ready.set()` at EOF of the `_reader` loop so the caller wakes immediately when the process exits without emitting bind events (instead of waiting the full `startup_timeout`).
- Changed the timeout check from `if not ready.wait(timeout=...)` to `ready.wait(timeout=...); if not (http_addr and tcp_addr):` — handles the case where EOF fires `ready` but no bind events arrived (process crashed early).
- Added `proc.stdout.close()` in the timeout/early-exit path to explicitly release the pipe fd and unblock the `_reader` thread.

---

### WR-01: `topo_sort` raises `ValueError` instead of documented exception type

**Files modified:** `python/beava/_validate.py`
**Commit:** `fb3815d`
**Applied fix:**
- Added `RegistrationError` to the import from `beava._errors` (alongside the existing `ValidationError` import). Note: `ValidationError` is a frozen dataclass, not an `Exception` subclass, so it cannot be raised directly — `RegistrationError` is the correct raiseable exception type.
- Changed both `raise ValueError(str(err))` calls in `topo_sort` to `raise RegistrationError(code="cycle", path=..., message=...)`.
- Updated docstring: `Raises: ValidationError` → `Raises: RegistrationError (code='cycle')`.

---

### WR-02: `decode_frame` / `read_frame` missing `length < 3` LengthUnderflow guard

**Files modified:** `python/beava/_wire.py`
**Commit:** `71ba607`
**Applied fix:**
- Added `if length < 3: raise IncompleteFrame(...)` immediately after unpacking the length field in both `decode_frame` (after line 115) and `read_frame` (after reading `len_bytes`). Mirrors Rust server `FrameError::LengthUnderflow`. Uses `IncompleteFrame` (existing exception) to preserve the two-exception convention.

---

### WR-03: `conftest.py` fixture leaks stdout pipe fd on server timeout

**Files modified:** `python/tests/conftest.py`
**Commit:** `94903a8`
**Applied fix:**
- Added `if proc.stdout: proc.stdout.close()` between `proc.wait()` and `pytest.fail()` in the `beava_server` fixture timeout branch. This releases the OS-level file descriptor and unblocks the daemon `_reader` thread, preventing fd exhaustion on CI.

---

### WR-04: No `__del__` safety net for eagerly-created transport in `App`

**Files modified:** `python/beava/_app.py`
**Commit:** `29c80f0`
**Applied fix:**
- Added `__del__` method to `App` that checks `not self._closed and self._transport is not None` and calls `self._transport.close()` inside a bare `except Exception: pass` guard. Sets `self._closed = True` after cleanup. Uses the existing `_closed` flag to ensure at-most-once cleanup; exceptions are swallowed so GC never raises.

---

### WR-05: Function-form decorators accept non-descriptor objects via `hasattr(_name)` check

**Files modified:** `python/beava/_events.py`, `python/beava/_tables.py`
**Commit:** `79e22ef`
**Applied fix:**
- In `_decorate_event_function` (`_events.py` line ~232): expanded the guard from `not hasattr(upstream_cls, "_name")` to also require `hasattr(upstream_cls, "_beava_kind")`. The `_beava_kind` class attribute is defined on all four Beava descriptor types and is a Beava-specific sentinel not shared with arbitrary Python objects.
- Applied the identical change to `_decorate_table_function` in `_tables.py`.

---

### WR-06: `extract_schema` silently drops inherited annotations

**Files modified:** `python/beava/_schema.py`, `python/tests/test_schema.py`
**Commit:** `84b2cb4`
**Applied fix:**
- Replaced `annotation_order = list(getattr(cls, "__annotations__", {}).keys())` with a full MRO walk: iterates `reversed(cls.__mro__)` (base-classes-first order, excluding `object`), collecting field names into `annotation_order` with a `seen_ann` set to deduplicate. This mirrors how `typing.get_type_hints()` already merges the MRO, keeping `annotation_order` and `hints` in sync.
- Added `test_extract_schema_includes_inherited_annotations` to `tests/test_schema.py` verifying that a subclass schema includes parent-declared fields in the correct order (base before derived).

---

## Skipped Issues

None — all 8 in-scope findings were fixed.

---

_Fixed: 2026-04-23_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
