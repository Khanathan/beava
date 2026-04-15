---
phase: 30-python-pipeline-api
plan: 01
subsystem: python-pipeline-api
tags: [pyo3, maturin, python, client, replica, phase-30]
one_liner: "Ship tally.Pipeline PyO3 extension with typed error hierarchy; Linux x86_64 abi3 wheel built by maturin."

dependency-graph:
  requires:
    - Phase 28: tally::client::{FrozenClient, run_clone, OutOfScopeError, SessionMode::Historical}
    - Phase 28: tally::client::wire::Scope (client-side duplicate, compile-time aligned)
    - Phase 27: OP_SNAPSHOT_FETCH server opcode
  provides:
    - "tally.Pipeline Python class (historical mode only; streaming mode gated with NotImplementedError)"
    - "tally.TallyError + 4 typed subclasses (OutOfScopeError, ClientConnectError, HandshakeError, ReplicaStateError)"
    - "Linux x86_64 abi3 wheel built by maturin (works on Python 3.10+)"
    - "python-native crate as workspace member"
  affects:
    - "python/tally/ is now a symlink to python-native/python_src/tally/ (canonical home)"
    - "python/tally/__init__.py re-exports Pipeline + error types from tally._native under try/except ImportError"
    - "CI gains python-native job (maturin build --release --strip + fresh-venv install + pytest)"

tech-stack:
  added:
    - pyo3 0.22 (extension-module + abi3-py310)
    - pythonize 0.22
    - maturin 1.7+ (build backend for python-native)
  patterns:
    - "abi3 stable-ABI to avoid per-minor-version wheels"
    - "Python::allow_threads around blocking tokio::block_on for GIL release (T-30-03 mitigation)"
    - "create_exception! hierarchy registered in #[pymodule] under bare names"

key-files:
  created:
    - python-native/Cargo.toml
    - python-native/pyproject.toml
    - python-native/README.md
    - python-native/src/lib.rs
    - python-native/src/pipeline.rs
    - python-native/src/errors.rs
    - python-native/python_src/tally/ (moved from python/tally/)
    - python-native/tests/__init__.py
    - python-native/tests/conftest.py
    - python-native/tests/test_pipeline_unit.py
    - python-native/tests/test_pipeline_errors.py
    - python/tally/_native.pyi
  modified:
    - Cargo.toml (added [workspace] with python-native member)
    - python/tally/__init__.py (re-exports from tally._native with ImportError fallback)
    - python/tally (now a symlink → ../python-native/python_src/tally)
    - .github/workflows/ci.yml (added python-native job)
    - .gitignore (ignore dev-installed _native*.so in source tree)

decisions:
  - "Wheel distribution name = 'tally' (single-wheel install), native extension lives at tally._native. Users write `from tally import Pipeline`."
  - "abi3-py310 stable ABI used so one wheel covers Python 3.10/3.11/3.12+."
  - "Tests live under python-native/tests/ (not python/tests/) to avoid pytest picking up python/pyproject.toml as rootdir and shadowing the installed wheel with the source tree."
  - "pyo3 extension-module feature avoids linking libpython — builds without Python.h dev headers."
  - "serde_json::Value via pythonize chosen for value conversion (SerializableEntityState is Serialize, no hand-rolled mapping needed)."

metrics:
  duration: "~65 min"
  completed: 2026-04-14
  tasks_completed: 3
  tests_added:
    - "python-native: 23 pytest pass + 1 planned skip (out-of-scope round-trip deferred to Plan 30-02 E2E)"
  rust_tests_total: 1252  # unchanged from baseline
---

# Phase 30 Plan 01: Python Pipeline API (PyO3 extension) Summary

## What shipped

A new `python-native/` cdylib crate that compiles to `tally/_native.abi3.so`
via maturin, exposing:

- `tally.Pipeline` — `__init__`, `run`, `get(key, stream)`, `inspect`, plus a
  test-only `_debug_effective_token` helper for the `TALLY_TOKEN` env-var
  fallback assertion.
- Typed exception hierarchy: `TallyError` (base) + `OutOfScopeError`,
  `ClientConnectError`, `HandshakeError`, `ReplicaStateError`.
- `tally._native.pyi` hand-written type stubs for IDE autocomplete.
- CI job `python-native` that runs `maturin build --release --strip`,
  installs the wheel in a fresh venv, then runs the unit + error tests.

## Final naming decision

The plan proposed `tally._native` as a submodule under the existing
`tally` distribution — implemented as written. The existing pure-Python
hatch build at `python/` continues to distribute `tally` too; its
`__init__.py` now has a `try/except ImportError` around the native
re-exports so it still works without the extension.

Alternatives considered and rejected (same as in the plan):
- A separate `tally_replica` distribution — two imports in user code is bad DX.

## Phase 28 Rust surface used (vs. plan assumptions)

The plan's `<assumptions>` block hypothesised a `tally::client::Session` +
`ClientConfig` + `Session::run` + `Session::state_store` +
`tally::client::StateError::OutOfScope` surface from an in-flight Phase 29.
**Phase 29 hasn't landed yet**, but Phase 28's `FrozenClient` + `run_clone`
ship an equivalent one-shot historical surface. We used that directly:

| Plan assumption                               | Actual (Phase 28) surface used                                   |
| --------------------------------------------- | ---------------------------------------------------------------- |
| `tally::client::Session::new(ClientConfig)`   | `tally::client::clone::run_clone(&CloneArgs)` (async)            |
| `Session::run()` then `session.state_store()` | `run_clone(&args).await?` returns `FrozenClient` already         |
| `StateStore::get(stream, key)`                | `FrozenClient::get(stream, key) -> Result<_, OutOfScopeError>`   |
| `StateStore::inspect()`                       | Computed in Python-native layer via `FrozenClient::iter_entities` |
| `ClientError::{Connect,Handshake,ReplicaState}` | `CloneError::{AuthFailed,FetchFailed,Protocol,Io,Decode}`      |
| `StateError::OutOfScope`                      | `OutOfScopeError` returned directly by `FrozenClient::get`       |

Error mapping in `python-native/src/errors.rs::map_clone_error`:
- `AuthFailed` → `HandshakeError`
- `FetchFailed`, `Io` → `ClientConnectError`
- `Protocol`, `Decode`, `StreamingNotSupported` → `ReplicaStateError`
- scope violations (from `FrozenClient::get`) → `OutOfScopeError` (mapped in `pipeline.rs`, not `errors.rs`)

## Wheel produced

```
target/wheels/tally-0.1.0-cp310-abi3-manylinux_2_34_x86_64.whl  (~597 KB)
```

ABI tag `cp310-abi3` means the same wheel loads on CPython 3.10, 3.11,
3.12, and any future 3.x (stable ABI). Platform tag
`manylinux_2_34_x86_64` restricts us to reasonably-current glibc — fine
for v0 since we're not shipping to PyPI yet.

## How to build + install locally

```bash
cd python-native
python -m venv /tmp/tally-venv
/tmp/tally-venv/bin/pip install --upgrade pip
/tmp/tally-venv/bin/pip install 'maturin>=1.7,<2.0'

# Release wheel:
/tmp/tally-venv/bin/maturin build --release --strip
/tmp/tally-venv/bin/pip install ../target/wheels/tally-*.whl

# Or for iterative dev (installs into the venv on `$VIRTUAL_ENV`):
source /tmp/tally-venv/bin/activate
maturin develop --release
```

No system Python dev headers (`Python.h`) are required because we depend on
`pyo3`'s `extension-module` feature, which elides the libpython link step.

## Value conversion approach

`FrozenClient::get` returns `Option<SerializableEntityState>`, which is
already `Serialize + Deserialize`. We go Rust value → `serde_json::Value`
→ Python via `pythonize::pythonize(py, &json)`. This avoids writing a
bespoke `PyDict` builder and keeps the Python surface in sync with
whatever fields future snapshots add.

Trade-off: double-convert cost vs. single-pass hand-rolled conversion.
Fine for v0 — `.get()` is called from Python per-query, not per-event.

## Deviations from Plan

### 1. [Rule 3 - Blocking] Moved Python source tree so maturin could find it

**Found during:** Task 1 (first wheel build).
**Issue:** Maturin 1.13's `[tool.maturin] python-source = "../python"`
does resolve the path (debug log confirms `python_module=Some("/data/home/tally/python/tally")`),
but the wheel-packaging walker silently drops the file list when the
directory is outside the Cargo project root. Symlinks (`python_src -> ../python`,
both relative and absolute) are not followed either. Hardlinks work but
are fragile across edits.
**Fix:** Physically moved `python/tally/` → `python-native/python_src/tally/`,
and replaced `python/tally` with a **relative symlink** (`python/tally → ../python-native/python_src/tally`).
Maturin now packages the real directory; `pip install -e python/` +
existing pytest paths continue to work because symlink traversal works
for reads. Updated `[tool.maturin] python-source = "python_src"`.
**Files touched:** directory move, `python-native/pyproject.toml`.

### 2. [Rule 3 - Blocking] Moved unit + error tests to `python-native/tests/`

**Found during:** Task 2 verification.
**Issue:** With tests under `python/tests/` and `python/pyproject.toml`
declaring `testpaths = ["tests"]`, pytest treats `python/` as its
rootdir and implicitly puts it on `sys.path`. That imports the
source-tree `tally/` (via symlink) instead of the freshly-installed
wheel, so the native `_native.abi3.so` in the venv is shadowed and
`_HAS_NATIVE` comes back `False` → all 23 tests skip silently in CI.
**Fix:** Moved `test_pipeline_unit.py` and `test_pipeline_errors.py`
under `python-native/tests/` (with a small `conftest.py` + `__init__.py`).
CI runs `pytest` from `python-native/` so that tree's rootdir wins
and the installed wheel is used. Existing `python/tests/` conftest +
fixtures remain untouched.
**Files touched:** moved tests, created `python-native/tests/conftest.py`
and `python-native/tests/__init__.py`, updated CI `working-directory`.

### 3. [Rule 2 - Critical] Allow `clippy::useless_conversion` in pipeline.rs

**Found during:** `cargo clippy -p tally-native -- -D warnings` verification.
**Issue:** `#[pymethods]` expansions under pyo3 0.22 emit Into<PyErr>
conversions that clippy flags as useless. Three sites in pipeline.rs.
**Fix:** Crate-level `#![allow(clippy::useless_conversion)]` in
`pipeline.rs` with a comment pointing at the pyo3 0.23 upgrade. No
behaviour change; purely a lint suppression.

### 4. [Rule 2 - Critical] Allow `unexpected_cfgs` in lib.rs

**Found during:** initial `cargo check` with workspace `RUSTFLAGS="-D warnings"`.
**Issue:** `create_exception!` macro in pyo3 0.22 emits
`#[cfg(feature = "gil-refs")]` guards that trip rustc's check-cfg
lint (no such feature is declared in our crate). Known upstream issue
fixed in pyo3 0.23.
**Fix:** Crate-level `#![allow(unexpected_cfgs)]` in `lib.rs`. Delete
when we bump pyo3.

### 5. Test skip: OutOfScope round-trip via `.get()`

**Found during:** Task 2 write-up.
**Issue:** Triggering `OutOfScopeError` end-to-end requires a populated
`FrozenClient` — i.e. a real snapshot-fetch round-trip. Plan 30-01 is
explicitly unit-level (constructor validation + exception identity);
standing up a mock TCP server here duplicates work Plan 30-02 does.
**Fix:** Plan 30-01's `test_out_of_scope_get_raises_typed_error`
`pytest.skip`s with a TODO pointing at Plan 30-02's E2E suite. Error
class construction + identity is still verified directly.

## Auth gates

None. Plan 30-01 is all offline unit work.

## Threat Flags

None. The threat register from the plan covered the new surface; no
additional trust boundaries introduced.

## Verification evidence

| Check                                                | Result          |
| ---------------------------------------------------- | --------------- |
| `cargo check --workspace`                            | ✅ clean         |
| `cargo clippy -p tally-native --no-deps -- -D warnings` | ✅ clean      |
| `cargo fmt --check -p tally-native`                  | ✅ clean         |
| `cargo test` (main crate)                            | ✅ 1252 pass     |
| `maturin build --release --strip`                    | ✅ wheel ≈597 KB |
| fresh venv install + `from tally import Pipeline`    | ✅               |
| `issubclass(OutOfScopeError, TallyError)` (+ 3 more) | ✅               |
| `pytest python-native/tests/` (fresh-venv wheel)     | ✅ 23 pass, 1 skip |
| `pytest python/tests/` (existing SDK suite)          | ✅ 451 pass      |
| CI YAML parse + job presence                         | ✅               |

## Self-Check: PASSED

Each listed file exists at the recorded path; `_native.abi3.so` is
rebuilt on every `maturin develop` / `maturin build` and lives at
`python-native/python_src/tally/_native.abi3.so` (gitignored).
