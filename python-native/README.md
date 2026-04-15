# tally-native

PyO3 native extension for Tally's replica-client surface (Plan 30-01).

This crate compiles to a `cdylib` that becomes the `tally._native` submodule
inside the installed `tally/` Python package. Users then write:

```python
from tally import Pipeline, TallyError, OutOfScopeError
```

and get the full Phase 28 local-replica client.

## Why a separate crate?

The existing `python/` tree is a pure-Python hatch-built SDK (distribution
name `tally`) — it can't host a Rust `cdylib`. Rather than reshuffle the
SDK, Plan 30-01 ships the PyO3 bits as a sibling crate and configures
maturin (`python-source = "../python"`) to bundle both the hand-written
modules and the compiled `_native.so` in a single wheel.

## Build + install locally

```bash
cd python-native
python -m venv /tmp/tally-venv
/tmp/tally-venv/bin/pip install --upgrade pip
/tmp/tally-venv/bin/pip install 'maturin>=1.7,<2.0'
/tmp/tally-venv/bin/maturin build --release --strip
# produces target/wheels/tally-0.1.0-cp310-abi3-manylinux*_x86_64.whl

/tmp/tally-venv/bin/pip install target/wheels/tally-*.whl
/tmp/tally-venv/bin/python -c "from tally import Pipeline; print(Pipeline)"
```

`maturin develop --release` installs an editable build into the active venv
for iterative work.

## Platform support (v0)

- Python **≥ 3.10** (abi3 stable ABI; one wheel serves 3.10 / 3.11 / 3.12 / …).
- **Linux x86_64** only.

macOS, Windows, and ARM wheels are deferred past v0 per the Phase 30 context
(`.planning/phases/30-python-pipeline-api/30-CONTEXT.md`).

## Build requirements

Only `rustc` / `cargo` and the `maturin` Python package. Because we depend
on `pyo3` with the `extension-module` feature, the build does **not** link
against `libpython`, so system Python development headers (`Python.h`) are
not required.

## Layout

```
python-native/
├── Cargo.toml                # cdylib + pyo3 + tally (client feature)
├── pyproject.toml            # maturin backend, module-name = tally._native
├── README.md                 # this file
└── src/
    ├── lib.rs                # #[pymodule] _native — registers Pipeline + errors
    ├── pipeline.rs           # #[pyclass] Pipeline (__init__/run/get/inspect)
    └── errors.rs             # TallyError + 4 subclasses; map_clone_error
```

Python-facing pieces live in `../python/tally/`:

- `__init__.py` — re-exports from `tally._native` behind a `try/except ImportError`.
- `_native.pyi` — hand-written type stubs for IDEs.
- `tests/test_pipeline_unit.py`, `tests/test_pipeline_errors.py` — Plan 30-01 unit tests.
