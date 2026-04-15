# Phase 30: Python Pipeline API + local query surface - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Ship the user-facing Python + CLI surface for querying a local replica. `tally.Pipeline(remote=..., streams=..., keys?=..., mode="historical").run()` bootstraps and catches up against a live server, then `.get(key, stream=...)` / `.inspect()` query the resulting in-memory state. `tally query` / `tally inspect` CLI wraps the same Rust code for shell usage.

**In scope:** PyO3-based Python extension built with maturin, the `Pipeline` class and `OutOfScopeError` type, `tally query` / `tally inspect` CLI subcommands, Linux x86_64 wheel, pytest integration coverage.

**Out of scope:** Streaming mode (`.watch()`) — Phase 31. DAG-derived automatic scope — deferred indefinitely. macOS / Windows / ARM wheels — post-v0. Writer-side APIs on the client — Phase 34 (stretch).

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** One binding path (PyO3), one distribution target (Linux x86_64), explicit scope only, minimum viable query methods.

### A1 — Python binding: PyO3 via maturin
- New top-level directory `python/` (or reuse if one already exists) with `Cargo.toml` declaring `crate-type = ["cdylib"]` and a `pyproject.toml` driving maturin.
- PyO3 binds a thin `Pipeline` class backed by the Phase 28/29 Rust client `Session` + `StateStore`.
- Build output: `tally-*.whl`. Users: `pip install tally` (or install from wheel for demo).
- `.run()` releases the GIL while the bootstrap/catchup loops execute.
- All exceptions from Rust cross as typed Python exceptions — `OutOfScopeError`, `ClientConnectError`, `HandshakeError` etc. subclass a base `tally.TallyError`.

### B1 — Scope source: explicit only
- `Pipeline(streams=["Transactions"], keys=["u1", "u2"], ...)` — same shape as Phase 29 CLI flags.
- Validation in the constructor: non-empty streams, keys xor key_prefix, valid `mode`.
- No DAG walking. No pipeline-definition objects.

### C1 — Query surface
- **`Pipeline.__init__(remote, streams, keys=None, key_prefix=None, mode="historical", token=None, since=None)`**: constructs, validates, stores config. Does not connect.
- **`.run()`**: blocking. Runs the full bootstrap → catchup sequence. Returns when mode reaches `Done`. Raises on terminal connect/handshake failure.
- **`.get(key, stream)`**: returns the current state for `(stream, key)` if in scope. Raises `OutOfScopeError` otherwise. Returns `None` if in scope but not seen (null-collapse consistent with v0 query model).
- **`.inspect()`**: returns `{stream_name: key_count}` for all in-scope streams. Minimal but enough for demo + debugging.
- **Streaming-mode attempts**: `Pipeline(..., mode="streaming").run()` raises `NotImplementedError("streaming mode ships in Phase 31")`.

### D1 — CLI subcommands
- Fill in `tally query` and `tally inspect` on the `tally` binary (Phase 28 already registered subcommand skeletons — Phase 30 either uses those or adds them if they weren't scaffolded).
- `tally query --remote HOST:PORT --streams S --key K [--key-prefix P] [--token T] --key LOOKUP_KEY --stream LOOKUP_STREAM` — runs a one-shot `Pipeline(...).run(); get(key)` and prints the result.
- `tally inspect --remote HOST:PORT --streams S [--keys...]` — prints the `.inspect()` dict as JSON.
- Shares zero Python — both CLI commands are pure Rust using the same underlying client primitives. The only PyO3 surface is the Python extension.

### E1 — Distribution: Linux x86_64 only for v0
- maturin build produces `tally-{ver}-cp3x-manylinux2014_x86_64.whl`.
- Python ≥ 3.10. Drop older to avoid the 3.9 EOL mess.
- No PyPI publish step in Phase 30 — user installs from local wheel or git+maturin for now. PyPI is a separate launch step after v0 proves out.
- Document in the plan how to build + install locally.

### F — Plan split (2 plans)
- **30-01**: `python/` crate scaffold, `pyproject.toml`, maturin config, PyO3 `Pipeline` class with `__init__`, `.run()`, `.get()`, `.inspect()`, typed exception hierarchy. Python unit tests covering construction validation and error mapping.
- **30-02**: `tally query` / `tally inspect` CLI wiring in `src/bin/tally_cli.rs`. End-to-end pytest: spin up a real server, push fixture events via existing Python SDK, `Pipeline(...).run()`, assert `.get()` returns expected values and `OutOfScopeError` for out-of-scope keys.

### Error type hierarchy
```
tally.TallyError                    (base)
├── tally.OutOfScopeError           (scope boundary violation at .get())
├── tally.ClientConnectError        (TCP connect / reconnect exhausted)
├── tally.HandshakeError            (scope rejected by server)
└── tally.ReplicaStateError         (invariant violation, e.g., non-monotonic seq)
```

### GIL behavior
- `.run()` releases the GIL (`Python::allow_threads`) so the blocking bootstrap/catchup doesn't freeze the interpreter. Important even in single-threaded scripts because signal handlers (Ctrl-C) need to fire.
- `.get()` and `.inspect()` are fast enough to hold the GIL; no `allow_threads`.

### Type stubs
- Ship a `tally.pyi` hand-written stub file with the `Pipeline` class and error types. Good DX for IDE autocomplete. Five minutes of work; big UX payoff.

</decisions>

<code_context>
## Existing code touchpoints

- Existing `python/` SDK directory from v0 (contains the `@tl.stream` / `@tl.table` decorators, Python client, pytest suite). The new maturin extension is a *separate* package — call it `tally` (or a distinct name to avoid conflict if the existing one is also `tally`; confirm in the plan).
- Phase 28 feature-flagged `client` build of the main crate — PyO3 extension consumes `tally` as a dependency with `default-features = false, features = ["client"]`.
- Phase 29 `Session` + bootstrap + catchup + `StateStore` — exposed via a narrow public API for PyO3 to call.
- Phase 28 `src/bin/tally_cli.rs` — add `query` / `inspect` subcommands.
- Existing pytest harness conventions in `tests/integration/` — follow them for the E2E test.

</code_context>

<specifics>
## Specific technical notes

- **Package naming conflict**: if the existing Python SDK package is already named `tally`, the PyO3 extension needs a distinct name (e.g., `tally_replica`) or it needs to live under a submodule of the existing package (e.g., `tally._native`). Inspect `python/pyproject.toml` in the plan before naming.
- **Build dependencies**: maturin requires `rustc` and a matching Python dev header. Document in the plan's "how to build" section.
- **CI coverage**: add one CI job that runs `maturin build --release --strip` and installs the resulting wheel in a fresh venv + runs the pytest suite. Linux only.
- **Token handling**: the `token=None` arg accepts a string; if `None`, falls back to `TALLY_TOKEN` env var at `.run()` time. Matches the CLI convention from Phase 28.

</specifics>

<deferred>
## Deferred

- `.watch(key)` streaming API — Phase 31
- macOS / Windows / ARM wheels — post-v0
- PyPI publish — separate launch step post-v0
- Automatic scope from DAG analysis — never, unless explicit user ask in v0.2+
- Write-back `.promote()` — Phase 34 (stretch)
- Python 3.9 support — dropped permanently

</deferred>

---

*Phase: 30-python-pipeline-api*
*Sources: `.planning/research/local-replica-design.md`, `.planning/phases/27-29-CONTEXT.md` chain, user directive 2026-04-14 "easiest for v0 and demo"*
