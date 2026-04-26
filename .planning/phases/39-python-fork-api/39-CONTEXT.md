# Phase 39: Python-native `tl.fork()` DX layer - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "user should define scope and everything in python, not command line")

<domain>
## Phase Boundary

Add a single Python entry point `tl.fork(...)` that wraps the Phase 37 `tally fork` CLI. Scientists define scope, streams, and pipelines as Python objects (using the existing `@tl.stream` / `@tl.table` decorators) and get back a handle they can query against. No JSON hand-authoring, no `tally fork` shell command in the user's script.

**In scope:**
- `tl.fork(remote=..., streams=[...], keys=[...]|None, key_prefix=None, since=..., token=..., pipelines=[...]|None, local_port=None) -> ForkedReplica`
- `ForkedReplica` class: `.get(pipeline_or_stream, key=...)`, `.inspect()`, `.stop()`, context-manager support.
- Pipelines auto-serialized (via existing `_to_register_json()` / `encode_register()` helpers — no new Rust).
- Subprocess management: spawns `tally fork` with generated `--pipeline-file`, polls `/debug/ready`, shuts down on exit.
- Integration test in Python that demonstrates the scientist workflow end-to-end.

**Out of scope:**
- Replace Phase 37 `tally fork` CLI — stays for power users.
- Register pipelines AFTER fork is running (backfill-on-register) — that's a bigger server feature, defer.
- Streaming event iterator `for ev in fork.events(...)` — Phase 31-02 removed this surface; stays out of MVP.
- Windows / macOS subprocess edge cases — Linux x86_64 demo is enough.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Pure Python addition on top of existing CLI.** Zero Rust. Zero new SDK primitives — reuse `_to_register_json()` and `encode_register()` that already exist in `python/tally/_app.py`. If the wrapper doesn't deliver, the scientist still has `tally fork` available.

### `tl.fork()` signature
```python
def fork(
    remote: str,
    streams: list[type],              # @tl.stream classes
    keys: list[str] | None = None,
    key_prefix: str | None = None,
    since: str | int = "1970-01-01T00:00:00Z",
    token: str | None = None,          # falls back to TALLY_REPLICA_TOKEN env
    pipelines: list = None,            # @tl.table descriptors
    local_port: int | None = None,     # auto-allocated if None
    binary_path: str | None = None,    # default: "tally" on PATH, override for tests
    ready_timeout: float = 30.0,
) -> ForkedReplica: ...
```

Validation at call time (before any subprocess):
- `streams` non-empty; each element is a `@tl.stream` class. Reject if anything's not.
- `keys` xor `key_prefix`.
- `pipelines` optional but if present, each must have `_to_register_json()`.
- `token` required (inline or via env).
- Reject `streams` containing duplicates.

### Serialization path
- Build a REGISTER JSON doc by calling `_to_register_json()` on each stream class AND each pipeline descriptor, same pattern as `App.register()` in `python/tally/_app.py`.
- Concatenate into a single seed JSON per the format Phase 36's `seed_pipelines_from_file` expects (check what format it reads — either a single object with lists, or newline-delimited JSON — mirror whatever's there).
- Write to temp file via `tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)`. Store the path on the `ForkedReplica` so `.stop()` can clean it up.

### Subprocess management
- Spawn `<binary_path> fork --remote HOST --streams s1,s2 --keys k1,k2 --since T --token T --local-port P --pipeline-file /tmp/xxx.json`.
- Capture stdout + stderr to the fork's log files (temp-file-based; location accessible via `ForkedReplica.log_path` for debugging).
- Poll `GET http://127.0.0.1:{local_port}/debug/ready` every 200ms up to `ready_timeout`; raise `ForkTimeoutError` if not ready in time.
- `ForkedReplica.stop()` sends SIGTERM, waits 5s, then SIGKILL. Cleans up temp files.
- Context manager: `__enter__` returns self (after `fork()` has already started the subprocess), `__exit__` calls `stop()`.

### `ForkedReplica` query surface
- `.get(pipeline_or_stream, key: str) -> dict | None` — resolves the feature name from the argument (pipeline descriptor → use its `_register_name`; stream class → uses class name), calls `GET http://127.0.0.1:{port}/debug/key/{key}` via the existing `tl.Client` under the hood, returns the `computed_features` dict OR None.
- `.inspect() -> dict[str, int]` — iterates streams in scope, returns `{stream_name: num_keys_seen}` by calling existing debug endpoints. If no existing endpoint gives this cleanly, build it by calling `/debug/key/{key}` per declared scope key — acceptable for demo.
- `.stop()` — idempotent.
- `.local_url` property — `"http://127.0.0.1:{port}"`. For scientists who want to drop to raw `requests` or `tl.Client`.

### Error hierarchy
- `ForkError` base class.
- `ForkTimeoutError(ForkError)` — catchup didn't hit ready within timeout.
- `ForkSubprocessError(ForkError)` — binary exited unexpectedly during start.
- `ForkValidationError(ForkError)` — caller arg errors (before subprocess spawn).

### Plan split
- One plan (39-01). Two tasks: (T1) Python module implementation + unit tests. (T2) Integration test that mirrors Phase 37's E2E but in Python-only form.

</decisions>

<code_context>
- `python/tally/__init__.py` — exposes `@tl.stream`, `@tl.table`, `tl.Client`, `tl.App`. Add `tl.fork`, `tl.ForkedReplica`, `tl.ForkError`, `tl.ForkTimeoutError`, `tl.ForkSubprocessError`, `tl.ForkValidationError`.
- `python/tally/_app.py` — existing `App.register()` shows the pattern. Uses `_to_register_json()` on descriptors and `encode_register()` for payloads.
- `python/tally/_client.py` — existing HTTP client. `ForkedReplica` delegates to this against localhost.
- `tests/integration/test_fork_demo.py` (Phase 37) — reference for the subprocess orchestration pattern. Our Python test mirrors the logic at a different level.
- `src/main.rs` (Phase 37) — `tally fork` subcommand flags. Don't modify; just call.

</code_context>

<specifics>
- **Seed file format**: check what Phase 36's `seed_pipelines_from_file` actually reads. If it expects a JSON array of REGISTER payloads vs a single REGISTER-bundle object, mirror it exactly. If format is surprising, flag via stop-and-report rather than improvising.
- **Port allocation**: if `local_port=None`, use `socket.socket(); s.bind(('',0)); s.getsockname()[1]; s.close()` to pick a free port. Small race but fine for demo.
- **Subprocess stderr inspection**: if `/debug/ready` times out, read the tail of the fork's stderr log and include in `ForkTimeoutError` message so scientists see what went wrong.
- **`@tl.table` descriptors**: they are function objects with attributes stamped by the decorator. The serialization path should tolerate both `@tl.table` and `@tl.stream` class objects; use `hasattr(obj, "_to_register_json")` duck-typing to match `_app.py:130`.

</specifics>

<deferred>
- Backfill-on-register (`fork.add_pipeline(p).backfill()`) — future phase.
- Events iterator `fork.events(stream, key)` — waits until we have a clean server-side watch endpoint without the Option K baggage.
- Cross-platform subprocess polish — v0 is Linux x86_64.
- Hot-reload pipelines without restart — power feature.

</deferred>

---

*Phase: 39-python-fork-api*
*Source: user directive 2026-04-15 — "user should define scope and everything in python, not command line"*
