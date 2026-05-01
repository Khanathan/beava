# Phase 37: `tally fork` convenience CLI + E2E demo - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "Option M minimal, demo-focused")

<domain>
## Phase Boundary

Add a `tally fork` CLI command that wraps `tally serve --replica-from ...` with scientist-friendly defaults. Ship the load-bearing E2E test that demonstrates the full data-scientist workflow end-to-end.

**In scope:**
- `tally fork --remote HOST --since T --streams S --keys K --token T [--local-port 7400] [--pipeline-file path]` — thin exec wrapper.
- One canonical E2E pytest that's both the demo script and the regression coverage for the whole Option M stack.

**Out of scope:**
- Interactive CLI mode, TUI, progress bars during catchup.
- Multiple concurrent fork sessions.
- Snapshot seeding (dropped from MVP).
- Deleting v0 embedded-client surfaces — Phase 38.

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### `tally fork` implementation
- Pure shell-out: parses its own flags, translates to `--replica-*` equivalents, exec's the server binary.
- Defaults chosen for scientist ergonomics:
  - `--local-port 7400` if omitted.
  - `--since "1970-01-01T00:00:00Z"` (full history) if omitted.
  - `--replica-block-until-catchup=true`.
- Admin token: `--token` or `TALLY_REPLICA_TOKEN` env var. Required.
- Output: prints "forking <remote>:<since> to localhost:<port>, catchup in progress..." and then stdout-forwards the server's logs.

### E2E pytest
- One load-bearing test. Call it `test_fork_demo.py`. If this passes, Option M is demo-ready.
- Test flow:
  1. Start prod server.
  2. Seed fixture events (multiple streams + keys + timestamps).
  3. Author a simple scientist pipeline (`@tl.stream` + `@tl.table` computing a count aggregate per key).
  4. Serialize it to a REGISTER JSON file.
  5. Launch `tally fork --remote ...:PROD_PORT --since T0 --streams Transactions --keys u1,u2 --pipeline-file /tmp/scientist-pipeline.json --local-port LOCAL_PORT`.
  6. Wait for catchup (poll `/debug/ready` or log-match).
  7. Connect `tl.Client(remote="localhost:LOCAL_PORT")`.
  8. Query the scientist's aggregate → assert historical correctness.
  9. Push more events to prod → wait 2s → re-query replica → assert live-updated correctness.
  10. Stop fork; stop prod.

### What the E2E is NOT testing
- Failure/reconnect paths (those live in Phase 36-01 T2 unit tests).
- Multiple concurrent fork processes.
- Large-scale performance.
- Scope-mismatch edge cases (those live in Phase 27 + 35 tests).

### Plan split
- One plan (37-01), two tasks:
  1. `tally fork` CLI subcommand (either inside `tally_cli` binary or top-level `tally` binary — confirm in plan).
  2. The E2E test.

</decisions>

<code_context>
- `src/bin/tally_cli.rs` — existing CLI. The `fork` subcommand could live here. OR it could be `tally fork` on the main `tally` binary. Confirm in plan — easier-for-scientist is `tally fork` (main binary) since they'll want "one binary".
- `src/main.rs` — if we put `fork` on the main binary, dispatch here.
- `tests/integration/` — pytest integration test lives here.
- Existing Python SDK (`python/tally/` → `python-native/python_src/tally/`) — test uses it to author scientist pipeline and push events.

</code_context>

<specifics>
- Pipeline-file format: look at what the existing server's `/register` HTTP endpoint expects. The test constructs one via `tl.register_payload(pipeline)` or equivalent, writes it to a temp file, passes to `tally fork --pipeline-file`.
- If no `/debug/ready` endpoint exists, add a minimal one as part of the plan (returns 200 once catchup-done signal fires). ~10 lines of axum.
- Subprocess orchestration mirrors `test_tally_clone.py` / `test_pipeline_e2e.py`.

</specifics>

<deferred>
- Progress bar / ETA during catchup.
- `tally fork` recovery across local restarts.
- Docs / tutorial page — later phase.
- `tally demo` one-command "start prod, start fork, run sample pipeline" — nice-to-have, not MVP.

</deferred>

---

*Phase: 37-tally-fork-e2e*
*Source: user directive 2026-04-15*
