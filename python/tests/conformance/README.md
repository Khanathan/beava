# Cross-SDK Conformance Harness

Single-orchestrator harness per Phase 13.6 D-03. Drives Python, TypeScript, and
Go SDKs against the same `scenario.json` and asserts identical outputs (catches
wire-format drift between language implementations immediately).

## Run

```bash
python -m pytest python/tests/conformance/test_cross_sdk.py -v
```

Skips automatically when:

- The `beava` binary cannot be discovered (`BEAVA_BINARY` env / `which beava` /
  `<workspace>/target/debug/beava`).
- Neither `node` nor `go` is on PATH.
- The Python SDK lacks `bv.App.register_json` (Plan 13.5 lands this; until then,
  the Python branch is silently skipped while TS+Go still verify against
  `scenario.expected`).
- The engine binary's wire shape lags `docs/wire-spec.md` (i.e., still rejects
  `kind: "table"`, expects `keys` instead of `key` in `/get`, etc.). The
  orchestrator detects these "engine alignment" errors and skips with a clear
  diagnostic — this means Phase 13.4 needs to land more before the harness goes
  fully green.

## What it does

1. Loads `scenario.json` (single source of truth: `register_payload`,
   `events`, `gets`).
2. For each available SDK adapter:
   - Spawns an embed-mode beava instance.
   - Sends the register payload verbatim (TS+Go are communicate-only — no DSL).
   - Replays the events.
   - Issues the gets.
   - Returns `[{...}, {}, ...]` — one row per get.
3. Asserts each adapter's results match `scenario.expected`.
4. Asserts pairwise agreement across adapters (transitive via expected, but
   explicit for clearer failure messages).

## Add a new scenario

Edit `scenario.json`. Both `register_payload` and `events`/`gets` are passed
through verbatim by all 3 adapters — no per-language code change needed.

## Add a new SDK

Add a `run_<lang>.<ext>` adapter that:

1. Reads `scenario.json` from `argv[1]`.
2. Registers, pushes, gets per the scenario.
3. Prints `{"sdk": "<lang>", "results": [...]}` to stdout.

Then add a branch in `test_cross_sdk.py` that runs the adapter and compares.

## Architecture

- **TS adapter** (`run_ts.ts`) — uses `node --experimental-strip-types` (Node
  22+) to run TypeScript directly without a build step or `tsx` runtime
  dependency. Imports the SDK from the in-tree built `dist/index.js` (compiled
  on demand by the orchestrator if absent).
- **Go adapter** (`run_go.go`) — single-file program in a local `go.mod` with a
  `replace` directive pointing to `../../../sdk/go` (the in-tree SDK source).
- **Python branch** — in-process `import beava as bv`; gated on
  `bv.App.register_json` until Plan 13.5 lands the new App surface.

## Known gaps (cross-phase handoff)

| Gap | Owner | Status |
|-----|-------|--------|
| `bv.App.register_json` JSON pass-through helper | Phase 13.5 | Pending — Python branch skipped meanwhile |
| Engine accepts `kind: "table"` (per ADR-001 partial-overturn 2026-05-03) | Phase 13.4 | Pending — engine still rejects per Phase 12.7 invariant; alignment-error skip in orchestrator |
| `/get` wire body shape (`{table, key}` vs current `{table, keys, features}`) | Phase 13.4 | Pending — alignment-error skip in orchestrator |
| Push wire body shape (`{fields: ...}` wrapper vs flat `Row`) | Phase 13.4 | Pending — alignment-error skip in orchestrator |
