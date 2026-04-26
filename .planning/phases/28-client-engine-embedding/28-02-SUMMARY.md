---
phase: 28-client-engine-embedding
plan: 02
subsystem: client-cli
tags: [cli, client, phase-28, stub, tdd]
requires: [28-01]
provides:
  - "tally_cli bin target (builds under both default and --features client --no-default-features)"
  - "parse_args(&[String]) -> Result<(Subcommand, ParsedArgs), String> — pure, unit-testable"
  - "ParsedArgs struct (Phase 29 arg surface frozen here)"
  - "resolve_token(flag, env_lookup) — injectable token resolver"
  - "stub handlers for clone / sync that print 'not implemented yet' and exit 0"
  - "--mode streaming gate: parser accepts, handler rejects with Phase 31 message (exit 2)"
affects:
  - Cargo.toml (added [[bin]] tally_cli entry; tally server bin explicitly gated)
  - src/bin/tally_cli.rs (new, 303 lines)
tech-stack:
  added: []
  patterns:
    - "hand-rolled argv parsing (std::env only) matching tally_suggest_config.rs style"
    - "injected env_lookup closure for deterministic token-precedence tests"
key-files:
  created:
    - src/bin/tally_cli.rs
  modified:
    - Cargo.toml
decisions:
  - "Streaming-mode handling: parser ACCEPTS --mode streaming, handler REJECTS. Cleaner error messaging and lets Phase 31 flip a single match arm to enable without re-parsing."
  - "No clap dep. Hand-rolled parser (~100 LOC) matches the existing crate style and preserves the 'no new top-level dependency' invariant from 28-CONTEXT."
  - "No required-features on tally_cli — builds under both default and client-only. This lets 28-03 wire the Session manager into tally_cli under client feature without a separate bin target."
  - "Token resolution uses an injected env_lookup closure (not std::env::set_var) for zero-flake tests."
metrics:
  duration: ~15min
  tasks: 1
  files_created: 1
  files_modified: 1
  tests_added: 8
  tests_pass: "8/8 (bin) + full suite green"
  lines: 303
completed: 2026-04-14
---

# Phase 28 Plan 02: tally_cli skeleton Summary

One-liner: Added `tally_cli` bin target with hand-rolled argv parsing for Phase 29's full flag surface, stub `clone`/`sync` handlers, and 8 unit tests — zero new dependencies, zero network code.

## Usage string

```
usage: tally_cli <SUBCOMMAND> [FLAGS]

subcommands:
  clone    Clone a scoped local replica from a tally server.
  sync     Resume / keep a local replica in sync with the server.

flags:
  --remote <host:port>        Server address (required).
  --streams <name>[,name...]  Streams to clone (required for clone).
  --keys <key>[,key...]       Key allow-list (mutually exclusive with --key-prefix).
  --key-prefix <prefix>       Key prefix scope (mutually exclusive with --keys).
  --mode historical|streaming Default: historical. streaming is Phase 31.
  --token <token>             Admin token (overrides TALLY_TOKEN env var).
  -h, --help                  Show this message.

environment:
  TALLY_TOKEN                 Admin token, used if --token not passed.

Phase 28 status: clone/sync are stubs; Phase 29 wires the real session.
```

## Arg-parsing edge cases covered

| # | Case | Expected outcome |
|---|------|------------------|
| 1 | `clone --remote foo:6400 --streams A,B` | parses; `handle_clone` → 0 |
| 2 | `sync --remote foo:6400` | parses (empty streams OK); `handle_sync` → 0 |
| 3 | `--mode streaming` (either sub) | parses, handler returns 2 with Phase 31 msg |
| 4 | missing `--remote` | `Err("--remote ... required")` |
| 5 | `--keys X --key-prefix Y` | `Err("mutually exclusive")` |
| 6 | token precedence | flag > env > None (injected env_lookup) |
| 7 | `-h` / `--help` / `clone --help` | `Err("__help__")` sentinel → exit 0 + usage |
| 8 | unknown subcommand (`foo`) | `Err("unknown subcommand ...")` |

Additional enforcement (not separately tested but covered by parser):
- `clone` without `--streams` → `Err("--streams is required for clone")`.
- flag without its required value (e.g., bare `--remote`) → `Err(... requires a value)`.
- `--mode foo` (not in historical/streaming) → `Err`.
- `--streams A,,B` / empty tokens trimmed out.

## Phase 29 inheritance

**Kept as-is (stable API):**
- `parse_args(&[String]) -> Result<(Subcommand, ParsedArgs), String>` — pure, no I/O. Phase 29 calls this verbatim.
- `ParsedArgs { remote, streams, keys, key_prefix, mode, token }` — the arg surface is frozen. Phase 29 adds behavior, not fields.
- `resolve_token(Option<String>, impl Fn(&str) -> Option<String>)` — call site in `main()` passes `|k| env::var(k).ok()`; tests pass deterministic closures.

**Expected Phase 29 replacements:**
- `handle_clone` / `handle_sync` stubs — replaced with real Session-manager + snapshot-fetch + log-consumer calls.
- `format_scope(&ParsedArgs)` — diagnostic helper; Phase 29 may keep it as `--dry-run` output.

**Explicit non-inheritance:** no `tally::client::*` import exists here (28-03 creates that module). `tally_cli` is currently dependency-free from the tally crate's own code — imports only `std`.

## Decision: streaming-mode parser vs handler rejection

Considered two designs:
1. **Parser rejects** `--mode streaming` outright.
2. **Parser accepts**, handler rejects with a Phase-31 pointer.

Chose (2). Rationale:
- Cleaner error message — parser errors are usage-style ("invalid value"), handler errors are feature-gate style ("not supported yet; Phase 31 will enable"). The latter is what users need.
- Phase 31 flips a single `if args.mode == "streaming" { ... return 2; }` guard to enable the feature — no parser changes, no re-testing of the flag grammar.
- `ParsedArgs.mode` remains a `String` in historical/streaming domain, so downstream code can branch on it once Phase 31 lands.

## Verification

- `cargo build --bin tally_cli` → green.
- `cargo build --no-default-features --features client --bin tally_cli` → green (builds under client feature, as required for 28-03).
- `cargo build` (default/server) → green.
- `cargo test --bin tally_cli` → 8/8 pass.
- `cargo test` full suite → all test binaries green, zero regressions from 28-01.
- Manual smoke (from `<verify>` block): all 4 invocations (clone OK, sync OK, streaming→2, missing-remote→2) behave per plan.

## Deviations from plan

None — plan executed exactly as written. One incidental note: `Cargo.toml` was separately updated (between my edit and my next read) to add an explicit `[[bin]] name = "tally"` entry with `required-features = ["server"]`, which formalizes the server-binary feature gate. This is orthogonal to 28-02's scope and leaves `tally_cli` correctly building under both features.

## Self-Check: PASSED

- `src/bin/tally_cli.rs` → FOUND (303 lines, exceeds min_lines=120).
- `Cargo.toml` contains `name = "tally_cli"` → FOUND.
- Commit hash recorded below.
