# Phase 28: Client engine embedding - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Make the engine runnable in a no-listener client context, and stub the `tally` CLI with `clone` / `sync` subcommands. Server-side replica endpoints (Phase 27) must exist; Phase 29 will wire the session manager and log consumer against this scaffolding.

**In scope:** Rust-side engine embedding, client-usable `StateStore`, `PipelineEngine::apply_event` with listeners/signals/metrics disabled, `tally` CLI binary with `clone`/`sync` subcommand skeletons (no network wiring yet — that's 29).

**Out of scope:** Session manager + TCP reconnect (Phase 29), log consumer that calls `apply_event` per entry (Phase 29), Python SDK (Phase 30), streaming mode (Phase 31), state persistence (Phase 32, stretch).

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Minimal crate surgery, minimal feature work. Every structural choice picks the cheapest path consistent with Phase 29+ being able to build on top.

### A1 — Crate split: feature-flag `client` on existing crate
- Add `client` and `server` features to the main `tally` crate's `Cargo.toml`.
- `server` is `default`; `client` strips server-only deps via `#[cfg(feature = "server")]` gates.
- Server-only modules: TCP listener, HTTP listener, SignalRegistry writer side, ingest networking, any `tokio::net` listeners.
- Client-only modules: the new `tally` CLI client subcommands, stub session/log-consumer types (filled in by Phase 29).
- Shared: `PipelineEngine`, `StateStore` (memory layer), `apply_event`, event decoding, scope types (from Phase 27), snapshot reader (Phase 27 filter-iterator), log entry decoder.

### B1 — State persistence: memory-only client `StateStore`
- Client `StateStore` lives entirely in-process memory. `tally clone` produces a queryable state for the session lifetime and is discarded on exit.
- Persistence is Phase 32 (stretch). Any future on-disk format deliberately not designed here.

### C1 — CLI location: new `[[bin]]` target in main `tally` crate
- Add `src/bin/tally_cli.rs` as the CLI entrypoint. (Existing server binary keeps its name.)
- `clap`-based subcommand dispatch: `tally clone`, `tally sync`. Both are skeletons in Phase 28 — they parse args and print a "not implemented yet" line. Phase 29 wires the network + state application.
- Flags in 28 scaffold: `--remote <host:port>`, `--streams <name>[,name...]`, `--keys <key>[,key...]` (or `--key-prefix <prefix>`), `--mode historical|streaming` (rejects `streaming` for now — Phase 31).
- Admin token via `TALLY_TOKEN` env var or `--token` flag. No config file.

### D1 — Apply-event path: `#[cfg(feature="server")]` gates on side-effect blocks
- Copy the server's `apply_event` in place. Wrap each side-effect branch (listener emit, signal emit, metric bump) in `#[cfg(feature = "server")]`.
- Client build compiles the exact same function with those blocks elided. Zero logic drift.
- Verification: add one client-only test that round-trips an event through `apply_event` in `cfg(feature="client")` mode and asserts state mutates correctly without touching signals/metrics.

### Shared dependencies
- Scope types (`Scope`, validation) — live in a shared module, usable by both client and server.
- Snapshot decoder — shared; the filter-iterator introduced in Phase 27 is consumed by Phase 29's bootstrap path.
- Event codec — shared.
- `OutOfScopeError` type — shared. Actual rejection logic is in Phase 29's query surface; defined as a type here so later phases import it.

### Non-goals this phase
- No network code in the CLI subcommands yet — subcommands are empty shells.
- No mode state machine (Phase 29).
- No event replay loop (Phase 29).
- No Python binding (Phase 30).

</decisions>

<code_context>
## Existing code touchpoints

- `Cargo.toml` (root) — add `client` and `server` feature flags; wire `default = ["server"]`.
- `src/lib.rs` — audit public re-exports for `#[cfg]` gates.
- `src/server/**` — all modules get `#[cfg(feature="server")]` at module level.
- `src/engine/pipeline.rs` — `apply_event` side-effect gating.
- `src/engine/state.rs` (or wherever `StateStore` lives) — confirm no server-only deps; make it trivially buildable from client.
- `src/server/tcp.rs` — server-only.
- `src/server/http.rs` — server-only.
- `src/server/signals.rs` — server-only.
- New: `src/bin/tally_cli.rs`.
- New: `src/client/mod.rs` — thin module housing client-only glue (CLI handlers, stub session type).

</code_context>

<specifics>
## Specific technical notes

- **cargo-features build check**: CI needs to build twice — once `--no-default-features --features client`, once default. Add both to the test command in plan.
- **clap version**: reuse whatever version the main crate already pins. No new top-level dependency unless strictly required.
- **tally_cli binary name**: exported as `tally_cli` at the Cargo level but the user-facing command is `tally`. Either (a) single binary with subcommand dispatching between server mode and client mode (cleaner for users) or (b) two binaries. Pick (a) if the server binary is already called `tally` — keep one binary, move server startup under `tally serve` subcommand. Confirm in the plan by reading Cargo.toml.
- **Feature propagation**: any dependency that's server-only (tokio full, hyper, etc.) must move behind `optional = true` + `features = ["server" = ["dep:…"]]`.
- **Backcompat**: every existing user command/test must work unchanged with the default (server) feature. Verify by running the full test suite after the refactor.

</specifics>

<deferred>
## Deferred

- On-disk client state — Phase 32 (stretch)
- Separate `tally-core` crate (A2) — revisit if client vs server dep drift forces it
- Env-generic `apply_event` via trait (D2) — revisit only if feature gates get unwieldy
- Separate `tally-cli` crate (C2) — never, unless A2 happens first

</deferred>

---

*Phase: 28-client-engine-embedding*
*Sources: `.planning/research/local-replica-design.md`, `.planning/phases/27-server-replica-endpoints/27-CONTEXT.md`, user directive 2026-04-14 "easiest for v0 and demo"*
