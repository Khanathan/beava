# Phase 29: Session manager + log consumer + historical mode E2E - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning
**Mode:** Interactive discuss (user directive: "easiest for v0 and demo")

<domain>
## Phase Boundary

Wire the client-side scaffolding from Phase 28 into a working end-to-end `tally clone` flow against a real Tally server. The command bootstraps state from `OP_SNAPSHOT_FETCH`, catches up via `OP_LOG_FETCH`, then exits with a queryable frozen replica. `tally sync` (streaming) remains a stub — Phase 31 owns it.

**In scope:** Rust-side persistent TCP session with reconnect, protocol handshake carrying scope, bootstrap loop (SNAPSHOT_FETCH → `apply_event` per entry), catchup loop (LOG_FETCH → `apply_event` per entry), mode state machine (`Bootstrap → Catchup → Done`), `OutOfScopeError` at client query time, full `tally clone` CLI wiring, asyncio-based E2E integration test.

**Out of scope:** automatic scope derivation from a pipeline DAG (Phase 30, where the Python API lands), streaming mode / SUBSCRIBE (Phase 31), state persistence across runs (Phase 32), backfill from external sources (Phase 33), write-back (Phase 34).

</domain>

<decisions>
## Implementation Decisions (LOCKED)

### Guiding principle
**Easiest for v0 and demo.** Single-path happy flow first; loud failure modes second; no production-grade resilience.

### A1 — Scope source: CLI flags only
- Scope comes from `--streams`, `--keys`, `--key-prefix` on the `tally clone` subcommand (scaffolded in Phase 28).
- No pipeline-DAG walker in this phase. Phase 30 will add the Python API that generates scope from dependency analysis.
- CLI validation: reject empty `--streams`; reject both `--keys` and `--key-prefix` set.

### B1 — Reconnect policy: exponential backoff with jitter
- Start delay 1s, double each failure, cap at 30s, ±20% random jitter.
- Give up after 5 consecutive failures → exit with clear error.
- On reconnect, re-send handshake with the same scope. Resume from the last successfully-applied seq (held in memory).

### C1 — Mode state machine: plain enum, explicit transitions
```rust
enum Mode {
  Bootstrap, // reading snapshot
  Catchup,   // reading log entries seq > snapshot_hwm
  Done,      // historical clone finished
}
```
- Log every transition at info level: `mode: Bootstrap → Catchup (hwm=N)`, `mode: Catchup → Done (last_seq=M)`.
- Each transition is triggered by a well-defined event: snapshot-terminal received → Catchup; LOG_FETCH reaches current tail → Done.
- `--mode streaming` on the CLI hard-errors: `streaming mode ships in Phase 31; use --mode historical`.

### D1 — OutOfScopeError at query time
- `client.get(key)` checks the declared scope before hitting the local `StateStore`. If `key` is not in scope (fails stream or key/prefix match), raise typed `OutOfScopeError { stream, key, reason }`.
- No check at event-apply time — server already filters, so out-of-scope data cannot arrive. Defense-in-depth is Phase 32+ hardening.
- Error type lives in the shared module introduced in Phase 28.

### E1 — `tally sync` is a stub in this phase
- `tally sync` prints `streaming mode not yet implemented; available in Phase 31` and exits 1.
- Phase 31 replaces the stub with a real SUBSCRIBE loop.

### F1 — Plan split (3 plans)
- **29-01**: TCP session type + reconnect loop + protocol handshake (opcode-agnostic framing sender/receiver) + scope serialization matching Phase 27's wire format.
- **29-02**: Bootstrap loop: call `OP_SNAPSHOT_FETCH`, decode entries streamingly, `apply_event` each into the client `StateStore`, capture HWM from terminal message.
- **29-03**: Catchup loop: call `OP_LOG_FETCH{from: hwm}`, decode entries, `apply_event`, stop on tail-reached. Mode state machine wiring. `tally clone` CLI wiring (removes the Phase 28 "not implemented" stub). E2E integration test: Python test harness that (a) starts a Tally server in a subprocess, (b) pushes known events via the existing Python SDK, (c) runs `tally clone --streams X --keys ...`, (d) asserts the client's printed state matches expectations.

### Shared scope validator
- Reuse the Scope struct + validator from Phase 27 (shared crate module). Client does client-side validation before sending; server also validates on receive.

### No config file
- All client options via CLI flags or env vars. No YAML/TOML config in v0. `TALLY_HOST`, `TALLY_TOKEN` env vars as overrides.

### Logging
- Use whatever log crate the main `tally` crate already depends on. `info` level for state transitions, `warn` for reconnects, `error` for terminal failures. No structured JSON logging — humans reading CLI output are the audience.

### Test strategy
- Unit tests: reconnect backoff math, state machine transitions, OutOfScopeError raising.
- Integration test (the load-bearing one): `tests/integration/test_tally_clone.py` — spins up a real server, pushes fixture events, invokes the `tally` binary via subprocess, parses its state output, asserts correctness across bootstrap + catchup. Covers A1/B1/C1/D1/E1 together.

</decisions>

<code_context>
## Existing code touchpoints

- Phase 27 `Scope` struct + wire codec — import into client session code.
- Phase 27 `OP_SNAPSHOT_FETCH` / `OP_LOG_FETCH` response framing — decoder lives client-side.
- Phase 28 `src/bin/tally_cli.rs` — replace `clone` stub with real wiring; `sync` stays stub.
- Phase 28 `src/client/mod.rs` — add session + bootstrap + catchup submodules.
- Phase 28 client `StateStore` + `apply_event` (memory-only) — called from bootstrap/catchup loops.
- Existing TCP framing helpers in `src/server/protocol.rs` — the decode-side helpers are shared (put behind `#[cfg(any(feature="server", feature="client"))]`).

</code_context>

<specifics>
## Specific technical notes

- **Frame decoder reuse**: existing 4-byte-length-prefix decoder on the server side handles incoming opcodes. Client needs the mirror: a decoder for server-sent response frames. Factor the decode side out of `src/server/protocol.rs` into a shared module in Phase 29 if it isn't already.
- **Streaming decode for snapshot**: SNAPSHOT_FETCH emits potentially many MB. Decode one entry at a time, `apply_event`, drop the decoded buffer before reading the next. Never accumulate.
- **Seq monotonicity check**: as entries arrive in Catchup, assert `seq > last_applied_seq`. If ever violated, that's a server bug — abort loudly with the seqs logged. This also catches a class of reconnect-resumption bugs cheaply.
- **Client `StateStore`**: no locks needed — single-threaded client, events applied one at a time in arrival order.
- **E2E harness fixture**: follow the pattern in `tests/integration/test_replica_*.py` introduced by Phase 27 (raw asyncio sockets against a real server). Phase 29's test invokes the `tally` binary via subprocess instead of writing sockets directly.

</specifics>

<deferred>
## Deferred

- Automatic scope derivation from pipeline DAG — Phase 30
- Streaming mode / SUBSCRIBE — Phase 31
- Persistent resume across CLI invocations — Phase 32 (stretch)
- Backfill source pluggability (s3://, snowflake://) — Phase 33
- Write-back / promote — Phase 34
- Belt-and-suspenders scope check at apply time — post-v0 hardening
- Configurable reconnect policy — never, unless an operator asks
- Structured JSON logs — never for CLI, consider for daemon modes later

</deferred>

---

*Phase: 29-session-manager-historical*
*Sources: `.planning/research/local-replica-design.md`, `.planning/phases/27-server-replica-endpoints/27-CONTEXT.md`, `.planning/phases/28-client-engine-embedding/28-CONTEXT.md`, user directive 2026-04-14 "easiest for v0 and demo"*
