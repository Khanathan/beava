---
phase: 28-client-engine-embedding
plan: 04
subsystem: client
tags: [client, clone, snapshot-fetch, option-k, wire-codec]
requires: [28-01, 28-02, 28-03, 27-01]
provides:
  - "tally::client::clone::run_clone — one-shot historical snapshot bootstrap"
  - "tally::client::FrozenClient — scope-aware queryable handle"
  - "tally::client::wire — client-side wire codec (Scope, write_scope, OP_SNAPSHOT_FETCH)"
  - "StateStore::bulk_load — aggregated-state insert helper (no apply_event)"
  - "tally_cli clone --dump-json — JSON state dump for test ergonomics"
affects:
  - "src/bin/tally_cli.rs (handle_clone now wired to real network path)"
  - "tests/integration/test_tally_clone.py (new E2E suite)"
tech-stack:
  added: ["rand 0.8"]
  patterns: ["exponential-jitter retry", "duplicate-then-assert wire codec"]
key-files:
  created:
    - src/client/wire.rs
    - src/client/clone.rs
    - tests/integration/test_tally_clone.py
  modified:
    - src/client/mod.rs
    - src/state/store.rs
    - src/bin/tally_cli.rs
    - Cargo.toml
decisions:
  - "Duplicate (not extract) the ~80-line wire surface client needs from server/protocol.rs"
  - "Use shared StateStore::bulk_load instead of replaying events via apply_event"
  - "Retry policy: 1→2→4→8→16s cap 30s, ±20% jitter, 5 attempts; injectable for tests"
metrics:
  duration_minutes: ~90
  completed: 2026-04-15
---

# Phase 28 Plan 04: Real `tally clone` — Option K Snapshot Bootstrap

**One-liner:** `tally clone` now performs a real TCP handshake + `OP_SNAPSHOT_FETCH` (Phase 27 wire codec) + postcard decode + `StateStore::bulk_load` into a scope-enforcing `FrozenClient`, wrapped in exponential-jitter retry and covered by a Python E2E subprocess test.

## What shipped

1. **Client-side wire codec (`src/client/wire.rs`).** Duplicates the minimum surface `Scope` + `write_scope` + `OP_SNAPSHOT_FETCH` + `REPLICA_FRAME_TAG_*` from `src/server/protocol.rs`. Cross-validated three ways:
   - Compile-time `const _: () = { assert!(OP_SNAPSHOT_FETCH == crate::server::protocol::OP_SNAPSHOT_FETCH); ... }` under `--features server`.
   - Runtime parity test `write_scope_matches_server_byte_for_byte` asserts the duplicated writer produces identical bytes to the server's `write_scope` for three scope shapes.
   - Existing cross-language test `tests/integration/test_replica_snapshot_fetch_asyncio.py` hand-rolls the same layout from Python and would fail first if the server wire layout drifts.
2. **`StateStore::bulk_load`** — aggregated-state insert helper that does NOT clear existing entities and does NOT run through `apply_event`. Idempotent (overlapping keys overwrite). No dirty-tracking side-effect. 5 unit tests.
3. **`FrozenClient`** in `src/client/mod.rs`:
   - Fields: `state: StateStore`, `scope: wire::Scope`, `pub snapshot_taken_at: SystemTime`.
   - `get(stream, key) -> Result<Option<SerializableEntityState>, OutOfScopeError>` enforces scope: stream-membership → keys-set OR key_prefix → store lookup.
   - `iter_entities() -> Vec<(String, String, SerializableEntityState)>` for the `--dump-json` path.
   - 6 unit tests covering every scope-enforcement branch + `snapshot_taken_at` preservation + iter_entities.
4. **`client::clone::run_clone`** — async `run_clone(&CloneArgs)` that opens a TCP socket, writes the request frame `[u32 total_len][u8 OP_SNAPSHOT_FETCH][u16-string token][scope]`, reads `[u32 13][u8 0x01][u64 secs][u32 nanos]` header + `[u32 len][u8 0x02][postcard bytes]` payload, postcard-decodes to `BaseSnapshotState`, bulk-loads, returns `FrozenClient`. On any per-attempt failure, retries up to `max_attempts` with exponential-jitter backoff. 6 unit tests (envelope, cap, happy path, auth-reject, mid-drop, streaming-guard).
5. **`tally_cli clone` wired to real network path.** `--dump-json` flag added; JSON shape is `{snapshot_taken_at, scope, entities: [{stream, key, last_event_at_epoch_ms}, ...]}`. 9 bin-level unit tests including the new `dump_json_flag_parses`.
6. **Python integration suite `tests/integration/test_tally_clone.py`** — 4 tests pass, 1 deliberately skipped:
   - `test_clone_filters_by_scope_keys`: seeds 3 entities, clones with `--keys u_a,u_b`, asserts `u_a,u_b` present and `u_c` filtered.
   - `test_clone_without_dump_json_prints_summary`: asserts summary line on stdout + exit 0.
   - `test_clone_bad_token_fails_loud`: non-zero exit + loud stderr on wrong token (runs through the full 5-attempt retry budget in ~30s).
   - `test_tally_sync_still_stubbed`: regression guard.
   - `test_out_of_scope_error_covered_by_rust_unit_tests`: `pytest.skip` with rationale (no subprocess-level `get` subcommand until Phase 30).

## Wire-type approach landed

**Duplication.** Plan granted a fallback to `src/client/wire.rs` when extraction ballooned; extraction did balloon (the Scope codec consumes `read_string`/`TallyError` machinery in `src/server/protocol.rs` which would have required hoisting >200 lines to a shared module). Duplication produced ~60 lines of Rust and zero server-side churn, with compile-time + runtime parity assertions preventing silent drift. `v0.2 TODO` comment placed at the top of `src/client/wire.rs` pointing to the eventual shared-module consolidation.

## Where Phase 27's codec lives

Canonical definitions remain in `src/server/protocol.rs`:
- `OP_SNAPSHOT_FETCH = 0x12` (line 111)
- `REPLICA_FRAME_TAG_HEADER = 0x01` (line 115)
- `REPLICA_FRAME_TAG_PAYLOAD = 0x02` (line 116)
- `pub struct Scope` (line 240)
- `pub fn write_scope` (line 277)

Client-side duplicates live in `src/client/wire.rs` with a `#[cfg(feature = "server")] const _: () = { assert!(...) }` alignment check and a `#[cfg(feature = "server")]` unit test that byte-compares both writers.

## Chosen handshake format

TCP, mirroring `src/server/protocol.rs:849` (parse_command OP_SNAPSHOT_FETCH branch) + `src/server/tcp.rs:378` (read_one_frame):

```
[u32 BE total_len][u8 opcode=0x12][u16-string admin_token][scope-bytes]
```

Not HTTP: Phase 27-01 is shipping the TCP framing per `27-CONTEXT.md` and the existing `test_replica_snapshot_fetch_asyncio.py` validates exactly this shape from Python. No new server-side work was needed.

## BaseSnapshotState shape used

From `src/state/snapshot.rs:308`:

```rust
pub struct BaseSnapshotState {
    pub header: SnapshotHeader,                                // SnapshotType::Base, sequence
    pub entities: Vec<(String, SerializableEntityState)>,      // (entity_key, aggregated state)
    pub pipelines: Vec<SerializablePipeline>,
    pub backfill_complete: Vec<(String, String)>,
}
```

`bulk_load` signature matches the `entities` field exactly: `pub fn bulk_load(&self, entities: Vec<(String, SerializableEntityState)>)`. The server-side `OP_SNAPSHOT_FETCH` handler filters entities by scope before postcard-serializing, so the client-side bulk-load is a simple un-gated insert.

## Python SDK surface — N/A

The plan suggested seeding events via the Python SDK (`Client.push(...)`); in practice the Rust server's `OP_SNAPSHOT_FETCH` handler serves the file-backed base snapshot directly, so a hand-encoded snapshot file (same pattern as `test_replica_snapshot_fetch_asyncio.py`) is a more isolated fixture. No Python SDK coupling introduced.

Fixture snapshot size: 3 entities × 1 stream each × empty operators = 94 bytes on disk (trivial).

## Retry policy

`next_delay(attempt, rng) -> Duration`:
- bases (ms): `1000, 2000, 4000, 8000, 16000, 16000, ...` (shift clamp at 4)
- cap: 30000 ms (never hit at these bases; documentary)
- jitter: `±20%` uniform via `rng.gen_range(-span..=span)`

Unit-tested with `StdRng::seed_from_u64(42)` across 100 draws per attempt 0..=4 for the envelope `[0.8*base, 1.2*base]`.

## Hand-offs

**Phase 31 (streaming catchup).** `FrozenClient.snapshot_taken_at` is the `since` cursor. Expected flow: `clone → snapshot_taken_at → OP_LOG_FETCH (re-introduced) from that cursor → streaming`.

**Phase 30 (Python binding).** Wraps `FrozenClient::get` as a Python method; `OutOfScopeError` becomes a Python exception subclass. The `FrozenClient::iter_entities` helper is a natural starting point for a `__iter__` binding.

## Known gaps / accepted risks

- **No TLS / mutual auth.** v0 assumes trusted transport. Documented in the plan's threat model (T-28-04-02 `accept`).
- **`--dump-json` leaks aggregated state to stdout.** Intended dev/test ergonomic; documented in `emit_json_dump`'s doc comment (T-28-04-04 `mitigate`).
- **No audit log of clone operations.** Server-side TCP logs are the only trail (T-28-04-03 `accept`).
- **Stream registration still required.** The server's scope validator rejects `UnknownStream`, so the server must have the stream registered (via `POST /pipelines` or pre-snapshot) before a client clones. The integration test seeds the stream via HTTP; production callers will hit the same requirement. Documented in the plan's `<interfaces>`.
- **Wire-code duplication.** `src/client/wire.rs` is structurally parallel to `src/server/protocol.rs`'s Scope/codec. Guarded by compile-time + runtime + cross-language assertions; still a v0.2 consolidation target.

## Test delta

- `cargo test` full suite: **1252 passed, 0 failed** (baseline was 1170 before Phase 28; net new: ~82 tests across 28-01 through 28-04, of which this plan added ~18).
- `cargo build --no-default-features --features client --lib`: green.
- `cargo build --no-default-features --features client --bin tally_cli`: green.
- `cargo build` (defaults): green.
- `pytest tests/integration/test_tally_clone.py -v`: **4 passed, 1 skipped** in 16s.

## Deviations from plan

**None.** The plan granted explicit permission to fall back from extraction to duplication if extraction cost exceeded ~200 lines of movement; duplication was selected up-front based on static analysis of the server-side coupling (see *Wire-type approach landed* above). All other tasks executed as specified.

## Self-Check: PASSED

- `src/client/wire.rs`: FOUND
- `src/client/clone.rs`: FOUND
- `src/client/mod.rs` (modified, FrozenClient + iter_entities): FOUND
- `src/state/store.rs` (modified, bulk_load): FOUND
- `src/bin/tally_cli.rs` (modified, handle_clone wired): FOUND
- `tests/integration/test_tally_clone.py`: FOUND
- 4 Python integration tests pass (+ 1 intentionally skipped).
- 1252 Rust tests pass.
