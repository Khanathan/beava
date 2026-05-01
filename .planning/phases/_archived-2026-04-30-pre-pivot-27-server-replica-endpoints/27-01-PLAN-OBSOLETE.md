---
phase: 27-server-replica-endpoints
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/server/protocol.rs
  - src/server/tcp.rs
  - src/server/mod.rs
  - src/state/snapshot.rs
  - tests/test_replica_snapshot_fetch.rs
autonomous: true
requirements:
  - PHASE-27-OP_SNAPSHOT_FETCH
  - PHASE-27-SCOPE-STRUCT
  - PHASE-27-SCOPE-VALIDATOR
  - PHASE-27-SNAPSHOT-FILTER-ITERATOR

must_haves:
  truths:
    - "A client that sends OP_SNAPSHOT_FETCH with a valid Scope and admin token receives the current snapshot's entries filtered by scope, followed by the snapshot's HWM seq, and then EOF on the stream."
    - "A client that sends OP_SNAPSHOT_FETCH without admin auth, with empty streams, with both keys and key_prefix, with pull != 'all', with an unknown stream name, with keys.len() > 10_000, or with empty key_prefix receives a distinct error frame before any entries are streamed, and nothing leaks."
    - "The snapshot is read frame-at-a-time by the server — peak heap during an OP_SNAPSHOT_FETCH on a 100MB snapshot stays on the order of one entry, not the full file."
    - "BackfillSource interface is documented as a comment in src/state/snapshot.rs (stub only — no code path uses it in Phase 27)."
  artifacts:
    - path: "src/server/protocol.rs"
      provides: "OP_SNAPSHOT_FETCH opcode const 0x12, OP_LOG_FETCH const 0x13 (reserved-not-implemented here — 27-02 wires it), Scope struct + wire codec (read_scope/write_scope), validate_scope() returning a typed error enum"
      contains: "pub const OP_SNAPSHOT_FETCH: u8 = 0x12"
    - path: "src/server/tcp.rs"
      provides: "handle_snapshot_fetch() dispatch arm — auth-gated via require_loopback_or_token; calls SnapshotReader::stream_filtered; emits length-prefixed entries + terminal HWM u64"
      contains: "handle_snapshot_fetch"
    - path: "src/state/snapshot.rs"
      provides: "SnapshotReader::stream_filtered(&Scope) -> impl Iterator<Item=Result<SnapshotEntryBytes>> — frame-at-a-time, zero full-file buffer; also exposes hwm_seq() on the reader for the terminal emit"
      contains: "stream_filtered"
    - path: "tests/test_replica_snapshot_fetch.rs"
      provides: "Rust integration tests: happy path (2 streams, key filter, key_prefix filter), all 7 validation rejects, auth rejection, HWM terminal frame present, streaming property (reader is not collect()-backed)"
      min_lines: 150
  key_links:
    - from: "src/server/tcp.rs::handle_snapshot_fetch"
      to: "require_loopback_or_token"
      via: "auth gate at top of handler, before any snapshot I/O"
      pattern: "require_loopback_or_token"
    - from: "src/server/tcp.rs::handle_snapshot_fetch"
      to: "SnapshotReader::stream_filtered"
      via: "iterator drives the write loop — never collect()"
      pattern: "stream_filtered\\("
    - from: "src/server/protocol.rs::parse_command"
      to: "validate_scope"
      via: "scope validated before dispatch; error frame written without touching storage"
      pattern: "validate_scope"
---

<objective>
Land the first of three scope-aware replica opcodes: `OP_SNAPSHOT_FETCH` (0x12). This plan
owns the shared wire layer for Phase 27 — the `Scope` struct, its codec, and the scope
validator — because 27-02 (OP_LOG_FETCH) and 27-03 (OP_SUBSCRIBE) will reuse all three.
Also add the streaming filter-iterator on `SnapshotReader` so the server can emit a
filtered snapshot without ever holding the whole v7 file in memory.

Purpose: Unblocks 27-02 (needs `Scope` + validator) and 27-03 (same). Delivers the
"bootstrap" half of the Phase 28+ replica client's bootstrap → catchup → live state
machine. This is the `tally clone` moment.

Output: Three new/modified Rust files + one Rust integration test file. Wire protocol
additions for Scope + 0x12. v7 snapshot reader gains `stream_filtered`. No Python yet —
end-to-end Python socket test lives in 27-02 where both opcodes are available.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/ROADMAP.md
@.planning/STATE.md
@.planning/phases/27-server-replica-endpoints/27-CONTEXT.md

@src/server/protocol.rs
@src/server/tcp.rs
@src/server/auth.rs
@src/state/snapshot.rs

<interfaces>
<!-- Extracted from codebase. Use directly; no exploration needed. -->

From src/server/protocol.rs (existing opcodes and string framing to reuse):
```rust
pub const OP_PUSH: u8 = 0x01;
// ... 0x02..0x0D used ...
pub const OP_SCAN_RESERVED: u8 = 0x10;         // keep as-is, stub stays
pub const OP_SUBSCRIBE_RESERVED: u8 = 0x11;    // 27-03 replaces this
// NEW in this plan:
// pub const OP_SUBSCRIBE: u8 = 0x11;          // (reserved → live in 27-03; DO NOT take here)
// pub const OP_SNAPSHOT_FETCH: u8 = 0x12;     // this plan
// pub const OP_LOG_FETCH: u8 = 0x13;          // reserve-as-stub here; 27-02 implements

pub fn parse_command(opcode: u8, payload: &[u8]) -> Result<Command, ProtocolError>;
// Existing u16-length-prefixed string framing helpers live in this file — reuse them.
```

From src/server/auth.rs (reuse unchanged):
```rust
pub fn require_loopback_or_token(...) -> Result<(), AuthError>;
```

From src/state/snapshot.rs (existing — extend):
```rust
pub enum SnapshotFile { ... }        // v7 variant exists
pub fn load_snapshot_file(bytes: &[u8]) -> Option<SnapshotFile>;
// NEW: SnapshotReader struct + stream_filtered(&Scope) iterator
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Scope struct + wire codec + validator in protocol.rs</name>
  <files>src/server/protocol.rs</files>
  <behavior>
    - `Scope { streams: Vec<String>, keys: Option<Vec<String>>, key_prefix: Option<String>, pull: String }` serializes/deserializes losslessly through `read_scope`/`write_scope`.
    - `validate_scope(&Scope, &known_streams)` returns `Ok(())` for valid scopes and a distinct `ScopeError` variant for each of the seven locked rejection rules (D-scope-validation in CONTEXT.md §Scope validation):
      1. empty `streams` → `ScopeError::EmptyStreams`
      2. unknown stream name → `ScopeError::UnknownStream(name)`
      3. `keys` AND `key_prefix` both Some → `ScopeError::KeysAndPrefix`
      4. `pull != "all"` → `ScopeError::PullNotImplemented(pull)`
      5. `keys.len() > 10_000` → `ScopeError::TooManyKeys(n)`
      6. empty `key_prefix` string → `ScopeError::EmptyPrefix`
      (Auth check is not in `validate_scope` — it lives in the TCP handler; the auth-missing case is rule 3 from CONTEXT but enforced at the dispatch layer.)
    - `parse_command(OP_SNAPSHOT_FETCH, payload)` round-trips a known Scope without data loss.
    - Unknown-stream validation accepts a `&HashSet<String>` or `&[String]` of known stream names — caller (TCP handler) owns the registry lookup.
  </behavior>
  <action>
    Add to `src/server/protocol.rs`:
    1. `pub const OP_SNAPSHOT_FETCH: u8 = 0x12;` and `pub const OP_LOG_FETCH: u8 = 0x13;`. Leave `OP_SUBSCRIBE_RESERVED = 0x11` untouched (27-03 replaces it).
    2. `pub struct Scope { ... }` with exact field shape from CONTEXT.md §Scope payload shape. Derive `Debug, Clone, PartialEq`.
    3. `pub fn read_scope(buf: &[u8]) -> Result<(Scope, usize), ProtocolError>` and `pub fn write_scope(buf: &mut Vec<u8>, scope: &Scope)`. Use the file's existing `u16`-length-prefixed string framing — do not invent a new codec. Shape on the wire: `u16 n_streams, [u16 len, utf8 bytes]*n, u8 has_keys, (u32 n_keys, [u16 len, bytes]*n_keys)?, u8 has_prefix, (u16 len, bytes)?, u16 pull_len, utf8 bytes`.
    4. `pub enum ScopeError { EmptyStreams, UnknownStream(String), KeysAndPrefix, PullNotImplemented(String), TooManyKeys(usize), EmptyPrefix }` with `Display` impl producing a single-line error string the TCP layer serializes into the existing error frame.
    5. `pub fn validate_scope(scope: &Scope, known_streams: &std::collections::HashSet<String>) -> Result<(), ScopeError>` — run all six checks in order above.
    6. Extend `Command` enum with `SnapshotFetch { scope: Scope }` and add a stub `LogFetch { from: u64, scope: Scope }` variant (27-02 wires its handler; parse it here so the wire test is complete). Extend `parse_command` to decode both.
    7. Unit tests in `#[cfg(test)]`: round-trip Scope codec (4 shape variants: keys-only, prefix-only, neither, full streams list), every ScopeError variant, wire parse of OP_SNAPSHOT_FETCH and OP_LOG_FETCH.

    Do NOT add a BackfillSource trait in this plan — just a `// Phase 33: BackfillSource trait will plug in here — see CONTEXT.md §Reserved` comment in snapshot.rs (task 2). Per D-Reserved in CONTEXT.md, no code.
  </action>
  <verify>
    <automated>cargo test --test test_reserved_opcodes --lib protocol:: -- --nocapture</automated>
  </verify>
  <done>`cargo build` clean; `cargo test --lib protocol::` passes; all six ScopeError variants exercised; codec round-trip test passes for all four Scope shapes.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: SnapshotReader::stream_filtered — frame-at-a-time filter iterator</name>
  <files>src/state/snapshot.rs</files>
  <behavior>
    - `SnapshotReader::open(path)` opens a v7 snapshot file and exposes `hwm_seq() -> u64` from the header.
    - `reader.stream_filtered(scope)` returns `impl Iterator<Item = Result<SnapshotEntryBytes, SnapshotError>>` yielding only entries whose stream is in `scope.streams` AND whose key matches (`scope.keys.contains(k)` if keys Some, else `scope.key_prefix`-prefix if prefix Some, else all keys in that stream).
    - Reading iterates frame-by-frame via `BufReader` — the test asserts peak heap stays bounded while iterating over a snapshot ≥ 50MB (use a small per-frame upper bound; the specific number is picked during implementation but must be orders of magnitude below total file size).
    - `SnapshotEntryBytes` is the already-serialized postcard bytes of one entry plus its (stream, key) header — the TCP layer can write it out with a 4-byte length prefix without re-serializing.
    - Corrupt frame → `SnapshotError` variant, iterator terminates.
  </behavior>
  <action>
    Add to `src/state/snapshot.rs`:
    1. `pub struct SnapshotReader { reader: BufReader<File>, hwm: u64, header: SnapshotHeader }` with `pub fn open(path: &Path) -> Result<Self, SnapshotError>` that reads + verifies the v7 header and pins HWM from it (per D-Snapshot/log consistency handoff: HWM = header's snapshot seq).
    2. `pub fn hwm_seq(&self) -> u64 { self.hwm }`.
    3. `pub fn stream_filtered<'a>(&'a mut self, scope: &'a Scope) -> impl Iterator<Item = Result<SnapshotEntryBytes, SnapshotError>> + 'a` — implement by reading one length-prefixed postcard entry at a time, deserializing just enough to inspect (stream_name, key), and emitting the raw bytes if the scope matches. Never `Vec::with_capacity(file_size)` or equivalent.
    4. `pub struct SnapshotEntryBytes { pub bytes: Vec<u8> }` — single-entry buffer reused across iterations where possible.
    5. Add top-of-file comment block: `// Phase 33 hook — the BackfillSource trait (reserved in Phase 22) will plug in above SnapshotReader. No code here in Phase 27; interface documented in CONTEXT.md §Reserved.`
    6. Tests in `#[cfg(test)]`: build a synthetic v7 snapshot with 3 streams × 100 keys, verify `stream_filtered` with (a) streams-only, (b) streams+keys explicit set, (c) streams+key_prefix, (d) a scope matching zero entries. For streaming property: use a snapshot with many entries and assert (via a counting BufReader or a sentinel) that the iterator pulls one frame at a time.

    Do NOT re-architect v7 — just add a streaming reader alongside the existing eager loader. Keep `load_snapshot_file` working unchanged.
  </action>
  <verify>
    <automated>cargo test --lib state::snapshot::stream_filtered_</automated>
  </verify>
  <done>All four filter-shape tests pass; streaming-property test asserts frame-at-a-time reads; `cargo clippy --lib` clean on snapshot.rs.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: OP_SNAPSHOT_FETCH handler in tcp.rs + Rust integration test</name>
  <files>src/server/tcp.rs, src/server/mod.rs, tests/test_replica_snapshot_fetch.rs</files>
  <behavior>
    - Server dispatching OP_SNAPSHOT_FETCH does, in order: (1) admin-token / loopback auth check via existing `require_loopback_or_token` — on failure emit the existing error frame and close (no state leak, no open snapshot file); (2) look up the live set of known stream names from the pipeline registry; (3) `validate_scope(&scope, &known_streams)` — on failure emit error frame and close; (4) open current v7 snapshot via `SnapshotReader::open`; (5) for each entry from `reader.stream_filtered(&scope)`, write a 4-byte-length-prefixed frame to the socket; (6) write a terminal `HWM` frame (4-byte length = 9, tag byte 0xFF, `u64` big-endian HWM) and flush.
    - Metric counter `tally_replica_snapshot_bytes_sent_total` (registered in 27-03 — here just a TODO-with-plan comment; or land a zero-value counter now so Prometheus parse stays stable — implementer picks the simpler option).
    - Terminal HWM frame format is documented in `protocol.rs` as a doc comment so 27-02 and the Phase 28 client read the same shape.
  </behavior>
  <action>
    In `src/server/tcp.rs`:
    1. Add `async fn handle_snapshot_fetch(stream, scope, ctx) -> Result<()>` modeled after existing OP_PUSH_TABLE dispatch (line ~1798 for shape reference).
    2. Wire it into the opcode dispatch match. Keep OP_SUBSCRIBE_RESERVED and OP_SCAN_RESERVED arms intact. Add OP_LOG_FETCH arm that returns `Command::ReservedNotImplemented` for now (27-02 replaces).
    3. Auth: call the existing `require_loopback_or_token` at the top — same call sites as OP_PUSH_TABLE. On failure, use the file's existing error-frame write path.
    4. Pass the pipeline registry handle through so `validate_scope` has the known-stream set (the TCP context already carries state; reuse whatever `handle_push_table` uses to look up the stream).
    5. Snapshot path: resolve current snapshot path the same way the existing snapshot cycle does (greps for the snapshot dir constant will reveal it). Open via `SnapshotReader::open`.
    6. Write loop: each iterator item → 4-byte big-endian length prefix + the entry bytes. Terminal frame: `[0,0,0,9, 0xFF, hwm_u64_be]`. Flush.
    7. Document the terminal frame shape in `src/server/protocol.rs` as a public `///` doc comment on `OP_SNAPSHOT_FETCH`.

    Create `tests/test_replica_snapshot_fetch.rs` as a Rust integration test (same style as `tests/test_op_push_table.rs`):
    - spins up the test server (reuse the shared helper that other tests use — grep `test_server` for the helper);
    - pushes events to 2 streams, forces a snapshot;
    - opens a TCP connection, sends OP_SNAPSHOT_FETCH with valid admin token, reads frames, asserts: (a) every entry belongs to the scoped streams, (b) terminal frame is HWM-shaped, (c) HWM value equals the snapshot's recorded HWM;
    - repeats with `keys`-set scope and with `key_prefix` scope;
    - negative tests for all 7 rejection rules (including missing auth) — each asserts the server returned an error frame and closed the connection without streaming any entries.

    No Python tests in this plan — 27-02 owns the first end-to-end Python socket test (per CONTEXT.md §Plan split). This is deliberate: SNAPSHOT_FETCH alone isn't the clone-then-catchup flow we want Python exercising.
  </action>
  <verify>
    <automated>cargo test --test test_replica_snapshot_fetch</automated>
  </verify>
  <done>All happy-path + rejection tests pass; `cargo test` full suite green; `cargo clippy` clean on tcp.rs.</done>
</task>

</tasks>

<test_plan>
## Test Plan (per user preference: every plan needs one)

**Levels:**
1. **Unit** — `src/server/protocol.rs` Scope codec + validator (task 1 tests).
2. **Unit** — `src/state/snapshot.rs` SnapshotReader::stream_filtered frame-at-a-time semantics (task 2 tests).
3. **Integration (Rust)** — `tests/test_replica_snapshot_fetch.rs`: full TCP round-trip with the live test server harness (task 3 tests).

**Coverage matrix:**

| Concern | Test | Level |
|---|---|---|
| Scope wire codec round-trip, 4 shapes | `protocol::scope_codec_roundtrip_*` | Unit |
| Each ScopeError variant rejects before I/O | `protocol::validate_scope_rejects_*` | Unit |
| OP_SNAPSHOT_FETCH command parses | `protocol::parse_snapshot_fetch` | Unit |
| Filter iterator: streams-only, keys, prefix, empty | `snapshot::stream_filtered_*` | Unit |
| Frame-at-a-time reading (no full buffer) | `snapshot::stream_filtered_is_streaming` | Unit |
| Happy path: entries + HWM terminal frame | `test_replica_snapshot_fetch::happy_*` | Integ |
| 7 rejections produce error frame, no stream leak | `test_replica_snapshot_fetch::rejects_*` | Integ |
| Auth missing closes connection pre-I/O | `test_replica_snapshot_fetch::rejects_missing_auth` | Integ |

**Out of scope for this plan's tests:** Python asyncio socket test (27-02), OP_LOG_FETCH catchup (27-02), OP_SUBSCRIBE live push (27-03), backpressure (27-03).

**Bench / load:** None required in this plan. 27-03 carries the backpressure load test.
</test_plan>

<verification>
- `cargo build --release` clean.
- `cargo test` full suite passes including new `test_replica_snapshot_fetch`.
- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `grep -n "OP_SNAPSHOT_FETCH\\|validate_scope\\|stream_filtered" src/server/protocol.rs src/server/tcp.rs src/state/snapshot.rs` shows all three symbols exported / referenced at their expected call sites.
- Manual: send a raw OP_SNAPSHOT_FETCH via `ncat` to a dev server, pipe through `xxd`, confirm terminal frame tag is `0xFF` and HWM decodes.
</verification>

<success_criteria>
- `Scope`, `read_scope`/`write_scope`, `validate_scope`, `ScopeError` exported from `protocol.rs`.
- `OP_SNAPSHOT_FETCH = 0x12` and `OP_LOG_FETCH = 0x13` constants in `protocol.rs` (log-fetch arm is reserved-stub, 27-02 replaces).
- `SnapshotReader` with `open`, `hwm_seq`, `stream_filtered` in `snapshot.rs`, streaming property verified.
- TCP handler dispatches 0x12 with auth + validation + streaming emit + terminal HWM frame.
- `tests/test_replica_snapshot_fetch.rs` exists with happy + 7 rejection + auth-missing cases; all pass.
- No Python tests added (27-02 owns that).
- No BackfillSource code added (comment-only stub per CONTEXT.md §Reserved).
</success_criteria>

<output>
After completion, create `.planning/phases/27-server-replica-endpoints/27-01-SUMMARY.md`
summarizing: new wire constants, new public APIs (`Scope`, `validate_scope`, `SnapshotReader`),
test counts, and the terminal-HWM-frame format (for 27-02 and Phase 28 to reference).
</output>
