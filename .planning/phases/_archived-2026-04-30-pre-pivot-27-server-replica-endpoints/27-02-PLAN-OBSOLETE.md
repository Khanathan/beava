---
phase: 27-server-replica-endpoints
plan: 02
type: execute
wave: 2
depends_on: [27-01]
files_modified:
  - src/server/tcp.rs
  - src/server/protocol.rs
  - src/state/event_log.rs
  - tests/test_replica_log_fetch.rs
  - tests/integration/test_replica_clone_catchup.py
autonomous: true
requirements:
  - PHASE-27-OP_LOG_FETCH
  - PHASE-27-PER-STREAM-LOG-FILTER-ITERATOR
  - PHASE-27-CLONE-CATCHUP-E2E

must_haves:
  truths:
    - "A client that sends OP_LOG_FETCH{from, scope} with valid admin auth receives every log entry with seq > from whose (stream, key) matches scope, in global seq order across the requested streams, and then a terminal frame marking 'caught up to current tail'."
    - "The clone-then-catchup flow works end-to-end from a Python asyncio client: OP_SNAPSHOT_FETCH yields entries + HWM S; OP_LOG_FETCH{from: S} yields every event with seq > S up to server tail; no duplicates, no gaps."
    - "Per-stream log filter iterator reads frame-at-a-time — peak heap stays bounded regardless of log file size."
    - "Scope validation + auth reuses the validate_scope + require_loopback_or_token paths from 27-01 (no copy-paste)."
  artifacts:
    - path: "src/server/tcp.rs"
      provides: "handle_log_fetch() dispatch arm replacing the 27-01 reserved stub; merge-iterator across requested streams; terminal 'caught-up' frame"
      contains: "handle_log_fetch"
    - path: "src/state/event_log.rs"
      provides: "PerStreamLogReader::filtered(scope, from_seq) -> impl Iterator — frame-at-a-time, skip entries with seq <= from_seq or key not in scope; plus a merge helper that interleaves per-stream iterators in global seq order"
      contains: "filtered"
    - path: "tests/test_replica_log_fetch.rs"
      provides: "Rust integration tests: happy path (from=0, from=HWM), empty result (from beyond tail), scope-miss stream, auth reject, merge order correctness across 3 streams with interleaved seqs"
      min_lines: 120
    - path: "tests/integration/test_replica_clone_catchup.py"
      provides: "End-to-end Python asyncio socket test — pushes events, triggers snapshot, issues SNAPSHOT_FETCH + LOG_FETCH, asserts set-equality with pushed events, no dupes, no gaps, HWM boundary honored"
      min_lines: 120
  key_links:
    - from: "src/server/tcp.rs::handle_log_fetch"
      to: "validate_scope"
      via: "reused from 27-01 — same auth + scope validation path as OP_SNAPSHOT_FETCH"
      pattern: "validate_scope\\("
    - from: "src/server/tcp.rs::handle_log_fetch"
      to: "PerStreamLogReader::filtered"
      via: "one iterator per scope.streams[i], merged in global seq order"
      pattern: "filtered\\("
    - from: "tests/integration/test_replica_clone_catchup.py"
      to: "server OP_SNAPSHOT_FETCH + OP_LOG_FETCH"
      via: "raw asyncio TCP socket — no SDK (SDK lands Phase 28+)"
      pattern: "asyncio\\.open_connection"
---

<objective>
Land `OP_LOG_FETCH{from, scope}` (0x13) and prove the full bootstrap-then-catchup flow
works end-to-end against a running server from a Python asyncio client. This is the
first time a non-Rust consumer can reconstruct server state through the replica
opcodes — validates the whole Phase 27 thesis before 27-03 adds live streaming.

Purpose: Closes the "catchup" half of the Phase 28+ replica's `bootstrap → catchup →
live` state machine. Snapshot gives a baseline at HWM S; LOG_FETCH{from: S} fills the
gap from S to current tail; 27-03's SUBSCRIBE takes it from there.

Output: One new handler in `tcp.rs`, one new reader in `event_log.rs`, one Rust
integration test, and one Python asyncio end-to-end test (the user's required test plan
deliverable — raw sockets, no SDK).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/ROADMAP.md
@.planning/STATE.md
@.planning/phases/27-server-replica-endpoints/27-CONTEXT.md
@.planning/phases/27-server-replica-endpoints/27-01-SUMMARY.md

@src/server/protocol.rs
@src/server/tcp.rs
@src/state/event_log.rs
@tests/integration/test_replay_30d.py

<interfaces>
<!-- From 27-01 (already landed by the time this plan runs): -->

From src/server/protocol.rs:
```rust
pub const OP_SNAPSHOT_FETCH: u8 = 0x12;
pub const OP_LOG_FETCH: u8 = 0x13;         // reserved stub in 27-01; this plan wires it
pub struct Scope { pub streams: Vec<String>, pub keys: Option<Vec<String>>,
                   pub key_prefix: Option<String>, pub pull: String }
pub fn read_scope(buf: &[u8]) -> Result<(Scope, usize), ProtocolError>;
pub fn write_scope(buf: &mut Vec<u8>, scope: &Scope);
pub fn validate_scope(scope: &Scope, known: &HashSet<String>) -> Result<(), ScopeError>;
pub enum Command { ..., SnapshotFetch { scope }, LogFetch { from: u64, scope } }
```

From src/state/event_log.rs (existing — extend):
```rust
// Per-stream log files live on disk (Phase 6). Existing readers load eagerly;
// this plan adds a frame-at-a-time filtered iterator.
```

Terminal-frame shape reference (from 27-01-SUMMARY.md): snapshot uses `[len=9, tag=0xFF, u64 HWM]`.
This plan uses an analogous terminal frame for log-fetch: `[len=9, tag=0xFE, u64 tail_seq]`.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: PerStreamLogReader::filtered + merge-iterator in event_log.rs</name>
  <files>src/state/event_log.rs</files>
  <behavior>
    - `PerStreamLogReader::open(stream_name, log_dir)` opens the per-stream log file(s) — handles multi-file rotation if the existing module has it (reuse whatever the existing reader does for rotation).
    - `reader.filtered(scope, from_seq) -> impl Iterator<Item = Result<LogEntryBytes>>` yields only entries where `entry.seq > from_seq`, `entry.stream == this reader's stream` (implicit), and the key matches the scope's `keys`/`key_prefix`/all rule (same three-way rule as 27-01's snapshot filter).
    - Reads frame-at-a-time; never buffers the whole log.
    - `merge_in_seq_order(iters: Vec<impl Iterator<Item=Result<LogEntryBytes>>>) -> impl Iterator<...>` interleaves per-stream iterators in global seq order (standard k-way merge on seq).
    - Iteration stops when all inputs reach the current tail captured at iterator-creation time (pin the tail, per D-Snapshot/log consistency handoff style — we do not chase writes that arrive mid-fetch; those are 27-03's job).
  </behavior>
  <action>
    In `src/state/event_log.rs`:
    1. Add `pub struct PerStreamLogReader { ... }` alongside the existing reader. `pub fn open(stream: &str, log_dir: &Path) -> Result<Self, LogError>`. Pin `tail_seq` at open time (read current highest seq; iteration stops at that).
    2. `pub fn filtered<'a>(&'a mut self, scope: &'a Scope, from_seq: u64) -> impl Iterator<Item = Result<LogEntryBytes>> + 'a`. Implementation: read one length-prefixed entry at a time, deserialize header to inspect (seq, key), emit raw bytes if it passes the filters, else skip.
    3. `pub struct LogEntryBytes { pub seq: u64, pub bytes: Vec<u8> }` — TCP handler uses `seq` for merge ordering and writes `bytes` to the socket.
    4. `pub fn merge_in_seq_order(iters: Vec<Box<dyn Iterator<Item=Result<LogEntryBytes>>>>) -> impl Iterator<Item = Result<LogEntryBytes>>` using `BinaryHeap` keyed on seq (ascending). Ties broken by stream name (deterministic).
    5. Tests: (a) filter on single stream, three `from_seq` values including 0, current HWM, beyond tail; (b) keys-set and key-prefix filters; (c) merge ordering across 3 streams with interleaved seqs; (d) streaming-property test ensures no full-file buffer.

    Do NOT modify the existing eager log reader — add new types alongside. Phase 6 invariants on per-stream files stay intact.
  </action>
  <verify>
    <automated>cargo test --lib state::event_log::filtered_ state::event_log::merge_</automated>
  </verify>
  <done>All filter + merge tests pass including streaming-property and interleaved-seq correctness.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: OP_LOG_FETCH handler in tcp.rs + Rust integration test</name>
  <files>src/server/tcp.rs, tests/test_replica_log_fetch.rs</files>
  <behavior>
    - `handle_log_fetch(stream, cmd, ctx)` runs in this order: (1) `require_loopback_or_token`; (2) `validate_scope`; (3) for each `stream in scope.streams`, open a `PerStreamLogReader` with tail pinned; (4) construct a `merge_in_seq_order` over the per-stream `filtered(&scope, from)` iterators; (5) write each entry as a 4-byte-length-prefixed frame; (6) write terminal "caught up" frame `[len=9, tag=0xFE, u64 tail_seq]`.
    - Replaces the 27-01 reserved-stub dispatch arm for 0x13.
    - Deterministic ordering across runs.
  </behavior>
  <action>
    In `src/server/tcp.rs`:
    1. Add `async fn handle_log_fetch(...)` mirroring the shape of `handle_snapshot_fetch` from 27-01.
    2. Replace the 27-01 reserved stub arm for `Command::LogFetch { from, scope }` with a real dispatch call.
    3. Reuse `require_loopback_or_token` + `validate_scope` — no copies.
    4. Open one `PerStreamLogReader` per requested stream, feed into `merge_in_seq_order`, loop-write each entry, emit terminal frame.
    5. Document the `0xFE` terminal tag in `protocol.rs` as a doc comment on `OP_LOG_FETCH` (symmetric with the `0xFF` HWM tag from 27-01).

    Create `tests/test_replica_log_fetch.rs` (Rust, shape mirrors `tests/test_replica_snapshot_fetch.rs` from 27-01):
    - Harness: spin up test server, push N events to 3 streams with interleaved timing so seqs are interleaved.
    - Happy path 1: `from=0`, scope={all 3 streams} → receive all events, seq-ordered, terminal tag `0xFE`.
    - Happy path 2: `from=<some mid-seq>` → receive only events with seq > from.
    - Empty result: `from=current tail` → zero entries + terminal frame.
    - Scope narrow: scope={1 stream} → only that stream's events.
    - Scope key filter: scope.keys={subset} → only matching entries.
    - Reject: missing auth; reject: empty streams; reject: pull="sample" (every rule already covered by 27-01 unit tests; here we sanity-check the dispatch wiring propagates them).
    - Merge determinism: run the happy path twice, assert byte-for-byte identical output.
  </action>
  <verify>
    <automated>cargo test --test test_replica_log_fetch</automated>
  </verify>
  <done>All LOG_FETCH tests pass; full `cargo test` green; clippy clean on tcp.rs.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: End-to-end Python asyncio clone-then-catchup integration test</name>
  <files>tests/integration/test_replica_clone_catchup.py</files>
  <behavior>
    - Test opens a raw asyncio TCP connection to a running test server (spawned by the test or via the shared `conftest.py` fixture — reuse whatever `test_replay_30d.py` uses for server lifecycle).
    - Drives the full clone-then-catchup sequence: authenticate → OP_SNAPSHOT_FETCH{scope} → collect entries until terminal HWM frame (tag 0xFF) → OP_LOG_FETCH{from: HWM, scope} → collect until terminal tail frame (tag 0xFE).
    - Asserts: (a) union of snapshot-entries ∪ log-entries == set of events the test pushed (modulo scope filter); (b) no duplicates (snapshot and log partition by the HWM); (c) no gaps in seq within the log portion; (d) entries with seq ≤ HWM came from snapshot; entries with seq > HWM came from log; (e) auth failure test: bad token → server closes with error frame, no entries delivered.
    - Also exercises: key_prefix scope, explicit-keys scope, scope that matches zero streams (expect rejection — 27-01 validator rule 2).
  </behavior>
  <action>
    Create `tests/integration/test_replica_clone_catchup.py`:
    1. Reuse the server-spawning fixture from `tests/integration/conftest.py` (inspect it; if it doesn't exist yet, add a minimal one — start the Tally server in a subprocess with a temp data dir, wait for `/metrics` 200, yield the TCP port, kill on teardown).
    2. Helper: `async def send_command(writer, opcode, payload_bytes, admin_token)` that writes the standard protocol frame (inspect `protocol.rs` for the exact framing; the CLI-push tests or Rust integration tests have a reference implementation to mirror).
    3. Helper: `async def read_framed(reader)` pulling 4-byte-length-prefixed frames until the terminal tag.
    4. Helper: `encode_scope(streams, keys=None, key_prefix=None, pull="all") -> bytes` — exact byte layout from 27-01 task 1 (`read_scope`/`write_scope`).
    5. Test function `test_clone_then_catchup_roundtrip`: push ~200 events across 2 streams (use existing OP_PUSH flow — grep for an existing Python push helper; `test_replay_30d.py` likely has one), trigger a snapshot (admin endpoint or wait for cycle), run the sequence, assert properties (a)-(d).
    6. Test function `test_clone_catchup_scope_filters`: same but with `keys` subset and `key_prefix` separately; assert only matching events delivered.
    7. Test function `test_auth_failure_closes_clean`: bad token → connection closes, no entries leaked.
    8. Mark tests with `@pytest.mark.asyncio` and follow whatever async runner config `conftest.py` uses.

    Do NOT build an SDK class. Direct socket code only — that's the whole point (Phase 28+ owns the SDK).
  </action>
  <verify>
    <automated>cd /data/home/tally && pytest tests/integration/test_replica_clone_catchup.py -x -v</automated>
  </verify>
  <done>Three pytest cases pass; full pytest run clean; test runs in under ~15s; no flakes across 3 back-to-back runs.</done>
</task>

</tasks>

<test_plan>
## Test Plan

**Levels:**
1. **Unit** — `PerStreamLogReader::filtered` + `merge_in_seq_order` (task 1).
2. **Integration (Rust)** — `tests/test_replica_log_fetch.rs` full TCP round-trip for OP_LOG_FETCH alone (task 2).
3. **Integration (Python, end-to-end)** — `tests/integration/test_replica_clone_catchup.py` clone-then-catchup across both 27-01 and 27-02 opcodes (task 3). **This is the user-flagged "every plan needs a test plan" deliverable for Phase 27.**

**Coverage matrix:**

| Concern | Test | Level |
|---|---|---|
| `filtered` iterator honors `from_seq`, scope | `event_log::filtered_*` | Unit |
| `merge_in_seq_order` k-way merge correct | `event_log::merge_*` | Unit |
| Streaming property (no full buffer) | `event_log::filtered_is_streaming` | Unit |
| LOG_FETCH happy path (from=0, from=mid) | `test_replica_log_fetch::happy_*` | Integ (Rust) |
| LOG_FETCH empty result (from=tail) | `test_replica_log_fetch::empty_beyond_tail` | Integ (Rust) |
| Auth + scope rejections | `test_replica_log_fetch::rejects_*` | Integ (Rust) |
| Determinism across runs | `test_replica_log_fetch::merge_deterministic` | Integ (Rust) |
| **Clone-then-catchup: snapshot ∪ log == pushed events, no dupes/gaps** | `test_replica_clone_catchup::test_clone_then_catchup_roundtrip` | **E2E (Python)** |
| Scope key-filter end-to-end | `test_replica_clone_catchup::test_clone_catchup_scope_filters` | E2E (Python) |
| Auth failure closes clean end-to-end | `test_replica_clone_catchup::test_auth_failure_closes_clean` | E2E (Python) |

**Out of scope:** live SUBSCRIBE push, backpressure drop — 27-03.

**Bench / load:** None in this plan. The Python test stays at ~200 events for fast feedback — larger volumes are 27-03's territory.
</test_plan>

<verification>
- `cargo test` full suite passes including `test_replica_log_fetch`.
- `pytest tests/integration/test_replica_clone_catchup.py -v` passes all 3 cases.
- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- Manual: run a real clone from `ncat` scripted sequence, verify 0xFE tag and tail_seq.
- Python test runs in CI under 15s (budget constraint).
</verification>

<success_criteria>
- OP_LOG_FETCH dispatch arm wired in tcp.rs, replacing the 27-01 reserved stub.
- `PerStreamLogReader::filtered` + `merge_in_seq_order` exist and are tested.
- Terminal tail frame tag `0xFE` documented on `OP_LOG_FETCH` in protocol.rs (symmetric with 0xFF from 27-01).
- Python asyncio end-to-end test demonstrates clone-then-catchup parity: snapshot ∪ log == pushed events.
- All three Python test cases pass; no SDK code added.
- Reused 27-01's `validate_scope` + `require_loopback_or_token` — no copies.
</success_criteria>

<output>
After completion, create `.planning/phases/27-server-replica-endpoints/27-02-SUMMARY.md`
summarizing: LOG_FETCH handler, the 0xFE tail-frame format, the Python test harness
patterns (so 27-03 and Phase 28+ reuse them), and observed performance of the 200-event
round-trip.
</output>
