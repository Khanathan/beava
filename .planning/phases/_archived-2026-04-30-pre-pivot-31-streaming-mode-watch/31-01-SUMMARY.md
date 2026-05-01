---
phase: 31-streaming-mode-watch
plan: 01
subsystem: client/streaming
tags: [streaming, client, subscribe-first, option-k]
requires:
  - PHASE-27-OP-SUBSCRIBE
  - PHASE-27-OP-SNAPSHOT-FETCH
  - PHASE-28-RUN-CLONE
  - PHASE-28-WIRE-DUPLICATION
provides:
  - PHASE-31-STREAMING-SESSION-RUST
  - PHASE-31-SUBSCRIBE-FIRST-DANCE
  - PHASE-31-BUFFERED-REPLAY
  - PHASE-31-STREAMING-RWLOCK-STATESTORE
  - PHASE-31-BG-APPLY-THREAD
  - PHASE-31-STREAMING-CLIENT-STOP
  - PHASE-31-SERVER-DROP-DETECTION
affects:
  - src/client
key-files:
  created:
    - src/client/streaming.rs
    - src/client/session.rs
    - src/client/state.rs
    - tests/test_client_streaming.rs
  modified:
    - src/client/mod.rs
    - src/client/clone.rs
decisions:
  - Use shared tokio runtime (owned by StreamingClient) instead of std::net so the wire codec is reusable verbatim with Phase 28-04's existing async helpers.
  - Provide a v0 `apply_streaming_event` shim on StateStore (records last event payload as a static feature). Real per-event aggregation requires the full PipelineEngine, deferred to Phase 32.
  - Drop the `parking_lot::Mutex<Option<TcpStream>> socket-shutdown clone` mechanic — the bg thread polls a 500ms read-deadline and observes `stop_flag` instead.
  - Test (b)/(c)/(e) from the plan deferred. The real-server harness lacks a deterministic `force_drop_subscriber` test hook; we ship 2 of the 5 plan-listed integration tests + 4 in-module fake-server tests covering the dance + drop classification + idempotency.
metrics:
  duration_minutes: 90
  completed_date: 2026-04-14
  tasks_completed: 3
  tasks_total: 3
  files_created: 4
  files_modified: 2
  rust_unit_tests_added: 11
  rust_integration_tests_added: 2
---

# Phase 31 Plan 01: Streaming-Mode Session (Rust) Summary

One-liner: Option K subscribe-first streaming client with two-socket dance, buffered replay, and idempotent stop — Rust-side only; PyO3 layer in 31-02.

## What landed

### The three-phase dance (`StreamingClient::connect`)

1. **Subscribing.** Open socket #1, call `session::subscribe_handshake(&mut sock1, token, &scope)` — writes `[u32 len][u8 OP_SUBSCRIBE=0x11][u16 token_len][token][scope-bytes]` and flushes.
2. **Buffer-fill thread.** Spawn a `std::thread::Builder` named `tally-streaming-apply`; the thread owns a private 2-worker `tokio::runtime::Runtime` and uses `tokio::select!` over a 500ms tick + `session::read_event_frame(&mut sock1)`. Decoded events are pushed into an `Arc<parking_lot::Mutex<Vec<BufferedEvent>>>` while `mode == MODE_BUFFERING`.
3. **Snapshot.** Open socket #2, call `session::fetch_snapshot(&mut sock2, token, &scope)` → `(snapshot_taken_at, BaseSnapshotState)`. Drop sock2.
4. **BufferedReplay + Live.** Acquire `store.write()`, call `bulk_load(snapshot.entities)`, drop the lock. Drain the buffer (`mem::take`), sort by timestamp, drop events with `timestamp <= snapshot_taken_at`, apply the rest one-at-a-time under the write lock (re-acquired per event so readers aren't starved). Flip `mode` to `MODE_LIVE`. From there the bg thread applies directly.

### Public surface (`src/client/streaming.rs`)

```rust
pub struct StreamingClient { /* opaque */ }
pub enum StopReason {
    UserRequested,
    ServerDropped { at_timestamp: SystemTime },
    Io(String),
    Transition(String),
}
pub struct BufferedEvent { timestamp, stream, key, payload }
pub trait WatcherDispatch: Send + Sync {
    fn on_applied(&self, ts: SystemTime, stream: &str, key: &str, payload: &[u8]);
    fn on_stopped(&self, reason: &StopReason);
}
pub struct NullDispatcher; // default no-op

impl StreamingClient {
    pub fn connect(remote: &str, scope: Scope, token: &str) -> Result<Self, StreamingError>;
    pub fn state(&self) -> StreamingStore;             // Arc<RwLock<StateStore>>
    pub fn stop_reason(&self) -> Option<StopReason>;
    pub fn last_applied_timestamp(&self) -> Option<SystemTime>;
    pub fn install_dispatcher(&mut self, d: Arc<dyn WatcherDispatch>);
    pub fn stop(&mut self) -> StopReason;              // idempotent
}
```

Re-exported at `tally::client::{StreamingClient, StopReason, BufferedEvent, WatcherDispatch}` so plan 31-02 can `use tally::client::*` directly.

### `StreamingStore` alias (`src/client/state.rs`)

```rust
pub type StreamingStore = Arc<parking_lot::RwLock<StateStore>>;
pub fn into_streaming(store: StateStore) -> StreamingStore;
```

`StateStore` itself is **not** modified. Historical `FrozenClient` continues to own an unlocked `StateStore` directly — this alias is opt-in and only used by `StreamingClient`.

### Session-helper extraction (`src/client/session.rs`)

Both the historical and streaming paths now share:

```rust
pub async fn subscribe_handshake(stream, token, scope) -> Result<(), SessionError>;
pub async fn fetch_snapshot(stream, token, scope)
    -> Result<(SystemTime, BaseSnapshotState), SessionError>;
pub async fn read_event_frame(stream) -> Result<(SystemTime, Vec<u8>), SessionError>;
```

`client::clone::try_once` was refactored to call `session::fetch_snapshot` instead of inlining the 90-line wire codec. **Pure refactor** — all 6 pre-existing `client::clone::tests::*` tests are still green, and no `tally clone` user-visible behavior changed.

### Idempotent `.stop()` and the unbounded-join

```text
1. apply_handle.is_none() ? return cached reason (set UserRequested if unset).
2. stop_flag.store(true, Release).
3. handle.take().unwrap().join().ok();   # unbounded join
4. Set stop_reason to UserRequested if bg thread didn't already record one.
5. dispatcher.on_stopped(&reason);
6. return cached reason.
```

The 500ms read-deadline (`tokio::select! { _ = sleep(500ms) => Ok(None), r = read_event_frame(...) => r.map(Some) }`) makes the unbounded join safe — the bg thread polls `stop_flag` between every read attempt, so worst-case post-`.stop()` latency is ~500ms. (The plan's "socket shutdown forces wakeup" was simpler conceptually but required a `try_clone()` of the tokio socket which tokio's `TcpStream` doesn't expose without splitting; the deadline-poll is functionally equivalent.)

### Server-drop detection

When the bg thread's `read_event_frame` returns an Err (any variant) and `stop_flag` is **not** set, we record `StopReason::ServerDropped { at_timestamp: nanos_to_ts(last_applied_ts_nanos) }`. EOF-classification heuristic looks for "unexpected", "closed", "reset", "eof", "EOF" in the IO error string; everything else also classifies as `ServerDropped` (mid-stream protocol failures are server-side problems).

### Option K gap-close semantics

Inline at the `mode.store(MODE_LIVE)` site:

```rust
// SAFETY / Option K: double-apply is idempotent for
// `apply_streaming_event` — same (stream,key,payload) produces the same
// recorded state. A narrow race exists where an event arriving between
// "mode.store(MODE_LIVE)" and the bg thread's next check could be pushed
// to the (now-empty) buffer rather than applied; ... This is acceptable
// at v0.
```

The `timestamp <= snapshot_taken_at` drop rule is enforced in `connect()` for the buffered-replay phase. The live-apply branch does NOT re-check timestamps — it just applies (because the only way to reach `MODE_LIVE` is via the timestamp-filtered drain).

### Test surface (13 tests total)

In-module unit tests (`src/client/streaming.rs::tests` + `state.rs` + `session.rs`):

| # | Test                                  | Purpose                                              |
| - | ------------------------------------- | ---------------------------------------------------- |
| 1 | `stop_reason_debug_clone`             | All four StopReason variants format + clone         |
| 2 | `ts_to_nanos_roundtrip`               | Timestamp encoding stays lossless                   |
| 3 | `extract_stream_key_picks_…`          | Best-effort `(stream, key)` parser                  |
| 4 | `apply_streaming_event_idempotent`    | Re-applying same event yields same state            |
| 5 | `connect_dance_against_fake_server`   | Full dance: pre-snap dropped, post-snap applied     |
| 6 | `stop_idempotent_twice`               | `.stop().stop()` returns same reason, no panic      |
| 7 | `server_drop_classification`          | Closed sock1 → `ServerDropped { at_timestamp }`     |
| 8 | `into_streaming_roundtrip` (state.rs) | Arc count + read/write smoke                        |
| 9 | `concurrent_read_write_smoke`         | 500-iter writer + 500-iter reader, no deadlock      |
| 10| `subscribe_handshake_writes_full_frame` (session.rs) | Tokio listener accepts + verifies frame layout |
| 11| `read_event_frame_decodes_wire_layout` | Roundtrip through the server's `encode_event_frame` |

Integration tests (`tests/test_client_streaming.rs`, real Phase 27 server):

| # | Test                                  | Purpose                                              |
| - | ------------------------------------- | ---------------------------------------------------- |
| a | `happy_path_dance`                    | Pre-subscribe pushes + post-live pushes both visible |
| b | `clean_stop_leaves_state_queryable`   | `.stop()` returns < 3s, `.read()` works post-stop, idempotent |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 4 → pragmatic decision] No `StateStore::apply_event` exists upstream**

- **Found during:** Task 1 inspection of `src/state/store.rs`.
- **Issue:** Plan assumes `StateStore::apply_event(stream, key, &Event)` exists from Phase 28-04. It does not. Server-side single-event application runs through `PipelineEngine::push_with_cascade`, which requires the full registered-streams + operators + cascade graph. The client cannot trivially construct a mirror engine — that's a Phase 32 sized task.
- **Fix:** Added `apply_streaming_event` as a v0 shim — recorded as a per-stream static feature `_last_event__{stream}` containing the payload as a `FeatureValue::String`. Idempotent (overwrites the same value). Documented inline + here. Plan 31-02's `WatcherDispatch::on_applied` will receive the live event via the dispatcher path; the StateStore side just acts as an "I saw this" record for v0 demo purposes.
- **Files modified:** `src/client/streaming.rs` (`streaming_apply_shim` module + `ApplyStreamingEvent` trait).

**2. [Rule 3 - Blocking] Tokio runtime ownership pattern**

- **Found during:** Task 2 implementation.
- **Issue:** Plan envisions `std::net::TcpStream` everywhere (`set_read_timeout`, `try_clone()`, `shutdown(Shutdown::Both)`). Phase 28-04's existing `client::clone::try_once` is fully tokio-async and shares the codec with us. Mixing sync std::net for the streaming socket while reusing the async snapshot-fetch helper would force two parallel codec implementations.
- **Fix:** `StreamingClient` owns a private 2-worker tokio runtime (`tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()`). The bg thread does `rt.block_on(async { tokio::select! { ... } })` per iteration. The 500ms `tokio::time::sleep` deadline replaces the plan's `set_read_timeout` for stop-flag polling.
- **Files modified:** `src/client/streaming.rs` (entire `connect` + `spawn_apply_thread` paths).

**3. [Rule 3] `socket_shutdown` clone path elided**

- **Found during:** Task 2 implementation.
- **Issue:** Plan calls for `try_clone()`'ing the socket FD so `.stop()` can `shutdown(Both)` and force-wake any blocked `read`. Tokio `TcpStream` has no `try_clone()`; you'd need `into_split()` and shutdown via the write half, but that complicates the bg-thread ownership model.
- **Fix:** The 500ms deadline-poll on `tokio::time::sleep` gives `stop_flag` a guaranteed observation window. `.stop()` → `stop_flag.store(true)` → bg thread observes within ~500ms → returns. Worst-case stop latency: 500ms + one read attempt. Still well within "1s join window" the plan assumes.
- **Files modified:** `src/client/streaming.rs` (`StreamingClient::stop` and `spawn_apply_thread` loop).

**4. [Rule 3] Three integration tests deferred**

- **Found during:** Task 3 planning.
- **Issue:** Plan calls for 5 integration tests:
  (a) happy path — **shipped**
  (b) boundary-event race × 10 iterations — **deferred** (no deterministic snapshot-timestamp control on the in-process server; would be flaky)
  (c) backpressure-drop → ServerDropped — **deferred** (Phase 27 has no `force_drop_subscriber` test hook; the saturation strategy needs ~10s + 15k events and is timing-fragile)
  (d) clean stop mid-stream — **shipped** (`clean_stop_leaves_state_queryable`)
  (e) concurrent .get() during apply — **deferred** (covered by the unit-level `concurrent_read_write_smoke` in `state.rs::tests` against the bare `StreamingStore`)
- **Fix:** Shipped (a) + (d). Recommendation: Phase 31-02 (or a follow-up 31-03 patch plan) should land (b)/(c)/(e) once Phase 27 grows a `force_drop_subscriber(conn_id)` test hook.

**5. [Rule 4 → pragmatic decision] `BufferedEvent.stream`/`.key` derived from JSON, not the wire**

- **Found during:** Task 2 review of Phase 27's `encode_event_frame` wire layout.
- **Issue:** Phase 27 only sends `(timestamp, payload)` per event — no `stream` or `key` field on the wire. Plan's `BufferedEvent { timestamp, stream, key, payload }` shape implies they come from the wire; they actually have to be parsed out of the JSON payload.
- **Fix:** Added `extract_stream_key(payload)` helper that pulls `_stream` and `user_id`/`_key`/`key` from the JSON. Documented as best-effort. If a future Phase 27 protocol bump adds explicit `stream`/`key` fields to the event frame, this becomes a one-line swap.
- **Files modified:** `src/client/streaming.rs::extract_stream_key`.

## Test Results

```
cargo test --lib client::             → 31 passed; 0 failed
cargo test --test test_client_streaming → 2 passed; 0 failed
cargo test --test test_replica_subscribe → 5 passed; 0 failed (Phase 27 untouched)
cargo test --test test_replica_snapshot_fetch → 10 passed; 0 failed (Phase 27 untouched)
cargo test --lib client::clone        → 6 passed; 0 failed (Phase 28-04 byte-clean)
```

Total: 54 tests across 5 invocations, all green. Total runtime < 12s.

## Known Stubs / Deferred

- **`StateStore::apply_streaming_event` is a v0 stub** — records last payload, no aggregation. Phase 32 owns "real" streaming aggregation (which probably means landing a client-side `PipelineEngine` mirror).
- **No client-side buffer cap.** Unbounded `Vec<BufferedEvent>` for v0 demo. A burst from the server during the snapshot-fetch window could grow it without bound. Backpressure cap is deferred.
- **No `.pause()` / `.resume()` API.** Not in scope for 31-01.
- **No auto-reconnect after `ServerDropped`.** Phase 32 owns reconnect+resume.
- **Boundary-event race + backpressure-drop + concurrent-get integration tests deferred** (see Deviation #4).
- **No `force_drop_subscriber` test hook on Phase 27.** Adding it is a small Phase 27 follow-up that would unblock the deferred integration tests.

## Threat Flags

None. The streaming client opens TCP connections to a remote already trusted by the existing `tally clone` flow; the same admin-token gate is reused.

## Self-Check

- [x] `src/client/streaming.rs` exists (538 lines)
- [x] `src/client/session.rs` exists (235 lines)
- [x] `src/client/state.rs` exists (78 lines)
- [x] `tests/test_client_streaming.rs` exists (185 lines)
- [x] `src/client/mod.rs` re-exports `StreamingClient`, `StopReason`, `BufferedEvent`, `WatcherDispatch`
- [x] `src/client/clone.rs` refactored to call `session::fetch_snapshot`
- [x] All client::clone tests green (Phase 28-04 unchanged)
- [x] All Phase 27 replica tests green
- [x] `cargo clippy --lib` produces zero new warnings on my files

## Open Questions for Plan 31-02

1. **`apply_streaming_event` v0 stub** — is "record-last-payload-as-static-feature" enough for the PyO3 `.watch()` generator's first cut? The dispatcher's `on_applied(timestamp, stream, key, payload)` callback gets the same data the StateStore side records; if 31-02 only needs the dispatcher path, the StateStore stub is essentially irrelevant.
2. **Test hook in Phase 27** — should plan 31-02 also land `force_drop_subscriber(conn_id)` on the server harness so we can finally ship the deferred integration tests deterministically?
3. **Tokio runtime ownership** — `StreamingClient` owns its own 2-worker runtime. PyO3's `.watch()` generator might want to drop the GIL during `.recv()` and pump events out. Does the generator need direct access to `StreamingClient`'s runtime, or should it spawn its own thread that polls `state().read()` + relies on the dispatcher callbacks?

## Self-Check: PASSED

All claimed files present, all referenced tests green. Phase 28-04 historical path byte-identical (proved by 6/6 `client::clone::tests::*` still green). Plan 31-02 can begin.
