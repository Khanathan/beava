---
phase: 31-streaming-mode-watch
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/client/session.rs
  - src/client/state.rs
  - src/client/mod.rs
  - src/client/streaming.rs
  - Cargo.toml
  - tests/test_client_streaming.rs
autonomous: true
requirements:
  - PHASE-31-SESSION-STREAMING-MODE
  - PHASE-31-SUBSCRIBE-UPGRADE
  - PHASE-31-BG-APPLY-THREAD
  - PHASE-31-STATESTORE-RWLOCK
  - PHASE-31-CLIENT-STOP-PATH
  - PHASE-31-SUBSCRIBER-DROPPED-DETECTION

must_haves:
  truths:
    - "After `Session::catchup` reaches LOG_FETCH tail, if the session was constructed with `mode=Streaming`, the client sends `OP_SUBSCRIBE{scope}` on the *same* socket and transitions `Catchup → Streaming`. No second socket, no second auth."
    - "In `Streaming` mode, a background Rust thread owns the socket and calls `apply_event` on every inbound event, taking a write lock on the client `StateStore` per event. `.get()` on the main thread takes a read lock and returns the post-apply value without blocking other readers."
    - "Events accepted server-side between LOG_FETCH tail and SUBSCRIBE handshake are still delivered — Phase 27 server guarantees the subscription resumes from `next_seq_after_handshake`. An integration test pushes events across the transition and asserts zero loss."
    - "`.stop()` is idempotent: signals the background thread (via an `AtomicBool` + socket shutdown), joins it with a bounded timeout (≤2s), transitions mode to `Stopped`. Subsequent `.get()` still works against the last applied state. Calling `.stop()` twice is a no-op."
    - "If the server drops the subscriber (backpressure / EOF), the background thread sets `stopped_reason = Dropped { at_seq }` on a shared `StreamingStatus` before exiting. Callers can observe the reason; no auto-reconnect."
    - "Historical mode path is untouched — the `RwLock` wrapper is inert on historical `StateStore` access (either a distinct type or `write()` is never taken outside the streaming thread)."
  artifacts:
    - path: "src/client/streaming.rs"
      provides: "StreamingSession runtime: owns the post-catchup socket, bg thread handle, StreamingStatus, stop signal; spawn_apply_thread(socket, state, status) → JoinHandle; StreamingStatus = { Running | Stopped{reason: StopReason} }; StopReason = UserRequested | Dropped{at_seq:u64} | Io(String)"
      contains: "spawn_apply_thread"
      min_lines: 180
    - path: "src/client/session.rs"
      provides: "Extended Session state machine with Streaming and Stopped variants; transition_to_streaming() sends OP_SUBSCRIBE after LOG_FETCH tail and hands socket ownership to the StreamingSession; stop() joins the bg thread and transitions to Stopped"
      contains: "Streaming"
    - path: "src/client/state.rs"
      provides: "parking_lot::RwLock wrapper around the streaming StateStore; read() for .get(), write() for apply_event on the bg thread. Historical StateStore keeps its current single-threaded shape (no wrapping)."
      contains: "RwLock"
    - path: "tests/test_client_streaming.rs"
      provides: "Rust integration test against a real Phase 27 server: (a) historical→streaming transition with events pushed across the boundary, (b) .stop() joins the thread within 2s, (c) server-drop → StopReason::Dropped{at_seq} observable, (d) concurrent .get() while apply thread is writing — no deadlock, values monotonic."
      min_lines: 220
  key_links:
    - from: "src/client/session.rs::Session::run"
      to: "src/client/streaming.rs::spawn_apply_thread"
      via: "after LOG_FETCH tail, if mode==Streaming → send OP_SUBSCRIBE, then hand socket + state Arc into streaming module"
      pattern: "spawn_apply_thread\\(|OP_SUBSCRIBE"
    - from: "src/client/streaming.rs::apply_loop"
      to: "src/client/state.rs::StateStore::write()"
      via: "bg thread reads framed events from socket, calls apply_event under write lock"
      pattern: "state\\.write\\(\\)|apply_event"
    - from: "src/client/session.rs::Session::stop"
      to: "src/client/streaming.rs::StreamingSession::stop"
      via: "AtomicBool stop flag + socket shutdown + JoinHandle::join with timeout"
      pattern: "stop_flag|JoinHandle"
    - from: "src/client/streaming.rs (EOF branch)"
      to: "StreamingStatus::Stopped{reason: Dropped{at_seq}}"
      via: "socket read returns 0 bytes unexpectedly → classify as server drop, record last-applied-seq"
      pattern: "StopReason::Dropped"
---

<objective>
Land the Rust-side client SUBSCRIBE upgrade. Extend the Phase 29 `Session` state
machine with a `Streaming` mode that, after LOG_FETCH reaches tail, sends
`OP_SUBSCRIBE{scope}` on the same socket, then hands the socket to a background
apply thread. Introduce a `parking_lot::RwLock` wrapper on the client
`StateStore` — only for the streaming path; historical mode is left intact.
Implement an idempotent `.stop()` that signals the thread, shuts down the
socket, joins with a bounded timeout, and records a `StopReason`.

Purpose: this plan delivers everything Phase 31 needs at the Rust layer. Plan
31-02 layers the PyO3 `.watch()` generator + `tally sync` CLI on top.

Output:
- New `src/client/streaming.rs` module (StreamingSession, bg-thread loop,
  StopReason classification).
- Extended `Session` state machine (Streaming + Stopped variants).
- `RwLock<StateStore>` on the streaming path only.
- `tests/test_client_streaming.rs` — integration test against a live Phase 27
  server covering mode transition, concurrent access, `.stop()`, and
  server-drop classification.

Locked decisions honored: A1 (reuse catchup socket), D1 (bg thread +
parking_lot::RwLock on client StateStore for streaming path only), E1
(idempotent .stop(), no auto-resume), plus Phase 27 handshake guarantee for
race coverage.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/ROADMAP.md
@.planning/STATE.md
@.planning/phases/31-streaming-mode-watch/31-CONTEXT.md
@.planning/phases/27-server-replica-endpoints/27-CONTEXT.md
@.planning/phases/28-client-engine-embedding/28-CONTEXT.md
@.planning/phases/29-session-manager-historical/29-CONTEXT.md

<interfaces>
<!-- These are the expected interfaces from Phases 27, 28, 29 that this plan
builds against. Phases 27-30 are in flight; if the landed interface differs
in a minor way (field ordering, helper name), match the landed shape — do
NOT block this plan on cosmetic drift. -->

From src/server/protocol.rs (Phase 27):
```rust
pub const OP_SUBSCRIBE: u8 = 0x11;
pub struct Scope { pub streams: Vec<String>, pub keys: Option<KeyFilter> }
// Wire format for OP_SUBSCRIBE: [opcode=0x11][scope payload (reuse SnapshotFetch encoding)]
// Server pushes framed events: [4-byte len][u64 seq][event bytes]
// No terminal frame — stream runs until disconnect.
```

From src/client/session.rs (Phase 29 — current expected shape):
```rust
pub enum Mode { Bootstrap, Catchup, Done }   // this plan adds: Streaming, Stopped
pub struct Session { pub mode: Mode, pub socket: TcpStream, pub state: StateStore, pub scope: Scope, pub requested_mode: RequestedMode, ... }
pub enum RequestedMode { Historical, Streaming }
impl Session {
    pub fn run(&mut self) -> Result<(), ClientError>;   // extend: after Catchup tail, branch on requested_mode
}
```

From src/client/state.rs (Phase 28 — current expected shape):
```rust
pub struct StateStore { ... }
impl StateStore {
    pub fn apply_event(&mut self, stream: &str, key: &str, seq: u64, event: &Event);
    pub fn get(&self, stream: &str, key: &str) -> Option<Value>;
}
```
This plan introduces a `StreamingStore` newtype (or `Arc<RwLock<StateStore>>`
alias) used only on the streaming path. Historical `Session::state` stays a
plain `StateStore`.

From Cargo.toml:
```toml
# parking_lot is already a workspace dependency (used in src/engine/).
# If not yet exposed under feature="client", add it to the client feature
# block in this plan's task 1.
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: StreamingStore (RwLock wrapper) + StreamingStatus + StopReason + module skeleton</name>
  <files>src/client/streaming.rs, src/client/state.rs, src/client/mod.rs, Cargo.toml</files>
  <behavior>
    - Introduce `StreamingStore = Arc<parking_lot::RwLock<StateStore>>` (type alias or thin newtype) in `src/client/state.rs`. Historical code paths do NOT wrap — `Session` holds `StateStore` directly in historical mode, `StreamingStore` in streaming mode (enum or two Session variants — implementer picks the cleanest expression).
    - `StreamingStatus`: atomic enum readable from any thread. Variants: `Running`, `Stopped(StopReason)`. Use `ArcSwap<StreamingStatus>` or a `Mutex<StreamingStatus>` — pick the simpler; this is not a hot path.
    - `StopReason`: `UserRequested`, `Dropped { at_seq: u64 }`, `Io(String)`. `Debug + Clone`.
    - `StreamingSession`: holds `JoinHandle<()>` for the apply thread, `Arc<AtomicBool>` stop flag, `Arc<Mutex<Option<StreamingStatus>>>` (or ArcSwap), `StreamingStore` handle, last-applied-seq `Arc<AtomicU64>` (updated by the bg thread after each successful apply — used to fill `Dropped{at_seq}`).
    - Tests (in `#[cfg(test)]` inside streaming.rs): construct a `StreamingStore`, spawn a dummy writer thread doing N writes, read from main thread — values monotonic, no deadlock, `get()` never blocks `write()` beyond one event.
  </behavior>
  <action>
    1. Verify `parking_lot` is available under the `client` feature. If not, add to `Cargo.toml`:
       ```toml
       [features]
       client = [..., "dep:parking_lot"]
       [dependencies]
       parking_lot = { version = "0.12", optional = true }
       ```
       (If `parking_lot` is already a non-optional workspace dep — grep `Cargo.toml` for it — skip this step.)
    2. Create `src/client/streaming.rs`:
       ```rust
       use std::sync::Arc;
       use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
       use parking_lot::{RwLock, Mutex};
       use std::thread::JoinHandle;

       #[derive(Debug, Clone)]
       pub enum StopReason { UserRequested, Dropped { at_seq: u64 }, Io(String) }

       #[derive(Debug, Clone)]
       pub enum StreamingStatus { Running, Stopped(StopReason) }

       pub struct StreamingSession {
           pub(crate) handle: Option<JoinHandle<()>>,
           pub(crate) stop_flag: Arc<AtomicBool>,
           pub(crate) status: Arc<Mutex<StreamingStatus>>,
           pub(crate) last_seq: Arc<AtomicU64>,
           pub(crate) store: StreamingStore,
           // socket ownership moves INTO the spawned thread; StreamingSession
           // only retains a clone of the shutdown handle if needed for
           // out-of-band socket::shutdown(Both) during .stop().
           pub(crate) shutdown_handle: Option<std::net::Shutdown /* or tokio::sync::Notify depending on socket type */>,
       }

       impl StreamingSession {
           pub fn status(&self) -> StreamingStatus { self.status.lock().clone() }
           pub fn stop(&mut self) -> StopReason { /* idempotent: if handle None, return cached status */ }
           pub fn last_applied_seq(&self) -> u64 { self.last_seq.load(Ordering::Acquire) }
       }
       ```
       The `stop()` implementation:
         - If `handle.is_none()` → return the cached `status` (idempotent).
         - Set `stop_flag = true`.
         - Shutdown the socket (via the retained shutdown handle or by sending a signal that causes the next `read` to error).
         - `handle.take().unwrap().join()` with a timeout wrapper — use a `std::thread::spawn` + `crossbeam_channel::recv_timeout(2s)` pattern, or document that we accept an unbounded join (v0 simplicity). **Pick unbounded join + socket-shutdown-forces-read-EOF** as the v0 approach; document in the SUMMARY. `.stop()` returns the `StopReason` stored in status (defaulting to `UserRequested` if we initiated).
    3. In `src/client/state.rs`: add
       ```rust
       pub type StreamingStore = Arc<parking_lot::RwLock<StateStore>>;
       pub fn into_streaming(store: StateStore) -> StreamingStore { Arc::new(RwLock::new(store)) }
       ```
       Keep `StateStore` itself unchanged.
    4. Register the module: `src/client/mod.rs` add `pub mod streaming;`.
    5. Unit tests inside `streaming.rs` (`#[cfg(test)] mod tests`):
       - `stop_reason_debug_clone` smoke test.
       - `streaming_store_concurrent_read_write`: spawn writer thread doing 10_000 `write().apply_event(..)`-like ops; reader thread doing 10_000 `read().get(..)` ops; assert no deadlock, final value matches expected last-write.
       - `streaming_session_stop_is_idempotent_without_thread`: construct a `StreamingSession` with `handle = None` and `status = Stopped(UserRequested)`; `stop()` returns the cached reason; calling it again returns the same reason.
  </action>
  <verify>
    <automated>cargo test --features client --lib client::streaming::</automated>
  </verify>
  <done>`streaming.rs` compiles; 3 unit tests pass; `parking_lot` confirmed available under `client` feature; clippy clean on the new module.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Session state-machine extension (Streaming + Stopped) + OP_SUBSCRIBE upgrade + bg apply thread</name>
  <files>src/client/session.rs, src/client/streaming.rs, src/client/mod.rs</files>
  <behavior>
    - Extend `Mode` enum with `Streaming` and `Stopped` variants. Extend `RequestedMode` to `{ Historical, Streaming }` if not already present.
    - After `Session::run()` drains the LOG_FETCH response and would transition `Catchup → Done`: if `requested_mode == Streaming`, instead send `OP_SUBSCRIBE{scope}` on the same socket, transition `Catchup → Streaming`, move the socket + an `Arc`-wrapped `StreamingStore` into a spawned apply thread (std::thread::spawn), and return from `run()`.
    - The apply thread loop:
      ```
      loop {
          if stop_flag.load(Acquire) { set_status(Stopped(UserRequested)); return; }
          match read_frame(&mut socket) {
              Ok(Frame { seq, event }) => {
                  store.write().apply_event(stream, key, seq, &event);
                  last_seq.store(seq, Release);
              }
              Err(Eof) => { set_status(Stopped(Dropped{ at_seq: last_seq.load(Acquire) })); return; }
              Err(Io(e)) => {
                  if stop_flag.load(Acquire) { set_status(Stopped(UserRequested)); }
                  else { set_status(Stopped(Io(e.to_string()))); }
                  return;
              }
          }
      }
      ```
      Frame format matches Phase 27 server push: `[4-byte len][u64 seq][event bytes]` (reuse any decode helper landed in Phase 29's catchup loop).
    - `Session::stop()`: delegates to `StreamingSession::stop()`; transitions `Mode → Stopped`. If mode was not `Streaming`, returns `Err(NotStreaming)` or is a no-op (implementer picks — prefer no-op to keep caller code simple; document in SUMMARY).
    - Mode-transition race coverage: after sending OP_SUBSCRIBE, the server's first pushed event has `seq > log_fetch_tail_seq` per Phase 27's handshake guarantee. The apply thread does NOT need a de-dup filter — it just applies in order. Comment this assumption explicitly at the `transition_to_streaming` call site.
    - Calling `Session::run()` on a session already in `Stopped` state → return `Err(ClientError::AlreadyStopped("construct a new Session to resume"))`. The PyO3 layer (plan 31-02) maps this to a Python `RuntimeError`.
  </behavior>
  <action>
    1. In `src/client/session.rs`:
       - Add `Mode::Streaming` and `Mode::Stopped` variants.
       - Add `RequestedMode::Streaming` if missing.
       - Extract the current catchup-complete branch into a method `fn on_catchup_tail_reached(&mut self) -> Result<(), ClientError>`. Inside, branch on `requested_mode`:
         ```rust
         match self.requested_mode {
             RequestedMode::Historical => { self.mode = Mode::Done; Ok(()) }
             RequestedMode::Streaming => self.transition_to_streaming(),
         }
         ```
       - Implement `fn transition_to_streaming(&mut self)`:
         1. Build `OP_SUBSCRIBE{scope}` frame (reuse protocol encoder from Phase 27).
         2. Write to self.socket, flush.
         3. Take ownership of `self.socket` (swap out via `Option<TcpStream>` field), wrap `self.state` into `StreamingStore`, and call `streaming::spawn_apply_thread(socket, store, stop_flag, status, last_seq)`.
         4. Store the returned `StreamingSession` on `self.streaming: Option<StreamingSession>`.
         5. `self.mode = Mode::Streaming;` return Ok(()).
    2. In `src/client/streaming.rs`, add the public entry point:
       ```rust
       pub fn spawn_apply_thread(
           socket: TcpStream,
           store: StreamingStore,
           stop_flag: Arc<AtomicBool>,
           status: Arc<Mutex<StreamingStatus>>,
           last_seq: Arc<AtomicU64>,
       ) -> StreamingSession { ... }
       ```
       Inside, call `socket.set_read_timeout(Some(Duration::from_millis(500)))` so the loop can periodically observe `stop_flag` even if no events arrive. On `WouldBlock` / timeout → continue. On Eof → `Dropped{at_seq: last_seq.load(Acquire)}`. On other IO → `Io(...)` unless stop_flag set, then `UserRequested`.
       Retain the socket's shutdown handle (via `socket.try_clone()` before moving the original into the thread, OR by wrapping the socket in `Arc<TcpStream>` — `TcpStream::shutdown` only needs `&self`). Store the clone on `StreamingSession` so `stop()` can force-wake the read loop.
    3. `impl Session::stop(&mut self) -> Result<StopReason, ClientError>`:
       - If `mode == Stopped` → return cached reason (idempotent).
       - If `mode != Streaming` → return `Err(NotStreaming)` (caller can ignore — PyO3 layer treats as no-op).
       - Otherwise: `self.streaming.take().unwrap().stop()` → sets mode to `Stopped`, returns reason.
    4. `impl Session::run`: if entering `run()` with mode already `Stopped` → `Err(ClientError::AlreadyStopped)`.
    5. No Rust integration test here yet — Task 3 adds it once the live server path is exercisable. Add one unit test: `transition_to_streaming_sets_mode_and_writes_subscribe` using an in-memory socket pair (`std::os::unix::net::UnixStream::pair()` or a mock `Write` impl) to assert the SUBSCRIBE opcode + scope bytes are written.
  </action>
  <verify>
    <automated>cargo test --features client --lib client::session:: client::streaming::</automated>
  </verify>
  <done>Unit tests pass; mode enum extended; `transition_to_streaming` writes OP_SUBSCRIBE before spawning the thread; `.stop()` is idempotent; clippy clean; inline comment at handshake site documents reliance on Phase 27 server's next-seq guarantee.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Integration test — live Phase 27 server, transition race, .stop(), server-drop classification</name>
  <files>tests/test_client_streaming.rs</files>
  <behavior>
    - Spin up an in-process test server (reuse the fixture from Phase 27's `test_replica_subscribe.rs` — import or duplicate the helpers; small refactor to `tests/common/replica_server.rs` is acceptable).
    - Tests cover:
      (a) **Historical→Streaming transition with race coverage.** Push 100 events via OP_PUSH, construct a Session with `mode=Streaming` and a scope covering those events, drive `run()` to completion. After `run()` returns, spawn a task that pushes 50 more events. Poll the client StateStore until `last_applied_seq >= 150` (timeout 5s). Assert no events missed (key coverage: all 150 keys present with correct values).
      (b) **Transition-boundary race.** Push events *during* the narrow window between LOG_FETCH tail and OP_SUBSCRIBE handshake — simulated by a pusher thread running concurrently with `run()`. Assert the final applied set equals the full pushed set; no gaps. This is the Phase 27 server-guarantee verification.
      (c) **.stop() joins within 2s.** Start a streaming session, let it apply a few events, call `session.stop()`. Assert it returns within 2s and `status() == Stopped(UserRequested)`. Verify `.get()` still works post-stop.
      (d) **Idempotent .stop().** Call `.stop()` twice; second call returns the same `StopReason` without panicking.
      (e) **Server-drop classification.** Force the server to drop the subscriber (push > 10_000 events without letting the client drain them — the bg thread drains by default, so instead: have the server-side test shut down the subscriber registry entry explicitly via a test hook, OR kill the server-side connection). Observe `status() == Stopped(Dropped{at_seq})` where `at_seq` equals the last seq the client applied.
      (f) **Concurrent `.get()` during apply.** Spawn a reader thread calling `state.read().get(k)` in a tight loop while the bg apply thread is writing. Assert no deadlock within 2s of sustained concurrent access, and observed values are monotonic in seq (never go backwards).
    - Run against release build not required; default debug is fine. Tests are `#[test]` (std threads, std::net), not tokio — the client uses blocking IO in v0.
  </behavior>
  <action>
    1. Create `tests/test_client_streaming.rs`:
       - Reuse/factor helpers: `start_test_server()` returning a `TestServerHandle` with `.addr() -> SocketAddr`, `.push_event(stream, key, event)`, `.force_drop_subscriber(conn_id)` (new test hook; add to Phase 27 server under `#[cfg(test)]` if not already present).
       - Six `#[test]` functions mapping 1:1 to behaviors (a)–(f).
       - Use `std::thread::spawn` for concurrent pushers / readers. Use `crossbeam_channel` or `std::sync::mpsc` to synchronize test phases.
       - For (b), the race window: spawn the pusher thread BEFORE calling `Session::run()`; have it push events in a loop with small sleeps; `Session::run()` takes a variable amount of time through bootstrap + catchup, naturally landing some pushes in the transition window. Run the test with `--test-threads=1` to avoid contention with other tests, and loop the scenario ~20 times to catch the race reliably. If flaky, add a deterministic race hook: a `#[cfg(test)] pub fn set_transition_delay(d: Duration)` on the client that sleeps between LOG_FETCH-tail and OP_SUBSCRIBE.
       - For (e), prefer the explicit `force_drop_subscriber` test hook over the backpressure approach — faster and deterministic.
    2. If the Phase 27 server doesn't already expose `force_drop_subscriber` under `#[cfg(test)]`, add it as a minimal test-only API on `SubscriberRegistry` (no production impact):
       ```rust
       #[cfg(test)]
       pub fn force_drop(&self, conn_id: u64) { self.map.remove(&conn_id); }
       ```
       Document in SUMMARY.
    3. For (a) and (b), assertions check both the StateStore's final contents (via `.read().get(k)`) AND `streaming_session.last_applied_seq()` — belt-and-suspenders.
    4. For (c), use a `std::time::Instant::now()` wrapper: `assert!(start.elapsed() < Duration::from_secs(2))` around the `.stop()` call.
    5. Timeout guard: each `#[test]` wraps its body in a `std::thread::spawn` + `join_timeout(30s)` pattern so a hung test doesn't wedge CI.
  </action>
  <verify>
    <automated>cargo test --features client --test test_client_streaming</automated>
  </verify>
  <done>All 6 integration tests pass; 3x back-to-back runs of the race test (b) all green; total runtime under 60s; `force_drop_subscriber` test hook (if added) is `#[cfg(test)]`-gated with zero production footprint.</done>
</task>

</tasks>

<test_plan>
## Test Plan

**Levels:**
1. **Unit** — `StreamingStore` RwLock concurrency, `StopReason`/`StreamingStatus` shape, `transition_to_streaming` writes OP_SUBSCRIBE (task 1 + 2, in-module `#[cfg(test)]`).
2. **Integration (Rust, live server)** — `tests/test_client_streaming.rs` drives a real Phase 27 server through the full bootstrap→catchup→streaming path (task 3).

**Coverage matrix:**

| Concern | Test | Level |
|---|---|---|
| `StreamingStore` concurrent read/write no deadlock | `streaming::tests::streaming_store_concurrent_read_write` | Unit |
| `StopReason` + `StreamingStatus` debug/clone | `streaming::tests::stop_reason_debug_clone` | Unit |
| `.stop()` idempotent with no live thread | `streaming::tests::streaming_session_stop_is_idempotent_without_thread` | Unit |
| `transition_to_streaming` writes correct OP_SUBSCRIBE bytes | `session::tests::transition_to_streaming_writes_subscribe` | Unit |
| Historical → Streaming end-to-end, live push after transition | `test_client_streaming::historical_to_streaming_and_live` | Integ (a) |
| Transition-boundary race: no events lost | `test_client_streaming::transition_boundary_no_loss` (looped 20×) | Integ (b) |
| `.stop()` joins within 2s + `.get()` still works | `test_client_streaming::stop_joins_fast_and_state_readable` | Integ (c) |
| `.stop()` idempotent | `test_client_streaming::stop_idempotent` | Integ (d) |
| Server-drop → `StopReason::Dropped{at_seq}` | `test_client_streaming::server_drop_sets_dropped_reason` | Integ (e) |
| Concurrent `.get()` during apply — no deadlock, monotonic | `test_client_streaming::concurrent_get_during_apply` | Integ (f) |

**Known gaps documented (not tested, per CONTEXT.md §Deferred):**
- No client-side backpressure — slow consumers rely on server drop (Phase 27 behavior). Tested at the server layer in 27-03.
- No auto-reconnect — `Dropped{at_seq}` surfaces to the caller; Phase 32 adds resume.
- No `.pause()` / `.resume()` API.

**Load considerations:** The race test (b) loops ~20 iterations; each iteration pushes ~5 events across the boundary. Total runtime budgeted at ~10s. The concurrent-access test (f) runs 2s of sustained load — no throughput assertion, just no-deadlock.

**Out of scope (plan 31-02):** PyO3 `.watch()` generator, GIL release, `tally sync` CLI, Python E2E.
</test_plan>

<verification>
- `cargo test --features client --lib client::` passes (unit tests).
- `cargo test --features client --test test_client_streaming` passes (integration tests, all 6).
- `cargo clippy --features client --all-targets -- -D warnings` clean.
- Manual sanity: run `cargo test --features client --test test_client_streaming transition_boundary_no_loss -- --nocapture` three times back-to-back — all green.
- Inline comment at `transition_to_streaming` call site explicitly documents reliance on Phase 27 server's "next-seq-after-handshake" guarantee.
- `StopReason::Dropped{at_seq}` is observable via `StreamingSession::status()` after a server-side drop.
</verification>

<success_criteria>
- `Mode::Streaming` and `Mode::Stopped` variants exist; `Session::run()` branches on `requested_mode` at catchup-tail.
- OP_SUBSCRIBE is sent on the *same* socket as bootstrap+catchup — single connection, single auth.
- Background apply thread owns the socket post-transition; applies events under a write lock on `StreamingStore`.
- `.stop()` is idempotent, joins the thread, and transitions mode to `Stopped`; returns a `StopReason`.
- Server-drop is classified as `StopReason::Dropped{at_seq}`; no auto-reconnect.
- Historical-mode path is unchanged — no `RwLock` on historical `StateStore`.
- Integration tests cover the transition race, the drop classification, and concurrent read/write.
- Rust surface ready for Plan 31-02 to bind `.watch()` and `tally sync` on top.
</success_criteria>

<output>
After completion, create `.planning/phases/31-streaming-mode-watch/31-01-SUMMARY.md` summarizing:
- The extended state machine (Bootstrap → Catchup → {Done | Streaming} → Stopped).
- The exact `StreamingStore` / `StreamingSession` types exported to plan 31-02.
- The socket-shutdown strategy used to unblock the bg thread on `.stop()`.
- The decision on bounded-vs-unbounded `JoinHandle::join` (v0: unbounded + socket-shutdown forces EOF; document rationale).
- The `force_drop_subscriber` test hook added to Phase 27 server (if any).
- Any deviations from the expected Phase 27/28/29 interfaces (minor drift expected; document what was matched vs. what was adjusted).
- Known gap: no client backpressure, no auto-resume (Phase 32).
</output>
