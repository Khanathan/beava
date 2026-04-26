---
phase: 27-server-replica-endpoints
plan: 03
type: execute
wave: 3
depends_on: [27-01, 27-02]
files_modified:
  - src/server/protocol.rs
  - src/server/tcp.rs
  - src/server/mod.rs
  - src/server/replica.rs
  - src/server/signals.rs
  - src/server/http.rs
  - src/engine/pipeline.rs
  - tests/test_replica_subscribe.rs
  - tests/integration/test_replica_subscribe_live.py
autonomous: true
requirements:
  - PHASE-27-OP_SUBSCRIBE
  - PHASE-27-SUBSCRIBER-REGISTRY
  - PHASE-27-INGEST-NOTIFY-HOOK
  - PHASE-27-BACKPRESSURE-DROP
  - PHASE-27-REPLICA-METRICS
  - PHASE-27-REPLICA-SIGNALS

must_haves:
  truths:
    - "A client that sends OP_SUBSCRIBE{scope} with valid admin auth, then receives live pushes of every newly-ingested event matching the scope, in seq-monotonic order, until the client disconnects."
    - "A slow consumer that lets its per-subscriber queue fill beyond 10_000 events gets dropped by the server with reason='backpressure'; server continues serving others; dropped consumer can reconnect via OP_LOG_FETCH{from: last_seq} to catch up."
    - "The ingest hot path (push_with_cascade) invokes notify_subscribers after a successful append without taking any shared lock that blocks other pushes — DashMap gives lock-free hot-path reads, matching Phase 14's concurrency model."
    - "/metrics exposes tally_replica_subscriptions_active, tally_replica_events_pushed_total{stream}, tally_replica_subscribers_dropped_total{reason}, tally_replica_snapshot_bytes_sent_total; SignalRegistry emits warning on backpressure-drop and error on replica-auth-failure."
  artifacts:
    - path: "src/server/replica.rs"
      provides: "SubscriberRegistry (DashMap<u64, ReplicaSession>) + register/unregister/notify_all APIs; ReplicaSession holds scope + bounded mpsc::Sender<Event> cap 10_000"
      contains: "SubscriberRegistry"
    - path: "src/server/tcp.rs"
      provides: "handle_subscribe() dispatch arm replacing OP_SUBSCRIBE_RESERVED; registers session, spawns per-conn drainer task, removes on disconnect"
      contains: "handle_subscribe"
    - path: "src/engine/pipeline.rs"
      provides: "notify_subscribers hook invocation inside push_with_cascade_internal after successful append; no-op if registry empty"
      contains: "notify_subscribers"
    - path: "src/server/http.rs"
      provides: "four new Prometheus metrics rendered in /metrics output"
      contains: "tally_replica_subscriptions_active"
    - path: "src/server/signals.rs"
      provides: "emit_replica_dropped(reason) and emit_replica_auth_failure() helpers using existing SignalRegistry::record"
      contains: "emit_replica_dropped"
    - path: "tests/test_replica_subscribe.rs"
      provides: "Rust tests: happy live push, backpressure drop at 10_001, multi-subscriber independence, disconnect cleanup, auth reject, seq-monotonic ordering property"
      min_lines: 180
    - path: "tests/integration/test_replica_subscribe_live.py"
      provides: "Python asyncio: subscribe → trigger pushes from another connection → receive matching events live; scope-filter test; slow-consumer drop test (assert metric + signal)"
      min_lines: 140
  key_links:
    - from: "src/engine/pipeline.rs::push_with_cascade_internal"
      to: "SubscriberRegistry::notify_all"
      via: "called after successful append + seq assignment; lock-free DashMap iteration"
      pattern: "notify_all\\(|notify_subscribers\\("
    - from: "src/server/replica.rs::ReplicaSession"
      to: "tokio::sync::mpsc::channel cap 10_000"
      via: "bounded channel; try_send failure → drop subscriber (backpressure)"
      pattern: "mpsc::channel\\(10_000\\)|try_send"
    - from: "src/server/replica.rs"
      to: "SignalRegistry + metrics"
      via: "on drop → emit signal + increment counter; on register/unregister → update gauge"
      pattern: "emit_replica_dropped|tally_replica_subscribers_dropped_total"
    - from: "tests/integration/test_replica_subscribe_live.py"
      to: "/metrics + /debug/signals (or equivalent)"
      via: "slow-consumer test asserts backpressure counter incremented"
      pattern: "tally_replica_subscribers_dropped_total"
---

<objective>
Land `OP_SUBSCRIBE{scope}` (0x11 — replaces the reserved stub) plus the ingest-path
notify hook, subscriber registry, backpressure policy, metrics, and signals required
for live streaming. This is the "live" leg of Phase 28+ replica's `bootstrap → catchup →
live` state machine. Everything else in this plan (DashMap registry, bounded mpsc,
10k-drop policy, metrics/signals) is infrastructure that Phase 27-03 must ship because
there is no other phase that owns it.

Purpose: Completes Phase 27. After this, Phase 28 has three working server opcodes to
build the client SDK against.

Output: New `src/server/replica.rs` module; `notify_subscribers` hook in the ingest
path; four new metrics; two new signal helpers; one Rust integration test; one Python
asyncio end-to-end test (including a slow-consumer backpressure-drop case, per user's
test-plan requirement).
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
@.planning/phases/27-server-replica-endpoints/27-02-SUMMARY.md

@src/server/protocol.rs
@src/server/tcp.rs
@src/server/signals.rs
@src/server/http.rs
@src/engine/pipeline.rs

<interfaces>
<!-- From 27-01 / 27-02 (landed before this plan runs): -->

From src/server/protocol.rs:
```rust
pub const OP_SUBSCRIBE_RESERVED: u8 = 0x11;   // this plan replaces with OP_SUBSCRIBE
pub struct Scope { ... };
pub fn validate_scope(scope: &Scope, known: &HashSet<String>) -> Result<(), ScopeError>;
pub enum Command { ..., Subscribe { scope: Scope } };   // this plan adds the Subscribe variant
```

From src/server/signals.rs:
```rust
pub type SharedRegistry = Arc<RwLock<SignalRegistry>>;
pub fn emit_snapshot_failure(registry: &SharedRegistry, err: &str);  // reference shape
impl SignalRegistry { pub fn record(&mut self, sig: Signal); }
```

From src/engine/pipeline.rs:
```rust
pub fn push_with_cascade(&mut self, stream: &str, event: Event, store: &mut Store, now: SystemTime) -> Result<u64, Error>;
// Returns assigned seq. This plan adds a notify call just before returning Ok(seq).
```

From src/server/http.rs (existing /metrics rendering):
```rust
// Current pattern: format! with HELP/TYPE lines and one gauge/counter per block.
// Extend by appending four new metric blocks.
```

Terminal-frame conventions (from 27-01, 27-02):
- Snapshot terminal: `[len=9, tag=0xFF, u64 HWM]`
- Log terminal: `[len=9, tag=0xFE, u64 tail_seq]`
- Subscribe has no terminal frame — stream runs until disconnect.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: SubscriberRegistry + ReplicaSession module (src/server/replica.rs) + metrics/signals hooks</name>
  <files>src/server/replica.rs, src/server/mod.rs, src/server/signals.rs, src/server/http.rs</files>
  <behavior>
    - `SubscriberRegistry` wraps `DashMap<u64, ReplicaSession>`; unique `conn_id: u64` per subscriber from an `AtomicU64`. `register(scope) -> (conn_id, mpsc::Receiver<Event>)`. `unregister(conn_id)`. `notify_all(&Event, seq: u64)` iterates the DashMap and `try_send`s to each matching session; on `try_send` error (full or closed) → call `unregister(conn_id)` + emit backpressure signal + bump the dropped counter.
    - `ReplicaSession { scope: Scope, tx: mpsc::Sender<(u64, Event)>, conn_id: u64 }` with `scope_matches(event_stream, event_key) -> bool` using the same three-way rule from 27-01.
    - `/metrics` endpoint renders four new counters/gauges (names in CONTEXT.md §Metrics). Gauge is live-read from `registry.len()`.
    - Signals: `emit_replica_dropped(registry, reason)` (category=operational, severity=warning), `emit_replica_auth_failure(registry, detail)` (category=safety, severity=error).
    - Concurrency: hot path `notify_all` must not hold any per-subscriber lock while iterating. DashMap guarantees shard-level locking — standard pattern from Phase 14 (per memory note: per-stream locks + DashMap).
  </behavior>
  <action>
    1. Create `src/server/replica.rs`:
       ```rust
       pub struct SubscriberRegistry { map: DashMap<u64, ReplicaSession>, next_id: AtomicU64,
                                       dropped_counter: Arc<AtomicU64>, pushed_counter: Arc<DashMap<String, AtomicU64>>,
                                       signals: SharedRegistry }
       pub struct ReplicaSession { pub scope: Scope, pub tx: tokio::sync::mpsc::Sender<(u64, Event)>, pub conn_id: u64 }
       impl SubscriberRegistry {
           pub fn register(&self, scope: Scope) -> (u64, tokio::sync::mpsc::Receiver<(u64, Event)>) { /* bounded channel cap 10_000 */ }
           pub fn unregister(&self, conn_id: u64);
           pub fn notify_all(&self, stream: &str, key: &str, seq: u64, event: &Event);
           pub fn active_count(&self) -> usize { self.map.len() }
           pub fn snapshot_pushed_counter(&self) -> Vec<(String, u64)>;
           pub fn dropped_count(&self) -> u64;
       }
       ```
       Bounded channel cap = 10_000 (D-Backpressure A1 — event-count, not byte-count). On `tx.try_send` Err(Full) or Err(Closed) → call `unregister` + `emit_replica_dropped(self.signals, "backpressure" or "disconnect")` + increment `dropped_counter`.
    2. Register the module in `src/server/mod.rs`: `pub mod replica;`.
    3. Add two helpers to `src/server/signals.rs`:
       ```rust
       pub fn emit_replica_dropped(registry: &SharedRegistry, reason: &str) { /* category=operational, severity=warning */ }
       pub fn emit_replica_auth_failure(registry: &SharedRegistry, detail: &str) { /* category=safety, severity=error */ }
       ```
       Mirror the shape of `emit_snapshot_failure` already in that file.
    4. Extend `src/server/http.rs` `/metrics` rendering: append four blocks for the new metric names (exact names in CONTEXT.md §Metrics). The gauge reads `registry.active_count()`; counters read from the registry's atomics; `tally_replica_snapshot_bytes_sent_total` is incremented in the 27-01 handler (go back and wire it during this task — one-line change).
    5. Thread the `Arc<SubscriberRegistry>` through the server's shared state struct (same pattern as `SharedRegistry` for signals — grep `SharedRegistry` for the plumbing convention).
    6. Tests in `#[cfg(test)]` inside `replica.rs`:
       - register/unregister round-trip; `active_count` updates.
       - `notify_all` delivers to matching scope, skips non-matching.
       - Fill a session's channel to capacity, call `notify_all` once more → expect session removed from map + `dropped_count` incremented.
       - Concurrent `notify_all` from 4 tasks while a subscriber registers/unregisters → no panic, no lost events on the survivors (property test with ~1_000 iterations).
  </action>
  <verify>
    <automated>cargo test --lib server::replica::</automated>
  </verify>
  <done>All unit tests pass; clippy clean on replica.rs; `/metrics` rendering unit test updated to expect the four new lines.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Ingest notify hook (pipeline.rs) + OP_SUBSCRIBE handler (tcp.rs) + Rust integration test</name>
  <files>src/engine/pipeline.rs, src/server/tcp.rs, src/server/protocol.rs, tests/test_replica_subscribe.rs</files>
  <behavior>
    - After a successful append inside `push_with_cascade_internal`, the pipeline calls `notify_subscribers(stream, key, seq, event)` on the registry passed in via context. Zero-cost when no subscribers registered (DashMap empty → early return).
    - `handle_subscribe(stream, scope, ctx)`: auth → validate_scope → `registry.register(scope)` → spawn a per-connection drainer task that reads `(seq, event)` from the receiver, serializes each into a 4-byte-length-prefixed frame, writes to socket; loop until channel closed or socket write error. On exit → `registry.unregister(conn_id)`.
    - Seq-monotonic ordering: verify in Phase 14's concurrency model that the ingest path serializes seq assignment such that `notify_all` is called in seq-ascending order globally (CONTEXT.md §Specific technical notes flagged this). If per-stream locks are held during seq assignment but not during notify, multiple concurrent streams could interleave — acceptable **only if** seqs observed by a subscriber are still monotonic. Implementer must verify (read `push_with_cascade_internal` around the existing seq-assignment site) and document the finding in the SUMMARY. If it is NOT monotonic, funnel notify_all calls through a single tokio task with an unbounded internal queue (simpler than redoing the seq assignment).
    - OP_SUBSCRIBE_RESERVED → OP_SUBSCRIBE transition: rename to `OP_SUBSCRIBE` in protocol.rs, update `parse_command` to return `Command::Subscribe { scope }` instead of `ReservedNotImplemented`. Leave the numeric value at 0x11.
  </behavior>
  <action>
    1. In `src/server/protocol.rs`: rename `OP_SUBSCRIBE_RESERVED` → `OP_SUBSCRIBE` (keep the old const as a deprecated alias pointing at the new name if needed to avoid churn, or delete — implementer picks). Update `parse_command` to decode a `Scope` payload and emit `Command::Subscribe { scope }`. Update the existing `test_reserved_opcodes` test to reflect that SUBSCRIBE is no longer reserved (SCAN stays reserved).
    2. In `src/engine/pipeline.rs`: thread `Option<Arc<SubscriberRegistry>>` through `push_with_cascade` (or through the context/state struct already passed in — prefer threading via context to keep the signature stable). Inside `push_with_cascade_internal`, after the successful append branch (where seq is assigned and event is written to the log), insert:
       ```rust
       if let Some(reg) = &ctx.subscribers {
           reg.notify_all(stream_name, &event.key, assigned_seq, &event);
       }
       ```
       Position: AFTER log append + seq finalization, BEFORE returning. Verify (via code inspection + a new test) that concurrent pushes to different streams observe monotonic global seqs from any single subscriber's perspective. If not, wrap `notify_all` calls in a serializer — a single `tokio::spawn`ed task reading from an internal `mpsc::UnboundedSender<(seq, event)>` and calling `notify_all` serially. Document the choice in the SUMMARY.
    3. In `src/server/tcp.rs`: add `async fn handle_subscribe(...)` — auth → validate_scope → register → spawn drainer task with `tokio::spawn`. Drainer reads from `mpsc::Receiver<(u64, Event)>`, serializes each as 4-byte-length-prefixed frame, writes to socket. On socket error or EOF: `registry.unregister(conn_id)` and `emit_replica_dropped(&signals, "disconnect")`. Replace the old OP_SUBSCRIBE_RESERVED dispatch arm.
    4. On auth failure for any replica opcode (OP_SNAPSHOT_FETCH, OP_LOG_FETCH, OP_SUBSCRIBE): call `emit_replica_auth_failure` before writing the error frame (CONTEXT.md §Signals — the error-severity signal). Revisit 27-01 and 27-02 handlers to add the same call — small follow-up touch.
    5. Create `tests/test_replica_subscribe.rs`:
       - Spin up test server, open a SUBSCRIBE connection, from another connection push 50 events to the scoped stream, assert the subscriber receives all 50 in seq-ascending order.
       - Scope filter: push events to 3 streams, subscribe to 1 → assert only that stream's events arrive.
       - Multi-subscriber independence: two SUBSCRIBE conns with different scopes, assert each receives its filtered view, no cross-talk.
       - Backpressure drop: create a subscriber that never reads from its socket, push > 10_000 events matching its scope, assert (a) subscriber removed from registry within reasonable time, (b) `tally_replica_subscribers_dropped_total{reason="backpressure"}` incremented by 1, (c) other subscribers on the same server unaffected.
       - Disconnect cleanup: drop a subscriber connection, push more events, assert registry size decremented and `reason="disconnect"` counter incremented.
       - Auth reject: bad token → error frame + `emit_replica_auth_failure` signal recorded (inspect via SignalRegistry snapshot).
       - Seq-monotonic property: 4 concurrent pushers, one subscriber → subscriber sees strictly ascending seqs (this is the ordering verification called out in CONTEXT.md §Specifics).
  </action>
  <verify>
    <automated>cargo test --test test_replica_subscribe</automated>
  </verify>
  <done>All 7 tests pass; `cargo test` full suite green; clippy clean; ordering decision documented in inline comment in pipeline.rs.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Python asyncio live-subscribe end-to-end test (incl. backpressure drop)</name>
  <files>tests/integration/test_replica_subscribe_live.py</files>
  <behavior>
    - Test opens a SUBSCRIBE connection on one asyncio task while another task pushes events via OP_PUSH.
    - Asserts subscriber receives matching events in seq-ascending order within a short timeout (~2s).
    - Scope-filter test: subscribe to stream A only; push to A, B, C; assert only A events arrive.
    - Slow-consumer / backpressure drop test: open SUBSCRIBE, never read from the socket reader, push > 10_000 matching events, poll `/metrics` until `tally_replica_subscribers_dropped_total{reason="backpressure"}` increments, assert the socket is closed by the server side.
    - Reconnect-and-catchup test: after being dropped, client opens a new connection and issues OP_LOG_FETCH{from: last_seq_seen, scope} (from 27-02) → receives exactly the events it missed. This closes the "client reconnects with OP_LOG_FETCH" loop mentioned in CONTEXT.md §Backpressure.
  </behavior>
  <action>
    Create `tests/integration/test_replica_subscribe_live.py`:
    1. Reuse server fixture + helpers from 27-02's `test_replica_clone_catchup.py` (import where possible; factor shared helpers into `tests/integration/_replica_helpers.py` if needed — small refactor is acceptable).
    2. `test_subscribe_live_push`: open SUBSCRIBE (via asyncio.open_connection), from a second task push 100 events, await receipt, assert set-equal + seq-monotonic.
    3. `test_subscribe_scope_filters_streams`: push to 3 streams, subscribe to 1, assert filtering.
    4. `test_subscribe_backpressure_drop_and_reconnect`: the full loop. Push enough events to overflow 10_000 without reading; poll `/metrics` for the `reason="backpressure"` counter increment (timeout 10s); confirm socket closed by server; reconnect and issue OP_LOG_FETCH{from: last_seq_seen} to pull the gap; assert final delivered set ∪ initial delivered set == full pushed set (no missing events in the durable log).
    5. All tests `@pytest.mark.asyncio`, timeout 20s, skip-if-slow marker if the backpressure test proves flaky in CI (but first-pass goal is making it reliable — implementer tunes the push rate + timing).
  </action>
  <verify>
    <automated>cd /data/home/tally && pytest tests/integration/test_replica_subscribe_live.py -x -v</automated>
  </verify>
  <done>All 3 pytest cases pass; backpressure-drop case is stable across 3 back-to-back runs; total runtime under 30s.</done>
</task>

</tasks>

<test_plan>
## Test Plan

**Levels:**
1. **Unit** — `SubscriberRegistry` register/unregister/notify_all + concurrent stress (task 1).
2. **Integration (Rust)** — `tests/test_replica_subscribe.rs` full TCP round-trip incl. backpressure drop + ordering + multi-subscriber (task 2).
3. **Integration (Python, end-to-end)** — `tests/integration/test_replica_subscribe_live.py` incl. the full "dropped-then-catchup-via-log-fetch" loop (task 3). **This is the user-flagged test-plan deliverable for 27-03.**

**Coverage matrix:**

| Concern | Test | Level |
|---|---|---|
| register/unregister/notify_all basic | `server::replica::basic_*` | Unit |
| Backpressure drop at 10_001 | `server::replica::backpressure_drops_subscriber` | Unit |
| Concurrent register/notify no panic | `server::replica::concurrent_stress` | Unit (property) |
| SUBSCRIBE happy path, 50 events | `test_replica_subscribe::happy_path` | Integ (Rust) |
| Scope filter at handler level | `test_replica_subscribe::scope_filters` | Integ (Rust) |
| Multi-subscriber independence | `test_replica_subscribe::multi_sub_independent` | Integ (Rust) |
| Backpressure drop + metric + signal | `test_replica_subscribe::backpressure_drop` | Integ (Rust) |
| Disconnect cleanup + metric + signal | `test_replica_subscribe::disconnect_cleanup` | Integ (Rust) |
| Auth reject + safety signal | `test_replica_subscribe::auth_reject_emits_signal` | Integ (Rust) |
| Seq-monotonic under concurrent pushers | `test_replica_subscribe::seq_monotonic_property` | Integ (Rust) |
| **Live push end-to-end, scope filter, Python** | `test_replica_subscribe_live::test_subscribe_live_push`, `_scope_filters_streams` | **E2E (Python)** |
| **Backpressure → reconnect via LOG_FETCH → no lost events** | `test_replica_subscribe_live::test_subscribe_backpressure_drop_and_reconnect` | **E2E (Python)** |

**Load / bench considerations:**
- Backpressure test pushes ~12_000 events at line rate to a non-reading socket; should complete under 5s on dev hardware. If CI is underpowered, parameterize the threshold down (registry constant visible to tests).
- Ordering property test uses 4 concurrent pushers for ~2_000 events each — small enough to fit in test suite budget, large enough to catch interleaving bugs.

**Known gaps documented (not tested, per CONTEXT.md §No rate-limiting and §Deferred):**
- No rate-limiting test — a reconnect-loop client can DoS the server. Fine for v0 (trusted clients).
- No byte-based backpressure cap — revisit if prod shows pressure.

**Out of scope:** client SDK, dependency analyzer, Python Pipeline API — all Phase 28+.
</test_plan>

<verification>
- `cargo test` full suite passes incl. `test_replica_subscribe`.
- `pytest tests/integration/test_replica_subscribe_live.py -v` all 3 cases pass reliably (3x back-to-back).
- `cargo clippy --all-targets --all-features -- -D warnings` clean.
- `curl http://localhost:PORT/metrics | grep tally_replica_` shows all four new metric lines.
- Manual: spin up dev server, run two `ncat` sessions — one SUBSCRIBE, one PUSH — observe live delivery; then deliberately stall the subscriber and watch `tally_replica_subscribers_dropped_total` tick.
- Seq-ordering decision (monotonic-as-is vs serializer-funnel) documented in an inline comment at the `notify_subscribers` call site in pipeline.rs and in 27-03-SUMMARY.md.
</verification>

<success_criteria>
- OP_SUBSCRIBE (0x11) is live — reserved stub replaced.
- `src/server/replica.rs` exists with `SubscriberRegistry` + `ReplicaSession`; wired through server state.
- Ingest-path `notify_subscribers` hook calls into the registry after every successful append.
- Backpressure drop policy enforced at exactly 10_000 queue depth with signal + metric emission.
- Four `tally_replica_*` metrics present in `/metrics`; two new signal helpers in `signals.rs`.
- Auth failure on any replica opcode (0x11, 0x12, 0x13) emits `emit_replica_auth_failure`.
- Python end-to-end test demonstrates the full backpressure → reconnect → log-fetch-catchup loop with no lost events.
- Phase 27 complete: `tally clone` server-side primitives ready for Phase 28's client.
</success_criteria>

<output>
After completion, create `.planning/phases/27-server-replica-endpoints/27-03-SUMMARY.md` and
`.planning/phases/27-server-replica-endpoints/27-SUMMARY.md` (phase-level) summarizing:
the three opcodes + their terminal frame tags (0xFF, 0xFE, none), the four metrics and
two signal categories, the seq-ordering decision (monotonic-as-is or serializer-funnel
with rationale), the known gap on rate-limiting, and the Phase 28 handoff surface.
</output>
