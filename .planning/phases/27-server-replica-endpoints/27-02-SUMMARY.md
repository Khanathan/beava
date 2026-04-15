---
phase: 27-server-replica-endpoints
plan: 02
subsystem: server
tags: [replica, subscribe, tcp, wire-protocol, backpressure, metrics]
requires: [27-01 Scope codec + entity_matches_scope, Phase 14 DashMap pattern, Phase 25 SignalRegistry]
provides:
  - OP_SUBSCRIBE (0x11) — promoted from reserved stub
  - REPLICA_FRAME_TAG_EVENT (0x03)
  - Command::Subscribe { admin_token, scope }
  - protocol::encode_event_frame(SystemTime, &[u8]) -> Vec<u8>
  - server::replica::SubscriberRegistry (DashMap<conn_id, ReplicaSession>)
  - server::replica::{ReplicaEvent, ReplicaSession, SharedSubscriberRegistry}
  - server::replica::SUBSCRIBER_CHANNEL_CAPACITY = 10_000
  - server::replica::{events_pushed_snapshot, subscribers_dropped_snapshot}
  - ConcurrentAppState.subscriber_registry : Arc<SubscriberRegistry>
  - PipelineEngine.subscriber_registry + install_subscribers() method
  - signals::emit_replica_drop_backpressure / emit_replica_auth_failure
  - tally_replica_subscriptions_active (gauge)
  - tally_replica_events_pushed_total{stream} (counter)
  - tally_replica_subscribers_dropped_total{reason} (counter)
affects:
  - src/server/tcp.rs (new handle_subscribe + dispatch-loop SUBSCRIBE arm takes connection ownership)
  - src/engine/pipeline.rs::push_internal (single ingest-path hook site)
  - /metrics HTTP endpoint (four replica metrics now emitted)
tech-stack:
  added: []
  patterns:
    - "ingest-hook inside push_internal (single site covers primary + cascade)"
    - "tokio::select biased over mpsc::recv vs reader.read_u8 for drain+EOF detection"
    - "TrySendError::Full → drop_subscriber(reason=backpressure) + warning signal"
    - "TrySendError::Closed → drop_subscriber(reason=disconnect)"
    - "per-stream DashMap<String,AtomicU64> counter behind OnceLock (DashMap::new not const fn)"
key-files:
  created:
    - tests/test_replica_subscribe.rs
    - tests/integration/test_replica_subscribe_asyncio.py
  modified:
    - src/server/protocol.rs
    - src/server/replica.rs
    - src/server/signals.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/engine/pipeline.rs
    - tests/test_reserved_opcodes.rs
decisions:
  - "Single ingest hook site inside PipelineEngine::push_internal covers both primary and cascade pushes — every cascade downstream goes through push_internal too (C-5 enrichment stays stack-local, hook reads the already-extracted key)."
  - "Hook placement moved ABOVE the read_features fast-path early-return so OP_PUSH_ASYNC events also wake live subscribers. The try_send is non-blocking and preserves async hot-path characteristics."
  - "SubscriberRegistry lives on ConcurrentAppState (new field) AND is Arc-shared into PipelineEngine via install_subscribers. Existing PipelineEngine::new() stays source-compatible (field defaults to None). Non-TCP test harnesses see the hook as a zero-cost no-op."
  - "handle_subscribe runs the drain loop INLINE rather than spawning a separate tokio task. The outer handle_connection already runs per-connection on its own task, so task isolation is inherited. Keeping BufReader + BufWriter together avoids unsafe ownership juggling."
  - "Per-event frame tag = 0x03 (REPLICA_FRAME_TAG_EVENT) — distinct from STATUS_ERROR=0x01 and REPLICA_FRAME_TAG_PAYLOAD=0x02 so subscribe clients disambiguate by tag, not body length (user direction §7). 27-01's tag collision (0x01 HEADER vs 0x01 STATUS_ERROR) was LEFT as-is because fixing it would have rippled into existing 27-01 tests for no protocol benefit."
  - "Backpressure test uses TcpSocket::set_recv_buffer_size(4096) on the client. This stalls the server's BufWriter much faster than 10k small frames otherwise would — without it the kernel buffers ~80k frames and the test would need multi-megabyte payloads to force a drop."
metrics:
  duration: "~100 minutes"
  completed: "2026-04-14"
  tasks: 3
  tests_added: 12
---

# Phase 27 Plan 02: OP_SUBSCRIBE + SubscriberRegistry Summary

One-liner: Promoted OP_SUBSCRIBE (0x11) from a reserved stub to a live-subscribe TCP endpoint, with a lock-free SubscriberRegistry on the hot ingest path, 10 000-slot bounded-channel backpressure drop, admin auth, four Prometheus metrics, and both Rust + Python integration coverage — the "live" half of the Phase 31 buffered-replay dance.

## What Shipped

### Wire protocol additions (src/server/protocol.rs)

| Symbol | Purpose |
|---|---|
| `OP_SUBSCRIBE: u8 = 0x11` | Live-subscribe opcode (replaces reserved stub) |
| `REPLICA_FRAME_TAG_EVENT: u8 = 0x03` | Per-event frame tag on a subscribe socket |
| `Command::Subscribe { admin_token: String, scope: Scope }` | Parsed command variant |
| `encode_event_frame(SystemTime, &[u8]) -> Vec<u8>` | Per-event frame writer helper |
| `OP_SUBSCRIBE_RESERVED` | Deprecated alias (= `OP_SUBSCRIBE`) for any lingering clients |

Request-payload wire shape (mirrors `OP_SNAPSHOT_FETCH` exactly):

```
[u32 BE frame_len][u8 opcode=0x11]
  [u16 BE token_len][token_bytes]
  [Scope bytes per read_scope]
```

Per-event frame (no global seq — per-connection accept order only):

```
[u32 BE frame_len]
[u8 tag=0x03]
[u64 BE ts_secs][u32 BE ts_nanos]
[u32 BE payload_len][payload_bytes]
```

where `payload_bytes` is the original event JSON the client pushed. Phase 31's streaming client reads this shape directly.

Error response (auth / validate_scope failure): a single standard `STATUS_ERROR` frame, then the socket closes. No registry entry is created.

### SubscriberRegistry (src/server/replica.rs)

Public API used by `handle_subscribe` and the ingest hook:

```rust
pub struct SubscriberRegistry {
    sessions: DashMap<u64, ReplicaSession>,
    next_id: AtomicU64,
    signals: SharedRegistry,
}

impl SubscriberRegistry {
    pub fn new(signals: SharedRegistry) -> Self;
    pub fn register(&self, scope: Scope, sender: mpsc::Sender<ReplicaEvent>) -> u64;
    pub fn active_count(&self) -> usize;            // backs the gauge
    pub fn drop_subscriber(&self, conn_id: u64, reason: &'static str);
    pub fn notify_subscribers(
        &self, stream: &str, key: &str, payload: &[u8], now: SystemTime,
    );
}

pub struct ReplicaEvent {
    pub timestamp: SystemTime,
    pub stream: String,
    pub key: String,
    pub payload: Vec<u8>,    // serialized event JSON
}

pub struct ReplicaSession {
    pub scope: Scope,
    pub sender: mpsc::Sender<ReplicaEvent>,
    pub last_err: Option<String>,
}

pub const SUBSCRIBER_CHANNEL_CAPACITY: usize = 10_000;
```

`notify_subscribers` is called once per successful push from inside `PipelineEngine::push_internal`, so every downstream cascade stream also fires the hook automatically. The predicate reuses `entity_matches_scope` exported by 27-01 verbatim. `TrySendError::Full` → `drop_subscriber(conn_id, "backpressure")` + `emit_replica_drop_backpressure` signal. `TrySendError::Closed` → `drop_subscriber(conn_id, "disconnect")`.

### Pipeline hook (src/engine/pipeline.rs)

One Option-gated call site inside `push_internal`, placed AFTER operator state mutations and BEFORE the `read_features` fast-path early-return so OP_PUSH_ASYNC events also wake subscribers:

```rust
if let Some(reg) = &self.subscriber_registry {
    if let Ok(payload_bytes) = serde_json::to_vec(event) {
        reg.notify_subscribers(stream_name, &key, &payload_bytes, now);
    }
}
```

`PipelineEngine` grew a new field `subscriber_registry: Option<Arc<SubscriberRegistry>>` (default `None`) and a mutator `install_subscribers(&mut self, registry)`. `PipelineEngine::new()` is source-compatible; all existing test harnesses keep working with `None` — the hook becomes a zero-cost no-op.

### TCP handler (src/server/tcp.rs)

- `handle_subscribe(reader, writer, admin_token, scope, state)` runs inline inside the per-connection task and takes ownership of the socket for the subscription lifetime. Returns from `handle_connection` on exit — SUBSCRIBE cannot mix with any other opcode on the same connection.
- Flow: admin-token gate (fails → `emit_replica_auth_failure` safety/error signal + STATUS_ERROR frame + close) → validate_scope → `mpsc::channel::<ReplicaEvent>(10_000)` → `registry.register(scope, tx)` → drain loop.
- Drain loop: `tokio::select! biased { rx.recv() → encode + write_all + flush; reader.read_u8() → EOF / protocol violation → break }`. On exit, `drop_subscriber(conn_id, "disconnect")`.

### AppState wiring (src/server/tcp.rs, make_concurrent_state_full)

`ConcurrentAppState` gained `subscriber_registry: Arc<SubscriberRegistry>`. Construction does `engine.install_subscribers(Arc::clone(&subscriber_registry))` before wrapping the engine in RwLock so the ingest hook reads the same Arc the TCP dispatcher inserts sessions into.

### /metrics (src/server/http.rs)

Four replica metrics, all added in ONE edit (user direction):

```
tally_replica_snapshot_bytes_sent_total               (counter)
tally_replica_subscriptions_active                    (gauge)
tally_replica_events_pushed_total{stream="..."}       (counter)
tally_replica_subscribers_dropped_total{reason="..."} (counter)
```

Reason labels: `"backpressure"` and `"disconnect"`. The per-stream label cardinality is bounded by the registered stream set (user direction §4: acceptable).

### Signal emitters (src/server/signals.rs)

- `emit_replica_drop_backpressure(&SharedRegistry, conn_id: u64)` — category=Operational, severity=Warning.
- `emit_replica_auth_failure(&SharedRegistry, peer: &str)` — category=Safety, severity=Error.

Both follow the existing `emit_*` helper shapes in that file (dedupe by id, evidence JSON, etc.).

## Tests

**12 new tests, all green.**

| Layer | File | Count |
|---|---|---|
| Unit (SubscriberRegistry) | `src/server/replica.rs` | 6 |
| Unit (parse_command for OP_SUBSCRIBE) | `src/server/protocol.rs` | 1 (replaced the `op_subscribe_reserved_parses_as_marker_variant` test) |
| Integration (Rust, real TCP server) | `tests/test_replica_subscribe.rs` | 5 |
| Integration (Python asyncio cross-language) | `tests/integration/test_replica_subscribe_asyncio.py` | 1 |

Rust integration coverage (all spawn a fresh ephemeral-port server per test):

- `subscribe_then_push_delivers_events` — scope=[orders], push 3 events via sync OP_PUSH, assert event frames arrive with monotonic-per-connection timestamps and payloads match in order.
- `backpressure_drops_subscriber` — paused reader with `set_recv_buffer_size(4096)`, push 60 000 events, assert active_count goes to 0 and backpressure counter bumps.
- `disconnect_cleans_up_registry` — subscribe, drop the socket, poll until registry entry removed and disconnect counter bumps.
- `subscribe_rejects_missing_auth` — wrong token → STATUS_ERROR + safety/error signal emitted + no registry entry.
- `subscribe_rejects_empty_streams_scope` — smoke test for one validate_scope rule; full matrix lives in 27-01.

Python asyncio coverage:

- `test_multi_subscriber_scope_isolation_and_ordering` — two raw-socket asyncio subscribers with disjoint scopes (`[orders]` and `[clicks]`), interleaved pushes `[orders(k1), clicks(k1), orders(k2), clicks(k2), orders(k3)]`, `asyncio.gather`-drained sockets, asserts each subscriber sees only its scope-matching events in its accept order. Does NOT assert a cross-subscriber total order.

Full suite status:

```
$ cargo test   → all binaries pass (includes 754 lib tests + ≥ 5 new subscribe integ tests)
$ pytest tests/integration/   → 14 passed in 3.80s
```

## Deviations from Plan

1. **[Rule 3 - Blocking] Hook placement moved earlier in push_internal.** Plan action §Task 2 said to hook at the tail after `Ok(features)`. That location is AFTER the `if !read_features { return ... }` early return, so OP_PUSH_ASYNC would have silently skipped subscriber notification. Moved the hook above the read_features check (post-state-mutation, pre-feature-read) so async pushes also wake live subscribers. Per-entity key extraction already happened by that line, so the hook still reads the correct `key` binding.
2. **[Rule 3 - Blocking] `handle_subscribe` runs inline, not on a separate spawned task.** Plan action §Task 3 suggested `tokio::spawn` of a drain task that owns the TcpStream. Inlining the drain loop with `tokio::select!` inside the existing per-connection tokio task keeps BufReader + BufWriter together without unsafe ownership moves across tasks, and the outer handle_connection already gives us per-connection isolation. Functional behavior is identical; simpler code.
3. **[Rule 2 - Correctness] Added a `SUBSCRIBER_CHANNEL_CAPACITY` constant instead of an inline `10_000` at the handler call site.** Makes the backpressure threshold visible in one place and lets future hardening tests refer to the same symbol.
4. **[Rule 3 - Blocking] Per-stream events-pushed counter uses `OnceLock<DashMap>` instead of a `Lazy` static.** The `dashmap` crate pinned in Cargo.toml has `DashMap::new` not `const fn`, so we can't initialize a plain `static DashMap`. `once_cell` is not a direct dependency; `std::sync::OnceLock` on edition 2021 works. Documented inline.
5. **Reserved-opcode test deleted per plan instruction §5.** `tests/test_reserved_opcodes.rs::subscribe_reserved_returns_error_and_keeps_connection_alive` removed and replaced with a comment pointing at `tests/test_replica_subscribe.rs`. The SCAN (0x10) assertion stays intact.
6. **Response-tag collision from 27-01 NOT fixed.** Per user direction §7: "if it would balloon scope, leave it and just pick a non-conflicting tag for events." Chose `REPLICA_FRAME_TAG_EVENT = 0x03` which is unambiguous vs both 27-01 tags and STATUS_ERROR. 27-01's HEADER (0x01) vs STATUS_ERROR (0x01) collision remains as documented in 27-01-SUMMARY — clients disambiguate by body length there.
7. **Backpressure integration test uses `set_recv_buffer_size(4096)` on the client socket.** Without this, 10 000 small frames fit entirely in the kernel's default TCP buffer and the server's BufWriter never stalls, so the mpsc never fills. Shrinking the client recv buffer is the standard pattern for exercising server-side backpressure in an integration test without ballooning payload sizes.

## Authentication Gates

None. Admin-token handling is entirely in-protocol (bearer in the SUBSCRIBE frame payload), mirroring 27-01. No out-of-band user action required.

## Key Links (for Phase 31 and downstream)

| From | To | Via |
|---|---|---|
| `src/engine/pipeline.rs::push_internal` | `src/server/replica.rs::SubscriberRegistry::notify_subscribers` | non-blocking try_send hook (single site, primary + cascade) |
| `src/server/tcp.rs::handle_subscribe` | `state.admin_token.as_deref() == Some(presented)` | bearer token gate before registry insertion |
| `src/server/tcp.rs::handle_subscribe` | `protocol::validate_scope` | scope validated before registry insertion |
| `src/server/replica.rs::notify_subscribers` | `entity_matches_scope` (27-01) | per-event scope predicate — identical to snapshot filter |
| `src/server/replica.rs::drop_subscriber` | `signals::emit_replica_drop_backpressure` | operational/warning signal on Full try_send |

**Phase 31 integration contract:**
- Subscribe frame shape: `[u32 frame_len][u8 opcode=0x11][u16 token_len][token][Scope]`.
- Per-event frame: `[u32 frame_len][u8 tag=0x03][u64 ts_secs][u32 ts_nanos][u32 payload_len][payload_bytes]`.
- No global seq; per-connection accept order only.
- Error response = single STATUS_ERROR frame + socket close.

## Self-Check: PASSED

```
$ ls tests/test_replica_subscribe.rs tests/integration/test_replica_subscribe_asyncio.py
FOUND both files
$ git log --oneline | head -1
0f33c9b feat(27-02): OP_SUBSCRIBE (0x11) + SubscriberRegistry + live-event hook
$ cargo test --lib server::replica::
15 passed, 0 failed
$ cargo test --test test_replica_subscribe
5 passed, 0 failed
$ pytest tests/integration/
14 passed
```

- [x] All created files exist on disk.
- [x] Commit `0f33c9b` exists in git log.
- [x] Subscribe integration tests green (5/5).
- [x] Replica unit tests green (15/15 — 6 new).
- [x] Full cargo test suite green (754 lib + all integration binaries pass).
- [x] pytest integration suite green (14/14, includes new asyncio test).
- [x] `/metrics` endpoint exposes all four replica metrics.
- [x] No OP_LOG_FETCH introduced. No global seq counter. No seq field in event frame.

## Threat Flags

None. New surface is admin-gated (TCP bearer token, identical to 27-01). No new network binding, no new file path, no new trust boundary. The 10 000-slot bounded channel + DashMap active-subscriber count are explicit DoS mitigations.
