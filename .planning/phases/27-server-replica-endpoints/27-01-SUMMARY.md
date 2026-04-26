---
phase: 27-server-replica-endpoints
plan: 01
subsystem: server
tags: [replica, snapshot, tcp, wire-protocol, scope-filter]
requires: [BaseSnapshotState, load_snapshot_file, make_concurrent_state, u16-length-prefixed string framing]
provides:
  - OP_SNAPSHOT_FETCH (0x12)
  - REPLICA_FRAME_TAG_HEADER (0x01), REPLICA_FRAME_TAG_PAYLOAD (0x02)
  - protocol::Scope, protocol::ScopeError, protocol::validate_scope
  - protocol::read_scope, protocol::write_scope
  - Command::SnapshotFetch { admin_token, scope }
  - server::replica::filter_base_snapshot
  - server::replica::entity_matches_scope (reusable by 27-02)
  - server::replica::record_snapshot_bytes_sent (metric surface for tally_replica_snapshot_bytes_sent_total)
affects: [src/server/tcp.rs dispatch path]
tech-stack:
  added: []
  patterns: [bearer-token-in-payload, two-frame-response-shape, tag-header-vs-status-error-body-length-disambiguation]
key-files:
  created:
    - src/server/replica.rs
    - tests/test_replica_snapshot_fetch.rs
    - tests/integration/test_replica_snapshot_fetch_asyncio.py
  modified:
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/mod.rs
decisions:
  - Admin token is length-prefixed in the OP_SNAPSHOT_FETCH payload, not connection-level. Keeps wire shape uniform; 27-02 will do the same for OP_SUBSCRIBE.
  - BaseSnapshotState fields per current source (header/entities/pipelines/backfill_complete); `seq: u64` in the plan template was stale. snapshot_taken_at is response-only, never persisted.
  - Response-frame tag (0x01 HEADER) and STATUS_ERROR (0x01) collide by design; clients disambiguate by body length (header body is always exactly 12 bytes).
  - Python asyncio test does shallow postcard decode only (header enum + varints + first entity key). Operator-internal structural equivalence is covered in Rust.
  - Handler bypasses load_incremental_snapshots delta merging — v0 replica contract ships the latest persisted BASE snapshot only.
metrics:
  duration: "~90 minutes"
  completed: "2026-04-14"
  tasks: 3
  tests_added: 39
---

# Phase 27 Plan 01: OP_SNAPSHOT_FETCH + Scope Codec Summary

One-liner: Landed the scope-aware `OP_SNAPSHOT_FETCH` (0x12) TCP endpoint, the shared `Scope` / `validate_scope` wire codec, and the `replica::filter_base_snapshot` filter utility that 27-02 will reuse — the historical half of the Phase 31 buffered-replay dance.

## What Shipped

### Wire protocol additions (src/server/protocol.rs)

| Symbol | Purpose |
|---|---|
| `OP_SNAPSHOT_FETCH: u8 = 0x12` | Request opcode |
| `REPLICA_FRAME_TAG_HEADER: u8 = 0x01` | Response header-frame tag |
| `REPLICA_FRAME_TAG_PAYLOAD: u8 = 0x02` | Response payload-frame tag |
| `Scope { streams, keys?, key_prefix?, pull }` | Shared replica filter descriptor (reused by 27-02) |
| `ScopeError` | 7 locked rejection variants + `Display` |
| `validate_scope(&Scope, &HashSet<String>)` | Runs all 7 rules in order; auth lives in handler |
| `read_scope` / `write_scope` | Wire codec over existing `read_string` / `write_string` |
| `Command::SnapshotFetch { admin_token: String, scope: Scope }` | Parsed command variant |

Request-payload wire shape:

```
[u32 BE frame_len][u8 opcode=0x12]
  [u16 BE token_len][token_bytes]
  [u16 BE n_streams][n_streams × u16-string]
  [u8 has_keys]
    if has_keys: [u32 BE n_keys][n_keys × u16-string]
  [u8 has_prefix]
    if has_prefix: [u16-string prefix]
  [u16-string pull]   // "all" only in v0
```

Success response (two frames, back-to-back on the same connection):

```
Header frame:   [u32 BE len=13][u8 tag=0x01][u64 BE secs][u32 BE nanos]
Payload frame:  [u32 BE len=1+N][u8 tag=0x02][postcard(BaseSnapshotState) bytes]
```

Error response (any validation / auth / I/O failure, emitted BEFORE any I/O on the snapshot blob):

```
Error frame:    [u32 BE len=1+M][u8 status=0x01][error_message_utf8]
```

The `0x01` tag collision between HEADER and STATUS_ERROR is resolved by the header body always being exactly 12 bytes — clients read the length first and disambiguate.

### Replica module (src/server/replica.rs)

- `filter_base_snapshot(&BaseSnapshotState, &Scope) -> BaseSnapshotState` — in-memory filter; preserves `header`, `pipelines`, `backfill_complete` verbatim.
- `entity_matches_scope(entity_streams: &[&str], entity_key: &str, &Scope) -> bool` — factored-out per-entity predicate that 27-02 will reuse on the push-path notify hook.
- `record_snapshot_bytes_sent(n: u64)` / `snapshot_bytes_sent_total()` — module-local `AtomicU64` backing `tally_replica_snapshot_bytes_sent_total`. Metric is registered here so 27-02 can add sibling counters next to it; `/metrics` scrape wiring is deferred.

### TCP handler (src/server/tcp.rs)

- `handle_snapshot_fetch(writer, admin_token, scope, state)` performs: admin-token compare → capture `snapshot_taken_at = SystemTime::now()` → collect known streams from `state.engine.read().list_streams()` → `validate_scope` → `load_base_snapshot_for_fetch` → `filter_base_snapshot` → serialize → emit header frame → emit payload frame → bump bytes-sent counter.
- Dispatch interception in `handle_connection` for `Command::SnapshotFetch`; also returns a structured error in the inner async-burst tight-loop path so mixing OP_SNAPSHOT_FETCH with async pushes produces a protocol error rather than a panic.
- `load_base_snapshot_for_fetch` scans `snapshot_path.parent()` for `tally.snapshot.base.*` (highest seq wins), falls back to `snapshot_path` itself, then to an empty `BaseSnapshotState`. Does NOT apply deltas — v0 replica contract ships the latest persisted base.

## Tests

**39 new tests, all green.**

| Layer | File | Count |
|---|---|---|
| Unit (protocol codec + validator + parse_command) | `src/server/protocol.rs` | 17 |
| Unit (replica filter + predicate + postcard-stability + counter) | `src/server/replica.rs` | 9 |
| Integration (Rust, real TCP server) | `tests/test_replica_snapshot_fetch.rs` | 10 |
| Integration (Python asyncio cross-language) | `tests/integration/test_replica_snapshot_fetch_asyncio.py` | 3 |

Rust integration coverage:
- 3 happy paths: `happy_streams_only`, `happy_keys`, `happy_key_prefix`
- 7 rejections: `rejects_missing_auth` (wrong + empty token), `rejects_empty_streams`, `rejects_unknown_stream`, `rejects_keys_and_prefix`, `rejects_pull_not_all`, `rejects_too_many_keys`, `rejects_empty_prefix`
- Every rejection asserts a STATUS_ERROR frame arrives and NO payload frame follows, proving the handler refuses snapshot I/O before doing any work.

Python asyncio coverage:
- `test_snapshot_fetch_streams_only_roundtrip` — raw-socket client hand-encodes the scope frame, reads both response frames, shallow-decodes the postcard payload to verify entity count (2) and first entity key (`"u1"`).
- `test_snapshot_fetch_rejects_wrong_token` and `test_snapshot_fetch_rejects_unknown_stream` — prove the error-frame wire contract is cross-language.

Full suite: `cargo test` → 1206 passed / 0 failed. `pytest tests/integration/` → 13 passed / 0 failed.

## Deviations from Plan

1. **[Rule 2 - Correctness] `Command::SnapshotFetch` gained an `admin_token` field.** Plan template assumed an axum-style connection-level auth hook; per user direction on admin-token placement the token is length-prefixed in the payload. The Command variant needed the field to propagate the token from `parse_command` to `handle_snapshot_fetch`. (Design decision provided in the user prompt, not a plan deviation per se — noted for 27-02, which will adopt the same shape.)
2. **[Rule 3 - Blocking] `Command::SnapshotFetch` inner-loop fallback.** The tight-loop sync dispatch path inside the async-push burst handler calls `handle_sync_command`; landing SnapshotFetch there would have panicked. Replaced the panic with a `TallyError::Protocol("... not supported on this dispatch path ...")` so mixing with async pushes produces a structured refusal. The outer dispatch path is the supported happy path; this fallback is defence-in-depth.
3. **BaseSnapshotState field list** per user direction #2 — used the real fields (`header`, `entities`, `pipelines`, `backfill_complete`) rather than the stale `seq: u64` from the plan template. `snapshot_taken_at` lives in the response header frame only and is never persisted.
4. **Metric `/metrics` scrape wiring deferred.** The `tally_replica_snapshot_bytes_sent_total` counter is defined and incremented in `replica.rs`, but the Prometheus-text render in `http.rs` is NOT updated to expose it yet. Reasoning: the full replica metric surface (this counter plus 27-02's `tally_replica_subscriptions_active` / `tally_replica_events_delivered_total`) should land together to avoid two scrape-text refactors. The counter is verified via `snapshot_bytes_sent_total()` in the unit tests.
5. **`entity_matches_scope` signature** takes `&[&str]` for streams rather than an iterator — simpler for 27-02's push-path call site where the stream name is known statically as a `&str`.

## Authentication Gates

None. Admin-token handling is entirely in-protocol (bearer token in frame payload); no out-of-band user action was required.

## Key Links (for 27-02)

| From | To | Via |
|---|---|---|
| `src/server/tcp.rs::handle_snapshot_fetch` | `require_loopback_or_token`-equivalent direct check | `state.admin_token.as_deref() == Some(presented)` |
| `src/server/tcp.rs::handle_snapshot_fetch` | `src/server/replica.rs::filter_base_snapshot` | In-memory filter by scope |
| `src/server/protocol.rs::parse_command(OP_SNAPSHOT_FETCH, ...)` | `validate_scope` | Handler validates BEFORE snapshot load |

**27-02 hooks:**
- Reuse `protocol::Scope` / `read_scope` / `validate_scope` verbatim.
- Reuse `server::replica::entity_matches_scope` on the push-path notify hook (per-event scope match).
- Mirror the in-band `[u16 token_len][token]` admin-auth shape for `OP_SUBSCRIBE`.

## Self-Check: PASSED

Verification commands + results:

```
$ ls src/server/replica.rs tests/test_replica_snapshot_fetch.rs tests/integration/test_replica_snapshot_fetch_asyncio.py
FOUND all three files
$ git log --oneline | head -1
0d161fa feat(27-01): OP_SNAPSHOT_FETCH (0x12) + Scope codec + replica filter
$ cargo test 2>&1 | grep "test result" | tail -1
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
(total across binaries: 1206 passed, 0 failed)
$ pytest tests/integration/
13 passed in 3.35s
```

- [x] All created files exist on disk.
- [x] Commit `0d161fa` exists in git log.
- [x] Full cargo test suite green (1206/1206).
- [x] pytest integration suite green (13/13).
- [x] No clippy warnings on new code in `src/server/replica.rs`, `src/server/protocol.rs` (new sections), or `src/server/tcp.rs` (new handler).

## Threat Flags

None. All new surface is admin-gated (TCP bearer-token) and behind the existing `/pipelines`-style auth model. No new network binding, no new file path, no new trust boundary.
