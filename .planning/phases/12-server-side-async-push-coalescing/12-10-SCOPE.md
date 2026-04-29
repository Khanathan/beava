---
phase: 12-server-side-async-push-coalescing
plan: 12-10
type: scope-not-yet-planned
captured: 2026-04-29
status: ready-for-planning
depends_on: 12-07, 12-09
supersedes: 12.5-01-PLAN, 12.5-02-PLAN, 12.5-03-PLAN
---

# Plan 12-10 (proposed) — push-and-get over HTTP+TCP via mio

## Goal

Atomic combined push + feature-vector read on the production binary `target/release/beava`. Single round-trip; read-after-write by construction (push and query share one apply-thread borrow). Latency target: P50 < 300 µs end-to-end on Apple-M4 LAN loopback (per Phase 12.5 SC6).

This plan **supersedes** Phase 12.5's three planned-but-not-executed plans (`12.5-01-PLAN.md`, `12.5-02-PLAN.md`, `12.5-03-PLAN.md`). Those were written for the legacy axum data plane and Phase 13.3 lockless apply (REJECTED). Plan 12-10 implements the same semantics on the post-Plan-12-07 mio data plane.

## Phase 12.5 locked decisions (still apply)

From `.planning/phases/12.5-push-and-get/12.5-CONTEXT.md`, ALL of D-01 through D-08 carry over EXCEPT D-04 which adapts:

- **D-01: Atomic under one borrow** — push apply + feature query under one `state_tables.lock()` (mio Mutex, not the rejected RefCell)
- **D-02: Two endpoints** — `/push-and-get` (acks=1, default), `/push-sync-and-get` (acks=all)
- **D-03: TCP opcodes** — `OP_PUSH_AND_GET = 0x0015`, `OP_PUSH_SYNC_AND_GET = 0x0016` (already reserved in `wire.rs`)
- **D-05: Request body** — `{"row": {...}, "query": {"entity_key": {...}, "features": [...]}}`
- **D-06: Response body** — `{"ack_lsn": N, "registry_version": N, "features": {...}, "warnings": []}`
- **D-07: Python SDK** — `app.push_and_get(...)` returning `(ack, features_dict)`
- **D-08 supersedes per user 2026-04-29:** push-and-get's get half is a regular feature query against current AggStateTable state. No special PIT semantics; joins resolve via Phase 11.5 temporal store as today's `/get` does. When Phase 15 (event-time PIT) lands, the get half automatically becomes event-time correct without changes here.

## Updated decisions for mio runtime

**D-04 (revised): Single-Mutex inline dispatch via `dispatch_push_and_get_sync`.**

The original Phase 12.5 D-04 said "reuse `execute_push` + feature-query service fn" but those were async axum-side functions. The mio runtime needs sync dispatch on the apply thread. New helper:

```rust
// In runtime_core_glue.rs
pub fn dispatch_push_and_get_sync(
    app: &Arc<AppState>,
    event_name: &str,
    body: &Bytes,
    body_format: u8,
    sync_mode: SyncMode,
) -> GlueResponse {
    // 1. Parse body once: {"row": ..., "query": {...}}
    let req = parse_push_and_get_body(body, body_format)?;

    // 2. Acquire state_tables lock ONCE for the whole operation
    let tables = app.dev_agg.state_tables.lock();

    // 3. Apply the push (mirror dispatch_push_sync inline, but reuse the locked tables)
    let ack = apply_push_inline(app, &tables, event_name, &req.row, sync_mode)?;

    // 4. Run the feature query (mirror dispatch_get_batch inline, reuse locked tables)
    let (features, warnings) = query_features_inline(app, &tables, &req.query)?;

    // 5. Drop lock; build response
    drop(tables);
    GlueResponse::PushAndGetResult {
        ack_lsn: ack.ack_lsn,
        registry_version: ack.registry_version,
        body: encode_response(features, warnings, body_format),
    }
}
```

Key constraint: step 4's feature query MUST see the just-pushed event's effects. Since both are under the same lock, this is automatic. Step 3's `apply_push_inline` MUST commit to AggStateTable BEFORE returning (no deferred apply). Step 4's `query_features_inline` reads the now-committed state.

For `/push-sync-and-get` (D-02 acks=all path), the WAL fsync wait happens AFTER step 4's query but BEFORE returning the response. That preserves read-after-write while satisfying the per-event-fsync semantics. The lock is RELEASED before fsync (other apply work proceeds).

**D-F: Wire format follows Plan 12-09 conventions.**

- HTTP: JSON request + JSON response (D-05 / D-06 shape)
- TCP: msgpack request + msgpack response when content_type == CT_MSGPACK; JSON+JSON when CT_JSON

The body shape is the same Map; just encoded differently.

**D-G: TCP response opcode allocation.**

- `OP_PUSH_AND_GET_RESPONSE = 0x0024` (next available; OP_GET_RESPONSE=0x0023 is the previous taken slot from Plan 12-07)

Both `/push-and-get` AND `/push-sync-and-get` responses use `OP_PUSH_AND_GET_RESPONSE`. The difference is timing (when the response is emitted relative to fsync), not the opcode.

## Plan structure (waves)

- **Wave 1**: Wire constants + opcodes
  - `OP_PUSH_AND_GET = 0x0015`, `OP_PUSH_SYNC_AND_GET = 0x0016` in `wire.rs` (already reserved; just promote from `Phase 12.5` reserved status to "Implemented")
  - `OP_PUSH_AND_GET_RESPONSE = 0x0024` (new)
  - `opcode_name`, uniqueness test, etc.

- **Wave 2**: Router + HTTP parser
  - `Route::PushAndGet { event_name }`, `Route::PushSyncAndGet { event_name }` in `router.rs`
  - HTTP route arms in `http_listener.rs`
  - WireRequest variants `HttpPushAndGet { event_name, body, body_format }`, `HttpPushSyncAndGet { ... }`

- **Wave 3**: TCP parser
  - `tcp_listener::parse_wire_request` arms for OP_PUSH_AND_GET / OP_PUSH_SYNC_AND_GET
  - WireRequest variants `TcpPushAndGet { event_name, body, body_format }`, `TcpPushSyncAndGet { ... }`
  - Note: TCP variants need `event_name` carried in the body (matching push), since the framed TCP format has `op + content_type + payload` only — no path component. So body shape is `{"event": "<name>", "row": {...}, "query": {...}}` for TCP. HTTP carries event_name in URL path.

- **Wave 4**: apply_shard dispatch
  - Match arms for HttpPushAndGet / HttpPushSyncAndGet / TcpPushAndGet / TcpPushSyncAndGet
  - Route to `dispatch_push_and_get_sync` with the right SyncMode

- **Wave 5**: `dispatch_push_and_get_sync` real impl in `runtime_core_glue.rs`
  - Parse body (JSON or msgpack per body_format)
  - Take lock, apply inline, query inline, drop lock
  - Build response (JSON or msgpack)
  - Return GlueResponse

- **Wave 6**: TCP encoder
  - Add `OP_PUSH_AND_GET_RESPONSE` arm to `encode_glue_response_tcp`
  - GlueResponse variant `PushAndGetResult { body, format }`

- **Wave 7**: Sync mode (acks=all)
  - For `/push-sync-and-get` and TcpPushSyncAndGet: call `WalGlue::wal_append_per_event` AFTER the feature query, before returning the response
  - Verifies read-after-write under fsync wait

- **Wave 8**: Python SDK
  - `app.push_and_get(event_type, row=..., entity_key=..., features=..., sync=False)` returning `(PushAck, dict)`
  - Auto-dispatch by transport: HTTP → POST /push-and-get/{event}; TCP → OP_PUSH_AND_GET frame

- **Wave 9**: Tests
  - Read-after-write atomicity: push event + query feature → response shows just-pushed value
  - HTTP and TCP both work; msgpack and JSON both work on TCP
  - Unknown feature → 200 with `null` + warning entry (Phase 12.5 SC5)
  - Unknown event → 4xx error code; no ack/features (Phase 12.5 SC5)
  - sync vs async modes: `/push-sync-and-get` waits for fsync; `/push-and-get` returns at acks=1
  - Latency benchmark: P50 < 300 µs HTTP / < 200 µs TCP single-cell, with combined apply+query under one lock

- **Wave 10**: Bench harness
  - `crates/beava-bench` with `--push-and-get` mode that drives the combined endpoint
  - Append rows to `.planning/throughput-baselines.md`

- **Wave 11**: SUPERSEDE Phase 12.5 plans
  - Add `12.5-01-SUMMARY-SUPERSEDED.md`, `12.5-02-SUMMARY-SUPERSEDED.md`, `12.5-03-SUMMARY-SUPERSEDED.md` notes referencing 12-10
  - Update Phase 12.5 status in ROADMAP

## Files to read

- `/Users/petrpan26/work/tally/CLAUDE.md`
- `/Users/petrpan26/work/tally/.planning/phases/12.5-push-and-get/12.5-CONTEXT.md` (locked decisions)
- `/Users/petrpan26/work/tally/.planning/phases/12.5-push-and-get/12.5-01-PLAN.md` through `12.5-03-PLAN.md` (axum-shaped originals being superseded)
- `/Users/petrpan26/work/tally/crates/beava-server/src/push_and_get.rs` (axum reference impl already shipped)
- `/Users/petrpan26/work/tally/crates/beava-server/src/runtime_core_glue.rs` (dispatch_push_sync at L355+, dispatch_get_batch at L300+)
- `/Users/petrpan26/work/tally/crates/beava-server/src/apply_shard.rs:88+` (dispatch_one)
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/wire_request.rs`
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/router.rs`
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/http_listener.rs`
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/tcp_listener.rs:736+`
- `/Users/petrpan26/work/tally/crates/beava-core/src/wire.rs`

## Out of scope

- Phase 15 PIT-correctness for join queries inside push-and-get — deferred per user 2026-04-29 ("apply event and fetch from table like normal get")
- `push_and_get_multi` (one push, N keys) — v0.1+
- `push_many_and_get` (N pushes, one key) — v0.1+
- /metrics middleware coverage — Phase 13 follow-up
- Apply busy-poll + response batching — Plan 12-08 (independent)
- TCP read schema msgpack — Plan 12-09 (sister plan; 12-10 inherits its conventions)

## Estimated impact

| Metric | Pre-12-10 (push + get separately) | Post-12-10 |
|---|---:|---:|
| Round trips for fraud scoring | 2 | 1 |
| Apple-M4 LAN loopback latency P50 | ~500 µs | ~250 µs |
| Apple-M4 TCP push-and-get throughput (after 12-08+12-09) | n/a | ~140k req/s estimated |
| Read-after-write race window | exists (between push + get) | ZERO (atomic under one lock) |

## Status

- **NOT YET PLANNED** — needs `/gsd-plan-phase 12` (or scoped planner)
- **Recommended ordering**: 12-08 (independent) ‖ 12-09 (independent) → 12-10
- **Blocking:** none for v0 critical path; this is the headline fraud-decisioning latency win.
