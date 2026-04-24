# `push_and_get` — combined push + feature query endpoint

**Captured:** 2026-04-24
**Status:** forward-looking; noted for v0 ship-gate packaging or v0.1 scoping.

## Motivation — fraud decisioning hot path

The canonical beava use case is "score a new transaction":

```
1. Transaction arrives
2. Push event into beava
3. Query N features for the transaction's entity (user_id, card_id, ...)
4. Feed features into a scoring function / rules engine
5. Approve or block
```

Today this requires TWO network round-trips from the scoring service to beava:

```
client → server /push         ~100μs (RTT)
server apply                   ~60ns-2μs
server → client ACK            ~100μs (RTT)
client → server /get           ~100μs (RTT)
server query                   ~60ns-2μs
server → client response       ~100μs (RTT)
────────────────────────────────────────────
total on LAN                   ~400-500μs
```

**Network dominates.** The real scoring latency is bottlenecked by round-trip count, not server compute.

## Proposal

One endpoint, one round-trip, atomic apply + query:

**HTTP:** `POST /push-and-get/{event_name}`
```json
{
  "row": {"user_id": "abc", "amount": 42.50, ...},
  "query": {
    "entity_key": {"user_id": "abc"},
    "features": ["user_tx_count_5m", "user_distinct_merchants_5m", "user_avg_amount_1h"]
  }
}
```

Response:
```json
{
  "ack_lsn": 12345,
  "registry_version": 42,
  "features": {
    "user_tx_count_5m": 17,
    "user_distinct_merchants_5m": 3,
    "user_avg_amount_1h": 38.20
  }
}
```

**TCP:** new opcode `OP_PUSH_AND_GET` with the same body shape (MessagePack or JSON content type).

## Semantics

- **Atomic**: push applies, then query runs, both inside the same `borrow_mut` on `AppState`. Read-your-writes by construction.
- **Durability**: `ack_lsn` returned BEFORE fsync completes (acks=1 by default, matches `/push`). Opt-in variant `POST /push-sync-and-get` waits for fsync (acks=all, matches `/push-sync`) — same pairing as existing `/push` vs `/push-sync`.
- **Error shape**: if push fails (validation / dedupe conflict / WAL unavailable), returns 4xx / 5xx with no feature values. If push succeeds but one or more features fail to resolve (missing feature, unknown entity), returns 200 with the push ack + per-feature null values and a `warnings` array.

## Impact

- **Latency**: ~500μs → ~250μs on LAN (saves one RTT). **~2× latency improvement** on the flagship fraud pattern.
- **Throughput**: neutral. Same apply + query work; just one fewer round-trip.
- **Client code simplification**: one call instead of two; atomic by construction (no client-side "what if the query races a concurrent push" concern).
- **Server load**: slight decrease (half the HTTP/TCP parsing and response serialization overhead per decision).

## Implementation sketch

- **HTTP**: new handler in `crates/beava-server/src/push_get_api.rs`. Reuses existing `push::push_event_json` + `feature_query::query_feature` functions. Inside one `borrow_mut` scope:
  ```rust
  let mut s = state.0.borrow_mut();
  let ack = apply_push(&mut s, event, row, ...)?;
  let features = query_features(&s, entity_key, feature_names);
  (ack, features)
  ```
- **TCP**: new opcode dispatch arm in `crates/beava-server/src/tcp.rs`, same reuse pattern.
- **Python SDK**: `app.push_and_get(Event, row, query={"features": [...], "entity_key": {...}})` → returns `(ack, features_dict)`.
- **Scope**: ~200 LoC Rust (both transports) + ~80 LoC Python SDK + tests.

## Where this could live

Options:
1. **Phase 12.5** — dedicated phase after Phase 12 ships (and Phase 15, since the underlying join / PIT story needs to be correct first).
2. **Phase 12 follow-up** — add as Plan 12-07 after the rest of Phase 12 closes.
3. **v0.1** — ship v0 without it; add as a point release.

**Recommendation**: Phase 12.5, AFTER Phases 12 + 15 land. Why:
- Phase 15 must land first so the combined endpoint uses event-time PIT correctly
- Phase 12 Plan 12-04 (event↔table join) must land first so feature queries resolve joined values correctly
- Small enough to be its own focused phase; can be a "headline DX feature" in v0 marketing
- Independent of Phase 13.3 lockless apply (actually benefits from it directly — atomic borrow_mut scope is cleaner)

## Variants to consider if scoping

- `push_and_get_multi` — push one event, query features for N entity keys (e.g., payer + payee + merchant). One round-trip for multi-entity lookup.
- `push_many_and_get` — push batch of events, query features for one entity. Bulk ingestion + scoring pattern.
- `push_sync_and_get` — acks=all variant with fsync before response. For high-assurance scoring workflows.

## Why NOT in Phase 12

Phase 12's charter is "the existing push/get API surface + joins." Adding `push_and_get` expands scope mid-phase. Better to close Phase 12 on its current scope, then add a small Phase 12.5 (or v0.1 point release) that introduces the combined endpoint as a focused delivery.

## Decision log reference

- User 2026-04-24: "push_event and get is also very good might unlock real use case. Note them down."
