# Plan 06-03 Summary — `/push` + `IdemCache`

**Status:** shipped 2026-04-23
**Branch:** v2/greenfield
**Commits:** `5771ff1` (RED) → `d1d72bc` (GREEN)

## What shipped

`POST /push/{event_name}` is live. The flow follows `06-CONTEXT.md` D-11/D-12
with one documented refinement (see below):

1. Parse JSON body (`Bytes` extractor so we can replay byte-identical).
2. Lookup event descriptor via `registry.read().events.get(&event_name)` →
   404 `{"error":{"code":"event_not_found"}}` on miss.
3. Schema-validate: every required (non-optional) field must be present and
   type-compatible (Str/I64/F64/Bool/Datetime; Bytes rejected since JSON has
   no binary) → 400 `{"error":{"code":"invalid_event"}}` on fail.
4. If the descriptor has `dedupe_key` set, extract the value (string/number/
   bool coerced to string) and check `IdemCache::get`. On hit, return the
   cached response bytes with `X-Beava-Idempotent-Replay: 1` header and a
   200 status.
5. Extract `event_time_ms` from the configured `event_time_field` or fall
   back to wall-clock.
6. Serialize the WAL payload: `{"v":1,"rv":registry_version,"s":event_name,
   "et":event_time_ms,"b":<raw body>}`.
7. `WalSink::append_event(payload).await` — resolves only after fsync,
   yielding the assigned LSN (SRV-DUR-02).
8. `apply_event_to_aggregations(&event_name, &row, event_time_ms, ack_lsn,
   &registry, &mut tables)` under the single-writer `state_tables` lock.
9. `fetch_max` on `next_event_id` (tracks highest seen LSN) and
   `max_event_time_ms` (deterministic query-time per D-06).
10. Build the response `{"ack_lsn": N, "idempotent_replay": false,
    "registry_version": V}` and serialize to bytes.
11. On the dedupe-configured path, insert a `CachedEntry` keyed by
    `(event_name, dedupe_str)` with `expires_at_ms = now_ms + window_ms`.
12. Return 200 with `Content-Type: application/json`.

### D-12 refinement — apply-AFTER-fsync

The plan flagged a choice between pre-fsync and post-fsync apply. We
committed to **apply-after-fsync** for v0:

- Apply cost measured in Phase 5 benches is ns–µs, well below the 2 ms
  fsync budget.
- Post-fsync apply gives stronger crash safety — in-memory state never
  diverges from disk. If fsync fails, the handler returns 503 and the
  event was never applied.
- If Phase 12 (`/push-sync` with computed features) needs sub-ms latency,
  we can revisit via `stage()` + `wait_for_durable()` split.

### X-Beava-Idempotent-Replay header

To keep the dedupe replay body **byte-identical** (success criterion #2)
while still signalling replay to observability, we introduced the
`X-Beava-Idempotent-Replay: 1` response header. Absent on fresh responses.
The body itself always carries `idempotent_replay: false` — this field
reflects the state at the time the original (cached) request was
processed. Phase 13 docs will codify the header.

## AppState

```rust
#[derive(Clone)]
pub struct AppState {
    pub dev_agg: DevAggState,
    pub wal_sink: WalSink,
    pub idem_cache: Arc<IdemCache>,
}
```

`Server::bind` now spawns the WAL sink + a periodic dedupe-cache sweeper
(interval from `cfg.durability.dedupe_sweep_interval_secs`), then
constructs `Arc<AppState>`. `serve()` shuts down the sink via
`WalSink::shutdown().await` before returning so pending pushes flush.

The router exposes two entry points: the historical `router(..)` (pre-dates
AppState; Phase 1 unit tests still use it; no `/push` mounted) and the new
`router_with_push(..)` which mounts `/push/:event_name` when `app_state` is
`Some`.

## Config env-var additions

| Env var | Config field | Default |
|---|---|---|
| `BEAVA_WAL_DIR` | `durability.wal_dir` | `./beava-wal` |
| `BEAVA_WAL_FSYNC_INTERVAL_MS` | `durability.wal_fsync_interval_ms` | `2` |
| `BEAVA_WAL_FSYNC_BYTES` | `durability.wal_fsync_bytes` | `1 << 20` |
| `BEAVA_WAL_SEGMENT_BYTES` | `durability.wal_segment_bytes` | `128 << 20` |
| `BEAVA_DEDUPE_SWEEP_SECS` | `durability.dedupe_sweep_interval_secs` | `60` |

## Tests

- `phase6_push.rs` — 10/10 pass:
  - `push_happy_path_returns_ack_and_applies_event`
  - `push_without_dedupe_key_bypasses_cache`
  - `push_with_dedupe_key_replays_byte_identical`
  - `push_with_dedupe_different_key_no_replay`
  - `push_dedupe_after_window_expires`
  - `push_unknown_event_returns_404`
  - `push_schema_mismatch_returns_400`
  - `push_ack_lsn_strictly_monotonic`
  - `push_persisted_to_wal`
  - `push_sync_data_before_ack`
- `IdemCache` unit tests — 4/4 pass.
- Workspace regression: all 531 prior tests remain green. New total: 598
  (workspace with `--features beava-server/testing`).

## Open items / follow-ups for Plan 04

- Wire `op=push` TCP handler (Phase 2.5 reserved the opcode; currently
  `op_not_implemented`). Plan 12 / follow-up.
- Pre-fsync apply exploration — only if Phase 12 latency SLA demands it.
