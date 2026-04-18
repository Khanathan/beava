# Phase 51: cross-shard-queries-joins - Context

**Gathered:** 2026-04-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Cross-shard read path + join correctness + shard observability. Four REQs:
- **TPC-PERF-05** `GET /streams` scatter-gather via `futures::join_all`
- **TPC-PERF-06** Lazy global-watermark publish across shards
- **TPC-INFRA-05** `GET /debug/shards` diagnostics + hot-shard warning
- **TPC-CORR-04** `JoinShardKeyMismatch` register-time fatal error

Ship-gate: scatter-gather p99 latency <15 μs added vs point-read; all existing join tests green at N>1 with co-located `shard_key` declarations; /debug/shards returns live data for every shard.

</domain>

<decisions>
## Implementation Decisions

### Global watermark publish cadence

- **D-01:** **Every 1024 events processed per shard per stream.** Each shard maintains a local `events_since_publish` counter per stream; on rollover past 1024, publishes its current `max event_time` for that stream to a global atomic array `Arc<Box<[AtomicU64]>>` indexed by shard. Amortizes atomic-write cost; deterministic in event-count space. Lag bound = 1024 / eps per shard.
- **D-02:** Global watermark for stream = `min` across published per-shard atomics. Unlabeled metric `beava_watermark_lag_seconds` (SRE-STREAM persona requirement) is derived from `min(beava_shard_watermark_lag_seconds{shard})`.
- **D-03:** Publish threshold (`N=1024`) is tunable via `BEAVA_WATERMARK_PUBLISH_INTERVAL` env (default 1024, clamp 64..=65536). Document in `docs/operations.md` later in Wave 5.

### Who uses global watermark

- **D-04:** Three consumers (documented for downstream agents):
  1. `GET /streams/{name}` response — scalar watermark field (scatter-gather min).
  2. Fork/replica `OP_SUBSCRIBE` framing — wire value for downstream.
  3. `beava_watermark_lag_seconds` unlabeled gauge (SRE alerting).
- TTL eviction and co-located joins continue using shard-local watermarks (design doc §5; eviction code path unchanged).

### /streams scatter-gather

- **D-05:** `GET /streams` handler uses `futures::join_all` across all shards. Each shard returns its view of the stream registry (should be identical — stream registration is replicated to every shard at registration time); handler deduplicates + merges watermarks (min). Budget: p99 <15 μs added vs a point read. Increment `beava_cross_shard_fanout_total{op="list_streams"}` on every call.
- **D-06:** `GET /streams/{name}` similarly scatter-gathers to get the per-stream watermark min; stream-definition fields (schema, registered_at, shard_key) come from the local shard's replica (all shards hold identical StreamDefinition).

### Hot-shard warning threshold

- **D-07:** Hot-shard = `keys_owned > 1.5 × fleet_mean`. Tighter than the initial 2× recommendation — user chose the tighter threshold to catch subtle imbalance earlier. Evaluate on each `/debug/shards` GET (on-demand computation; no background loop). Tunable via `BEAVA_HOT_SHARD_THRESHOLD` env (float, default 1.5, clamp 1.1..=10.0).
- **D-08:** Warning surface: the `/debug/shards` response includes a top-level `hot_shards: [{shard: N, keys_owned: K, fleet_mean: M, ratio: R}]` array. Empty when balanced. Also emit a log warning once every 60s when any shard crosses threshold. Do NOT emit Prometheus-level warning metric — operators reading /debug/shards are the intended consumers.

### /debug/shards response shape

- **D-09:** JSON response:
  ```json
  {
    "shard_count": 8,
    "shards": [
      {"id": 0, "inbox_depth": 42, "reactor_utilization": 0.73, "keys_owned": 12504, "watermark_lag_seconds": 1.2, "events_total": 12345678, "inbox_full_total": 0, "down": false},
      ...
    ],
    "hot_shards": [],
    "ready": true
  }
  ```
  Ready field mirrors `/ready` semantics (true only when every shard passed its boot barrier and no shard is in DOWN state).

### JoinShardKeyMismatch error channel

- **D-10:** **Dual-channel.** Synchronous error to the SDK caller during stream registration: structured error with fields `{streams: [A, B], keys: [keyA, keyB], suggested_fix: "Add shard_key=\"<common_field>\" to @bv.stream of both streams"}`. Pipeline does not start — registration RPC returns the error, the engine is not activated with an inconsistent join.
- **D-11:** Additionally emit `JoinShardKeyMismatchWarning` to `/debug/warnings` for operators watching that surface; fires once per invalid registration attempt.
- **D-12:** Error message format locks the text: `"join operator between '{A}' and '{B}' requires matching shard_key; got '{keyA}' vs '{keyB}'. Fix: declare @bv.stream(shard_key='{suggested_common}') on both streams."` — downstream agents must preserve this text for grep-testability.

### Claude's Discretion

- Exact type of the global-watermark atomic storage (`Vec<AtomicU64>` vs a lock-free `DashMap<StreamId, AtomicU64>` vs `Box<[AtomicU64]>` keyed by (shard_id, stream_ord_id)): planner picks.
- Whether /debug/shards reads compute `reactor_utilization` by polling Tokio metrics or from an explicit per-shard EWMA (latter matches Redpanda's `vectorized_reactor_utilization` pattern): planner picks.
- Suggested common field heuristic for `JoinShardKeyMismatch` message (simple: use the join's `on=` field; sophisticated: intersect declared shard_keys if both are tuples): planner picks the simple one.

</decisions>

<canonical_refs>
## Canonical References

### Design + research
- `.planning/arch/TPC-SHARD-DESIGN.md` §3 "Cross-shard queries" (scatter-gather + co-location), §5 "Watermark propagation across shards", Q6 (metrics).
- `.planning/arch/TPC-RESEARCH.md` §2 Prior art (ScyllaDB scatter-gather pattern, Redpanda reactor_utilization), §Q6 metrics vocabulary.
- `.planning/research/SUMMARY.md` §"Wave 3".
- `.planning/research/PITFALLS.md` §1.1 (inter-shard ordering — N=1↔N=K parity test in Wave 5 is the safety net).

### Requirements
- `.planning/REQUIREMENTS.md` — TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04.

### Upstream phases
- Phase 48 D-01 (routing function).
- Phase 49 D-04/D-05/D-06 (WatermarkState on Shard, full relocation already done — Wave 3 is purely additive).
- Phase 49 D-07/D-08/D-09 (StreamDefinition.shard_key available for JoinShardKeyMismatch check).
- Phase 50 D-07 (metrics crate wiring — Wave 3 ADDS `beava_cross_shard_fanout_total{op=...}` emissions; the counter itself is defined in Wave 2).

### Existing code
- `src/server/http.rs` — GET /streams, GET /streams/{name} handlers (extend with scatter-gather).
- `src/engine/pipeline.rs` — stream registration path (add JoinShardKeyMismatch check).
- `src/state/shard/watermark.rs` (Phase 49 output) — extend with publish-to-global-atomic logic.
- `src/server/shard_probe.rs` — existing cross-shard diagnostics; /debug/shards may reuse data sources.
- `src/debug/warnings.rs` — /debug/warnings emission (extend with JoinShardKeyMismatchWarning).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `futures::join_all` — already a dep via Phase 50 (metrics crate pulls it transitively, or add explicit dep per STACK.md).
- Phase 50's ShardRouter — read-path scatter-gather iterates the same shard array.
- Phase 50's metrics infrastructure — `beava_cross_shard_fanout_total{op}` counter; Wave 3 increments it on each scatter.

### Established Patterns
- /debug/* endpoints are JSON (no Prometheus format), admin-only via require_loopback_or_token — same pattern for /debug/shards.
- /debug/warnings append pattern (seen in /public/warnings middleware) — add JoinShardKeyMismatchWarning as a new warning kind.

### Integration Points
- Scatter-gather call site: `src/server/http.rs` GET /streams and GET /streams/{name} handlers.
- Global watermark publish call site: inside Shard's event-processing loop (every N events, call `publish_watermark(stream, max)`).
- JoinShardKeyMismatch validation: runs inside `register_pipeline` in `src/engine/pipeline.rs`; fires before any shard activation.

</code_context>

<specifics>
## Specific Ideas

- Wave 3 is **purely additive** on top of Wave 1's full WatermarkTracker relocation and Wave 2's routing. Lazy publish is new code (no unwinding); scatter-gather is new code on existing handlers; /debug/shards is a new endpoint.
- Hot-shard threshold `1.5×` is tighter than the 2× recommendation — expect a few false-positives on uniform workloads with small N. Tunable via env; document calibration advice in Wave 5 operations doc.

</specifics>

<deferred>
## Deferred Ideas

- Per-shard event log directory layout → Wave 4.
- Snapshot v8 + hard-fail boot guard → Wave 4.
- Fork/replica re-hash at ingest → Wave 4.
- Reshard CLI tool → Wave 4.
- N=1↔N=K proptest parity harness → Wave 5 (TPC-CORR-05).

</deferred>

---

*Phase: 51-cross-shard-queries-joins*
*Context gathered: 2026-04-18*
