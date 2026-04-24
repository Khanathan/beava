# Session windows — v0.1 candidate

**Captured:** 2026-04-24
**Status:** v0.1 placeholder; design locked to Option (b) per user decision in the 2026-04-24 design session.
**Scope estimate:** ~300 LoC core + ~50 LoC SDK + tests.

## Motivation

Session windows are a streaming-fundamentals feature missing from v0. Fraud use cases care about sessions — "3 failed logins in this session," "total purchases in this checkout session," "duration of active user session." Today these can only be approximated via `time_since_last` at query time; no operator emits per-session state directly.

Tumbling and windowed-tumbling windows (shipped in v0) don't express session semantics because tumbling windows have fixed-schedule boundaries (every 5m, every 1h) that slice across sessions arbitrarily. Sessions are **data-dependent** — their boundaries depend on arrival pattern, specifically "has `gap_ms` elapsed since the last event for this entity?"

## Locked design: Option (b) — generic `SessionWindowed<Op>` wrapper

Same architectural shape as the existing `Windowed<Op>` wrapper (which adds tumbling semantics to any core op). A session-windowed op wraps any commutative inner op (count, sum, avg, variance, ratio, histograms, sketches with merge support) and resets the inner state when a gap elapses.

**NOT** a catalogue of per-op session variants (which was Option (a) — `session_count`, `session_sum`, etc. dedicated operators). Option (b) was chosen because:

1. Applies uniformly to any commutative inner op — users get session semantics on their operator of choice without us shipping N session variants
2. Implementation is bounded (one wrapper type) rather than O(N-ops)
3. Matches the existing `Windowed<Op>` wrapper precedent — internally consistent

### User-facing DX

```python
@bv.event
def UserSessionStats(tx: Transaction):
    return tx.group_by("user_id").agg(
        session_count = bv.count(session="30m"),               # count in current session
        session_total = bv.sum("amount", session="30m"),       # sum in current session
        session_avg = bv.avg("amount", session="30m"),         # avg in current session
    )
```

The `session="30m"` kwarg makes the op session-windowed. Mutually exclusive with `window="5m"` (tumbling) — specifying both → register 400.

### State

```rust
pub struct SessionWindowedOp {
    pub inner_kind: AggKind,
    pub gap_ms: u64,
    pub inner: Box<AggOp>,        // the in-progress session's state
    pub last_event_time_ms: i64,  // for gap detection
    pub sketch_params: SketchParams,  // for sketch inner-ops
}
```

Per-entity state is ONE in-progress session + its last-event timestamp. O(1) memory per entity per session-feature — dramatically simpler than tumbling's 64 buckets.

### Algorithm

**On `apply_event(row, event_time_ms)`:**

```rust
if event_time_ms - self.last_event_time_ms > self.gap_ms {
    // Gap elapsed → start new session
    self.inner = Box::new(fresh_op(self.inner_kind, &self.sketch_params));
}
self.inner.update(row, event_time_ms, ...);
self.last_event_time_ms = event_time_ms;
```

**On `query(query_time_ms)`:**

```rust
if query_time_ms - self.last_event_time_ms > self.gap_ms {
    // Session has closed — return Null or 0 depending on op
    return Value::Null;  // per-op semantics: count → 0, sum → Null, etc.
}
self.inner.query(query_time_ms)
```

The design is intentionally cheap: no explicit session registry, no "all open sessions" collection — one active session per (entity, feature) at any time.

### Commutativity constraint (why inner ops are restricted)

The session-reset on gap is NOT a commutative operation — it's ordered-sensitive. Out-of-order events within tolerate_delay could land in "the previous session" or "the new session" depending on their `event_time` relative to `last_event_time_ms`.

**Only commutative inner ops** (count, sum, avg, variance, stddev, ratio, histograms, sketch ops with merge support) compose correctly because the per-session fold is commutative. Session-wrapping non-commutative ops (first, last, lag, streak, decay, velocity) is rejected at register time with `BV-E-AGG-SESSION-NON-COMMUTATIVE`.

This mirrors the existing `Windowed<Op>` restriction — same set of allowed inner ops.

### Out-of-order event handling (Phase 14 interaction)

Session windows need Phase 14's front-door watermark drop to handle out-of-order events. With `tolerate_delay_ms = 5s` and `session_gap = 30m`:

- Events with `event_time < max_event_time - 5s` → dropped at front door (never reach the session op)
- Events within tolerate → if they fall within the current session window (`event_time > last_event_time - gap_ms`), they update the session's inner state; if they fall before that (late event that should have been in the PREVIOUS session), they're effectively misattributed

For audit-perfect session correctness under out-of-order arrival, users would need Phase 14.1 opt-in modifiability + insert-replay. Most fraud workloads are in-order-within-tolerance, so this edge case is real but rare.

### Session-open vs session-closed observability

A runtime metric exposes "how many entities have an active session":

```
beava_session_active{feature} gauge
```

Incremented on session open, decremented when `query_time - last_event > gap` (lazy — only observed at sweep time). Memory implication: open sessions hold state forever if the entity never returns. The cold-entity GC (shipped in Phase 13-followup) reclaims them once `idle_ttl_ms > session_gap + buffer`.

## Dependencies

- **Phase 14** (watermark + front-door drop) — required. Session behavior under out-of-order is undefined without it.
- **Phase 13.3 Wave 6** (apply-path post-Redis-shape) — not required but cleaner integration; session state lives inside the `Rc<RefCell<AppState>>` with everything else.
- **Phase 17** (table aggregation with tiered modifiability) — independent. Session windows apply to streams, not tables.

## Register-time validation

```
SessionWindowedOp:
- session kwarg REQUIRED if present; must parse as duration string
- window kwarg forbidden (conflict with session) → BV-E-AGG-SESSION-AND-WINDOW
- inner_kind must be in session-safe allowlist:
  allowed = {Count, Sum, Avg, Variance, StdDev, Ratio, Min, Max,
             Histogram, HourOfDayHistogram, DowHourHistogram, EventTypeMix,
             CountDistinct, Percentile, TopK, Entropy}
  rejected = {First, Last, Lag, FirstN, LastN, FirstSeen, LastSeen, HasSeen,
              TimeSince, Streak, MaxStreak, NegativeStreak,
              Ewma, EwVar, EwZScore, DecayedSum, DecayedCount, Twa,
              RateOfChange, InterArrivalStats, BurstCount, DeltaFromPrev,
              Trend, TrendResidual, OutlierCount, ValueChangeCount,
              GeoVelocity, GeoDistance, GeoSpread, UniqueCells, GeoEntropy,
              DistanceFromHome, SeasonalDeviation, MostRecentN, ReservoirSample,
              BloomMember}
  → BV-E-AGG-SESSION-NON-COMMUTATIVE
- gap_ms must be positive, finite; no "forever" (forever-session = lifetime, use omitted kwarg)
- Duration upper bound: 24h recommended; no hard limit
```

## Wire / SDK changes

### Python SDK

Add `session` kwarg to all commutative op builders (`bv.count`, `bv.sum`, etc.):

```python
bv.count(session="30m")          # session-windowed count, 30m gap
bv.sum("amount", session="1h")   # session-windowed sum, 1h gap
```

Grammar: `session` is mutually exclusive with `window`. Validated at decoration + register time.

### AggSpec JSON

```json
{
  "op": "count",
  "session_ms": 1800000
}
```

`session_ms` field added to `AggSpec`; serde default `None` (absent = lifetime / tumbling depending on `window_ms`). Register-time validator rejects `session_ms.is_some() && window_ms.is_some()`.

### TCP wire

No changes. `AggSpec` passes through the existing `AggSpec.params` JSON blob.

### Bincode compatibility

AggSpec already uses a custom serde impl that branches on `is_human_readable()` (Phase 13.3 Wave 5b fix for the bincode deserialize_any bug). Adding `session_ms: Option<u64>` is additive and round-trips through both JSON and bincode without new work.

## Plan breakdown (if/when this becomes Phase 18 or whatever)

- **Task 1** — `AggSpec.session_ms` field + parse + register-time validators (NON-COMMUTATIVE + SESSION-AND-WINDOW error codes). ~80 LoC.
- **Task 2** — `SessionWindowedOp` struct + apply + query methods + 64-op-family unit tests. ~180 LoC.
- **Task 3** — Python SDK `session=` kwarg surface + SDK validation. ~50 LoC.
- **Task 4** — Criterion microbench for session-apply cost + SUMMARY + VERIFICATION. ~40 LoC.

Total: ~300 LoC + tests. Single-agent session should fit cleanly.

## Non-goals

- **Session emission / event-driven output** — in some streaming systems (Flink, Beam), a session "emits a record" when it closes. Beava doesn't emit events from operators; features are query-pulled, not push-emitted. Session closure is observable via `beava_session_active` metric and feature value transitions to null.
- **Session merging / cross-entity sessions** — sessions are per-entity (per `group_by` key). Cross-entity session merging (e.g., "these two user IDs belong to the same session because same device_id") would require session-key rewriting and is v1+ scope.
- **Session state persistence in WAL / snapshot** — SessionWindowedOp serializes naturally through the existing snapshot mechanism (just another `AggOp` variant). No special handling needed; user inherits snapshot + recovery semantics from Phase 7.

## Decision log reference

- User 2026-04-24: "Can you add these in v0.1 b)" — locked to Option (b), generic `SessionWindowed<Op>` wrapper.

## Related v0.1 candidates

Other items discussed/decided in the 2026-04-24 session:
- **Phase 17** — Table aggregation with tiered modifiability (already a roadmap placeholder)
- **Cohort-bucket GC** — Time-TTL-driven memory reclaim with swap-and-drop-off-thread (requires Phase 14 watermark)
- **COW snapshot** — Apply-thread-non-blocking snapshot via `Arc` state clone (requires Wave 6 storage shape decision to preserve Arc)
- **Stream event-time sort buffer** — Opt-in per-stream, correctness-over-latency for decay/velocity ops (alternative to Phase 14.1 modifiability)
- **Perf-optimization ladder** (sharding / io_uring / binary schema) — explicitly dropped until Phase 14 lands; revisit post-ship
