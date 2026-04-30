---
phase: 14-streaming-correctness
type: context
created: 2026-04-24
mode: locked
---

# Phase 14 — Streaming Semantics Chunk A (Correctness Only)

## Purpose

Ship the **correctness-critical** piece of streaming semantics: per-stream watermark + front-door drop of late events + bucket widening that fixes the `agg_windowed` epoch-mismatch silent-data-loss bug. Everything about *modifiability / retraction on the apply path* is explicitly out of scope and deferred to Phase 14.1.

Phase 14 exists to make the server's event-time behaviour **deterministic and lossless within the tolerated lateness window**, and to make genuinely late events fail **loud and observable** (metric + rate-limited log) instead of silently corrupting aggregate state via bucket-reset.

## Background — Why split from original Phase 14

Original Phase 14 bundled watermark + drop + bucket widening + opt-in modifiability + event-time sort buffer into one phase. On 2026-04-24 it was split so the bucket-reset bug fix can ship independently without being gated on the larger Chunk B (modifiability/retraction) design work. The event-time sort buffer is **cut entirely** — replaced by the simpler front-door drop.

## Locked Decisions

### D-01 — Per-stream watermark, default `tolerate_delay_ms = 5000`
Each `@bv.event` source tracks its own watermark (`max_event_time_ms` observed so far on that stream). Default `tolerate_delay_ms = 5000` matches the existing `DEFAULT_TOLERATE_DELAY_MS` constant in `crates/beava-core/src/defaults.rs:9`. Users override via `@bv.event(tolerate_delay_ms=...)` which is already plumbed through `EventDescriptor.tolerate_delay_ms` (`crates/beava-core/src/registry.rs:43`).

### D-02 — Front-door drop policy
An event is **dropped** if `event_time_ms < stream_max_event_time_ms - tolerate_delay_ms`. The check runs **before** `apply_event_to_aggregations` so no operator ever sees a late event. One policy protects all 55 operators uniformly.

### D-03 — Silent drop + metric + rate-limited log
Drops do NOT return an error to the client. Behaviour:
- Push ACK succeeds (client sees 200/ack_lsn as usual) — the event is still appended to the WAL so replay reproduces the drop deterministically.
- `beava_events_dropped_late_total{stream}` counter increments on every drop.
- A WARN-level structured log fires **at most once per stream per minute** (rate-limited) with shape `{stream, event_time_ms, watermark_ms, drift_ms}`.

Rationale: fraud/ad-tech pipelines commonly see a small tail of late events; returning errors would make clients build redundant retry logic for events that are structurally doomed. The metric makes it observable; the rate-limit keeps logs readable.

### D-04 — Drop happens inside the apply `borrow_mut` scope
The late check is atomic with the apply: **read watermark + compare + (update watermark | increment drop counter) + apply** all happen inside the same `borrow_mut` on `AppState`. This avoids any window where a concurrent push could race the watermark and apply out of order relative to the drop decision.

### D-05 — Bucket widening (the bug fix)
Change `WindowedOp::new` / `WindowedOp::new_with_params` from:
```rust
let bucket_ms = window_ms.div_ceil(64);
```
to:
```rust
let bucket_ms = (window_ms + tolerate_delay_ms).div_ceil(64);
```
`tolerate_delay_ms` must be threaded into the agg-compile pipeline (`agg_compile.rs`) from the source `EventDescriptor.tolerate_delay_ms` so each compiled `WindowedOp` is sized for `window + tolerate_delay`, not `window` alone.

**Why this fixes silent data loss:** currently, with `bucket_ms = window / 64`, an in-tolerance late event whose `event_time_ms` falls into the same ring-buffer slot as a just-written fresh event but a **different bucket epoch** causes `bucket_epoch_start_ms[idx] != bucket_epoch` to fire, which **resets the bucket to fresh** (see `agg_windowed.rs:180,212`), silently destroying the fresh event's contribution. Widening the bucket to `window + tolerate_delay` guarantees that within-tolerance late events always hit the correct (still-fresh) epoch.

### D-06 — Register-time validator: `tolerate_delay ≤ window`
If a user declares a feature with `tolerate_delay > window`, register returns **400** with code `tolerate_delay_exceeds_window`. This is a hard constraint — widening buckets to `window + tolerate_delay` only makes sense when tolerance is bounded by the window it's smoothing over. Server-side validator in `register_validate.rs`; SDK can add a client-side pre-check but the server is the source of truth.

### D-07 — WAL replay reconstructs watermark (no persistence)
Per-stream watermark state is **not** written to snapshots. WAL replay in `crates/beava-server/src/recovery.rs` rebuilds the watermark naturally as events replay: each event's `event_time_ms` updates the stream's watermark during replay, exactly as at live-runtime. After replay finishes, watermarks are consistent with the replayed event stream.

**Implication:** a snapshot-without-WAL (e.g. a user who disabled WAL — not supported in v0) would lose watermarks, but since the WAL replay is the canonical recovery path, this is safe.

### D-08 — Post-13.3 apply-path assumption
Phase 14 lands **after** Phase 13.3 (lockless apply via `Arc<LocalState<RefCell<AppState>>>`) merges to `v2/greenfield`. Watermark state is a **field on `AppState`**, accessed inside the handler's `borrow_mut` scope (D-04). The front-door drop check is a few lines inside the same `borrow_mut` block that currently wraps `apply_event_to_aggregations` — no new synchronization primitive needed.

**Fallback:** if Phase 13.3 has not yet merged when Phase 14 execution starts, the executor must rebase onto post-13.3 `v2/greenfield` HEAD before proceeding. Do NOT design around the pre-13.3 `Mutex<AggStateTables>` shape — it's about to disappear.

### D-09 — Drop-check cost budget: < 20ns per event
The watermark check (read atomic / u64, compare two i64s, update or increment) must cost < 20ns per push on Apple-M4. This is enforced by a criterion microbench in Plan 04. If the check exceeds 20ns we've made a coding error (the operation is genuinely a handful of cycles).

### D-10 — End-to-end throughput regression < 5%
Adding the drop check must not regress `beava-bench simple-fraud (small, HTTP, BATCH_MS=0)` by more than 5% vs. the current `v2/greenfield` baseline. Plan 04 captures this explicitly in `.planning/throughput-baselines.md`.

## Scope Boundary — What is OUT

| Concern | Phase | Reason |
|---|---|---|
| Opt-in modifiability (`@bv.event(modifiable=True)`) | 14.1 | Requires per-(entity,feature) K-event log; separate design |
| Retraction on apply path (stream retractions) | 14.1 | Needs modifiability machinery |
| Event-time PIT temporal store | 15 | Tables only; separate |
| Explicit `@bv.source` annotation | 16 | SDK surface change |
| Event-time sort buffer | — | **Cut entirely**; replaced by front-door drop |

## Success Criteria (SC1–SC6)

1. **SC1 — Front-door drop correctness.** Events with `event_time < stream_max - tolerate_delay_ms` are dropped at the front door; `apply_event_to_aggregations` never observes them. Regression test pushes `t=10_000`, then `t=4_999` (tolerate=5000) and asserts the late event's contribution is absent from every downstream feature.
2. **SC2 — Metric increments.** `beava_events_dropped_late_total{stream="X"}` increments by exactly 1 per dropped event. Verified by Prometheus-scrape integration test.
3. **SC3 — Rate-limited log.** Under a burst of 1000 drops in the same second on the same stream, exactly 1 WARN log line is emitted. Under drops on two different streams in the same window, 2 lines are emitted (rate-limit is per-stream).
4. **SC4 — Bucket-reset bug fixed.** Regression test encoding the specific original bug (stream with `window=60s, tolerate_delay=5s`, write event at `t=60_100`, write event at `t=59_999`, query sum) passes: the in-tolerance late event's value IS included in the window's sum. With the old `bucket_ms = window / 64` code, the second event's value was silently destroyed; with the new `bucket_ms = (window + tolerate_delay) / 64` it is not.
5. **SC5 — Register-time validator.** `POST /register` with a feature whose `tolerate_delay_ms > window_ms` returns **400** `{error: {code: "tolerate_delay_exceeds_window", ...}}`. Validator added in `register_validate.rs`; SDK tests cover HTTP + TCP parity.
6. **SC6 — WAL replay preserves semantics.** Scenario: run N events (some in-tolerance late, some out-of-tolerance), kill, restart, assert post-restart feature values are byte-identical to pre-restart and drop counters match.

## File Inventory (expected touch set)

| File | Why |
|---|---|
| `crates/beava-server/src/app_state.rs` (or wherever `AppState` lives post-13.3) | Add watermark field + drop counter + rate-limited log state |
| `crates/beava-server/src/push.rs` | Front-door drop inside `borrow_mut` (step ~8 in current order — between dedupe and apply) |
| `crates/beava-core/src/agg_windowed.rs` | `WindowedOp::new*` signature: take `tolerate_delay_ms`; `bucket_ms = (window + tolerate_delay) / 64` |
| `crates/beava-core/src/agg_compile.rs` | Thread `tolerate_delay_ms` from `EventDescriptor` into `WindowedOp::new_with_params` |
| `crates/beava-core/src/register_validate.rs` | `tolerate_delay ≤ window` validator per feature |
| `crates/beava-server/src/recovery.rs` | Watermark rebuild during WAL replay |
| `crates/beava-server/src/metrics.rs` (or equivalent in Phase 13 `/metrics`) | Declare `beava_events_dropped_late_total` counter |
| `crates/beava-core/benches/*` | Drop-check microbench |
| `.planning/perf-baselines.md` | Drop-check baseline row |
| `.planning/throughput-baselines.md` | simple-fraud regression baseline row |
| tests / integration smokes | SC1–SC6 coverage |

## Plan Structure (4 plans, ~11 tasks)

| Plan | Scope | Tasks |
|---|---|---|
| 14-01 | Watermark state struct + `AppState` field + front-door drop + counter + rate-limited log | 3 |
| 14-02 | Bucket widening in `WindowedOp`; thread `tolerate_delay_ms` through agg_compile; regression test for the original bucket-reset data-loss scenario | 3 |
| 14-03 | Register-time validator (`tolerate_delay ≤ window`); WAL replay reconstructs watermark | 2 |
| 14-04 | Criterion microbench (drop-check < 20ns); `beava-bench` throughput row (< 5% regression); SUMMARY.md + VERIFICATION.md | 3 |

All plans are sequential (Wave 1 → 2 → 3 → 4) because the bucket-widening in 14-02 is meaningless without the drop path in 14-01 landing first, the validator in 14-03 depends on the feature-compile path in 14-02, and the ship-gate work in 14-04 depends on everything above.

## TDD Discipline

Every code task produces a red commit (failing test) followed by a green commit (impl) per CLAUDE.md §Conventions. Test subjects per task are enumerated in each PLAN.md's `<behavior>` block.

## Performance Discipline

- Plan 14-04 MUST include a `criterion` microbench for the drop-check path (< 20ns/event on Apple-M4, captured to `.planning/perf-baselines.md`).
- Plan 14-04 MUST run `beava-bench` simple-fraud (small × HTTP × BATCH_MS=0) and append the result row to `.planning/throughput-baselines.md` with ≤ 5% regression vs. current `v2/greenfield` baseline. If regression exceeds 5%, Plan 14-04 BLOCKS and we dig in before shipping.

## Grey Areas (sign-off before execution)

1. **Rate-limit implementation.** Simplest viable: per-stream `AtomicU64` holding `last_log_ms_unix`; WARN fires iff `now - last_log_ms >= 60_000` and swap succeeds. Alternatives (sliding window, token bucket) are over-engineered for v0 — this is a log-readability feature, not a rate-limiting contract. Assumed simple-atomic unless user objects.
2. **Watermark storage shape.** `BTreeMap<String, u64>` keyed by event name, guarded only by the `borrow_mut` on `AppState` (no inner lock). Alternative: `AHashMap`. The map is tiny (O(num streams), typically < 50). Assumed `BTreeMap` for deterministic ordering during debug / snapshot dump.
3. **Metric label cardinality.** `stream` label is the event name (user-defined, finite, small — typically < 100 streams). Safe for Prometheus. No risk of unbounded cardinality.
4. **Drop counter in `/metrics` vs apply-loop-local.** Plan 14-01 defines the counter as a field on `AppState`; Plan 13-01 already has the `/metrics` Prometheus registry. Assumed Phase 13 metric-counter wiring pattern is extended — one more counter declared in the metrics module, incremented from the push path.
