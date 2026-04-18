# `src/engine/`

The pipeline and operator engine — event-time windowing, watermark tracking,
and the push-batch execution path. This module owns the semantics of "given a
stream of events with timestamps, compute per-key feature values". Everything
in here is stateless with respect to disk; durable state flows through the
`../state/` module.

## Files

- **`pipeline.rs`** — the load-bearing hot path. `push_batch_with_cascade_no_features`
  takes `&[(&Value, SystemTime)]`, groups events by event-time bucket, and
  dispatches to per-stream operator sets. The CORR-01 "2a fix" lives here
  (Phase 46): a single shared `now` is captured once per batch so distinct
  event-times within a batch are never collapsed.
- **`event_time.rs`** — `parse_event_time(payload, fallback)`, the
  `WATERMARK_LATENESS` default (5 s), per-stream lateness overrides
  (CORR-03/04), `WatermarkTracker` (the event-time clock source for eviction
  and TTL, per CORR-07), and γ-propagation helpers for join/fork watermark
  advancement.
- **`operators.rs`** — the operator registry and dispatch layer. Each operator
  is a `Box<dyn Operator>` registered per stream × feature; the file enumerates
  count, sum, avg, min, max, stddev, percentile, distinct_count, last, first,
  lag, ema, last_n, exact_min, exact_max, and derive.
- **`window.rs`** — sliding-window ring-buffer abstraction shared across
  ring-buffer operators. Handles bucket rotation, partial-bucket eviction, and
  the `retracting_ring.rs` variant used for mutable-state operators.
- **`expression.rs`** — derived-feature expression evaluator (the `derive`
  operator uses it to compute expressions over other features).
- **`register.rs`** — stream and pipeline registration logic; validates operator
  configs and allocates per-stream operator state.
- **`recommend.rs`** — recommendation-style aggregation operator (collaborative
  filtering window features).
- **`hll.rs`** / **`cms.rs`** / **`uddsketch.rs`** — probabilistic data
  structures backing the `distinct_count`, `frequency`, and `percentile`
  operators respectively.

## Not here

- HTTP / TCP routing and connection handling (see `../server/`).
- WAL, snapshots, and eviction execution (see `../state/`).

## Read order

`pipeline.rs` first — it is the main hot path and the best entry point for
understanding execution flow. Then `event_time.rs` for the event-time
semantics documented in `docs/event-time.md`. `operators.rs` and the
individual operator files can be read lazily; each is self-contained.
