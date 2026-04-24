# Beava v0 — operator catalogue

This is the user-facing reference for the 40+ aggregation operators
shipped in the Beava v0 OSS cut. Each operator slot has the same shape
in the REGISTER JSON wire:

```json
{
  "<feature_name>": {
    "op": "<op_name>",
    "params": {
      "field":  "<column>",   // optional or required, see per-op
      "window": "5m",          // optional or required, see per-op
      "where":  "<expr>",      // optional WHERE predicate
      "n":      <int>          // required for first_n/last_n/lag/time_since_last_n
    }
  }
}
```

Window strings: `\d+(ms|s|m|h|d)` or `forever`. Examples: `"5m"`,
`"1h"`, `"100ms"`. `where` is a `bv.col(...)` expression string.

## Phase 5 — Core (8)

| Op | Field | Window | Output | Notes |
|---|---|---|---|---|
| `count` | — | optional | i64 | Counts events in window. |
| `sum` | required | required | f64 | Sum of numeric field. |
| `avg` | required | required | f64 | Arithmetic mean. |
| `min` | required | required | inherits | Running minimum. |
| `max` | required | required | inherits | Running maximum. |
| `variance` | required | required | f64 | Welford sample variance (n-1). |
| `stddev` | required | required | f64 | sqrt(variance). |
| `ratio` | — | optional | f64 | Matching/total of `where=` predicate. |

## Phase 8 — Point/ordinal (5)

| Op | Field | Output | Notes |
|---|---|---|---|
| `first` | required | inherits | First non-null value seen. Lifetime. |
| `last` | required | inherits | Most recent non-null value. Lifetime. |
| `first_n` | required, `n` required | str (JSON-array) | First `n` values, oldest-first. Returns `Value::Str` containing a JSON-encoded array (`Value::List` is a v0.1+ type). |
| `last_n` | required, `n` required | str (JSON-array) | Most recent `n` values, oldest-first. |
| `lag` | required, `n` required | inherits | Value `n` events ago. `lag(field, 1)` = previous event's value. Null until `n+1` events seen. |

`n` must be in `[1, 1024]`. Phase 8 point ops do NOT accept `window=`
(lifetime-only); supplying one returns
`{"code": "aggregation_invalid_window"}`.

## Phase 8 — Recency markers (6)

| Op | Field | Output | Notes |
|---|---|---|---|
| `first_seen` | — | datetime | event_time_ms of first matching event. |
| `last_seen` | — | datetime | event_time_ms of most recent matching event. |
| `age` | — | i64 (ms) | `query_time_ms - first_seen_ms`. Null when never seen. |
| `has_seen` | — | bool | True iff any matching event observed. |
| `time_since` | — | i64 (ms) | `query_time_ms - last_seen_ms`. Null when never seen. |
| `time_since_last_n` | `n` required | i64 (ms) | ms since the n-th most recent matching event. Null until `n` events seen. |

These ops are lifetime-only and don't accept `window=`. Use `where=` to
filter which events count as "matching".

## Phase 8 — Streaks (3)

| Op | Field | Output | Notes |
|---|---|---|---|
| `streak` | — | i64 | Current consecutive matching events. Resets on first non-match. |
| `max_streak` | — | i64 | High-watermark of streak over the entity's lifetime. |
| `negative_streak` | — | i64 | Mirror — current consecutive NON-matching events. |

Typically used with `where=` to define what "matching" means
(`where=bv.col("status") == "ok"`).

## Phase 8 — Windowed recency (1)

| Op | Field | Window | Output | Notes |
|---|---|---|---|---|
| `first_seen_in_window` | — | required | bool | True iff the most-recent matching event is within `window_ms` of the query time. Lifetime state with a window-duration parameter (NOT a tumbling-bucket window). |

## Future families (Phase 9–11)

- Decay + velocity (Phase 9): `ewma`, `ewvar`, `ew_zscore`, `decayed_sum`,
  `decayed_count`, `twa`, `rate_of_change`, `inter_arrival_stats`,
  `burst_count`, `delta_from_prev`, `trend`, `trend_residual`,
  `outlier_count`, `value_change_count`, `z_score`
- Sketches (Phase 10): `count_distinct` (HLL), `percentile` (DDSketch),
  `top_k` (SpaceSaving), `bloom_member`, `entropy`
- Bounded-buffer + geo (Phase 11): `histogram`, hour/dow histograms,
  `seasonal_deviation`, `event_type_mix`, `most_recent_n`,
  `reservoir_sample`, `geo_velocity`, `geo_distance`, `geo_spread`,
  `unique_cells`, `geo_entropy`, `distance_from_home`
