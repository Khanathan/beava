# bv.ew_zscore

> Current-event z-score against an exponentially-weighted baseline (EWMA mean / EWVar stddev).

## Signature

```python
bv.ew_zscore(
    field: str,
    *,
    half_life: str,
    where: bv.Col | None = None,
) -> AggDescriptor
```

## Description

`bv.ew_zscore` reports how unusual the **current event's** value is
relative to the entity's exponentially-weighted recent baseline. It is a
two-line composition: maintain an [`bv.ewma`](./ewma.md) and an
[`bv.ewvar`](./ewvar.md) on the same field with the same `half_life`,
then at query time return
`z = (last_x - ewma) / sqrt(ewvar)`. The `half_life` parameter is the
same exponential decay coefficient used by EWMA / EWVar — observations
older than one `half_life` contribute half as much; older than two,
quarter; and so on. `Δt` is **server processing-time** between
consecutive matching events per
[`project_redis_shaped_no_event_time_ever`](../../../.planning/PROJECT.md).

`bv.ew_zscore` is the standard primitive for **drift-aware anomaly
scoring**. Pair it with a downstream rule like
`if abs(z) > 3: flag()` to detect events that look big relative to the
entity's *recent* volatility — which is much more useful than a flat
z-score against all-time stats, because both legitimate user behaviour
and fraud patterns drift over time. Pick a `half_life` equal to the
timescale of the behavioural drift you care about (a transaction-amount
EWMA half-life of 1 day is reasonable for retail; 1 hour for high-velocity
fraud).

`bv.ew_zscore` belongs to the **decay** family. Per-event update wraps
EWVar's update path; cost is **Tier 1** (~18 ns algorithm floor / ~38 ns
measured) and memory is `O(1)` per entity. Lifetime mode is the only
mode — `half_life` sets the decay rate, no `window=` kwarg exists.

## Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `field` | `str` | Yes | — | Numeric field (`i64` or `f64`) to z-score. |
| `half_life` | `str` | Yes | — | Duration string matching `\d+(ms\|s\|m\|h\|d)`. Must be positive; `"forever"` is **rejected**. Same value drives both the EW-mean and EW-variance behind the score. |
| `where` | `bv.Col` | No | `None` | Boolean expression on event fields; only matching events update the baseline and produce a z-score. |

## Returns

A single `f64` — the z-score of the most recent matching event against
the entity's running EWMA / EWVar baseline. Cold-start (no matching
events seen) returns `null` (Python `None`). When the EW-variance is
still `0.0` (only one matching event observed), the z-score is also
reported as `null` (no baseline volatility to divide by).

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | **Tier 1** (~18 ns floor / ~38 ns measured) — see [cost-class.md](../cost-class.md#tier-1-fast-40-nscall--38-ops) |
| Memory per entity | `O(1)` — wraps EWVarState ≈ 32 B + last-x cache |
| Lifetime mode | **Required** — no `window=` kwarg; `half_life` controls decay rate |

## Examples

### Example 1: Anomaly z-score for transaction amount, 1h half-life

```python
import beava as bv

@bv.event
class Txn:
    user_id: str
    amount: float

@bv.table(key="user_id")
def UserAmtAnomaly(txns) -> bv.Table:
    return (
        txns.group_by("user_id")
            .agg(amt_z=bv.ew_zscore("amount", half_life="1h"))
    )

# After a stable history of small purchases, a sudden $5000 charge
result = app.get("UserAmtAnomaly", "alice")
# result == {"amt_z": <large positive float, e.g. 4.7>}
```

### Example 2: Latency outlier detection, 30m half-life, only successful payments

```python
@bv.table(key="user_id")
def UserLatencyAnomaly(txns) -> bv.Table:
    return (
        txns.group_by("user_id")
            .agg(latency_z=bv.ew_zscore("latency_ms",
                                          half_life="30m",
                                          where=bv.col("status") == "ok"))
    )
```

## Wire

JSON wire form in a register payload:

```json
{
  "kind": "derivation",
  "name": "UserAmtAnomaly",
  "output_kind": "table",
  "key": ["user_id"],
  "agg": {
    "amt_z": {
      "op": "ew_zscore",
      "params": {
        "field": "amount",
        "half_life": "1h"
      }
    }
  }
}
```

See [examples/wire/register-fraud-team.request.json](../../../examples/wire/register-fraud-team.request.json) for a full payload example.

## Edge cases

- **Empty stream / cold-start:** result is `null`.
- **Single matching event:** variance is `0.0` so the z-score is `null` (no baseline volatility yet). Two or more events are needed.
- **Constant value stream (variance stays `0.0`):** z-score is `null` for every read — there is no spread to normalize against.
- **Late or duplicate event (Δt ≤ 0):** the helper applies an unweighted blend (treats the event as same-instant) and does **not** advance `last_now_ms`.
- **Missing or non-numeric `field`:** the event is silently skipped; baseline is unchanged.
- **`where=` filter excludes the event:** no update; the z-score reported is for the most recent matching event, which may be older than the most recent inserted event.
- **Missing `half_life=`:** raises `ValueError` at SDK-helper-call time.
- **`half_life="forever"`:** rejected by `_validate_half_life` with `ValueError` — use [`bv.z_score`](../velocity/z_score.md) for the lifetime / non-decay variant.
- **`half_life="0…"`:** rejected at SDK call time; server returns structured error `aggregation_invalid_half_life` if reached.
- **Cold-entity eviction (`@bv.event(cold_after=...)`):** drops the baseline.

## See also

- [Decay family index](./index.md) — overview of all 6 decay-family ops
- [cost-class.md](../cost-class.md) — performance tier (Tier 1)
- [bv.ewma](./ewma.md) — the EWMA component of the baseline
- [bv.ewvar](./ewvar.md) — the EWVar component of the baseline
- [bv.z_score](../velocity/z_score.md) — entity z-score against a non-decay rolling Welford mean / stddev (use this when you want lifetime statistics rather than a recency-weighted baseline)
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
