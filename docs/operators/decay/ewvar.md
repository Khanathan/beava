# bv.ewvar

> Exponentially-weighted variance over arrival-time, with `half_life`-controlled decay.

## Signature

```python
bv.ewvar(
    field: str,
    *,
    half_life: str,
    where: bv.Col | None = None,
) -> AggDescriptor
```

## Description

`bv.ewvar` is the companion second-moment estimator to
[`bv.ewma`](./ewma.md): the exponentially-weighted variance of a numeric
field, where the influence of older observations decays exponentially
with arrival age. Conceptually, beava maintains an EW-mean and an
EW-mean-of-squares with the same decay coefficient
`α = 1 - 0.5^(Δt / half_life)`, then reports `EW[x²] - EW[x]²` as the
variance estimate. `Δt` is the **server processing-time** gap (`now_ms()`
between consecutive matching events) per
[`project_redis_shaped_no_event_time_ever`](../../../.planning/PROJECT.md).

`half_life` is the time after which an observation's contribution to the
variance has decayed to ½. Use `bv.ewvar` when you want a smoothed
running variance that adapts to drift faster than a long fixed-window
variance would. The classic application is anomaly scoring — pair it
with [`bv.ew_zscore`](./ew_zscore.md) (which divides the current event's
deviation by `sqrt(ewvar)`) to flag events that look big relative to
recent volatility, not just relative to all-time volatility.

`bv.ewvar` belongs to the **decay** family. Per-event update is one
`exp()` call plus five scalar multiply-adds (EW-Welford form); cost is
**Tier 1** (~18 ns algorithm floor / ~38 ns measured) and memory is
`O(1)` per entity. Lifetime mode is the only mode — `half_life` sets
the decay rate, no `window=` kwarg exists.

## Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `field` | `str` | Yes | — | Numeric field (`i64` or `f64`) to track. |
| `half_life` | `str` | Yes | — | Duration string matching `\d+(ms\|s\|m\|h\|d)`. Must be positive; `"forever"` is **rejected**. |
| `where` | `bv.Col` | No | `None` | Boolean expression on event fields; only matching events update the EWVar. |

## Returns

A single `f64` — the current EW-variance estimate. Cold-start (no
matching events seen) returns `null` (Python `None`). After exactly one
matching event the variance is `0.0` (no spread yet observed).

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | **Tier 1** (~18 ns floor / ~38 ns measured) — see [cost-class.md](../cost-class.md#tier-1-fast-40-nscall--38-ops) |
| Memory per entity | `O(1)` — EW-mean + EW-mean-of-squares + `last_now_ms` ≈ 32 B |
| Lifetime mode | **Required** — no `window=` kwarg; `half_life` controls decay rate |

## Examples

### Example 1: EW-variance of transaction amount per user, 1h half-life

```python
import beava as bv

@bv.event
class Txn:
    user_id: str
    amount: float

@bv.table(key="user_id")
def UserAmtVolatility(txns) -> bv.Table:
    return (
        txns.group_by("user_id")
            .agg(amt_ewvar_1h=bv.ewvar("amount", half_life="1h"))
    )

# Push events
app.push("Txn", {"user_id": "alice", "amount": 100.0})
app.push("Txn", {"user_id": "alice", "amount": 200.0})
app.push("Txn", {"user_id": "alice", "amount": 50.0})

# Query
result = app.get("UserAmtVolatility", "alice")
# result == {"amt_ewvar_1h": <positive float, EW-variance estimate>}
```

### Example 2: EW-variance of approved-payment latency, 30m half-life

```python
@bv.table(key="user_id")
def UserOkLatencyVar(txns) -> bv.Table:
    return (
        txns.group_by("user_id")
            .agg(latency_ewvar=bv.ewvar("latency_ms",
                                          half_life="30m",
                                          where=bv.col("status") == "ok"))
    )
```

## Wire

JSON wire form in a register payload:

```json
{
  "kind": "derivation",
  "name": "UserAmtVolatility",
  "output_kind": "table",
  "key": ["user_id"],
  "agg": {
    "amt_ewvar_1h": {
      "op": "ewvar",
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

- **Empty stream / cold-start:** result is `null`. The first matching event seeds the EW-mean and flips the `initialized` flag; variance is reported as `0.0`.
- **Single matching event:** variance is `0.0` (no spread yet observed). Two or more events are needed for a meaningful estimate.
- **Late or duplicate event (Δt ≤ 0):** the helper applies an unweighted blend (treats the event as same-instant) and does **not** advance `last_now_ms`.
- **Missing or non-numeric `field`:** the event is silently skipped; EWVar is unchanged.
- **`where=` filter excludes the event:** no update.
- **Missing `half_life=`:** raises `ValueError` at SDK-helper-call time.
- **`half_life="forever"`:** rejected by `_validate_half_life` with `ValueError` — use [`bv.var`](../core/var.md) for fixed-window variance.
- **`half_life="0…"`:** rejected at SDK call time (regex requires `[1-9]\d*`); server returns structured error `aggregation_invalid_half_life` if reached.
- **Cold-entity eviction (`@bv.event(cold_after=...)`):** drops the underlying state.

## See also

- [Decay family index](./index.md) — overview of all 6 decay-family ops
- [cost-class.md](../cost-class.md) — performance tier (Tier 1)
- [bv.ewma](./ewma.md) — companion exponentially-weighted moving average (the EW first-moment)
- [bv.ew_zscore](./ew_zscore.md) — current-event z-score against EWMA / EWVar baseline (the typical consumer of this op)
- [bv.var](../core/var.md) — fixed-window arithmetic variance (no decay; pick this when window is fixed)
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
