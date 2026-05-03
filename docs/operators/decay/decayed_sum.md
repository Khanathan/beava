# bv.decayed_sum

> Forward-decay sum à la Cormode (2009) — running total where older contributions decay exponentially with arrival age.

## Signature

```python
bv.decayed_sum(
    field: str,
    *,
    half_life: str,
    where: bv.Col | None = None,
) -> AggDescriptor
```

## Description

`bv.decayed_sum` maintains a running sum where each new observation
contributes its full value, but every prior contribution decays
exponentially with arrival age. On each matching event the running
total is updated as
`total_t = x_t + total_{t-1} * 0.5^(Δt / half_life)`
(equivalently `x_t + total_{t-1} * exp(-Δt * ln(2) / half_life)`).
`Δt` is the **server processing-time** gap (`now_ms()` between
consecutive matching events) per
[`project_redis_shaped_no_event_time_ever`](../../../.planning/PROJECT.md);
beava intentionally has no event-time concept.

This is the **Cormode forward-decay** primitive — useful when you want a
"recency-weighted total" that converges to a stable steady-state value
over many events rather than growing without bound the way `bv.sum`
does. For a roughly constant arrival rate `r` and value `v`, the
steady-state is `r * v * half_life / ln(2)` (the geometric series of
decayed contributions). The shape is "running fuel gauge": each event
tops the tank up by `x`, and elapsed time bleeds it down. Pick a
`half_life` equal to the timescale of the spending / activity behaviour
you want to capture — `bv.decayed_sum("amount", half_life="1h")`
roughly answers "how much has this user spent in the last hour or so,
weighted toward the present".

`bv.decayed_sum` belongs to the **decay** family. Per-event update is
one `exp()` call plus two scalar operations; cost is **Tier 1**
(~15 ns algorithm floor / ~35 ns measured) and memory is `O(1)` per
entity. Lifetime mode is the only mode — `half_life` sets the decay
rate, no `window=` kwarg exists.

## Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `field` | `str` | Yes | — | Numeric field (`i64` or `f64`) to accumulate. |
| `half_life` | `str` | Yes | — | Duration string matching `\d+(ms\|s\|m\|h\|d)`. Must be positive; `"forever"` is **rejected** (use [`bv.sum`](../core/sum.md) with `window="forever"` for an undecayed lifetime sum). |
| `where` | `bv.Col` | No | `None` | Boolean expression on event fields; only matching events contribute to the sum. |

## Returns

A single `f64` — the current decayed sum. Cold-start (no matching events
seen) returns `null` (Python `None`). For a roughly constant arrival
rate the value asymptotes near `r * v * half_life / ln(2)`.

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | **Tier 1** (~15 ns floor / ~35 ns measured) — see [cost-class.md](../cost-class.md#tier-1-fast-40-nscall--38-ops) |
| Memory per entity | `O(1)` — `(total: f64, last_now_ms: i64, initialized: bool)` ≈ 24 B |
| Lifetime mode | **Required** — no `window=` kwarg; `half_life` controls decay rate |

## Examples

### Example 1: Decayed spend per user, 1h half-life

```python
import beava as bv

@bv.event
class Txn:
    user_id: str
    amount: float

@bv.table(key="user_id")
def UserDecayedSpend(txns) -> bv.Table:
    return (
        txns.group_by("user_id")
            .agg(spend_decay_1h=bv.decayed_sum("amount", half_life="1h"))
    )

# Push events
app.push("Txn", {"user_id": "alice", "amount": 100.0})
# 30 minutes pass...
app.push("Txn", {"user_id": "alice", "amount": 50.0})
# Decayed total ≈ 100 * 0.5^0.5 + 50 ≈ 70.7 + 50 = 120.7

result = app.get("UserDecayedSpend", "alice")
# result == {"spend_decay_1h": ~120.7}
```

### Example 2: Decayed sum of approved fraud-score contributions, 5m half-life

```python
@bv.table(key="user_id")
def UserHotnessScore(events) -> bv.Table:
    return (
        events.group_by("user_id")
              .agg(hotness=bv.decayed_sum("risk_delta",
                                            half_life="5m",
                                            where=bv.col("approved") == True))
    )
```

## Wire

JSON wire form in a register payload:

```json
{
  "kind": "derivation",
  "name": "UserDecayedSpend",
  "output_kind": "table",
  "key": ["user_id"],
  "agg": {
    "spend_decay_1h": {
      "op": "decayed_sum",
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

- **Empty stream / cold-start:** result is `null`. The first matching event seeds `total = x`.
- **Long quiescent periods:** the running total decays toward `0` even with no new events, but the value reported by `app.get(...)` is **not** decayed forward to query time — it is the value as of the last matching event. (This is by design: querying does not mutate state, and the `Δt` from the last event to "now" is captured at the next matching event, not at every read.)
- **Late or duplicate event (Δt ≤ 0):** the helper applies an unweighted addition (`total += x`) and does **not** advance `last_now_ms`.
- **Missing or non-numeric `field`:** the event is silently skipped.
- **`where=` filter excludes the event:** no update.
- **Missing `half_life=`:** raises `ValueError` at SDK-helper-call time.
- **`half_life="forever"`:** rejected by `_validate_half_life` with `ValueError` — for an undecayed running total, use [`bv.sum(field, window="forever")`](../core/sum.md).
- **`half_life="0…"`:** rejected at SDK call time; server returns structured error `aggregation_invalid_half_life` if reached.
- **Cold-entity eviction (`@bv.event(cold_after=...)`):** drops the underlying state.
- **Negative values in `field`:** allowed; the running total can be negative (e.g. credits and debits).

## See also

- [Decay family index](./index.md) — overview of all 6 decay-family ops
- [cost-class.md](../cost-class.md) — performance tier (Tier 1)
- [bv.decayed_count](./decayed_count.md) — same primitive without a field — answers "how active recently?"
- [bv.sum](../core/sum.md) — fixed-window or lifetime undecayed sum (pick when you want hard-edged totals, not decayed ones)
- [bv.ewma](./ewma.md) — exponentially-weighted **mean** (forward-decay average rather than forward-decay total)
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
