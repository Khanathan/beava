# bv.decayed_count

> Forward-decay event count à la Cormode (2009) — running event count where older contributions decay exponentially with arrival age.

## Signature

```python
bv.decayed_count(
    *,
    half_life: str,
    where: bv.Col | None = None,
) -> AggDescriptor
```

## Description

`bv.decayed_count` is [`bv.decayed_sum`](./decayed_sum.md) with each
event contributing `1` instead of a field value: a running event count
where every event's contribution decays exponentially with arrival age.
On each matching event the running count is updated as
`count_t = 1 + count_{t-1} * 0.5^(Δt / half_life)`. `Δt` is the
**server processing-time** gap (`now_ms()` between consecutive matching
events) per
[`project_redis_shaped_no_event_time_ever`](../../../.planning/PROJECT.md);
beava intentionally has no event-time concept.

This is the canonical "recent-activity rate" primitive — useful when
you want a single scalar that reflects how busy an entity has been
recently, without committing to a hard window. For a roughly constant
arrival rate `r`, the steady-state value is `r * half_life / ln(2)`,
which makes the half-life directly interpretable: events per `half_life
/ ln(2)` units of time. Compared to a fixed-window
`bv.count(window="1h")`, decayed_count weights more recent events more
heavily and degrades smoothly when the entity goes quiet, instead of
dropping discontinuously at the window boundary.

`bv.decayed_count` belongs to the **decay** family. It takes **no**
`field` argument — every matching event contributes `1`. Per-event
update is one `exp()` call plus one scalar add; cost is **Tier 1**
(~12 ns algorithm floor / ~32 ns measured) and memory is `O(1)` per
entity. Lifetime mode is the only mode — `half_life` sets the decay
rate, no `window=` kwarg exists.

## Parameters

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `half_life` | `str` | Yes | — | Duration string matching `\d+(ms\|s\|m\|h\|d)`. Must be positive; `"forever"` is **rejected** (use [`bv.count(window="forever")`](../core/count.md) for an undecayed lifetime count). |
| `where` | `bv.Col` | No | `None` | Boolean expression on event fields; only matching events contribute. |

## Returns

A single `f64` — the current decayed count. Cold-start (no matching
events seen) returns `null` (Python `None`). For a roughly constant
arrival rate the value asymptotes near `r * half_life / ln(2)`.

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | **Tier 1** (~12 ns floor / ~32 ns measured) — see [cost-class.md](../cost-class.md#tier-1-fast-40-nscall--38-ops) |
| Memory per entity | `O(1)` — `(count: f64, last_now_ms: i64, initialized: bool)` ≈ 24 B |
| Lifetime mode | **Required** — no `window=` kwarg; `half_life` controls decay rate |

## Examples

### Example 1: Recent-activity rate per user, 5m half-life

```python
import beava as bv

@bv.event
class Click:
    user_id: str

@bv.table(key="user_id")
def UserActivityRate(clicks) -> bv.Table:
    return (
        clicks.group_by("user_id")
              .agg(activity_5m=bv.decayed_count(half_life="5m"))
    )

# Push events in a burst
for _ in range(10):
    app.push("Click", {"user_id": "alice"})

# Steady-state count for a sustained 10/min rate at 5m half-life:
# value ≈ rate * half_life / ln(2) = (10/60s) * 300s / 0.693 ≈ 72
result = app.get("UserActivityRate", "alice")
# result == {"activity_5m": <float, ramping toward steady-state>}
```

### Example 2: Decayed count of failed-login attempts, 10m half-life

```python
@bv.table(key="user_id")
def UserRecentFails(logins) -> bv.Table:
    return (
        logins.group_by("user_id")
              .agg(recent_fails=bv.decayed_count(
                       half_life="10m",
                       where=bv.col("status") == "failed"))
    )
```

## Wire

JSON wire form in a register payload:

```json
{
  "kind": "derivation",
  "name": "UserActivityRate",
  "output_kind": "table",
  "key": ["user_id"],
  "agg": {
    "activity_5m": {
      "op": "decayed_count",
      "params": {
        "half_life": "5m"
      }
    }
  }
}
```

See [examples/wire/register-fraud-team.request.json](../../../examples/wire/register-fraud-team.request.json) for a full payload example.

## Edge cases

- **Empty stream / cold-start:** result is `null`. The first matching event seeds `count = 1`.
- **Long quiescent periods:** the running count decays toward `0` even with no new events, but the value reported by `app.get(...)` is **not** decayed forward to query time — it is the value as of the last matching event.
- **Late or duplicate event (Δt ≤ 0):** the helper applies an unweighted increment (`count += 1`) and does **not** advance `last_now_ms`.
- **`where=` filter excludes the event:** no update.
- **No `field` argument:** correct — `decayed_count` is field-less by design (counts events, not values). Passing a positional argument raises `TypeError` at SDK-helper-call time.
- **Missing `half_life=`:** raises `ValueError` at SDK-helper-call time.
- **`half_life="forever"`:** rejected by `_validate_half_life` with `ValueError` — for an undecayed lifetime count, use [`bv.count(window="forever")`](../core/count.md).
- **`half_life="0…"`:** rejected at SDK call time; server returns structured error `aggregation_invalid_half_life` if reached.
- **Cold-entity eviction (`@bv.event(cold_after=...)`):** drops the underlying state.

## See also

- [Decay family index](./index.md) — overview of all 6 decay-family ops
- [cost-class.md](../cost-class.md) — performance tier (Tier 1; the cheapest decay op)
- [bv.decayed_sum](./decayed_sum.md) — same primitive but adds a numeric field instead of `1`
- [bv.count](../core/count.md) — fixed-window or lifetime undecayed event count
- [bv.ewma](./ewma.md) — exponentially-weighted mean (numerator-decay-only variant of decayed_count would not be useful — pick this for averaged signals)
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
