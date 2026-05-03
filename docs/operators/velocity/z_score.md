# bv.z_score

> Entity-level z-score against rolling mean/stddev baseline.

## Signature

```python
bv.z_score(...) -> AggDescriptor
```


## Description

> TODO (Plan 13.0-09): 2-3 paragraphs describing what this op
> computes, mathematically and informally. When to use it. What category
> it belongs to.

## Parameters

> TODO (Plan 13.0-09): table with Name | Type | Required | Default | Description.

## Returns

> TODO (Plan 13.0-09): output type and shape (scalar / list / dict / windowed).

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | TODO Tier 1/2/3 — see [cost-class.md](../cost-class.md) |
| Memory per entity | O1 |
| Lifetime mode | TODO Allowed / Required-kwarg / Forbidden |

## Examples

> TODO (Plan 13.0-09): 1-2 worked Python examples + JSON wire form.

## Wire

JSON wire form (in a register payload):

```json
{
  "kind": "derivation",
  "name": "<Name>",
  "output_kind": "table",
  "key": ["<key>"],
  "agg": {
    "<feature>": {
      "op": "z_score",
      "params": {}
    }
  }
}
```

See [examples/wire/register-fraud-team.request.json](../../../examples/wire/register-fraud-team.request.json) for a full payload example.

## Edge cases

> TODO (Plan 13.0-09): empty stream, NaN inputs, lifetime mode,
> structured-error code if applicable.

## See also

- [cost-class.md](../cost-class.md) — performance tier
- TODO related ops in same family
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
