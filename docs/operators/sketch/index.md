# Sketch Aggregation Operators

The 5 sketch ops cover approximate-cardinality, quantile, frequency, set-membership, and entropy estimation. All use bounded data structures regardless of stream length.

| Op | Memory class | CPU tier |
|----|--------------|----------|
| [`bv.n_unique`](./n_unique.md) | BoundedSketch (HLL) | Tier 2/3 |
| [`bv.quantile`](./quantile.md) | BoundedSketch (DDSketch) | Tier 2/3 |
| [`bv.top_k`](./top_k.md) | BoundedByConfig("k") | Tier 2 |
| [`bv.bloom_member`](./bloom_member.md) | BoundedSketch (Bloom) | Tier 1 |
| [`bv.entropy`](./entropy.md) | BoundedByConfig("max_categories") | Tier 2 |

Two of the five (`n_unique`, `quantile`) were renamed per [ADR-002](../../../.planning/decisions/ADR-002-polars-op-rename.md) for Polars-convention consistency.

## See also

- [Operator catalog index](../index.md) — full 53-op catalogue
- [cost-class.md](../cost-class.md) — per-op CPU tier metadata (Tier 1/2/3)
- Per-operator memory governance: [V0-MEM-GOV-02](../../../.planning/REQUIREMENTS.md) — every lifetime aggregation operator declares a finite per-entity memory ceiling at register-time
- [Pipeline DSL compilation rules](../../pipeline-dsl/compilation-rules.md) — how `bv.<op>(...)` calls compile to JSON wire form
