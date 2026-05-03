# ADR-002: Rename core aggregation ops to Polars conventions

## Status

Accepted
Date: 2026-05-03

## Context

Beava's operator catalogue originally chose op names that read naturally in
SQL prose: `avg`, `variance`, `stddev`, `count_distinct`, `percentile`. These
names are unambiguous and read well in feature definitions written by analysts
who think in SQL terms.

The 2026-05-03 v0-launch design session locked Polars-style chained syntax as
the canonical Python API. Polars uses different names for the same operations:
`mean`, `var`, `std`, `n_unique`, `quantile`. The Polars naming convention is
documented, well-known to data-engineering audiences, and consistent with
pandas / dplyr / sqlglot / NumPy. Mixing the two name families in one codebase
(Polars-style chains calling beava-named ops) creates persistent friction at
the language boundary — every chain becomes a context-switch between the
chain syntax (Polars) and the operator name (SQL prose).

The current names are baked into:

- `crates/beava-core/src/agg_compile.rs::parse_agg_kind` — op-string → AggKind
  enum mapping
- `crates/beava-core/src/register_validate.rs::lifetime_bound_for_op_str` —
  op-string → lifetime-bound classification
- `python/beava/_agg.py` — helper function names exposed in the public `bv.*`
  namespace
- `crates/beava-bench/configs/{small,medium,large,fraud-team}.json` — bench
  pipeline definitions
- `docs/operators/cost-class.md` — the alive Phase 19.2 cost-class metadata
- approximately 60 test fixtures across the workspace

`FORMAT_VERSION` for WAL + snapshot stays at **1** (the rename does not change
the binary record format — only string constants in JSON-shaped op fields).
Snapshot bodies (`SNAPSHOT_BODY_FORMAT_VERSION = 1`), WAL records
(`FORMAT_VERSION = 1`), and the Phase 12.7 schema reset all carry through this
rename unchanged.

## Decision

Rename five core operators across server, SDK, and config artifacts:

| Old name | New name | Rationale |
|----------|----------|-----------|
| `avg` | `mean` | Polars / NumPy / pandas convention |
| `variance` | `var` | Polars convention |
| `stddev` | `std` | Polars convention |
| `count_distinct` | `n_unique` | Polars convention |
| `percentile` | `quantile` | Polars convention |

`bv.ema` stays as an alias for `bv.ewma` (existing convention; `bv.ewma` is
the canonical name in both beava and Polars-streaming idioms).

Implementation roadmap:

1. **Phase 13.4** lands the server-side rename. `parse_agg_kind` accepts the
   new names. `AggKind` Rust enum variants stay unchanged (`AggKind::Avg`,
   `AggKind::Variance`, `AggKind::Stddev`, `AggKind::CountDistinct`,
   `AggKind::Percentile`) — only the public string mapping in JSON changes.
   This minimizes Rust diff scope while flipping the wire contract.
2. **Phase 13.4** also updates the four `crates/beava-bench/configs/*.json`
   files (small / medium / large / fraud-team) and `docs/operators/cost-class.md`
   to use the new names, keeping benchmarks runnable through the rename.
3. **Phase 13.5** lands the Python SDK rename in `python/beava/_agg.py`. New
   helper function names: `bv.mean / bv.var / bv.std / bv.n_unique / bv.quantile`.
   Old names (`bv.avg / bv.variance / bv.stddev / bv.count_distinct / bv.percentile`)
   remain as **deprecation aliases** that log a `DeprecationWarning` and
   dispatch to the new helper. The aliases ship in v0 and are removed in v0.1.
4. **Phase 13.6** ships TypeScript + Go SDKs with the new names from day one
   (no legacy aliases — v0 is unreleased; TS/Go users have no migration burden).
5. **Phase 13.0** (this phase) per-op pages cite **both** names. The H1 + the
   Python signature use the NEW name. A "Previously called `<old>`" line in
   the Edge Cases section preserves searchability for users who inherited
   pre-v0 pipelines or arrived from beava-v1 documentation. Plans 13.0-05
   through 13.0-11 author the per-op pages.

`FORMAT_VERSION` stays at 1 throughout — the rename is a string-constant change
in JSON-shaped op fields, not a binary record format change.

## Consequences

**Easier:**

- Polars-experienced users transition immediately; signatures match what they
  already know. `bv.col(...).mean()` reads naturally in chains (no surprise
  from `bv.col(...).avg()`).
- Cross-language SDK consistency: TS `pipeline.col("x").mean().over("1h")`
  matches Python `bv.col("x").mean().over("1h")` matches Go's equivalent.
  No per-language alias proliferation.
- Documentation cohesion — one op name per concept across docs, blog posts,
  and external tutorials. SEO-friendly: each op has a single canonical landing
  page on beava.dev (the per-op page Plan 13.0-05..11 authors).

**Harder:**

- One-time refactor across server (Rust) + Python SDK + 4 bench configs +
  approximately 60 test fixtures. Mechanical but multi-touchpoint — Phase 13.4
  + 13.5 own the implementation; Phase 13.0 only documents.
- Pre-13.0 dev users (small group, no production deploys yet) need to rename
  in their local pipelines. Mitigation: Python SDK keeps deprecation aliases
  for the v0 minor (v0.0.x); v0.1 removes them entirely. Wire JSON remains
  flexible at the server boundary too — `parse_agg_kind` could accept both
  forms in v0 if a smoother migration is needed (Phase 13.4 decides).

**Follow-on actions:**

- **Phase 13.4 Plan:** server-side rename in `parse_agg_kind` +
  `lifetime_bound_for_op_str` + bench-config update + `docs/operators/cost-class.md`
  string update. Unit tests assert the new names parse and the old names are
  rejected (or accepted as wire-aliases — Phase 13.4 picks).
- **Phase 13.5 Plan:** Python SDK rename in `_agg.py` + deprecation aliases
  with `DeprecationWarning` log lines. Tests assert both names work in v0;
  v0.1 plan removes aliases.
- **Phase 13.6 Plans:** TS + Go SDKs use the new names from day one — no
  legacy aliases. Wire JSON parity with Python is the contract.
- **Phase 13.0 (this phase) per-op pages** (Plans 13.0-05 through 13.0-11)
  document the new names as the H1 + signature; old names referenced once in
  Edge Cases for searchability.
- **CLAUDE.md** may reference ADR-002 as the Polars-naming locked decision
  (Plan 13.0-15 closure may add the footnote).
- **fraud-team.json** weighted-aggregation declarations get updated alongside
  the bench-config rename in Phase 13.4 (shared mechanical step).
