#!/usr/bin/env python3
"""
Phase 13.0 Plan 05 — operator-catalog scaffold script.

Reads (for context only — paths cited in this header for traceability):
- python/beava/_agg.py                       (existing helper signatures, source for D-02 polish)
- crates/beava-core/src/agg_op.rs            (53 unique AggKind variants, source of truth)
- crates/beava-core/src/register_validate.rs (lifetime_bound_for_op_str classification)
- docs/operators/cost-class.md               (CPU tier classification, alive Phase 19.2)

Writes:
- docs/operators/<family>/<op>.md  (54 stub files)
- docs/operators/index.md           (master 54-row catalog)

54 page paths = 53 unique AggKind variants + ema alias documented INSIDE ewma.md
"## Aliases" section (ema is NOT a separate page — it's an alias of the ewma
AggKind variant per the Python SDK convention `ema = ewma`).

Family layout (matches RESEARCH §3 directory tree, RESEARCH §5 op classification,
and CONTEXT D-02):
- core/         (8): count, sum, mean, min, max, var, std, ratio
- sketch/       (5): n_unique, quantile, top_k, bloom_member, entropy
- point-ordinal/(5): first, last, first_n, last_n, lag
- recency/     (10): first_seen, last_seen, age, has_seen, time_since,
                     time_since_last_n, streak, max_streak, negative_streak,
                     first_seen_in_window
- decay/        (6): ewma (with ema alias inline), ewvar, ew_zscore,
                     decayed_sum, decayed_count, twa
- velocity/     (9): rate_of_change, inter_arrival_stats, burst_count,
                     delta_from_prev, trend, trend_residual, outlier_count,
                     value_change_count, z_score
- buffer-geo/  (11): histogram, hour_of_day_histogram, dow_hour_histogram,
                     seasonal_deviation, event_type_mix, most_recent_n,
                     reservoir_sample, geo_velocity, geo_distance, geo_spread,
                     distance_from_home

rate_of_change.md lives under velocity/ per RESEARCH §3 + RESEARCH §5
(Phase 9 velocity family classification), NOT under decay/.

ADR-002 honored: renamed ops (avg→mean / variance→var / stddev→std /
count_distinct→n_unique / percentile→quantile) use the NEW name as filename + H1;
each renamed page has a "Previously called bv.<old>" note pointing at ADR-002.

Idempotent — re-running OVERWRITES existing stubs. Polish plans (13.0-06..11)
should NOT re-run the scaffold; they Edit individual files in place. The Aliases
block append for ewma.md is also idempotent (skipped if already present).

Usage: python3 scripts/scaffold_op_pages.py
"""
from __future__ import annotations

import pathlib
import sys

# 54-path canonical inventory: (family, op_name, polars_renamed_from, brief_description)
# polars_renamed_from is None unless ADR-002 renamed; otherwise the old name as a string.
# rate_of_change lives under velocity/ per RESEARCH §3 directory tree + RESEARCH §5
# op classification (Phase 9 velocity family).
OPS = [
    # core/ (8)
    ("core", "count", None, "Event count over a window or lifetime."),
    ("core", "sum", None, "Sum of a numeric field."),
    ("core", "mean", "avg", "Arithmetic mean of a numeric field."),
    ("core", "min", None, "Minimum value of a numeric field."),
    ("core", "max", None, "Maximum value of a numeric field."),
    ("core", "var", "variance", "Sample variance via Welford."),
    ("core", "std", "stddev", "Standard deviation (sqrt of variance)."),
    ("core", "ratio", None, "Count matching predicate divided by total count."),
    # sketch/ (5)
    ("sketch", "n_unique", "count_distinct", "HLL cardinality estimate."),
    ("sketch", "quantile", "percentile", "DDSketch-backed quantile estimator."),
    ("sketch", "top_k", None, "SpaceSaving top-K most-frequent values."),
    ("sketch", "bloom_member", None, "Bloom-filter ever-seen membership test."),
    ("sketch", "entropy", None, "Shannon entropy over categorical distribution."),
    # point-ordinal/ (5)
    ("point-ordinal", "first", None, "First observed value."),
    ("point-ordinal", "last", None, "Most recent value by arrival order."),
    ("point-ordinal", "first_n", None, "First N values."),
    ("point-ordinal", "last_n", None, "Last N values."),
    ("point-ordinal", "lag", None, "Value n events ago."),
    # recency/ (10)
    ("recency", "first_seen", None, "First-seen server arrival timestamp."),
    ("recency", "last_seen", None, "Last-seen server arrival timestamp."),
    ("recency", "age", None, "Milliseconds since first_seen."),
    ("recency", "has_seen", None, "Boolean ever-matched predicate."),
    ("recency", "time_since", None, "Milliseconds since last matching event."),
    ("recency", "time_since_last_n", None, "Milliseconds since kth most recent matching event."),
    ("recency", "streak", None, "Length of current consecutive matching streak."),
    ("recency", "max_streak", None, "Longest streak length ever observed."),
    ("recency", "negative_streak", None, "Length of current consecutive non-matching streak."),
    ("recency", "first_seen_in_window", None, "Bloom + timestamp: is this value new in window N?"),
    # decay/ (6) — ema alias is documented INSIDE ewma.md (not a separate page)
    ("decay", "ewma", None, "Exponentially-weighted moving average."),
    ("decay", "ewvar", None, "Exponentially-weighted variance."),
    ("decay", "ew_zscore", None, "Current event z-score against EWMA/EWVar baseline."),
    ("decay", "decayed_sum", None, "Forward-decay sum (Cormode)."),
    ("decay", "decayed_count", None, "Forward-decay count."),
    ("decay", "twa", None, "Time-weighted average."),
    # velocity/ (9) — rate_of_change lives here per RESEARCH §3 + §5
    ("velocity", "rate_of_change", None, "Rate or acceleration delta across two adjacent windows."),
    ("velocity", "inter_arrival_stats", None, "Mean / stddev / CV of gaps between matching events."),
    ("velocity", "burst_count", None, "Max events in any sub-window inside outer window."),
    ("velocity", "delta_from_prev", None, "Current value minus previous event value."),
    ("velocity", "trend", None, "Slope of EW linear regression."),
    ("velocity", "trend_residual", None, "Current value minus trend-predicted value."),
    ("velocity", "outlier_count", None, "Count of events beyond N-sigma in window."),
    ("velocity", "value_change_count", None, "Count of field value flips."),
    ("velocity", "z_score", None, "Entity-level z-score against rolling mean/stddev baseline."),
    # buffer-geo/ (11)
    ("buffer-geo", "histogram", None, "Count per fixed bucket."),
    ("buffer-geo", "hour_of_day_histogram", None, "24-bin count histogram per entity."),
    ("buffer-geo", "dow_hour_histogram", None, "168-bin (day x hour) histogram per entity."),
    ("buffer-geo", "seasonal_deviation", None, "Z-score against this entity's hour-of-day baseline."),
    ("buffer-geo", "event_type_mix", None, "Proportion per category over window."),
    ("buffer-geo", "most_recent_n", None, "Deque of N most-recent values."),
    ("buffer-geo", "reservoir_sample", None, "Uniform K-sample over all history."),
    ("buffer-geo", "geo_velocity", None, "Max implied km/h between consecutive events."),
    ("buffer-geo", "geo_distance", None, "Total path length in window."),
    ("buffer-geo", "geo_spread", None, "Max distance from mean center."),
    ("buffer-geo", "distance_from_home", None, "Distance from running centroid of top-K frequent locations."),
]

# 54 entries total: 8+5+5+10+6+9+11 = 54 unique paths.
# 53 unique AggKind variants (ema = ewma kind alias, documented inline in ewma.md).

# Memory bound classification per op (mirrors register_validate.rs::lifetime_bound_for_op_str).
# Hardcoded mirror; check_op_page_coverage.py asserts paths stay in sync with the engine.
LIFETIME_BOUND = {
    # core: all O1
    "count": "O1", "sum": "O1", "mean": "O1", "min": "O1", "max": "O1",
    "var": "O1", "std": "O1", "ratio": "O1",
    # sketch
    "n_unique": "BoundedSketch",
    "quantile": "BoundedSketch",
    "bloom_member": "BoundedSketch",
    "top_k": 'BoundedByConfig("k", 10)',
    "entropy": 'BoundedByConfig("max_categories", 256)',
    # point-ordinal
    "first": "O1", "last": "O1",
    "first_n": 'BoundedByRequiredKwarg("n")',
    "last_n": 'BoundedByRequiredKwarg("n")',
    "lag": 'BoundedByRequiredKwarg("n")',
    # recency: all O1 except time_since_last_n
    "first_seen": "O1", "last_seen": "O1", "age": "O1", "has_seen": "O1",
    "time_since": "O1",
    "time_since_last_n": 'BoundedByRequiredKwarg("n")',
    "streak": "O1", "max_streak": "O1", "negative_streak": "O1",
    "first_seen_in_window": "O1",
    # decay: all O1
    "ewma": "O1", "ewvar": "O1", "ew_zscore": "O1",
    "decayed_sum": "O1", "decayed_count": "O1", "twa": "O1",
    # velocity: all O1 (rate_of_change lives here per RESEARCH §3)
    "rate_of_change": "O1",
    "inter_arrival_stats": "O1", "burst_count": "O1", "delta_from_prev": "O1",
    "trend": "O1", "trend_residual": "O1", "outlier_count": "O1",
    "value_change_count": "O1", "z_score": "O1",
    # buffer
    "histogram": 'BoundedByRequiredKwarg("buckets")',
    "hour_of_day_histogram": "O1", "dow_hour_histogram": "O1",
    "seasonal_deviation": "O1",
    "event_type_mix": 'BoundedByConfig("max_categories", 256)',
    "most_recent_n": 'BoundedByRequiredKwarg("n")',
    "reservoir_sample": 'BoundedByRequiredKwarg("samples")',
    # geo
    "geo_velocity": "O1", "geo_distance": "O1", "geo_spread": "O1",
    "distance_from_home": 'BoundedByConfig("samples", 100)',
}

# Polish-plan mapping (which Plan 13.0-NN polishes which family/op).
# Wave 2 polish split: 06=core+sketch, 07=point-ordinal+recency, 08=decay,
# 09=velocity (incl. rate_of_change), 10=buffer (7 ops), 11=geo (4 ops).
POLISH_PLAN = {
    "core": "06", "sketch": "06",
    "point-ordinal": "07", "recency": "07",
    "decay": "08",
    "velocity": "09",
}
GEO_OPS = {"geo_velocity", "geo_distance", "geo_spread", "distance_from_home"}


def polish_plan_for(family: str, op: str) -> str:
    """Map (family, op) -> the 13.0-NN plan that fills in prose."""
    if family == "buffer-geo" and op in GEO_OPS:
        return "11"
    if family == "buffer-geo":
        return "10"
    return POLISH_PLAN[family]


# Stub template — 9 mandatory sections per RESEARCH §3 + CONTEXT D-02.
STUB = """# bv.{op}

> {brief}

## Signature

```python
bv.{op}(...) -> AggDescriptor
```
{rename_note}

## Description

> TODO (Plan 13.0-{polish_plan}): 2-3 paragraphs describing what this op
> computes, mathematically and informally. When to use it. What category
> it belongs to.

## Parameters

> TODO (Plan 13.0-{polish_plan}): table with Name | Type | Required | Default | Description.

## Returns

> TODO (Plan 13.0-{polish_plan}): output type and shape (scalar / list / dict / windowed).

## Complexity

| Resource | Bound |
|----------|-------|
| CPU per event | TODO Tier 1/2/3 — see [cost-class.md](../cost-class.md) |
| Memory per entity | {bound} |
| Lifetime mode | TODO Allowed / Required-kwarg / Forbidden |

## Examples

> TODO (Plan 13.0-{polish_plan}): 1-2 worked Python examples + JSON wire form.

## Wire

JSON wire form (in a register payload):

```json
{{
  "kind": "derivation",
  "name": "<Name>",
  "output_kind": "table",
  "key": ["<key>"],
  "agg": {{
    "<feature>": {{
      "op": "{op}",
      "params": {{}}
    }}
  }}
}}
```

See [examples/wire/register-fraud-team.request.json](../../../examples/wire/register-fraud-team.request.json) for a full payload example.

## Edge cases

> TODO (Plan 13.0-{polish_plan}): empty stream, NaN inputs, lifetime mode,
> structured-error code if applicable.

## See also

- [cost-class.md](../cost-class.md) — performance tier
- TODO related ops in same family
- [pipeline-dsl/compilation-rules.md](../../pipeline-dsl/compilation-rules.md) — chain compilation rules
"""


def build_rename_note(renamed_from: str | None, op: str) -> str:
    """Return the ADR-002 'Previously called' note (empty if not renamed)."""
    if not renamed_from:
        return ""
    return (
        f"\n> Previously called `bv.{renamed_from}`. "
        f"Renamed to `{op}` per [ADR-002](../../../.planning/decisions/ADR-002-polars-op-rename.md) "
        "for Polars-convention consistency. The old name remains as a deprecation alias in v0.0.x and is removed in v0.1."
    )


def write_op_pages(repo_root: pathlib.Path) -> int:
    """Write one stub file per OPS entry; return count written."""
    written = 0
    for family, op, renamed_from, brief in OPS:
        target_dir = repo_root / "docs" / "operators" / family
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{op}.md"
        target.write_text(STUB.format(
            op=op,
            brief=brief,
            rename_note=build_rename_note(renamed_from, op),
            polish_plan=polish_plan_for(family, op),
            bound=LIFETIME_BOUND[op],
        ))
        written += 1
    return written


def append_ema_alias_to_ewma(repo_root: pathlib.Path) -> None:
    """Insert an '## Aliases' section into ewma.md documenting bv.ema (idempotent)."""
    ewma_path = repo_root / "docs" / "operators" / "decay" / "ewma.md"
    ewma_text = ewma_path.read_text()
    if "## Aliases" in ewma_text and "bv.ema" in ewma_text:
        return  # Already present — idempotent no-op.
    aliases_block = (
        "## Aliases\n"
        "\n"
        "- `bv.ema` — same op; alias preserved as a convention shortcut.\n"
        "  ema and ewma map to the same `AggKind::Ewma` variant in `crates/beava-core/src/agg_op.rs`.\n"
        "\n"
    )
    if "## See also" in ewma_text:
        ewma_text = ewma_text.replace("## See also", aliases_block + "## See also")
    else:
        ewma_text = ewma_text.rstrip() + "\n\n" + aliases_block
    ewma_path.write_text(ewma_text)


def write_master_index(repo_root: pathlib.Path) -> None:
    """Render docs/operators/index.md — master 54-row catalog (with per-family subsections)."""
    index_path = repo_root / "docs" / "operators" / "index.md"
    lines: list[str] = []
    lines.append("# Operator Catalog")
    lines.append("")
    lines.append(
        "> 54 operator pages (53 unique AggKind variants + `ema` alias documented inline "
        "in `ewma.md`), across 7 family subdirectories."
    )
    lines.append("")
    lines.append(
        "Each operator page follows the same 9-section template "
        "(Signature / Description / Parameters / Returns / Complexity / Examples / Wire / "
        "Edge cases / See also). Renamed ops (per [ADR-002](../../.planning/decisions/ADR-002-polars-op-rename.md)) "
        "use the new Polars-convention name as filename + H1; each carries a "
        "\"Previously called `bv.<old>`\" note for searchability."
    )
    lines.append("")

    by_family: dict[str, list[tuple[str, str, str | None]]] = {}
    for family, op, renamed, brief in OPS:
        by_family.setdefault(family, []).append((op, brief, renamed))

    family_titles = {
        "core": "Core (8)",
        "sketch": "Sketch (5)",
        "point-ordinal": "Point / ordinal (5)",
        "recency": "Recency (10)",
        "decay": "Decay (6)",
        "velocity": "Velocity (9)",
        "buffer-geo": "Bounded buffer + geo (11)",
    }
    family_order = [
        "core", "sketch", "point-ordinal", "recency",
        "decay", "velocity", "buffer-geo",
    ]

    for family in family_order:
        ops = by_family[family]
        title = family_titles[family]
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| Op | Description |")
        lines.append("|----|-------------|")
        for op, brief, renamed in ops:
            tag = f" *(renamed from `bv.{renamed}` per ADR-002)*" if renamed else ""
            lines.append(f"| [`bv.{op}`](./{family}/{op}.md){tag} | {brief} |")
        lines.append("")

    lines.append("## Aliases")
    lines.append("")
    lines.append(
        "- `bv.ema` is an alias of [`bv.ewma`](./decay/ewma.md) — documented inline on "
        "the ewma page (same `AggKind::Ewma` variant; 53 unique kinds + 1 alias = 54 page paths)."
    )
    lines.append("")

    lines.append("## Cost-class metadata")
    lines.append("")
    lines.append(
        "- See [cost-class.md](./cost-class.md) for per-op CPU tier (Tier 1 / Tier 2 / Tier 3) — "
        "alive Phase 19.2 metadata, cross-linked from each op page's Complexity section."
    )
    lines.append("")

    lines.append("## See also")
    lines.append("")
    lines.append("- [pipeline-dsl/compilation-rules.md](../pipeline-dsl/compilation-rules.md) — chain compilation rules")
    lines.append("- [examples/wire/](../../examples/wire/) — JSON wire form fixtures")
    lines.append("- [ADR-002 Polars op rename](../../.planning/decisions/ADR-002-polars-op-rename.md)")
    lines.append("")

    index_path.write_text("\n".join(lines))


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    written = write_op_pages(repo_root)
    append_ema_alias_to_ewma(repo_root)
    write_master_index(repo_root)
    print(f"OK — scaffolded {written} op pages + index.md (ema alias inline in ewma.md)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
