# Phase 11 — Microbench rows

> Per-phase criterion microbench results for Phase 11 buffer + geo ops.
> Source: `crates/beava-core/benches/phase11_buffer_geo.rs`.
> Per CLAUDE.md §Performance Discipline: tripwire baseline, not a target.
> Hardware: Darwin-24.3.0 / 10 cores. Commit: `6235ba2`.

## Bench results — `cargo bench -p beava-core --bench phase11_buffer_geo`

| bench id                              | median time (ns) | range (ns)            | iter   |
|---------------------------------------|------------------|------------------------|--------|
| buffer/histogram/update               | 5.77             | [5.76, 5.79]          | 830M   |
| buffer/hour_of_day_histogram/update   | 1.05             | [1.03, 1.08]          | 4.6B   |
| buffer/dow_hour_histogram/update      | 1.98             | [1.94, 2.05]          | 2.5B   |
| buffer/seasonal_deviation/update      | 3.35             | [3.24, 3.50]          | 1.5B   |
| buffer/event_type_mix/update          | 20.62            | [20.06, 21.38]        | 245M   |
| buffer/most_recent_n/update           | 7.10             | [6.94, 7.30]          | 701M   |
| buffer/reservoir_sample/update        | 7.81             | [7.67, 8.00]          | 630M   |
| geo/geo_velocity/update               | 24.28            | [22.88, 26.13]        | 216M   |
| geo/geo_distance/update               | 20.26            | [19.93, 20.79]        | 223M   |
| geo/unique_cells/update               | 12.43            | [12.15, 12.84]        | 386M   |
| geo/geo_entropy/update                | 14.64            | [12.99, 16.58]        | 385M   |
| geo/distance_from_home/update         | 16.49            | [14.50, 19.13]        | 281M   |

## Observations

- **Cheapest:** `hour_of_day_histogram` at 1 ns — flat 24-bucket array index by `(event_time_ms / 3_600_000) % 24`.
- **Most expensive:** `geo_velocity` at 24 ns — haversine call + 1 stored-prev-coord update + dt arithmetic.
- **Buffer family** (5 reps, excluding event_type_mix outlier): 1–8 ns, consistent with Phase 5 simple-counter baseline (~5–8 ns).
- **Geo family** (5 reps): 12–24 ns, ~3× Phase 5 ops as expected. Haversine math is the cost; trig ops dominate.
- **event_type_mix** at ~21 ns is the outlier among buffer ops — uses BTreeMap insert + count update which is more expensive than the array-indexed buckets.

## Regression status

This is the first Phase 11 baseline; no prior Phase 11 numbers to compare. Phase 6+ regression gates apply going forward (10% warn / 25% block) per CLAUDE.md §Performance Discipline.

> Geo deps: `haversine 0.2` (workspace dep; pure-Rust ~20-line great-circle formula). No `h3o` — using equirectangular grid cells per CONTEXT D-02.
