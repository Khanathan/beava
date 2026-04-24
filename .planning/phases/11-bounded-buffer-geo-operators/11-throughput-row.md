# Phase 11 — Throughput rows

> Per-phase throughput captures. Not appended to canonical
> `.planning/throughput-baselines.md` (orchestrator instruction).
> Hardware: Darwin-24.3.0 / 10 cores. Commit: `6235ba2`.

## Geo pipeline (new shape: geo_velocity + unique_cells + most_recent_n)

| pipeline | transport | sustained_eps | push_p50_us | push_p95_us | push_p99_us | get_p99_us | rss_mb | duration | notes |
|----------|-----------|---------------|-------------|-------------|-------------|------------|--------|----------|-------|
| geo      | http      | **701**       | 9519        | 19999       | 33215       | 32095      | 61     | 30.7s    | 8-way parallel; haversine + cell hashing per event; first geo-shape baseline |

## Small (regression check vs Phase 7.5 simple-fraud baseline)

| pipeline | transport | sustained_eps | push_p50_us | push_p95_us | push_p99_us | get_p99_us | rss_mb | duration | notes |
|----------|-----------|---------------|-------------|-------------|-------------|------------|--------|----------|-------|
| small    | http      | **1097**      | 6955        | 9687        | 13535       | 4595       | 32     | 30.3s    | 8-way parallel; vs Phase 7.5 baseline 990 EPS → +10.8% (improvement) |

## Verdict

- **Small/HTTP regression check:** 1097 EPS vs 990 EPS baseline = **+10.8% (improvement, not a regression)**. Phase 11 ops did not regress simple-fraud throughput.
- **Geo/HTTP first baseline:** 701 EPS. ~36% slower than simple-fraud as expected — haversine + per-event cell hash + buffer state. Sets the geo-shape regression floor for Phase 12+.
- **Bottleneck:** macOS F_FULLSYNC fsync (~7.4 ms/op per Phase 6 baseline) still dominates push p50; per-op aggregation cost is sub-microsecond per the criterion bench.

> Geo workload uses `lat,lon ∈ [0, 1000)` random doubles (not real coords); haversine math is exercised but not over real-world distances. v1+ may add a real-coord workload variant for SC2 fidelity.
