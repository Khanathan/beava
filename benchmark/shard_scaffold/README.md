# Baseline: shard_hint Wave 0 (Phase 48)

**Committed:** 2026-04-18
**Branch:** arch/tpc-full-shard
**Runner:** dev machine (macOS, Apple Silicon — criterion local run)
**Ship-gate:** p50 <100 ns per invocation. Nightly CI on reference Ubuntu box
anchors the official ±1% regression gate (see `.github/workflows/bench-nightly.yml`).

## Results

| Bench ID | p50 (ns) | Budget | Status |
|----------|----------|--------|--------|
| `shard_hint/string_key` | 6.46 | <100 ns | PASS |
| `shard_hint/tuple_two_field_key` | 12.56 | <100 ns | PASS |
| `shard_hint/numeric_key` | 5.61 | <100 ns | PASS |

Full criterion output from Plan 02 bench run:

```
shard_hint/string_key   time:   [6.3030 ns 6.4599 ns 6.6105 ns]
                        thrpt:  [151.27 Melem/s 154.80 Melem/s 158.65 Melem/s]

shard_hint/tuple_two_field_key
                        time:   [12.307 ns 12.556 ns 12.935 ns]
                        thrpt:  [77.307 Melem/s 79.645 Melem/s 81.256 Melem/s]

shard_hint/numeric_key  time:   [5.5419 ns 5.6104 ns 5.6855 ns]
                        thrpt:  [175.89 Melem/s 178.24 Melem/s 180.44 Melem/s]
```

## Notes

- Numeric key path returns `0` (graceful fallback) — measures only the type-check branch cost.
- Tuple two-field key hashes the first field only (Wave 0); full tuple hashing lands in Wave 1 (Phase 49).
- SPSC roundtrip bench (<10 μs budget) is deferred to Wave 1 (Phase 49) per CONTEXT.md D-08.
- These numbers are from a development machine. The nightly CI job (`bench-nightly.yml`) runs
  on `ubuntu-latest` and uploads the official criterion output as an artifact.

## Regression policy

Future waves must keep all cells within ±1% of the baseline on the reference box at N=1.
A drift >1% indicates a cost bug in the scaffolding and blocks merge to main.
