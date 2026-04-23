# Beava v2 — Performance Baselines

**Created:** 2026-04-23 (Phase 5.5 plan 01)
**Regression gates:** 10% slower than baseline in same hw-class = WARNING; 25% slower = BLOCKER. See CLAUDE.md §Performance Discipline.

## How to read this file

Baselines are recorded per **hw-class**, not per machine. A hw-class is the tuple
`(cpu-arch-family, OS family, core count bucket)` — e.g. `apple-m1-pro / darwin-24.3.0 / 10 cores`.
Regression checks compare a new bench run against the same hw-class only.

To capture a hw-class string on macOS:
```bash
echo "$(sysctl -n machdep.cpu.brand_string | tr ' ' '-') / $(uname -sr | tr ' ' '-') / $(sysctl -n hw.ncpu) cores"
```

On Linux:
```bash
echo "$(lscpu | awk -F: '/Model name/ {print $2}' | xargs | tr ' ' '-') / $(uname -sr | tr ' ' '-') / $(nproc) cores"
```

## hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores

Captured: 2026-04-23

| Bench | Median | Captured | Phase | Notes |
|---|---|---|---|---|
| encode/register_small | 21.7 ns | 2026-04-23 | 2.5 | |
| encode/register_medium | 102.2 ns | 2026-04-23 | 2.5 | |
| encode/register_near_limit | 27.56 µs | 2026-04-23 | 2.5 | |
| decode/register_small | 96.2 ns | 2026-04-23 | 2.5 | |
| decode/register_medium | 261.2 ns | 2026-04-23 | 2.5 | |
| decode/register_near_limit | 37.27 µs | 2026-04-23 | 2.5 | |
| parse/small | 282.6 ns | 2026-04-23 | 4 | |
| parse/medium | 2.04 µs | 2026-04-23 | 4 | |
| parse/deep | 11.83 µs | 2026-04-23 | 4 | |
| eval/arith | 110.0 ns | 2026-04-23 | 4 | |
| eval/compare | 16.1 ns | 2026-04-23 | 4 | |
| eval/boolean | 84.0 ns | 2026-04-23 | 4 | |
| eval/nullcheck | 26.4 ns | 2026-04-23 | 4 | |
| eval/cast | 55.3 ns | 2026-04-23 | 4 | |
| op_chain/compile_4op | 2.69 µs | 2026-04-23 | 4 | |
| op_chain/apply_4op | 401.5 ns | 2026-04-23 | 4 | |
| agg_op/count | 1.8 ns | 2026-04-23 | 5 | |
| agg_op/sum | 5.7 ns | 2026-04-23 | 5 | |
| agg_op/avg | 5.5 ns | 2026-04-23 | 5 | |
| agg_op/min | 6.6 ns | 2026-04-23 | 5 | |
| agg_op/max | 9.5 ns | 2026-04-23 | 5 | |
| agg_op/variance | 12.1 ns | 2026-04-23 | 5 | |
| agg_op/stddev | 10.9 ns | 2026-04-23 | 5 | |
| agg_op/ratio | 3.3 ns | 2026-04-23 | 5 | |
| windowed/fold_count_5m_1Mevt | 7.11 ms | 2026-04-23 | 5 | |
| windowed/fold_sum_5m_1Mevt | 8.75 ms | 2026-04-23 | 5 | |
| apply/3agg_100ent_1Kevt | 1.01 ms | 2026-04-23 | 5 | |
| test_register_compile_10_descriptors | 110.63 µs | 2026-04-23 | 3 | pytest-benchmark median |
| wal/append_nofsync | 279.71 ns | 2026-04-23 | 6 | serialize + CRC32C + BufWriter write; 256-byte payload |
| wal/append_fsync_default_coalesce | 7.40 ms | 2026-04-23 | 6 | single push awaited through WalSink with default 2ms/1MB coalesce. WARNING: exceeds success-criterion-#3 target of <2ms — macOS `F_FULLSYNC` is substantially slower than Linux `fdatasync`; hw-class-limited. Linux baseline to be captured in Phase 13 CI. |
| wal/append_fsync_burst_1k | 10.62 ms/batch | 2026-04-23 | 6 | 1000 concurrent appends through group-commit = ~10.6 µs/push amortized — proves coalescing works under load |
| snapshot/serialize_state_1k_features | 10.68 µs | 2026-04-23 | 7 | bincode encode of SnapshotBody with 1 derivation × 1k entities × CountState. Pure CPU, no I/O. |
| snapshot/atomic_write_default_fsync | 8.45 ms | 2026-04-23 | 7 | full write+fsync+rename of a populated SnapshotBody; macOS F_FULLSYNC dominated (matches Phase 6 wal_fsync_default_coalesce hw-class limit). |
| recovery/replay_wal_10k_records | 675.93 µs | 2026-04-23 | 7 | WalReader::read_all over a 10k-record segment = ~14.8 M records/sec disk-read+decode throughput. |

> Regression thresholds: +10% = WARNING (flag in VERIFICATION.md); +25% = BLOCKER. Compare within same hw-class only.
