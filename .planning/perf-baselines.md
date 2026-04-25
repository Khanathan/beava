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

### Phase 18-04 — I/O threads write phase (informational, Apple-M4)

| Bench | Median | Captured | Phase | Notes |
|---|---|---|---|---|
| io_write/serialize_into/TcpAck | ~4 ns (estimated) | 2026-04-25 | 18-04 | BytesMut BufMut ops: put_u32+put_u16+put_u8+put_u64 = 4 ops, no alloc. Criterion bench deferred to 18-04.5 (bench infra plan). |
| io_write/64_clients_500_events | 30ms total | 2026-04-25 | 18-04 | test_p99_tail_latency_under_load: 64 clients × 500 events via 4-thread IoPool in debug mode; serialize_into + pool dispatch. Release numbers deferred to 18-04.5. |

> Apple-M4 is INFORMATIONAL for Phase 18-01 through 18-04 (D-16). Linux Xeon hard gate activates at Phase 18-05.

> Regression thresholds: +10% = WARNING (flag in VERIFICATION.md); +25% = BLOCKER. Compare within same hw-class only.

---
## Per-phase rows merged from parallel worktrees (2026-04-24)

### Phase 6.1 — async-durability (Periodic mode bench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| wal/append_periodic_default | ~870 µs | 2026-04-24 | 6.1 | Single-task `append_event_with_mode(…, Periodic).await`; ~8.5× faster than Phase 6 PerEvent baseline (7.40 ms) |
| wal/append_periodic_burst_1k | ~3.92 ms/batch (~3.9 µs/push amortized) | 2026-04-24 | 6.1 | 1000 concurrent appends; ~2.7× faster than Phase 6 PerEvent burst (10.62 ms) |

### Phase 8 — point/recency/streak ops (criterion microbench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| agg_op_phase8/first | ~3.8 ns | 2026-04-24 | 8 | inferred from FirstState shape |
| agg_op_phase8/last | 7.60 ns | 2026-04-24 | 8 | early-exit once current.is_some() |
| agg_op_phase8/first_n | 3.76 ns | 2026-04-24 | 8 | early-exit after N events |
| agg_op_phase8/last_n | 7.89 ns | 2026-04-24 | 8 | VecDeque push+pop |
| agg_op_phase8/lag | 7.84 ns | 2026-04-24 | 8 | VecDeque push+pop |
| agg_op_phase8/first_seen | 23.75 ns | 2026-04-24 | 8 | shared SeenState |
| agg_op_phase8/last_seen | 26.31 ns | 2026-04-24 | 8 | |
| agg_op_phase8/age | 34.99 ns | 2026-04-24 | 8 | includes query-time subtraction |
| agg_op_phase8/has_seen | 17.91 ns | 2026-04-24 | 8 | pure Bool projection |
| agg_op_phase8/time_since | 75.44 ns | 2026-04-24 | 8 | high variance; quiescent baseline needed |
| agg_op_phase8/time_since_last_n | 90.91 ns | 2026-04-24 | 8 | ring-buffer update + query |
| agg_op_phase8/streak | 17.04 ns | 2026-04-24 | 8 | |
| agg_op_phase8/max_streak | 31.97 ns | 2026-04-24 | 8 | |
| agg_op_phase8/negative_streak | 33.41 ns | 2026-04-24 | 8 | |
| agg_op_phase8/first_seen_in_window | 117.24 ns | 2026-04-24 | 8 | windowed lifetime-state |

### Phase 9 — decay + velocity ops (criterion microbench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| agg_op_p9/ewma | 8.55 ns | 2026-04-23 | 9 | EWMA with α = 1 - exp(-Δt·ln2/half_life) |
| agg_op_p9/ewvar | 9.60 ns | 2026-04-23 | 9 | EW Welford-adapted variance |
| agg_op_p9/ewzscore | 10.08 ns | 2026-04-23 | 9 | wraps EwVar |
| agg_op_p9/decayedsum | 9.06 ns | 2026-04-23 | 9 | Cormode forward decay |
| agg_op_p9/decayedcount | 5.80 ns | 2026-04-23 | 9 | no field — fastest |
| agg_op_p9/twa | 8.24 ns | 2026-04-23 | 9 | sum_v_dt + sum_dt + last_v + last_t |
| agg_op_p9/rateofchange | 8.40 ns | 2026-04-23 | 9 | Δvalue / Δt |
| agg_op_p9/interarrivalstats | 15.57 ns | 2026-04-23 | 9 | Welford on inter-arrival gaps |
| agg_op_p9/burstcount | 9.74 ns | 2026-04-23 | 9 | 64-bucket sliding sub-window |
| agg_op_p9/deltafromprev | 6.35 ns | 2026-04-23 | 9 | scalar diff |
| agg_op_p9/trend | 6.85 ns | 2026-04-23 | 9 | online OLS accumulator |
| agg_op_p9/trendresidual | 13.22 ns | 2026-04-23 | 9 | |
| agg_op_p9/outliercount | 32.49 ns | 2026-04-23 | 9 | Welford + sigma threshold |
| agg_op_p9/valuechangecount | 9.89 ns | 2026-04-23 | 9 | |
| agg_op_p9/zscore | 18.01 ns | 2026-04-23 | 9 | Welford + sqrt at query |

### Phase 10 — sketch ops (criterion microbench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| sketch_ops/count_distinct/exact_array_update | 17.2 ns | 2026-04-23 | 10 | hybrid mode 1 (binary-search insert ≤16) |
| sketch_ops/count_distinct/hash_set_update | 262.1 ns | 2026-04-23 | 10 | hybrid mode 2 (HashSet ≤1024) |
| sketch_ops/count_distinct/hll_update | 23.1 ns | 2026-04-23 | 10 | hybrid mode 3 (HLL p=12) |
| sketch_ops/count_distinct/promote_array_to_set | 1.41 µs | 2026-04-23 | 10 | one-shot promotion |
| sketch_ops/count_distinct/promote_set_to_hll | 4.22 µs | 2026-04-23 | 10 | one-shot promotion |
| sketch_ops/percentile/exact_update | ~17 ns | 2026-04-23 | 10 | exact Vec push |
| sketch_ops/percentile/uddsketch_update | 111.2 ns | 2026-04-23 | 10 | UDDSketch insert post-promotion |
| sketch_ops/percentile/uddsketch_query_p99 | 288.8 ns | 2026-04-23 | 10 | quantile lookup over 10k inserts |
| sketch_ops/top_k/exact_update | 70.5 ns | 2026-04-23 | 10 | BTreeMap entry+bump |
| sketch_ops/top_k/hybrid_update | 260.5 ns | 2026-04-23 | 10 | CMS+heap with O(log k) HashMap heap-position index |
| sketch_ops/top_k/hybrid_query_top10 | 205.3 ns | 2026-04-23 | 10 | snapshot top-k vec |
| sketch_ops/bloom/update_1k_capacity | 95.2 ns | 2026-04-23 | 10 | Kirsch-Mitzenmacher 7 hashes |
| sketch_ops/bloom/query_member_1k | 8.6 ns | 2026-04-23 | 10 | bit-array probe |
| sketch_ops/entropy/update_100cat | 693.3 ns | 2026-04-23 | 10 | dominated by `format!()` in fixture |
| sketch_ops/entropy/query_bits_100cat | 253.7 ns | 2026-04-23 | 10 | Σ p log₂ p across 100 buckets |
| windowed/hll_1Mevt | 821.9 µs | 2026-04-23 | 10 | 1M HLL inserts ≈ 822 ns/elem |
| windowed/uddsketch_1Mevt | 22.10 ms | 2026-04-23 | 10 | 1M inserts ≈ 22 ns/elem |
| windowed/cms_1Mevt | 5.01 ms | 2026-04-23 | 10 | 1M inserts ≈ 5 ns/elem |
| windowed/entropy_1Mevt | 75.0 ms | 2026-04-23 | 10 | dominated by `format!()` in fixture |

### Phase 11 — buffer + geo ops (criterion microbench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| buffer/histogram/update | 5.77 ns | 2026-04-24 | 11 | 830M iter |
| buffer/hour_of_day_histogram/update | 1.05 ns | 2026-04-24 | 11 | flat 24-bucket array |
| buffer/dow_hour_histogram/update | 1.98 ns | 2026-04-24 | 11 | |
| buffer/seasonal_deviation/update | 3.35 ns | 2026-04-24 | 11 | |
| buffer/event_type_mix/update | 20.62 ns | 2026-04-24 | 11 | BTreeMap insert + count |
| buffer/most_recent_n/update | 7.10 ns | 2026-04-24 | 11 | |
| buffer/reservoir_sample/update | 7.81 ns | 2026-04-24 | 11 | |
| geo/geo_velocity/update | 24.28 ns | 2026-04-24 | 11 | haversine + dt arithmetic |
| geo/geo_distance/update | 20.26 ns | 2026-04-24 | 11 | |
| geo/unique_cells/update | 12.43 ns | 2026-04-24 | 11 | |
| geo/geo_entropy/update | 14.64 ns | 2026-04-24 | 11 | |
| geo/distance_from_home/update | 16.49 ns | 2026-04-24 | 11 | |

### Phase 11.5 — temporal MVCC (criterion microbench)

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| temporal_mvcc/upsert/depth_1 | 2.57 µs | 2026-04-23 | 11.5 | |
| temporal_mvcc/upsert/depth_10 | 4.84 µs | 2026-04-23 | 11.5 | |
| temporal_mvcc/upsert/depth_100 | 19.82 µs | 2026-04-23 | 11.5 | |
| temporal_mvcc/upsert/depth_1000 | 430.12 µs | 2026-04-23 | 11.5 | super-linear; iter_batched setup cost dominates |
| temporal_mvcc/as_of_lookup/depth_1 | 220.79 ns | 2026-04-23 | 11.5 | empty-tree probe noise |
| temporal_mvcc/as_of_lookup/depth_10 | 68.54 ns | 2026-04-23 | 11.5 | warm-cache representative |
| temporal_mvcc/as_of_lookup/depth_100 | 160.32 ns | 2026-04-23 | 11.5 | |
| temporal_mvcc/as_of_lookup/depth_1000 | 8.36 µs | 2026-04-23 | 11.5 | BTreeMap range walk; 1250× under Phase 13 batch-get target |
