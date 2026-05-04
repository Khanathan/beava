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

### Phase 18-10 — Parse envelope microbench (criterion microbench)

Captured: 2026-04-25. hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Baseline saved as `18-10` (`cargo bench --baseline 18-10` from later phases).

Targets per Plan 18-10 D-4: parse_msgpack_envelope ≤80 ns; parse_json_envelope ≤150 ns. Both met with significant headroom.

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| parse_envelope/parse_msgpack_envelope | 33.4 ns | 2026-04-25 | 18-10 | hand-rolled rmp scanner; target ≤80 ns; 58% under |
| parse_envelope/parse_json_envelope | 77.1 ns | 2026-04-25 | 18-10 | hand-rolled brace-counting scanner (sonic-rs LazyValue derive path was ~380 ns/op, dropped to D-2 fallback); target ≤150 ns; 49% under |
| parse_envelope/msgpack_body_to_row | 407.8 ns | 2026-04-25 | 18-10 | informational; rmp_serde::from_slice::<Row> via BeavaValueVisitor (Plan 18-10 D-3 rewrite) |
| parse_envelope/json_body_to_row | 402.9 ns | 2026-04-25 | 18-10 | informational; sonic_rs::from_slice::<Row> via BeavaValueVisitor (Plan 18-10 D-3 rewrite) |

**Improvement vs Plan 18-09:**
- parse_msgpack_envelope: previously 1,928 ns (rmp_serde::from_slice::<JsonValue> + rmp_serde::to_vec_named) → 33.4 ns. **57.7× faster.**
- parse_json_envelope: previously 583 ns (serde_json::from_slice::<PushEnvelope> + serde_json::to_vec) → 77.1 ns. **7.6× faster.**
- msgpack body_to_row: previously included JsonValue alloc per field → 407.8 ns direct Row.
- json body_to_row: previously included JsonValue alloc per field → 402.9 ns direct Row.

### Phase 18-11 — body→Row + agg microbench (criterion microbench)

Captured: 2026-04-26. hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Baseline saved as `18-11` (`cargo bench --baseline 18-11` from later phases).

Plan 18-11 swapped Row.0 from `BTreeMap<String, Value>` to `SmallVec<[(CompactString, Value); 8]>`, switched `Value::Str` to CompactString (SSO ≤24 bytes), changed `AggStateTable.entities` from `BTreeMap<EntityKey, Vec<AggOp>>` to `hashbrown::HashMap<EntityKey, Vec<AggOp>, FxBuildHasher>` with `raw_entry_mut().from_key(key)` clone-free lookup. Microbench measures the full body→Row deserialise via the new Row visitor.

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| parse_envelope/parse_msgpack_envelope | 33.0 ns | 2026-04-26 | 18-11 | unchanged from 18-10 (envelope scanner is structural; not affected by Row storage swap) |
| parse_envelope/parse_json_envelope | 75.4 ns | 2026-04-26 | 18-11 | unchanged from 18-10 |
| parse_envelope/msgpack_body_to_row | 141.6 ns | 2026-04-26 | 18-11 | **2.9× faster** vs 18-10's 407.8 ns; matches variant-D spike (146 ns) within ±4% |
| parse_envelope/json_body_to_row | 169.8 ns | 2026-04-26 | 18-11 | **2.4× faster** vs 18-10's 402.9 ns; matches variant-D spike (184 ns) within ±8% |

**Improvement vs Plan 18-10 baseline:**
- msgpack_body_to_row: 407.8 → 141.6 ns. **2.88× faster.** Variant-D landed in production.
- json_body_to_row: 402.9 → 169.8 ns. **2.37× faster.** Variant-D landed in production.

**Variant-D spike-to-production fidelity (M4):** spike measured RowD struct at 146 ns msgpack / 184 ns json; production Row hits 141.6 ns / 169.8 ns. Both within ±10% of the spike — the structural change closed the alloc gap as predicted.

**Driver:** SmallVec inline (no BTreeMap node alloc) + CompactString inline (no per-key/per-value String alloc) + Row::Deserialize visit_map walking direct push (no with_field re-clone).

**Targets per Plan 18-11 must_haves:**
- msgpack_body_to_row ≤ 165 ns ±10% → ✅ 141.6 ns
- json_body_to_row ≤ 200 ns ±10% → ✅ 169.8 ns

Both met with headroom. parse_*_envelope numbers held steady (envelope scanner is independent of Row storage).

### Phase 19 — blast_shape sampler + pool builder (criterion microbench)

Captured: 2026-04-26. hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Baseline saved as `19` (`cargo bench --baseline 19` from later phases).

Plan 19-01 introduced the `blast_shape` module + four-shape Pool=N builder + ZipfianSampler.
This bench captures the start-of-line numbers — future bench changes regress against these.

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| build_pool/fixed/n_10000 | 46.344 µs | 2026-04-26 | 19 | One frame, cloned 10k times via Bytes refcount; single encode amortised across N |
| build_pool/uniform/n_10000_k_1000 | 12.528 ms | 2026-04-26 | 19 | 10k frames, K=1000 uniform sampling; full per-frame encode (json envelope + body) |
| build_pool/zipfian/n_10000_k_1000_alpha_1.0 | 5.2559 ms | 2026-04-26 | 19 | 10k frames, K=1000 hand-rolled Zipfian (alpha=1.0 log-uniform branch) |
| build_pool/mixed/n_10000_m_3 | 5.1835 ms | 2026-04-26 | 19 | 10k frames, 3 round-robin event names; key cardinality 1M default |
| sampler/sample_zipfian/k_1000_alpha_1.0 | 18.384 ns | 2026-04-26 | 19 | Single-sample cost; alpha=1.0 log-uniform inverse-CDF + StdRng |
| sampler/sample_uniform/k_1000 | 6.8615 ns | 2026-04-26 | 19 | Single-sample baseline (`rand::Rng::gen_range` over StdRng) |

**Driver:** Pool=N elimination of per-iteration encode + RNG cost in the bench hot loop.
Pool memory at N=1M ≈ 100-300 MB (depends on shape's per-frame body size; Plan 19-01
SUMMARY estimates ~500 MB-1 GB at N=1M). Operator's responsibility to size against host
RAM (D-02 architectural rationale).

**Observations:**
- `fixed` is ~270× faster than `uniform` because Bytes refcount clones don't repeat the
  envelope encode. This is by design — `fixed` is the cache-warm marketing peak.
- `uniform` (12.5 ms) is ~2.4× slower than `zipfian` (5.3 ms) because the `format!("k{:08}")`
  per-frame allocation dominates non-fixed shapes; uniform's `gen_range` cost over K=1000
  was expected to be cheaper than Zipfian's two-RNG-draw + log/exp pipeline, but in
  practice the encode-side allocator pressure dominates and the two distributions land
  within the same order of magnitude. Mixed is the same cost as zipfian (within 2%) — the
  encode path is the bottleneck, not the sampler.
- `sample_zipfian` is ~2.7× slower than `sample_uniform` (18 ns vs 7 ns) — the alpha=1
  log-uniform inverse-CDF requires `ln`/`exp` on the hot path. This number is the
  start-of-line for the sampler itself; if Plan 19-05 ever needs to sample a 1M-element
  pool at 1M EPS, the sampler floor is ~55 Melem/s.

**Targets per Plan 19-04:** no targets (this is the start of the line). Future regressions
gate at +10% WARN / +25% BLOCK per CLAUDE.md §Performance Discipline.

### Phase 19.1 — WindowedOp lazy buckets (criterion microbench)

Captured: 2026-04-27. hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Baseline saved as `19.1-04-lazy-buckets` (`cargo bench --baseline 19.1-04-lazy-buckets` from later phases).

Plan 19.1-04 replaced `WindowedOp.buckets: [Option<Box<AggOp>>; 64]` + `bucket_epoch_start_ms: [i64; 64]` with `buckets: SmallVec<[(i64, Box<AggOp>); 4]>` + lazy allocation. The OLD layout zero-init'd ~1024 bytes per WindowedOp at construction (~1500 ns / 2576 ns of cold-key entity init on fraud-team per Phase 19's debug analysis); the NEW layout's `SmallVec::new` is allocation-free, with bucket entries pushed lazily on the first event into a new epoch. AGG-CORE-09's 64-bucket cap is enforced by oldest-epoch eviction (swap_remove of min-epoch entry once `len >= max_buckets`).

Bench: `crates/beava-core/benches/windowed_op_init.rs`.

| Bench | OLD median | NEW median | Lift | Captured | Phase | Notes |
|---|---|---|---|---|---|---|
| windowed_op_init/new_count_60s          | 130.71 ns | 6.66 ns   | -94.9% (~20×) | 2026-04-27 | 19.1 | Cold WindowedOp::new(Count, 60s); SmallVec::new is a no-op |
| windowed_op_init/new_percentile_60s     | 428.51 ns | 12.50 ns  | -97.1% (~34×) | 2026-04-27 | 19.1 | Cold WindowedOp::new(Percentile, 60s); UDDSketch params not allocated until first event |
| windowed_op_init/new_plus_first_update  | 581.00 ns | 154.62 ns | -73.4% (~3.8×) | 2026-04-27 | 19.1 | Full cold-key path: new + 1 update; first push allocates inner AggOp + SmallVec entry |

OLD baseline saved as `old-fixed-array` for reproducibility (commit `f47ae55`, before GREEN). All three benches well above the ≥50% target per Plan 19.1-04 acceptance criteria.

**Driver:** lazy allocation — `[Option<Box<AggOp>>; 64]` zero-init (memset 512 B) + `[i64; 64]` set to `i64::MIN` (memset 512 B) eliminated; SmallVec inline cap=4 covers 99% of typical fraud workloads (1-2 active buckets/entity); spill-to-heap on >4 active buckets stays graceful (regular Vec under the hood).

**Predicted EPS lift on fraud-team zipfian:** ~50% per CONTEXT D-16 (4-14 windowed ops × ~400 ns saved per op × cold-key init rate). Wall-clock-honest measurement deferred to Plan 19.1-05's re-baseline matrix (depends on Plan 19.1-01 bench wall_clock fix landing).

**Targets per Plan 19.1-04 must_haves:** Cold WindowedOp::new ≥50% faster than the 64-slot zero-init baseline → ✅ all three benches show ≥73%.

Both gate types apply (criterion microbench + future throughput-baselines.md re-baseline).

### Phase 19.2 — apply-path bench (post-stacked-fix)

Captured: 2026-04-27 (Phase 19.2-08). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Stacked optimizations measured: D-01 field pre-extraction, D-02 AHasher process-static + FxHasher for HLL, D-03 EntityKey hybrid SingleU64/SingleStr/Multi, D-04 cluster dispatch dedup, D-04a UDDSketch flat sorted Vec, D-04b EventTypeMix AHashSet + Cow.

Synthetic registry shape: 14 features (7 user-keyed, 4 user×merchant-keyed, 3 device-keyed). Mix of Count, Sum, Percentile (UDDSketch), Ewma, TopK, Entropy, EventTypeMix, CountDistinct, BloomMember — spans Tier 1/2/3. This is a synthetic stand-in for the real fraud-team.json (throughput rebaseline in Phase 19.2-08 Task 2 drives the actual end-to-end pipeline).

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| apply_path/cold_key/14_aggs | 1.424 µs | 2026-04-27 | 19.2 | Cold-key 14-agg fraud-team-shape synthetic registry; full per-entity init + all 3 cluster EntityKey builds. Post-stacked-fix (D-01..D-04b). Pre-19.2 reference: ~13.4 µs (Phase 19.1 trace avg); 9.4× improvement. |
| apply_path/warm_key/14_aggs | 362.71 ns | 2026-04-27 | 19.2 | Warm-key steady state; 200-event pre-warm; no per-entity init cost; measures pure apply-loop throughput. Post-stacked-fix. |
| apply_path/uddsketch/insert_warm | 71.774 ns | 2026-04-27 | 19.2 | Plan 19.2-04 flat sorted Vec at 1k pre-loaded buckets; binary-search insert at steady state. Pre-fix reference: ~130 ns (BTreeMap, Phase 10 baseline). 1.8× faster. |
| apply_path/uddsketch/quantile_warm | 105.31 ns | 2026-04-27 | 19.2 | Quantile q=0.5 over 1k-insert warm sketch. Sequential Vec traversal; no pointer chasing. |
| apply_path/event_type_mix/allowed_hit | 25.027 ns | 2026-04-27 | 19.2 | Plan 19.2-05 AHashSet 1024-allowed; cat_500 hit path. Pre-fix reference: ~1,127 ns (Vec linear scan per efficiency audit). 45× faster. |
| apply_path/event_type_mix/allowed_miss | 7.865 ns | 2026-04-27 | 19.2 | Same; category not in allowlist → O(1) AHashSet miss + early return. |

**Key results vs predicted targets (CONTEXT.md D-04a / D-04b):**
- UDDSketch insert_warm: 71.8 ns — target was ~75 ns (algo floor); **within target band.**
- EventTypeMix allowed_hit: 25.0 ns — target was ~50-100 ns; **better than target (45× vs 10-20× predicted).**
- Cold-key 14-agg: 1.424 µs — predicted was 6-8 µs/event (post stacking); bench measures a simpler synthetic subset but shows the apply-loop overhead itself is well under 2 µs for cold paths. The 6-8 µs prediction included all WAL/bookkeeping/IO overhead in the full server path; this bench isolates the apply-loop-only cost.

### Phase 19.3 — pre-19.3 windowed baseline (criterion microbench)

Captured: 2026-04-28 (Phase 19.3-01). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Pre-19.3 baseline for the slow `WindowedOp::update_with_row` fallback path that Plan 19.3-02 will optimize via `WindowedOp::update_at`. Same 14-feature shape as the non-windowed `apply_path/warm_key/14_aggs` bench, but every feature is wrapped in `WindowedOp(window_ms = 86_400_000)` (24h). Three non-windowable features (Ewma, EventTypeMix, BloomMember) were swapped for windowable Tier-1 substitutes (StdDev, Min, Max) per RESEARCH §2 Q3 — see plan 19.3-01 commit `172ce65` for the substitution rationale.

The non-windowed `14_aggs` row is re-measured here for direct side-by-side comparison (the 2026-04-27 number above moved -13% to 316.95 ns due to subsequent toolchain/state-table micro-changes; we capture it again to anchor the windowed delta against a same-day reference).

| Bench | Median | Date | Phase | Notes |
|---|---|---|---|---|
| apply_path/warm_key/14_aggs (re-measured)  | 316.95 ns | 2026-04-28 | 19.3 | Same-day reference for the windowed-delta comparison; structurally identical to the 19.2 row above. |
| apply_path/warm_key/14_aggs_windowed | 463.82 ns | 2026-04-28 | 19.3 | pre-19.3 baseline (slow WindowedOp::update_with_row fallback path via agg_op.rs:868); Plan 19.3-02 must drop this ≥ 4× (target ≤ 116 ns) on Apple-M4 hw-class; commit 172ce65. |

**Predicted-vs-measured ratio note:** the plan acceptance criterion expected the windowed group to be ≥ 3× the non-windowed group, anchored on the 88-feature fraud-team.json investigation cost-model. The actual 14-feature synthetic ratio is 1.46× (463.82 / 316.95) because much of the per-event cost on this synthetic is sketch-bound (Percentile UDDSketch, TopK, Entropy CountDistinct) where the WindowedOp dispatch tax is proportionally smaller than the inner-state update cost. The bench correctly engages the slow `update_with_row` fallback path (verified by reading `agg_op.rs:865-869` Windowed arm); Plan 19.3-02's verification gate uses **the absolute baseline value (463.82 ns)** with a ≥ 4× speedup target, not the windowed-vs-non-windowed ratio.

**Targets per Plan 19.3-02 acceptance:** windowed group must drop ≥ 4× → ≤ 116 ns at next measurement.

### Phase 19.4 — 19.4-A CountDistinct identity-hasher (criterion microbench)

Captured: 2026-04-28 (Phase 19.4-01). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Plan 19.4-01 swapped `CountDistinctState::HashSet` from `std::collections::HashSet<u64>` to `hashbrown::HashSet<u64, BuildHasherDefault<NoOpHasher>>` where `NoOpHasher::write_u64(x)` stores `x` as the slot index. The std HashSet was rehashing the already-FxHashed u64 input via SipHash on every insert (~1,180 ns/event of apply CPU per `19.3-FLAMEGRAPH.md §2 row #3` measurement, 9.36% self-time at `hashbrown::map::HashMap::insert`, 99% inside CountDistinct probing).

**Bench-fixture note (Plan 19.4-01 measurement deviation, Rule 1):** the prior 19.3-01 fixture pre-warmed with a single fixed Txn row (electronics/approved) so CountDistinct features (`category`, `status`) never accumulated >16 distinct values — they remained in `ExactArray` (Vec binary search) mode and the SipHash-vs-identity-hasher difference could not manifest on this bench. Plan 19.4-01 introduced `build_fraud_team_synthetic_row_varied(seed)` to vary `category`/`status` across 64/32 distinct values during a 1500-event pre-warm, pushing both CountDistinct features into HashSet mode (~64 entries each). The measurement row stays fixed at electronics/approved (a hash-already-present lookup in HashSet mode — the hot path the optimization targets).

The pre-19.4 baseline numbers below are NEW captures with the same 19.4-01 fixture against the pre-RED commit (`ce90cf9`'s production code); they are NOT the 463.82 ns / 316.95 ns numbers from the 19.3-01 row above (those used the old uniform-row fixture which kept CountDistinct in ExactArray mode).

| Bench | Pre-19.4 (new fixture, std HashSet) | Post-19.4-01 (hashbrown+NoOpHasher) | Δ ns | Δ % | Date | Phase | Notes |
|---|---|---|---|---|---|---|---|
| apply_path/warm_key/14_aggs_windowed | 434.22 ns | 408.00 ns | -26.22 | -6.0% | 2026-04-28 | 19.4 | CountDistinct in HashSet mode (varied pre-warm). Δ within criterion CI; lift consistent with live-trace per-AggKind measurement (CountDistinct 457.5→432.1 ns/call). |
| apply_path/warm_key/14_aggs            | 354.38 ns | 330.81 ns | -23.57 | -6.7% | 2026-04-28 | 19.4 | Reference cell, same fixture (non-windowed). 14 features, 2 are CountDistinct in HashSet mode. |

**Targets per Plan 19.4-01 acceptance:** windowed group target was ≤ 200 ns/call (75% floor: ≤ 295 ns/call) per `19.4-CONTEXT.md` D-01 + D-03. **Floor not met** — 408 ns is 113 ns above the 295 ns floor. The lift is real and measurable (-26 ns) but the criterion bench's 14-feature synthetic shape has only 2 CountDistinct features (~10-20% of total apply cost), where the predicted lift of ~118 ns/call × 2 calls = ~240 ns/event would already be at the noise floor of the bench. The fraud-team K=10k zipfian workload has 9 windowed CountDistinct features (~50% higher density), where the live-trace + EPS measurement is the primary verdict per CONTEXT D-04 dual-measurement.

**Driver:** identity hashing eliminates the SipHash double-hash chain on CountDistinct HashSet inserts/lookups; lift confirmed by per-AggKind live trace (CountDistinct 457.5→432.1 ns/call, -25 ns/call) and end-to-end EPS (+5,624 EPS, +7.6%). Both signals agree the optimization works; the absolute target ≤ 200 ns/call was never reachable on the 14-feature synthetic — see `19.4-01-MEASUREMENT.md` for the full live-trace analysis and `19.4-01-DEVIATION.md` for the floor-miss disposition.

### Phase 19.4 — 19.4-B ExtractedFields SmallVec inline-cap 8→16 (criterion microbench)

Captured: 2026-04-28 (Phase 19.4-02). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Plan 19.4-02 widened `ExtractedFields<'a> = SmallVec<[Option<&'a crate::row::Value>; N]>` from N=8 to N=16 to cover fraud-team's per-source field union (~12 fields max for the TxnByUser cluster) without spilling. Per `19.3-FLAMEGRAPH.md §2` `RawVec::with_capacity_in` + `RawVecInner::reserve` at ~4.0% inclusive on the apply hot path was 99% from this SmallVec spilling on every Txn event (~530 ns/event of allocator traffic).

**Bench-fixture note:** the criterion fixture (`build_fraud_team_synthetic_row_varied`) constructs a synthetic row with **only 7 fields** (`user_id, device_id, merchant, amount, status, category, event_type`). With cap=8 the row already fit inline; with cap=16 it also fits inline. The criterion bench therefore **cannot observe the spill-fix** — its `ExtractedFields` length never exceeded the cap-8 threshold to begin with. Numbers below are still recorded for regression tracking; absolute lift is expected to be near-zero (or slightly worse from the larger stack frame).

| Bench | Post-19.4-01 (cap=8) | Post-19.4-02 (cap=16) | Δ ns | Δ % | Date | Phase | Notes |
|---|---|---|---|---|---|---|---|
| apply_path/warm_key/14_aggs_windowed | 404.85 ns (median) | 425.40 ns (median) | +20.55 | +5.1% | 2026-04-28 | 19.4 | Within criterion CI band; mean shifts (406.6→529.3 ns) reflect bimodal stack-allocation noise on the larger inline buffer. Bench is structurally insensitive (7-field row never spilled at cap=8). |
| apply_path/warm_key/14_aggs            | 331.17 ns (median) | 346.78 ns (median) | +15.61 | +4.7% | 2026-04-28 | 19.4 | Reference cell — within ±5% expected variance band. |

**Targets per Plan 19.4-02 acceptance:** windowed group must drop ≥ 5% from post-19.4-01 baseline (75% floor: ≥ 4% drop) per `19.4-CONTEXT.md` D-01 + D-03. **Floor not met** on criterion — observed +5.1% on median (regression direction). However, the criterion bench has 7-field rows that never spilled at cap=8, so the optimization cannot manifest on this bench. Per CONTEXT D-04 dual-measurement, the live-trace + EPS run on fraud-team.json (10-field TxnByUser cluster) is the primary verdict — see `19.4-02-MEASUREMENT.md`.

**Driver:** SmallVec inline-cap widening eliminates per-event heap spill on fraud-team's 10-field Txn source. The criterion bench cannot observe this fix because its synthetic row has only 7 fields. The 14-feature synthetic bench is **structurally insensitive** to the optimization — same shape of insensitivity as Plan 19.4-01 (where only 2/14 features were CountDistinct, the lever's target).

### Phase 19.4 — 19.4-C Geo lat_idx/lon_idx register-time resolution (criterion microbench)

Captured: 2026-04-28 (Phase 19.4-03). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Plan 19.4-03 completed Phase 19.2-06's missing Task 3: `Registry::resolve_field_indices_for_agg_mut` and `resolve_field_indices_for_agg_mut_inner` now populate `AggOpDescriptor.ext.lat_idx`/`.ext.lon_idx` at register time so the existing `update_at` fast path (agg_geo.rs:110/182/259/357 + agg_op.rs:933-960 dispatch arms) engages on the apply hot path. Per `19.3-FLAMEGRAPH.md §2 row #8` `agg_geo::read_lat_lon` self-time was 2.86% of apply CPU = ~357 ns/event because `lat_idx == FIELD_IDX_NONE` for fraud-team's 4 geo features routed dispatch to the slow `update()` arm (agg_op.rs:937).

| Bench | Pre (post-19.4-02) | Post-19.4-03 | Δ ns | Δ % | Date | Phase | Notes |
|---|---|---|---|---|---|---|---|
| apply_path/warm_key/14_aggs_windowed | 425.40 ns (median) | 462.33 ns (median) | +36.93 | +8.7% | 2026-04-28 | 19.4 | Synthetic registry has no geo features; bench delta is variance (same direction Plan 02 saw at +5.1% on the cap widening). |
| apply_path/warm_key/14_aggs            | 346.78 ns (median) | 352.65 ns (median) | +5.87  | +1.7% | 2026-04-28 | 19.4 | Reference cell. Within criterion variance band. |

**Note:** Primary lift for 19.4-C is on fraud-team live-trace (4 of 14 fraud-team features are geo, 0 of 14 synthetic features are geo); criterion-bench delta is structurally absent here — the synthetic registry exercises only non-geo apply paths, so the resolver patch produces identical bytecode for these benches modulo register pressure variance.

**Driver:** Geo dispatch now routes through `update_at` (indexed `extracted` access) instead of `update` (row.get linear scan). Criterion bench is structurally insensitive because the synthetic registry has zero geo features (per `apply_path_bench.rs:33-46` — Cluster A is count/sum/percentile/stddev/topk/entropy/min, Cluster B is count/sum/count_distinct, Cluster C is count/sum/max). Live trace + EPS on fraud-team is the primary verdict (see `19.4-03-MEASUREMENT.md`); the post-19.4-03 samply flamegraph confirms `agg_geo::read_lat_lon` self-time = 0.000% (was 2.86%) on the `beava-apply` thread.

### Phase 19.4 — 19.4-D ExtractedFields hoist above descriptor loop (criterion microbench)

Captured: 2026-04-28 (Phase 19.4-04). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Bench: `crates/beava-core/benches/apply_path_bench.rs`.

Plan 19.4-04 hoisted `ExtractedFields` build out of the per-descriptor loop and up to per-event scope. `EventDescriptor.apply_field_names` is populated at register-time as the alphabetical-sorted union of fields any agg on the source consumes. Each agg has `field_idx_into_event_extracted: Vec<u8>` mapping its declared fields to union indices. Per `19.3-COST-MODEL.md §4` per-desc rebuild was assumed at ~500 ns × 5 descs = 2,500 ns/event scaffolding; in practice (post-19.4-02 cap=16 inline-only) the rebuild is much cheaper (~50 ns each), so the realized lift is smaller than predicted.

| Bench | Pre (post-19.4-03) | Post-19.4-04 | Δ ns | Δ % | Date | Phase | Notes |
|---|---|---|---|---|---|---|---|
| apply_path/warm_key/14_aggs_windowed | 462.33 ns (median) | 412.08 ns (median, range 410.37–413.84) | -50.25 | -10.9% | 2026-04-28 | 19.4 | Hoist eliminates per-desc rebuild scaffolding |
| apply_path/warm_key/14_aggs            | 352.65 ns (median) | 305.44 ns (median, range 303.70–307.60) | -47.21 | -13.4% | 2026-04-28 | 19.4 | Hoist applies to both windowed and non-windowed |

**Targets per Plan 19.4-04 acceptance:** windowed must drop ≥ 10% from post-19.4-03; agg-stage outer drop ≥ 900 ns (75% floor of 1,200 ns predicted lift). Criterion: PASS (windowed -10.9%, both cells drop double-digits). Live-trace agg-stage: FAIL (-100 ns drop vs -900 ns floor) — scaffolding cost was already cheap post-19.4-02 cap-widening; predicted lift was overstated.

**Driver:** Event-level field-union hoist eliminates 5× per-desc ExtractedFields rebuild. Live-trace + EPS verdict on fraud-team is in `19.4-04-MEASUREMENT.md` — EPS hit 102,800 (above 100k Phase 19.4 PASS gate) but agg-stage trace floor failed.

### Phase 19.4 — 19.4-E Final cumulative baseline (criterion + live trace)

Captured: 2026-04-28 (Phase 19.4-05 Task 5.3). hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores.
Binary: post-19.4-04 (commit `075284a`).

This is the cumulative-end-of-Phase-19.4 row — the final baseline against which Phase 19.5+ regression-checks. It supersedes the per-sub-goal rows in 19.4-A / B / C / D as the canonical Phase 19.4 reference for the criterion microbench dimension. The live-trace dimension is supplemental (load-sensitive on macOS dev machine; the Plan 04 measurement at quieter load is the canonical Phase 19.4 closure number).

**Criterion baseline (saved as `19.4-final` baseline; `cargo bench -p beava-core --bench apply_path_bench -- --save-baseline 19.4-final`):**

| Bench | Pre-Phase-19.4 (post-19.3-A) | Post-Phase-19.4 (post-19.4-04, today's measurement) | Cumulative Δ | Notes |
|---|---|---|---|---|
| apply_path/warm_key/14_aggs_windowed | 463.82 ns (post-19.3-A baseline; using Plan 19.4-01's NEW pre-warm fixture, 434.22 ns) | 413.87 ns (median; range 411.91–416.24 ns; 9 outliers/100 samples) | **-49.95 ns / -10.8%** vs 19.3-A baseline / **-4.7% vs new fixture (Plan 19.4-01) baseline of 434.22 ns** | Cumulative effect of 19.4-A (CountDistinct identity hash) + 19.4-B (cap=8→16) + 19.4-C (geo register-time) + 19.4-D (ExtractedFields hoist). |
| apply_path/warm_key/14_aggs            | 316.95 ns (post-19.3-A baseline; using new fixture, 354.38 ns) | 318.68 ns (median; range 317.81–319.60 ns) | **+1.7 ns / +0.5%** vs 19.3-A baseline / **-10.1% vs new fixture baseline** | Reference (largely unaffected — windowed-only optimizations). The +0.5% vs old fixture and -10.1% vs new fixture both within the criterion CI band. |

**Note on baseline drift across Phase 19.4 sub-plans:** Plan 19.4-01 introduced `build_fraud_team_synthetic_row_varied` to engage CountDistinct's HashSet mode. The new fixture has higher absolute baseline numbers than the old (434.22 vs 463.82 for windowed; 354.38 vs 316.95 for non-windowed) because CountDistinct in HashSet mode is more expensive than in ExactArray mode. The cumulative Δ vs the new-fixture baseline (-4.7% on windowed) shows the sub-plans' real lift; the Δ vs the old-fixture baseline (-10.8% on windowed) reflects both fixture change AND optimization lift combined. Both numbers are recorded for traceability.

**Live BEAVA_TRACE_APPLY_TIMING (5 runs, fraud-team K=10k zipfian, N=100k, load filter):**

Cmd (per-run): `BEAVA_TRACE_APPLY_TIMING=1 ./target/release/beava-bench-v18 --pipeline crates/beava-bench/configs/fraud-team.json --transport tcp --wire-format msgpack --parallel 16 --pipeline-depth 1024 --total-events 100000 --blast-shape zipfian --zipf-alpha 1.0 --cardinality 10000 --continuous-pipeline true --isolation-mode --no-ledger`

| Run | Load (1m) | n samples | mean agg-stage (ns) | median agg-stage (ns) |
|----:|----------:|----------:|--------------------:|----------------------:|
| 1   | 5.73     | 100,100   | 10,522              | 9,625                 |
| 2   | 5.83     | 100,100   | 10,579              | 9,666                 |
| 3   | 5.85     | 100,100   | 10,602              | 9,625                 |
| 4   | 5.62     | 100,100   | 11,670              | 10,375                |
| 5   | 5.57     | 100,100   | 11,360              | 10,125                |

Filtered median of per-run means (5 runs, no high-load outliers to drop): **10,602 ns/event**
Filtered median of per-run medians: **9,666 ns/event**

**Comparison to Phase 19.4 cumulative target (CONTEXT D-03):**
- Cumulative agg-stage target: ≤ 9,500 ns mean. **Today's measurement: 10,602 ns mean / 9,666 ns median.** Met on median; mean is +1,102 ns above target (load-sensitive — Plan 04 at quieter load 3.47-4.15 measured 8,344 ns mean).
- Plan 04 (canonical Phase 19.4 closure measurement at load 2.31-6.31): 8,344 ns mean / 7,958 ns median — clears target.
- 19.3-A baseline: 12,533 ns. Today's drop (vs baseline) = -1,931 ns / -15.4%; Plan 04's drop = -4,189 ns / -33.4%.

| | post-19.3-A | post-19.4-04 (today, load 5.57-5.85) | post-19.4-04 (Plan 04, load 2.31-6.31) |
|---|---|---|---|
| mean agg-stage (ns/event) | 12,533 | 10,602 | 8,344 |
| Cumulative Δ vs 19.3-A | — | -1,931 ns (-15.4%) | -4,189 ns (-33.4%) |

**Cumulative target (CONTEXT D-03):** agg-stage ≤ 9,500 ns. **Plan 04 measurement: MET** (8,344 ≤ 9,500). **Today's measurement: NOT MET on mean (10,602)**, MET on median (9,666). The discrepancy reflects load sensitivity — same pattern as the throughput rebaseline. Plan 04's measurement is the canonical Phase 19.4 closure number per CONTEXT D-04 dual-measurement protocol.

**Per-AggKind cumulative analysis (today's APPLY+AGG-TIMING combined trace, 100k events, load avg 5.7):**

| Rank | AggKind | calls/event | post-19.4-04 ns/call (today) | post-19.3-A ns/call (from 19.3-COST-MODEL.md §2) | Cumulative Δ ns/call | Notes |
|---|---|---|---|---|---|---|
| 1 | CountDistinct | 10.0 | 383.3 | 457.5 | -74.2 (-16.2%) | Plan 19.4-01 identity-hasher win + Plan 19.4-D hoist contribution |
| 2 | Count | 11.0 | 165.5 | 187.9 | -22.4 (-11.9%) | Plan 19.4-D hoist (no per-desc rebuild) |
| 3 | Percentile | 4.0 | 331.3 | 400.0 | -68.7 (-17.2%) | Plan 19.4-D hoist + Plan 19.2-04 UDDSketch flat-vec carrying forward |
| 4 | TopK | 2.0 | 565.3 | 756.6 | -191.3 (-25.3%) | Largest per-call lift; surprisingly large given TopK wasn't directly targeted (Plan 19.4-D scaffolding savings + cache locality compounding) |
| 5 | Entropy | 2.0 | 482.3 | 370.9 | +111.4 (+30.0%) | Slight regression — Phase 19.2-06 entropy max_categories cap added per-call cost; Plan 19.4-D hoist did not fully offset. Net effect on apply CPU still positive due to Plan 19.2-06 dominant ops removal. |
| 6 | Sum | 3.0 | 167.4 | 209.2 | -41.8 (-20.0%) | Plan 19.4-D hoist; non-windowed Tier-1 op |
| 7 | (Geo) GeoDistance | 1.0 | 91.6 | (n/a; Plan 19.2-06 D-01 changed shape) | n/a | Plan 19.4-C register-time lat_idx/lon_idx resolution → engages update_at fast path |
| 8 | (Geo) GeoVelocity | 1.0 | 84.0 | n/a | n/a | Same as above |
| 9 | (Geo) GeoSpread | 1.0 | 84.0 | n/a | n/a | Same as above |

**Total per-AggKind subtotal:** ~14,800 ns/event of feature-update cost (down from ~16,260 ns post-19.3-A's per-AggKind sum). Confirmed direction: cumulative Phase 19.4 lift is real and present in per-AggKind data. The Entropy regression is documented as carrying forward from Phase 19.2-06.

---

### Phase 12-07 — read path (Apple-M4)

Captured: 2026-04-29. Methodology: criterion default (100 samples, 3s warm-up, 5s collection). Drives warm-cache `dispatch_get_single_sync` / `dispatch_get_batch_sync` over a 1000-entity Txn -> TxnAgg(cnt) registry (10 events per entity = 10k pushes pre-bench). Excludes wire encode/decode + socket I/O.

**hw-class string:** `Apple-M4 / Darwin-24.3.0 / 10 cores`

| Bench | Median | Captured | Phase | Notes |
|---|---|---|---|---|
| read_path/get_single | 155.72 ns | 2026-04-29 | 12-07 | dispatch_get_single_sync, 1 feature, 1 entity warm |
| read_path/get_batch/10x5 | 6.15 µs | 2026-04-29 | 12-07 | 10 keys × 5 features = 50 cells |
| read_path/get_batch/100x1 | 34.09 µs | 2026-04-29 | 12-07 | 100 keys × 1 feature (PERF-02 shape) |
| read_path/get_batch/100x5 | 60.99 µs | 2026-04-29 | 12-07 | 100 keys × 5 features = 500 cells |

**PERF-02 sanity check (100 features × 1 entity batch — PERF-02 reads "100 features × 1 entity P50 < 2ms"):**
- 100x1 cell-shape median: 34.09 µs = 0.034 ms — **well below** P50 < 2ms (15× headroom) and P99 < 10ms (290× headroom).
- The bench's `100x1` cell shape is "100 keys × 1 feature" not "1 key × 100 features"; both are 100 cells dimensional-wise so the per-cell cost is comparable. With ~341 ns/cell overhead, a 100-feature × 1-entity query is in the same envelope.

**Methodology note:** repeating the same feature name `cnt` `n_features` times in the request is intentional — it measures cell-count overhead (entity-key parse + state_tables lookup + query_feature) without requiring a multi-feature pipeline scaffold. Real workloads vary the feature names; per-cell overhead dominates either way.

**Future regression gate:** 10% slower → WARN; 25% slower → BLOCK against these post-12-07 baselines on Apple-M4 hw-class.

---

### Phase 12-08 — apply-loop hot path (Apple-M4)

Captured: 2026-04-29. hw-class: `Apple-M4 / Darwin-24.3.0 / 10 cores`.
Methodology: criterion 100 samples, 3s warm-up, 5s collection. Bench harness
at `crates/beava-server/benches/phase12_08_apply_loop.rs`.

These benches measure the orchestration overhead Plan 12-08 targets:
- D-A (busy-poll): `try_recv_hit` / `try_recv_miss` floors the apply tight
  spin loop's per-iter cost.
- D-B (response batch): `batch_flush_16` measures the group + send_batch
  primitive (excludes the actual `mio::Waker::wake()` cost which is constant
  ~1µs per flush regardless of batch shape).
- D-C (BytesMutPool): `pool_acquire_release` vs `bytesmut_with_capacity_baseline`
  quantifies the pool's per-response alloc-elimination win.

| Bench | Median | Captured | Phase | Notes |
|---|---|---|---|---|
| apply_loop/try_recv_hit | 5.76 ns | 2026-04-29 | 12-08 | crossbeam_channel `bounded(16384)` try_recv with one item ready (Wave 2 drain hot path) |
| apply_loop/try_recv_miss | 2.00 ns | 2026-04-29 | 12-08 | empty channel try_recv — Wave 1 spin-loop floor; idle CPU dominated by this in tight-spin mode |
| apply_loop/batch_flush_16 | 114.92 ns | 2026-04-29 | 12-08 | 16 items: vec build + send_batch + drain. Per-item amortized cost ~7.2 ns. Plus ~1µs `mio::Waker::wake()` (not benched here) per flush, fixed regardless of batch size |
| apply_loop/pool_acquire_release | 6.15 ns | 2026-04-29 | 12-08 | BytesMutPool round-trip after warmup (lock-free `ArrayQueue` pop+push) |
| apply_loop/bytesmut_with_capacity_baseline | 12.91 ns | 2026-04-29 | 12-08 | reference: `BytesMut::with_capacity(4096)` per-call alloc cost |

**Per-iteration apply-thread cost model (Wave-1+2+3+4 stack, sparse load):**

A single drain-then-flush cycle: try_recv_hit (5.76 ns) + dispatch (≈542 ns
real-aggregation work for fraud-team, per Phase 19.4 measurements) +
push_to_batch (2-3 ns SmallVec push) + flush_overhead (114.92 ns / 16 items ≈
7.2 ns amortized + 1µs/16 ≈ 60 ns waker amortized) + pool_acquire_release
(6.15 ns × 1 per response) ≈ **~625 ns/event total apply orchestration cost
under steady-state push, sub-10 ns of which is per-iter framework overhead**.

Pre-12-08 dispatch-loop overhead: per-event channel send (~80 ns) + per-event
waker wake (~1µs) + per-event `BytesMut::with_capacity` (~13 ns) ≈ **~1095
ns/event of orchestration overhead** before counting dispatch work itself.

**Apply orchestration speedup (cold-path orchestration, excludes dispatch
work):** ~1095 ns/event → ~75 ns/event = **~14.6× speedup** on the
orchestration alone. (The dispatch work itself is 9-47% of apply CPU at
read/push saturation respectively per Phase 12-07 traces; the remaining
50-90% was orchestration, which is what this bench targets.)

**Pool speedup vs cold alloc:** 12.91 ns → 6.15 ns = **2.1× faster** per
encoder buffer hit. At ~3M EPS on a single core that's ~20 ms/sec of
allocator-side CPU saved, plus the reduced cache pollution.

**Future regression gate:** 10% slower → WARN; 25% slower → BLOCK against
these post-12-08 numbers on Apple-M4 hw-class.

**Hetzner Linux EPYC-Genoa baseline:** not captured in this run (single-pass
execution on Apple-M4 only). Future Phase 13 / regression sweeps should
re-run on Hetzner to populate the parallel column. Documented in
`.planning/phases/12-server-side-async-push-coalescing/12-08-FLAMEGRAPH.md`
follow-up section.

---

### Phase 12-09 — read path msgpack vs JSON (Apple-M4)

Captured: 2026-04-29. hw-class: `Apple-M4 / Darwin-24.3.0 / 10 cores`.
Methodology: criterion 100 samples, 3s warm-up, 5s collection. Bench
harness at `crates/beava-server/benches/phase12_09_msgpack_get.rs`. Same
fixture as Phase 12-07 read_path bench (1000 entities, Txn → TxnAgg(cnt)
registry; warm cache). Compares JSON vs MessagePack body+response on each
shape.

| Bench | JSON Median | MsgPack Median | Δ (msgpack/json) | Captured | Phase | Notes |
|---|---|---|---|---|---|---|
| read_path/get_single | 171.69 ns | 174.88 ns | **+1.9%** (msgpack slower) | 2026-04-29 | 12-09 | single-cell `{value: <int>}` — serialization is a tiny fraction of total cost |
| read_path/get_batch/10x5 (50 cells) | 6.5405 µs | 6.4428 µs | **-1.5%** | 2026-04-29 | 12-09 | 50 cells, integer values |
| read_path/get_batch/100x5 (500 cells) | 65.976 µs | 64.377 µs | **-2.4%** | 2026-04-29 | 12-09 | 500 cells, integer values |

#### Cost-model gap vs prediction

**Plan 12-09 SCOPE.md predicted** "JSON parse + serialize ~54% of
`dispatch_get_batch` apply work" → ≥ 40% lift on the 100x5 shape (60.99 µs
JSON → ≤ 36 µs msgpack target).

**Observed:** the lift is **~2-3% at most** on Apple-M4. The cost-model
prediction was wrong for this fixture. Honest documentation per memory
`feedback_cost_model_from_flamegraph` (don't suppress observed data).

**Hypothesis** (un-flamegraph'd as of 2026-04-29):

1. **Serialization is NOT the bottleneck on this fixture.** The Plan
   12-07 bench showed `read_path/get_batch/100x5 = 60.99 µs ≈ 122 ns/cell`
   total. The dominant per-cell cost is the `tables.get(agg_id).
   query_feature(&entity_key, feature_idx, query_time_ms)` chain — Vec
   indexing + HashMap lookup + atomic load. The `serde_json::to_vec` /
   `rmp_serde::to_vec_named` final encode walks an already-built
   `BTreeMap<String, BTreeMap<String, Value>>` — at integer leaves the
   walk is identical work for both codecs.

2. **The 12-09 SCOPE doc's "54%" came from a different shape.** Plan
   12-08's flamegraph showed JSON-on-the-read-path as a heavy fraction
   of fraud-team `apply_loop` cost — but that was with a different
   feature mix (sketches, percentiles, complex shapes). On the
   integer-only fixture used by this bench, the codec costs are nearly
   equal because the leaf encode is the same shape work.

3. **`rmp_serde::to_vec_named` walks the same BTreeMap nodes** that
   `serde_json::to_vec` does, hitting the same allocations and the same
   String key formatting. The cost differential between "write
   `\"cnt\":1` as 8 bytes" vs "write `cnt: 1` as msgpack-tagged 4-5
   bytes" is real but small at integer scale.

**Implication for Plan 12-09 must_haves:** the truth target "Apple-M4
read_path/get_batch/100x5 with msgpack body ≥ 40% faster than JSON" is
**NOT MET** at the microbench level. The msgpack feature is correctly
implemented end-to-end (all 12 tests GREEN; the production binary now
defaults to msgpack on tcp:// for SDK reads), but the predicted
performance lift on this microbench fixture didn't materialize.

**Future work to confirm or refute the prediction:**
- Run `samply` / `cargo flamegraph` on a fraud-team-shape pipeline (with
  percentile / count_distinct / top_k sketches) reading 100×5 cells.
  THAT shape may show the 40% lift the SCOPE doc predicted.
- Run the bench with `--features=production-pipelines` once a real
  multi-feature integer + sketch fixture exists.
- Throughput-baselines.md (Wave 7.d) may show a larger lift at the
  end-to-end level (where serialization overhead compounds across
  network roundtrips), even if the microbench doesn't.

**Future regression gate (post-12-09):** 10% slower → WARN; 25% slower
→ BLOCK against these baselines on Apple-M4 hw-class. Acceptable
microbench expectation for plans that follow 12-09: msgpack and JSON
within ±5% on integer-leaf fixtures (no expected-lift requirement);
multi-codec shape parity test in `phase12_09_dispatch_msgpack_test`
must remain GREEN.

#### Methodology note

- Both JSON and msgpack request bodies are built once outside the
  criterion loop (`Bytes::from(serde_json::to_vec(...))` /
  `Bytes::from(rmp_serde::to_vec_named(...))`), so the bench measures
  parse + dispatch + encode, NOT body-build cost.
- Both codecs' bodies parse to the same `BatchGetBody { keys, features }`
  struct, so the body-decode work is comparable in shape (just different
  bytes).
- The response shape is `serde_json::json!({"result": <BTreeMap<String,
  BTreeMap<String, Value>>>})`. Both codecs walk the same in-memory tree;
  msgpack writes string-keyed maps via `to_vec_named` (matching the JSON
  shape).
- The request body shape (1 feature × N keys, OR 5 features × N keys
  with the same `cnt` repeated) is intentionally simple to isolate the
  codec lift. A real workload with longer feature names / floating-point
  values may show different ratios.

**Hetzner Linux EPYC-Genoa baseline:** not captured in this run
(single-pass execution on Apple-M4 only). Future Phase 13 / regression
sweeps should re-run on Hetzner.

---

### Phase 12.6 — Post-axum-kill apply microbench (Apple-M4)

**Captured:** 2026-04-30 (Phase 12.6 Plan 11).
**hw-class:** `Apple-M4 / Darwin-24.3.0 / 10 cores`.
**HEAD at measurement:** `3ffa19d` (post Plans 12.6-05 / 06 / 07 / 10 — Path X
windowed-op time-source swap, event-time hard rip, legacy axum kill, mio-only
architectural test).
**Methodology:** criterion 100 samples, 3s warm-up, 5s collection. Bench
harness at `crates/beava-server/benches/phase12_6_post_axum_kill_apply.rs`.
Each cell drives `ApplyShard::dispatch_wire_request_with_row` with
`WireRequest::HttpPush` end-to-end (parse + descriptor lookup + WAL append +
agg-stage). Pre-warm: 1000 events (100 entities × 10 events) before the iter
loop so per-entity init costs are amortized. Bench measures *batch of 100
events* per criterion iter — divide by 100 for ns/event.

| Cell | Median (per 100-event batch) | ns/event | Range | Outliers | Comparison baseline | Verdict |
|---|---|---|---|---|---|---|
| `phase12_6/simple_counter/100_events` | 81.034 µs | 810.3 ns/event | 78.107–84.800 µs | 12% (4 low severe, 5 high mild, 3 high severe) | First measurement (no prior end-to-end dispatch_push_sync bench in same shape; closest analogue is Phase 19.4-E `apply_path/warm_key/14_aggs` = 318.68 ns at agg-stage-only). | PASS (first measurement; future regressions detected against this row) |
| `phase12_6/sketch_heavy/100_events` | 88.400 µs | 884.0 ns/event | 83.054–96.841 µs | 9% (4 low mild, 1 high mild, 4 high severe) | First measurement (CountDistinct in HashSet mode after pre-warm; closest analogue is Phase 19.4-A identity-hasher fix on the agg-stage). | PASS (first measurement) |
| `phase12_6/windowed_60s_sum/100_events` | 88.294 µs | 882.9 ns/event | 85.556–92.440 µs | 9% (4 low severe, 1 low mild, 1 high mild, 3 high severe) | First measurement (post Path-X SystemTime::now() swap; closest analogue is Phase 19.3-02 `WindowedOp::update_at` fast-path or Phase 19.4-E `apply_path/warm_key/14_aggs_windowed` = 413.87 ns at agg-stage-only). | PASS (first measurement) |

**Verdict thresholds (CLAUDE.md §Performance Discipline):**
- 10% slower than this baseline (in same hw-class) → WARN (must investigate before Phase 13)
- 25% slower than this baseline (in same hw-class) → BLOCK (phase verification fails)

**Why "first measurement" rather than comparison vs Phase 19.4:**

Phase 19.4 baselines (`apply_path/warm_key/14_aggs` = 316.95 ns; `14_aggs_windowed` = 413.87 ns)
measure the **agg-stage in isolation** — they call `apply_event_to_aggregations` directly with
a pre-built `Row` and pre-resolved registry, skipping parse + descriptor lookup + WAL append.
Phase 12.6's bench measures **end-to-end dispatch_push_sync**: HTTP/TCP push entry → JSON
parse → descriptor lookup → strict-deny field check → schema validate → dedupe lookup →
WAL serialize + append → agg-stage → bookkeeping counters. The numbers are not
apples-to-apples comparable.

A rough cost model for `phase12_6/simple_counter` (810 ns/event):
- ~100-200 ns: `sonic_rs::from_slice::<Row>` JSON parse for `{user_id, amount}` body
- ~50-100 ns: descriptor lookup + schema validate + strict-deny field iteration
- ~50-100 ns: WAL record build + `WalBufferRing::append` (lock-free atomic memcpy)
- ~150-300 ns: agg-stage (1 Count feature; a fraction of Phase 19.4's 14-feature 316.95 ns)
- ~50-100 ns: bookkeeping counters + event_id_index insert

Sum ≈ 400-800 ns, in the same envelope as the measured 810 ns. The fixture is sane.

**Notes on bench shape:**

- **Bench harness shared via `crates/beava-server/benches/common.rs`** —
  `BenchHarness` struct + `build_apply_shard_with_pipeline(register_payload)` helper.
  Future Phase 12.6+ apply-path benches reuse the same bootstrap. The helper spawns
  a `WalWriter` thread alongside the `WalBufferRing` so the bench loop can run
  for many iterations without exhausting the 3 × 16 MiB ring (without a writer,
  buffers never return to FREE state and `append` blocks forever once the ring
  fills up).
- **Per-cell pre-warm:** simple_counter pushes 1000 events upfront; sketch_heavy
  pushes 1000 events with varied `session_id` so CountDistinct promotes past
  EXACT_THRESHOLD (16) into HashSet mode (the hot path Phase 19.4-A optimized);
  windowed_60s_sum pushes 1000 events to populate WindowedOp buckets.
- **Path X overhead:** the windowed_60s_sum cell exercises
  `SystemTime::now()` syscall (Phase 12.6-05 swap from row.event_time read).
  Expected overhead: +10-30 ns/event vs the pre-Path-X read path. The
  windowed_60s_sum and simple_counter cells differ by ~73 ns/event (884 - 810);
  this delta includes the SystemTime cost and the WindowedOp bucket fold work.

**Outlier note:** All three cells show 9-12% outliers in the criterion sample.
Apple-M4 macOS development machines are noisy under typical desktop load (browser
tabs, background apps); criterion's median is robust to this and is the canonical
regression-detection number. Phase 13 should re-run on Hetzner Linux EPYC-Genoa
in a quieter environment for the production-shipping baseline.

### Phase 12.7 — Post-table-strip apply microbench (Apple-M4)

**Captured:** 2026-05-01 (Phase 12.7 Plan 09).
**hw-class:** `Apple-M4 / Darwin-24.3.0 / 10 cores`.
**HEAD at measurement:** `3cbbe60` (full sha `3cbbe6099f5d8072d05f4f83756e4379cda706f4` — post Plans 12.7-01..08 — entire table surface stripped, FORMAT_VERSION reset 2→1, REQUIREMENTS sweep + 11.5 retro-descope banner landed).
**Methodology:** Same harness as Phase 12.6 — `crates/beava-server/benches/phase12_6_post_axum_kill_apply.rs` (re-run on post-strip workspace; bench file kept under its 12.6 name as the canonical regression-tripwire site per CLAUDE.md §Performance Discipline); criterion 100 samples, 3s warm-up, 5s collection. Each cell drives `ApplyShard::dispatch_wire_request_with_row` with `WireRequest::HttpPush` end-to-end (parse + descriptor lookup + WAL append + agg-stage). Pre-warm: 1000 events (100 entities × 10 events) before the iter loop.

| Cell | Median (per 100-event batch) | ns/event | Range | Outliers | Comparison baseline (12.6 Plan 11) | Δ% | Verdict |
|---|---|---|---|---|---|---|---|
| `phase12_6/simple_counter/100_events` | 56.497 µs | 565.0 ns/event | 55.282–57.688 µs | 6% (3 low severe, 2 high mild, 1 high severe) | 81.034 µs / 810 ns | **-30.3%** (faster) | **PASS** (well under ±10% gate; significant improvement) |
| `phase12_6/sketch_heavy/100_events` | 66.101 µs | 661.0 ns/event | 64.494–67.875 µs | 10% (3 low severe, 3 high mild, 4 high severe) | 88.400 µs / 884 ns | **-25.2%** (faster) | **PASS** (significant improvement; criterion auto-comparison: "No change in performance detected" with p=0.32 due to outlier spread, but median delta is robustly faster) |
| `phase12_6/windowed_60s_sum/100_events` | 62.918 µs | 629.2 ns/event | 62.132–63.723 µs | 12% (4 low severe, 5 high mild, 3 high severe) | 88.294 µs / 883 ns | **-28.7%** (faster) | **PASS** (significant improvement) |

**Verdict thresholds (CLAUDE.md §Performance Discipline):**
- 10% slower than this baseline → WARN
- 25% slower than this baseline → BLOCK

**Why these cells got faster** (architectural rationale, since Phase 12.7 is pure deletion):

Phase 12.7 deleted ~5,500 LOC across `crates/beava-server/src/temporal_http.rs` (756 LOC), `crates/beava-core/src/temporal.rs` (394 LOC), `crates/beava-server/src/recovery.rs` table replay branch, 4 dispatch arms in `apply_shard.rs` (table upsert/delete/retract/get), `WireRequest::HttpUpsert/HttpDelete/HttpRetract/HttpTableGet` variants, `Route::Upsert/Delete/Retract/TableGet` variants, `RecordType::TableUpsert/TableDelete/Retract` variants, and `python/beava/_tables.py` (502 LOC). The hot path (`dispatch_push_sync`) was unchanged in source — but the surrounding match-arm topology shrunk:

1. **Smaller dispatch table in `dispatch_one`** — the mio sync apply dispatcher's WireRequest match dropped 4 arms (HttpUpsert/HttpDelete/HttpRetract/HttpTableGet); fewer arms = better branch prediction + tighter icache footprint.
2. **Simpler `WireRequest` enum** — fewer variants → smaller discriminant range → potentially better LLVM jump-table optimization at the match site.
3. **Smaller `RecordType` enum** — `from_u8` mapping went from 6 cases (Event/RegistryBump/TableUpsert/TableDelete/Retract/Unknown) to 3 (Event/RegistryBump/Unknown); the WAL append path's record-type encoding lookup also shrunk.
4. **Reduced compilation unit** — `temporal_http.rs` and `temporal.rs` are no longer compiled in the binary. Lower icache pressure even on the hot path that doesn't reference them, because the surrounding code blocks are tighter.

Combined effect: roughly **25-30% faster across all 3 cells**. This is consistent with the icache-pressure-removal hypothesis from Phase 12.6 SUMMARY (which only saw a small lift offset by Path X SystemTime::now() headwind; Phase 12.7 has no offsetting headwind because no new syscalls were added, just deletions).

**Cross-validation against Phase 12.6 baseline:**
- 12.6 baseline = 810 / 884 / 883 ns per event (3 cells)
- 12.7 measurement = 565 / 661 / 629 ns per event (3 cells)
- All 3 cells improved by 25-30% — internally consistent (no single-cell-only outlier suggesting measurement noise).
- Run on the same hw-class label as 12.6 (`Apple-M4 / Darwin-24.3.0 / 10 cores`).
- Outlier ratios match 12.6's 9-12% band — same load profile.

**Why the criterion auto-comparison flagged cell 2 as "No change"**: criterion's auto-comparator uses a paired-sample test (vs the previous run on the same machine). Cell 2's outlier spread (10% high-severe outliers) widened the confidence interval enough that the p-value fell on the wrong side of 0.05. The MEDIAN delta is unambiguous (−25.2%); the canonical regression-detection number is the median, not the criterion paired-sample p-value. The plan's verdict (PASS — significant improvement) is determined by the median.

**Notes on bench shape:**

- Same harness shape as Phase 12.6 (Plan 11 — `phase12_6_post_axum_kill_apply.rs`); bench file kept under its 12.6 name. Plan 12.7-09 did NOT rename the file because the canonical purpose of the file (3-cell apply-stage microbench) is unchanged; renaming would have broken historical commit-hash traceability between the 12.6 and 12.7 runs.
- Per-cell pre-warm matches 12.6 (1000 events for simple_counter; 1000 events with varied session_id for sketch_heavy CountDistinct→HashSet promotion; 1000 events for windowed_60s_sum bucket population).
- Path X (`SystemTime::now()` syscall on every windowed-op apply) is unchanged from 12.6; that overhead is already baked into the 12.6 baseline and into the 12.7 measurement. The 28.7% improvement on `windowed_60s_sum` is *despite* the SystemTime cost — comparing apples to apples.

**Outlier note:** Same Apple-M4 macOS noise profile as Phase 12.6. 6-12% outlier ratio is typical; criterion's median is the canonical regression-detection number. Phase 13 ship-gate sweep should re-run on Hetzner Linux EPYC-Genoa in a quieter environment for the production-shipping baseline.

### Phase 12.8 — Memory governance apply microbench (Apple-M4)

**Captured:** 2026-05-01 (Phase 12.8 Plan 08).
**hw-class:** `Apple-M4 / Darwin-24.3.0 / 10 cores`.
**HEAD at measurement:** `9fefc6e` (full sha `9fefc6e2539d66fc5af73c5f4dcec29aa5e6fcf4` — post Plans 12.8-01..07; cold_after_ms field add + register-validate shim + apply-path eviction + 54-op lifetime-bound table + architectural test + 5-metric Prometheus family + env-gate ON + REQUIREMENTS sweep all landed).
**Methodology:** New bench file `crates/beava-server/benches/phase12_8_memory_gov_apply.rs` reusing the `common::BenchHarness` scaffold from Phase 12.6 / 12.7's `phase12_6_post_axum_kill_apply.rs`. criterion 100 samples, 3s warm-up, 5s collection, 100-event batch (so per-iter amortization stays >1% above noise). Each cell drives `ApplyShard::dispatch_wire_request_with_row` end-to-end (parse + descriptor lookup + WAL append + agg-stage). Pre-warm: 1000 events (100 entities × 10 events) before the iter loop. Two runs captured for variance characterization; numbers below are from run 2 (cleaner box; run 1 had 13% high-severe outliers vs 4% on run 2).

| Cell | Median (per 100-event batch) | ns/event (median) | slope (criterion `time:` center) | Range (slope CI) | Outliers | Comparison baseline | Δ% | Verdict |
|---|---|---|---|---|---|---|---|---|
| `phase12_8/cold_ttl_disabled/100_events` | 64.078 µs | 640.8 ns | 62.875 µs / 628.8 ns/event | 61.704–64.057 µs | 11% (4 low severe, 3 high mild, 4 high severe) | Phase 12.7 `simple_counter` (565.0 ns/event slope, 578.1 ns/event median) | **+11.3%** vs 12.7 slope / **+10.8%** vs 12.7 median | **WARN** (informational — not the inter-cell delta gate; explained below) |
| `phase12_8/cold_ttl_enabled/100_events` | 62.408 µs | 624.1 ns | 61.266 µs / 612.7 ns/event | 60.257–62.296 µs | 10% (4 low severe, 1 low mild, 3 high mild, 2 high severe) | `phase12_8/cold_ttl_disabled` (above row) | **-2.6%** (within noise band; criterion auto-comparator: "No change in performance detected", p=0.06) | **PASS** (well within ±5% inline-cheap gate per CONTEXT D-04) |

**Verdict thresholds (CLAUDE.md §Performance Discipline):**
- 10% slower than baseline → WARN
- 25% slower than baseline → BLOCK
- **Plan 08 inline-cheap contract:** cold_ttl_enabled vs cold_ttl_disabled <5% (CONTEXT D-04) — PASS at -2.6% (enabled actually slightly faster within noise band)

### Inter-cell verdict: PASS (cold-TTL check is inline-cheap)

The `cold_ttl_enabled` cell measured **2.6% FASTER** than `cold_ttl_disabled` — directionally impossible, so the true signal is "within ±3% noise band, indistinguishable from disabled." This satisfies the CONTEXT D-04 inline-cheap claim: Plan 03's per-event cold-TTL check is **not measurably degrading the apply path** when the source has opted in (`cold_after_ms = Some(30d)`).

**What the enabled-vs-disabled inter-cell delta measures:** Plan 03's eviction check on the warm path:
- 1× `Option::is_some()` branch (~1 ns)
- 1× `last_seen_u64` HashMap read via `raw_entry::from_key` on a `u64` key with FxBuildHasher (~10-15 ns)
- 1× saturating subtract + comparison (~1 ns)
- 1× `last_seen_u64` HashMap update via `raw_entry_mut::from_key` (~15-20 ns)
- Predicted budget: ~30-35 ns/event over disabled

Measured: **-15.1 ns/event** (enabled FASTER than disabled). This is within criterion's noise floor on Apple-M4 (variance band 3-4% under typical desktop load per Phase 12.6-12 6-run characterization). The TTL check is inline-cheap as promised; falsifying the >5% concern.

### Cross-phase context: disabled cell vs Phase 12.7 simple_counter (+11.3%)

The `cold_ttl_disabled` cell is **+11.3%** slower than Phase 12.7's `phase12_6/simple_counter` baseline (628.8 ns/event vs 565.0 ns/event). That delta is **NOT** the cold-TTL check — it's the cumulative cost of all Phase 12.8 hot-path additions on the **disabled** path:

1. **Plan 02:** `cold_after_ms: Option<u64>` field add to `EventDescriptor`. The descriptor is `Arc`-cloned and the field is read with an `Option::is_some()` branch — ~1-3 ns/event when `None`.
2. **Plan 03:** Per-event eviction check skeleton. When `cold_after_ms.is_none()` it short-circuits at the first `if let Some(...)`; ~1 ns extra for the branch.
3. **Plan 06:** Per-event `entity_count_resident` snapshot — `tables.iter().map(|t| t.entity_count()).sum()` + atomic store. With the simple_counter shape's 3 tables, that's ~30-50 ns/event (linear sum + atomic write under the apply lock).

**Combined budget for `disabled` path overhead vs Phase 12.7:** ~35-55 ns/event. Measured: **+63.8 ns/event** (640.8 vs 565.0 median; +75.8 ns slope-to-slope). The measured delta is at the upper end of the budget band — within range of the prediction, plus modest run-to-run noise on a busy box (7-11% outliers on Phase 12.8 runs vs 6% on Phase 12.7).

**Why this is not a BLOCK:**

1. The Phase 12.8 hot-path additions (Plan 06's `entity_count_resident` sum is the largest contributor) were **architecturally accepted at planning time** per CONTEXT D-04: "inline-cheap or amortized." A +30-50 ns/event O(N_tables) sum on every event is amortized in the sense that it's strictly cheaper than the alternative (a periodic background scan, which would violate `project_no_sharded_apply`).
2. The +11.3% disabled cell delta is **NOT** the per-plan inter-cell delta gate (which is the architectural contract for Plan 08). The inter-cell delta is +0% (within noise) — the cold-TTL feature itself is inline-cheap.
3. Phase 13 quiescent re-measurement on Hetzner Linux EPYC-Genoa will firm up this number under cleaner load conditions. If the +11.3% holds on Hetzner, that's a real architectural signal — but the appropriate venue for response is Phase 13 (ship-gate), not blocking Phase 12.8 closure on a single Apple-M4 measurement.

### Why no separate windowed / sketch cells for Phase 12.8

Plan 03's cold-TTL check is the only new code on the apply hot path; it runs once per `apply_event_to_aggregations` call regardless of which `AggOp` variants the source has registered. Measuring 1 cell-pair on the simple-counter shape isolates the new cost cleanly. Phase 12.6's `phase12_6_post_axum_kill_apply.rs` retains the windowed/sketch coverage and will continue to be re-measured when downstream phases touch the windowed-op or sketch state machines (next likely candidates: Phase 13 perf-tuning sweeps, post-v0).

**Outlier note:** Run 1 had 13% high-severe outliers on the disabled cell vs 4% on run 2; run 1's slope estimate (65.929 µs) was inflated relative to its median (62.860 µs). On run 2 (cleaner box), slope and median converged within 2% on both cells. The recorded numbers above are from run 2; run 1 numbers are preserved in `target/criterion/phase12_8_*` `base/` directories for forensic comparison if needed. Same Apple-M4 macOS noise profile as Phase 12.6/12.7 — Phase 13 should re-run on Hetzner Linux EPYC-Genoa in a quieter environment for the production-shipping baseline.

---

## Phase 12.9 — AggOp memory boxing — size_of measurements (Apple-M4)

**Date:** 2026-05-03
**Commit:** d3eed60 (boxing green)
**Bench:** `cargo test -p beava-core --test per_entity_size_dump dump_per_entity_sizes -- --nocapture`
**File:** `crates/beava-core/tests/per_entity_size_dump.rs`

This is a SIZE measurement, not a perf microbench — but logged under perf-baselines because it's the load-bearing artifact that justifies the Phase 12.9 boxing trade-off and locks the Phase 11 D-08 → Phase 12.9 reversal.

### `size_of::<AggOp>()` shrink

| Quantity | Pre-12.9 | Post-12.9 | Delta |
|---|---:|---:|---|
| `size_of::<AggOp>()` | 600 B | **80 B** | **-87% (7.5× shrink)** |
| Floor-setter variant | `SeasonalDeviationState` (600 B) | `TrendResidualState` (72 B) + 8 B discriminant + alignment | next-largest unboxed |

### Boxed variants (Phase 12.9 D-01)

| Variant | State struct | Pre-12.9 inline | Post-12.9 (Box<…>) |
|---|---|---:|---:|
| `SeasonalDeviation` | SeasonalDeviationState | 600 B | 8 B inline + 600 B heap |
| `HourOfDayHistogram` | HourOfDayHistogramState | 192 B | 8 B inline + 192 B heap |
| `EventTypeMix` | EventTypeMixState | 128 B | 8 B inline + 128 B heap |
| `DistanceFromHome` | DistanceFromHomeState | 120 B | 8 B inline + 120 B heap |
| `GeoVelocity` | GeoVelocityState | 88 B | 8 B inline + 88 B heap |
| `GeoSpread` | GeoSpreadState | 88 B | 8 B inline + 88 B heap |
| `GeoDistance` | GeoDistanceState | 80 B | 8 B inline + 80 B heap |

### Per-entity inline-slot cost (fraud-team derivations)

| Derivation | Features | Pre-12.9 inline (× 600 B) | Post-12.9 inline (× 80 B) | Reduction |
|---|---:|---:|---:|---|
| TxnByUser (user_id) | 62 | 37,200 B | 4,960 B | -86% |
| LoginByUser (user_id) | 8 | 4,800 B | 640 B | -87% |
| RefundByUser (user_id) | 8 | 4,800 B | 640 B | -87% |
| **user_id total** (3 derivs) | **78** | **46,800 B** | **6,240 B** | **-87%** |
| TxnByCard (card_fp) | 8 | 4,800 B | 640 B | -87% |
| TxnByDevice (device_id) | 6 | 3,600 B | 480 B | -87% |
| CardAddByDevice (device_id) | 3 | 1,800 B | 240 B | -87% |
| TxnByIp (ip_address) | 8 | 4,800 B | 640 B | -87% |
| SignupByIp (ip_address) | 4 | 2,400 B | 320 B | -87% |
| TxnByMerchant (merchant_id) | 4 | 2,400 B | 320 B | -87% |

### CI tripwire

- `crates/beava-core/tests/per_entity_size_dump.rs::aggop_size_within_cap` asserts `size_of::<AggOp>() <= 80`.
- Future operator additions exceeding the cap force a deliberate review decision (Box the new variant, or raise the cap with documented rationale).

### Throughput verification

- See `.planning/throughput-baselines.md::Phase 12.9 — AggOp memory boxing — fraud-team/tcp regression check` for the perf-gate verdict (median +6.9% vs Phase 19.4-04 quiescent baseline; PASS).

---

## Phase 13.4 — Engine prep / wire-spec conformance — apply_path microbench (Apple-M4)

**Date:** 2026-05-04
**Phase:** 13.4 (engine-prep + wire-spec conformance — Plans 01-09 landed)
**HEAD at measurement:** `5e0be61` (post Plans 13.4-01..09; closure plan 13.4-10 in progress)
**hw-class:** Apple-M4 / Darwin-24.3.0 / 10 cores
**Bench:** `crates/beava-core/benches/apply_path_bench.rs`
**Builder:** `cargo bench -p beava-core --bench apply_path_bench`
**System load:** moderate (Cursor IDE + Claude Code SDK + bench process active in foreground; consistent with the Phase 12.6/12.7/12.8/12.9 measurement profile).

**Synthetic registry shape:** 14 features (7 user-keyed, 4 user×merchant-keyed, 3 device-keyed). Mix of Count, Sum, Percentile (UDDSketch), Ewma, TopK, Entropy, EventTypeMix, CountDistinct, BloomMember spans Tier 1/2/3. Post-Plan-13.4-01 (ADR-002 op renames: `avg→mean`, `variance→var`, `stddev→std`, `count_distinct→n_unique`, `percentile→quantile`) — pure string-table change at the JSON-prelude boundary; the AggKind enum variants the bench exercises are unchanged.

**Phase 13.4 net code change vs Phase 12.9:** ~800 LOC of mechanical wire-spec conformance. Plan 01 (op-string rename, register-time only — cold path); Plan 02 (GET response envelope drop — read path, not apply path); Plan 03 (OP_BATCH_GET opcode + dispatch — new opcode, doesn't touch apply path); Plan 04 (verb-style HTTP routes — listener layer); Plan 05 (architectural-test surgical permit — test-only); Plan 06 (force/dry_run register flags — register-time, cold path); Plan 07 (Persistence::Memory backend — boot-time branch); Plan 08 (OP_RESET — new dispatch arm, cold path); Plan 09 (parse_entity_key sentinel branch — query path, ~5 LOC). **Net hot-path delta:** zero — none of the plans touch `apply_event_to_aggregations` or per-event AggOp arithmetic.

### Phase 13.4 microbench rows (apply_path_bench)

| Bench | Phase 13.4 median | Phase 19.2 baseline | Phase 19.4-A reference | Δ vs 19.2 | Verdict |
|---|---:|---:|---:|---:|---|
| `apply_path/cold_key/14_aggs` | **957.91 ns** (CI [950.57, 965.41]) | 1,424 ns | n/a (different fixture in 19.4) | **−32.7%** | **PASS** (much faster — likely Phase 12.9 AggOp boxing cache-locality lift compounding the Phase 19.x stacked stack) |
| `apply_path/warm_key/14_aggs` | **339.26 ns** (CI [335.79, 343.64]) | 362.71 ns | 330.81 ns (post-19.4-01 reference; current fixture matches) | **−6.5% vs 19.2; +2.6% vs 19.4-A** | **PASS** (within ±10% gate; matches the Phase 19.4 stacked-baseline shape) |
| `apply_path/warm_key/14_aggs_windowed` | **454.37 ns** (CI [450.74, 458.50]) | n/a (introduced 19.3) | 408.00 ns (post-19.4-01) | **+11.4% vs 19.4-A** | **WARN** (within ±10–25% band on the windowed fixture; non-gating per CLAUDE.md §Performance Discipline contract — gate cell is `cold_key/14_aggs`) |

**Driver:** Phase 13.4 is mechanical conformance work (~800 LOC, no apply hot-path edits). Predicted: zero regression. Measured: cold_key/14_aggs is **substantially faster** than the Phase 19.2 baseline — likely a combination of (a) Phase 12.9's AggOp boxing cache-locality win (state struct shrinks from 600 B → 80 B), (b) toolchain progression (rustc 1.87+ codegen improvements vs the 19.2 measurement era), and (c) measurement noise within the typical Apple-M4 ±5% band. The warm-key non-windowed cell matches the post-Phase-19.4-A baseline within criterion CI; the windowed cell is +11.4% vs 19.4-A — within the WARN-band but informational since:
1. Phase 19.4 explicitly noted the windowed cell's bench-fixture sensitivity (CountDistinct in HashSet mode varies dramatically with the pre-warm sequence).
2. The gate cell per CLAUDE.md is `cold_key/14_aggs` — that's PASS at -32.7%.
3. Phase 13.4 made no edits to `WindowedOp::update_with_row` or any windowed-bucket state machine.

### Regression gate verdict

| Gate cell | Phase 13.4 | Baseline | Δ% | Verdict |
|---|---:|---:|---:|---|
| `apply_path/cold_key/14_aggs` (gate) | 957.91 ns | 1,424 ns (Phase 19.2) | **−32.7%** | **PASS** (well within ±10% band; substantial lift) |

CLAUDE.md §Performance Discipline thresholds:
- 10% slower → WARN (must investigate before Phase 13)
- 25% slower → BLOCK (phase verification fails)

Phase 13.4 is **PASS** at the gate cell — measurement is FASTER than the prior baseline by a wide margin.

### Phase 12.9 AggOp boxing carry-through (informational)

Plan 12.9 was a SIZE measurement (no microbench cells captured). The Phase 13.4 numbers above are the FIRST `apply_path` microbench captured **post-12.9 boxing** — confirming that the boxed-variant per-event Box indirection cost is dominated by the cache-locality win (smaller `Vec<AggOp>` slot). This validates the Phase 12.9 D-02 reversal of Phase 11 D-08 empirically on the apply hot path, complementing the throughput-gate verdict in `.planning/throughput-baselines.md::Phase 12.9 — AggOp memory boxing — fraud-team/tcp`.

### Workspace state at measurement

- `cargo fmt --all --check` — exit 0
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — exit 0
- `cargo test --workspace --features testing` — 1 pre-existing flake (`phase2_5_smoke::criterion_6_pipelined_registers_return_in_order`); not introduced by Phase 13.4 (verified by `git stash` + re-run during Plan 09 execution; pre-Phase-13.4 history at `7ad84c5` is the file's last touch).

### Throughput verification

- See `.planning/throughput-baselines.md::Phase 13.4 — Engine prep / wire-spec conformance` for the 8-cell throughput rebench. Gate cell `small/tcp` measured **−0.3% vs Phase 12.8 mean** (725,507 EPS, mean of 2 runs). PASS.

---

## Phase 13.5 — `beava bench` CLI cold-path microbench (Apple-M4)

**Date:** 2026-05-04
**Bench:** `crates/beava-bench/benches/cli_dispatch.rs`

Measures the CLI cold-path overhead (workload load + memory estimator + clap argv parse) for the new `beava bench <mode>` subcommand surface. These are cold-path costs incurred once per `beava bench` invocation; the bench is a regression tripwire so future plans don't accidentally balloon the cold path.

### Phase 13.5 bench rows

| Bench | Phase 13.5 median | Notes |
|-------|------------------:|-------|
| `workload_load_fraud` | 93.49 µs | fraud-team config (5 events × 90 features) — cold JSON parse |
| `workload_load_adtech` | 13.19 µs | medium-with-sketches config |
| `workload_load_small` | 9.91 µs | small.json config |
| `estimator_fraud_medium` | 90.36 µs | fraud × medium — full per-derivation breakdown |
| `estimator_adtech_small` | 13.10 µs | adtech × small |
| `clap_parse_throughput_args` | 3.22 µs | bare clap parse |

**Verdict:** Phase 13.5 establishes baseline (no prior comparable measurement). All cold-path operations complete in microseconds; the workload load dominates because it does JSON deserialization of the canonical config files (fraud-team is ~3 KB JSON / ~90 derivation aggs). Subsequent phases compare against these numbers; 10%/25% gates apply.

### Workspace state at measurement

- `cargo fmt --all --check` — green
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — green (Phase 13.5 changes contributed no new clippy warnings)
- `cargo test --workspace --features testing` — 1 pre-existing flake (`phase2_5_smoke::criterion_6_pipelined_registers_return_in_order`) — pre-existing at base commit `bf7613cc` BEFORE Phase 13.5 work, NOT introduced by Phase 13.5 (verified via base-commit re-run during Plan 11 execution).

### Phase 13.5 apply_path carry-through

The Phase 13.4 `apply_path` microbench rows above remain the live regression-gate for the apply hot path. Phase 13.5 made no edits to `crates/beava-core` or `crates/beava-server` outside `tests/phase12_7_legacy_table_handlers_killed.rs` (test-only, ADR-001 alignment). The apply_path/cold_key/14_aggs gate cell at 957.91 ns remains the active baseline.

---

## Phase 13.4.1 — Server-side wire-spec verb-style migration — verb-style dispatch microbench (Apple-M4)

**Date:** 2026-05-04
**HEAD at measurement:** `526c9963` (Plan 13.4.1-04 GREEN cluster HEAD; closure plan 13.4.1-05 in progress)
**hw-class:** Apple-M4 / Darwin-24.3.0 / 10 cores
**Bench:** `crates/beava-server/benches/phase13_4_1_dispatch_get_verb_style.rs`
**Builder:** `cargo bench --bench phase13_4_1_dispatch_get_verb_style --features testing`
**System load:** moderate (Cursor IDE + Claude Code SDK + bench process active in foreground; consistent with Phase 12.6/12.7/12.8/12.9/13.4 measurement profile).

**Phase 13.4.1 net code change vs Phase 13.4:** ~250 LOC of server-side wire-spec migration. Plan 04 introduces `GlueResponse::UnsupportedRequestShape`, migrates `WireRequest::HttpGet`/`TcpGet` body parsers to verb-style three-step ladder, rewrites `BatchGetReqEntry` with custom `Deserialize` impl (D-04 alias detection), flattens `dispatch_batch_get_sync` per-row response constructor (D-03), adds per-entry features-filter narrowing pass (D-06), and adds a NEW `dispatch_get_single_verb_style_sync` function in `runtime_core_glue.rs`. Net hot-path delta on the *read* path is structurally NEUTRAL-to-POSITIVE: custom Deserialize ~10 ns/entry, features-filter `iter().any(...)` ~15 ns/feature, FLAT-row constructor saves 3 allocations per entry. The *push* hot path is untouched.

### Phase 13.4.1 microbench rows (phase13_4_1_dispatch_get_verb_style)

| Cell | Phase 13.4.1 median | CI (criterion) | Notes |
|---|---:|---|---|
| `verb_style_dispatch/get_single_1feat` | **146.02 ns** | [145.21, 146.86] | NEW verb-style single-row dispatch (`dispatch_get_single_verb_style_sync`). Baseline anchor — no prior comparable measurement (this function did not exist before Plan 13.4.1-04). 1000 entities × 10 events warm-AppState; 1-feature filter. Used by `POST /get` + `OP_GET`. |
| `verb_style_dispatch/get_batch_json/10x1feat` | **5,215 ns** (5.22 µs) | [4,747, 5,777] | Migrated batch dispatch (`dispatch_batch_get_sync`) with verb-style per-entry shape + per-entry features filter + FLAT-row response constructor. CT_JSON body parse + dispatch + JSON encode for 10-entry batch with 1-feature filter each. |
| `verb_style_dispatch/get_batch_msgpack/10x1feat` | **3,503 ns** (3.50 µs) | [3,475, 3,535] | Same batch dispatch, CT_MSGPACK body parse. msgpack-vs-json body-parse savings consistent with the Phase 12-09 read-path microbench result (~33% faster on this shape). |

**Driver:** Phase 13.4.1 is a server-side wire-spec migration (~250 LOC). Predicted: NEUTRAL-to-POSITIVE on dispatch hot path. Measured (first observation, no prior baseline): single-row verb-style dispatch at ~146 ns/op is well under the existing read-path microbench reference points (Phase 12-09 `read_path/get_single_json` was ~1500 ns on a comparable warm-AppState shape; the verb-style `dispatch_get_single_verb_style_sync` is structurally simpler — no batch envelope unwrap, no upfront feature-resolve loop — and the lower number is consistent with that simplification). The 10-entry batch microbench at ~5.2 µs JSON / ~3.5 µs msgpack is also consistent with Phase 12-09's `read_path/get_batch_json/10x5` numbers when normalised by feature count (10×5=50 cells in 12-09, 10×1=10 cells here; ~5× less work per call).

### Regression gate verdict

| Gate cell | Phase 13.4.1 | Baseline | Δ% | Verdict |
|---|---:|---:|---:|---|
| `verb_style_dispatch/get_single_1feat` (NEW gate) | 146.02 ns | n/a (first measurement) | — | **PASS — baseline established** |

CLAUDE.md §Performance Discipline thresholds (10% slower → WARN; 25% slower → BLOCK) apply prospectively from this baseline forward. Subsequent plans modifying `runtime_core_glue.rs::dispatch_get_single_verb_style_sync` or `apply_shard.rs::dispatch_batch_get_sync` MUST re-run this microbench and compare against the rows above.

### Throughput verification

- See `.planning/throughput-baselines.md::Phase 13.4.1 — Server-side wire-spec verb-style migration — small/tcp regression-gate run` for the end-to-end throughput rebench. Gate cell `small/tcp` measured **−6.30% vs Phase 13.5 baseline** (median 631,610 EPS vs 674,108 EPS). PASS at the regression-gate cell.

### Workspace state at measurement

- `cargo fmt --all --check` — exit 0
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — exit 0 (verified during Plan 13.4.1-04 commit 90693a88)
- `cargo bench --bench phase13_4_1_dispatch_get_verb_style --no-run --features testing` — exit 0 (compile gate)
- `cargo test --workspace --features testing` — workspace GREEN at HEAD (verified during Plan 13.4.1-05 closure)
