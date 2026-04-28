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
