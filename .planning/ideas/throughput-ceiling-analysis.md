# Throughput ceiling analysis — post-Phase-13.3

**Captured:** 2026-04-24
**Context:** Post-Phase-13.3 (Option B lockless apply, `Arc<LocalState<RefCell<AppState>>>`) the apply ceiling moves from Mutex-bound ~17k EPS/core to ~60ns/event = theoretical ~16M EPS/core. The real ceiling per transport × mode falls out of the per-event overhead budget.

## Per-event cost model

Assumes Option B apply at ~60ns and typical fraud-shape event payload.

### Framed TCP + MessagePack

| Mode | Breakdown (ns per event) | EPS/core (M4) | EPS/core (Linux Xeon) |
|---|---|---|---|
| Single push | 100 recv + 5 parse + 300 msgpack + 60 apply + 200 encode + 100 send = **~765ns** | ~1.3M | ~2.6M |
| Pipelined (strict-FIFO, context mgr) | Syscall amortized ~10ns each way: **~585ns** | ~1.7M | ~3.4M |
| `push_many(128)` | Single frame amortizes all overhead: **~363ns** | ~2.7M | ~5.4M |
| `push_many(1024)` asymptotic | ~360ns | ~2.8M | ~5.6M |

### HTTP/JSON (axum)

| Mode | Breakdown (ns per event) | EPS/core (M4) | EPS/core (Linux Xeon) |
|---|---|---|---|
| Single push | 100 + 300 parse + 200 body + 200 route + **3000 JSON decode** + 60 apply + **1000 JSON encode** + 200 = **~5060ns** | ~200k | ~400k |
| Pipelined HTTP/1.1 | Same — JSON dominates | ~200k | ~400k |
| `push_batch(128)` (JSON array body) | HTTP overhead amortized + inner-array decode ~500ns: **~670ns** | ~1.5M | ~3M |

## 3M EPS/core ship-gate clearance

Which modes clear the Phase 13 perf gate on Linux:

- ✅ **TCP `push_many`**: 5.4M — clears by ~2× (headroom for complex_fraud / recommendation shapes)
- ✅ **TCP pipelined**: 3.4M — clears JUST
- ✅ **HTTP `push_batch`**: 3.0M — clears JUST
- ❌ TCP single-event: 2.6M — misses by ~13%
- ❌ HTTP single-event: 400k — way under

**Conclusion:** the 3M EPS/core v0 ship-gate is achievable with Phase 13.3 landing + Phase 12's `push_many` / `push_batch` (already in flight). No additional perf work needed for v0.

## What unlocks what — priority order

| Change | Home | Lift |
|---|---|---|
| Phase 13.3 lockless apply | In flight | 17k → ~1.3M TCP single = **~75×** |
| `push_many(N)` | Phase 12 (partial; 12-01 Task 2 shipped, rest pending) | ~1.3M → ~2.7M TCP = **~2×** |
| Pipeline context manager (Python SDK) | Proposed Phase 16 | ~1.3M → ~1.7M TCP single-op workloads = **~1.3×** |
| `push_batch` HTTP | Already in Architecture section of ROADMAP | 200k → 1.5M = **~7×** |
| `push_and_get` | Proposed Phase 12.5 (latency win, not throughput) | neutral for throughput |

## v0.1 ladder (previously discussed, parked pending Phase 14)

| Change | Ceiling lift on top of above |
|---|---|
| SO_REUSEPORT sharding (N-core scale-out) | Linear × N cores: 8× on 8-core box → ~20M/machine |
| io_uring batched syscalls (Linux) | Amortize recv/send: ~1.5-2× at low batch |
| Binary schema format (fixed-layout, skip MessagePack) | Skip ~300ns MessagePack decode: ~2× for TCP |

Stacking: `~2.7M/core × 8 shards × 1.5 io_uring × 2 binary = ~65M EPS on 8-core machine`. Theoretical. v0.1+ work.

## Assumptions + caveats

- MessagePack decode at ~300ns is a rough figure; real cost depends on event size (doubles for 5KB events).
- JSON decode at ~3μs is for ~200-byte fraud event; scales linearly with payload size.
- Apple-M4 cost model; Linux Xeon estimates at ~2× multiplier based on typical syscall + memory bandwidth differences.
- `push_many` cost dominated by inner per-event decode; batch size beyond ~128 has diminishing returns.
- Does NOT account for: TLS (add ~500ns-1μs for TCP handshake on new connection; effectively zero for kept-alive); apply-path variance for complex-fraud-shape features (multiple aggregations per event).

## Decision log references

- Phase 13.3 = Option B (Redis-shape, no mpsc on apply path) — user decision 2026-04-24
- Perf ladder (sharding / io_uring / binary schema) = dropped until Phase 14 lands — user decision 2026-04-24
- `push_and_get` = noted as forward-looking idea (`.planning/ideas/push-and-get-endpoint.md`)
