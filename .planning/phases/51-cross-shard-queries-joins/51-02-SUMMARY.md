---
phase: 51-cross-shard-queries-joins
plan: "02"
subsystem: shard
tags: [tpc, scatter-gather, watermark, global-watermark, tpc-perf-05, futures]

requires:
  - phase: 51-01
    provides: GlobalWatermarkStore flat AtomicU64 array with publish/global_min API
  - phase: 50-shard-routing-push
    provides: ConcurrentAppState, shard_event_loop, ShardHandle

provides:
  - GlobalWatermarkStore wired to ConcurrentAppState as parking_lot::RwLock<GlobalWatermarkStore>
  - publish_if_due called in shard_event_loop on every _event_time observation
  - GET /streams returns watermark_ns (global min) + scatter_gather dedup for N shards
  - GET /streams/{name} returns watermark_ns from global_min (fleet-wide atomic read)
  - futures = "0.3" declared in Cargo.toml for Wave 2 join_all readiness

affects: [51-03, 51-04, 51-05, beava_watermark_lag_seconds metric, GET /streams clients]

tech-stack:
  added:
    - "futures = \"0.3\" (Cargo.toml) — Wave 2 join_all fanout readiness"
  patterns:
    - "parking_lot::RwLock<GlobalWatermarkStore>: write lock only for register_stream at registration time; read lock for global_min on every HTTP request (uncontended — AtomicU64 interior mutability)"
    - "GlobalWatermarkConfig::from_env() read once at shard boot — no per-event env parsing"
    - "scatter_gather(n_shards, per_shard_fn, merge_stream_lists) deduplicates N identical shard views at Wave 1"

key-files:
  created: []
  modified:
    - src/server/tcp.rs
    - src/shard/thread.rs
    - src/server/http_ingest.rs
    - Cargo.toml

key-decisions:
  - "parking_lot::RwLock<GlobalWatermarkStore> in ConcurrentAppState: write lock for register_stream (registration time only), read lock for publish/global_min (hot path — AtomicU64 ops don't need exclusive access)"
  - "publish_if_due called in shard_event_loop after every observe() — threshold from BEAVA_WATERMARK_PUBLISH_INTERVAL read once at shard boot to avoid per-event env lookups"
  - "http_list_streams acquires global_watermark read lock once per request (not per stream) to batch all global_min reads under a single lock acquisition"
  - "futures = 0.3 added now — Wave 1 scatter_gather is synchronous; Wave 2 replaces body with futures::future::join_all without changing the public API"
  - "watermark_ns (nanosecond, no lateness) added alongside existing watermark_ms (millisecond, lateness-adjusted) — different semantics, both useful"

patterns-established:
  - "RwLock-around-GlobalWatermarkStore: read lock for hot-path atomic reads, write lock only at registration — models after parking_lot RwLock used elsewhere in tcp.rs"
  - "Env-config-read-once-at-boot: GlobalWatermarkConfig::from_env() in shard_event_loop before event loop entry — avoids env syscall per event"

requirements-completed: [TPC-PERF-05]

duration: 25min
completed: 2026-04-19
---

# Phase 51 Plan 02: Scatter-Gather GET /streams + publish_if_due Wiring Summary

**GlobalWatermarkStore wired to ConcurrentAppState (RwLock), publish_if_due called in shard_event_loop after every _event_time observation, GET /streams and GET /streams/{name} both return watermark_ns from fleet-wide global_min, scatter_gather deduplicates N-shard stream lists.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-04-19
- **Tasks:** 2 (TDD RED commit + TDD GREEN commit)
- **Files modified:** 4 (tcp.rs, thread.rs, http_ingest.rs, Cargo.toml)

## Accomplishments

- `global_watermark: parking_lot::RwLock<GlobalWatermarkStore>` added to `ConcurrentAppState`, initialized with `n_shards x 64` slot array at server boot
- `publish_if_due` wired in `shard_event_loop`: threshold from `BEAVA_WATERMARK_PUBLISH_INTERVAL` read once at shard boot, called on every `_event_time` observation after `observe()`
- `GET /streams` uses `scatter_gather` for dedup + emits `watermark_ns` (global min) alongside existing `watermark_ms` (shard-local, lateness-adjusted)
- `GET /streams/{name}` appends `watermark_ns: global_min(&name)` to response
- `beava_cross_shard_fanout_total{op="list_streams"}` incremented on every `GET /streams` call (already wired in pre-existing handler, now confirmed active)
- `futures = "0.3"` declared in Cargo.toml for Wave 2 readiness
- 36 tests green: 5 scatter, 4 new http_ingest, 13 global_watermark, 14 watermark

## Task Commits

1. **test(51-02): TDD RED** — `745acb6`
   - 4 failing tests: field access, 3-shard min, watermark_ns in response, publish_if_due integration
   - Compilation failure confirmed: `E0609: no field global_watermark on Arc<ConcurrentAppState>`

2. **feat(51-02): scatter-gather GET /streams + GET /streams/{name}** — `fa3b7d9`
   - `global_watermark: RwLock<GlobalWatermarkStore>` in ConcurrentAppState + constructor
   - `publish_if_due` wired in shard_event_loop
   - `http_list_streams` and `http_get_stream` updated
   - `futures = "0.3"` in Cargo.toml
   - All 4 RED tests turn GREEN; 36 total tests pass

## Files Created/Modified

- `/Users/petrpan26/work/tally/src/server/tcp.rs` — `global_watermark` field added to `ConcurrentAppState`; initialized in `make_concurrent_state_full`
- `/Users/petrpan26/work/tally/src/shard/thread.rs` — `wm_publish_threshold` from env read once at shard boot; `publish_if_due` called after every `observe()` in event loop
- `/Users/petrpan26/work/tally/src/server/http_ingest.rs` — `http_list_streams` uses `scatter_gather` + global_min for `watermark_ns`; `http_get_stream` appends `watermark_ns`; 4 TDD tests added
- `/Users/petrpan26/work/tally/Cargo.toml` — `futures = "0.3"` added to `[dependencies]`

## Decisions Made

- `parking_lot::RwLock<GlobalWatermarkStore>` — write lock at registration (rare), read lock on HTTP path (uncontended since AtomicU64 doesn't need exclusion). Considered `Arc<GlobalWatermarkStore>` but `register_stream` requires `&mut self`.
- `watermark_ns` alongside `watermark_ms` — different semantics: `watermark_ns` is raw observed_max (no lateness, nanosecond precision from global atomic), `watermark_ms` is lateness-adjusted shard-local value. Both surfaced; callers can use either.
- `futures = "0.3"` added now — scatter_gather is synchronous at Wave 1 (N=1); Wave 2 replaces the body with `futures::future::join_all` over shard inboxes without changing the `scatter_gather` public API signature.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] StreamDefinition field correction in test**
- **Found during:** TDD GREEN (test compilation)
- **Issue:** Test used incorrect `RegisterRequest` fields (`shard_key`, wrong `ephemeral` type) — `RegisterRequest` has different fields from `StreamDefinition`; engine uses `register()` not `register_stream()`
- **Fix:** Changed test to construct `StreamDefinition` directly with all required fields including `pipeline_ttl` and `watermark_lateness`
- **Files modified:** `src/server/http_ingest.rs`
- **Verification:** `cargo build` clean, all 4 tests green
- **Committed in:** `fa3b7d9` (part of GREEN implementation commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 - Bug in test construction)
**Impact on plan:** Purely test-level fix; no implementation change. No scope creep.

## Issues Encountered

Pre-existing test failures (confirmed pre-existing per 51-01 SUMMARY):
- `hll_mode_within_2_percent_on_100k` — probabilistic HLL accuracy, flaky under load
- `test_concurrent.rs::{test_enriched_concurrent_clients, fan_out_under_concurrency, ...}` — OS error 49 "Can't assign requested address" (macOS port binding under parallel test runs)

None introduced by this plan.

## Known Stubs

- `scatter_gather` in `src/routing/scatter.rs` is synchronous (Wave 1, N=1). Wave 2 replaces the body with `futures::future::join_all` over SPSC inboxes. API is stable — no caller changes needed at Wave 2.
- `beava_watermark_lag_seconds` gauge uses placeholder `0.0` — Phase 51-03 wires live watermark lag.
- `http_list_streams` acquires a `sharded_store` mutex lock to get `n_shards` — this could be cached at state construction time in Phase 52.

## Threat Flags

None — no new network endpoints. `GET /streams` and `GET /streams/{name}` were already public endpoints; the `watermark_ns` field adds no new PII surface (T-51-02-02: accepted per plan). T-51-02-01 (scatter timeout) is deferred: Wave 1 is synchronous and cannot hang; Wave 2 will add `tokio::time::timeout` around `join_all`.

## Self-Check: PASSED

- `/Users/petrpan26/work/tally/src/server/tcp.rs` — exists, `global_watermark` field present
- `/Users/petrpan26/work/tally/src/shard/thread.rs` — exists, `publish_if_due` call present
- `/Users/petrpan26/work/tally/src/server/http_ingest.rs` — exists, `watermark_ns` in both handlers, 4 tests
- `/Users/petrpan26/work/tally/Cargo.toml` — `futures = "0.3"` present
- Commits `745acb6` (RED) and `fa3b7d9` (GREEN) verified in `git log`
- 36 tests: `routing::scatter` (5) + `server::http_ingest::tests` (4) + `shard::global_watermark` (13) + `shard::watermark` (14) all green
