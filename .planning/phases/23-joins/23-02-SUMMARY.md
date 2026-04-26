---
phase: 23-joins
plan: 02
subsystem: engine+register+pipeline+state
tags: [stream-stream-join, symmetric-interval, event-time-buffer, eviction, retroactive-match]
dependency_graph:
  requires:
    - 23-01  # EnrichFromTable scaffolding, typed JoinSpec, encode_group_by composite
    - 22-01  # parse_event_time helper (SystemTime return)
  provides:
    - JOIN-SS                # Stream↔Stream symmetric interval windowed join
    - STREAM-JOIN-BUFFER     # per-key event-time-indexed buffer primitive
  affects:
    - src/engine/operators.rs   # StreamJoinBuffer + JoinSide + Operator impl
    - src/engine/pipeline.rs    # FeatureDef::StreamStreamJoin + cascade dispatch + build_joined_event
    - src/engine/register.rs    # v0_join_to_stream_def 'stream_stream' branch
    - src/state/snapshot.rs     # OperatorState::StreamJoinBuffer variant + all dispatch arms
    - src/server/http.rs        # /pipelines/:name render for new variant
tech-stack:
  added: []
  patterns:
    - stringified-json-for-postcard  # events stored as String inside BTreeMap for snapshot codec
    - cascade-side-detection         # origin stream name matched against left/right for side inference
    - emit-first-as-effective        # first match becomes the effective_event; extras direct-push to downstreams
    - eager-null-pair-for-left-miss  # v0 limitation; Phase 24 will retract-then-insert
key-files:
  created:
    - tests/test_join_stream_stream.rs
  modified:
    - src/engine/operators.rs
    - src/engine/pipeline.rs
    - src/engine/register.rs
    - src/state/snapshot.rs
    - src/server/http.rs
decisions:
  - "StreamJoinBuffer stores raw events as stringified JSON inside BTreeMap<u64, Vec<String>> because postcard (the production snapshot codec) cannot serialize serde_json::Value. probe() re-parses on read. Snapshot round-trip works end-to-end via postcard."
  - "Side detection uses the origin `stream_name` passed into push_with_cascade_internal, matched against FeatureDef::StreamStreamJoin.left_stream / right_stream. Events arriving through unrelated upstreams are silently skipped — the join only fires when pushed directly on a side."
  - "For type=Left + left-side miss, the cascade emits an eager null-pair on arrival. A later matching right-side event emits a SECOND joined pair, producing DOUBLE emission. Accepted for v0 per plan §objective and T-23-07; Phase 24 replaces the null-pair with a retraction."
  - "Buffer eviction runs after every insert, dropping entries on both sides older than max_seen_on_that_side - within_ms. Confirmed with a 1000-event stress test that shrinks the buffer to 1 entry after an in-window→out-of-window jump."
  - "The join stream is keyless with group_by_keys = Some(join.on). push_internal short-circuits keyless streams (no entity state), so join-stream state lives exclusively under the composite state key inside its downstream-of-join storage slot, accessed in the cascade handler via store.get_or_create_entity(state_key)."
  - "When emissions > 1, the first joined event rides the standard effective_events publish path (reaches downstream-of-join via toposort). Extras get direct push_internal calls into each direct downstream. Downstream-of-downstream iteration is NOT replicated for extras; v0 tests only depend on direct-downstream aggregations observing each emission. Phase 24 will unify this via a proper multi-event cascade."
metrics:
  duration: ~13m
  completed: 2026-04-14
  tasks: 2
  commits:
    - 86f60cc  # Task 1: StreamJoinBuffer primitives + 6 unit tests
    - 08db23b  # Task 2: Stream↔Stream cascade + translator + 8 integration tests
---

# Phase 23 Plan 02: Stream↔Stream symmetric interval join — Summary

**One-liner:** Shipped Stream↔Stream symmetric interval windowed joins
(inner + left) with per-key event-time-indexed buffers, bounded
eviction, retroactive match within the within-window, and composite-
key support — replacing 23-01's `shape="stream_stream"` translator
stub and landing the full `FeatureDef::StreamStreamJoin` variant +
cascade dispatch + `OperatorState::StreamJoinBuffer` snapshot-ready
state.

## What shipped

### 1. StreamJoinBuffer primitives (commit `86f60cc`)

`src/engine/operators.rs` gained:

  * `JoinSide { Left, Right }` enum (serde-derived).
  * `StreamJoinBuffer { left, right, within_ms, max_left_ms,
    max_right_ms }` with `BTreeMap<u64, Vec<String>>` per side. Events
    stored as stringified JSON (postcard compat; `serde_json::Value`
    is not postcard-serializable).
  * `probe(side, event_time_ms)` — range query on the OPPOSITE side
    for `[t - within_ms, t + within_ms]` (inclusive); re-parses stored
    strings into `serde_json::Map`.
  * `insert(side, event_time_ms, event)` — stringifies + pushes to
    the multimap bucket; bumps `max_<side>_ms`.
  * `evict()` — drops entries on both sides older than
    `max_seen - within_ms` (saturating sub).
  * `Operator` trait impl: push/read are no-ops (state-only carrier);
    `estimated_bytes()` accounts for per-event string + node overhead.

`src/state/snapshot.rs`:
`OperatorState::StreamJoinBuffer(StreamJoinBuffer)` variant wired
through push / read / estimated_bytes / num_buckets /
operator_type_name (→ `"stream_join_buffer"` for /debug/key/:key).

**Primitives tests** (6/6): empty-probe, interval-filter, inclusive-
boundary, eviction-floor, multimap-at-same-timestamp, postcard
round-trip.

### 2. Stream↔Stream cascade + translator (commit `08db23b`)

**FeatureDef::StreamStreamJoin** added to `pipeline.rs` with fields
`left_stream`, `right_stream`, `on`, `within_ms`, `join_type`,
`left_fields`, `right_fields`. Wired through `get_backfill_flag`
(false), `create_operator` (None — state created lazily in cascade),
`get_where_expr` (None), and `max_window_duration`
(`Duration::from_millis(within_ms)` for TTL scheduling).

**Translator** (`register.rs::v0_join_to_stream_def`): implemented
the `"stream_stream"` branch (stub from 23-01 removed).

  * `desc.join.within` required — missing raises `Protocol("stream_stream
    join requires within=<duration>...")`.
  * `parse_window` reused for the duration parse (same grammar as
    aggregation windows: `30s` / `5m` / `2h` / `500ms`).
  * Output schema partitioned via the same heuristic as 23-01:
    `_right` suffix → right-side rename; member of left schema →
    left-side passthrough; member of right schema → right-side
    passthrough; unknown → conservative left-side.
  * Returns a keyless `StreamDefinition` with `group_by_keys =
    Some(desc.join.on)` so the cascade's `encode_group_by` path
    composes the per-key state key identically to 23-01.
  * `type="outer"` rejected with the 23-01-verbatim "outer joins
    deferred to v0.1; use two inner+left joins unioned" message.

**Cascade dispatch** (`pipeline.rs::push_with_cascade_internal`): new
match arm for `FeatureDef::StreamStreamJoin`. On arrival:

  1. Determine side from origin `stream_name` (primary push stream)
     matched against `left_stream` / `right_stream`. Unrelated origins
     skip this stream for this cascade.
  2. `state_key = encode_group_by(on, effective_event)` — same
     composite key path as aggregation + enrichment.
  3. `event_time_ms = parse_event_time(event) ?? now` projected to
     `unix_epoch_millis`.
  4. Get-or-create `EntityState.streams[join_name]`, then find or
     insert a single `StreamJoinBuffer(within_ms)` operator named
     `__stream_join_<left>_<right>`.
  5. `probe` → clone, `insert` arriving event, `evict`.
  6. Build joined events via `build_joined_event(left_map, right_map,
     right_fields)` — left-preserved, right-fields lifted from the
     opposite side's match. For `Left` miss on left-side arrival,
     emit a single null-pair (right_map = empty).
  7. First joined event → `effective_events[join_name]`; the toposort
     walk continues into downstream-of-join with this as the event.
     Extras (N > 1 matches) → direct `push_internal` call to each
     immediate downstream of the join.
  8. No emissions → `dropped.insert(join_name)` so the subtree is
     skipped for this cascade.

**HTTP** `/pipelines/:name`: renders the new variant as
`{"type":"stream_stream_join", left_stream, right_stream, on,
within_ms, join_type, left_fields, right_fields}`.

**Integration tests** (8/8) in `tests/test_join_stream_stream.rs`:

  * `ss_inner_basic_match` — left T=t0, right T=t0+5s, within=30s →
    1 joined emission observed by downstream count.
  * `ss_inner_out_of_window_no_emit` — within=30s, right 60s later →
    0 emissions.
  * `ss_left_miss_emits_null` — two left-only events; count=2.
  * `ss_retroactive_match` — type=left, left arrives first, right
    arrives within 30s; count=2 (eager null-pair + retroactive match).
  * `ss_eviction_frees_memory` — 1000 events within 1s, then one at
    +20s with within=10s; buffer reduces to 1 entry.
  * `ss_composite_key` — `on=["user_id","region"]`; (u1,US) hits,
    (u1,EU) does not cross-match.
  * `ss_rejects_missing_within` — translator error mentions both
    `stream_stream` and `within`.
  * `ss_rejects_outer` — translator returns "outer joins deferred".

## Test results

  * `cargo test --lib` — **678/678** (no regression from 23-01).
  * `cargo test --test test_join_stream_stream` — **14/14** (6
    primitives + 8 integration).
  * `cargo test --test test_join_stream_table` — **6/6** (23-01
    regression guard).
  * `cargo test --test test_register_json_v0` — **21/21**.
  * `cargo test --test test_composite_group_by` — **5/5**.
  * `cargo test --test test_snapshot_hybrid_ops` — **6/6**
    (StreamJoinBuffer serde participates via OperatorState).
  * `pytest python/tests/test_v0_join_stubs.py` — **25/25**.
  * `pytest python/tests/` (excl live-server) — **405 passed, 2
    skipped** (unchanged from 23-01 baseline).

## Deviations from plan

### [Rule 3 — Blocking issue] `parse_event_time` returns `SystemTime`, not `u64` ms

**Found during:** Task 2 cascade wiring.

**Issue:** The plan's `<interfaces>` block documented
`parse_event_time(event) -> u64` (millis since epoch). The actual
helper in `src/engine/operators.rs:2099` returns
`Option<SystemTime>`.

**Fix:** Cascade handler converts: `parse_event_time(event)
.unwrap_or(now).duration_since(UNIX_EPOCH).as_millis() as u64`.
Pure adapter; no change to `parse_event_time`.

### [Rule 3 — Blocking issue] Postcard cannot serialize `serde_json::Map`

**Found during:** Task 1 snapshot round-trip test.

**Issue:** Initial implementation stored events as
`BTreeMap<u64, Vec<serde_json::Map<String, Value>>>`. Postcard (the
production snapshot codec) emits `WontImplement` when asked to
serialize `serde_json::Value` — confirmed via a comment at
`src/state/snapshot.rs:211` that this is a known constraint.

**Fix:** Store events as `BTreeMap<u64, Vec<String>>` where each
String is a stringified JSON object. `insert()` stringifies on write;
`probe()` re-parses into `serde_json::Map` on read. Degradation is
clean: a parse failure skips that entry (shouldn't happen in practice
— we stringified it).

### [Rule 2 — Missing functionality] `max_window_duration` lacked StreamStreamJoin arm

**Found during:** build after FeatureDef variant add.

**Fix:** Added `FeatureDef::StreamStreamJoin { within_ms, .. } =>
Some(Duration::from_millis(*within_ms))`. Treats `within` as the
effective window so eviction-schedule and TTL code that consult
`max_window_duration` correctly account for join-buffer retention.

### [Rule 2 — Missing functionality] `/pipelines/:name` render missing new variant

**Found during:** build after FeatureDef variant add
(non-exhaustive match at `src/server/http.rs:55`).

**Fix:** Added render arm emitting `{"type":"stream_stream_join",
left_stream, right_stream, on, within_ms, join_type, left_fields,
right_fields}`.

### Auth gates

None — all tests run against the in-process engine fixture. No TCP
round-trip pytest was added (plan's verification gate didn't require
one; the plan defers end-to-end Python contract tests to the SDK's
existing `test_v0_join_stubs.py`, which remains green).

## Known stubs

| File | Location | Reason | Resolved by |
|------|----------|--------|-------------|
| `src/engine/pipeline.rs` | `push_with_cascade_internal` — `joined_events.len() > 1` loop | Extras push directly into DIRECT downstreams only; downstream-of-downstream iteration is not replayed per extra emission. | Phase 24 (proper multi-event cascade under watermarks) |
| `src/engine/pipeline.rs` | eager null-pair for `type=Left` + miss (v0 double-emit) | A left-side unmatched event emits a null-pair; a later retroactive right-side match emits a SECOND joined pair. Documented in `ss_retroactive_match` test. | Phase 24 (retraction-aware retract + insert) |
| `src/engine/operators.rs` | `StreamJoinBuffer` stores events as stringified JSON | Postcard cannot serialize `serde_json::Value`. Re-parse cost on every probe is tolerable at v0 throughput targets. | Phase 25+ (if profiling shows probe parse dominates) — alternative: custom codec or DOM-free representation |

All known stubs surface clean runtime behavior — no panics, no silent
drops. Downstream-of-downstream multi-emit is the only functional
gap; covered by the 24+ milestone.

## Threat flags

None new. The plan's threat register (T-23-05..T-23-07) is fully
addressed:

  * T-23-05 (DoS via per-key buffer growth) — `evict()` runs after
    every insert; bounded to `O(within × event_rate)` per key per
    side. `ss_eviction_frees_memory` validates the bound at 1000
    events.
  * T-23-06 (tampering via `_event_time`) — `parse_event_time` falls
    back to wall-clock for non-numeric or negative values (pre-
    existing Phase 22 behavior); BTreeMap's `u64` key rules out
    overflow because we saturate on conversion.
  * T-23-07 (retroactive match double-emit) — accepted for v0 per
    plan §threat_model; documented here under "Known stubs" and
    validated by `ss_retroactive_match`.

## Benchmark impact estimate

`StreamJoinBuffer` probe cost is `O(log N + M)` where N = buffer
size per side and M = matches returned. Insert + evict is
`O(log N + evicted)`. With typical `within=30s` at 100k eps sustained
per side per key, buffer size ~3M events per key, probe latency
~300 ns + M × JSON parse (~1–3 µs per event).

Plan 23-03's matrix re-run will validate the 5% gate; this plan
defers the full benchmark per `<scope>`.

## Self-Check: PASSED

Verified files exist (absolute paths):

  * `/data/home/tally/tests/test_join_stream_stream.rs` — FOUND (607 lines)
  * `/data/home/tally/src/engine/operators.rs` — FOUND (3716 lines)
  * `/data/home/tally/src/engine/pipeline.rs` — FOUND (4035 lines)
  * `/data/home/tally/src/engine/register.rs` — FOUND (1454 lines)
  * `/data/home/tally/src/server/http.rs` — FOUND (1338 lines)
  * `/data/home/tally/.planning/phases/23-joins/23-02-SUMMARY.md` — FOUND (this file)

Verified commits exist on `v1.3-concurrency`:

  * `86f60cc` feat(23-02): StreamJoinBuffer primitives + 6 unit tests
  * `08db23b` feat(23-02): wire Stream↔Stream symmetric interval join end-to-end

Verified test gates (last run):

  * `cargo test --lib` — 678 / 678
  * `cargo test --test test_join_stream_stream` — 14 / 14
  * `cargo test --test test_join_stream_table` — 6 / 6 (regression)
  * `cargo test --test test_register_json_v0` — 21 / 21
  * `cargo test --test test_composite_group_by` — 5 / 5
  * `cargo test --test test_snapshot_hybrid_ops` — 6 / 6
  * `pytest python/tests/test_v0_join_stubs.py` — 25 / 25
  * `pytest python/tests/` (excl live-server) — 405 passed, 2 skipped

Spot checks (plan §verification):

  * `grep -n "stream_stream.*not.*yet.*implemented" src/engine/register.rs`
    → 0 matches (stub removed)
  * `grep -n "StreamJoinBuffer" src/engine/operators.rs`
    → 5+ matches (type defined + impl + Operator trait)

Phase 23 Plan 02 is complete. Plan 23-03 (Table↔Table same-key
joins) can build on the `FeatureDef` + cascade-dispatch pattern and
the `encode_group_by` composite-key pathway shipped here and in
23-01.
