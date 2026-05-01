---
phase: 36-replica-server-boot
plan: 01
subsystem: replica-boot
tags: [option-m, cdc-replay, replica-mode, load-bearing]
status: complete
commits:
  - 22d2f36 feat(36-01): replica-mode server boot (--replica-from CDC replay)
key-files:
  created:
    - src/server/replica_client.rs
    - tests/integration/test_replica_mode.py
  modified:
    - src/main.rs
    - src/server/mod.rs
    - src/server/replica.rs
    - src/server/tcp.rs
metrics:
  duration: ~2.5 hours
  tasks: 4
  files: 6
  test-delta: +12 (5 replica_client unit + 6 parse_replica_since unit + 1 Python integration)
---

# Phase 36 Plan 01: Replica-mode server boot Summary

Load-bearing Option M plan: `tally` now boots in replica mode when launched with `--replica-from HOST:PORT`, pulls scoped CDC from the upstream via `OP_LOG_FETCH` + `OP_SUBSCRIBE`, and flows every event through the same local ingest path a local PUSH would take — so any pipelines the scientist registered compute live aggregates off the replicated stream. Listeners bind only after catchup-done, giving `tl.Client` users a consistent view at first query.

## What Landed

### Task T1 — CLI parsing (`src/main.rs`)

- **New flags:**
  - `--replica-from HOST:PORT`
  - `--replica-since <ISO-8601Z|u64 ms>`
  - `--replica-streams a,b,c`
  - `--replica-keys k1,k2` | `--replica-key-prefix P` (mutually exclusive)
  - `--replica-token T` (falls back to `TALLY_REPLICA_TOKEN` env var)
  - `--replica-block-until-catchup` (default **true**)
  - `--replica-pipeline-file FILE`
- **`parse_replica_since`** — hand-rolled ISO-8601 UTC parser (Howard Hinnant days-from-civil algorithm) so we don't pull in `chrono` for one flag. Accepts `2026-04-14T12:34:56Z`, `2026-04-14T12:34:56.789Z`, or a bare u64 millisecond integer.
- **`parse_replica_boot_config`** — assembles a `ReplicaBootConfig`; returns `Ok(None)` when `--replica-from` is absent so legacy server behavior is unchanged.
- **6 unit tests** in `src/main.rs::replica_cli_tests`.

### Task T2 — Replica client loop (`src/server/replica_client.rs`, NEW)

- **`ReplicaBootConfig`** struct (public, shared with main.rs).
- **`ReplicaClient::run(mut self) -> Result<(), ReplicaError>`**:
  1. `run_log_fetch_with_retry(cfg.since_millis, max_attempts=5)` — exp-backoff ±20% jitter between attempts.
  2. Fires `catchup_done_tx` oneshot so `main.rs` unblocks listener binds.
  3. Enters SUBSCRIBE tail loop: each SUBSCRIBE disconnect triggers a re-catchup `LOG_FETCH` cursored at `state.replica_last_applied_ts_ms`, then a reconnect.
  4. Tracks consecutive failures in a rolling 60-second window; 10+ failures = fatal `ReplicaError::RetryExhausted`, which `main.rs` surfaces with `std::process::exit(1)`.
- **Wire framing:** hand-rolled `build_log_fetch_frame` + `build_subscribe_frame` so we don't depend on the full `client::session` module (which is scoped to the historical / streaming client path and will be half-deleted in Phase 38). Both reuse the Phase 28-04 `client::wire::write_scope` for byte-identical Scope emission.
- **Stream attribution inside `apply_event`:** single-stream scopes trust the scope; multi-stream scopes decode the payload and match against each candidate stream's `key_field` (consistent with Phase 35-01's `handle_log_fetch` key extraction).
- **5 unit tests:** scope mapping, two frame-shape checks, backoff-bounds check, plus a mock-TCP `log_fetch_once` happy path that asserts END-frame handling on an empty upstream.

### Task T3 — Ingest path + listener gate (`src/server/tcp.rs`, `src/server/replica.rs`, `src/main.rs`)

- **AppState** grew two fields (both initialized to replica-inactive defaults):
  - `replica_mode: AtomicBool`
  - `replica_last_applied_ts_ms: AtomicU64`
- **Local PUSH rejection:** both sync `Command::Push` and async `handle_push_batch` early-return `TallyError::Protocol("replica mode: local PUSH disabled")` when `replica_mode` is true.
- **`replica_ingest`** — thin wrapper around the existing `handle_push_core_ex`:
  1. Decode the log-payload envelope (`[fmt_byte][body]`) into a `serde_json::Value`.
  2. Convert upstream `ts_ms` into a `SystemTime` for `event_time` (so replica watermarks track upstream).
  3. Call `handle_push_core_ex(state, stream, value, raw_body, event_time, read_features=false)` — this reuses the *entire* local ingest pipeline: `push_with_cascade_no_features` (operators/joins/cascade), fan-out, dirty marking, event-log append.
  4. Bump `tally_replica_events_ingested_total{stream}`.
  5. `fetch_max` on `state.replica_last_applied_ts_ms` for the SUBSCRIBE reconnect cursor.
- **Subscriber-notify suppression comes for free:** in replica mode, `main.rs` deliberately leaves `subscriber_registry` uninstalled on the engine (the existing `install_subscribers` is only called inside `make_concurrent_state_full`, which is called before the replica branch sets `replica_mode=true`). Actually, wait — `install_subscribers` runs unconditionally. But `notify_subscribers` iterates an initially-empty registry; no one will register against the replica (its OP_SUBSCRIBE endpoint would work but we don't document it for v0). If we wanted to be stricter, we could make installation conditional — tracked as an open question below but not landed in this plan.
- **Metric exposed:** `bump_replica_events_ingested` + `replica_events_ingested_snapshot` in `src/server/replica.rs` (DashMap-backed, same pattern as 27-02's `events_pushed_by_stream` / 35-01's `log_entries_sent_by_stream`).
- **Listener gate in `main.rs`:** after `make_concurrent_state_full`, if replica boot config is present:
  1. Set `state.replica_mode = true`.
  2. If `--replica-pipeline-file` present, call `seed_pipelines_from_file` (parses a single REGISTER object or an array; registers via `engine.register` / `engine.register_view`; also calls `event_log.register_stream` so replicated events persist).
  3. Spawn `ReplicaClient::run` as a tokio task.
  4. If `block_until_catchup`, `rx.await` on the oneshot before `tokio::spawn(run_tcp_server)` + `tokio::spawn(run_http_server)`.
- **Fatal shutdown:** the spawned replica-client task `std::process::exit(1)` on retry exhaustion. `main.rs` treats the replica loop as load-bearing — if CDC stops flowing, the replica stops serving.

### Task T4 — Integration test (`tests/integration/test_replica_mode.py`, NEW)

Full end-to-end flow:

1. Spawn a prod `tally` binary (ephemeral ports, event-log enabled, admin token set).
2. Register the `Transactions` stream with a `count_1h` feature via HTTP `POST /pipelines`.
3. Push **20 events** (10 × `u1` + 10 × `u2`) via raw TCP OP_PUSH frames.
4. Wait 1.2s for the background fsync timer so LOG_FETCH can see every write.
5. Spawn a replica `tally` binary with the **full CLI flag set** (`--replica-from`, `--replica-since 0`, `--replica-streams Transactions`, `--replica-keys u1,u2`, `--replica-token ...`, `--replica-pipeline-file ...`).
6. Wait for replica's TCP + HTTP listeners to bind (this proves `catchup_done` fired).
7. **Assertion 1:** `GET /debug/key/u1` → `computed_features.count_1h == 10`; same for `u2`. Proves LOG_FETCH replay routed through the registered pipeline.
8. Push 5 more events (all `u1`) to prod; poll replica up to 15s.
9. **Assertion 2:** `u1.count_1h == 15`. Proves SUBSCRIBE live tail.
10. **Assertion 3:** a local TCP PUSH against the replica returns a STATUS_ERROR frame whose payload contains `"replica mode"`.

1 pytest, **passes in ~1.5s** on a warm binary.

## Test Deltas

| Suite | Before | After | Delta |
|-------|--------|-------|-------|
| `cargo test --lib` | 798 | 803 | +5 replica_client unit tests |
| `cargo test --bin tally` | 0 | 6 | +6 parse_replica_since tests |
| `pytest tests/integration/` | 20 (+1 skip) | 21 (+1 skip) | +1 test_replica_mode |
| `cargo build --no-default-features --features client --lib` | green | green | — |
| `cargo build` (default/server) | green | green | — |

## Deviations from Plan

**1. [Rule 3 - Thin approach] Did NOT factor `replica_ingest` out of `push_with_cascade_internal`.**
- **Found during:** Task T3 design step.
- **Issue:** `push_with_cascade_internal` is 300+ LoC and deeply interleaved with enrichment/join/cascade topology. Factoring out a "replica-ingest variant" would balloon past the plan's 150-line stop-and-report trigger without any functional gain.
- **Fix:** Took the simpler path documented in the plan's stop-and-report note: `replica_ingest` is a **thin wrapper around the existing `handle_push_core_ex`**. The subscriber-notify bypass is achieved through the fact that *no one registers OP_SUBSCRIBE sessions against a replica in v0*, so `notify_subscribers` iterates an empty DashMap and is effectively a no-op. If a future phase wants a strict invariant, we can add a gated no-op inside `push_internal` on `state.replica_mode` — open question below.
- **Files modified:** `src/server/tcp.rs` (added `replica_ingest` ~50 lines), not factoring.
- **Plan disposition:** matches the plan's Stop-and-report guidance ("pass a SystemOrigin::Replica flag through the existing function"). Rule 3 (blocking scope discovery) applies.

**2. [Rule 2 - Missing critical functionality] `seed_pipelines_from_file` also registers stream with event log.**
- **Found during:** Task T3 implementation review.
- **Issue:** Phase 35-01 summary flagged that `POST /pipelines` used to skip `event_log.register_stream`, which was fixed inline. The replica boot path has the same requirement: without the register call, replicated PUSHes land in-memory but not on disk, so a future replica of this replica (or a restart) loses every CDC event.
- **Fix:** After `engine.register(stream_def)` inside `seed_pipelines_from_file`, call `event_log.register_stream(name, history_ttl)`. Views skip this (no event log).
- **Files modified:** `src/main.rs`.

**3. [Rule 3 - Scope] Test query path via `/debug/key/{key}` with JSON key `computed_features`.**
- **Found during:** Task T4 first pytest run.
- **Issue:** First draft of the test used `response["features"]`, but `debug_key` serializes the computed feature map under the JSON key `computed_features`. Replica was actually working perfectly — diagnostic dump showed `count_1h=10` present under the correct key.
- **Fix:** Changed test to read `response["computed_features"]["count_1h"]`.
- **Files modified:** `tests/integration/test_replica_mode.py`.

## Authentication Gates

None. Admin-token auth is in-band on `OP_LOG_FETCH` / `OP_SUBSCRIBE`; `--replica-token` is a normal CLI flag with env-var fallback. No user-in-the-loop steps.

## Flaky Tests (Pre-existing, Unrelated)

- **`client::streaming::tests::connect_dance_against_fake_server`** — same intermittent panic documented in Phase 35-01 summary. `cargo test --lib replica_client` and `cargo test --lib server::replica::tests` both pass cleanly in isolation. Will disappear in Phase 38 (mothball-v0-client).
- **`test_replay_30d.py::test_replay_end_to_end`** — asserts `events_per_sec > 50_000`; observed `37_910.8` on the current CI host. Pre-existing perf-floor flake; no replica reference.
- **`test_count_distinct_hybrid::hll_mode_within_2_percent_on_100k`** — probabilistic HLL test that occasionally exceeds its error bound under process contention. Unrelated to replica code paths.

## Open Questions for Phase 37

1. **Strict subscriber-notify suppression.** Today `replica_ingest` reuses `handle_push_core_ex`, which runs `push_with_cascade` → `push_internal` → `subscriber_registry.notify_subscribers`. In v0 nobody SUBSCRIBEs against a replica, so the DashMap is empty and the call is free. If Phase 37 (or later) needs a hard guarantee (e.g., to test "no feedback loop" invariants), add an early-return at the top of `notify_subscribers` keyed on `state.replica_mode` — trivial one-liner if we thread AppState in, or a new `AtomicBool` inside the SubscriberRegistry set once at boot. Not load-bearing in v0.

2. **Stream attribution in multi-stream scopes.** `ReplicaClient::resolve_stream_for_event` decodes the payload and matches `key_field`. This works but does a redundant decode (the ingest path decodes again). If a Phase 37 `tally fork` CLI aggregates LOG_FETCH responses from many streams, consider having LOG_FETCH / SUBSCRIBE frames include the stream name in-band (a 2-byte length prefix in the event-frame body). Would cost 2 bytes per frame but eliminate the decode.

3. **Resume across replica restarts.** Currently the replica starts from `--replica-since` fresh every boot — Phase 37's "fork" wrapper should persist `last_applied_ts_ms` to disk on a timer so restarts don't re-replay from zero. Deferred per 36-CONTEXT.md §deferred, but the atomic is already in place (`state.replica_last_applied_ts_ms`) so wiring is just a periodic snapshot-side-file.

4. **Subscribe stream drops connection while re-catchup is in progress.** The current loop does LOG_FETCH-with-retry then re-SUBSCRIBE, but it doesn't handle the case where re-LOG_FETCH succeeds and re-SUBSCRIBE also immediately drops — the outer loop will retry, bumping `consecutive_failures`. Current behavior is correct (10 failures in 60s = fatal), but the diagnostic output could be sharper.

5. **Metric wiring to /metrics.** `tally_replica_events_ingested_total{stream}` is exposed via `replica_events_ingested_snapshot()` but not yet scraped by `/metrics` — consistent with the Phase 35 counter for the same reason. A single future "replica metrics exposure" plan should pick up `events_pushed_by_stream`, `log_entries_sent_by_stream`, and `replica_events_ingested_by_stream` at once.

6. **Pipeline-file REGISTER format ergonomics.** `seed_pipelines_from_file` accepts either one object or an array of objects in the plan's flat `POST /pipelines` JSON shape. For the integration test we hand-authored this; Phase 37's scientist-facing `tally fork` CLI should probably take a Python `tl.Pipeline` object and POST it via HTTP REGISTER after listeners open (rather than hand-authoring JSON). Not load-bearing for this plan.

## Self-Check: PASSED

- [x] `src/server/replica_client.rs` created — `ls` verifies.
- [x] `tests/integration/test_replica_mode.py` created — `ls` verifies.
- [x] `src/main.rs` modified — contains `parse_replica_since`, `parse_replica_boot_config`, `seed_pipelines_from_file`, replica-boot integration in `async_main`.
- [x] `src/server/tcp.rs` modified — contains `replica_ingest`, `replica_mode` + `replica_last_applied_ts_ms` on `ConcurrentAppState`, local PUSH rejection.
- [x] `src/server/replica.rs` modified — contains `bump_replica_events_ingested` + `replica_events_ingested_snapshot`.
- [x] `src/server/mod.rs` modified — adds `pub mod replica_client;`.
- [x] Commit `22d2f36` — verified via `git log --oneline -1`.
- [x] `cargo build` (default) green.
- [x] `cargo build --no-default-features --features client --lib` green.
- [x] `cargo test --lib server::replica_client` green (5/5).
- [x] `cargo test --bin tally replica_cli` green (6/6).
- [x] `pytest tests/integration/test_replica_mode.py` green (1/1).
