---
captured: 2026-05-05
status: idea (post-13.5.2-postclose)
prerequisites: none — independent of v0 critical path
suggested_phase: v0.0.x point release OR pre-13.8 hardening pass
---

# Workspace test determinism — kill all `cargo test --workspace` flakes

## Goal

`cargo test --workspace --features testing` MUST pass 100% deterministically across consecutive runs (target: 5/5 green back-to-back). Today individual lib tests (`-p beava-core --lib` = 618/618) and Python v0 acceptance (`pytest python/tests/v0/` = 89/89) are deterministic; the workspace integration tier flakes 1-2 test crates per run from a rotating set due to **process-global state shared across parallel `TestServer` instances**.

## Observed flake population (Phase 13.5.2-postclose session)

All of the below pass deterministically when run in isolation (`cargo test -p beava-server --test <name>`); only flake under workspace parallelism:

| Test | Failure mode | Fix verdict |
|---|---|---|
| `phase2_5_smoke::criterion_6_pipelined_registers_return_in_order` | OP_ERROR_RESPONSE (0xFFFF) instead of OP_REGISTER (0x0001) for pipelined `/register` calls | **partially fixed** — `BEAVA_TCP_MAX_FRAME_BYTES` env-var leak killed in commit `acac4254`; flake still reproduces (different env-var leak suspected) |
| `phase12_6_join_union_rejection::register_{join,union}_returns_feature_removed_no_*_v0` | `WalSpawn("io: File exists (os error 17)")` at TestServer::spawn | unfixed |
| `phase12_8_metrics_endpoint::test_cold_entity_evictions_starts_at_zero` | same `WalSpawn(File exists)` race | unfixed |
| `phase13_4_get_row_shape` (5 tests) | 0/5 in workspace, 5/5 isolated; suspected port collision or readiness race | unfixed |

## Root-cause family — process-global state across parallel TestServers

Each `cargo test --workspace` invocation spawns ~107 test binaries; cargo runs multiple binaries in parallel. Every test binary is one OS process; tests within a binary further parallelize via `--test-threads`. `TestServer::spawn()` reads several `BEAVA_*` env vars at boot and stores values into per-server config. When test A sets an env var without restoring it, test B's TestServer reads the leaked value mid-test.

### Confirmed leaks (1 fixed)

| Env var | Set by | Read by | Status |
|---|---|---|---|
| `BEAVA_TCP_MAX_FRAME_BYTES` | `TestServerBuilder.tcp_max_frame_bytes()` (now removed) | `io_thread_worker::parse_and_push` per-frame | **FIXED** in `acac4254` — plumbed via `WorkerConfig.tcp_max_frame_bytes` field; production main.rs config-load-time read in `config.rs:267` preserved |
| `BEAVA_WAL_BUFFERS` / `BEAVA_WAL_BUFFER_SIZE_MB` / `BEAVA_WAL_TICK_MS` | `wal_env_var_tunables` test (sets to 99999/10000/99999 then restores via local mutex) | `WalConfig::resolve_from_env` at every `bind_with_state` | UNFIXED — local-only mutex doesn't protect parallel test crates |
| `BEAVA_IO_THREADS` | `phase12_08_*` tests (set/restore) | `default_io_threads()` at server boot | UNFIXED |
| `BEAVA_TEST_MODE` | `phase13_4_reset_gated_env_var` (uses `serial_test::serial`) | `bind_with_config` / `build_runtime_state` | partial — serial_test serializes within file but not cross-file |
| `BEAVA_MEMORY_GOV_ENFORCE` | `phase12_8_*` tests (local mutex) | `pre_check_unbounded_op_in_lifetime_mode` | partial — local-mutex-only |

### Filesystem race (separate from env vars)

`WalSink::spawn` calls `WalWriter::open` which uses `OpenOptions::new().create_new(true)` on `wal-{lsn:016x}.log`. Default WAL dir is `temp_dir/beava-test-wal-{pid}-{atomic_counter}`. Despite `(pid, counter)` being unique, EEXIST surfaces in `phase12_6_join_union_rejection` and `phase12_8_metrics_endpoint` under workspace parallelism. Cause unclear; possible candidates:
- `std::fs::create_dir_all` race on shared parent path
- macOS tempdir caching / writeback
- counter wrap-around or alignment with another concurrent allocator

## Two paths forward

### Path A — Architectural (recommended)

Plumb every `BEAVA_*` env-var hot-path read through the `ServerV18Config` struct (mirrors what we did for `tcp_max_frame_bytes`):

1. Add `wal_buffers`, `wal_buffer_size_mb`, `wal_tick_ms` fields to `ServerV18Config` (or a nested `WalConfigOverride`); read env at config-load time only in production `main.rs`.
2. Add `io_threads: Option<usize>` field; read env at config-load time only.
3. Add `test_mode_override: Option<bool>`; consolidate the two env-var paths into one config-time resolution.
4. Add `memory_governance_enforce: Option<bool>`; same treatment.
5. Update `TestServerBuilder` to set these via builder methods, not env-var pokes.
6. Update each test that previously poked env vars to use the new builder methods.
7. Add an `architectural_test` (similar to `phase12_6_mio_only_dataplane.rs` and `phase12_7_no_table_surface.rs`) that greps for `std::env::set_var` in `crates/beava-server/tests/` and fails on any new occurrence.

For the WAL EEXIST race: instead of `temp_dir + (pid, counter)`, use `tempfile::tempdir()` (which returns a guaranteed-unique path with auto-cleanup). Update `TestServerBuilder::default()` accordingly.

**Estimated effort:** 1-2 days. Touches `crates/beava-core/src/config.rs`, `crates/beava-server/src/server.rs`, `crates/beava-server/src/testing.rs`, `crates/beava-server/src/wal_config.rs`, ~10 test files. Companion architectural-test tripwire makes the fix permanent.

### Path B — Stopgap (faster, less correct)

Add a process-global mutex (`static TEST_SERVER_SPAWN_LOCK: tokio::sync::Mutex<()>`) inside `TestServerBuilder::spawn()` so only one TestServer is being constructed at a time. Held only across the env-var-read window, released before `wait_ready` polls. Doesn't kill the underlying smell, but is a 5-line patch that buys deterministic test runs immediately.

Trade-off: increases workspace test wall-clock by ~10-30s (sequential bind windows; each is fast). Doesn't help if one test sets an env var and never clears it (leaked state survives the lock release).

**Estimated effort:** 30 minutes. Surgical to one file. Will NOT catch the WAL EEXIST race (that's filesystem state, not env-var-bound) — needs the `tempfile::tempdir()` swap regardless.

## Recommended sequencing

1. **Path B first** (30 min) — kills 80% of flakes immediately, unblocks workspace tests as a deterministic gate.
2. **`tempfile::tempdir()` swap in `TestServerBuilder::default()`** (30 min) — kills WAL EEXIST family.
3. **Path A architectural** (1-2 days, defer to v0.0.x or pre-13.8 hardening week) — closes the underlying smell + adds tripwire so it can't reappear.

## What's already landed (reference)

Phase 13.5.2-postclose session shipped 5 atomic commits as foundation:
- `4e4e9e81` fix(13.5.2): geo `lat_field`/`lon_field` parsing
- `4e57f31f` chore(13.5.2): cargo fmt drift
- `1a8da88d` test(13.5.2): align FirstN/LastN/HourOfDayHistogram unit tests
- `4d483f8a` chore(13.5.2): drop dead code + unused import
- `acac4254` fix(13.5.2): plumb tcp_max_frame_bytes via WorkerConfig

The `tcp_max_frame_bytes` plumbing in `acac4254` is the **template** for Path A — same pattern repeated for every other `BEAVA_*` env var.

## Acceptance criteria

- [ ] `for i in $(seq 1 5); do cargo test --workspace --features testing; done` — 5/5 runs all-green
- [ ] No `std::env::set_var(...)` calls in `crates/beava-server/tests/` (architectural tripwire enforces)
- [ ] `TestServerBuilder` exposes builder methods for all per-server config that was previously env-driven
- [ ] Production main.rs continues to read `BEAVA_*` env vars at config-load time (env interface preserved for ops)

## Reference files

- `crates/beava-server/src/testing.rs` — TestServer + TestServerBuilder
- `crates/beava-server/src/server.rs` — `bind_with_state` / `build_runtime_state` / `run_serve_loop` plumbing
- `crates/beava-runtime-core/src/io_thread_worker.rs` — `WorkerConfig` + `parse_and_push` (already cleaned up for `tcp_max_frame_bytes`)
- `crates/beava-server/src/wal_config.rs` — `WalConfig::resolve_from_env`
- `crates/beava-core/src/config.rs:267` — production env-var read site
- `crates/beava-server/tests/phase12_6_mio_only_dataplane.rs` — example architectural-test tripwire pattern
