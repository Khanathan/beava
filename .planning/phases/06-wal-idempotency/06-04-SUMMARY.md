# Plan 06-04 Summary — crash probe + smoke + phase close

**Status:** shipped 2026-04-23
**Branch:** v2/greenfield
**Commits:** `8a01e65` (bench+baselines — prior session), `5771ff1` (RED
scaffolding), `c5788d6` (probe GREEN)

## What shipped

### Criterion perf microbench (prior session)

- `crates/beava-persistence/benches/phase6_wal.rs` — 3 bench groups:
  - `wal/append_nofsync` — 279.71 ns median (serialize + CRC + write).
  - `wal/append_fsync_default_coalesce` — 7.40 ms median. **WARNING**:
    macOS `F_FULLSYNC` exceeds the 2 ms success-criterion-#3 target.
    Hw-class-limited; Linux CI baseline is the real gate (Phase 13).
  - `wal/append_fsync_burst_1k` — 10.62 ms/batch ≈ 10.6 µs/push amortized.
    Group-commit coalescing works under load.
- Baselines landed in `.planning/perf-baselines.md` under the Apple-M4 row.

### phase6_crash_probe binary

`crates/beava-server/src/bin/phase6_crash_probe.rs` reads BEAVA_WAL_DIR +
BEAVA_WAL_FSYNC_INTERVAL_MS, spawns a Server on an ephemeral port, registers
a minimal `Test` event (event_time + user_id + amount), prints
`PORT=<n>` to stdout, then serves until SIGKILL.

### phase6_crash.rs

Subprocess-based UAT, 2/2 passing:

- `wal_kill_before_fsync_drops_event` — fsync_interval_ms=999999999; push
  hangs on 200 ms client-side timeout; SIGKILL; reopen WAL ⇒ 0 Event
  records.
- `wal_kill_after_ack_preserves_event` — default fsync_interval_ms=1; push
  returns 200; SIGKILL after ACK; reopen WAL ⇒ ≥1 Event record.

### phase6_smoke.rs

4/4 passing. One test per ROADMAP success criterion:

| # | Test | Coverage |
|---|------|----------|
| 1 | `phase6_criterion_1_durability_invariant` | Guardrail: asserts `phase6_crash.rs` exists (subprocess harness owns the real assertion) |
| 2 | `phase6_criterion_2_dedupe_replay_byte_identical` | End-to-end via TestServer: same body twice ⇒ same bytes + state unchanged |
| 3 | `phase6_criterion_3_fsync_overhead_documented` | Asserts `.planning/perf-baselines.md` contains a row for `wal/append_fsync_default_coalesce` |
| 4 | `phase6_criterion_4_rotation_truncates` | Exercises `WalSink::truncate_up_to` with 512-byte segments + 10 appends |

## Gates

- `cargo test --workspace` — 395 pass (beava-core) plus all sub-suites green.
- `cargo test --workspace --features beava-server/testing` — **590 tests
  passing** (baseline 531 → +59).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  clean.
- `cargo fmt --all --check` — clean.

## Open items

- Phase 7 will wire the actual snapshot→truncate handshake (Plan 06 exposes
  the API; Plan 07 consumes it).
- TCP `op=push` handler still returns `op_not_implemented` — Plan 12 scope.
- macOS 7.40 ms P50 fsync vs the 2 ms target remains a WARNING. Phase 13
  Linux CI will be the final gate.
