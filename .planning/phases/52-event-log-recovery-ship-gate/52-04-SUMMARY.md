---
phase: 52-event-log-recovery-ship-gate
plan: 04
subsystem: reshard
tags: [reshard, migration, cli, ahash, tpc-dx-03, offline-tool]
requirements: [TPC-DX-03]

dependency_graph:
  requires:
    - Phase 52-01 (snapshot v8 with shard_count — reshard reads/writes v8)
    - Phase 52-02 (per-shard layout — reshard reads shard-N/streams/*/log.bin)
    - Phase 48 (ahash routing — rehash_to_shard uses same AHasher)
  provides:
    - beava::reshard::rehash_to_shard(key, N) deterministic routing
    - beava::reshard::reshard_data_dir(from_n, to_k, data_dir, out_dir)
    - beava::reshard::swap_replace(data_dir, out_dir) atomic rename swap
    - beava::reshard::parse_reshard_args, is_reshard_subcommand, print_reshard_help
    - tally reshard subcommand in main.rs (offline N→K migration)
  affects:
    - Any operator needing to scale from N=1 to N=K shards offline (D-06/D-07)
    - store.rs error string references "tally reshard --from N --to K" (already correct)

tech_stack:
  added:
    - fs2 = "0.4" (exclusive file-lock for offline tool safety)
  patterns:
    - "rehash_to_shard uses AHasher::default() + hash(key.as_bytes()) % shard_count"
    - "N=1 identity shortcut: shard_count==1 always returns 0 (no hash needed)"
    - "fs2 try_lock_exclusive on .beava.lock — refuses concurrent server+reshard"
    - "Length-prefixed postcard framing for log I/O (matches EventLog wire format)"
    - "extract_routing_key: JSON key field extraction with raw-bytes fallback"
    - "swap_replace: two sequential fs::rename calls (POSIX atomic per rename(2))"

key_files:
  created:
    - src/reshard/mod.rs
    - src/reshard/rehash.rs
    - tests/test_reshard_cli.rs
  modified:
    - src/lib.rs
    - src/main.rs
    - Cargo.toml

decisions:
  - "reshard module lives in src/reshard/ and is pub in lib.rs — accessible as beava::reshard from tests and future in-process callers (D-07)"
  - "rehash_to_shard uses AHasher::default() (fixed seed) — deterministic within a binary version; matches Phase 48 live routing"
  - "N=1 identity shortcut avoids hash entirely when shard_count==1 (correctness guarantee)"
  - "extract_routing_key tries JSON .key field; falls back to raw payload bytes — no entries silently dropped on corrupt/non-JSON payloads"
  - "parse_reshard_args is pub so tests can call it directly without spawning a process"
  - "swap_replace is a separate pub fn (not inlined in main.rs) — testable without process spawn"

metrics:
  duration_minutes: 25
  completed_at: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  tasks_total: 2
  files_created: 3
  files_modified: 3
---

# Phase 52 Plan 04: tally reshard CLI Tool Summary

**One-liner:** Offline N→K shard migration via `tally reshard --from N --to K` using deterministic ahash routing, fs2 lock guard, and atomic --replace swap (TPC-DX-03, D-06/D-07).

## What Was Built

### Task 1: reshard module + rehash_to_shard + reshard_data_dir

`src/reshard/rehash.rs`:

- `pub fn rehash_to_shard(key: &str, shard_count: u8) -> u8`: deterministic
  routing via `ahash::AHasher::default()` + `hash(key.as_bytes()) % shard_count`.
  N=1 identity shortcut returns 0 without hashing. Consistent with Phase 48 live
  routing path.

`src/reshard/mod.rs`:

- `pub mod rehash` + `pub use rehash::rehash_to_shard` re-export.
- `pub fn reshard_data_dir(from_n, to_k, data_dir, out_dir) -> io::Result<()>`:
  1. Acquires `fs2::try_lock_exclusive()` on `data_dir/.beava.lock`; returns
     `Err(WouldBlock, "data-dir is held by a running server")` on contention
     (T-52-04-01).
  2. Loads `data_dir/snapshot.bin` via `load_snapshot_file`; validates
     `shard_count == from_n` (T-52-04-04).
  3. Creates `out_dir/shard-{0..to_k-1}/streams/` directory trees.
  4. For each source shard: walks `shard-{s}/streams/*/log.bin`, reads all
     postcard-framed entries, routes each via `rehash_to_shard(key, to_k)`,
     appends to target shard's log. Corrupt entries surface as `Err`
     (T-52-04-03).
  5. Writes `out_dir/snapshot.bin` (v8, `shard_count = to_k`).
  6. Prints `"Resharding shard N/M..."` + `"Done. Output: {out_dir}"` to stdout.
- `pub fn swap_replace(data_dir, out_dir) -> io::Result<()>`:
  `fs::rename(data_dir, data_dir.bak)` then `fs::rename(out_dir, data_dir)`.
  POSIX `rename(2)` is atomic per call (T-52-04-02).
- `pub fn parse_reshard_args(args) -> Result<ReshardArgs, String>`: parses
  `--from N --to K --data-dir PATH --out-dir PATH [--replace]`; returns `Err`
  with usage message on missing/malformed args.
- `pub fn print_reshard_help()`: emits usage to stderr.
- `pub fn is_reshard_subcommand(args) -> bool`: checks `args[1] == "reshard"`.

`src/lib.rs`: `pub mod reshard` added.

### Task 2: tally reshard CLI subcommand dispatch

`src/main.rs`:

- Before the fork-subcommand check, dispatches on `is_reshard_subcommand`.
- Calls `parse_reshard_args`; on error prints message + `print_reshard_help` + exits 1.
- Calls `reshard_data_dir`; on error prints message + exits 1.
- If `--replace` flag set: calls `swap_replace`; on error exits 1.
- On success: exits 0 (never reaches `async_main` / server startup).

`Cargo.toml`: `fs2 = "0.4"` added to `[dependencies]`.

### Test Suite (`tests/test_reshard_cli.rs`)

9 tests, all passing:

| # | Name | Coverage |
|---|------|----------|
| 1 | `test_reshard_rehash_determinism` | 1000-iteration determinism check |
| 2 | `test_reshard_n1_identity` | N=1 always returns 0 for 6 varied keys |
| 3 | `test_reshard_n8_distribution` | 10k keys, all in [0,7], each shard ≥500 |
| 4 | `test_reshard_n1_round_trip` | Double N=1 application returns 0 both times |
| 5 | `test_reshard_data_dir_refuses_locked_dir` | fs2 lock held → Err with "held by a running server" |
| 6 | `test_reshard_cli_e2e_1_to_8` | N=1→N=8 with 10 entries; all 8 shard dirs exist; total entries=10; snapshot shard_count=8 |
| 7 | `test_reshard_cli_missing_args_returns_error` | 4 missing-arg scenarios each return Err |
| 8 | `test_reshard_cli_replace_atomic_swap` | swap_replace produces data_dir.bak and moves out_dir |
| 9 | `test_reshard_cli_locked_dir_error` | Locked dir → CLI-level error with correct message |

## Test Results

```
cargo test --release --test test_reshard_cli -- --nocapture
running 9 tests
test test_reshard_cli_missing_args_returns_error ... ok
test test_reshard_n1_identity ... ok
test test_reshard_n1_round_trip ... ok
test test_reshard_rehash_determinism ... ok
test test_reshard_n8_distribution ... ok
test test_reshard_cli_locked_dir_error ... ok
test test_reshard_data_dir_refuses_locked_dir ... ok
test test_reshard_cli_replace_atomic_swap ... ok
test test_reshard_cli_e2e_1_to_8 ... ok
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

cargo test --release -p beava -- --test-threads=1
Pre-existing failures (OS error 49, macOS network bind — confirmed pre-existing):
  - backpressure_drops_subscriber
  - subscribe_then_push_delivers_events
All other tests: ok
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing] `parse_reshard_args` and `swap_replace` extracted as public functions**
- **Found during:** Task 2 test design
- **Issue:** Plan specified testing `--replace` and missing-args behavior via the CLI,
  but spawning a subprocess from integration tests is fragile and slow. The plan's
  own test descriptions say "test the parse helper directly to stay hermetic."
- **Fix:** Extracted `parse_reshard_args` and `swap_replace` as `pub fn` in `src/reshard/mod.rs`
  so tests call them directly without process spawning. CLI dispatch in `main.rs` calls
  the same functions.
- **Files modified:** `src/reshard/mod.rs`, `tests/test_reshard_cli.rs`

**2. [Rule 2 - Missing] `extract_routing_key` fallback for non-JSON payloads**
- **Found during:** Task 1 implementation
- **Issue:** Plan specified routing by `entry.key`, but `LogEntry.payload` is raw bytes
  (JSON or binary-tagged per `LOG_FMT_JSON`/`LOG_FMT_BINARY`). No `.key` field exists
  directly on `LogEntry`. A format-tag strip + JSON parse is needed.
- **Fix:** `extract_routing_key` strips the LOG_FMT tag byte if present, attempts JSON
  `.key` field extraction, and falls back to raw payload bytes as the routing key.
  This ensures no entries are silently dropped on corrupt or non-JSON payloads
  (T-52-04-03 correctness).
- **Files modified:** `src/reshard/mod.rs`

## Known Stubs

None — `reshard_data_dir` is fully wired and produces a complete output directory. The
`engine=None` stub from 52-03 (no operator-state replay during recovery) is unrelated
to this plan; reshard operates on persisted log bytes, not operator state.

## Threat Flags

None — all security surfaces were in the plan's `<threat_model>`:
- T-52-04-01: fs2 try_lock_exclusive → `WouldBlock` with clear message ✓
- T-52-04-02: swap_replace uses `fs::rename` (POSIX atomic) + bak suffix ✓
- T-52-04-03: corrupt entries surface as `Err` from postcard decode ✓
- T-52-04-04: `shard_count != from_n` returns `Err` with actionable message ✓

## Self-Check: PASSED

Files verified:
- `src/reshard/rehash.rs`: `rehash_to_shard` with AHasher + N=1 shortcut ✓
- `src/reshard/mod.rs`: `reshard_data_dir`, `swap_replace`, `parse_reshard_args`,
  `print_reshard_help`, `is_reshard_subcommand` all present ✓
- `src/lib.rs`: `pub mod reshard` registered ✓
- `src/main.rs`: reshard subcommand dispatch before fork check ✓
- `tests/test_reshard_cli.rs`: 9 tests, all passing ✓
- Commits: 0cdec29 (RED tests + Cargo.toml), 9d3fa54 (GREEN implementation) ✓
