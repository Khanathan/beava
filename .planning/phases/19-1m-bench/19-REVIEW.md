---
phase: 19-1m-bench
reviewed: 2026-04-26T22:45:00Z
depth: standard
files_reviewed: 17
files_reviewed_list:
  - crates/beava-bench/src/blast_shape.rs
  - crates/beava-bench/src/lib.rs
  - crates/beava-bench/src/bin/beava-bench-v18.rs
  - crates/beava-bench/tests/blast_shape_test.rs
  - crates/beava-bench/tests/bench_v18_blast_smoke.rs
  - crates/beava-bench/benches/blast_shape_bench.rs
  - crates/beava-bench/Cargo.toml
  - python/benches/__init__.py
  - python/benches/_configs.py
  - python/benches/blast.py
  - python/benches/blast_shape.py
  - python/tests/bench/__init__.py
  - python/tests/bench/conftest.py
  - python/tests/bench/test_blast_smoke.py
  - python/pyproject.toml
  - scripts/run_phase19_blast_matrix.sh
  - python/tests/conftest.py
findings:
  critical: 0
  warning: 6
  info: 8
  total: 14
status: issues_found
---

# Phase 19: Code Review Report

**Reviewed:** 2026-04-26T22:45:00Z
**Depth:** standard
**Files Reviewed:** 17 (16 in scope + python/tests/conftest.py for cross-reference)
**Status:** issues_found

## Summary

The Phase 19 1M-EPS bench harness is overall well-designed and carefully instrumented, with explicit attention to measurement honesty (D-02 / D-13 / D-15). Test coverage is solid — TDD-discipline-compliant per CLAUDE.md (red tests precede impl), the Pool=N builder has 10 invariant tests including 2 proptests, and three end-to-end smoke tests cover the wiring. Wire endianness is correctly big-endian per Phase 2.5 (verified `decode_pool_frame` against `beava_core::wire::encode_frame`). The cargo bench harness=false flag is set correctly. The pyproject.toml exclude rule for `benches/**` is in place per D-08.

No Critical issues found. Six Warnings touch: (1) a divide-by-zero panic risk if `total_events=0`, (2) `set -e` deliberately omitted from the bash script but missing fallback for `cargo build` failure, (3) a known Python Zipfian sampler accuracy compromise (alpha=1.0 routed to alpha=1.0001), (4) a temp-file/dir cleanup gap on script error/SIGINT in the Python helper, (5) a `pushes_cap.fetch_sub(1)` race during cap rollback, and (6) a barrier-deadlock window if `build_worker_pool` returns None on connect failure.

Eight Info items cover code cleanliness opportunities — empty `__init__.py` files (intentional but worth a one-line note), TODO-style comments, magic numbers, and an unused variable.

## Warnings

### WR-01: Pool indexing panics if `--total-events 0` is passed

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:955, 975, 1180, 1206`

**Issue:** `pool_len = pool.len() as u64` and then `idx % pool_len` on lines 975 and 1206. If a user invokes the bench with `--total-events 0`, `build_worker_pool` calls `build_pool_timed` with `n=0`, returning an empty `Vec<Bytes>`. `pool_len = 0`, and `idx % 0` is a panic in Rust (integer division by zero). The CLI parser doesn't guard against `--total-events 0` (clap accepts `Option<u64>` with default `None`).

The same exposure exists in both `run_tcp_burst_push_worker` (line 975) and `run_tcp_continuous_push_worker` (line 1206).

**Fix:** Either guard `pool_len > 0` before the modulo, or reject `--total-events 0` at CLI parse time. Suggested CLI-level fix:

```rust
// In main() after `let cli = Cli::parse();`
if let Some(n) = cli.total_events {
    anyhow::ensure!(n > 0, "--total-events must be > 0 (got 0)");
}
```

Or defensively in the workers:

```rust
let pool_len = pool.len() as u64;
if pool_len == 0 {
    pool_ready_barrier.wait().await; // already barrier'd above; just bail
    return;
}
```

### WR-02: `set -e` deliberately omitted but `cargo build` failures silently propagate

**File:** `scripts/run_phase19_blast_matrix.sh:20, 37-39`

**Issue:** Line 20 uses `set -uo pipefail` (no `-e`), which is intentional because per-cell `rc=$?` checks need to keep the matrix running across individual cell failures. However, the unconditional `cargo build` at lines 38-39 has no exit-status check. If the build fails (e.g., compilation error after a code edit), the script will proceed to invoke `"$BENCH_BIN"` which doesn't exist, and every cell will produce a misleading "command not found" failure rather than a single clear "build failed" message.

**Fix:** Add explicit exit-status checks for the build steps:

```bash
echo "=== building beava-bench-v18 + beava (release) ==="
if ! cargo build -p beava-bench --release --bin beava-bench-v18; then
    echo "FAIL: cargo build of beava-bench-v18 failed; aborting matrix run"
    exit 1
fi
if ! cargo build -p beava-server --release --bin beava; then
    echo "FAIL: cargo build of beava-server failed; aborting matrix run"
    exit 1
fi
```

### WR-03: Python `_ZipfianSampler` rounds alpha=1.0 to 1.0001 (silent accuracy compromise)

**File:** `python/benches/blast_shape.py:104-109`

**Issue:** The Python `_ZipfianSampler.__init__` quietly bumps `alpha` from 1.0 to 1.0001 to dodge the eta-formula singularity. Comment acknowledges this is a "negligible" statistical difference, but: (a) the Rust sampler in `blast_shape.rs:170-179` handles alpha=1 correctly via a log-uniform inverse-CDF branch, and (b) the Python and Rust harnesses are advertised in CONTEXT.md D-09 as producing apples-to-apples comparable numbers. The drift is unlikely to be visible at typical fraud-shape K=1M, but it means same-seed Python+Rust sequences will diverge — making same-seed cross-language comparison harder.

This is BURST-ONLY harness (Warning 9 deferral) so the Python row is informational, not a regression gate. But the comment is misleading: "matches Gray et al.'s original published recipe" is incorrect — Gray et al. covers alpha != 1 separately, and the standard approach is to use a log-uniform inverse for alpha=1, exactly as the Rust sampler does.

**Fix:** Either implement the alpha=1 log-uniform branch to match the Rust sampler:

```python
def sample(self) -> int:
    u = self.rng.random()
    uz = u * self.zetan
    if uz < 1.0:
        return 0
    if uz < 1.0 + 0.5**self.alpha:
        return 1
    v = self.rng.random()
    if abs(self.alpha - 1.0) < 1e-9:
        # alpha == 1: cumulative ∝ ln(r); inverse is r = 2 * exp(v * (ln(k) - ln(2)))
        ln_k = math.log(self.k)
        ln_2 = math.log(2.0)
        rank = int(2.0 * math.exp(v * (ln_k - ln_2)))
    else:
        rank = int(self.k * ((self.eta * v - self.eta + 1.0) ** (1.0 / (1.0 - self.alpha))))
    return min(max(rank, 0), self.k - 1)
```

Or update the comment to call out the deliberate compromise without the misleading "Gray et al." reference.

### WR-04: Bash `run_python_cell` leaks tempfiles on error/SIGINT

**File:** `scripts/run_phase19_blast_matrix.sh:170-198, 261-264`

**Issue:** `run_python_cell` creates four tempfiles per cell (`cfg_file`, `wal_dir`, `snap_dir`, `srv_log` at lines 171-188). The cleanup at lines 261-264 (`rm -f`/`rm -rf`) only runs on the happy path. If the `python "$PYTHON_BLAST" ...` invocation hangs and the user Ctrl-C's the script, OR if one of the early `return 3`/`return 4` paths fires (lines 233, 280) without falling through to the cleanup, the tempfiles + dirs are left behind in `/tmp`. Across 12 cells × N reruns, `/tmp` accumulates `beava-blast-*` artifacts.

The early-return path at line 230-233 has its own cleanup, but the path at line 280 does NOT clean up `srv_pid`, `srv_log`, `wal_dir`, or `snap_dir` because the `kill "$srv_pid"` happens at line 261 — AFTER the `return 4` path skips it.

Wait — re-reading: line 280 returns BEFORE lines 261-264 run, leaking `srv_pid`, `cfg_file`, `srv_log`, `wal_dir`, `snap_dir`.

**Fix:** Use a per-cell trap or factor cleanup into a helper:

```bash
run_python_cell() {
    # ...
    cfg_file=$(mktemp /tmp/beava-blast-XXXXXX.yaml)
    wal_dir=$(mktemp -d /tmp/beava-blast-wal-XXXXXX)
    snap_dir=$(mktemp -d /tmp/beava-blast-snap-XXXXXX)
    rmdir "$wal_dir" "$snap_dir"
    srv_log=$(mktemp /tmp/beava-blast-log-XXXXXX)

    # Ensure cleanup runs on every exit path (including SIGINT/return).
    local cleanup_done=0
    cleanup() {
        (( cleanup_done )) && return
        cleanup_done=1
        kill "${srv_pid:-}" 2>/dev/null || true
        wait "${srv_pid:-}" 2>/dev/null || true
        rm -f "$cfg_file" "$srv_log"
        rm -rf "$wal_dir" "$snap_dir"
    }
    trap cleanup RETURN

    # ... rest of function — every `return N` triggers cleanup automatically ...
}
```

Or guard each early return with explicit cleanup. Note: this is a cleanliness/CI-hygiene issue — it's not a correctness bug because `/tmp` is purged on reboot.

### WR-05: `pushes_cap.fetch_sub(1, Relaxed)` rollback race on D-13 hard cap

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:797-802 (HTTP), 968-974 (TCP burst), 1187-1194 (TCP continuous)`

**Issue:** The D-13 hard-cap pattern is:
```rust
let prev = pushes_cap.fetch_add(1, Ordering::Relaxed);
if prev >= cap {
    pushes_cap.fetch_sub(1, Ordering::Relaxed);
    break;
}
```

This is the standard "reservation" pattern, but the semantic guarantee is weaker than the comment "{requested, pushed, acked} stay equal" claims:

- Worker A reads `prev = cap` (e.g., cap=1000, prev=1000), then sleeps before its `fetch_sub`.
- Worker B reads `prev = 1001`, also bails, fetch_subs back to 1000.
- Worker A wakes, fetch_subs to 999.
- Now the next caller sees `prev = 999 < cap` and ISSUES one more push past the cap.

In practice this is bounded by `parallel × pdepth` extra pushes (very small relative to N=1M), but the comment claims strict equality. The smoke tests pass because at total_events=1000 with 4 workers / pdepth=16, the over-count probability is essentially zero.

The issue is NOT a correctness bug for the canonical regression-gate cell, but the assertion at line 408-413 of main() (`anyhow::ensure!(requested == pushed)`) could occasionally trip if the over-count race fires in a future scaled-up run.

**Fix:** Either accept the bounded slack and weaken the assertion to `pushed.abs_diff(requested) <= parallel * pipeline_depth`, OR use a CAS (compare-and-swap) loop that prevents the rollback race:

```rust
let cap = match total_cap { Some(c) => c, None => return Ok(true /* unbounded */) };
loop {
    let cur = pushes_cap.load(Ordering::Relaxed);
    if cur >= cap { return Ok(false); }
    if pushes_cap.compare_exchange_weak(cur, cur + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
        return Ok(true);
    }
}
```

Note: The receiver-side D-12 `stop.store(true)` + `sem.close()` PROBABLY fires before the over-count window opens in continuous mode, so this is largely theoretical for the canonical mode. Still worth documenting.

### WR-06: Barrier deadlock window if `TcpStream::connect` fails AFTER barrier wait

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:945-951 (burst), 1155-1161 (continuous)`

**Issue:** In `run_tcp_burst_push_worker` and `run_tcp_continuous_push_worker`, the worker:
1. Calls `build_worker_pool(...)` (returns None on builder error → wait barrier + return — handled correctly)
2. Calls `pool_ready_barrier.wait().await` (line 943, 1153)
3. Calls `TcpClient::connect(tcp_addr).await` / `TcpStream::connect(tcp_addr).await`

If the TCP connection fails AFTER the barrier (e.g., transient ECONNREFUSED), the worker just `return`s (lines 947-950, 1158-1161) WITHOUT incrementing any counter or signaling other workers. The main task's `wall_clock_ms` timer has already started (line 657), so EPS gets divided by elapsed wall time, but the failed worker contributes zero pushes — so the published EPS will be silently lower than intended. There is no error counter increment.

Worse: if ALL workers fail to connect, the bench will silently report `eps = 0 / elapsed_secs` and `pushed=0`, but the assertion at line 411 (`requested == pushed`) will trip with a confusing message that doesn't mention the connect failure.

**Fix:** Increment `errors` on TCP connect failure so the run-level summary surfaces it:

```rust
let mut client = match TcpClient::connect(tcp_addr).await {
    Ok(c) => c,
    Err(e) => {
        eprintln!("TcpClient::connect failed: {e}");
        errors.fetch_add(1, Ordering::Relaxed);
        return;
    }
};
```

Same for the continuous variant. Better yet, surface a per-worker "did_connect" boolean back to main and assert `connected_workers > 0` before reporting EPS.

## Info

### IN-01: Empty `__init__.py` files are a namespace marker only

**File:** `python/benches/__init__.py` and `python/tests/bench/__init__.py` (both 0 bytes)

**Issue:** Both files exist as zero-byte placeholders. This is fine for namespace marking, but a one-line module docstring would document intent and prevent future "is this file supposed to be empty?" confusion.

**Fix:** Add a minimal docstring:

```python
"""Phase 19 bench harness namespace package."""
```

Same for the tests directory.

### IN-02: `test_blast_smoke.py` line 24 — RED-test docstring is now stale

**File:** `python/tests/bench/test_blast_smoke.py:24-25`

**Issue:** The module docstring states "All three tests are RED at this commit (blast.py is not yet created and the pyproject `exclude` rule has not yet been added)." This was true at the test-first commit but is now stale — `blast.py` exists, the pyproject excludes are in place, and the tests should be GREEN. This is a documentation rot issue, not a code bug.

**Fix:** Update the module docstring to reflect current state:

```python
"""Phase 19 Plan 03 — Smoke tests for the Python multi-process blast harness.
[...keep test list...]
At commit 88f1161 these tests are GREEN; the blast.py harness and the
pyproject [tool.hatch.build.targets.wheel] exclude rule are in place.
"""
```

### IN-03: `bench_v18_blast_smoke.rs` — `let _ = stdout` discards potentially useful data

**File:** `crates/beava-bench/tests/bench_v18_blast_smoke.rs:85, 149, 204`

**Issue:** All three smoke tests grab `stdout` from the helper but only assert against `stderr`. The bench binary's `--no-ledger` flag suppresses stdout, so `stdout` should be empty in these tests, but if a future change starts emitting to stdout (e.g., a compatibility output), the tests won't notice. Minor — included only because the tests already capture stdout but never use it.

**Fix:** Either drop the stdout binding (use `let (code, _, stderr)`) or assert `stdout.is_empty()` to catch unintended stdout output:

```rust
let (code, stdout, stderr) = run_with_timeout(cmd, Duration::from_secs(10));
assert!(stdout.is_empty(), "unexpected stdout in --no-ledger mode:\n{stdout}");
```

### IN-04: Magic constants in continuous worker

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:1245-1248`

**Issue:** Two magic numbers (`8 * 1024` for `read_buf` capacity, `64` for `HIST_FLUSH_BATCH`) appear without rationale comments. The HIST_FLUSH_BATCH=64 has a brief explanation in the surrounding comment, but the 8K read buffer is unexplained — at typical fraud-shape ack sizes (~30 bytes), 8K is ~270 acks-per-read, which seems undersized for pdepth=1024 cells.

**Fix:** Either add a brief comment explaining the choice, or factor into named consts:

```rust
const READ_BUF_INITIAL_CAP: usize = 8 * 1024;  // ~270 acks at 30B each; grows as needed
const MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;
const HIST_FLUSH_BATCH: usize = 64;  // mirrors burst-mode lock granularity
```

### IN-05: `KEY_SPACE` = 100,000 is unused in the Pool=N path

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:51`

**Issue:** `const KEY_SPACE: u64 = 100_000;` is used by `make_event_payload` (line 250) and the get-batch sampler (line 575), but the Pool=N path uses `cli.cardinality` (default 1M, not 100K). This means the legacy duration-only path samples from a different keyspace than the new Pool=N path — keys in `[0, 100K)` for HTTP/non-cap mode, keys in `[0, cardinality)` for the Pool path. This is an intentional asymmetry (legacy mode predates Pool=N), but it's not documented anywhere visible.

**Fix:** Add a comment at the const definition:

```rust
/// Key space for the LEGACY duration-only HTTP path (`make_event_payload` +
/// the get-batch sampler). The Pool=N path uses `cli.cardinality` (default 1M)
/// instead. The two paths produce comparable results only when shape=fixed.
const KEY_SPACE: u64 = 100_000;
```

### IN-06: `decode_pool_frame` allows `payload_end < payload_start` check is dead

**File:** `crates/beava-bench/src/bin/beava-bench-v18.rs:1083-1085`

**Issue:** The check `if payload_end < payload_start { return None; }` is unreachable: `payload_start = 7`, `payload_end = 4 + len`, and the prior check `buf.len() < 4 + len` plus `buf.len() < 7` ensures `len >= 3`, so `payload_end >= 7 = payload_start`. This is defensive but dead code. Not a bug — clippy may flag it as `clippy::absurd_extreme_comparisons` if it can prove the constraint, but at runtime there's no way to hit this branch.

**Fix:** Either remove the dead check, or add an `#[allow(clippy::...)]` with a comment explaining it's defense-in-depth against future refactors:

```rust
// Defense-in-depth: if the pre-condition `len >= 3` ever changes, this guards
// against a backward-overlapping slice() panic.
if payload_end < payload_start {
    return None;
}
```

### IN-07: Test 10 in `blast_shape_test.rs` uses `matches!` without asserting the result

**File:** `crates/beava-bench/tests/blast_shape_test.rs:372`

**Issue:** Line 372 says `matches!(err, BlastShapeError::MixedRequiresMultipleEvents);` — but `matches!` returns a `bool` that's discarded. The test will pass even if the error is the wrong variant (e.g., `InvalidAlpha`), as long as `res.is_err()`. The previous line `assert!(res.is_err(), ...)` does the bulk of the check.

**Fix:** Wrap `matches!` in `assert!`:

```rust
assert!(
    matches!(err, BlastShapeError::MixedRequiresMultipleEvents),
    "expected MixedRequiresMultipleEvents, got {err:?}"
);
```

### IN-08: Mixed-shape `cardinality` hardcoded to 1M ignores `--cardinality` flag

**File:** `crates/beava-bench/src/blast_shape.rs:287`

**Issue:** Inside `build_pool` for `BlastShape::Mixed`, the keyspace is hardcoded to 1M:

```rust
BlastShape::Mixed { .. } => rng.gen_range(0..1_000_000_u64),
```

But the user-facing `--cardinality` flag (CLI line 132) is plumbed into the `Uniform` and `Zipfian` shapes, NOT `Mixed`. The plan/CONTEXT may have intended this (mixed varies the EVENT NAME, not the key) per the comment at line 285-286, but the discrepancy means `--cardinality 100 --blast-shape mixed` silently ignores the flag.

**Fix:** Either thread `cardinality` into the `Mixed` variant (and let the CLI pass it through), or document the asymmetry in `--help` text for `--cardinality`:

```rust
/// Cardinality K for uniform/zipfian shapes. Default 1_000_000.
/// IGNORED by --blast-shape=mixed (which uses a fixed 1M keyspace; mixed
/// varies the event name, not the key).
#[arg(long, default_value_t = 1_000_000)]
cardinality: u64,
```

---

_Reviewed: 2026-04-26T22:45:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
