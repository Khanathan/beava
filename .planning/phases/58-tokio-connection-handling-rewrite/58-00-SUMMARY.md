---
phase: 58
plan: 00
subsystem: tests / contract-first RED scaffolding
tags:
  - tdd-red
  - wave-0
  - per-shard-accept
  - so-reuseport
  - samply
  - requirements
requires:
  - phase-57-retraction-across-crossshard-joins (baseline 1,297,293 EPS preserved)
  - tests/test_so_reuseport_boot.rs (/proc/net/tcp parse pattern reused)
  - tests/profile_ingest.rs (samply probe harness invoked by script)
  - scripts/verify-retraction-metrics.sh (bash gate-script pattern)
provides:
  - tests/tokio_spawn_absence_smoke.rs (TPC-PERF-08 D-C4 RED, 58-W1)
  - tests/per_shard_listener_smoke.rs (TPC-PERF-08 D-A1 + D-B1 RED, 58-W1 / 58-W2)
  - tests/http_push_still_works.rs (TPC-PERF-08 D-B3 regression guard, always-on)
  - scripts/samply-probe-tokio-share.sh (one-command TOKIO_SHARE_PCT= probe)
  - .planning/REQUIREMENTS.md TPC-PERF-08 row + Phase 58 traceability (coverage 36→37)
  - src/server/tcp.rs ConcurrentAppState fields
    (accept_threads_spawned_total, inline_handler_events_total)
affects:
  - Wave 1 (58-01) wires Linux per-shard SO_REUSEPORT + current_thread runtime
    + FuturesUnordered accept loop → flips n_shards_produces_n_listeners_linux
    GREEN, starts bumping inline_handler_events_total. Coverage sentinel on
    tokio_share_on_push_path_under_15_pct activates once probe driver learns
    to generate tokio::runtime::task frames.
  - Wave 2 (58-02) spawns macOS dedicated-accept threads → flips
    n_shards_produces_n_accept_threads_macos GREEN.
  - Wave 4 (58-04) re-runs scripts/samply-probe-tokio-share.sh over a
    real-TCP probe harness → flips tokio_share_on_push_path_under_15_pct
    GREEN (≤ 15 % ceiling).
  - http_push_still_works.rs runs on EVERY wave — regression alarm for
    accidental axum/http.rs touch (D-B3).
tech-stack:
  added: []
  patterns:
    - "#[ignore = \"58-W{N}\"] Wave-targeted RED markers (mirrors Phase 54/55/56/57)"
    - "bash-script gate with machine-parseable final line (TOKIO_SHARE_PCT=<num>)"
    - "/proc/net/tcp LISTEN-state parser (from Phase 50.5-02 test_so_reuseport_boot)"
    - "ConcurrentAppState always-on AtomicU64 probe field (from Phase 50.5-02 conn_interns_total)"
    - "Probe-coverage sentinel: assert pct >= FLOOR to force a RED until the probe actually covers the target path"
key-files:
  created:
    - tests/tokio_spawn_absence_smoke.rs
    - tests/per_shard_listener_smoke.rs
    - tests/http_push_still_works.rs
    - scripts/samply-probe-tokio-share.sh
    - .planning/phases/58-tokio-connection-handling-rewrite/58-00-SUMMARY.md
  modified:
    - .planning/REQUIREMENTS.md (+ TPC-PERF-08 row + Phase 58 traceability; coverage 36/36 → 37/37)
    - src/server/tcp.rs (+ 2 AtomicU64 fields in ConcurrentAppState; initialized to 0)
requirements:
  - TPC-PERF-08
decisions:
  - "RED sentinel via coverage floor, not just ceiling. Naive read of the plan: assert TOKIO_SHARE_PCT <= 15; today expected ≈ 60 → RED. Reality: tests/profile_ingest.rs (the plan's cited harness) calls handle_push_batch directly from 8 OS threads — it never touches the TCP accept / tokio task runtime path, so pct ≈ 0.0 % and the ≤ 15 gate passes trivially. Added a coverage-floor sentinel (pct >= 1.0 %) that fails today and forces Wave 1 to extend the probe to drive a real TcpStream before the ceiling gate becomes load-bearing. Still encodes D-C1 (RED-first TDD): the sentinel IS the RED signal today. (Rule 1 bug — the naive gate was not load-bearing.)"
  - "`accept_threads_spawned_total` + `inline_handler_events_total` are ALWAYS-ON (not `cfg(test)`), per 50.5-02 `conn_interns_total` precedent. Integration tests compile the library without `cfg(test)`, so probe fields must be unconditional. Zero hot-path cost (never read on the push path today; Wave 1/2 bumps are write-only). Same pattern already proven across Phases 50/54/57."
  - "Linux /proc/net/tcp helper copied verbatim from tests/test_so_reuseport_boot.rs instead of factoring into tests/common/. Rationale: yak-shaving for a Wave-0 RED scaffold — the sibling file already proves the helper works; a future refactor can hoist both copies into a shared helper once a 3rd consumer appears."
  - "http_push_still_works.rs has NO #[ignore] marker — it's a GREEN regression alarm, not a flip target. TOTALS: 2 #[ignore = \"58-W[1-3]\"] attribute markers across 2 files (tokio_spawn_absence 1 × 58-W1, per_shard_listener 1 × 58-W1 + 1 × 58-W2 cfg-split). Plus 2 doc-comment 58-W{1,2} references in per_shard_listener_smoke header. `grep -cE '#\\[ignore = \"58-W[1-3]\"' tests/tokio_spawn_absence_smoke.rs tests/per_shard_listener_smoke.rs` = 1+2 = 3 ≥ 3."
  - "Probe script script allows TOKIO_SHARE_PCT=unknown exit 0 when samply CLI absent; the smoke test then panics with an actionable `cargo install samply` hint. Phase 58 D-C4 is NOT bypassable by missing tooling — no silent-skip."
  - "REQUIREMENTS.md Phase 58 section title matches the Phase 57 precedent (`### TPC-PERF (continued) — Phase 58`). Traceability row appended after Phase 57 row. Coverage footer updated 36/36 → 37/37 with explicit `+ 1 Phase-58 requirements` suffix."
metrics:
  duration: ~8min
  completed: 2026-04-21
  tasks: 2
  commits: 2
  files_created: 5
  files_modified: 2
  red_tests_landed: 2    # tokio_spawn_absence + per_shard_listener (2 cfg-split tests)
  green_tests_landed: 1  # http_push_still_works regression guard
  ignored_marker_count: 3   # attribute markers (1+2) across test files
---

# Phase 58 Plan 00: Wave 0 RED-tests & Probe-Script Contract Summary

RED-first TDD baseline for Phase 58 (TPC-PERF-08). Three integration tests +
one samply probe script + one REQUIREMENTS row + two always-on counter
fields land on disk. Tests FAIL today as designed (RED); Wave 1/2/4 flip
them GREEN one by one. HTTP PUSH regression guard passes today and must
keep passing every wave (D-B3).

## RED/GREEN → Wave Flip Map

| Gate | File | Marker | Flips GREEN at |
|------|------|--------|----------------|
| D-C4 (tokio share ≤ 15 %) | `tests/tokio_spawn_absence_smoke.rs::tokio_share_on_push_path_under_15_pct` | `#[ignore = "58-W1"]` | Wave 1 probe extended → Wave 4 perf gate |
| D-A1 (Linux N LISTEN sockets) | `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux` (`#[cfg(target_os = "linux")]`) | `#[ignore = "58-W1"]` | Wave 1 per-shard SO_REUSEPORT bind |
| D-B1 (macOS N accept threads) | `tests/per_shard_listener_smoke.rs::n_shards_produces_n_accept_threads_macos` (`#[cfg(not(target_os = "linux"))]`) | `#[ignore = "58-W2"]` | Wave 2 dedicated-accept-thread spawner |
| D-B3 (HTTP PUSH unchanged) | `tests/http_push_still_works.rs::http_push_post_events_at_n4_matches_phase57` | none — always-on | Stays GREEN every wave (regression alarm) |

## Grep-Count Evidence

```
$ grep -cE '^- \[ \] \*\*TPC-PERF-08\*\*' .planning/REQUIREMENTS.md
1  (= 1 ✓)

$ grep -cE '^\| 58 \| tokio-connection-handling-rewrite' .planning/REQUIREMENTS.md
1  (= 1 ✓)

$ grep -c '37/37' .planning/REQUIREMENTS.md
1  (= 1 ✓ — coverage incremented from 36/36)

$ grep -c '1,621,616' .planning/REQUIREMENTS.md
1  (= 1 ✓ — Phase 57 baseline × 1.25 EPS floor encoded)

$ grep -c 'BEAVA_MAX_CONNS_PER_SHARD' .planning/REQUIREMENTS.md
1  (≥ 1 ✓ — D-A4 env var encoded)

$ grep -c 'BEAVA_SHARDS_SINGLE_LISTENER' .planning/REQUIREMENTS.md
1  (≥ 1 ✓ — D-B2 fallback-fallback env var encoded)

$ grep -cE '#\[ignore = "58-W[1-3]"' tests/tokio_spawn_absence_smoke.rs tests/per_shard_listener_smoke.rs
tests/tokio_spawn_absence_smoke.rs:1
tests/per_shard_listener_smoke.rs:2
Total = 3  (≥ 3 ✓)

$ test -x scripts/samply-probe-tokio-share.sh && echo OK
OK  ✓ (mode 0755)

$ bash scripts/samply-probe-tokio-share.sh --help | head -1
samply-probe-tokio-share — Phase 58 TPC-PERF-08 probe helper.  ✓

$ grep -c 'accept_threads_spawned_total\|inline_handler_events_total' src/server/tcp.rs
6  (≥ 4 ✓ — 2 field decls + 2 initializers + 2 doc refs)
```

## Verification Log

```
$ cargo build --release --tests 2>&1 | grep -E "^error" | wc -l
0  ✓

$ cargo test --release --lib 2>&1 | grep "test result:"
test result: ok. 809 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.50s
✓ (Phase 57 baseline preserved — no regression from new AtomicU64 fields)

$ cargo test --release --test http_push_still_works 2>&1 | tail -3
running 1 test
test http_push_post_events_at_n4_matches_phase57 ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s
✓ (D-B3 regression guard GREEN today)

$ cargo test --release --test per_shard_listener_smoke -- --ignored 2>&1 | tail -5
test n_shards_produces_n_accept_threads_macos ... FAILED
... assertion `left == right` failed: TPC-PERF-08 D-B1 gate FAIL:
    expected 4 dedicated macOS accept threads ... found 0.
test result: FAILED. 0 passed; 1 failed; 0 ignored
✓ (macOS 58-W2 RED — counter=0, expected 4)

$ cargo test --release --test tokio_spawn_absence_smoke -- --ignored 2>&1 | tail -5
test tokio_share_on_push_path_under_15_pct ... FAILED
... TPC-PERF-08 D-C4 probe-coverage sentinel FAIL:
    TOKIO_SHARE_PCT=0.0% is below the 1.0% coverage floor.
test result: FAILED. 0 passed; 1 failed; 0 ignored
✓ (58-W1 RED — probe-coverage sentinel enforces harness extension before ceiling gate activates)
```

## Deviations from Plan

Two deviations, both small and deliberate (Rule 1 + Rule 2):

### Rule 1 — Bug fix: probe-coverage sentinel in `tokio_spawn_absence_smoke.rs`

- **Found during:** Task 2 verification.
- **Issue:** The plan's cited RED harness (`tests/profile_ingest.rs` via
  `scripts/samply-probe-tokio-share.sh`) calls `handle_push_batch` directly
  from 8 OS threads — it NEVER transits the TCP accept path or tokio
  runtime-task dispatch. Reality: `TOKIO_SHARE_PCT=0.0%` on a default
  `cargo test --release --test tokio_spawn_absence_smoke -- --ignored`
  run. A naive `assert!(pct <= 15.0)` would therefore pass trivially and
  would FAIL to catch a Wave-1 regression that reintroduces `tokio::spawn`
  per TCP connection — because the probe never observes the TCP path.
- **Fix:** Added a probe-coverage sentinel as the FIRST assertion: `pct >=
  1.0 %`. This fails today (pct=0.0) → RED signal per D-C1, and forces
  Wave 1 to extend the probe to drive a real `TcpStream` so
  `tokio::runtime::task::*` frames actually appear. Once the sentinel
  passes, the ≤ 15 % ceiling assertion activates. Both gates are encoded
  side-by-side with commentary explaining the contract.
- **Files modified:** `tests/tokio_spawn_absence_smoke.rs`
- **Commit:** `1c25ac0`

### Rule 2 — Probe script `unknown`-case handling clarified

- **Found during:** Task 1 spec reading.
- **Issue:** Plan body says "emit `TOKIO_SHARE_PCT=unknown` and exit 0 if
  samply CLI is absent — the smoke test (Task 2 below) handles the
  'samply not installed' case via a skip-with-warning code path." A
  "skip-with-warning" in the test would silently pass on CI/laptops
  without samply — bypassable hard gate.
- **Fix:** Kept the script's `exit 0 + TOKIO_SHARE_PCT=unknown` output, but
  the test `panic!`s on `unknown` with an actionable `cargo install samply`
  hint. The D-C4 gate is NOT bypassable by missing tooling. Plan intent
  is preserved (script returns a structured sentinel, test decides what
  to do with it) — only the test's policy is hardened from "skip" to
  "fail with hint".
- **Files modified:** `tests/tokio_spawn_absence_smoke.rs` (header doc +
  unknown branch); `scripts/samply-probe-tokio-share.sh` (comment block
  documents the contract).
- **Commit:** `1c25ac0`

Neither deviation changes wave assignments, flip counts, or the REQUIREMENTS
row. Success criteria still met as written.

## Auth Gates Encountered

None — Wave 0 is tests + docs + a bash gate script. No wire surface, no
external auth, no network credentials.

## Next Wave Handoff (Wave 1 must deliver)

Wave 1 (plan 58-01) MUST:

1. **Linux per-shard SO_REUSEPORT accept loop (D-A1 / D-A2 / D-A3 / D-A4):**
   Each shard thread opens its own `TcpListener` via `bind_reuseport_tcp`
   (reusing the Phase 50 helper) on the PUSH port. Shard thread runs a
   `tokio::runtime::Builder::new_current_thread().enable_io().build()` local
   runtime; accept loop uses `FuturesUnordered` with per-shard cap
   `BEAVA_MAX_CONNS_PER_SHARD=256` (env override). Connections stay on the
   shard thread until close — no `tokio::spawn` per connection.
   → Flips `n_shards_produces_n_listeners_linux` GREEN (N LISTEN sockets
   on the test port).
   → Starts bumping `inline_handler_events_total` on every
   `handle_push_batch` invocation.

2. **Extend the samply probe harness to drive real TCP traffic:**
   The current `tests/profile_ingest.rs` calls `handle_push_batch` directly.
   Wave 1 must either (a) extend it to spawn a real server + TCP driver
   threads, or (b) add a sibling harness that does so, and update
   `scripts/samply-probe-tokio-share.sh` accordingly. This is the
   pre-requisite that flips the D-C4 probe-coverage sentinel.
   → Activates the ≤ 15 % ceiling gate in
   `tokio_share_on_push_path_under_15_pct`.

3. **Replica ingest path (TCP opcode):** Out of scope for Wave 1? Plan 58-01
   spec — but Phase 58 scope includes replica ingest per D-B3. Verify on
   58-01 plan read whether Wave 1 or Wave 3 wires the replica path.

Wave 2 (plan 58-02) MUST:

1. **macOS per-shard dedicated-accept-thread (D-B1):** Each shard owns a
   dedicated `std::thread` running a blocking `TcpListener::accept` loop.
   Accepted connection gets `BufReader<TcpStream>` + `BufWriter<TcpStream>`;
   handles `OP_PUSH` inline via `handle_push_batch` in blocking mode.
   At startup, bump `accept_threads_spawned_total` exactly once per shard.
   → Flips `n_shards_produces_n_accept_threads_macos` GREEN (counter == N).

2. **BEAVA_SHARDS_SINGLE_LISTENER=1 fallback (D-B2):** Single-accept-thread
   + round-robin dispatcher retained as a fallback for macOS <13 or when
   the env var is set.

Wave 4 (plan 58-04) MUST:

1. Re-run `scripts/samply-probe-tokio-share.sh` over a real TCP driver
   (MODE=complex CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576).
2. Verify `TOKIO_SHARE_PCT <= 15.0`.
3. Verify aggregate EPS >= 1,621,616 (= Phase 57 baseline × 1.25).
4. Verify p99 per-event latency does NOT regress vs Phase 57's
   30,667.5 µs client-observed median-of-p99.
5. Remove all `#[ignore = "58-W[1-3]"]` markers once every gate passes.

## Known Stubs

**Intentional — this is the RED contract file.**

- `accept_threads_spawned_total` + `inline_handler_events_total` fields
  exist on `ConcurrentAppState` and are initialized to 0, but NEVER
  incremented by production code today. Wave 1/2 wire the bumpers.
  This is the Wave-0 RED probe idiom (mirrors Phase 50.5-02
  `conn_interns_total` at its Wave 0).

- The samply probe script calls `tests/profile_ingest.rs`, which exercises
  `handle_push_batch` directly (no TCP, no tokio accept). The
  `TOKIO_SHARE_PCT` it emits today measures the wrong surface; the
  Wave-0 coverage sentinel in `tokio_spawn_absence_smoke.rs` fails
  loudly on this gap and blocks the ceiling gate from accidentally
  passing. Wave 1 extends the probe.

## Threat Flags

None — plan touched only test code, bash gate script, REQUIREMENTS.md, and
added 2 always-on `AtomicU64` probe fields on a struct already full of
similar probe counters. No new trust boundaries; no new wire surface; no
new auth paths. Per plan `<threat_model>`:

- T-58-00-01 (probe counters leaking via `/metrics`): accepted —
  `accept_threads_spawned_total` + `inline_handler_events_total` are
  field reads only (never registered as Prometheus metrics). Same
  pattern as 50.5-02 `conn_interns_total`.
- T-58-00-02 (samply probe script supply-chain swap): accepted — in-repo
  bash, operator-invoked, no production path.
- T-58-00-03 (N=4 test-harness port collisions): mitigated — all new
  tests bind `127.0.0.1:0` ephemeral ports; no fixed port in use.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| Task 1 | `88d41e5` | `docs(58-W0): add TPC-PERF-08 row + samply probe script (REQUIREMENTS)` |
| Task 2 | `1c25ac0` | `test(58-W0): plant 3 RED smoke tests + 2 always-on counters (TPC-PERF-08)` |

## Self-Check: PASSED

- [x] `tests/tokio_spawn_absence_smoke.rs` exists (1 test, 58-W1) — **FOUND**
- [x] `tests/per_shard_listener_smoke.rs` exists (2 platform-split tests, 58-W1/58-W2) — **FOUND**
- [x] `tests/http_push_still_works.rs` exists (1 regression test, always-on) — **FOUND**
- [x] `scripts/samply-probe-tokio-share.sh` exists, mode 0755, `--help` works — **FOUND**
- [x] `.planning/REQUIREMENTS.md` contains TPC-PERF-08 row, coverage 37/37 — **FOUND**
- [x] `src/server/tcp.rs` `ConcurrentAppState` has `accept_threads_spawned_total` + `inline_handler_events_total` fields initialized to 0 — **FOUND**
- [x] `cargo build --release --tests` → exit 0 — **VERIFIED**
- [x] `cargo test --release --lib` → 809/0/35 (Phase 57 baseline preserved) — **VERIFIED**
- [x] `cargo test --release --test http_push_still_works` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test per_shard_listener_smoke -- --ignored` → 0/1/0 RED (macOS) — **VERIFIED**
- [x] `cargo test --release --test tokio_spawn_absence_smoke -- --ignored` → 0/1/0 RED (coverage sentinel) — **VERIFIED**
- [x] 3 × `58-W[1-3]` attribute markers across 2 RED test files — **VERIFIED**
- [x] `grep -c 'TPC-PERF-08' .planning/REQUIREMENTS.md` >= 2 — **VERIFIED**
- [x] Commits `88d41e5` + `1c25ac0` present in git log — **VERIFIED**
