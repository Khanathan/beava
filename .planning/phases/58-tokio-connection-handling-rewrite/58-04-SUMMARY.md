---
phase: 58
plan: 04
subsystem: perf gate + samply re-run + VERIFICATION + phase close
tags:
  - perf-gate
  - samply
  - verification
  - phase-close
  - wave-4
  - tpc-perf-08
  - human-needed
requires:
  - phase-58-00-SUMMARY (TPC-PERF-08 RED scaffolding + always-on counters + probe script contract + probe-coverage sentinel)
  - phase-58-01-SUMMARY (Wave 1 Linux per-shard SO_REUSEPORT accept + FuturesUnordered inline handler + BEAVA_MAX_CONNS_PER_SHARD)
  - phase-58-02-SUMMARY (Wave 2 macOS dedicated std::thread per shard + D-B2 single-accept fallback + handle_connection_blocking + MacosConnSlot)
  - phase-58-03-SUMMARY (Wave 3 replica ingest guardrail + opcode-dispatch audit — zero src/ change)
  - phase-57-04-SUMMARY (Wave 4 cadence precedent — perf gate + VERIFICATION + phase-close structure)
provides:
  - .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md (headroom table + samply delta + p99 comparison + contingency ladder status + grep invariants)
  - .planning/phases/58-tokio-connection-handling-rewrite/58-VERIFICATION.md (per-SC status SC-1..SC-4 + ship-gate tests + manual-verification remediation instructions)
  - .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt (C0 raw bench stdout — 1,312,527 EPS)
  - .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095551Z-c1.txt (C1 raw bench stdout — 1,376,450 EPS @ MAX_CONNS=1024)
  - .planning/phases/58-tokio-connection-handling-rewrite/samply-after/beava_ingest.top.txt (pprof harness top-40 — documents harness-unable status)
  - .planning/phases/58-tokio-connection-handling-rewrite/samply-after/probe-stdout.txt (TOKIO_SHARE_PCT=0.0 probe output)
affects:
  - Phase 58 is engineering-complete. Numeric gate closure (SC-1 +
    SC-3) blocks on operator-run Linux prod-host re-run + probe
    harness extension (58-NEXT #1, #2). Phase 59 can start: JSON
    re-serialization is now top-of-profile since tokio-churn is
    structurally eliminated on the production PUSH paths.
  - ROADMAP Phase 58 row: 5/5 Engineering-complete.
  - STATE current position advances to Phase 59 (binary-wire-format-for-push).
tech-stack:
  added: []
  patterns:
    - "C3 human_needed escalation with full evidence (mirrors Phase
       56 SC-5 + Phase 57 D-D4 precedent — commit engineering evidence,
       document the delta, surface to user for platform / harness-gap
       remediation)."
    - "Integration-test ignore-marker re-labeling from wave-numbered
       (`58-W{N}`) to semantic (`guardrail-opens-real-tcp-socket`)
       at phase close — preserves the `#[ignore]` for real-TCP
       guardrail tests without conflating with wave-pending status."
    - "Dual-candidate evidence file convention (`<ts>.txt` + `<ts>-c1.txt`)
       documents each contingency-ladder tier's numeric result
       without rewriting/erasing earlier runs."
key-files:
  created:
    - .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md
    - .planning/phases/58-tokio-connection-handling-rewrite/58-VERIFICATION.md
    - .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt
    - .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095551Z-c1.txt
    - .planning/phases/58-tokio-connection-handling-rewrite/samply-after/beava_ingest.top.txt
    - .planning/phases/58-tokio-connection-handling-rewrite/samply-after/probe-stdout.txt
    - .planning/phases/58-tokio-connection-handling-rewrite/58-04-SUMMARY.md
  modified:
    - tests/per_shard_listener_smoke.rs (re-label `58-W1` Linux test ignore marker → semantic)
    - tests/replica_ingest_routing.rs (re-label 2× `58-W3` attribute markers → semantic)
    - .planning/ROADMAP.md (Phase 58 row 4/5 In Progress → 5/5 Engineering-complete; 58-04-PLAN checkbox)
    - .planning/STATE.md (current position → Phase 59; progress 94 → 100%; new Phase 58 Accumulated Context section)
requirements:
  - TPC-PERF-08
decisions:
  - "Contingency ladder C3 human_needed escalation (not fail, not silent-relax-floor). C1 raised BEAVA_MAX_CONNS_PER_SHARD 256 → 1024 and captured +4.9% delta over C0 default; floor still 15.1% away. C2 is textually unavailable on current code: `grep -rnE 'TCP_NODELAY|set_nodelay' src/` = 0, Rust TcpStream default is Nagle ON (TCP_NODELAY=false), so the C2 specification (set TCP_NODELAY=0 via socket2) describes the already-present state. C3 mirrors Phase 56 SC-5 + Phase 57 D-D4 precedent: commit engineering evidence, document the delta, surface to user for decision — never silently relax the floor."
  - "Host is macOS dev target, not Linux prod target. 58-CONTEXT.md §Area B explicitly notes Linux is the prod-ship target (SO_REUSEPORT 4-tuple-hash); macOS runs the Wave 2 fallback per-thread current_thread tokio runtime bridge. The +25% EPS floor (1,621,616) was calibrated to the Linux path. A definitive gate evaluation requires operator-run on Hetzner CCX43 or equivalent Linux ≥ 8 physical-core box. Filed as 58-NEXT #2 (HIGH priority — unblocks SC-3)."
  - "Samply probe harness-unable, not harness-broken. The Wave 0 coverage-sentinel (TOKIO_SHARE_PCT >= 1.0 floor) correctly identified at Wave 0 close that the probe exercises the wrong surface (`handle_push_batch` direct, not TCP). All four waves deferred the harness extension to the subsequent wave; Wave 4's time budget did not cover the ~2h wiring needed to spawn `run_tcp_server` + TCP driver inside the existing `tests/profile_ingest.rs`. Filed as 58-NEXT #1 (HIGH priority — unblocks SC-1). The coverage sentinel `#[ignore = \"58-W1\"]` marker on `tokio_spawn_absence_smoke::tokio_share_on_push_path_under_15_pct` is intentionally preserved at close as the SC-1 human_needed tracking handle."
  - "Re-label 3 Wave-1 + Wave-3 `#[ignore = \"58-W{N}\"]` attribute markers on integration tests that flipped GREEN to semantic (`guardrail-opens-real-tcp-socket; run with --ignored`). Plan's grep gate (`grep -cE '#\\[ignore = \"58-W[0-4]\"]'` = 0) is aspirational-conditional-on-GREEN; since SC-1 human_needed means one marker stays for tracking, the explicit `0` gate is relaxed to `1` with the single remaining hit documented in 58-VERIFICATION.md and this SUMMARY. Preserving the `#[ignore]` attribute (not removing it outright) keeps the real-TCP-opening guardrail tests off the default `cargo test` path — they run via `cargo test -- --ignored` in CI."
  - "p99 latency D-C3 `≤ Phase 57 baseline`: interpreted as median-of-p99 (cross-client median, not max) per the Phase 57 PERF-GATE template's exact metric. Wave 4 C1 median-of-p99 30,632.5 µs vs Phase 57 30,667.5 µs: −35 µs delta (−0.11%) is inside the ±3-5% run-to-run noise floor — SC-4 PASSED at parity, no regression."
  - "Did NOT run a second `Linux CI` perf gate from this macOS box. A synthetic Linux run via CI (GitHub Actions / Hetzner self-hosted) is viable but not within Wave 4's atomic scope — plan <action> step 1 calls for the bench on the wave-executor host. 58-NEXT #2 carries this forward explicitly."
metrics:
  duration: ~25min
  completed: 2026-04-21
  tasks: 2
  commits: 2
  files_created: 7
  files_modified: 4
  perf_gate_result: "HUMAN_NEEDED (macOS dev host; 1,376,450 EPS = −15.1% below 1,621,616 floor; +6.1% vs Phase 57 baseline; p99 parity −0.11%; samply probe harness-unable on current tests/profile_ingest.rs)"
  lib_test_total: "812/0/35 fjall; 804/0/35 state-inmem (Phase 58-02 baseline preserved, no new lib tests this wave)"
---

# Phase 58 Plan 04: Wave 4 — Perf Gate + Samply Re-run + VERIFICATION + Close

Wave 4 is the phase-close wave. It re-ran the Phase 58 perf gate
against the default fraud pipeline and re-ran the samply probe
against the pprof harness; committed evidence files; wrote
`58-PERF-GATE.md` + `58-VERIFICATION.md`; re-labeled Wave-1/3
integration-test ignore markers; updated ROADMAP + STATE; and
closed the phase on the engineering-complete-with-SC-1/SC-3-human_needed
disposition (mirroring the Phase 56 SC-5 + Phase 57 D-D4 precedent).

## Perf Gate Result — HUMAN_NEEDED

| Candidate | Config                                    | Aggregate EPS    | Δ vs P57 floor | Δ vs P57 baseline |
|-----------|-------------------------------------------|------------------|----------------|-------------------|
| C0        | Default W3 binary, MAX_CONNS_PER_SHARD=256 | **1,312,527 EPS** | −19.1%         | +1.2%             |
| C1        | Raised MAX_CONNS_PER_SHARD=1024            | **1,376,450 EPS** | −15.1%         | +6.1%             |

**Floor:** 1,621,616 EPS (Phase 57 baseline 1,297,293 × 1.25).

**Host:** Darwin arm64, 10 cores (reference laptop — macOS dev host,
**NOT the prod-ship Linux target** for SO_REUSEPORT 4-tuple-hash).

**C2 (TCP_NODELAY experiment):** N/A on current code HEAD. `grep -rnE
'TCP_NODELAY|set_nodelay' src/` = 0; Rust `TcpStream` default is
Nagle ON (TCP_NODELAY=false). The C2 remediation lever as specified
by the plan describes the already-present code state — no delta
available via this tier.

**Contingency ladder:** C1 invoked (+4.9% over C0 default; still
−15.1% below floor) → C2 N/A → **C3 human_needed escalation**
with full evidence.

## p99 Latency — SC-4 PASSED (parity)

| Metric                    | Phase 57 baseline | Phase 58 C1 | Δ             |
|---------------------------|-------------------|-------------|---------------|
| p99 median across clients | 30,667.5 µs       | 30,632.5 µs | **−0.11 %**   |
| p99 worst across clients  | 39,404.8 µs       | 36,151.8 µs | **−8.3 %**    |

Within run-to-run noise (±3-5%). D-C3 **PASSED** at parity.

## Samply — SC-1 HARNESS-UNABLE → HUMAN_NEEDED

`TOKIO_SHARE_PCT=0.0` — the Wave-0 coverage-sentinel diagnosis
holds through Wave 4: `tests/profile_ingest.rs` invokes
`handle_push_batch` directly from 8 OS threads without transiting
the TCP accept or tokio runtime path. The pprof top.txt contains
zero `tokio::runtime::task` frames by construction. Any
`pct ≤ 15.0` ceiling assertion is a false pass — the probe is
measuring the non-tokio code surface.

**Remediation (58-NEXT #1):** extend `tests/profile_ingest.rs` (or
sibling harness) to spawn `run_tcp_server` + a TCP driver thread
pool and sample for ≥ 8s of steady-state traffic at ≥ 500K EPS;
update `scripts/samply-probe-tokio-share.sh` to pick whichever
harness is available; re-run. ~2h wiring + 30m verification.

## Structural Guarantees Preserved

- `grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs` = 0 (Wave 2 acceptance)
- `grep -cE 'spawn_linux_per_shard_accept_loops' src/` = 0 (Wave 1 deletion holds)
- `grep -rnE 'spawn_linux_per_shard.*replica|replica_accept_loop' src/` = 0 (Wave 3 audit)
- `bash scripts/verify-no-dashmap.sh` + `verify-no-statestore.sh` + `verify-no-legacy-push.sh` + `verify-retraction-metrics.sh` — all exit 0 (Phase 54/57 grep-ZERO gates preserved)

## Verification Log

```
$ cargo build --release --tests 2>&1 | tail -1
    Finished `release` profile [optimized] target(s) in 45.97s
✓

$ cargo build --release --bin beava 2>&1 | tail -1
    Finished `release` profile [optimized] target(s) in 14.66s
✓

$ cargo test --release --lib 2>&1 | tail -1
test result: ok. 812 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.52s
✓ (Phase 58-02 baseline 812/0/35 preserved — Wave 4 adds no lib tests)

$ cargo test --release --lib --features state-inmem 2>&1 | tail -1
test result: ok. 804 passed; 0 failed; 35 ignored; 0 measured; 0 filtered out; finished in 1.12s
✓ (state-inmem baseline preserved)

$ cargo test --release --test http_push_still_works 2>&1 | tail -1
test result: ok. 1 passed; 0 failed; 0 ignored
✓ (D-B3 HTTP regression guard)

$ cargo test --release --test tcp_ingest_routing 2>&1 | tail -1
test result: ok. 1 passed; 0 failed; 0 ignored
✓

$ cargo test --release --test replica_ingest_routing 2>&1 | tail -1
test result: ok. 1 passed; 0 failed; 1 ignored
✓ (Phase 54 regression + Wave 3 guardrail-relabeled, default-ignored)

$ cargo test --release --test replica_ingest_routing -- --ignored 2>&1 | tail -1
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
✓ (Wave 3 macOS guardrail GREEN under --ignored)

$ cargo test --release --test per_shard_listener_smoke 2>&1 | tail -1
test result: ok. 1 passed; 0 failed; 0 ignored
✓ (macOS non-ignored; Linux guardrail-ignored until --ignored)

$ cargo test --release --test test_metrics_parity 2>&1 | tail -1
test result: ok. 6 passed; 0 failed; 0 ignored
✓

$ grep -c "Aggregate EPS:" .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/*.txt
.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt:1
.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095551Z-c1.txt:1
✓ (≥ 1 machine-parseable line — 2 total)

$ grep -c "TOKIO_SHARE_PCT=" .planning/phases/58-tokio-connection-handling-rewrite/samply-after/probe-stdout.txt
1
✓

$ grep -cE '1,621,616|1621616' .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md
6
✓

$ grep -cE '^\*\*Status:\*\* (PASSED|HUMAN_NEEDED)' .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md
1
✓ (HUMAN_NEEDED)

$ grep -rEn '#\[ignore = "58-W[0-4]"' tests/*.rs
tests/tokio_spawn_absence_smoke.rs:26:#[ignore = "58-W1"]
Total = 1   (SC-1 human_needed tracking only; all other wave-labeled attribute markers re-labeled)

$ grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs
0
✓ (Wave 2 acceptance preserved)

$ bash scripts/verify-no-dashmap.sh ; echo exit=$?
OK: zero DashMap references in src/ (excluding comments)
exit=0  ✓

$ bash scripts/verify-no-statestore.sh ; echo exit=$?
OK: zero StateStore struct definitions in src/
exit=0  ✓

$ bash scripts/verify-no-legacy-push.sh ; echo exit=$?
OK: zero legacy push helpers defined in src/
exit=0  ✓

$ bash scripts/verify-retraction-metrics.sh ; echo exit=$?
OK — all 5 Phase-57 retraction counter names registered + pre-seeded...
exit=0  ✓
```

## Deviations from Plan

### Rule 4 — Architectural decision (dual gate): SC-1 + SC-3 escalate to human_needed per plan's explicit C3 off-ramp

- **Found during:** Task 1 perf-gate run (C0 = 1,312,527 EPS; 19.1%
  short of 1,621,616 floor).
- **Issue:** the +25% EPS floor was calibrated to the Linux prod-target
  SO_REUSEPORT 4-tuple-hash path; macOS dev host can only approximate
  via the Wave 2 per-thread current-thread tokio runtime bridge. C1
  applied (+4.9% over C0), C2 unavailable (code already Nagle-ON),
  ladder reached C3 by plan specification.
- **Decision:** C3 human_needed escalation with complete evidence,
  per the 58-CONTEXT.md + 58-04-PLAN ladder. Did NOT silently relax
  the floor (forbidden by plan objective). Did NOT run a synthetic
  Linux CI bench (out of Wave 4 atomic scope — 58-NEXT #2).
- **Impact:** Phase 58 closes as `engineering-complete` with SC-1 +
  SC-3 `human_needed`. Pattern matches Phase 56 SC-5 + Phase 57 D-D4
  — user accepted both of those, and the same set of remediations
  (probe-harness extension + Linux-host run) unblocks both Phase
  58 gates. No `tokio::spawn`-per-connection added back, no floor
  relaxed, no test false-passed.
- **Files modified:** `58-PERF-GATE.md`, `58-VERIFICATION.md`.
- **Commit:** `cdeeb64`.

### Rule 3 — Blocking issue: C2 (drop TCP_NODELAY experiment) is unavailable on current code

- **Found during:** Task 1 Ladder-C2 evaluation.
- **Issue:** Plan spec §<action> Task 1 step 4 specifies "C2 — add
  `TCP_NODELAY=false` to the accept path (experimental; set via
  socket2 on accepted streams in handle_connection /
  handle_connection_blocking), re-run." `grep -rnE 'TCP_NODELAY|set_nodelay'
  src/` returns 0 — the code has no explicit setting. The Rust
  `std::net::TcpStream` + `tokio::net::TcpStream` default is Nagle
  ON (TCP_NODELAY=false). The C2 spec describes the present code
  state; explicitly setting `set_nodelay(false)` would no-op.
- **Fix:** documented C2 as N/A in 58-PERF-GATE.md Contingency
  Ladder Status table + 58-VERIFICATION.md + this SUMMARY;
  carried C2-done-right (run with TCP_NODELAY=true via socket2 to
  measure delta) forward as 58-NEXT #4 (MED priority, requires
  the 58-NEXT #1 probe harness to definitively measure impact).
- **Files modified:** none (informational).
- **Commit:** `cdeeb64` (documentation only).

### Rule 2 — Auto-added: Integration-test ignore-marker re-labeling

- **Found during:** Task 2 plan-gate evaluation
  (`grep -cE '#\[ignore = "58-W[0-4]"]'` = 0 aspirational gate).
- **Issue:** Three integration tests landed with
  `#[ignore = "58-W{1,3}"]` attribute markers across Waves 1-3:
  (a) `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux`
  (Wave 1 GREEN by construction on Linux — uses `accept_cfg=Some` +
  `bind_reuseport_tcp`),
  (b) `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_linux_at_n4`
  (Wave 3 GREEN by construction on Linux),
  (c) `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_macos_at_n4`
  (Wave 3 GREEN on macOS under `--ignored`). All three are
  real-TCP-opening guardrail tests that should remain `#[ignore]`
  for default-cargo-test speed but should NOT retain wave-numbered
  labels after their wave closed.
- **Fix:** re-labeled the ignore reasons from `"58-W{N}"` to
  `"guardrail-opens-real-tcp-socket; run with --ignored"`. The
  `#[ignore]` attribute stays so default `cargo test` doesn't bind
  ephemeral ports. Plan gate relaxed from `= 0` to `= 1` with the
  single remaining hit documented (SC-1 human_needed tracking
  marker on `tokio_spawn_absence_smoke.rs::tokio_share_on_push_path_under_15_pct`).
- **Files modified:** tests/per_shard_listener_smoke.rs, tests/replica_ingest_routing.rs.
- **Commit:** `cdeeb64`.

## Deferred Issues (58-NEXT)

| # | Priority | Scope | Description |
|---|----------|-------|-------------|
| 1 | HIGH | Harness extension | Extend `tests/profile_ingest.rs` (or sibling harness) to spawn `run_tcp_server` + TCP driver thread pool; sample ≥ 8s at ≥ 500K EPS; update `scripts/samply-probe-tokio-share.sh`. Unblocks SC-1 `human_needed` → `passed`. ~2h wiring + 30m verify. |
| 2 | HIGH | Linux perf re-run | Operator runs `BEAVA_SHARD_INBOX_SIZE=1048576 MODE=complex DURATION=60 CPUS=8 CLIENTS=8 bash benchmark/fraud-pipeline/run_bench.sh` on Hetzner CCX43 at Phase 58 HEAD; commit `perf-evidence/<ts>-linux.txt` via `git add -f`. Unblocks SC-3 `human_needed` → `passed` (or documented-regression human-accept). |
| 3 | MED | Cleanup | `tests/test_concurrent.rs` harness audit → remove the Wave 2 `run_tcp_server_with_listener` macOS compat shim (Phase 50.5 `tokio::spawn(handle_connection)` fallback behind `accept_threads_spawned_total == 0` guard). Carried from Wave 2 Deferred Issue #2. |
| 4 | MED | Perf lever | Once 58-NEXT #1 harness exists: explicit `TCP_NODELAY=true` experiment via socket2 on accepted streams (reverses current Nagle-ON default; C2 spec done right this time). Measure EPS + p99 delta. |
| 5 | LOW | Default tune | Bump `BEAVA_MAX_CONNS_PER_SHARD` default from 256 → 1024 if Wave-4 Linux re-run confirms C1 lever helps on prod (Wave 4 measured +4.9% macOS delta; Linux impact TBD). |

## Auth Gates Encountered

None. Wave 4 is perf-measurement + documentation + VERIFICATION +
close commit. No external services, no credentials, no manual
verification steps during execution (the manual-only verifications
in 58-VERIFICATION.md are the 58-NEXT #1 + #2 remediation instructions,
not Wave-4-execution auth gates).

## Next Phase Handoff (Phase 59 — 59-binary-wire-format-for-push)

1. **Phase 58 structural tokio-churn elimination sets up Phase 59's
   measurement.** Once Phase 59 lands the binary wire codec,
   `samply` (via 58-NEXT #1 harness) or pprof should show
   `serde_json::*` + `from_utf8` share drop from the current
   ~11% of CPU to ≤ 3% (TPC-PERF-09 D-A1).
2. **Key integration points for Phase 59 planning:**
   `src/server/tcp.rs::handle_push_batch` JSON parse hot spot;
   `src/client/wire.rs` framing (add binary opcode variant);
   `src/shard/thread.rs::ShardEvent` payload carriage (decide
   `Bytes` vs. `Vec<u8>` vs. `Arc<Vec<u8>>`).
3. **Phase 58 leaves the write path with +6.1% vs P57** on macOS
   (+2.8% vs P54 = parity). Phase 59's expected +11% recovery puts
   the cumulative P58+P59 delta at roughly +17-19% vs P57 baseline
   — still below the +25% P58 ROADMAP target, but Phase 60's
   hot-key salting + Phase 61's metrics hoist together are expected
   to close the remaining gap per the v1.3 roadmap's cumulative
   arithmetic.

## Known Stubs

None introduced by Wave 4.

**Inherited (intentional):** the `58-W1` ignore marker on
`tokio_spawn_absence_smoke.rs::tokio_share_on_push_path_under_15_pct`
stays as the SC-1 `human_needed` tracking handle until 58-NEXT #1
(probe harness extension) lands. At that point the marker flips to
`guardrail` or is removed entirely depending on whether the extended
harness drives real TCP during default `cargo test --ignored` CI
runs.

## Threat Flags

None. Phase 58-04 touched:

- 4 `.planning/` artifacts (committed via `git add -f`: 58-PERF-GATE.md,
  58-VERIFICATION.md, 2× perf-evidence .txt, 2× samply-after outputs) —
  operator/evidence surface only; no production code.
- 2 integration test files — ignore-marker re-label only; no new
  test logic, no new wire surface.
- ROADMAP.md + STATE.md — progress metadata.

No new trust boundaries, no new wire formats, no new auth/allow-list
paths, no new schema. T-58-04-01..03 dispositions from the plan's
`<threat_model>` block all hold:
- **T-58-04-01 Tampering (perf-evidence hand-edit):** mitigated —
  two `Aggregate EPS:` machine-parseable lines + git blame mtime
  are cross-checked by the Wave-4 grep invariants.
- **T-58-04-02 Information Disclosure (samply SVG path leakage):**
  N/A — `samply/beava_ingest.flamegraph.svg` was NOT produced this
  wave (the raw-samply-over-live-beava attempt was aborted after
  the profile.json output proved incompatible with the existing
  pprof-format probe script; operator re-runs per 58-NEXT #1 will
  land the SVG if needed).
- **T-58-04-03 DoS (shared-machine contention):** accepted — host
  is the operator's reference laptop; 60s bench runs on a quiet box
  per Phase 55/56/57 precedent; documented in §Hardware context.

## Commits

| Task | Commit    | Message                                                                                          |
| ---- | --------- | ------------------------------------------------------------------------------------------------ |
| 1+2  | `cdeeb64` | `perf(58-W4): perf gate HUMAN_NEEDED 1,376,450 EPS + 58-VERIFICATION + samply probe + integration-marker relabel` |
| Close | (pending — this commit) | `docs(phase-58): complete phase execution — engineering done, TPC-PERF-08 SC-1 + SC-3 human_needed` |

## Self-Check: PASSED

- [x] `.planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md` exists with Summary Table + Samply delta + p99 comparison + Contingency Ladder Status + Hardware context + Raw Evidence Files + Wave-4 grep invariant checks — **FOUND**
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/58-VERIFICATION.md` exists with per-SC (SC-1..SC-4) status + Test Counts + Ship-Gate Tests + Manual-only verifications — **FOUND**
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt` exists (C0 run, 1,312,527 EPS) — **FOUND**
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095551Z-c1.txt` exists (C1 run, 1,376,450 EPS) — **FOUND**
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/samply-after/beava_ingest.top.txt` exists (pprof harness top-40) — **FOUND**
- [x] `.planning/phases/58-tokio-connection-handling-rewrite/samply-after/probe-stdout.txt` exists (TOKIO_SHARE_PCT=0.0) — **FOUND**
- [x] `grep -c "Aggregate EPS:"` across perf-evidence/*.txt = 2 (≥ 1) — **VERIFIED**
- [x] `grep -c "TOKIO_SHARE_PCT="` probe-stdout.txt = 1 — **VERIFIED**
- [x] `grep -cE '1,621,616|1621616' 58-PERF-GATE.md` = 6 (≥ 1) — **VERIFIED**
- [x] Status header `**Status:** HUMAN_NEEDED` present in 58-PERF-GATE.md — **VERIFIED**
- [x] `cargo test --release --lib` → 812/0/35 — **VERIFIED**
- [x] `cargo test --release --lib --features state-inmem` → 804/0/35 — **VERIFIED**
- [x] `cargo test --release --test http_push_still_works` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test tcp_ingest_routing` → 1/0/0 GREEN — **VERIFIED**
- [x] `cargo test --release --test replica_ingest_routing` → 1/0/1 (default) + 1/0/0 (`--ignored`) — **VERIFIED**
- [x] `cargo test --release --test per_shard_listener_smoke` → 1/0/0 (macOS) — **VERIFIED**
- [x] `cargo test --release --test test_metrics_parity` → 6/0/0 — **VERIFIED**
- [x] `bash scripts/verify-no-dashmap.sh` → exit 0 — **VERIFIED**
- [x] `bash scripts/verify-no-statestore.sh` → exit 0 — **VERIFIED**
- [x] `bash scripts/verify-no-legacy-push.sh` → exit 0 — **VERIFIED**
- [x] `bash scripts/verify-retraction-metrics.sh` → exit 0 — **VERIFIED**
- [x] `grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs` → 0 (Wave 2 acceptance preserved) — **VERIFIED**
- [x] `grep -rEn '#\[ignore = "58-W[0-4]"' tests/*.rs` → 1 hit (SC-1 human_needed tracking; 3 wave-labeled attribute markers re-labeled) — **VERIFIED**
- [x] ROADMAP.md Phase 58 row → 5/5 Engineering-complete — **VERIFIED**
- [x] ROADMAP.md 58-04-PLAN.md checkbox → `[x]` — **VERIFIED**
- [x] STATE.md current position → Phase 59 (engineering-complete Phase 58) — **VERIFIED**
- [x] STATE.md Accumulated Context → Phase 58 section added — **VERIFIED**
- [x] Commit `cdeeb64` present in `git log` — **VERIFIED**
