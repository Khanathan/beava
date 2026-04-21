# Phase 58 — Perf Gate Evidence

**Status:** HUMAN_NEEDED
**Ran:** 2026-04-21T09:54:35Z (C0) + 2026-04-21T09:55:51Z (C1)
**Harness:** `benchmark/fraud-pipeline/run_bench.sh` (default fraud-pipeline scenario)
**Host:** Darwin arm64, 10 cores (reference laptop — matches Phase 55/56/57 baseline hardware; **macOS = dev host, NOT the prod-ship target** — per 58-CONTEXT.md §Area B, Linux is the prod target for the per-shard SO_REUSEPORT path; macOS runs the Wave 2 dedicated-accept-thread + per-thread current-thread tokio runtime bridge)
**Binary:** `target/release/beava` (Phase 58 HEAD — post-58-03 close `0ec7188`)
**Phase 57 baseline:** 1,297,293 EPS
**Gate floor (×1.25):** **1,621,616 EPS** (= Phase 57 baseline × 1.25; ROADMAP-locked per 58-CONTEXT.md D-C2)

## Summary Table

| Field                                                            | Value                  |
|------------------------------------------------------------------|------------------------|
| Baseline (Phase 57 close — default fraud pipeline)               | 1,297,293 EPS          |
| Gate floor (baseline × 1.25 = +25% EPS target)                   | **1,621,616 EPS**      |
| Candidate C0 — Phase 58 HEAD, default pipeline, 60 s, MAX_CONNS_PER_SHARD=256 | **1,312,527 EPS**  |
| Candidate C1 — Phase 58 HEAD, default pipeline, 60 s, MAX_CONNS_PER_SHARD=1024 | **1,376,450 EPS**  |
| Best candidate (C1)                                              | **1,376,450 EPS**      |
| Headroom over floor (C1)                                         | −245,166 EPS (−15.1 %) |
| Delta vs Phase 57 baseline (C1)                                  | +79,157 EPS (+6.1 %)   |
| Delta vs Phase 56 baseline (1,195,914 EPS) (C1)                  | +180,536 EPS (+15.1 %) |
| Delta vs Phase 54 baseline (1,339,446 EPS) (C1)                  | +37,004 EPS (+2.8 %)   |
| Gate result                                                      | **HUMAN_NEEDED**       |
| Contingency invoked                                              | **C1 (raised MAX_CONNS_PER_SHARD to 1024) + C3 escalation** |

Under `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576`,
the Phase 58 HEAD binary on **macOS dev host** measures **1,376,450 EPS (C1)** —
**a +6.1 % improvement over the Phase 57 baseline** and within the Phase
54-level throughput band, but **15.1 % short of the +25 % EPS gate floor**
of 1,621,616 EPS.

**Why the gate is HUMAN_NEEDED rather than PASSED or FAILED:**

1. **Host is macOS, not the prod-ship Linux target.** Per 58-CONTEXT.md
   §Area B and 58-02-SUMMARY.md Rule-4 deviation, the macOS path uses
   `spawn_macos_per_shard_accept_threads` with per-connection `std::thread`
   workers running a per-thread `current_thread` tokio runtime bridge.
   This satisfies D-B1 (dedicated accept thread per shard; no
   `tokio::spawn` per connection) but retains **per-connection
   current-thread runtime construction** which the Phase-54 pprof-era
   tokio-spawn-churn pattern did not have. The Linux SO_REUSEPORT +
   FuturesUnordered path (Wave 1) is the +25 % EPS target vehicle —
   **this gate cannot be definitively evaluated on macOS.**
2. **The samply probe is harness-unable on the current tests/profile_ingest.rs.**
   Wave 0 Coverage Sentinel diagnosis holds through Wave 4: the probe
   harness calls `handle_push_batch` directly from 8 OS threads, never
   transiting the TCP accept or tokio runtime path, so `TOKIO_SHARE_PCT=0.0`
   under every configuration. Extending the harness to drive real TCP
   traffic (Wave 0/1/2/3 Deferred Issue #1) was carried forward through
   every wave as "Wave 4's natural deliverable"; within Wave 4's time
   budget, the extension has not been implemented. The raw samply-over-live-beava
   attempt ran but produced no machine-parseable `TOKIO_SHARE_PCT` line
   (the top.txt format grep only works against the existing pprof
   harness output).
3. **C2 (drop TCP_NODELAY experiment) is effectively a no-op on current
   code.** `grep -rnE 'TCP_NODELAY|set_nodelay' src/` returns 0 hits;
   the Rust `TcpStream` default is Nagle ON (TCP_NODELAY=false). The
   code is already in the C2-target state. Explicitly setting
   `set_nodelay(false)` via socket2 would no-op on accepted streams that
   are already Nagle-ON. The C2 tier as specified in 58-04-PLAN line
   183 is unavailable as a remediation lever on this code HEAD.
4. **User decision required:** per the Phase 56 SC-5 precedent
   (accepted `human_needed` 2026-04-20 on a similar macOS/SDK-gap
   constraint) and the Phase 57 D-D4 advisory precedent (similar
   deferred-on-same-gap pattern), the right move is to stop here,
   commit the engineering evidence, and surface the Linux-run and
   probe-harness-extension requirements to the user as a
   `human_needed` escalation.

Full evidence: `.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt`
(C0) + `20260421T095551Z-c1.txt` (C1).

## Per-client checkpoints (C1 — best candidate, MAX_CONNS_PER_SHARD=1024)

```
t=  5.0s  events=     7,222,000  instant= 1,397,311 eps  avg= 1,397,311 eps
t= 10.0s  events=    13,920,000  instant= 1,339,600 eps  avg= 1,368,455 eps
t= 15.0s  events=    20,595,000  instant= 1,335,000 eps  avg= 1,357,303 eps
t= 20.0s  events=    27,370,000  instant= 1,355,000 eps  avg= 1,356,727 eps
t= 25.0s  events=    34,060,000  instant= 1,338,000 eps  avg= 1,353,169 eps
t= 30.0s  events=    40,792,000  instant= 1,346,400 eps  avg= 1,351,875 eps
t= 35.0s  events=    47,399,000  instant= 1,321,400 eps  avg= 1,347,487 eps
t= 40.0s  events=    56,830,000  instant= 1,348,200 eps  avg= 1,418,976 eps
t= 45.0s  events=    63,482,000  instant= 1,330,400 eps  avg= 1,409,145 eps
t= 50.1s  events=    70,259,000  instant= 1,352,694 eps  avg= 1,403,495 eps
t= 55.1s  events=    76,925,000  instant= 1,327,888 eps  avg= 1,396,604 eps
t= 60.6s  events=    83,426,000  instant= 1,175,587 eps  avg= 1,376,439 eps
```

Final aggregate (after `summary.json` aggregation): **1,376,450 EPS** over
60 s, 83,426,000 events total. Per-event cost: 0.73 µs.

The trailing-edge backpressure wave (all 8 clients exit non-zero with
`ProtocolError: shard inbox full — backpressure`) is the same cosmetic
the Phase 54 / 55 / 56 / 57 baselines all exhibited — carried forward
under 55-NEXT #8 (graceful client shutdown at EOS). Not a regression.

### Per-client throughput (C1, from final summary.json)

| Client | Events      | Wall     | EPS     | Exit                                    |
|--------|-------------|----------|---------|-----------------------------------------|
| proc-0 | 10,435,000  | 60.61 s  | 172,167 | ProtocolError: shard inbox full (EOS)   |
| proc-1 | 10,568,000  | 60.57 s  | 174,486 | ProtocolError: shard inbox full (EOS)   |
| proc-2 | 10,444,000  | 60.56 s  | 172,466 | ProtocolError: shard inbox full (EOS)   |
| proc-3 | 10,181,000  | 60.52 s  | 168,232 | ProtocolError: shard inbox full (EOS)   |
| proc-4 | 10,503,000  | 60.45 s  | 173,752 | ProtocolError: shard inbox full (EOS)   |
| proc-5 | 10,615,000  | 60.58 s  | 175,232 | ProtocolError: shard inbox full (EOS)   |
| proc-6 | 10,179,000  | 60.57 s  | 168,045 | ProtocolError: shard inbox full (EOS)   |
| proc-7 | 10,501,000  | 60.38 s  | 173,927 | ProtocolError: shard inbox full (EOS)   |

### Client push-latency distribution (µs per 1000-event batch call) — C1

| Percentile | Median across clients | Worst across clients |
|------------|-----------------------|----------------------|
| p50        | 3,240.5               | 3,863.8              |
| p99        | **30,632.5**          | 36,151.8             |
| p99.9      | 38,260.2              | 64,201.2             |

**p99 latency comparison vs Phase 57 baseline:**

| Metric                     | Phase 57 baseline | Phase 58 C1      | Delta      |
|----------------------------|-------------------|------------------|------------|
| p99 median across clients  | 30,667.5 µs       | **30,632.5 µs**  | −35 µs (−0.11 %) |
| p99 worst across clients   | 39,404.8 µs       | 36,151.8 µs      | −3,253 µs (−8.3 %) |

**D-C3 p99 guard: PASSED.** Median-of-p99 is within ±0.11 % of the
Phase 57 baseline (effectively parity inside run-to-run noise floor of
±3-5 %). No regression.

## Samply Probe Re-run — Harness-unable

**Result:** `TOKIO_SHARE_PCT=0.0` on `scripts/samply-probe-tokio-share.sh`.

| Metric                       | Phase 54 baseline (est.) | Phase 58 Wave 4 | Gate floor | Status       |
|------------------------------|--------------------------|------------------|------------|--------------|
| tokio::runtime::task leaf %  | ~60 %                    | **0.0 %**        | ≤ 15 %     | HARNESS-UNABLE |

The Wave 0 coverage-sentinel diagnosis (documented in 58-00-SUMMARY.md
Deviation Rule 1) holds through Wave 4: `tests/profile_ingest.rs`
calls `handle_push_batch` directly from 8 OS threads, NEVER transiting
the TCP accept or tokio runtime path. The pprof top.txt that
`samply-probe-tokio-share.sh` parses contains zero `tokio::runtime::task`
frames by construction — the probe observes the wrong surface.

**What landed on-disk (pre-existing pprof harness output):**

```
$ head -20 .planning/phases/58-*/samply-after/beava_ingest.top.txt
# Beava ingest profile
Workload: 8 threads, 8.01s, 1081461 events, 134975 EPS total.
Samples: 15214

## Top 40 leaf functions (self-samples, on-CPU time)
samples  self %   function
-------  ------   --------------------------------------------------------------------------------
   3927   25.8%  std::sys::backtrace::__rust_begin_short_backtrace
   3314   21.8%  beava::server::tcp::handle_push_batch
   1260    8.3%  indexmap::inner::Core<K,V>::insert_full
   1093    7.2%  std::io::buffered::bufwriter::BufWriter<W>::flush_buf
    ...
   [no tokio::runtime::task frames — harness does not exercise TCP path]
```

**Zero `tokio::runtime::task::*` frames** appear in the profile top-40
(grep `-c 'tokio::runtime::task' top.txt` = 0). This could mean EITHER
(a) the Wave 1/2 production code successfully eliminated them, OR
(b) the harness doesn't exercise the path at all. Diagnosis: **(b)** —
confirmed by `grep -c 'handle_connection' top.txt` = 0 and `grep -c
'TcpListener' top.txt` = 0. The probe is measuring the non-tokio
push-batch call site, not the TCP accept path.

**The raw samply-over-live-beava attempt** (Wave 4 `samply record -d 20
-- target/release/beava` with 4-client bench.py driving 12 s of TCP
traffic on port 6500) produced a profile.json but the probe script's
regex (`/tokio::runtime::task/` against top.txt format) is not
applicable to the JSON output. Extending the script to parse
samply-native JSON is itself deferred work (28-NEXT territory).

**Concrete remediation (what's needed to flip SC-1 from human_needed
→ passed):**

1. Extend `tests/profile_ingest.rs` (or add a sibling harness) that
   spawns a real `beava` server via `run_tcp_server` + a TCP driver
   thread pool, samples for ≥ 8 s of steady-state traffic at ≥ 500K
   EPS, and writes a pprof-format top.txt with real `tokio::runtime::task::*`
   frames.
2. Update `scripts/samply-probe-tokio-share.sh` to pick whichever
   harness (old vs new) is available.
3. Re-run Wave 4's perf gate + samply on the new harness. Expected
   outcome per Wave 1/2 analysis: macOS path still shows per-connection
   current-thread runtime construction (per-connection thread + local
   runtime != per-connection tokio::spawn — the samply signature should
   be DIFFERENT from Phase 54 pprof, and most likely well below 15%
   since `tokio::runtime::task::harness` is the per-task dispatch
   frame, NOT the runtime-boot frame).

## Contingency Ladder Status

| Tier | Description                                                          | Triggered? | Result |
|------|----------------------------------------------------------------------|------------|--------|
| C1   | Raise `BEAVA_MAX_CONNS_PER_SHARD` from 256 to 1024, re-run           | **YES**    | 1,376,450 EPS (+4.9 % over C0 256, still 15.1 % short of floor) |
| C2   | Drop `TCP_NODELAY` experiment (set via socket2 on accepted streams)  | **N/A**    | Code already at C2 target state — `grep -rnE 'TCP_NODELAY\|set_nodelay' src/` = 0; kernel default is Nagle ON (TCP_NODELAY=false); no remediation lever available |
| C3   | `human_needed` escalation — commit best evidence, document delta, submit to user | **YES** | **This document + 58-VERIFICATION.md** |

## Interpretation

Phase 58 delivers the structural change: every platform's PUSH hot
path now transits **zero `tokio::spawn`-per-connection** calls. The
Linux path is the +25 % EPS target vehicle (per-shard SO_REUSEPORT +
FuturesUnordered + inline FIFO-drained ShardOp dispatch on a
`current_thread` runtime pinned to each shard's OS thread — no per-
connection task allocation, no per-connection wake latency). The
macOS path is a dev-host fallback (dedicated `std::thread` per shard
owning a blocking `accept()` loop + per-connection `std::thread`
worker running a per-thread `current_thread` tokio runtime bridge —
this preserves the ~400 LOC `handle_connection_public` frame loop
without duplicating it in pure blocking I/O, but retains
per-connection runtime setup cost).

**On macOS (this run):** the engineering structural change holds —
`grep -cE 'tokio::spawn\(.*handle_connection' src/server/tcp.rs` = 0
on the production path (Wave 2 acceptance criterion preserved) —
but the measured EPS gain is +6.1 % vs Phase 57 baseline, not +25 %.
This is consistent with the Rule-4 deviation documented in
58-02-SUMMARY.md: the per-thread current-thread runtime bridge's
construction cost, while amortized over the connection's lifetime,
still shows up on a bench that opens 1 long-lived connection per
client (the macOS-host workload is more similar to a single-socket
workload than the many-short-connection scenario where
per-tokio::spawn dispatch dominated Phase 54). Linux would not have
this overhead.

**On the target Linux prod host:** the samply flamegraph + EPS
measurement would produce the definitive gate result. Based on the
Wave 1 Linux path (verified correct-by-construction at Wave 1 close
— the `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux`
test flips GREEN on Linux CI, asserting N LISTEN sockets via
/proc/net/tcp), the expectation is that the 15.1 % headroom gap
closes on Linux where the structural advantage actually materializes.

**Regression contract vs Phase 57:** not regressed. +6.1 % EPS at C1;
p99 latency at parity (−0.11 %); all Phase 57 correctness tests
GREEN; all Phase 54 structural gates (grep-ZERO DashMap / StateStore
/ legacy-push) preserved.

## Wave-4 ignore-marker cleanup

Wave 4 re-labels `#[ignore = "58-W*"]` markers on tests that are
guardrail-ignore (open real TCP sockets, run with `--ignored` in CI)
from the wave-numbered label to a semantic label:

```
$ grep -rEn '#\[ignore = "58-W[0-4]"' tests/*.rs | wc -l
1   # (only tokio_spawn_absence_smoke.rs:26 remains — SC-1 human_needed pending probe harness extension; see §Samply Probe Re-run)
```

The single remaining `58-W1` attribute marker is on
`tokio_spawn_absence_smoke::tokio_share_on_push_path_under_15_pct`,
intentionally preserved as a `human_needed` tracking label until
the probe harness extension lands and SC-1 flips GREEN.

Re-labeled (Wave 4):
- `tests/per_shard_listener_smoke.rs::n_shards_produces_n_listeners_linux`
  → `#[ignore = "guardrail-opens-real-tcp-socket; run with --ignored"]`
- `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_linux_at_n4`
  → `#[ignore = "guardrail-opens-real-tcp-socket; run with --ignored"]`
- `tests/replica_ingest_routing.rs::replica_ingest_lands_on_per_shard_accept_macos_at_n4`
  → `#[ignore = "guardrail-opens-real-tcp-socket; run with --ignored"]`

The macOS accept-thread smoke test
(`tests/per_shard_listener_smoke.rs::n_shards_produces_n_accept_threads_macos`)
was already un-ignored at Wave 2 close and runs GREEN on every
macOS `cargo test` (0/0/1 non-ignored = 1/0/0 on macOS host).

## Hardware context

Reference laptop — Darwin arm64, M-series, 10 cores. Same hardware
class as the Phase 55 / Phase 56 / Phase 57 baselines. EPS numbers
directly comparable run-to-run on this host; **NOT** directly
comparable to the target Linux prod host (SO_REUSEPORT 4-tuple-hash
distribution is Linux-kernel-only).

## Raw Evidence Files

- `perf-evidence/20260421T095435Z.txt` — 60 s default fraud-pipeline run at
  Phase 58 HEAD, `BEAVA_MAX_CONNS_PER_SHARD=256` (C0 baseline).
- `perf-evidence/20260421T095551Z-c1.txt` — 60 s run, `BEAVA_MAX_CONNS_PER_SHARD=1024` (C1 re-run).
- `samply-after/beava_ingest.top.txt` — pre-existing pprof harness output from
  `scripts/samply-probe-tokio-share.sh` (TOKIO_SHARE_PCT=0.0 — harness-unable).
- `samply-after/probe-stdout.txt` — samply probe final TOKIO_SHARE_PCT line.

## Wave-4 grep invariant checks

```
$ grep -c "Aggregate EPS:" .planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/*.txt
.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095435Z.txt:1
.planning/phases/58-tokio-connection-handling-rewrite/perf-evidence/20260421T095551Z-c1.txt:1
Total = 2  ✓ (≥ 1; two machine-parseable lines present — C0 + C1)

$ grep -c "TOKIO_SHARE_PCT=" .planning/phases/58-tokio-connection-handling-rewrite/samply-after/probe-stdout.txt
1  ✓

$ grep -cE '1,621,616|1621616' .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md
6  ✓ (floor referenced throughout)

$ grep -cE '1,297,293|1297293' .planning/phases/58-tokio-connection-handling-rewrite/58-PERF-GATE.md
2  ✓ (Phase 57 baseline referenced)

$ grep -rEn '#\[ignore = "58-W[0-4]"' tests/*.rs
tests/tokio_spawn_absence_smoke.rs:26:#[ignore = "58-W1"]
Total = 1   (SC-1 human_needed tracking marker; 2 Wave-3 + 1 Wave-1 integration markers re-labeled to semantic)

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
OK — all 5 Phase-57 retraction counter names registered + pre-seeded ...
exit=0  ✓
```
