---
phase: 53-fjall-state-backend
plan: 01
gate: spike
status: STOP
cache_stats_available: false
cache_stats_method: "<none>"
date: 2026-04-19
host: macOS (Darwin 24.3.0, dev box)
fjall_version: 2.11.2
lsm_tree_version: 2.10.4
---

# Phase 53 Spike Gate Results

**Verdict for Plan 02: STOP.** The microbench shows fjall read-modify-write
~2,468× slower than the AHashMap baseline on this payload shape — orders of
magnitude outside the Wave-0 −25% budget. The other three probes pass
cleanly: postcard p95 = 64 B (well under 4 KB), SIGKILL verified, and the
cache-stats API probe is definitive (`cache_stats_available: false`, so
Plans 05/06 know to omit the gauge/alert).

The raw micro number overstates end-to-end regression dramatically — the
real hot path includes JSON parsing + cascade + operator state work that
dwarfs a single fjall op — but the sheer magnitude of the bench gap means
this plan should NOT be rubber-stamped. Escalate to `## CHECKPOINT
REACHED` and let the user choose between (a) adopt the deferred
write-through cache before Plan 02 lands, (b) renegotiate the end-to-end
regression budget from −15% to something wider, or (c) re-scope Phase 53.

---

## 1. fjall read-modify-write vs AHashMap (Criterion)

Measured on macOS (Darwin 24.3.0) dev box. Criterion config:
`--measurement-time 5 --sample-size 30`. Bench code:
`benches/fjall_spike.rs`. Both paths do 100 read-modify-write ops on a
1000-key pre-populated partition/map, with the same `SerializableEntityState`
payload (3 static_features, ~64 B postcard encoded). fjall runs with
`fsync_ms(None)` (background fsync disabled) for determinism — i.e. the
bench is already *optimistic* vs production `fsync_ms(5)`.

| Metric                          | AHashMap baseline | fjall RMW      | Δ%         |
|---------------------------------|-------------------|----------------|------------|
| time per 100 ops (mean)         | 94.378 µs         | 232.97 ms      | +246,648%  |
| time per op (mean)              | 943.78 ns         | 2,329,700 ns   | ~+246,648% |
| throughput (Melem/s, mean)      | 1.0596            | 0.000429       | −99.96%    |
| 95% CI (100-op time)            | [93.603, 95.161] µs | [231.56, 234.41] ms | — |

**Budget:** −25% tolerance at bench level (end-to-end −15% at integration).
**Result:** FAIL.

**Diagnosis (from fjall 2.11.2 source at
`~/.cargo/registry/src/index.crates.io-*/fjall-2.11.2/src/partition/mod.rs:980`):**

Every `PartitionHandle::insert`:
1. Takes the journal writer mutex.
2. Writes the KV pair to the journal file (`journal_writer.write_raw`).
3. Calls `persist(PersistMode::Buffer)` — a `write()` syscall on the journal
   file (no fsync, but still crosses the kernel boundary and hits the
   filesystem cache).
4. Inserts into the memtable (`self.tree.insert`).
5. Checks memtable / write-buffer overflow.

On macOS that per-op cost is ~23 µs just for the buffered journal write +
memtable insert, vs AHashMap's ~9 ns. A 2,500× gap is consistent with
"100 RMW ops × (1 small file write + memtable + postcard encode + postcard
decode)" on an APFS tempfs.

**Remediation options (for user decision at the checkpoint):**

- **(a) Write-through AHashMap cache** (CONTEXT-deferred item). Cap the
  cache per shard; only spill to fjall on eviction. Expected bench impact:
  90%+ of RMWs skip fjall entirely, closing the gap to ~−15% at end-to-end.
  Cost: Plan 02 adds ~200 LOC + a new correctness surface (cache/fjall
  consistency on crash). Probably the right call — this is precisely what
  CONTEXT Area 2 §specifics anticipated: *"Consider an `AHashMap
  write-through cache` in front of fjall for the hottest keys — but defer
  to a follow-up phase if measurements say the regression is worse than
  −15%."*
- **(b) Renegotiate budget.** Raise the end-to-end regression budget from
  −15% to e.g. −30% on the 9-cell matrix. Durability + unbounded state is
  the Phase 53 win, not throughput. Cost: user-facing "Beava got slower"
  narrative; TPC-PERSIST-05 is the shipping gate that'd need an
  amendment.
- **(c) Re-scope Phase 53.** Keep fjall for snapshot/recovery only, not
  hot-path state. Contradicts CONTEXT Area 1 ("State becomes
  durable-by-default ... crash-safe without event-log replay on the
  critical path"). Unlikely preferred.
- **(d) fjall 3.x evaluation.** 3.x has a new disk format ("Simple File
  Archive") that may close some of the per-op cost; out of scope per
  CONTEXT D-04 but worth flagging.

**Recommendation:** Option (a). It is already the CONTEXT-deferred plan for
this exact contingency and preserves the Phase 53 win. Plan 02 would need
to absorb ~1 extra day of write-through cache design.

---

## 2. Postcard EntityState byte-size distribution

Sampled 10 000 synthetic `SerializableEntityState` values from the seeded
`gen_entity` generator (3 static_features: `country`, `tier`, `score`;
streams + table_rows empty — matches TPC hot-path shape per 53-CONTEXT).

| Percentile | Bytes |
|------------|-------|
| p50        | 64    |
| p95        | 64    |
| p99        | 64    |
| max        | 64    |
| n          | 10 000 |

**Budget:** p95 ≤ 4 KB (fjall default `block_size`). **Result: PASS.**
p95 is ~64 bytes, 64× below the default block size — no need to bump
`PartitionCreateOptions::block_size(16384)` in Plan 02.

**Caveat:** This bench does not exercise real operator state (streams
vector is empty). A production entity with 8 features × 3 operators ×
1-min buckets over 24h can push into 1–3 KB. Plan 02 / Plan 05 should
re-measure on a real workload before declaring this assumption fully
closed — but for the spike gate it's comfortably within budget.

---

## 3. `std::process::Child::kill()` on macOS → SIGKILL verified

Test: `tests/macos_sigkill_verify.rs`. The test spawns a tiny ad-hoc
compiled child program with a `Drop` impl that writes `"drop-ran"` to a
canary file, sends `child.kill()`, `child.wait()`s, and asserts:

1. `child.kill()` returns `Ok(())`.
2. Exit status is a signal (`WIFSIGNALED`), not a clean exit.
3. The signal number is `libc::SIGKILL` (= 9).
4. The canary file is empty — the Drop handler did NOT run, proving the
   signal was uncatchable (SIGKILL) not SIGTERM (catchable, would run Drop).

**Test output:**
```
running 1 test
sigkill verified: child killed by signal 9 (SIGKILL=9), canary empty
test child_kill_delivers_sigkill_on_unix ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

**Drop impl ran? no. Signal = 9 (SIGKILL).** **Result: PASS.**

**Implication for Wave 4:** Plan 53-XX (crash-recovery test) can use
`std::process::Child::kill()` directly. No `nix` crate dependency
required on macOS. Linux SIGKILL semantics are identical — the same test
will be green on the Hetzner CCX43 reference box.

---

## 4. fjall 2.11 cache-stats API probe (W-4 revision)

Test: `tests/fjall_cache_stats_probe.rs`. Vendored source inspected at
`~/.cargo/registry/src/index.crates.io-*/fjall-2.11.2/src/keyspace.rs`.

**Methods tested (by grepping `fn\s+\w*[Cc]ache\w*` across fjall-2.11.2
and transitively lsm-tree-2.10.4):**

| Candidate accessor                     | Exists? | Signature |
|----------------------------------------|---------|-----------|
| `Keyspace::cache_capacity()`           | YES     | `pub fn cache_capacity(&self) -> u64` |
| `Keyspace::cache_hits()`               | NO      | — |
| `Keyspace::cache_misses()`             | NO      | — |
| `Keyspace::cache_stats()`              | NO      | — |
| `PartitionHandle::cache_hits()`        | NO      | — |
| `PartitionHandle::cache_misses()`      | NO      | — |
| `lsm_tree::Cache::hits/misses`         | NO (not public) | — |

**Cache-warm hits/misses observed:** `<API not exposed>`. `cache_capacity()`
returns only the configured max bytes, not runtime hit/miss counters.
There is no way in fjall 2.11.2 to compute a cache-hit-ratio without
forking fjall or wrapping its `BlockCache`.

**Working accessor:** `Keyspace::cache_capacity() -> u64` (returns
configured capacity, not hit ratio).

**Result for Plan 05:** `cache_stats_available: false` — Plan 05 Task 2
MUST OMIT the `beava_fjall_cache_hit_ratio` gauge. Hardcoding a placeholder
(`1.0` or `NaN`) would silently make any downstream alert vacuous and is
worse than emitting nothing.

**Result for Plan 06:** `cache_stats_available: false` — Plan 06
`docs/operations.md` MUST NOT document the `beava_fjall_cache_hit_ratio <
0.8 sustained` alert. Remove the alert entry from the operations-docs
draft.

**Forward-compat:** If a fjall 2.x patch release (or a 3.x port) exposes
cache stats, re-run `tests/fjall_cache_stats_probe.rs` and flip this
boolean. The probe test is authored to compile today (validating the only
accessor that exists — `cache_capacity()`) and carries the future API
surface in commented-out form.

---

## Verdict for Plan 02

**STOP.** Gate (1) fails by ~3 orders of magnitude. Gates (2), (3), (4)
pass.

Escalate to `## CHECKPOINT REACHED` — user must decide between
remediation option (a), (b), (c), or (d) from §1 above. Do NOT execute
Plan 02 until the checkpoint returns.

Per plan 53-01 `<acceptance_criteria>`:
> If status is `STOP`, the planner MUST follow with `## CHECKPOINT REACHED`
> to the orchestrator and Plans 02–06 are NOT executed until the user
> decides.

This SPIKE-RESULTS.md and the accompanying SUMMARY.md are both committed so
the evidence is permanent. The bench code (`benches/fjall_spike.rs`),
SIGKILL test (`tests/macos_sigkill_verify.rs`), and cache-stats probe
(`tests/fjall_cache_stats_probe.rs`) stay in the tree — they are the
regression guards Plans 02 / 05 / 06 consume. No rollback.

---

## Raw bench output

```
     Running benches/fjall_spike.rs (target/release/deps/fjall_spike-7315ee5fae22ceda)
Benchmarking fjall_spike::ahashmap_baseline/rmw_100_ops
Benchmarking fjall_spike::ahashmap_baseline/rmw_100_ops: Warming up for 3.0000 s
Benchmarking fjall_spike::ahashmap_baseline/rmw_100_ops: Collecting 30 samples in estimated 5.0587 s (24k iterations)
Benchmarking fjall_spike::ahashmap_baseline/rmw_100_ops: Analyzing
fjall_spike::ahashmap_baseline/rmw_100_ops
                        time:   [93.603 µs 94.378 µs 95.161 µs]
                        thrpt:  [1.0508 Melem/s 1.0596 Melem/s 1.0683 Melem/s]
Found 4 outliers among 30 measurements (13.33%)
  1 (3.33%) low severe
  2 (6.67%) low mild
  1 (3.33%) high mild

Benchmarking fjall_spike::fjall_rmw/rmw_100_ops
Benchmarking fjall_spike::fjall_rmw/rmw_100_ops: Warming up for 3.0000 s

Warning: Unable to complete 30 samples in 5.0s. You may wish to increase target time to 8.1s, or reduce sample count to 10.
Benchmarking fjall_spike::fjall_rmw/rmw_100_ops: Collecting 30 samples in estimated 8.1092 s (30 iterations)
Benchmarking fjall_spike::fjall_rmw/rmw_100_ops: Analyzing
fjall_spike::fjall_rmw/rmw_100_ops
                        time:   [231.56 ms 232.97 ms 234.41 ms]
                        thrpt:  [426.61  elem/s 429.24  elem/s 431.86  elem/s]

postcard_size_p50=64 p95=64 p99=64 max=64 n=10000
Benchmarking fjall_spike::postcard_sizes/noop_histogram_dump
Benchmarking fjall_spike::postcard_sizes/noop_histogram_dump: Warming up for 3.0000 s
Benchmarking fjall_spike::postcard_sizes/noop_histogram_dump: Collecting 30 samples in estimated 5.0000 s (12B iterations)
Benchmarking fjall_spike::postcard_sizes/noop_histogram_dump: Analyzing
fjall_spike::postcard_sizes/noop_histogram_dump
                        time:   [416.70 ps 417.44 ps 418.44 ps]
Found 2 outliers among 30 measurements (6.67%)
  1 (3.33%) high mild
  1 (3.33%) high severe
```

---

## Budget reference (for grep discoverability)

Budget: −25% tolerance at bench level; p95 ≤ 4 KB; SIGKILL verified;
cache-stats API probe outcome recorded.
