# Tally — Pending Benchmarks for VAS 2026 Deck

Every number in `deck.md` that is currently a **target** instead of a **measured result**. Run these against the implemented binary before the deck ships to judges. Mark each one with the real number and a verdict (`MEASURED ✓` / `ADJUSTED` / `ASPIRATIONAL`).

## How to use this file

1. Run each benchmark. Save raw output to `benches/results/YYYY-MM-DD-<name>.txt` so a skeptical judge can reproduce.
2. Record the real number in the "Measured" column below.
3. If the real number is close to the target, mark `MEASURED ✓` and the slide stays as-is.
4. If the real number is off (better or worse), mark `ADJUSTED` and update the slide to the true number.
5. If the benchmark can't be run yet (Phase 9 code not shipped), mark `ASPIRATIONAL` and decide whether to keep the claim or soften it.
6. Commit the benchmark scripts themselves to `benches/` so the numbers are reproducible.

---

## Core performance claims (Slide 8 — load-bearing)

These four stat cards are the most load-bearing numbers in the deck. A judge will fixate on them.

### B-01 · Throughput, sustained single-threaded PUSH
- **Claim:** 100K+ events/sec on one thread
- **How to measure:** Run for 60 seconds minimum with a mixed operator workload — 3 count operators (1h / 24h / 7d windows), 2 sum operators, 1 avg, 1 distinct_count (HLL), 1 last, 2 derives. Realistic 10-feature stream. Use a single producer with persistent TCP connection.
- **Hardware:** Commodity x86 (e.g., AWS c7i.xlarge, 4 vCPU, 8GB RAM) AND one Apple Silicon (M2/M3) baseline
- **Record:** p50, p99, p99.9 throughput; sustained rate; any latency spikes
- **Slide impact if miss:** If sustained rate is below 80K, change slide to "80K+ events/sec." If above 150K, brag louder.
- **Measured:** _________________

### B-02 · PUSH latency p99
- **Claim:** <100µs
- **How to measure:** Single-event PUSH, realistic payload (10 features, 5 operators). Measure two things:
  - **Server-side:** from protocol receive to response write
  - **End-to-end from Python SDK:** `time.perf_counter()` around `app.push(...)` — this is the number a user actually experiences
- **Record:** p50, p99, p99.9 for both
- **Slide impact if miss:** If server-side p99 > 150µs, update slide. The end-to-end SDK number will almost certainly be higher (network round-trip); decide whether to show both or just server-side.
- **Measured:** _________________

### B-03 · GET latency p99
- **Claim:** <50µs
- **How to measure:** Single key lookup, return all features for that key (assume 10 features). Server-side and Python SDK end-to-end.
- **Record:** p50, p99, p99.9 for both
- **Measured:** _________________

### B-04 · Memory per key, 10 mixed features
- **Claim:** <5KB average
- **How to measure:** Populate with 100K keys, each with 10 features matching the mix above (3 counts, 2 sums, 1 avg, 1 HLL, 1 last, 2 derives). Measure RSS of the Tally process after populating. Divide by key count. Report both average and p99 per-key size.
- **Record:** Average KB/key, p99 KB/key, total process RSS
- **Slide impact if miss:** If average is >8KB, adjust claim. The HLL alone is ~12KB per distinct_count operator, so if every key has one, the "5KB" number isn't tenable — change to "12KB per key with HLL, <2KB without."
- **Measured:** _________________

---

## Snapshot and recovery (Slides 5, 8, 12)

### B-05 · Snapshot write time, 1M keys
- **Claim:** <1 second (current deck says "< 1s per 1M keys")
- **How to measure:** Load 1M keys with 10 features each. Trigger a full snapshot. Measure wall-clock time from trigger to disk sync completion. Also measure with Phase 9 incremental snapshot (if shipped).
- **Record:** Full snapshot time, incremental snapshot time, snapshot file size on disk
- **Slide impact if miss:** If full snapshot takes >3 seconds, soften to "<5s per 1M keys" and emphasize the incremental path as the hot loop.
- **Measured:** _________________

### B-06 · Snapshot recovery time, 1M keys
- **Claim:** <5 seconds from cold start
- **How to measure:** Start with an existing 1M-key snapshot on disk. Launch the binary. Measure wall-clock from process start to "serving traffic" (first successful PUSH/GET round-trip).
- **Record:** Cold start time, time to first successful request
- **Measured:** _________________

---

## Cost comparison (Slide 4, Slide 8 — the cost card is currently estimated)

Every dollar figure on the benchmarks and solution slides needs a real model behind it.

### B-07 · Status-quo stack real cost model
- **Claim:** "$450K–$1M / year" all-in for Kafka + Flink + Feast + platform team
- **What to do:** Build a defensible spreadsheet with line items for a realistic 100K events/sec production workload:
  - **Kafka on Confluent Cloud** — use the real Confluent Cloud pricing calculator. Standard cluster, enough partitions for 100K events/sec throughput, 7-day retention. Record the monthly number.
  - **Flink** — either Confluent Cloud for Flink, or AWS EKS with a 6-node Flink cluster (r6i.2xlarge or equivalent). Whichever is cheaper. Record monthly cost.
  - **Feast** — infrastructure only, assume AWS with Redis (ElastiCache medium) + Postgres (RDS medium). Record monthly cost.
  - **Platform engineer salaries** — use levels.fyi or Pave for "Sr. ML Infra Engineer" at growth-stage ($180K–$250K base, ~$350K total comp with benefits/tax). Multiply by 3.
  - **Cloud + SaaS license subtotal** — Kafka + Flink + Feast monthly × 12
  - **Salaries subtotal** — 3 × total comp
  - **All-in total** — add them up
- **Commit to:** `deck/cost-model.xlsx` or `deck/cost-model.md` with explicit sources for each number
- **Slide impact if miss:** If the real all-in is $300K not $450K, update both the stack box on Slide 3 and the cost card on Slide 8. The "100× cheaper" claim on Slide 4 also has to match — if actual compression is 50× or 75×, say that.
- **Measured:** _________________

### B-08 · Tally real cost model
- **Claim:** "$6K–$60K / year" all-in for Tally
- **What to do:** Same spreadsheet format. Line items:
  - **Commodity VM** — e.g., AWS c7i.xlarge at $0.18/hr × 730h = ~$130/month. Or Hetzner CCX23 at ~$35/month.
  - **Snapshot storage on S3** — 1M keys × ~500 bytes/key × 2 daily snapshots × 30 days = negligible (~$1/month)
  - **Bandwidth** — cross-AZ or egress, depends on client location. Usually <$50/month for this workload.
  - **Monitoring sidecar** — Grafana Cloud free tier, or $20/month paid tier
  - **Platform engineer** — $0 (the claim is that you don't need one)
  - **All-in total** — monthly × 12
- **Slide impact if miss:** If the all-in is $15K instead of $6K, update the lower bound. The cost card on Slide 8 should match reality exactly.
- **Measured:** _________________

### B-09 · Compression ratio claim verification
- **Claim:** "100× cheaper" (Slide 4 punchline) / "75×" implied by the $450K vs $6K comparison
- **What to do:** Once B-07 and B-08 are measured, compute the real ratio. Pick one number and use it consistently across Slide 4, Slide 8, and any narration.
- **Slide impact:** If real ratio is 50×, update the Slide 4 punchline from "100×" to "50×" — still impressive, and defensible under scrutiny.
- **Measured:** _________________

---

## DevEx claims (Slides 1, 4, 6 — currently unmeasured)

These are user-experience claims. They need a stopwatch, not a load generator.

### B-10 · Time to install Tally on a fresh Linux box
- **Claim:** "Install in 60 seconds" (hero slide)
- **How to measure:** Fresh Ubuntu 22.04 VM. Run the install command you plan to publish (`cargo install tally`, or `curl | sh`, or `apt install`). Stopwatch from first keystroke to `tally --version` returning.
- **Record:** Real wall-clock time
- **Slide impact if miss:** If it's 3 minutes, change hero to "Install in 3 minutes." If it's 15 seconds, change to "Install in 15 seconds" — that's stronger.
- **Measured:** _________________

### B-11 · Time to first working feature from a fresh install
- **Claim:** "10 minutes to first feature" (Slides 4, 10)
- **How to measure:** Stopwatch from `tally --version` working to a `@tl.stream` class defined + `app.push()` returning a computed feature value. Include the time to write the Python class manually (don't use Claude for this measurement — measure the cold human experience).
- **Record:** Real time for a backend engineer unfamiliar with Tally
- **Slide impact if miss:** Use the real number. "4 minutes" or "12 minutes" is fine — just be accurate.
- **Measured:** _________________

### B-12 · Claude-authored pipeline demo (not a benchmark, a demonstration)
- **Claim:** "Vibe-code your pipelines" (hero, Slide 6)
- **What to do:** Record a 30–60 second screencast: plain English fraud-detection spec → Claude outputs a valid `@tl.stream` Python class → paste into a running Tally → feature returns a value. This becomes the optional 3-minute VAS pitch video.
- **Deliverable:** `deck/demo-vibe-coding.mp4` (or .gif)
- **Done:** _________________

---

## New metrics to measure and add to the deck (optional but valuable)

These aren't currently claimed but would meaningfully strengthen specific slides if the numbers are good.

### B-13 · Compiled binary size
- **What to measure:** `ls -lh target/release/tally` after a release build with `cargo build --release`. Strip symbols with `strip` for the distribution binary.
- **Why it matters:** A 12MB static Rust binary makes "one binary" visceral. A 180MB binary undersells it. If it's small, add "N-MB single binary" to Slide 5 or Slide 8.
- **Target:** <20MB stripped
- **Measured:** _________________

### B-14 · Cold start time to serving traffic
- **What to measure:** Process launch to first successful request served (empty state, no snapshot to load). Should be ~milliseconds.
- **Why it matters:** Supports the "Install in 60 seconds" hero claim. Also supports scale-with-usage narrative — Tally instances spin up fast.
- **Target:** <500ms
- **Measured:** _________________

### B-15 · MSET bulk write throughput
- **What to measure:** Bulk-insert 100K keys via MSET. Measure wall-clock time and derived keys/sec throughput.
- **Why it matters:** Supports "scale with usage" — going from 0 to 100K users is a one-time bulk load, and if it's fast, that's a good story. Claim: "100K keys in <N seconds."
- **Target:** 100K keys loaded in <5 seconds
- **Measured:** _________________

### B-16 · Snapshot file size (1M keys, 10 features each)
- **What to measure:** Actual disk bytes of the snapshot file after populating 1M keys × 10 features.
- **Why it matters:** "Your entire state fits on a USB stick" is a memorable line if the number is small. If the file is 50MB, say it. If it's 5GB, don't.
- **Target:** <500MB for 1M keys × 10 features
- **Measured:** _________________

### B-17 · Python SDK connection latency
- **What to measure:** Time from `app.push()` call in Python to response received in Python. This is the SDK-inclusive version of B-02.
- **Why it matters:** Users experience SDK latency, not server latency. If SDK overhead is >50µs, the "end-to-end sub-ms" story weakens. Measure and decide whether to report the raw server number or the realistic SDK number on Slide 8.
- **Target:** <200µs end-to-end p99
- **Measured:** _________________

---

## Benchmark reporting template

When you run each benchmark, append a section to this file:

```
## Results — YYYY-MM-DD

### B-01 · Throughput
- Hardware: AWS c7i.xlarge (4 vCPU, 8GB RAM)
- Workload: 10 features, mixed operators, 60s run
- Measured: 142K events/sec sustained (p50 7µs, p99 34µs, p99.9 180µs)
- Verdict: MEASURED ✓ — exceeds claim, update slide to "140K+"
- Raw output: benches/results/2026-04-12-b01-throughput.txt
- Reproducer: benches/b01_throughput.rs
```

Keep raw outputs in `benches/results/` so a judge can verify.

## Benchmarks that CANNOT wait for the deck

The VAS submission deadline is April 15, 2026. These four are non-negotiable and must be measured before the deck ships — everything else is nice-to-have:

1. **B-01** — Throughput. The "100K+ events/sec" claim is the most load-bearing number.
2. **B-02** — PUSH latency p99. The sub-100µs claim is what makes "real-time" credible.
3. **B-07 + B-08** — Cost models. The "100× cheaper" claim can be demolished in one question if the model isn't real.

If the four above come in significantly off the targets, update the slides before submission. Everything else can stay as "target" with a note in the appendix explaining that the benchmark suite runs at Phase 9 completion in Q2 2026.

## Quick command reference

```bash
# Build release binary
cargo build --release

# Run core throughput benchmark (once benches/throughput.rs exists)
cargo bench --bench throughput

# Run latency benchmark
cargo bench --bench latency

# Save raw output with timestamp
cargo bench --bench throughput 2>&1 | tee benches/results/$(date +%Y-%m-%d)-throughput.txt

# Render deck to PDF after any number updates
cd deck && marp deck.md -o deck.pdf --allow-local-files
```
