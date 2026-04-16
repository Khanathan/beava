# Tally Throughput Benchmark — Baseline

Captures the **before-FINDINGS** throughput numbers for Tally in its current state (single-threaded core, JSON payloads inside binary frames). These numbers become the baseline for v1.2 Performance milestone phases.

## What it measures

- **Sync PUSH throughput** — events/sec, p50/p95/p99 latency per event
- **Three pipeline shapes** — small (1 stream, 5 features), medium (2 streams + 1 view), large (3 streams + 2 views with cascade + fan-out)
- **Concurrent client scaling** — 1, 4, 8, 16 parallel SDK connections

## Running

```bash
# 1. Start Tally in release mode
cd /data/home/tally
export PATH="/data/home/.cargo/bin:$PATH"
cargo build --release
./target/release/tally &
TALLY_PID=$!
sleep 1

# 2. Run the benchmark
cd benchmark/tally-throughput
python3 bench.py --events 100000 --clients 1 --pipeline medium

# 3. Profile with perf (run while bench is executing)
perf record -F 997 -g -p $TALLY_PID -- sleep 10
perf report --stdio | head -50

# 4. Clean up
kill $TALLY_PID
rm -f /data/home/tally/tally.snapshot.*
```

## Results location

Runs are written to `benchmark/tally-throughput/results/` as timestamped JSON files.
