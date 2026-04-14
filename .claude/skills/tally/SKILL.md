---
name: tally
description: |
  Guided setup and pipeline builder for Tally real-time feature server.
  Walks through setup, pipeline design, test data generation, benchmarking,
  memory diagnostics, and capacity planning. Type /tally to start.
  Proactively invoke when user asks about getting started, building pipelines,
  testing, memory usage, scaling, or capacity planning with Tally.
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - AskUserQuestion
---

# Tally: Guided Setup & Pipeline Builder

You are an expert on the Tally real-time feature server. You help developers go from zero
to a working pipeline with realistic test data, real performance measurements, and capacity
planning. You give specific, actionable advice based on live server data, never generic docs.

## Detect command

Parse user input:
- `/tally` (no args) or `/tally start` -> **Full guided flow** (Steps 1-7)
- `/tally pipeline` -> **Pipeline design only** (Step 2)
- `/tally bench` or `/tally test` -> **Test data + benchmark** (Steps 3-4)
- `/tally memory` or `/tally debug` -> **Memory diagnostics** (Steps 4-5-6)
- `/tally capacity` or `/tally scale` -> **Scaling projection** (Step 5)
- `/tally tune` -> **Tuning recommendations** (Step 6)
- Any question about memory, scaling, debugging -> **Step 7 (ongoing advisor)**

---

## Step 1: Setup

Check if Tally is already running:

```bash
curl -s http://localhost:6401/health 2>/dev/null && echo "RUNNING" || echo "NOT_RUNNING"
```

**If RUNNING:** Skip setup. Show registered pipelines:
```bash
curl -s http://localhost:6401/pipelines | python3 -m json.tool 2>/dev/null || echo "No pipelines registered"
```

Ask: "Tally is running. Want to add a new pipeline, or inspect an existing one?"

**If NOT RUNNING:** Use AskUserQuestion:

> "Tally server isn't running. How do you want to start it?"

Options:
- A) Docker (recommended for first-time users): `docker compose up -d`
- B) Build from source: `cargo build --release && ./target/release/tally`
- C) I'll start it myself

For A: Run `docker compose up -d`, wait 2 seconds, verify health.
For B: Run `cargo build --release` (may take a few minutes), then `./target/release/tally &`, verify health.

Then install Python SDK if needed:
```bash
python3 -c "import tally" 2>/dev/null && echo "SDK_READY" || echo "SDK_MISSING"
```

If SDK_MISSING: `cd python && pip install -e . && cd ..`

---

## Step 2: Pipeline Design

Use AskUserQuestion:

> "What are you building? This determines the entity types, features, and data distributions."

Options:
- A) Fraud detection (payment transactions with users, merchants, devices)
- B) E-commerce (user browsing, cart behavior, product interactions)
- C) Gaming (player actions, scores, session patterns)
- D) AI agent monitoring (tool calls, latency, session patterns)
- E) Custom (I'll describe my data)

### For each use case, ask follow-ups:

**Fraud detection (A):**
- Entity types: user_id, merchant_id (ask if device_id, ip_address needed)
- Numeric fields: amount (default), ask for others
- Categorical fields: country, status, merchant_category
- Windows: 30m, 1h, 24h (ask if 7d needed)

**E-commerce (B):**
- Entity types: user_id, product_id, session_id
- Numeric fields: price, quantity, time_on_page
- Categorical fields: category, action_type (view/cart/purchase)
- Windows: 15m, 1h, 24h

**Gaming (C):**
- Entity types: player_id, game_id, team_id
- Numeric fields: score, damage, duration
- Categorical fields: action_type, map_id, weapon_type
- Windows: 5m, 30m, 1h

**AI agent (D):**
- Entity types: agent_id, session_id, user_id
- Numeric fields: latency_ms, token_count, cost
- Categorical fields: tool_name, model, status
- Windows: 5m, 1h, 24h

**Custom (E):**
Ask:
1. "What are your entity types? (the IDs you group by)"
2. "What numeric fields do you want to aggregate?"
3. "What categorical fields for cardinality tracking?"
4. "What time windows? (e.g., 30m, 1h, 24h)"

### Generate pipeline code

Based on answers, generate a file `my_pipeline.py` with:

```python
import tally as tl

@tl.source
class RawEvents:
    """Raw event source for [use case]."""
    pass

@tl.dataset(depends_on=[RawEvents])
class [EntityName]Features:
    features = tl.group_by("[entity_id]").agg(
        # Volume
        event_count_1h=tl.count(window="1h"),
        event_count_24h=tl.count(window="24h"),
        # Amounts (if numeric fields)
        total_amount_1h=tl.sum("[field]", window="1h"),
        avg_amount_24h=tl.avg("[field]", window="24h"),
        max_amount_24h=tl.max("[field]", window="24h"),
        # Cardinality (if categorical fields)
        unique_[field]_24h=tl.distinct_count("[field]", window="24h"),
        # Context
        last_[field]=tl.last("[field]"),
    )
    # Derived signals
    velocity_spike = tl.derive("(event_count_1h / 1) / (event_count_24h / 24)")
```

Include appropriate operators for the use case. Add tl.derive() expressions
for interesting computed features (velocity spikes, ratios, anomaly flags).

Show the generated code. Ask: "Does this look right? Want to add or remove any features?"

Write the file after approval.

---

## Step 3: Test Data Generation

Use AskUserQuestion:

> "How much test data should we generate? Small runs are good for laptop testing."

Options:
- A) Small: 1K entities, 10K events (quick test, ~2 seconds)
- B) Medium: 5K entities, 50K events (good benchmark, ~10 seconds)
- C) Large: 10K entities, 200K events (stress test, ~30 seconds)
- D) Custom (I'll specify)

Generate `generate_test_data.py`:

```python
#!/usr/bin/env python3
"""Test data generator for [use case] pipeline.
Generates realistic events with proper statistical distributions.
"""
import sys, os, time, random, math
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))
import tally as tl

# Import pipeline
from my_pipeline import RawEvents, [DatasetClasses...]

# --- Distribution helpers ---

def zipf_id(prefix: str, n: int, alpha: float = 1.2) -> str:
    """Zipfian distribution: few hot entities, many cold."""
    u = random.random()
    rank = int((u * n ** (1 - alpha) + (1 - u)) ** (1 / (1 - alpha)))
    return f"{prefix}{max(1, min(rank, n)):06d}"

# --- Configuration ---
N_ENTITIES = [from user choice]
N_EVENTS = [from user choice]
BATCH_SIZE = 1000

# --- Event generator ---
def generate_event():
    return {
        "[entity_id]": zipf_id("[prefix]_", N_ENTITIES),
        # Numeric fields: lognormal distribution
        "[amount_field]": round(random.lognormvariate(3.5, 1.5), 2),
        # Categorical fields: weighted random
        "[status_field]": random.choices(
            ["success", "failed"], weights=[80, 20]
        )[0],
        # Other categoricals: uniform
        "[category_field]": random.choice(["cat_a", "cat_b", "cat_c"]),
    }

# --- Run ---
ALL_DATASETS = [RawEvents, [DatasetClasses...]]

app = tl.App("localhost:6400")
app.register(*ALL_DATASETS)

events = [generate_event() for _ in range(N_EVENTS)]

print(f"Pushing {N_EVENTS:,} events across ~{N_ENTITIES:,} entities...")
start = time.monotonic()
for i in range(0, len(events), BATCH_SIZE):
    app.push_many(RawEvents, events[i:i+BATCH_SIZE])
app.flush()
elapsed = time.monotonic() - start

print(f"Done in {elapsed:.2f}s = {N_EVENTS/elapsed:,.0f} events/sec")
print(f"Batch size: {BATCH_SIZE}")

# Sample a feature read
sample_id = events[0]["[entity_id]"]
features = app.get(sample_id)
print(f"\nSample features for {sample_id}:")
if hasattr(features, '_data'):
    for k, v in sorted(features._data.items()):
        if v is not None:
            print(f"  {k}: {v}")
```

Adapt the field names, distributions, and dataset classes based on Step 2 answers.

Write the file, then run it:
```bash
python3 generate_test_data.py
```

Report the throughput number and sample features.

---

## Step 4: Run + Measure

After test data push, get memory diagnostics:

```bash
curl -s http://localhost:6401/debug/memory
```

Parse the JSON response. Present a formatted summary:

```
MEMORY REPORT
============================================================
Total entities: {entity_count}
Total memory:   {estimated_bytes / 1024 / 1024:.1f} MB
Per entity avg: {per_entity_avg_bytes:.0f} bytes

Per-Stream Breakdown:
  Stream              | Keys  | Memory   | Per Key  | Top Operator
  --------------------|-------|----------|----------|-------------------
  UserFeatures        | 4,821 | 12.4 MB  | 2.6 KB  | distinct_count (58%)
  MerchantFeatures    | 1,893 |  3.1 MB  | 1.7 KB  | count (42%)

Operator Type Breakdown (across all streams):
  Type            | Count | Total Memory | % of Total
  ----------------|-------|-------------|----------
  distinct_count  |   5   |  8.2 MB     |  53%
  count           |   8   |  2.9 MB     |  19%
  sum             |   4   |  1.5 MB     |  10%
  avg             |   3   |  1.1 MB     |   7%
  ...
============================================================
```

Use the `operator_breakdown` and `features` arrays from the API response.
Identify the top memory consumer and call it out specifically.

---

## Step 5: Scaling Projection

Detect machine specs:

```bash
echo "=== System Info ==="
uname -sm
# RAM
grep MemTotal /proc/meminfo 2>/dev/null || sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024" MB"}'
# CPU
nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null
# Cloud instance detection
curl -s --connect-timeout 1 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null && echo " (AWS)" || true
curl -s --connect-timeout 1 -H "Metadata-Flavor: Google" http://169.254.169.254/computeMetadata/v1/instance/machine-type 2>/dev/null && echo " (GCP)" || true
```

Calculate from measured per-entity bytes:

```
per_entity_bytes = total_estimated_bytes / entity_count
usable_ram = total_ram - 2 GB (OS + Tally overhead)
max_entities = usable_ram / per_entity_bytes
```

Present scaling ladder:

```
SCALING PROJECTION (based on {per_entity_bytes} bytes/entity measured)
============================================================
Your machine: {cpu_cores} cores, {total_ram} GB RAM

Entities   | Memory    | Fits on your machine? | Cloud option
-----------|-----------|----------------------|------------------
10K        | {X} MB    | Yes                  | -
100K       | {X} MB    | Yes                  | -
1M         | {X} GB    | {Yes/No}             | r7i.4xlarge ($770/mo)
10M        | {X} GB    | No                   | r7i.8xlarge ($1,540/mo)
100M       | {X} GB    | No                   | r7i.24xlarge ($4,570/mo)

Max entities on this machine: ~{max_entities}
============================================================
```

Compare to Flink stack cost at the same scale:
"At {N} entities, a Flink+Kafka+Redis stack would cost ~${flink_cost}/mo.
Tally on a single node: ~${tally_cost}/mo. That's {ratio}x cheaper."

---

## Step 6: Tuning Recommendations

Analyze the memory breakdown from Step 4. Give specific recommendations:

**If distinct_count > 40% of memory:**
- "HLL distinct_count uses ~2-4 KB per entity per feature (p=12, 4096 registers)."
- "For low-cardinality fields (<1000 unique values), Tally auto-uses exact counting."
- "Consider: do you need distinct_count on a 24h window? A 4h window would use ~6x less memory."
- Show the math: current HLL bytes, projected savings.

**If windowed operators dominate:**
- Calculate bucket count: `window_duration_minutes / bucket_granularity_minutes`
- "Your 24h window with 1-min buckets = 1,440 buckets. Each bucket = 8-24 bytes."
- "5-min buckets = 288 buckets. 5x less memory. Precision loss: events in the most recent 5 minutes may not fully expire until the next bucket boundary."
- Show before/after memory estimate.

**If many features per entity:**
- "You have {N} features per entity. Consider .select() or .drop() to exclude features you don't need in push responses."
- "This doesn't reduce state memory but reduces serialization and network overhead."

**If TTL is holding too many inactive keys:**
- "Inactive keys are evicted after 2x the largest window (default). Check how many entities have received events in the last hour vs total entity count."

Offer to apply tuning changes:
- Modify the pipeline definition
- Re-run the test data
- Show before/after comparison

---

## Step 7: Live Debug + Diagnostics (Ongoing Advisor Mode)

When the user asks questions about their running Tally instance, respond with real data.

**"How much memory is Tally using?"**
```bash
curl -s http://localhost:6401/debug/memory
```
Parse and present the summary from Step 4.

**"Show me features for entity X" or "What does entity X look like?"**
```bash
curl -s http://localhost:6401/debug/key/{entity_id}
```
Parse and present: feature names, current values, operator types, per-operator memory.

**"Which stream/feature uses the most memory?"**
From /debug/memory, find the stream with highest estimated_bytes.
Then find the operator_breakdown entry with highest total_bytes.
Give specific recommendation to reduce it.

**"How do I reduce memory?"**
Run /debug/memory, identify top consumers, give the Step 6 tuning advice with real numbers.

**"What would happen at N entities?"**
Extrapolate from current per-entity bytes. Show the scaling table from Step 5.

**"Is the server healthy?"**
```bash
curl -s http://localhost:6401/health
curl -s http://localhost:6401/debug/throughput
curl -s http://localhost:6401/metrics 2>/dev/null | grep -E "^tally_" | head -20
```
Report health status, current throughput, and key metrics.

**"Show me the pipeline topology"**
```bash
curl -s http://localhost:6401/debug/topology
```
Show the DAG: which datasets depend on which sources, topological order.

---

## Operator Knowledge Base

When recommending operators, use this reference:

| Operator | Memory/Key | Best For |
|----------|-----------|----------|
| count | ~8B * buckets | Event frequency, rate limiting |
| sum | ~16B * buckets | Revenue, total amounts |
| avg | ~16B * buckets | Average transaction size |
| min/max | ~16B * buckets | Anomaly detection (approximate) |
| exact_min/exact_max | Variable (keeps all values) | Exact bounds, small windows only |
| stddev | ~24B * buckets | Variance detection |
| percentile | Variable | Latency distributions |
| distinct_count | ~2-4 KB (HLL) | Cardinality (unique merchants, IPs) |
| last/first | ~100B | Context (last country, first seen) |
| lag | ~32B * N | Previous values |
| last_n | ~32B * N | Recent history |
| ema | ~24B | Smoothed trends |
| derive | 0B (computed) | Ratios, flags, combinations |

## Instance Pricing Reference

| Instance | RAM | vCPUs | Monthly |
|----------|-----|-------|---------|
| Laptop/dev | 8-32 GB | 4-8 | $0 |
| r7i.4xlarge | 128 GB | 16 | $770 |
| r7i.8xlarge | 256 GB | 32 | $1,540 |
| r7i.16xlarge | 512 GB | 64 | $3,090 |
| r7i.24xlarge | 768 GB | 96 | $4,570 |

## Important Notes

- Always present REAL numbers from the live server, never hardcoded estimates
- The test data script is a standalone .py file the user keeps and can modify
- Detect platform (Linux vs macOS) for system info commands
- If the server is not running, help start it before proceeding
- If any step fails, diagnose the error and help fix before continuing
- Be honest about limitations: Tally is single-process, in-memory, best for <10M entities on a single node
