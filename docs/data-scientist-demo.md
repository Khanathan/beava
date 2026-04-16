# Data Scientist Fork Demo — Setup Guide

Show how a data scientist clones a scoped slice of a running production Beava cluster to their laptop, registers their own pipelines, runs them against historical events backfilled from prod, and watches live updates tail in.

Product surface:
- `beava fork` CLI (bash)
- `bv.fork(...)` Python API

---

## What "fork" does

```
             production                            scientist's laptop
 ┌──────────────────────────────┐         ┌──────────────────────────────┐
 │  beava serve (port 6400)     │         │  beava fork  (port 7400)     │
 │  ┌────────────────────────┐  │  LOG    │  ┌────────────────────────┐  │
 │  │ event log (per-stream) ├──┼────────▶│  │ local engine + state   │  │
 │  └────────────────────────┘  │  FETCH  │  │ runs SCIENTIST's       │  │
 │  ┌────────────────────────┐  │         │  │ registered pipelines   │  │
 │  │ ingest + pipelines     │  │  SUB    │  │                        │  │
 │  └────────────────────────┘  │────────▶│  └────────────────────────┘  │
 └──────────────────────────────┘  SCRIBE └──────────────────────────────┘
                                                        ↑
                                                 bv.Client queries
```

**Key properties:**
- Replica pulls **raw CDC events** from prod (not prod's aggregates). The scientist's pipelines — which may be different from prod's — run against those events locally.
- Scoped: only events matching declared streams + keys are transferred.
- Historical backfill (`--since T`) + live tail (`OP_SUBSCRIBE`) in one command.
- Replica is a full Beava server locally. Scientist queries it with the normal `bv.Client` HTTP/TCP API.
- Replica REJECTS local `PUSH` — it's read-only from prod.

---

## Prerequisites

- Production Beava running and reachable (TCP port 6400 by default).
- Admin token to the production server (`BEAVA_ADMIN_TOKEN`).
- Scientist's laptop: Linux x86_64, Rust toolchain if building from source, or a pre-built `beava` binary.
- Python 3.10+ with the Beava SDK installed (`pip install -e ./python` from the repo, or `pip install beava` once published).

---

## Path A — one-liner Python (recommended)

Uses the real fraud pipeline from `benchmark/fraud-pipeline/bench_fraud.py` — 5 entity
tables, 47 features across four window tiers (30m / 1h / 24h / 7d), derived risk
signals. The scientist imports it rather than re-authoring a toy.

```python
import sys
sys.path.insert(0, "benchmark/fraud-pipeline")
import bench_fraud as bf   # RawTransactions + 5 table pipelines
import beava as bv

# Spawn a scoped local replica, register prod's fraud pipeline, start streaming.
with bv.fork(
    remote="prod.beava.internal:6400",
    streams=[bf.RawTransactions],
    key_prefix=["user_fraud_"],          # scope to the four injected fraud users
    since="2026-04-15T00:00:00Z",        # backfill from this wall-clock
    token="prod-admin-token",            # or set BEAVA_REPLICA_TOKEN env
    pipelines=[
        bf.UserTransactions,             # 25 features: velocity, stddev, geo, etc.
        bf.UserFailedTxns,               # 4 features: card-testing signal
        bf.MerchantActivity,             # 8 features: merchant risk
        bf.DeviceActivity,               # 5 features: device fingerprint
        bf.IPActivity,                   # 5 features: IP fan-out
    ],
) as fork:
    # Fork is now running on localhost:7400 (by default).
    # Historical backfill has completed. Live tail is active.
    print(fork.get(bf.UserTransactions, key="user_fraud_001"))
    # → {"tx_count_1h": 20, "unique_merchants_1h": 20, "velocity_spike": 15.2,
    #    "merchant_diversity_1h": 1.0, "country_hop_flag": False, ...}
    print(fork.inspect())   # {"RawTransactions": <event count in scope>}
# On exit, the fork shuts down cleanly.
```

The `with` block handles subprocess lifecycle, port allocation, `/debug/ready` polling, and teardown.

> Operators exercised by this pipeline: `count`, `sum`, `avg`, `min`, `max`, `stddev`,
> `count_distinct` (HLL), `last`, `filter` (`.filter(bv.col("status") == "failed")`),
> `group_by`, `with_columns` (derived risk signals like `velocity_spike`,
> `merchant_diversity_1h`, `country_hop_flag`). See `docs/operators.md` for the full
> operator reference.

---

## Path B — CLI (for ops-style usage)

```bash
# Hand-author a REGISTER JSON for the scientist's pipeline.
# Or export it via the Python SDK: bv.serialize_pipeline(TxnSummary, "/tmp/my_pipeline.json")
cat > /tmp/my_pipeline.json <<'JSON'
{
  "kind": "table",
  "name": "txn_summary",
  "key": "user_id",
  "source": {"name": "Transactions", ...},
  "aggregation": {
    "count": {"op": "count", "window": "1h"},
    "total": {"op": "sum", "field": "amount", "window": "1h"}
  }
}
JSON

# Launch the fork.
beava fork \
  --remote prod.beava.internal:6400 \
  --streams Transactions \
  --keys u1,u2,u3 \
  --since 2026-03-01T00:00:00Z \
  --token $PROD_ADMIN_TOKEN \
  --local-port 7400 \
  --pipeline-file /tmp/my_pipeline.json

# In another terminal — query the replica like any Beava server.
curl http://127.0.0.1:7400/debug/ready
curl -H "Authorization: Bearer $PROD_ADMIN_TOKEN" \
     http://127.0.0.1:7400/debug/key/u1
```

`beava fork` is a thin wrapper around `beava serve --replica-from ...`. Power users can drop to the underlying flags directly.

---

## Path C — historical point-in-time extraction (Phase 44-01)

Scientists frequently need "what did these feature values look like at `T_i`?" for multiple `T_i` in one go — e.g. training a model that needs features as-of each label timestamp. `bv.fork(extract_at=[...])` does this in a single replay:

```python
from datetime import datetime, timezone

t1 = datetime(2026, 3, 5, 10, 0, 0, tzinfo=timezone.utc)
t2 = datetime(2026, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
t3 = datetime(2026, 4, 1, 10, 0, 0, tzinfo=timezone.utc)

with bv.fork(
    remote="prod.beava.internal:6400",
    streams=[Transactions],
    keys=["u1", "u2"],
    since="2026-03-01T00:00:00Z",
    token="prod-admin-token",
    pipelines=[TxnSummary],
    extract_at=[t1, t2, t3],   # datetime / ISO-8601 str / unix-ms int all OK
) as fork:
    history = fork.extract_history()
    # {
    #   "2026-03-05T10:00:00Z": {
    #       "u1": {"count": 3, "total": 60.0},
    #       "u2": {"count": 1, "total": 5.0}
    #   },
    #   "2026-03-15T10:00:00Z": {...},
    #   "2026-04-01T10:00:00Z": {...}
    # }
```

**Semantics:**
- Replay streams events from `since` forward once.
- Maintains a sorted cursor across the declared `extract_at` timestamps.
- Just before applying the first event with `event.ts > extract_at[i]`, snapshots the computed feature map for every key in scope into `extracted_history[extract_at[i]]`.
- After `LOG_FETCH END`, snapshots any trailing thresholds (no event crossed them) against end-of-replay state — so `extract_at` values "in the future" relative to the log end return the final state.

**Under the hood:** exposed as `GET /extracts` on the fork; the Python wrapper is a thin one-shot fetch after catchup. CLI equivalent: `beava fork --extract-at T1,T2,T3 ...`.

**Scope:** extractions honour the same `--keys` / `--key-prefix` filter the fork uses. Keys outside scope are not captured. Keys in scope with no events yet at `extract_at[i]` are skipped (consistent with "missing key → None" elsewhere in the API).

**Memory:** `N extractions × K keys × F features`. For the typical scientist workflow (N≤10, K≤100, F<20) this is trivial. Scope aggressively if you need hundreds of checkpoints × thousands of keys.

---

## Fraud demo — end-to-end flow

`benchmark/fraud-pipeline/fraud_demo.py` drives a realistic mixed stream against
a local Beava server: ~50 eps of Zipfian benign traffic for 60s, with four fraud
archetypes injected at scheduled moments against dedicated user IDs. It prints
the wall-clock timestamp of each burst so the scientist can snapshot features at
exactly the moment each fraud fired.

```bash
# Terminal 1 — local "prod"
export BEAVA_ADMIN_TOKEN=dev-admin-token
./target/release/beava serve --http-port 6400 --tcp-port 6401 \
                             --data-dir /tmp/beava-prod

# Terminal 2 — generator
python3 benchmark/fraud-pipeline/fraud_demo.py
# ...
# FRAUD_TIMESTAMPS — paste into bv.fork(extract_at=[...])
# [
#   {"archetype": "card_testing",       "user_id": "user_fraud_001", "fired_at": "2026-04-15T17:02:15.312Z"},
#   {"archetype": "velocity_burst",     "user_id": "user_fraud_002", "fired_at": "2026-04-15T17:02:25.488Z"},
#   {"archetype": "geo_hop",            "user_id": "user_fraud_003", "fired_at": "2026-04-15T17:02:40.611Z"},
#   {"archetype": "high_value_anomaly", "user_id": "user_fraud_004", "fired_at": "2026-04-15T17:02:50.702Z"}
# ]
```

Archetypes injected:

| Archetype | User | Signal the pipeline should surface |
|---|---|---|
| `card_testing` | `user_fraud_001` | `failed_count_30m` spike, `merchant_diversity_1h ≈ 1.0`, `unique_merchants_1h` ≫ avg |
| `velocity_burst` | `user_fraud_002` | `velocity_spike` ≫ 1, `tx_count_1h` ≫ 24h-normalized rate |
| `geo_hop` | `user_fraud_003` | `unique_countries_24h > 3`, `country_hop_flag = True` |
| `high_value_anomaly` | `user_fraud_004` | `amount_vs_avg` ≫ 1, `high_value_ratio` ≫ 1, `tx_stddev_24h` jumps |

Then — the scientist's ML-training-style snapshot:

```python
import json, sys
sys.path.insert(0, "benchmark/fraud-pipeline")
import bench_fraud as bf
import beava as bv

# Captured by fraud_demo.py stdout.
fraud_events = json.loads(open("/tmp/fraud_timestamps.json").read())
extract_at   = [f["fired_at"]  for f in fraud_events]
fraud_keys   = [f["user_id"]   for f in fraud_events]

with bv.fork(
    remote="127.0.0.1:6401",                  # TCP port (HTTP is 6400)
    streams=[bf.RawTransactions],
    keys=fraud_keys,
    since="2026-04-15T00:00:00Z",
    token="dev-admin-token",
    pipelines=[
        bf.UserTransactions, bf.UserFailedTxns,
        bf.MerchantActivity, bf.DeviceActivity, bf.IPActivity,
    ],
    extract_at=extract_at,
) as fork:
    history = fork.extract_history()
    for fe in fraud_events:
        snap = history[fe["fired_at"]].get(fe["user_id"], {})
        print(fe["archetype"], "→",
              {k: snap[k] for k in
               ("tx_count_1h", "velocity_spike", "unique_merchants_1h",
                "merchant_diversity_1h", "failed_count_30m",
                "unique_countries_24h", "country_hop_flag",
                "amount_vs_avg", "high_value_ratio")
               if k in snap})
```

Each row is the **feature vector a fraud-detection model would see at decision
time** — exactly what you'd want as labels for offline training.

---

## Throughput benchmark — simple vs complex pipelines

`benchmark/fraud-pipeline/run_bench.sh` auto-sizes to the host, runs the
**simple** pipeline (1 table / 2 features) and the **complex** pipeline (5
tables / ~40 features, HLL `count_distinct`, `stddev`, multi-window, `filter`)
against a fresh server between runs, and prints a sample feature vector +
memory footprint.

Key design points:
- **Server** runs with `BEAVA_WORKER_THREADS = host CPU count` (all cores).
  DashMap shard count is DashMap's default (`num_cpus × 4`, power-of-2), so
  the server auto-sizes to the host.
- **Clients** default to `CPU count` independent `python3` OS processes
  spawned via shell `&` — not `multiprocessing.Pool`. Each runs in its own
  interpreter with no shared GIL and no fork-pool scheduling overhead.
- Each client writes a JSON result line to stdout including per-phase
  timings; the shell aggregates wall-clock EPS **and** prints an averaged
  runtime profile.

```bash
./benchmark/fraud-pipeline/run_bench.sh                # defaults
EVENTS=1000000 ./benchmark/fraud-pipeline/run_bench.sh # bigger run
CLIENTS=16 ./benchmark/fraud-pipeline/run_bench.sh     # override fan-out
```

Sample output on a 10-core M-series Mac (200k events, 10 clients, 10 server threads):

```
==> Host CPUs: 10  |  server threads: 10  |  client procs: 10  |  events: 200000

=== SIMPLE pipeline benchmark ===
  [client-0..9]  each 20,000 events in 0.36-0.40s
  Wall time:  0.60s
  Aggregate:  331,313 events/sec   (3.0 µs/event)

  Runtime profile (avg across clients):
    register   :   5.5 ms
    gen events :  76.3 ms  (client-side)
    push loop  : 187.2 ms
    flush      : 100.0 ms
    batch p50 / p95 / p99:  4.94 / 31.92 / 31.92 ms

  Sample features (key=user_000001):
    tx_count_1h: 30,906    tx_sum_1h: 3,133,529.41
  Memory: 11.1 MB across 8,078 entities (~1.4 KB/entity)

=== COMPLEX pipeline benchmark ===
  [client-0..9]  each 20,000 events in ~1.9s
  Wall time:  2.13s
  Aggregate:  93,956 events/sec    (10.6 µs/event)

  Runtime profile (avg across clients):
    register   :    6.7 ms
    gen events :   79.5 ms
    push loop  : 1197.6 ms
    flush      :  507.0 ms
    batch p50 / p95 / p99:  4.43 / 484.65 / 484.65 ms

  Sample features (key=user_000001):
    tx_count_30m/1h/24h/7d:  30,906
    tx_sum_1h:               3,133,529.41
    tx_avg_24h:              101.39
    tx_max_24h:              14,112.44
    tx_stddev_24h:           295.78
    unique_merchants_1h:     ~1,678 (HLL estimate)
    unique_devices_24h:      ~2,747 (HLL estimate)
    unique_countries_24h:    10
  Memory: 4.8 GB across 21,793 entities (~232 KB/entity)
```

The ~3.5× throughput gap and ~160× per-entity memory gap come from the HLL
sketches, stddev moment state, and per-window ring buffers in the complex
pipeline. The push-loop + flush dominance tells you where to invest: batch
tail latency (p99 ~485 ms on complex) is server-side aggregate work, not
client-side overhead.

---

## What runs where

| Step | Where |
|---|---|
| `OP_LOG_FETCH{from_ts, scope}` | Prod → replica: streams raw events since `T` matching scope |
| Event ingest | Replica server applies events through its **locally-registered** pipelines |
| Snapshot on disk | Replica persists per-stream log + snapshot locally |
| `OP_SUBSCRIBE{scope}` | After LOG_FETCH hits tail, replica stays subscribed for live events |
| `bv.Client.get()` / `/debug/...` | Queries the replica's local state |

`--replica-block-until-catchup=true` (default) gates the local HTTP/TCP listeners until historical catchup finishes. Scientist queries never see mid-backfill inconsistent state.

---

## Catches & gotchas

### 1. Historical pipelines must be registered **before** catchup starts
If the scientist registers a pipeline AFTER LOG_FETCH has completed, historical events have already been processed through whatever pipelines WERE registered at that moment. Register **first** via `--pipeline-file` or by passing `pipelines=[...]` to `bv.fork()`. Post-catchup registration only sees new events going forward.

Workaround for late registration: stop the fork, re-run with the new pipeline file. The replica's local data dir is ephemeral; re-forking re-backfills.

### 2. Admin token is shared with prod
The token that authenticates `OP_LOG_FETCH` + `OP_SUBSCRIBE` is the production admin token. That means anyone with fork access has **full admin rights on prod** (including the ability to issue PUSH). Locked as-is for v0. Future: a scoped "replica-read" token class.

Mitigation: rotate or issue short-lived admin tokens per scientist session. Never commit tokens to scripts.

### 3. Local PUSH is rejected
`beava fork` sets `replica_mode=true`. Any `app.push(stream, event)` to the replica port returns `BeavaError::Protocol("replica mode: local PUSH disabled")`. This is intentional — the replica is read-only from prod.

If the scientist wants to push synthetic test events, they have to run an unrelated local Beava instance (no `--replica-from`) and point their client there.

### 4. Event ordering is not deterministic across prod producers
Prod may have ingested events from multiple connections concurrently. Their log order is the order the prod server's append picked them up — not the order they were "emitted". Beava's correctness model (event-time + watermark) handles this: operators bucket by `event.timestamp`, not by log arrival. Scientists whose pipelines care about strict ordering should:
- Use `@bv.stream` streams with explicit `_event_time` in payloads.
- Design operators with commutative aggregates where possible.

### 5. Two-port binding (HTTP + TCP)
`beava fork --local-port 7400` actually binds TWO ports:
- HTTP on `7400`
- TCP on `7401` (next port up)

This is because the server's HTTP and TCP listeners can't share a bind. The Python `bv.fork()` wrapper handles this automatically; if you use the CLI directly, plan for two contiguous ports per fork instance.

### 6. Snapshot is not bootstrapped
The MVP design (per user directive) skips snapshot-seeding: the replica starts empty and replays all events from `--since T`. If `T` is a long time ago, backfill time grows linearly with the total event count in scope. For very long history windows, add bounded `--replica-keys` scope to keep the replayed volume manageable.

Future: an optional snapshot seed would accelerate backfill for large time windows. Not in v0.

### 7. Replica dies → scientist re-forks
No resume across restarts. If `beava fork` crashes or the laptop closes, the next `beava fork` invocation starts from `--since T` again. Phase 32 (stretch) will persist `last_applied_timestamp` so restarts resume from the last applied event.

### 8. Scope can't be changed mid-session
`--streams`, `--keys`, `--key-prefix` are boot-time args. Want different scope? Stop and re-fork. The Python `bv.fork()` context manager makes this a 2-line edit.

### 9. Subscriber backpressure on slow consumers
Prod's `SubscriberRegistry` drops the replica if the replica can't keep up with live events (10k-event bounded queue). The replica observes this as a socket EOF and shuts down with `StopReason::ServerDropped{at_timestamp}`. Manual recovery: re-fork with `--since {at_timestamp}`.

Symptom: scientist's queries stop returning fresh data; fork process exited non-zero. Check stderr logs.

### 10. The fork is not a "source of truth"
Data scientists should treat fork output as **exploratory**, not production. It's scoped to their declared subset of keys, computed by their own pipelines, and may be stale if the subscriber was dropped. For production-grade feature values, query prod directly.

### 11. `/debug/ready` is the sync point
If you're scripting around the fork, poll `GET http://127.0.0.1:<port>/debug/ready` and wait for 200 before firing queries. The HTTP listener doesn't bind until catchup completes under the default `--replica-block-until-catchup=true`.

### 12. Python SDK must match fork's Beava version
If `pip install beava` gives a newer SDK than the fork binary, you may hit wire-protocol mismatches. Pin both versions to the same release; both are versioned in the same repo.

### 13. Replay does NOT apply prod's late-event gate (yet)

Prod's `handle_push_batch` drops events where `event_time < watermark` (per-stream gate, Phase 24 late-event handling). The dropped events are still **logged** — they just don't reach operator state.

The replica's `replica_ingest` path **does not apply this gate**. It processes every event LOG_FETCH returns — including ones prod late-dropped. Consequences:

| Operator kind | Behavior |
|---|---|
| Commutative aggregates (count, sum, avg, HLL, top-k) | Replica can be ≥ prod by however many late events existed in the window. Usually tiny. |
| Order-sensitive ops (first, last, ema, lag) | Can produce different values than prod if the log contains out-of-order arrivals. |

**Not a data-loss issue** — the replica is MORE complete than prod. But it IS a parity issue if you're training a model against prod-matched features. A dedicated follow-up (Phase 45 candidate) adds a watermark gate to `replica_ingest` and bumps the replica-side `late_drops` counter, giving bit-for-bit parity with prod for any operator semantics.

Edge case: starting replay mid-history (`--since T_MID`) means the replica's watermark starts at zero. Events near the T_MID boundary that prod late-dropped will be applied by the replica until its watermark catches up to prod's T_MID watermark. For `--since` values near or before the earliest event (i.e. "replay from the beginning"), this edge vanishes.

### 14. `extract_at` semantics relative to watermark

Because the replica doesn't late-drop, snapshots at `extract_at[i]` reflect "every event in the log with `ts <= extract_at[i]` has been applied" — including any events prod considered late. For commutative aggregates this matches what the scientist expects intuitively ("give me the full count as of T"). For order-sensitive ops the same caveat as #13 applies.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `failed to connect to 127.0.0.1:7400` | Fork not ready or wrong port | Poll `/debug/ready`; verify `--local-port` |
| `OutOfScopeError` on `.get(key=X)` | X not in declared `--keys` | Add to scope and re-fork |
| `BeavaError::Protocol("replica mode: local PUSH disabled")` | PUSH attempt against replica | Don't push to the fork — it's read-only |
| Backfill hangs for minutes | Large time window × high event volume | Narrow scope (fewer keys/streams or more recent `--since`) |
| `SubscriberDroppedError` | Replica queue overflowed | Re-fork; optionally increase prod's `beava_replica_subscriber_backpressure_limit` |
| Scientist's pipeline not returning expected values | Pipeline was registered AFTER catchup | Register via `--pipeline-file` at boot, not post-catchup |

---

## Ops checklist (production side)

- [ ] Prod server has `OP_SNAPSHOT_FETCH` (0x12), `OP_LOG_FETCH` (0x13), `OP_SUBSCRIBE` (0x11) — all present as of Phase 35.
- [ ] Admin token provisioned and accessible to the scientist.
- [ ] `beava_replica_subscriptions_active`, `beava_replica_events_pushed_total`, `beava_replica_subscribers_dropped_total` metrics scraped — gives ops visibility into how many forks are active.
- [ ] Document per-team / per-environment fork conventions: which remote to target, token rotation, whether laptops are allowed to fork directly or only jumphosts.

---

## References

- `.planning/phases/35-op-log-fetch/35-01-SUMMARY.md` — OP_LOG_FETCH wire protocol.
- `.planning/phases/36-replica-server-boot/36-01-SUMMARY.md` — replica-mode server boot.
- `.planning/phases/37-beava-fork-e2e/37-01-SUMMARY.md` — `beava fork` CLI + E2E test.
- `.planning/phases/39-python-fork-api/39-01-SUMMARY.md` — `bv.fork()` Python wrapper.
- `docs/protocol.md` — wire protocol overview.
- `docs/python-sdk.md` — existing Python client (non-fork) usage.
