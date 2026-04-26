# Data Scientist Fork Demo — Setup Guide

Show how a data scientist clones a scoped slice of a running production Tally cluster to their laptop, registers their own pipelines, runs them against historical events backfilled from prod, and watches live updates tail in.

Product surface:
- `tally fork` CLI (bash)
- `tl.fork(...)` Python API

---

## What "fork" does

```
             production                            scientist's laptop
 ┌──────────────────────────────┐         ┌──────────────────────────────┐
 │  tally serve (port 6400)     │         │  tally fork  (port 7400)     │
 │  ┌────────────────────────┐  │  LOG    │  ┌────────────────────────┐  │
 │  │ event log (per-stream) ├──┼────────▶│  │ local engine + state   │  │
 │  └────────────────────────┘  │  FETCH  │  │ runs SCIENTIST's       │  │
 │  ┌────────────────────────┐  │         │  │ registered pipelines   │  │
 │  │ ingest + pipelines     │  │  SUB    │  │                        │  │
 │  └────────────────────────┘  │────────▶│  └────────────────────────┘  │
 └──────────────────────────────┘  SCRIBE └──────────────────────────────┘
                                                        ↑
                                                 tl.Client queries
```

**Key properties:**
- Replica pulls **raw CDC events** from prod (not prod's aggregates). The scientist's pipelines — which may be different from prod's — run against those events locally.
- Scoped: only events matching declared streams + keys are transferred.
- Historical backfill (`--since T`) + live tail (`OP_SUBSCRIBE`) in one command.
- Replica is a full Tally server locally. Scientist queries it with the normal `tl.Client` HTTP/TCP API.
- Replica REJECTS local `PUSH` — it's read-only from prod.

---

## Prerequisites

- Production Tally running and reachable (TCP port 6400 by default).
- Admin token to the production server (`TALLY_ADMIN_TOKEN`).
- Scientist's laptop: Linux x86_64, Rust toolchain if building from source, or a pre-built `tally` binary.
- Python 3.10+ with the Tally SDK installed (`pip install -e ./python` from the repo, or `pip install tally` once published).

---

## Path A — one-liner Python (recommended)

```python
import tally as tl

@tl.stream
class Transactions:
    user_id: str
    amount: float

# Scientist's custom pipeline — not what prod computes
def _summary(t: Transactions) -> tl.Table:
    return t.group_by("user_id").agg(
        count=tl.count(window="1h"),
        total=tl.sum("amount", window="1h"),
    )
_summary.__name__ = "txn_summary"
TxnSummary = tl.table(key="user_id")(_summary)

# Spawn a scoped local replica, register the scientist's pipeline, start streaming.
with tl.fork(
    remote="prod.tally.internal:6400",
    streams=[Transactions],
    keys=["u1", "u2", "u3"],           # scope to three users
    since="2026-03-01T00:00:00Z",      # backfill from this wall-clock
    token="prod-admin-token",          # or set TALLY_REPLICA_TOKEN env
    pipelines=[TxnSummary],
) as fork:
    # Fork is now running on localhost:7400 (by default).
    # Historical backfill has completed. Live tail is active.
    print(fork.get(TxnSummary, key="u1"))  # {"count": 3, "total": 60.0}
    print(fork.inspect())                   # {"Transactions": 3}
# On exit, the fork shuts down cleanly.
```

The `with` block handles subprocess lifecycle, port allocation, `/debug/ready` polling, and teardown.

---

## Path B — CLI (for ops-style usage)

```bash
# Hand-author a REGISTER JSON for the scientist's pipeline.
# Or export it via the Python SDK: tl.serialize_pipeline(TxnSummary, "/tmp/my_pipeline.json")
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
tally fork \
  --remote prod.tally.internal:6400 \
  --streams Transactions \
  --keys u1,u2,u3 \
  --since 2026-03-01T00:00:00Z \
  --token $PROD_ADMIN_TOKEN \
  --local-port 7400 \
  --pipeline-file /tmp/my_pipeline.json

# In another terminal — query the replica like any Tally server.
curl http://127.0.0.1:7400/debug/ready
curl -H "Authorization: Bearer $PROD_ADMIN_TOKEN" \
     http://127.0.0.1:7400/debug/key/u1
```

`tally fork` is a thin wrapper around `tally serve --replica-from ...`. Power users can drop to the underlying flags directly.

---

## Path C — historical point-in-time extraction (Phase 44-01)

Scientists frequently need "what did these feature values look like at `T_i`?" for multiple `T_i` in one go — e.g. training a model that needs features as-of each label timestamp. `tl.fork(extract_at=[...])` does this in a single replay:

```python
from datetime import datetime, timezone

t1 = datetime(2026, 3, 5, 10, 0, 0, tzinfo=timezone.utc)
t2 = datetime(2026, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
t3 = datetime(2026, 4, 1, 10, 0, 0, tzinfo=timezone.utc)

with tl.fork(
    remote="prod.tally.internal:6400",
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

**Under the hood:** exposed as `GET /extracts` on the fork; the Python wrapper is a thin one-shot fetch after catchup. CLI equivalent: `tally fork --extract-at T1,T2,T3 ...`.

**Scope:** extractions honour the same `--keys` / `--key-prefix` filter the fork uses. Keys outside scope are not captured. Keys in scope with no events yet at `extract_at[i]` are skipped (consistent with "missing key → None" elsewhere in the API).

**Memory:** `N extractions × K keys × F features`. For the typical scientist workflow (N≤10, K≤100, F<20) this is trivial. Scope aggressively if you need hundreds of checkpoints × thousands of keys.

---

## What runs where

| Step | Where |
|---|---|
| `OP_LOG_FETCH{from_ts, scope}` | Prod → replica: streams raw events since `T` matching scope |
| Event ingest | Replica server applies events through its **locally-registered** pipelines |
| Snapshot on disk | Replica persists per-stream log + snapshot locally |
| `OP_SUBSCRIBE{scope}` | After LOG_FETCH hits tail, replica stays subscribed for live events |
| `tl.Client.get()` / `/debug/...` | Queries the replica's local state |

`--replica-block-until-catchup=true` (default) gates the local HTTP/TCP listeners until historical catchup finishes. Scientist queries never see mid-backfill inconsistent state.

---

## Catches & gotchas

### 1. Historical pipelines must be registered **before** catchup starts
If the scientist registers a pipeline AFTER LOG_FETCH has completed, historical events have already been processed through whatever pipelines WERE registered at that moment. Register **first** via `--pipeline-file` or by passing `pipelines=[...]` to `tl.fork()`. Post-catchup registration only sees new events going forward.

Workaround for late registration: stop the fork, re-run with the new pipeline file. The replica's local data dir is ephemeral; re-forking re-backfills.

### 2. Admin token is shared with prod
The token that authenticates `OP_LOG_FETCH` + `OP_SUBSCRIBE` is the production admin token. That means anyone with fork access has **full admin rights on prod** (including the ability to issue PUSH). Locked as-is for v0. Future: a scoped "replica-read" token class.

Mitigation: rotate or issue short-lived admin tokens per scientist session. Never commit tokens to scripts.

### 3. Local PUSH is rejected
`tally fork` sets `replica_mode=true`. Any `app.push(stream, event)` to the replica port returns `TallyError::Protocol("replica mode: local PUSH disabled")`. This is intentional — the replica is read-only from prod.

If the scientist wants to push synthetic test events, they have to run an unrelated local Tally instance (no `--replica-from`) and point their client there.

### 4. Event ordering is not deterministic across prod producers
Prod may have ingested events from multiple connections concurrently. Their log order is the order the prod server's append picked them up — not the order they were "emitted". Tally's correctness model (event-time + watermark) handles this: operators bucket by `event.timestamp`, not by log arrival. Scientists whose pipelines care about strict ordering should:
- Use `@tl.stream` streams with explicit `_event_time` in payloads.
- Design operators with commutative aggregates where possible.

### 5. Two-port binding (HTTP + TCP)
`tally fork --local-port 7400` actually binds TWO ports:
- HTTP on `7400`
- TCP on `7401` (next port up)

This is because the server's HTTP and TCP listeners can't share a bind. The Python `tl.fork()` wrapper handles this automatically; if you use the CLI directly, plan for two contiguous ports per fork instance.

### 6. Snapshot is not bootstrapped
The MVP design (per user directive) skips snapshot-seeding: the replica starts empty and replays all events from `--since T`. If `T` is a long time ago, backfill time grows linearly with the total event count in scope. For very long history windows, add bounded `--replica-keys` scope to keep the replayed volume manageable.

Future: an optional snapshot seed would accelerate backfill for large time windows. Not in v0.

### 7. Replica dies → scientist re-forks
No resume across restarts. If `tally fork` crashes or the laptop closes, the next `tally fork` invocation starts from `--since T` again. Phase 32 (stretch) will persist `last_applied_timestamp` so restarts resume from the last applied event.

### 8. Scope can't be changed mid-session
`--streams`, `--keys`, `--key-prefix` are boot-time args. Want different scope? Stop and re-fork. The Python `tl.fork()` context manager makes this a 2-line edit.

### 9. Subscriber backpressure on slow consumers
Prod's `SubscriberRegistry` drops the replica if the replica can't keep up with live events (10k-event bounded queue). The replica observes this as a socket EOF and shuts down with `StopReason::ServerDropped{at_timestamp}`. Manual recovery: re-fork with `--since {at_timestamp}`.

Symptom: scientist's queries stop returning fresh data; fork process exited non-zero. Check stderr logs.

### 10. The fork is not a "source of truth"
Data scientists should treat fork output as **exploratory**, not production. It's scoped to their declared subset of keys, computed by their own pipelines, and may be stale if the subscriber was dropped. For production-grade feature values, query prod directly.

### 11. `/debug/ready` is the sync point
If you're scripting around the fork, poll `GET http://127.0.0.1:<port>/debug/ready` and wait for 200 before firing queries. The HTTP listener doesn't bind until catchup completes under the default `--replica-block-until-catchup=true`.

### 12. Python SDK must match fork's Tally version
If `pip install tally` gives a newer SDK than the fork binary, you may hit wire-protocol mismatches. Pin both versions to the same release; both are versioned in the same repo.

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
| `TallyError::Protocol("replica mode: local PUSH disabled")` | PUSH attempt against replica | Don't push to the fork — it's read-only |
| Backfill hangs for minutes | Large time window × high event volume | Narrow scope (fewer keys/streams or more recent `--since`) |
| `SubscriberDroppedError` | Replica queue overflowed | Re-fork; optionally increase prod's `tally_replica_subscriber_backpressure_limit` |
| Scientist's pipeline not returning expected values | Pipeline was registered AFTER catchup | Register via `--pipeline-file` at boot, not post-catchup |

---

## Ops checklist (production side)

- [ ] Prod server has `OP_SNAPSHOT_FETCH` (0x12), `OP_LOG_FETCH` (0x13), `OP_SUBSCRIBE` (0x11) — all present as of Phase 35.
- [ ] Admin token provisioned and accessible to the scientist.
- [ ] `tally_replica_subscriptions_active`, `tally_replica_events_pushed_total`, `tally_replica_subscribers_dropped_total` metrics scraped — gives ops visibility into how many forks are active.
- [ ] Document per-team / per-environment fork conventions: which remote to target, token rotation, whether laptops are allowed to fork directly or only jumphosts.

---

## References

- `.planning/phases/35-op-log-fetch/35-01-SUMMARY.md` — OP_LOG_FETCH wire protocol.
- `.planning/phases/36-replica-server-boot/36-01-SUMMARY.md` — replica-mode server boot.
- `.planning/phases/37-tally-fork-e2e/37-01-SUMMARY.md` — `tally fork` CLI + E2E test.
- `.planning/phases/39-python-fork-api/39-01-SUMMARY.md` — `tl.fork()` Python wrapper.
- `docs/protocol.md` — wire protocol overview.
- `docs/python-sdk.md` — existing Python client (non-fork) usage.
