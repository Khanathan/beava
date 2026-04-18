# Feature Landscape — Beava v1.2 TPC + Full Key-Shard (User-Facing Surfaces)

**Domain:** Thread-Per-Core runtime + full key-shard — user-facing UX of the TPC milestone
**Researched:** 2026-04-18
**Scope:** User-facing surfaces only. Internals covered by TPC-SHARD-DESIGN.md. Runtime choices covered by TPC-RESEARCH.md.

---

## 1. Python SDK Shape — `@bv.stream(shard_key=...)` Details

### 1.1 Current decorator signature (today)

Source: `python/beava/_stream.py` lines 329–338.

```python
# TODAY — stream() signature
def stream(
    cls: type | FunctionType | None = None,
    *,
    history_ttl: str | None = None,
    watermark_lateness: str | None = None,
):
    ...
```

`StreamSource.__init__` (lines 94–108) accepts `name`, `schema`, `history_ttl`, `watermark_lateness`. No `shard_key` parameter exists anywhere.

### 1.2 After: proposed v1.2 signature

```python
# v1.2 — stream() signature
def stream(
    cls: type | FunctionType | None = None,
    *,
    history_ttl: str | None = None,
    watermark_lateness: str | None = None,
    shard_key: str | tuple[str, ...] | None = None,  # NEW
):
    ...
```

`StreamSource.__init__` gains `shard_key: str | tuple[str, ...] | None = None`.

### 1.3 Before/After: source stream declaration

```python
# BEFORE (v1.0-v1.1)
@bv.stream
class Transactions:
    user_id: str
    amount: float
    _event_time: int

# AFTER — explicit shard_key (recommended when joins are used)
@bv.stream(shard_key="user_id")
class Transactions:
    user_id: str
    amount: float
    _event_time: int

# AFTER — multi-field tuple key
@bv.stream(shard_key=("region", "user_id"))
class Transactions:
    region: str
    user_id: str
    amount: float
    _event_time: int

# AFTER — omitted shard_key (fallback behavior, see below)
@bv.stream
class Transactions:          # identical to today; no behavioral change at N=1
    user_id: str
    amount: float
    _event_time: int
```

### 1.4 Fallback when shard_key is omitted: primary-key inference

**How the "primary key field" is determined today:**

`extract_schema()` in `python/beava/_schema_v0.py` (lines 47–137) builds an ordered `dict[str, FieldSpec]` from class annotations in **declaration order**, skipping fields whose names start with `_`. There is no explicit "primary key" marker — the concept of a primary key only exists at the server side through the `key_field` parameter on `@bv.table` (e.g., `@bv.table(key="user_id")`). Streams themselves have no declared primary key field today.

**Decision required (two options):**

**Option A — fallback to first non-`_` field (simple, potentially wrong):**
```python
# @bv.stream with no shard_key -> infer shard key = first declared field
@bv.stream
class Transactions:
    user_id: str    # <- inferred shard_key at N>1
    amount: float
```
Risk: a user with `merchant_id` as the first field gets sharding on the wrong key without knowing it. Surprising at N>1.

**Option B — require explicit shard_key for N>1; warn at N>1 if omitted (recommended):**
```python
# At N=1: runs correctly with no shard_key (current behavior preserved)
# At N>1: server emits a ShardKeyMissingWarning on /debug/warnings at register time
#          and routes all events for this stream to shard 0 (safe, suboptimal)
@bv.stream
class Transactions:
    user_id: str
```
Migration: existing users get correct N=1 behavior unchanged. Users opting into N>1 see a warning in `/debug/warnings` and `beava suggest-config` output nudging them to add `shard_key=`.

**Recommendation: Option B.** Less surprising. The warning is actionable. Users who never set `BEAVA_SHARDS>1` are unaffected.

### 1.5 Tuple multi-field key rendering

How `shard_key=("region", "user_id")` appears in user-facing surfaces:

**In error messages:**
```
stream 'Transactions' shard_key=('region', 'user_id')
```
Use the Python repr of the tuple literal — familiar to Python users, unambiguous.

**In `describe()` output (stream introspection):**
```python
stream.describe()
# Returns:
{
  "name": "Transactions",
  "kind": "stream",
  "shard_key": ["region", "user_id"],   # JSON array (not Python tuple)
  "schema": {...}
}
```
JSON arrays are cleaner than JSON strings with parentheses. The `describe()` method on `StreamSource` (lines 110–119 of `_stream.py`) currently returns `key=None`; it gains a `shard_key` field.

**In `GET /streams/{name}` HTTP response:**
```json
{
  "name": "Transactions",
  "shard_key": ["region", "user_id"],
  "features": [...]
}
```

**In docs:** always shown as `shard_key=("region", "user_id")` with the Python tuple syntax.

### 1.6 Join-time shard_key mismatch error

**Trigger:** Two streams participating in a join declare different shard keys. Caught at `app.register(...)` time (server-side register validation, Wave 3).

**Proposed error message:**

```
BeavaError: join shard_key mismatch
  stream 'Transactions' shard_key='user_id'
  stream 'Refunds'      shard_key='merchant_id'

Joined streams must be co-located on the same shard key. Fix:
  @bv.stream(shard_key="user_id")
  class Refunds: ...

Or, if 'merchant_id' is the correct join key, update Transactions:
  @bv.stream(shard_key="merchant_id")
  class Transactions: ...
```

The error:
- Names both streams and both shard keys (not just "mismatch").
- Shows the exact decorator fix.
- Suggests which stream to change based on the join's `on=` field (the server knows the join key at register time, so it can pick the more actionable suggestion).
- Raised as `BeavaError` with a new `JoinShardKeyMismatch` variant (see section 4).

### 1.7 Migration compatibility for existing Python SDK users

Existing users have no `shard_key`. Under v1.2:

- `BEAVA_SHARDS=1` (the v1.2 initial default in debug builds, and in release builds until the user opts in): zero behavior change. Existing pipelines are byte-compatible.
- `BEAVA_SHARDS>1`: streams without `shard_key` fall back to shard 0 for all events (safe; correct; suboptimal). Server emits `ShardKeyMissingWarning` on `/debug/warnings`. `beava suggest-config` outputs a recommendation.
- **No flag day.** Existing SDK users keep working. They discover `shard_key` via the warning when they opt into N>1.

---

## 2. CLI / Env-Var Surface

### 2.1 Current env-var inventory (today)

From `src/main.rs` lines 137–148 and scattered `std::env::var` calls:

| Env var | Current meaning |
|---|---|
| `BEAVA_WORKER_THREADS` | Tokio multi-thread worker count (default 4) |
| `BEAVA_HTTP_PORT` | HTTP port (default 6900 / 6401) |
| `BEAVA_TCP_PORT` | TCP port (default 6400) |
| `BEAVA_ADMIN_TOKEN` | Auth token for admin endpoints |
| `BEAVA_MEMORY_LIMIT_MB` | Soft memory warning threshold |
| `BEAVA_EVENT_LOG_MAX_BYTES` | WAL cap |
| `BEAVA_ENTITIES_SHARDS` | DashMap internal shard count (internal tuning knob, not user-facing in docs; `src/state/store.rs` line 256) |
| `BEAVA_REPLICA_TOKEN` | Token for fork/replica auth |

### 2.2 New env vars for v1.2

| Env var | Default | Meaning |
|---|---|---|
| `BEAVA_SHARDS` | Release: `num_cpus::get_physical()`, Debug: `1` | Number of TPC shards. Always overrides CLI. |

Note: `BEAVA_ENTITIES_SHARDS` (the DashMap internal shard count) is **not** the same as `BEAVA_SHARDS`. The DashMap env var is a legacy internal knob that disappears when DashMap is replaced by per-shard HashMaps in Wave 1. It should be deprecated in docs when Wave 1 ships.

### 2.3 `BEAVA_SHARDS` vs CLI `--shards`: precedence

**Decision:** env var always wins over CLI flag, consistent with every other `BEAVA_*` env override. CLI flag is a convenience alias for the env var. This matches `BEAVA_WORKER_THREADS` / `BEAVA_HTTP_PORT` behavior today.

```
BEAVA_SHARDS=4 beava serve --shards 8
# -> 4 shards (env wins)
```

The `--shards` CLI flag sets `BEAVA_SHARDS` in-process if not already set by the environment, so the ordering is:

1. `BEAVA_SHARDS` from the environment (highest priority)
2. `--shards N` CLI flag
3. Compiled default (`num_cpus::get_physical()` in release; `1` in debug)

### 2.4 `beava reshard` — Wave 4 re-sharding tool

**Purpose:** Migrate an existing data directory from N=1 layout (`data/`) to N=K layout (`data/shard-0/`, ..., `data/shard-K-1/`). One-time offline operation.

**Proposed flag set:**

```
beava reshard \
  --from 1 \            # current shard count (must match the stored layout)
  --to 8 \              # target shard count
  --data-dir ./data \   # source data directory (default: BEAVA_DATA_DIR or ./data)
  --out-dir ./data-resharded  # output directory (separate path; atomic rename when done)
  [--dry-run]           # validate layout + estimate output size without writing
  [--progress]          # print per-shard progress lines (useful for large state)
```

**Safety design:**
- Output to a **separate directory** (`--out-dir`), not in-place. The operator does: run reshard, validate, swap dirs, restart. No atomic-in-place rewrite; too risky at 4+ GB state sizes.
- `--dry-run` scans the source log, computes new shard assignments, prints a per-shard key count estimate. Does not write.
- On completion, prints: `reshard complete: 1 -> 8 shards in ./data-resharded; review then swap with ./data and restart`.
- If `--from` does not match the actual shard layout on disk, exits with an error before writing anything.

**No `--reshard-from upstream-N` flag on `beava fork`** — design doc Q4 resolved this: fork always re-hashes on ingest; upstream shard count is irrelevant. No CLI surface needed.

### 2.5 `beava fork` — shard count for downstream replica

**Today's `beava fork` flags** (from `src/main.rs` lines 192–286):
```
beava fork --remote HOST:PORT --streams s1,s2 [--since T] [--keys k1,k2]
           [--key-prefix PREFIX] [--token TOKEN] [--local-port 7400]
           [--pipeline-file PATH] [--extract-at T1,T2,...]
```

**v1.2 addition:** no `--shards` flag on `fork`. The downstream replica's shard count comes from `BEAVA_SHARDS` in the scientist's environment (default: 1 in debug builds, which is the typical dev laptop usage). Fork always re-hashes on ingest by its own `downstream_N`. This is silent and correct by the design doc Q4 resolution.

**What scientists see:** nothing changes. Fork works as before. The startup banner gains a line:

```
beava fork — remote=prod:6400 scope=['transactions'] since_ms=0 -> http://localhost:7400 (tcp :7401)
shards: 1 (BEAVA_SHARDS not set; using debug default)
```

### 2.6 `beava suggest-config` — shard sizing section

**Today:** `src/engine/recommend.rs` + `src/bin/beava_suggest_config.rs` emit config recommendations through the signals bus to `/debug/config-recommendations`.

**v1.2 additions to `suggest-config` output:**

```
# beava suggest-config — 3 recommendation(s)

BEAVA_SHARDS=10
  current: 1 (debug default)
  suggested: 10 (num_cpus::get_physical() on this host)
  reason: Running N=1 on a 10-core host leaves 9 cores idle for ingest.
          Set BEAVA_SHARDS=num_cpus to unlock thread-per-core throughput.

BEAVA_SHARDS tuning: shard_probe reports cross_shard_fraction=62%
  current: BEAVA_SHARDS=8
  action: Your workload has high cross-shard key scatter (62%; threshold 40%).
          TPC throughput gains will be limited. Consider declaring shard_key=
          on your streams to improve locality, or reduce BEAVA_SHARDS.
  docs: docs/operations.md#shard-sizing
```

The `ShardImbalanceWarning` (section 4) feeds into `/debug/warnings` as `category=performance`. `suggest-config` surfaces it as an action item.

---

## 3. HTTP / Debug Surfaces

### 3.1 Existing debug endpoints (today)

From `src/server/http.rs` lines 1563–1583, the current admin router includes:
```
GET /debug/key/{key}
GET /debug/streams/{name}
GET /debug/memory
GET /debug/backfill
GET /debug/config-recommendations
GET /debug/warnings
GET /debug/topology
GET /debug/throughput
GET /debug/latency
GET /debug/shard_probe          <- already exists (Phase 14)
```

`/debug/shard_probe` already exists and is the architectural go/no-go gate. It is **not** a new endpoint; v1.2 should surface its `cross_shard_fraction` output in `/debug/shards` for convenience.

### 3.2 New: `GET /debug/shards`

**Purpose:** Per-shard operational state snapshot. Admin-gated (same `require_loopback_or_token` middleware as other `/debug/*` endpoints).

**Proposed response shape:**

```json
{
  "generated_at": "2026-04-18T12:00:00Z",
  "n_shards": 8,
  "shards": [
    {
      "shard": 0,
      "reactor_utilization": 0.73,
      "inbox_depth": 0,
      "events_accepted_total": 1482931,
      "events_dropped_total": 0,
      "keys_owned": 12403,
      "watermark_lag_seconds": 0.12,
      "status": "ok"
    },
    {
      "shard": 3,
      "reactor_utilization": 0.97,
      "inbox_depth": 4821,
      "events_accepted_total": 3901002,
      "events_dropped_total": 0,
      "keys_owned": 38200,
      "watermark_lag_seconds": 2.41,
      "status": "hot"
    }
  ],
  "hot_shards": [3],
  "imbalance_ratio": 3.08,
  "cross_shard_fraction": 0.18
}
```

Fields:
- `reactor_utilization` — fraction of last 1-second window the shard was not idle (primary shard health metric, per design doc Q6 and Scylla/Redpanda precedent).
- `inbox_depth` — SPSC queue backlog; non-zero steady state means the shard is falling behind.
- `keys_owned` — distinct keys currently routed to this shard; exposes hot-shard imbalance.
- `watermark_lag_seconds` — per-shard event-time lag behind wall clock.
- `status` — `"ok"` | `"hot"` | `"recovering"` | `"degraded"`. `"hot"` triggers when `reactor_utilization > 0.85` or `inbox_depth > 1000` (thresholds configurable via env or suggest-config).
- `hot_shards` — list of shard IDs in `"hot"` status.
- `imbalance_ratio` — `max(keys_owned) / mean(keys_owned)`; above 2.0 triggers `ShardImbalanceWarning`.
- `cross_shard_fraction` — from shard_probe; also surfaced here for convenience (same data source as `/debug/shard_probe`).

### 3.3 `/health` vs `/debug/ready` — behavior during shard recovery

**Today (`src/server/http.rs` lines 35–51):**
- `/health` — always returns `{"status": "ok"}` if reachable. No state awareness.
- `/debug/ready` — returns `{"ready": true, "replica_mode": bool}`. The HTTP listener only binds after replica catchup, so reachability = readiness in replica mode.

**v1.2 change:**

`/debug/ready` gains shard-recovery awareness (Wave 4 parallel recovery):

```json
{
  "ready": false,
  "replica_mode": false,
  "shards_recovering": [2, 5, 7],
  "shards_ready": [0, 1, 3, 4, 6],
  "recovery_progress_pct": 62
}
```

Once all shards complete recovery, transitions to:
```json
{
  "ready": true,
  "replica_mode": false,
  "shards_recovering": [],
  "shards_ready": [0, 1, 2, 3, 4, 5, 6, 7],
  "recovery_progress_pct": 100
}
```

The HTTP listener **does not bind** until all shards are ready (matching the current replica behavior). `/health` stays dumb (reachable = alive). `/debug/ready` is the readiness probe — operators should use it in Kubernetes `readinessProbe`.

**Impact on `docs/operations.md`:** The existing note "Snapshot load is synchronous before the listener binds" becomes "Per-shard recovery runs in parallel before the listener binds; use `/debug/ready` as your readiness probe."

### 3.4 `GET /streams` and `GET /streams/{name}` — scatter-gather extra fields

**Today:** `/streams` (routed as `/pipelines` in `src/server/http.rs` line 1564) returns `{"pipelines": ["name1", "name2"]}`. `/pipelines/{name}` returns per-stream feature definitions.

**v1.2 (Wave 3 scatter-gather):**

`GET /streams` gains:
```json
{
  "streams": ["transactions", "refunds"],
  "shards_queried": [0, 1, 2, 3, 4, 5, 6, 7],
  "shards_responded": [0, 1, 2, 3, 4, 5, 6, 7],
  "scatter_latency_us": 312
}
```

`shards_responded` is a list of shard IDs that replied within the scatter-gather timeout. If any shard is degraded, `shards_responded` may be a subset of `shards_queried`, and the response includes a `"partial": true` field. This surfaces incomplete reads without silently returning wrong data.

`GET /streams/{name}` gains per-stream shard metadata:
```json
{
  "name": "transactions",
  "shard_key": "user_id",
  "features": [...],
  "shard_distribution": {
    "keys_by_shard": [12403, 11890, 12100, 38200, 11500, 12003, 11700, 12010]
  }
}
```

---

## 4. Error Taxonomy

### 4.1 Current error hierarchy

`src/error.rs` defines a single flat enum:
```rust
pub enum BeavaError {
    Parse(String),
    Type { field, expected, got },
    Window(String),
    Expression(String),
    Protocol(String),
    NotImplemented(String),
}
```

No structured sub-types for sharding errors.

### 4.2 New variants for v1.2

**Proposed additions to `BeavaError`:**

```rust
/// Streams participating in a join have incompatible shard keys.
/// Raised at register time (Wave 3). Fatal — registration is rejected.
JoinShardKeyMismatch {
    stream_a: String,
    key_a: ShardKeySpec,       // enum: Single(String) | Tuple(Vec<String>)
    stream_b: String,
    key_b: ShardKeySpec,
    join_on: Vec<String>,      // the join's `on=` fields, for the suggestion
},

/// A shard failed to replay its event log during recovery.
/// Fatal — the shard cannot serve events until resolved.
/// Surfaced on /debug/warnings as severity=critical.
ShardRecoveryFailed {
    shard: u16,
    reason: String,
},
```

**Warning signals (not `BeavaError` variants — emitted through the signals bus to `/debug/warnings`):**

```
// Severity: warning. Category: performance.
// Triggered when imbalance_ratio > 2.0 on /debug/shards.
ShardImbalanceWarning {
    hot_shard: u16,
    keys_owned: u64,
    mean_keys: f64,
    imbalance_ratio: f64,
    suggestion: "Declare shard_key= on streams feeding shard N to improve distribution",
}

// Severity: info. Category: config.
// Triggered when a stream is registered without shard_key at N>1.
ShardKeyMissingWarning {
    stream: String,
    n_shards: u16,
    suggestion: "@bv.stream(shard_key=\"<first_field>\") on {stream} will enable balanced sharding",
}
```

### 4.3 Severity classification

| Error / Warning | Level | Behavior |
|---|---|---|
| `JoinShardKeyMismatch` | Fatal | Registration rejected; client gets error response |
| `ShardRecoveryFailed` | Fatal | Shard does not come up; server fails startup |
| `ShardImbalanceWarning` | Warning | Emitted to `/debug/warnings`; server continues |
| `ShardKeyMissingWarning` | Info | Emitted to `/debug/warnings`; server continues |

### 4.4 Python SDK error propagation

`BeavaError` is the Rust server error. The Python SDK's `BeavaError` class (imported in `python/beava/__init__.py` line 25 from `beava._types`) receives the error message string from the server over the TCP protocol. For `JoinShardKeyMismatch`, the protocol error message must be formatted as shown in section 1.6 (actionable, with decorator fix shown).

---

## 5. Documentation Deltas

### 5.1 Pages requiring major updates

**`docs/architecture.md`** — the "Current architecture" ASCII diagram and all sharding references need a full rewrite for TPC. The DashMap / shared state model must be replaced with the per-shard ownership model. Estimate: full section rewrite, not a patch.

**`docs/operations.md`** — currently has no shard-sizing content. Gains:
- New section **Shard Sizing** — how to pick `BEAVA_SHARDS`, rule of thumb (physical CPUs), when to go lower (few keys, dev machines), when N=1 is fine.
- New section **Hot-Shard Diagnosis** — how to read `/debug/shards`, what `reactor_utilization > 0.85` means, what `imbalance_ratio > 2.0` means, how to fix (declare `shard_key=`, reduce N, re-shard).
- New section **Re-sharding** — how to use `beava reshard`, the swap-dir procedure, downtime expectations.
- Update the existing CPU section: "Default worker threads: 4" becomes "TPC shards: `num_cpus::get_physical()` in release builds."
- Update the Recovery section: "7 s for 4.7 GB" becomes "~1.5 s with parallel shard recovery on the same hardware."

**`docs/python-sdk.md`** — gains:
- New section **Shard Key Declaration** — explaining `shard_key=`, when required vs optional, tuple keys.
- Before/after code snippets for single-field and tuple-field `shard_key`.
- A note on migration: "Existing pipelines work unchanged at N=1."

**`docs/faq.md`** — gains two new Q&As:

> **Will my pipeline get slower on N=1?**
> No. N=1 is identical to today's behavior. The TPC scaffolding adds a shard-routing layer that is a no-op at N=1 — all events route to shard 0, which is the existing single-writer engine. The 9-cell benchmark matrix must be within -5% of the v1.1 baseline at N=1 before TPC merges to main.

> **Do I need to re-shard for every deploy?**
> Only if you change `BEAVA_SHARDS`. If you keep the same N across restarts, Beava reads each shard's log independently at startup (no re-sharding). If you change N — e.g., from 1 to 8 for a capacity upgrade — you must run `beava reshard --from 1 --to 8` offline before restarting with the new shard count. `BEAVA_SHARDS` is stored in the snapshot header; a mismatch between the stored count and the configured count is detected at startup and the server refuses to start (rather than silently misrouting events).

### 5.2 New doc page required

**`docs/architecture-tpc.md`** — deep-dive on the TPC architecture. Explicitly mentioned in Wave 5 of TPC-SHARD-DESIGN.md. Content:
- Thread-per-core model, one reactor per pinned shard.
- Shard ownership: `hash(key) mod N` routing.
- SPSC channel handoff (listener to shard).
- Per-shard state, event log, watermark.
- Cross-shard queries (scatter-gather for listing, co-location for joins).
- Recovery flow (parallel, one thread per shard).
- Migration path (N=1 to N>1, reshard tool).
- Benchmark expectations table (from design doc).

### 5.3 Pages that do NOT need TPC updates

- `docs/event-time.md` — TPC does not change event-time semantics.
- `docs/concepts.md` — abstract enough to survive unchanged.
- `docs/getting-started.md` — users start at N=1 (debug default); no mention of shards needed.
- `docs/http-api.md` — wire format unchanged; only new debug endpoints need to be added as an appendix.
- `docs/protocol.md` — TCP binary protocol unchanged.

---

## Conflicts with Locked Design-Doc Choices

No conflicts found between this UX spec and the locked design-doc decisions. Specific alignments verified:

- Section 1.4 fallback behavior (Option B: warn at N>1, route to shard 0) is consistent with design doc section 7 migration compatibility: "Single-shard mode (N_SHARDS=1) must be byte-compatible with current state format."
- Section 2.3 env-var-wins precedence matches design doc Q1: "Env override: `BEAVA_SHARDS=N` always wins."
- Section 2.5 no `--shards` on `beava fork` is consistent with design doc Q4 resolution: "No `--reshard-from upstream-N` CLI flag."
- Section 3.3 HTTP listener does not bind until all shards are ready is consistent with design doc goal 7 (correctness preserved, crash-replay determinism) and the existing replica-mode behavior.
- Section 4 `JoinShardKeyMismatch` as register-time fatal error is consistent with design doc section 3c: "Error out at register time if streams in a join disagree."

---

## Open Questions Deferred to Phase-Level Research

1. **`shard_key=` on function-form `@bv.stream`:** The function-form decorator (used for derivations) inherits the shard key from its upstream streams. If two upstreams have different shard keys, the function-form derivation should also raise `JoinShardKeyMismatch`. This propagation logic is straightforward but needs explicit design at implementation time.

2. **`@bv.table(key=..., shard_key=...)` interaction:** Tables today declare a `key` field for feature read routing. At N>1, the table's read key must be the shard key for point reads to avoid scatter-gather. Whether `shard_key=` on `@bv.table` is a separate parameter or inferred from `key=` needs a decision. Recommendation: infer `shard_key = key` for tables (the key_field drives both entity identity and shard routing). No new parameter needed.

3. **`/debug/shards` auth:** All existing `/debug/*` endpoints are admin-gated. `GET /debug/shards` should follow the same pattern. No `/public/shards` equivalent is proposed — shard internals are operational data, not user-facing.

4. **`beava reshard` — handling in-progress writes during reshard:** The reshard tool is an offline operation. Documentation must make clear that the server must be stopped before running `beava reshard`. If the server is running, the tool should detect the lock file and refuse to proceed.

5. **`BEAVA_ENTITIES_SHARDS` deprecation timing:** The legacy DashMap internal shard count env var (`src/state/store.rs` line 256) conflicts in name with `BEAVA_SHARDS`. It should be renamed to `BEAVA_DASHMAP_SHARDS` or deprecated entirely when Wave 1 (per-shard HashMap) lands, to avoid operator confusion.
