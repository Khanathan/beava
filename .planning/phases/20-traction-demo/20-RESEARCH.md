# Phase 20: Traction Demo — Research

**Researched:** 2026-04-14
**Domain:** Ship / deploy / public demo — no new core engine work
**Confidence:** HIGH (every recommendation maps to existing Tally code)

## Summary

Every capability this phase needs already exists inside Tally. The debug UI (Phase 10/10.1/10.2) ships via `rust-embed` from `src/server/ui/`, `/debug/latency` returns real p50/p95/p99 JSON, `/metrics` exposes counters, OP_PUSH_BATCH + DashMap hit 1.1M eps on 8 clients (MIG-03). The phase is 90% packaging, 10% new code.

**Primary recommendation:** Synthesize 30 days × ~1.1M events/day ≈ **30M events** with the existing `bench.py` fraud generator; replay with `push_many(batch_size=1000)` × 8 client processes targeting the 1.1M eps baseline — that produces a ~27 s headline. Extend the existing Axum router in `src/server/http.rs` with three `/public/*` routes that wrap existing handlers in a read-only guard, gate the UI on a `--public-mode` flag that hides write buttons. Deploy single binary on a **$5/mo Hetzner CX22** behind `systemd` + `caddy` for TLS. Total new surface: ~400 lines Rust, ~150 lines Python, one HTML page edit.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **Public URL is read-only:** GET features by key, browse recent events, view aggregate metrics. No public PUSH/SET/MSET/REGISTER. Writes either authenticated (admin), localhost-bound, or driven by internal generator.
- **HTTP only for public surface.** Direct TCP exposure to the internet is out. Web demo talks to an HTTP proxy/gateway that translates read requests to Tally's TCP GET or uses HTTP management API's debug endpoints.
- **Replay uses OP_PUSH_BATCH (Phase 13) and exploits Phase 14 DashMap per-stream locks.** Match or exceed v1.3 baseline 1.1M eps (per `19-05-SUMMARY.md`).
- **Replay script is deterministic** (same input → reproducible timing); scale to a headline-worthy eps figure.
- **Live metrics source = existing `/metrics` (port 6401).** If p99/current-eps missing, extend that endpoint — don't invent a new one. Frontend polls on ~2s interval, sparklines ok, no WebSockets required v1.
- **Frontend:** reuse Phase 10/10.1/10.2 debug UI if feasible; otherwise minimal static page. Single-page, zero new infra preferred.
- **Deployment:** single Tally binary + static assets on modest VM (1-2 vCPU), local-disk snapshots, no Docker Compose. 5-day uptime requires Phase 9 snapshot recovery verified end-to-end before launch.
- **Blog lives in `docs/blog/`**; add/update launch post with replay headline, embedded live-metrics widget or screenshot+link, backfill benchmark story.
- **Tests required:** unit (replay parsing, rate limits, metrics shape), integration (E2E small-scale replay in CI), smoke (deployment health check).

### Claude's Discretion
- Exact frontend layout and visual polish.
- Choice of historical data source (given deterministic + sizing constraints).
- Host choice (fly.io / Render / cheap VM / tunneled box) as long as 5-day uptime holds.

### Deferred Ideas (OUT OF SCOPE)
- Public write access / user-submitted events.
- Multi-region, auto-scaling.
- In-browser historical replay scrubber / controls.
- Feature comparison dashboards vs competitors (goes in blog copy, not the app).
</user_constraints>

## Project Constraints (from CLAUDE.md)

- Single-binary, single-threaded core — add no new processes/services.
- In-memory state + periodic snapshot. Demo's 5-day uptime must rely on Phase 9 incremental snapshot recovery; no new durability layer.
- Management API already on port **6401**; public demo must live there or behind a reverse proxy that fronts it. Do not open TCP port 6400 to the internet.
- Python SDK is a thin client; replay script must be Python calling `push_many()`, not a new Rust binary.
- "Zero ops" contract: one deploy command, no orchestration.

## Standard Stack

### Reuse from existing repo (do not add libs)
| Component | Already in repo | Use For |
|-----------|----------------|---------|
| Axum 0.7 + tokio | `src/server/http.rs` (12 routes registered) | New `/public/*` routes |
| `rust-embed` | `src/server/ui/` + `UiAssets` in http.rs:945 | Ship a second `demo.html` via same embed |
| `RollingHistogram` (Phase 10.2) | `src/server/latency.rs` | p99 PUSH latency feeding `/metrics` |
| `ThroughputTracker` EWMA 5s/60s/5m (Phase 10.2 wiring) | `AppState` | "current eps" number |
| `tally` Python SDK `push_many` | `python/tally/` | Replay driver |
| `bench.py` fraud generators | `benchmark/tally-throughput/bench.py` (small/medium/large defs) | Event synthesizer |
| SSD event log (Phase 6, ELOG-01..05) | `src/state/` (via phase 6) | Optional: dump + replay source if we want "real" captures |
| Phase 9 incremental snapshots | wired into timer | Crash-recovery guarantee for 5-day uptime |
| d3 + dagre-d3 + htmx vendored | `src/server/ui/vendor/` | Reuse if embedding widgets; otherwise skip |

### Newly needed (small)
| Library | Version | Purpose | Notes |
|---------|---------|---------|-------|
| `caddy` (binary, not a Rust dep) | v2 latest | TLS + reverse proxy in front of :6401 | `apt install caddy`, 10-line Caddyfile, auto Let's Encrypt |
| `systemd` unit file | — | Auto-restart on panic, start on boot | Standard `/etc/systemd/system/tally.service` |

**Do NOT add:** nginx (caddy is simpler), a new web framework, a JS build step, Docker, WebSocket crate, Prometheus push gateway, any auth library. `systemd` + caddy + existing Axum is the whole stack. `[VERIFIED: repo inspection]`

## Architecture Patterns

### Recommended Layout
```
streamlet/
├── src/server/
│   ├── http.rs              # ADD: /public/features/:key, /public/recent-events, /public/stats
│   │                        # ADD: public_mode flag in AppState; guard /pipelines POST/DELETE, /snapshot POST,
│   │                        #      /debug/key to localhost-only OR reject in public_mode
│   └── ui/
│       ├── index.html       # (existing debug UI, unchanged)
│       └── demo.html        # NEW: ~150 lines HTML/CSS/vanilla JS polling /public/stats
├── benchmark/
│   └── replay/
│       └── replay_30d.py    # NEW: generator + push_many driver + wall-clock reporter
├── deploy/
│   ├── tally.service        # NEW: systemd unit
│   └── Caddyfile            # NEW: reverse proxy + TLS
└── docs/blog/
    └── streaming-shouldnt-require-a-platform-team.md  # EDIT: add headline number + demo link
```

### Pattern: Public-mode guard
Add `pub public_mode: bool` to `AppState`. In `build_router`, split the router:
```rust
// src/server/http.rs (sketch)
pub fn build_router(state: SharedState) -> Router {
    let public = Router::new()
        .route("/public/features/{key}", get(public_features))     // wraps debug_key, strips internal bytes
        .route("/public/recent-events", get(public_recent_events)) // reads tail of event log
        .route("/public/stats", get(public_stats))                 // aggregates /metrics + /debug/latency + uptime
        .route("/metrics", get(metrics_endpoint))                  // safe: counters only
        .route("/health", get(health))
        .route("/", get(demo_index));                              // serves demo.html when public_mode

    let admin = Router::new()
        .route("/pipelines", get(list_pipelines).post(create_pipeline))
        .route("/snapshot", post(trigger_snapshot))
        .route("/debug/{*rest}", get(debug_handlers))
        .layer(middleware::from_fn(require_loopback_or_token));    // NEW: 127.0.0.1 or Bearer token

    public.merge(admin).with_state(state)
}
```
TCP port 6400 never gets exposed. Caddy only proxies :6401. Admin routes additionally bounce non-loopback requests unless `TALLY_ADMIN_TOKEN` header matches. `[ASSUMED]` — middleware shape is standard Axum 0.7 but the exact `from_fn_with_state` signature should be verified against the already-pinned axum version.

### Pattern: Replay driver (Python)
```python
# benchmark/replay/replay_30d.py
import tally as tl
from tally import source, dataset, group_by

rng = random.Random(42)  # deterministic

@source
class RawTxns: pass

@dataset(depends_on=[RawTxns])
class Transactions:
    features = group_by('user_id').agg(
        tx_count_1h=tl.count(window='1h'),
        tx_sum_1h=tl.sum('amount', window='1h'),
        failed_30m=tl.count(window='30m', where="status == 'failed'"),
    )
    failure_rate = tl.derive('failed_30m / tx_count_1h')

def gen_events(n, rng): ...  # reuse bench.py fraud generator, stamp ts across 30 days

def main():
    app = tl.App("localhost:6400"); app.register(RawTxns, Transactions)
    events = gen_events(30_000_000, rng)   # 30M events = ~1M/day fraud-realistic
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as ex:
        chunks = [events[i::8] for i in range(8)]
        for c in chunks: ex.submit(push_chunk, c, batch_size=1000)
    elapsed = time.perf_counter() - t0
    print(f"{len(events):,} events in {elapsed:.2f}s → {len(events)/elapsed:,.0f} eps")
```

### Pattern: Demo page (vanilla, single file)
Do **not** extract components from `app.js`. The Phase 10.1 UI is a d3/dagre DAG + drill-in panel — overkill for public display and its routes (`/debug/topology`, `/debug/throughput`) expose internals. Write ~150 lines: a wordmark, three giant counters (`events_total`, `current_eps`, `p99_us`), a key-lookup box (`fetch('/public/features/' + key)`), a scrolling `<ul>` of last 20 events (`setInterval(poll, 2000)`). Share `app.css` for typography + color tokens.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TLS termination | stunnel, nginx-with-certbot | **caddy v2** | One-line auto-TLS config; zero renewal cronjob |
| Auto-restart on panic | Custom supervisor / while loop | **systemd `Restart=always`** | Handles SIGTERM, backoff, journald logs |
| "Current eps" counter | New rolling counter | **`ThroughputTracker.eps_5s()`** from Phase 10.2 | Already wired, already dedup'd for cascade |
| p99 push latency | New histogram | **`LatencyTracker.command_histograms[PUSH].percentile(99.0)`** | Already exists in `src/server/latency.rs` |
| Event feed | Pub/sub, WebSocket | **Tail of Phase 6 event log** exposed as `/public/recent-events?limit=20` | Read-only, cheap, bounded |
| Historical dataset | NYC taxi / GH archive | **Synthesize from `bench.py` generators** | Deterministic, license-safe, tuned to hit 1.1M eps; public datasets add licensing review and shape drift |
| Web framework | Add Next.js / Svelte | **One HTML file + vanilla JS** | 150 lines, no build step, embedded via existing rust-embed |
| Live-update from blog | Iframe / widget with state | **Server-rendered number at build time + link to live demo** | mkdocs is static; widget complexity not worth it for a blog post |

**Key insight:** Every "real engineering" piece (histograms, throughput EWMA, snapshots, batch decode, TLS) is someone else's job — either existing Tally code or caddy/systemd. New code in this phase is **glue + a replay script + one HTML page**.

## Answers to the 8 Scoping Questions

### Q1 — Historical replay data source
**RESOLVED: (a) Synthesize from `bench.py` generators** with a fixed seed (`rng = random.Random(42)`).

Evidence: `benchmark/tally-throughput/bench.py` (inspected) already defines small/medium/large fraud pipelines and emits structured transactions. The medium pipeline matches the CLAUDE.md canonical fraud example. Size the run at **30M events (≈ 1M/day × 30 days)** — that produces a headline of **≈ 27 seconds at the 1.1M eps baseline**, which is the sub-minute memorable number the context demands.

Rejected:
- (b) Captured internal trace via SSD event log: requires an internal run to have happened for 30 days + log retention tuning + exposure of key space. Speculative at launch.
- (c) Public datasets (NYC taxi, GH archive): adds license review, download infra (GB of data), and schema-drift work (they don't naturally hit the fraud pipeline we advertise). Net negative. `[ASSUMED — no show-stopper license found, but synthesis is strictly cheaper]`.

**Deterministic contract:** seed the RNG, fix the schema version, commit `replay_30d.py` so anyone reproducing on the same Tally version gets within a few % of the advertised eps.

### Q2 — Replay performance strategy
**RESOLVED:** `push_many` with `batch_size=1000` × **8 concurrent Python processes** (not threads — GIL limits single-process encoding). Target = match 1.1M eps baseline.

Evidence from `19-05-SUMMARY.md` and `14-03-SUMMARY.md`:
- Single-client batch medium: **476k eps**.
- 4-client async-batch: **483k eps aggregate**.
- 8-client aggregate @ batch: **~1.1M eps** is the MIG-03 gate (per `19-05-SUMMARY.md`).
- Pure-Python encoding ceiling: **542k eps** per client (key cache + local refs).

Optimal `batch_size`: per Phase 13 summary, `batch_size=1000` is used as the reported benchmark point. Larger batches (5000+) increase tail latency per batch without improving eps. Smaller (100) underutilizes the decode loop. **Lock in 1000.** `[VERIFIED: 19-05-SUMMARY grep above]`

Per-stream lock contention (Phase 14): the fraud pipeline fans out to `user_id` and `merchant_id` streams. DashMap per-stream locks plus bucket-level parking_lot RwLock mean two streams scale independently — confirmed by the 1.1M/8-client result. Generating 8 chunks by `user_id % 8` further reduces per-shard hot keys.

**No bulk-ingest bypass.** CLAUDE.md / roadmap offer no sanctioned path that skips the pipeline engine, and the phase explicitly says "as fast as possible" via OP_PUSH_BATCH, not "invent new code paths." Don't do it.

### Q3 — Read-only public exposure
**RESOLVED: (b) Extend existing HTTP API with `/public/*` + guard admin routes to loopback-or-token.**

Evidence: `src/server/http.rs` already hosts 12 routes on Axum on port 6401. Adding three GET handlers that wrap existing `debug_key` / `metrics` / event-log-tail logic is ~150 LOC. Adding a `require_loopback_or_token` layer over `/pipelines` POST/DELETE, `/snapshot`, `/debug/*` is ~30 LOC. Zero new processes.

Rejected:
- (a) Separate reverse proxy whitelist: still needs the `/public/*` routes to exist; adds a second config file (nginx location blocks) to maintain in lock-step with router changes. Caddy/nginx should only do TLS, not auth logic.
- (c) Separate read-only gateway process: violates single-binary principle from CLAUDE.md.

**Caddy role:** TLS + HSTS + a basic rate-limit (`caddy-ratelimit` plugin) in front of the combined router. Caddy does **not** do auth; the router does.

### Q4 — Frontend reuse
**RESOLVED: Fresh minimal static page (`demo.html`), share `app.css` color tokens only.**

Evidence: `src/server/ui/index.html` (read) loads d3, dagre-d3, htmx — ~200 KB vendor bundle — and `app.js` is a topology DAG drill-in. Adapting it to a read-only public page means deleting ~80% of it and stripping routes (`/debug/topology`, `/debug/throughput`) that expose stream internals we'd rather not publish. Writing a fresh ~150-line vanilla-JS page polling three endpoints is strictly less code.

Ship via same `rust-embed` block in `http.rs:945` — add `demo.html` to the embedded directory, gate serving on `AppState.public_mode`. Binary stays one file.

### Q5 — Metrics exposure
**Current `/metrics` (http.rs:232) exposes:** `tally_keys_total`, `tally_events_total`, `tally_push_latency_seconds` (last observed, NOT p99), `tally_snapshot_duration_seconds`, `tally_memory_bytes`, `tally_snapshots_skipped_total`.

**Missing for demo:** p99 push latency (histogram, not last-observed), current eps, uptime-since-boot.

**Cheapest fix** — do NOT add new counters; **compose existing trackers into a JSON response on `/public/stats`**:
```rust
async fn public_stats(State(s): State<SharedState>) -> Json<Value> {
    let push_hist = &s.latency_tracker.command_histograms[CMD_PUSH];
    let eps_5s = s.throughput.eps_5s();   // Phase 10.2 EWMA
    let uptime = s.started_at.elapsed().as_secs();
    Json(json!({
        "events_total":    s.metrics.lock().events_total,
        "current_eps":     eps_5s,
        "p99_push_us":     push_hist.percentile(99.0),
        "p50_push_us":     push_hist.percentile(50.0),
        "uptime_seconds":  uptime,
        "keys_total":      s.store.entity_count(),
    }))
}
```
`RollingHistogram::percentile` and `command_histograms[PUSH]` already exist (`src/server/latency.rs` grep above). `ThroughputTracker.eps_5s()` exists from Phase 10.2 wiring (`/debug/throughput` uses it). `started_at` needs adding to `AppState` (single `Instant::now()` at boot). `[VERIFIED: latency.rs grep]`

Also extend `/metrics` (Prometheus) with `tally_push_latency_p99_seconds` sourced from the same histogram — one extra `format!` line. Keeps monitoring dashboards honest.

### Q6 — Deployment target for 5-day uptime
**RESOLVED: $5/mo Hetzner CX22** (2 vCPU, 4 GB RAM, 40 GB SSD, Frankfurt or Ashburn) + `systemd` + caddy.

Comparison:
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| fly.io hobby | Free/cheap, auto-TLS | Ephemeral /tmp; persistent volume adds a separate line item; free tier sleeps ("scale to zero") — kills 5-day uptime. | **No** (sleep kills the promise) |
| Render free | TLS, web UI | Free web services spin down after 15 min idle; persistent disk is paid tier only. | **No** |
| Hetzner CX22 $5/mo | Full control, 40 GB SSD (fine for Phase 9 snapshots), Frankfurt/Ashburn low-latency | Need to install caddy + systemd yourself (10 min). | **Yes** |
| DO droplet $6/mo | Same tradeoff as Hetzner; slightly pricier | — | Acceptable fallback |
| Tunneled home box (cloudflared/tailscale) | Free | ISP uptime, power, router reboots; can't commit to 5-day SLA on residential. | **No** |

Snapshot persistence: Phase 9 writes `tally.snapshot.base.NNN` + deltas to a config'd dir. On Hetzner, point at `/var/lib/tally/`; systemd sets `StateDirectory=tally` which auto-creates with correct perms. `[ASSUMED — standard systemd behavior]`

**systemd unit essentials:** `Restart=always`, `RestartSec=5`, `StateDirectory=tally`, `Environment=RUST_LOG=info`, `StandardOutput=journal`. 5-day unattended = `journalctl -u tally --since '5 days ago'` is the whole postmortem story.

### Q7 — Blog integration
**RESOLVED: Static headline number + link to live demo; optionally a tiny async JS snippet that replaces `<span id="live-count">…</span>` with the current `events_total` on page load.**

Evidence: `mkdocs.yml` (read) shows **mkdocs-material**, static site, no server. The existing post (`docs/blog/streaming-shouldnt-require-a-platform-team.md`) is pure markdown. Iframes add jank + CSP complexity. A full widget requires JS build tooling that mkdocs doesn't have.

**Concrete edit to the blog post:**
- Add new section "We ran it for 30 days in 27 seconds" with the wall-clock number bolded.
- Add a screenshot PNG of the demo page saved to `docs/assets/demo.png`.
- Add a callout admonition linking to the live URL.
- Optional: an inline `<script>` that does `fetch('https://demo.tally.dev/public/stats').then(r=>r.json()).then(d=>document.getElementById('live-count').textContent=d.events_total.toLocaleString())` — 6 lines, no build step, CORS allowed via caddy header.

`[VERIFIED: mkdocs.yml — material theme, no JS framework]`

### Q8 — Risks for 5-day unattended run
| Failure mode | Phase-9-era status | Mitigation for demo |
|--------------|---------------------|---------------------|
| Snapshot file growth | Phase 9 bounded by incremental deltas + periodic full snapshot (OPS-04) — full every Nth cycle caps recovery time | Configure full-snapshot-every = 20 cycles; `systemd-tmpfiles` rotates old `.delta` past last full. Alert via `tally_snapshots_skipped_total > 0` in caddy log. |
| TTL memory leak (wrong config) | Per-stream `entity_ttl` (Phase 6) evicts inactive keys | Set `entity_ttl=48h` on demo streams; monitor `tally_keys_total` — if it climbs monotonically past a ceiling, eviction is misconfigured. Pre-flight test: run 24 h locally, confirm flatline. |
| Log file growth | journald rotates by default | `SystemMaxUse=500M` in `/etc/systemd/journald.conf` |
| Panic on protocol edge case | Rust panics currently crash the process | `systemd Restart=always` + Phase 9 recovery on reboot gives sub-10s recovery. Zero public writes eliminates 95% of fuzz surface. |
| Caddy cert renewal | Auto | None — caddy handles it |
| Disk full | 40 GB SSD, 30M events ≈ 2-3 GB snapshots | 40 GB has ~15× headroom. Monitor with `df` in a weekly cron; not critical for 5-day demo. |
| OS reboot (kernel update) | Hetzner Debian unattended-upgrades | Disable `unattended-upgrades` for kernel during demo window; apply after. |

**Monitoring:** caddy access log + `journalctl -u tally -f` is sufficient for a 5-day promotional run. No Prometheus scrape needed (though `/metrics` is there if a reviewer wants to hit it).

## Common Pitfalls

### Pitfall 1: Exposing admin endpoints accidentally
**What goes wrong:** Forget to gate `/pipelines DELETE` behind loopback-or-token; trolls wipe the demo.
**How to avoid:** Middleware attached at router-merge time, not per-route. Integration test that asserts `curl -X DELETE https://demo.tally.dev/pipelines/Transactions` returns 403 **before** the deploy step.

### Pitfall 2: Replay blows past server ingest
**What goes wrong:** `push_many` fire-and-forget with 8 clients can outpace server decode; TCP backpressure → timeouts → skewed eps.
**How to avoid:** Use `push_many` in sync-batch mode (awaits ACK); measured throughput is still in the 480k+/client range per 19-05. Don't chase async-batch for the headline — the summary explicitly says async-batch plateaus at 178k single-client due to server-side.

### Pitfall 3: 30-day window operators on a 27-second replay
**What goes wrong:** Replaying 30 days of events in 27s of wall clock means `window="24h"` aggregations all fall inside one bucket if we use wall-clock timestamps.
**How to avoid:** Replay uses **event timestamps** (Phase 8 backfill semantics — SCHM-03 already shipped). Each synthesized event carries `ts` spread across 30 days; operators bucket on `ts`, not `now()`. This is the existing backfill code path; nothing to invent. `[VERIFIED: Phase 8 success criteria in v2.0-ROADMAP.md line 117]`

### Pitfall 4: CORS
**What goes wrong:** Blog post JS `fetch('https://demo.tally.dev/public/stats')` gets CORS-blocked.
**How to avoid:** `header Access-Control-Allow-Origin https://petrpan26.github.io` in Caddyfile. One line.

### Pitfall 5: Measuring eps wrong
**What goes wrong:** Starting the clock before `register()` completes, or stopping it before the last batch is ACK'd, produces inflated numbers.
**How to avoid:** Copy `bench.py` methodology — `t0` after `app.register()`, barrier on all futures before `time.perf_counter()` end. Print the same fields: total, elapsed, eps, p50/p95/p99 from `/debug/latency`.

## Runtime State Inventory

**Not a rename/refactor phase — omitted.** All new code is additive.

## Code Examples

### Public stats handler
```rust
// src/server/http.rs (new handler)
async fn public_stats(State(state): State<SharedState>) -> Json<serde_json::Value> {
    let push_p99 = state.latency_tracker.command_histograms[CMD_PUSH].percentile(99.0);
    let push_p50 = state.latency_tracker.command_histograms[CMD_PUSH].percentile(50.0);
    let eps = state.throughput.eps_5s();
    let uptime = state.started_at.elapsed().as_secs();
    let m = state.metrics.lock();
    Json(serde_json::json!({
        "events_total":    m.events_total,
        "keys_total":      state.store.entity_count(),
        "current_eps":     eps,
        "p50_push_us":     push_p50,
        "p99_push_us":     push_p99,
        "uptime_seconds":  uptime,
    }))
}
```

### Public recent-events handler
```rust
async fn public_recent_events(
    State(state): State<SharedState>,
    Query(q): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let limit = q.get("limit").and_then(|s| s.parse().ok()).unwrap_or(20).min(100);
    // Tails the Phase 6 SSD event log; returns only stream_name + ts + opaque hash (no payload fields, for privacy)
    let tail = state.event_log.tail(limit);
    Json(serde_json::json!({ "events": tail }))
}
```

### systemd unit
```ini
# /etc/systemd/system/tally.service
[Unit]
Description=Tally demo
After=network.target

[Service]
Type=simple
User=tally
ExecStart=/usr/local/bin/tally --public-mode --admin-token-file /etc/tally/admin.token
StateDirectory=tally
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### Caddyfile
```
demo.tally.dev {
    reverse_proxy 127.0.0.1:6401
    header Access-Control-Allow-Origin "https://petrpan26.github.io"
    header Strict-Transport-Security "max-age=31536000"
    encode zstd gzip
}
```

## Environment Availability

| Dependency | Required By | Available on target VM | Fallback |
|------------|-------------|------------------------|----------|
| Rust toolchain (build) | Binary | Build locally, scp; VM doesn't need toolchain | — |
| caddy v2 | TLS | `apt install caddy` on Debian 12 | nginx + certbot (worse) |
| systemd | Restart policy | Default on Debian | — |
| Python 3.11+ + tally SDK | Replay (runs on VM or laptop) | `apt install python3` + `pip install -e python/` | Run replay from laptop pointing at VM:6400 via SSH tunnel |
| Port 443 open | HTTPS | Hetzner default allows | — |
| Port 6400 open | TCP replay | **Keep closed publicly**; only open to replay-source IP for warmup, then close | SSH tunnel from laptop → `ssh -L 6400:localhost:6400` |

No blocking dependencies.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Axum 0.7 `middleware::from_fn` signature used in sketch matches pinned version | Architecture Patterns | Minor — 10-minute fix against actual Cargo.toml |
| A2 | No public dataset has a strictly simpler license path than synthesis | Q1 | Low — synthesis is still the technical winner on determinism |
| A3 | `ThroughputTracker::eps_5s()` is the exact method name | Q5 | Trivial — method exists per Phase 10.2, name verifiable in 1 min |
| A4 | systemd `StateDirectory=tally` creates `/var/lib/tally/` with tally:tally perms | Q6 / systemd unit | Trivial — documented behavior |
| A5 | Hetzner CX22 Debian 12 image is stable for 5 days with unattended-upgrades disabled | Q6 / Q8 | Low — documented uptime norms |

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `pytest` (Python SDK) + `cargo test` (Rust handlers) |
| Config file | `python/pyproject.toml`, root `Cargo.toml` |
| Quick run | `cargo test -p tally --lib http::tests` + `pytest python/tests/test_replay.py -x` |
| Full suite | `cargo test && pytest python/tests/` |

### Phase Requirements → Test Map
| Req | Behavior | Test Type | Command | Exists? |
|-----|----------|-----------|---------|---------|
| SC-1 | Replay CLI prints total/eps/final-state | unit | `pytest python/tests/test_replay.py::test_cli_output -x` | Wave 0 |
| SC-1 | Replay determinism (same seed → same event count) | unit | `pytest python/tests/test_replay.py::test_deterministic -x` | Wave 0 |
| SC-1 | Replay runs end-to-end on CI-sized data (100k events) | integration | `pytest tests/test_replay_integration.py -x` | Wave 0 |
| SC-2 | `/public/features/:key` returns JSON, no internal bytes | unit | `cargo test http::tests::test_public_features_shape` | Wave 0 |
| SC-2 | Admin routes require loopback/token — public refused | unit | `cargo test http::tests::test_admin_guard_rejects_public` | Wave 0 |
| SC-3 | `/public/stats` returns all 6 fields with correct types | unit | `cargo test http::tests::test_public_stats_schema` | Wave 0 |
| SC-3 | `p99_push_us` is populated after 100 PUSHes | integration | `cargo test http::tests::test_public_stats_p99_live` | Wave 0 |
| SC-4 | Blog post renders with live-count JS fetch | manual smoke | `mkdocs serve` + visual check | N/A (manual) |
| SC-5 | Crash recovery: kill -9 server, restart, state within 1 snapshot cycle | integration | `cargo test snapshot::tests::test_crash_recovery_5day_sim` (existing Phase 9 test) | ✅ Exists |
| SC-5 | Deployment script health-check returns 200 | smoke | `scripts/deploy_smoke.sh` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test -p tally --lib http::tests && pytest python/tests/test_replay.py -x` (< 30 s)
- **Per wave merge:** `cargo test && pytest` (full suite)
- **Phase gate:** Full suite green + manual 5-day staging run kicked off 5 days before launch

### Wave 0 Gaps
- [ ] `python/tests/test_replay.py` — replay CLI + determinism
- [ ] `tests/test_replay_integration.py` — E2E 100k-event replay against fresh server
- [ ] Add `http::tests::test_public_*` cases to existing `src/server/http.rs` `#[cfg(test)]` module
- [ ] `scripts/deploy_smoke.sh` — curl health + stats after systemd start

## Security Domain

### Applicable ASVS Categories

| ASVS | Applies | Control |
|------|---------|---------|
| V2 Auth | yes (admin only) | Bearer token from `/etc/tally/admin.token`, read by server at boot; public routes unauth by design |
| V3 Session | no | No sessions; stateless HTTP |
| V4 Access Control | yes | Middleware gates admin routes on loopback-or-token; no RBAC needed |
| V5 Input Validation | yes | `key` path param length-capped at 256 bytes; `limit` query param clamped 1..=100; reject non-UTF8 |
| V6 Crypto | yes (TLS only) | Caddy auto-TLS via Let's Encrypt; do not hand-roll |
| V11 Business Logic | yes | Rate-limit `/public/*` via caddy (`caddy-ratelimit`), e.g. 60 req/min/IP |

### Known Threat Patterns for Axum + public HTTP

| Pattern | STRIDE | Mitigation |
|---------|--------|-----------|
| Key enumeration via `/public/features/:key` | Info disclosure | Return 200 with empty FeatureMap for unknown keys (don't distinguish 404 vs no-data); rate-limit |
| Unbounded response size on recent-events | DoS | Cap `limit=100`, strip event payloads to {stream, ts, hash} |
| Admin route reachable from internet | EoP / Tampering | Middleware checks `ConnectInfo` loopback OR `Authorization: Bearer $TOKEN` — test explicitly |
| TLS downgrade | Tampering | HSTS header from caddy |
| Abuse of replay port 6400 | Tampering | Never expose 6400 publicly; replay driver runs on VM or via SSH tunnel |
| Log injection via key | Tampering | Escape user-supplied key before logging; standard tracing/log crate already handles |

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Last-observed push latency only in `/metrics` | Rolling histogram with p50/p95/p99 | Phase 10.2 (v1.1) | Reuse directly for `/public/stats` |
| Single-threaded global mutex | DashMap + per-stream RwLock | Phase 14 (v1.3) | Replay can saturate with 8 clients |
| Per-event PUSH only | OP_PUSH_BATCH (client batches N events into one frame) | Phase 13 (v1.3) | 1.1M eps achievable — the whole reason the headline number is possible |
| Full snapshot every cycle | Incremental delta + periodic full | Phase 9 (v1.1) | 5-day uptime with recovery < 5 s |

**Deprecated:** None relevant.

## Open Questions (RESOLVED)

1. **Admin token storage.** Put the token in `/etc/tally/admin.token` (root:tally, mode 0640) or bake into the systemd `Environment=`? Files are auditable and rotatable; env vars leak to `ps`. **RESOLVED: file.**
2. **Should the replay run on the demo VM or from a laptop?** VM means eps isn't bottlenecked by home-broadband upload; laptop means the server is never exposed on :6400. **RESOLVED: ship a one-off "warmup" phase — replay runs on the VM at deploy time via `ExecStartPost=` or a separate `tally-replay.service`, then the demo serves the pre-warmed state forever.**
3. **Does the launch blog post get a new file or edit the existing "streaming-shouldnt-require-a-platform-team.md"?** The existing post is the launch narrative; editing it preserves the URL. **RESOLVED: edit in place, add section + headline number.**
4. **Restart cadence during 5 days.** Do we reset state nightly (for a "clean demo") or let it evolve? Phase 6 `entity_ttl` handles organic eviction. **RESOLVED: do not restart; let TTL + Phase 9 snapshot do their job. If state drifts visually weird, that's itself a good bug report.**

## Sources

### Primary (HIGH confidence)
- `/data/home/tally/src/server/http.rs` (lines 22–947) — existing routes, `/metrics`, `/debug/*`, rust-embed UI wiring
- `/data/home/tally/src/server/latency.rs` — `RollingHistogram`, `percentile()`, `command_histograms[CMD_PUSH]`
- `/data/home/tally/benchmark/tally-throughput/bench.py` — fraud generators, pipeline shapes reusable verbatim
- `/data/home/tally/.planning/phases/19-test-migration-and-old-api-removal/19-05-SUMMARY.md` — 1.1M eps / 8-client gate, `batch_size=1000` reference point
- `/data/home/tally/.planning/phases/14-per-stream-locks-dashmap-concurrency/14-03-SUMMARY.md` — DashMap concurrency result
- `/data/home/tally/mkdocs.yml` — confirms static-material site, no JS build
- `/data/home/tally/src/server/ui/{index.html,app.js,app.css}` — existing debug UI stack (d3/dagre/htmx)
- `/data/home/tally/docs/blog/streaming-shouldnt-require-a-platform-team.md` — blog style/voice
- `.planning/milestones/v2.0-ROADMAP.md` — Phase 6/8/9/10/10.2 success-criteria

### Secondary (MEDIUM)
- Standard systemd `Restart=always` / `StateDirectory` semantics
- Caddy v2 auto-TLS behavior

### Tertiary (LOW)
- None material to this research

## Metadata

**Confidence breakdown:**
- Reuse map (what already exists): HIGH — every piece was inspected in this session
- Replay strategy: HIGH — numbers pulled from 19-05 summary
- Deployment: HIGH — systemd + caddy is boring and well-documented
- Admin middleware exact Axum signature: MEDIUM — needs 10-min verification against pinned crate version

**Research date:** 2026-04-14
**Valid until:** 2026-05-14 (30 days — internal codebase is stable)
