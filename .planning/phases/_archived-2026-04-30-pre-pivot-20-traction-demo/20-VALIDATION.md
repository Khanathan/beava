# Phase 20: Traction Demo — Validation

**Phase:** 20-traction-demo
**Validation framework:** Nyquist (test-first, per-requirement mapping)
**Created:** 2026-04-14

## Test Framework

| Property | Value |
|----------|-------|
| Rust framework | `cargo test` (workspace) |
| Python framework | `pytest` (Python SDK + replay CLI) |
| Config files | `Cargo.toml`, `python/pyproject.toml` |
| Quick run (per task commit) | `cargo test -p tally --lib http::tests && pytest python/tests/test_replay.py -x` — target < 30s |
| Wave merge run | `cargo test && pytest python/tests/` — full suite |
| Phase gate | Full suite green + manual 5-day staging sign-off (`LIVE_SIGNOFF.md`) |

## Requirement → Test Coverage Map

Every phase requirement (TRAC-01 through TRAC-11) must have at least one automated or documented-manual test. "Wave 0" means the test file is created in the same plan's first task (test-first TDD).

| Req | Plan | Description | Test Type | Command / Evidence | Status |
|-----|------|-------------|-----------|---------------------|--------|
| TRAC-01 | 20-01 | Standalone 30-day historical replay benchmark CLI exists and runs | integration | `pytest tests/integration/test_replay_30d.py -x` | Wave 0 |
| TRAC-01 | 20-01 | CLI prints `events_total`, `elapsed_seconds`, `events_per_sec`, `p50_push_us`, `p99_push_us`, `keys_total`, `final_state_mb` | integration | `pytest tests/integration/test_replay_30d.py::test_report_fields` | Wave 0 |
| TRAC-02 | 20-01 | Deterministic generator — same seed → byte-identical event stream | unit | `pytest tests/integration/test_replay_generator.py::test_determinism` | Wave 0 |
| TRAC-02 | 20-01 | Timestamp spread covers full 30-day window | unit | `pytest tests/integration/test_replay_generator.py::test_timestamp_spread` | Wave 0 |
| TRAC-02 | 20-01 | Failure rate ≈ 5% (distribution sanity) | unit | `pytest tests/integration/test_replay_generator.py::test_failure_rate` | Wave 0 |
| TRAC-03 | 20-01 | Wall-clock reporting accurate — `eps = events_total / elapsed` with `t0` captured after `register()` | integration | `pytest tests/integration/test_replay_30d.py::test_eps_floor` (asserts eps > 50k at 100k-event CI scale) | Wave 0 |
| TRAC-04 | 20-02 | `GET /public/features/:key` returns feature map, no operator state | unit | `cargo test --test test_public_http public_features_returns_feature_map` | Wave 0 |
| TRAC-04 | 20-02 | Response excludes `buckets`, `hll`, `operator_state` fields | unit | `cargo test --test test_public_http public_features_no_operator_state` | Wave 0 |
| TRAC-04 | 20-02 | `GET /public/recent-events` returns bounded list (default 20, max 100) | unit | `cargo test --test test_public_http public_recent_events_default_limit`, `public_recent_events_limit_clamp` | Wave 0 |
| TRAC-04 | 20-02 | `GET /public/stats` returns all 6 fields | unit | `cargo test --test test_public_http public_stats_shape` | Wave 0 |
| TRAC-04 | 20-02 | `Access-Control-Allow-Origin: *` on `/public/*` | unit | `cargo test --test test_public_http public_stats_cors_header` | Wave 0 |
| TRAC-05 | 20-02 | Loopback request to admin route → 200 | unit | `cargo test --test test_admin_auth loopback_get_debug_memory_ok`, `loopback_post_pipelines_ok` | Wave 0 |
| TRAC-05 | 20-02 | Non-loopback without token → 403 | unit | `cargo test --test test_admin_auth public_get_debug_memory_forbidden`, `public_post_pipelines_forbidden` | Wave 0 |
| TRAC-05 | 20-02 | Non-loopback with valid bearer → 200 | unit | `cargo test --test test_admin_auth public_with_token_ok` | Wave 0 |
| TRAC-05 | 20-02 | Non-loopback with wrong bearer → 403 | unit | `cargo test --test test_admin_auth public_with_wrong_token_forbidden` | Wave 0 |
| TRAC-05 | 20-02 | `/metrics` and `/health` remain ungated | unit | `cargo test --test test_admin_auth public_get_metrics_ok`, `public_get_health_ok` | Wave 0 |
| TRAC-05 | 20-02 | `--tcp-bind` CLI flag exists; defaults to `127.0.0.1` — TCP port 6400 not reachable on public interface | integration | `cargo test --test test_tcp_bind test_default_bind_is_loopback` + smoke assertion `! nc -z -w 2 $PUBLIC_IP 6400` | Wave 0 |
| TRAC-06 | 20-02 | Demo frontend served via rust-embed when `--public-mode` set | integration | `cargo test --test test_demo_page demo_page_served_when_public` | Wave 0 |
| TRAC-06 | 20-02 | Debug UI still served when `--public-mode` absent | integration | `cargo test --test test_demo_page debug_page_served_when_not_public` | Wave 0 |
| TRAC-06 | 20-02 | `demo.js` embedded in binary, references `/public/stats` | integration | `cargo test --test test_demo_page demo_assets_embedded` | Wave 0 |
| TRAC-06 | 20-02 | Combined demo.html+css+js ≤ 200 LOC | automated check | `wc -l src/server/ui/demo.html src/server/ui/demo.css src/server/ui/demo.js` (gated in Task 3 verify) | Wave 0 |
| TRAC-07 | 20-02 | `/metrics` exposes `tally_events_total`, `tally_push_latency_p99_seconds`, `tally_current_eps` | unit | `cargo test --test test_public_http metrics_contains_new_fields` | Wave 0 |
| TRAC-08 | 20-03 | `deploy/tally.service` has `Restart=always`, `StateDirectory=tally`, `--tcp-bind 127.0.0.1` | shell | `grep -q 'Restart=always' deploy/tally.service && grep -q 'StateDirectory=tally' deploy/tally.service && grep -q 'tcp-bind 127.0.0.1' deploy/tally.service` | Wave 0 |
| TRAC-08 | 20-03 | `deploy/Caddyfile` reverse-proxies to `127.0.0.1:6401` and blocks admin paths at edge | shell | `grep -q 'reverse_proxy.*6401' deploy/Caddyfile && grep -q '/pipelines' deploy/Caddyfile` | Wave 0 |
| TRAC-08 | 20-03 | `deploy/provision.sh` passes `bash -n` | shell | `bash -n deploy/provision.sh` | Wave 0 |
| TRAC-08 | 20-03 | `deploy/provision.sh` adds UFW rules allowing 22/80/443 only | shell | `grep -E 'ufw allow (22|80|443)' deploy/provision.sh && grep -E 'ufw deny 6400' deploy/provision.sh` | Wave 0 |
| TRAC-08 | 20-03 | Admin token file has mode 600, owner tally:tally | manual | `stat -c '%a %U:%G' /etc/tally/admin.token` == `600 tally:tally` (checked during deploy, recorded in LIVE_SIGNOFF.md) | Manual |
| TRAC-09 | 20-03 | `deploy/smoke.sh` runs 6 invariants: health, stats shape, admin denied, replay eps floor, crash recovery, **TCP 6400 unreachable publicly** | integration | `bash deploy/smoke.sh https://demo.tally.dev --with-replay` returns exit 0 | Wave 0 |
| TRAC-09 | 20-03 | `nc -z -w 2 $PUBLIC_IP 6400` fails (port closed on public interface) | shell | invariant 6 in `deploy/smoke.sh` | Wave 0 |
| TRAC-10 | 20-03 | Blog post contains measured replay number + live URL | shell | `grep -q 'demo.tally.dev' docs/blog/streaming-shouldnt-require-a-platform-team.md && grep -qE '[0-9]+ *(second|sec|s\b)' docs/blog/...` | Wave 0 |
| TRAC-10 | 20-03 | Screenshot committed at `docs/assets/demo.png` | shell | `test -f docs/assets/demo.png` | Manual (Task 3) |
| TRAC-11 | 20-03 | 5 consecutive days uptime recorded in `LIVE_SIGNOFF.md` | manual | `LIVE_SIGNOFF.md` has Day-0 deploy timestamp + 5 daily rows + final row | Manual (Task 4) |
| TRAC-11 | 20-03 | Mid-run crash recovery verified — `keys_total` within 10% of pre-restart value within 15s | manual | Event + before/after `keys_total` recorded in `LIVE_SIGNOFF.md` | Manual (Task 4) |
| TRAC-11 | 20-03 | No unrecovered panics over 5 days | shell | `journalctl -u tally --since '5 days ago' | grep -c panic` == 0 (or every panic followed by successful restart in logs) | Manual (Task 4) |

## Test Coverage Matrix

| Requirement | Automated (unit) | Automated (integration) | Automated (shell/smoke) | Manual / Signoff |
|-------------|:----------------:|:-----------------------:|:-----------------------:|:----------------:|
| TRAC-01 |  | ✅ | | |
| TRAC-02 | ✅ | | | |
| TRAC-03 |  | ✅ | | |
| TRAC-04 | ✅ | | | |
| TRAC-05 | ✅ | ✅ | ✅ | |
| TRAC-06 | | ✅ | ✅ | |
| TRAC-07 | ✅ | | | |
| TRAC-08 | | | ✅ | ✅ (token perms) |
| TRAC-09 | | ✅ | ✅ | |
| TRAC-10 | | | ✅ | ✅ (screenshot) |
| TRAC-11 | | | ✅ | ✅ (5-day run) |

Every requirement is covered. Requirements with purely operational semantics (TRAC-11 — 5-day uptime) combine shell assertions with a documented signoff artifact (`LIVE_SIGNOFF.md`).

## Wave 0 Test Gaps (tests to create before implementation)

Per-plan, in order:

**Plan 20-01 (Wave 1):**
- [ ] `tests/integration/test_replay_generator.py` — determinism, timestamp spread, failure rate, schema shape
- [ ] `tests/integration/test_replay_30d.py` — end-to-end 100k replay against spawned Tally subprocess

**Plan 20-02 (Wave 1):**
- [ ] `tests/integration/test_admin_auth.rs` — 8 cases (loopback/public × token/no-token × allowed/denied)
- [ ] `tests/integration/test_public_http.rs` — 8 cases for /public/features, /public/recent-events, /public/stats, /metrics extension, CORS
- [ ] `tests/integration/test_demo_page.rs` — 3 cases (public-mode vs debug-mode routing, asset embedding)
- [ ] `tests/integration/test_tcp_bind.rs` — 2 cases: default bind is 127.0.0.1, `--tcp-bind 0.0.0.0` overrides

**Plan 20-03 (Wave 2):**
- [ ] `deploy/smoke.sh` — 6 invariants (health, stats shape, admin denied, replay eps, crash recovery, TCP 6400 unreachable)
- [ ] Syntax checks: `bash -n deploy/provision.sh && bash -n deploy/smoke.sh` (in Task 1 `<verify>`)

## Sampling Rate

| Cadence | Command | Expected runtime |
|---------|---------|------------------|
| Per task commit | `cargo test -p tally --lib http::tests && pytest python/tests/test_replay.py -x` | < 30s |
| Per wave merge | `cargo test && pytest` | ~5–10 min |
| Phase gate | Full suite + 5-day staging + `LIVE_SIGNOFF.md` countersigned | 5 calendar days |

## Security Validation (maps to RESEARCH Security Domain)

| ASVS | Control | Test |
|------|---------|------|
| V2 Auth | Admin bearer token required from non-loopback | `test_admin_auth::public_with_token_ok`, `public_with_wrong_token_forbidden` |
| V4 Access Control | Loopback bypass, token fallback | `test_admin_auth::loopback_*`, `public_get_debug_memory_forbidden` |
| V5 Input Validation | `limit` clamp 1..=100; key length cap | `test_public_http::public_recent_events_limit_clamp` |
| V6 Crypto | Caddy auto-TLS | Smoke invariant 1 (`https://demo.tally.dev/health` returns 200 with valid cert) |
| V11 Business Logic | Rate limit via Caddy | Manual: `ab -n 200 https://demo.tally.dev/public/stats` triggers 429 (if `caddy-ratelimit` bundled) |
| V14 Config | TCP 6400 not publicly reachable | Smoke invariant 6: `! nc -z -w 2 $PUBLIC_IP 6400` |

## Out of Scope

- Load-testing beyond smoke replay (not a benchmark phase; replay CLI itself is the benchmark)
- Multi-region deployment validation (single VM, single region by design)
- Long-term (>5 day) uptime validation — deferred; this phase validates launch-window stability only
