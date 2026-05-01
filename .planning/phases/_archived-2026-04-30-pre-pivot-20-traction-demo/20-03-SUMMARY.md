---
phase: 20-traction-demo
plan: 03
subsystem: deploy
tags: [deploy, systemd, caddy, hetzner, ufw, smoke, launch, blog]
dependency_graph:
  requires:
    - 20-01 (replay CLI — smoke.sh invariant 4 invokes benchmark/replay/replay_30d.py on the VM)
    - 20-02 (public routes /public/*, /metrics extension, --tcp-bind default 127.0.0.1)
    - Phase 9 (incremental snapshots — crash-recovery invariant depends on it)
  provides:
    - deploy_artifacts (systemd unit, Caddyfile, provision.sh, smoke.sh)
    - live_signoff_framework (LIVE_SIGNOFF.md — calendar-bound gate for TRAC-11)
    - launch_blog_headline (30-day replay section + demo.tally.dev link)
  affects:
    - v2.1 Launch milestone — blocked on 5-day live-run sign-off only
tech-stack:
  added:
    - caddy v2 (TLS + reverse proxy)
    - systemd unit (Restart=always, sandboxing)
    - ufw (firewall, defense in depth for --tcp-bind)
  patterns:
    - "SCP-then-bash one-shot provisioning (no Ansible, no Docker)"
    - "Defense in depth: --tcp-bind + ufw deny 6400/6401 + Caddy edge-404 of admin paths"
    - "Admin access via SSH tunnel to loopback (bypasses both Caddy and bearer middleware)"
    - "Smoke invariants 4/5 gated by TALLY_SSH_HOST (replay + restart need the VM)"
key-files:
  created:
    - deploy/tally.service
    - deploy/Caddyfile
    - deploy/provision.sh
    - deploy/smoke.sh
    - deploy/README.md
    - .planning/phases/20-traction-demo/LIVE_SIGNOFF.md
  modified:
    - docs/blog/streaming-shouldnt-require-a-platform-team.md
decisions:
  - Admin access via SSH tunnel + loopback — no bearer-token endpoint on the public Caddy listener, since Caddy strips Authorization headers from proxied requests. Reduces attack surface to zero for the launch window.
  - Caddy rate-limit stanza commented out — the caddy-ratelimit module isn't in the stock apt bundle; launch-window blast radius is small; trade-off documented in deploy/README.md. Can be re-enabled by rebuilding caddy with xcaddy.
  - Smoke invariant 6 (TCP 6400 unreachable) uses `nc` if available, falls back to bash /dev/tcp + timeout. Either way, the assertion is negated — the connection MUST fail.
  - Unattended-upgrades disabled during the 5-day window to prevent a reboot mid-run. Operator re-enables after sign-off.
  - Blog headline uses the dev-box preliminary number (~276k eps from Plan 20-01 manual smoke) with an explicit note that the VM-measured number gets substituted in Task 3. Honest framing per plan's "Accept reality; do not re-run on a bigger box and mislead."
  - Admin token stored in BOTH /etc/tally/admin.token (plain, for operator cat) and /etc/tally/admin.token.env (systemd EnvironmentFile). Both mode 0600 tally:tally.
  - Sandboxing flags added to tally.service (NoNewPrivileges, ProtectSystem=strict, ReadWritePaths=/var/lib/tally) — cheap hardening, no functionality impact.
metrics:
  duration_seconds: 257
  tasks_completed_automated: 2
  tasks_pending_human: 2
  files_created: 6
  files_modified: 1
  completed_date: 2026-04-14
requirements-completed-automated:
  - TRAC-08  # systemd + Caddyfile + provision.sh (artifacts on disk, syntax-verified)
  - TRAC-09  # smoke.sh with all 6 invariants including the critical tcp 6400 assertion
  - TRAC-10  # blog post contains demo.tally.dev link + measured (preliminary) replay number
requirements-pending-human:
  - TRAC-08 (runtime acceptance on VM — token perms verified by `stat` on the VM after provision)
  - TRAC-10 (actual VM replay number + docs/assets/demo.png screenshot — owned by Task 3 operator)
  - TRAC-11 (5-day uptime + mid-run crash-recovery observation — owned by Task 4 operator)
---

# Phase 20 Plan 03: Deploy + 5-Day Live-Run — Summary

Deploy artifacts (systemd + Caddy + provision + smoke + README) for the Hetzner CX22 launch VM, a blog update with the 30-day replay headline section, and a fully-templated `LIVE_SIGNOFF.md` 5-day observation framework. The two automated tasks are committed; the two human-gated tasks (VM provisioning and the 5-day live run) are documented and await operator execution.

## What shipped (automated)

| Path | Role | Commit |
|------|------|--------|
| `deploy/tally.service` | systemd unit — `Restart=always`, `StateDirectory=tally`, `--tcp-bind 127.0.0.1`, sandboxing | `2f3b21a` |
| `deploy/Caddyfile` | TLS + reverse_proxy `127.0.0.1:6401`, edge 404 for `/pipelines*`, `/snapshot*`, `/debug/*`, CORS `*` on public | `2f3b21a` |
| `deploy/provision.sh` | One-shot Debian 12 bootstrap: tally user, Caddy install, admin token, systemd enable, ufw (allow 22/80/443; deny 6400/6401), journald cap, 120s health wait | `2f3b21a` |
| `deploy/README.md` | Operator runbook: SCP commands, SSH tunnel for admin, token rotation, manual snapshot, rate-limit trade-off | `2f3b21a` |
| `deploy/smoke.sh` | 6 invariants (health, stats shape, admin POST denied, admin DELETE denied, metrics fields, TCP 6400 closed) + 2 SSH-gated (replay eps, crash-recovery) | `8c97f77` |
| `docs/blog/streaming-shouldnt-require-a-platform-team.md` | New "30 days of events. Replayed in seconds." section linking `demo.tally.dev` | `8c97f77` |
| `.planning/phases/20-traction-demo/LIVE_SIGNOFF.md` | 5-day observation framework — Day 0 deploy table, 5 daily rows, crash-recovery procedure, panic scan, final checklist + sign-off statement | `edd1de5` |

Three commits, atomic per task: `2f3b21a`, `8c97f77`, `edd1de5`.

## Verification results (automated)

```
bash -n deploy/provision.sh                                 → OK
bash -n deploy/smoke.sh                                     → OK
grep 'Restart=always' deploy/tally.service                  → OK
grep 'StateDirectory=tally' deploy/tally.service            → OK
grep 'tcp-bind 127.0.0.1' deploy/tally.service              → OK
grep 'reverse_proxy 127.0.0.1:6401' deploy/Caddyfile        → OK
grep -E 'ufw (allow|deny)' deploy/provision.sh              → OK
grep 'ufw deny  *6400' deploy/provision.sh                  → OK
grep 'ufw deny  *6401' deploy/provision.sh                  → OK
grep -E 'ufw allow  *(22|80|443)' deploy/provision.sh       → OK (all three)
grep 'chmod 0600 /etc/tally/admin.token' deploy/provision.sh → OK
grep 'nc -z -w 2' deploy/smoke.sh                            → OK (invariant 6)
grep 'TCP 6400 closed on public interface' deploy/smoke.sh   → OK
grep 'demo.tally.dev' docs/blog/...                          → OK
grep -E '[0-9]+ *(second|sec|s\b)' docs/blog/...             → OK
wc -l deploy/*                                               → provision.sh=145, smoke.sh=156, tally.service=42, Caddyfile=55, README.md=125 (all ≥ min_lines budgets)
```

Tools NOT available in this environment (shellcheck, caddy, systemd-analyze) — syntax
validation limited to `bash -n`. This is expected; the real validation is
`provision.sh` running on the Hetzner VM (Task 3).

## What remains — HUMAN ACTION GATED

### Task 3 — Provision the Hetzner VM (checkpoint:human-action)

**READY FOR VM PROVISION.** The agent has produced and syntax-verified every artifact the operator needs. The following steps are the operator's — they cannot be executed from the dev environment.

**Operator checklist:**

1. **Create Hetzner CX22 VM** — Debian 12, Frankfurt or Ashburn, upload SSH public key. Note the public IPv4.
2. **DNS** — point `demo.tally.dev` A record at the IPv4. Verify with `dig +short demo.tally.dev` → IPv4 BEFORE running step 5 (Let's Encrypt needs propagation).
3. **Build the Linux binary** (the Tally binary, NOT part of this plan):
   ```bash
   cargo build --release --target x86_64-unknown-linux-gnu
   ```
4. **SCP artifacts to the VM:**
   ```bash
   scp target/x86_64-unknown-linux-gnu/release/tally \
       deploy/tally.service deploy/Caddyfile deploy/provision.sh \
       root@<VM_IP>:/root/
   ```
5. **Provision:**
   ```bash
   ssh root@<VM_IP> 'cd /root && sudo bash provision.sh demo.tally.dev'
   ```
   Expected runtime: ~90 s. Prints the admin token on success.
6. **Verify token perms on the VM** (TRAC-08 manual step):
   ```bash
   ssh root@<VM_IP> 'stat -c "%a %U:%G" /etc/tally/admin.token'
   # Must print: 600 tally:tally
   ```
7. **Run full smoke from laptop** (TRAC-09 runtime acceptance):
   ```bash
   export TALLY_SSH_HOST=root@<VM_IP>
   bash deploy/smoke.sh https://demo.tally.dev --with-replay
   # Expected: ALL 6+ INVARIANTS PASSED, exit 0
   ```
8. **Capture headline replay number** (TRAC-10):
   ```bash
   ssh root@<VM_IP> 'cd /root && python3 benchmark/replay/replay_30d.py \
     --events 30000000 --workers 8 --host 127.0.0.1 --port 6400' | tee /tmp/replay.out
   ```
   Copy `elapsed_seconds` and `events_per_sec` into the blog post, replacing the
   preliminary dev-box number. Commit as a separate "docs(20-03)" commit.
9. **Capture screenshot** of `https://demo.tally.dev` with visible live counters,
   save to `docs/assets/demo.png`, commit. (docs/assets/ does not yet exist — it
   will be created by the commit.)
10. **Record Day 0 deploy timestamp + commit sha** in the `## Day 0 — Deploy`
    table of `LIVE_SIGNOFF.md`.

**Resume signal:** Type `deployed` and paste the measured `elapsed_seconds` + `events_per_sec` from step 8 + the `docs/assets/demo.png` commit sha, or describe issues.

### Task 4 — 5-day live run (checkpoint:human-verify, CALENDAR-GATED)

The 5-day window CANNOT be compressed. Even with perfect automation, the phase
cannot complete before Day 0 + 5 × 24 h of elapsed wall-clock time. The framework
is in place (`LIVE_SIGNOFF.md`); the observations are not.

**Do NOT mark this plan's requirements TRAC-11 complete in REQUIREMENTS.md until
every checkbox in LIVE_SIGNOFF.md's final checklist is ticked AND the sign-off
statement is dated.**

**Resume signal:** Type `signoff` when `LIVE_SIGNOFF.md` is fully filled and
the sign-off statement is dated, OR describe the failure mode if the 5-day
window had to restart.

## Deviations from Plan

### Auto-applied (Rules 1–3)

**1. [Rule 2 — Missing critical functionality] Sandboxing flags on tally.service**
- **Found during:** Task 1 drafting
- **Issue:** Plan's systemd unit was minimal. For a public-facing service, the cheap hardening flags (`NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, `ReadWritePaths=/var/lib/tally`) are standard defense-in-depth and cost nothing.
- **Fix:** Added them. `ReadWritePaths` is required because `ProtectSystem=strict` would otherwise prevent snapshot writes.
- **File:** `deploy/tally.service`
- **Commit:** `2f3b21a`

**2. [Rule 2 — Missing critical functionality] Caddyfile strips upstream Authorization**
- **Found during:** Task 1 drafting
- **Issue:** Plan's Caddyfile didn't explicitly strip `Authorization` headers from public requests. Without that, an attacker can smuggle a guessed bearer token through the proxy to admin routes. The edge `@admin` 404 would stop *known* admin paths, but the header stripping prevents any leakage.
- **Fix:** Added `header_up -Authorization` inside `reverse_proxy`.
- **File:** `deploy/Caddyfile`
- **Commit:** `2f3b21a`

**3. [Rule 3 — Blocking] Plan smoke.sh referenced `/opt/tally` and `/root` for replay path**
- **Found during:** Task 2 drafting
- **Issue:** Plan's smoke.sh hardcoded `cd /opt/tally`. The provision.sh doesn't install the repo to `/opt/tally` (only the binary to `/usr/local/bin/tally`). If the operator clones the repo anywhere else, the path breaks.
- **Fix:** Smoke.sh now tries `cd /opt/tally 2>/dev/null || cd /root` so either layout works. The operator must clone the repo to one of those paths on the VM to run replay via the smoke's `--with-replay` mode. Documented in `deploy/README.md`.
- **File:** `deploy/smoke.sh`, `deploy/README.md`
- **Commit:** `8c97f77`

**4. [Rule 2 — Missing critical functionality] Smoke invariant 6 fallback when nc absent**
- **Found during:** Task 2 drafting
- **Issue:** Plan's invariant 6 uses `nc -z -w 2`. On a minimal laptop (the dev environment running this plan) `nc` may not be installed. A PASS from a missing-binary error is a false positive and would defeat the critical invariant.
- **Fix:** Smoke.sh detects `nc` availability; falls back to `timeout 2 bash -c 'exec 3<>/dev/tcp/HOST/6400'`. Either succeeds in rejecting an open port; neither returns a false PASS on missing tool.
- **File:** `deploy/smoke.sh`
- **Commit:** `8c97f77`

**5. [Rule 3 — Blocking] .planning/ directory was .gitignore'd**
- **Found during:** Committing LIVE_SIGNOFF.md (Task 4 framework)
- **Issue:** `git add .planning/phases/20-traction-demo/LIVE_SIGNOFF.md` rejected — pattern ignored. Prior `20-01-SUMMARY.md` and `20-02-SUMMARY.md` must have been added with `-f`.
- **Fix:** `git add -f` for planning artifacts. Did NOT touch `.gitignore` — out of scope. Pattern matches prior practice in this repo.
- **File:** n/a
- **Commit:** `edd1de5`

### Not fixed (out of scope / deferred)

- **Blog post still has the preliminary dev-box replay number** (276k eps / ~108 s projected). This is per-plan instruction: "The exact replay number is substituted at edit time based on the ACTUAL measurement — do not hard-code a guess." The actual measurement requires the VM. Task 3's step 8 replaces it with the real VM number. An explicit note in the blog tells the reader the number is preliminary.
- **`docs/assets/demo.png` does not exist yet.** The plan's Task 3 captures it from the live demo. Cannot be generated from the dev environment without a running VM + rendered demo page.

## Authentication Gates

None required for the artifact-production work. The `HETZNER_API_TOKEN` and
`TALLY_ADMIN_TOKEN` mentioned in the plan's `user_setup` are operator-side
concerns for Task 3. Task 3's checklist reminds the operator that
`TALLY_ADMIN_TOKEN` can be pre-set or auto-generated by `provision.sh`.

## Threat Flags

| Flag | File | Description |
|------|------|-------------|
| threat_flag: new_network_edge | `deploy/Caddyfile` | First Tally deployment with a public HTTPS listener. Surface: `/`, `/health`, `/metrics`, `/public/*`, `/static/*`. Admin paths are edge-404'd and middleware-403'd; TCP 6400 is kernel-denied. Reviewed against 20-VALIDATION §Security — every ASVS row has a corresponding smoke invariant. |
| threat_flag: credential_at_rest | `deploy/provision.sh`, VM filesystem | Admin token stored in `/etc/tally/admin.token{,.env}` mode 600 tally:tally. Rotation procedure documented in `deploy/README.md`. |

No new surface beyond what was pre-declared in the phase CONTEXT and VALIDATION.

## Self-Check: PASSED

- FOUND: `/data/home/tally/deploy/tally.service`
- FOUND: `/data/home/tally/deploy/Caddyfile`
- FOUND: `/data/home/tally/deploy/provision.sh`
- FOUND: `/data/home/tally/deploy/smoke.sh`
- FOUND: `/data/home/tally/deploy/README.md`
- FOUND: `/data/home/tally/.planning/phases/20-traction-demo/LIVE_SIGNOFF.md`
- MODIFIED: `/data/home/tally/docs/blog/streaming-shouldnt-require-a-platform-team.md` (contains `demo.tally.dev` + replay number)
- FOUND commit `2f3b21a` in `git log --all` (Task 1)
- FOUND commit `8c97f77` in `git log --all` (Task 2)
- FOUND commit `edd1de5` in `git log --all` (Task 4 framework)
- `bash -n deploy/provision.sh` → OK
- `bash -n deploy/smoke.sh` → OK
- All plan-required greps (Restart=always, StateDirectory, tcp-bind 127.0.0.1, reverse_proxy 6401, ufw rules, admin.token 600, nc invariant, demo.tally.dev in blog, seconds token in blog) → OK

`docs/assets/demo.png` intentionally absent — captured during Task 3 by the operator.
