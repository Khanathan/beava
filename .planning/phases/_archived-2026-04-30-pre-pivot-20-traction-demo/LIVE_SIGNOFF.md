# Phase 20 — 5-Day Live-Run Sign-Off

**Target:** `https://demo.tally.dev`
**Host:** Hetzner CX22 (2 vCPU, 4 GB RAM, Debian 12)
**Service:** `tally.service` under systemd (`Restart=always`, `StateDirectory=/var/lib/tally`)
**Reverse proxy:** Caddy v2 with auto-TLS
**Launch milestone:** v2.1
**Requirement:** TRAC-11 — 5 consecutive days uptime with at least one mid-run crash-recovery verified.

This file is updated in place during the 5-day observation window. Do NOT mark
this phase complete until every checkbox below is checked AND the final sign-off
line at the bottom is dated.

---

## Day 0 — Deploy

| Field | Value |
|-------|-------|
| Deploy timestamp (UTC) | _TBD — fill at `provision.sh` exit_ |
| Commit deployed (short sha) | _TBD_ |
| Admin token rotated? | _yes / no_ |
| `dig +short demo.tally.dev` | _VM IPv4_ |
| `curl https://demo.tally.dev/health` | _{"status":"ok"}_ |
| `bash deploy/smoke.sh https://demo.tally.dev --with-replay` | _exit 0 / exit 1 — paste summary_ |
| Admin token perms (`stat -c '%a %U:%G' /etc/tally/admin.token`) | _`600 tally:tally`_ |
| UFW status | _allow 22/80/443; deny 6400/6401_ |
| `! nc -z -w 2 <public-ip> 6400` | _closed / open_ |
| Measured replay wall-clock (seconds) | _TBD — paste `elapsed_seconds` from replay_30d.py run on VM_ |
| Measured replay events/sec | _TBD — paste `events_per_sec`_ |
| Blog post updated with actual VM number? | _yes / no_ |
| Screenshot at `docs/assets/demo.png` | _committed / pending_ |

---

## Daily check template

Repeat once per calendar day. Run from the operator's laptop (does NOT require SSH):

```bash
curl -s https://demo.tally.dev/public/stats | python3 -m json.tool
```

Record below. `uptime_seconds` should be monotonically increasing within a run;
if it resets, that is a restart event (expected at least once mid-run, see next section).

| Day | Date (UTC) | events_total | uptime_seconds | current_eps | p99_push_us | keys_total | Notes |
|----:|-----------|-------------:|---------------:|------------:|------------:|-----------:|------|
| 1   |           |              |                |             |             |            |      |
| 2   |           |              |                |             |             |            |      |
| 3   |           |              |                |             |             |            |      |
| 4   |           |              |                |             |             |            |      |
| 5   |           |              |                |             |             |            |      |

---

## Mid-run crash-recovery event

**When:** pick any time between day 1 and day 4 (NOT day 0 or day 5 — avoids
skewing the before/after measurement with a cold cache or end-of-run pressure).

**Procedure:**

```bash
# 1. Record before
BEFORE=$(curl -s https://demo.tally.dev/public/stats)
echo "$BEFORE" | python3 -m json.tool

# 2. Force-restart the service (simulates crash + systemd auto-recovery)
ssh root@demo.tally.dev sudo systemctl restart tally

# 3. Wait up to 15s for snapshot recovery, then record after
sleep 15
AFTER=$(curl -s https://demo.tally.dev/public/stats)
echo "$AFTER" | python3 -m json.tool

# 4. Acceptance: after.keys_total >= before.keys_total * 0.90
```

| Field | Value |
|-------|-------|
| Event date (UTC) | _TBD_ |
| Day of run | _1 / 2 / 3 / 4_ |
| Trigger | `systemctl restart tally` (controlled) / unexpected panic / OOM / other |
| `before.keys_total` | _TBD_ |
| `after.keys_total` (15s post-restart) | _TBD_ |
| Delta (%) | _TBD_ (must be ≥ -10%) |
| `before.events_total` | _TBD_ |
| `after.events_total` | _TBD_ (should equal or exceed — snapshot preserves) |
| Time from `systemctl restart` to `/public/stats` returning 200 | _TBD seconds (target ≤ 15s)_ |
| Snapshot file used (`ls -la /var/lib/tally/`) | _TBD_ |
| Pass/Fail | _PASS / FAIL_ |

If FAIL: stop the clock. Debug, fix, redeploy, restart the 5-day window at Day 0.

---

## Panic / error log scan

At day 5, before signing off, run:

```bash
ssh root@demo.tally.dev 'journalctl -u tally --since "5 days ago" | grep -cE "panic|FATAL|ERROR"'
ssh root@demo.tally.dev 'journalctl -u tally --since "5 days ago" | grep -c panic'
ssh root@demo.tally.dev 'systemctl status tally' | head -20
```

| Check | Value | Pass threshold |
|-------|------:|---------------|
| panic count | _TBD_ | 0 (or every panic followed by successful auto-restart visible in logs) |
| FATAL/ERROR count | _TBD_ | record — no hard threshold, but investigate spikes |
| Current `Active:` | _TBD_ | `active (running)` |
| Total restarts observed in logs | _TBD_ | ≤ expected (1 controlled + any panic-recovery) |

---

## Caddy access-log summary

```bash
ssh root@demo.tally.dev 'sudo journalctl -u caddy --since "5 days ago" | wc -l'
ssh root@demo.tally.dev 'sudo journalctl -u caddy --since "5 days ago" | grep -oE "\"method\":\"[A-Z]+\"" | sort | uniq -c'
```

| Metric | Value |
|-------|------:|
| Total log lines | _TBD_ |
| GET count | _TBD_ |
| POST count | _TBD_ (should be ~0 from public, since admin is edge-blocked) |
| Any 5xx responses? | _TBD_ (investigate each) |

---

## Final sign-off checklist

- [ ] Day 0 deploy row complete (deploy ts, commit, smoke exit 0, all token perms correct)
- [ ] Day 1 through Day 5 rows filled with real `/public/stats` snapshots
- [ ] `uptime_seconds` grew monotonically except across the one intentional restart
- [ ] Mid-run crash-recovery event recorded with before/after `keys_total` delta ≥ -10% and recovery time ≤ 15 s
- [ ] `journalctl -u tally | grep -c panic` == 0, OR every panic was followed by a successful auto-restart AND a subsequent `/public/stats` 200 within 15 s
- [ ] Final calendar span between Day 0 deploy timestamp and Day 5 final snapshot ≥ 5 × 24 h
- [ ] Caddy access log summary recorded
- [ ] `! nc -z -w 2 <public-ip> 6400` still returns closed at Day 5 (re-check, not just Day 0)
- [ ] `bash deploy/smoke.sh https://demo.tally.dev --with-replay` re-run on Day 5 passes all 6 invariants
- [ ] Blog post updated with the ACTUAL measured replay wall-clock from the VM (not the dev-box preliminary number)
- [ ] `docs/assets/demo.png` committed (screenshot with visible live counters)

---

## Sign-off

| Field | Value |
|-------|-------|
| Signed by | _operator name_ |
| Signed on (UTC) | _TBD — fill only when every box above is checked_ |
| Final `uptime_seconds` | _TBD_ |
| Final `events_total` | _TBD_ |
| Final `keys_total` | _TBD_ |
| Final blog commit sha | _TBD_ |

**Sign-off statement:** _"As of the timestamp above, `https://demo.tally.dev`
has been continuously serving HTTP 200 on `/health` and returning a populated
`/public/stats` for five consecutive calendar days, with one mid-run crash-recovery
event verified against the acceptance threshold. TCP port 6400 remains closed
on the public interface. No unrecovered panics occurred. The v2.1 launch
traction demo is live."_

_(Do NOT commit this statement until all checkboxes are checked.)_
