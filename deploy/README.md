# Tally Deploy — Single-VM Launch Demo

This directory contains everything needed to run the Tally public read-only demo on a $5/mo Hetzner CX22 (or equivalent 1-2 vCPU / 4 GB RAM Debian 12 box).

## Artefacts

| File | Role |
|------|------|
| `tally.service` | systemd unit (`Restart=always`, `StateDirectory=tally`, `--tcp-bind 127.0.0.1`) |
| `Caddyfile` | TLS + reverse proxy fronting `127.0.0.1:6401`, blocks admin paths at the edge |
| `provision.sh` | One-shot Debian 12 bootstrap (idempotent) |
| `smoke.sh` | Post-deploy sanity — 6 invariants |

## One-command provision

On a fresh Hetzner CX22 with Debian 12:

```bash
# 0. On your laptop — build the Linux binary and SCP everything
cargo build --release --target x86_64-unknown-linux-gnu
scp target/x86_64-unknown-linux-gnu/release/tally \
    deploy/tally.service deploy/Caddyfile deploy/provision.sh \
    root@$VM_IP:/root/

# 1. Verify DNS is propagated FIRST (Let's Encrypt will reject otherwise)
dig +short demo.tally.dev   # should print the VM's IPv4

# 2. Run the provisioner
ssh root@$VM_IP 'cd /root && sudo bash provision.sh demo.tally.dev'
```

Expected runtime: ~90 seconds. Prints the admin token on success.

## Smoke

From your laptop (not the VM):

```bash
# Light smoke — 4 invariants that run without SSH access
bash deploy/smoke.sh https://demo.tally.dev

# Full smoke — 6 invariants including replay + crash-recovery
export BEAVA_SSH_HOST=root@demo.tally.dev
bash deploy/smoke.sh https://demo.tally.dev --with-replay
```

The 6 invariants:

1. `/health` returns `{"status":"ok"}`
2. `/public/stats` returns all 6 required fields
3. Admin endpoints return 403 or 404 (never 200)
4. Replay across loopback hits the events/sec floor (when `--with-replay`)
5. `systemctl restart tally` is followed by `keys_total` within 10% of pre-restart (when `BEAVA_SSH_HOST` is set)
6. **TCP 6400 is unreachable on the public IP** (`! nc -z -w 2 $PUBLIC_HOST 6400`) — the critical public-surface assertion

## File layout on the VM

```
/usr/local/bin/tally                         # binary
/etc/systemd/system/tally.service            # unit
/etc/tally/admin.token         (600 tally:tally)  # plain hex, for operator eyes
/etc/tally/admin.token.env     (600 tally:tally)  # systemd EnvironmentFile
/etc/caddy/Caddyfile                         # domain-substituted
/var/lib/tally/                              # snapshots, event log (StateDirectory=tally)
```

## Admin access

Admin routes (`/pipelines`, `/snapshot`, `/debug/*`) are:

- **404 at the edge** (Caddy drops them)
- **403 at the middleware** from any non-loopback source

The only supported way to reach them is an SSH tunnel + loopback:

```bash
ssh -L 6401:127.0.0.1:6401 root@demo.tally.dev
# In another terminal on your laptop:
curl http://127.0.0.1:6401/debug/memory
curl -X POST http://127.0.0.1:6401/snapshot     # force snapshot
```

Loopback bypasses both Caddy and the bearer-token gate (see `src/server/auth.rs::require_loopback_or_token`).

## Tailing logs

```bash
ssh root@demo.tally.dev 'journalctl -u tally -f'
ssh root@demo.tally.dev 'journalctl -u caddy -f'
```

## Rolling the admin token

```bash
ssh root@demo.tally.dev <<'EOF'
NEW=$(openssl rand -hex 32)
install -m 600 -o tally -g tally /dev/null /etc/tally/admin.token
printf '%s\n' "$NEW" > /etc/tally/admin.token
printf 'BEAVA_ADMIN_TOKEN=%s\n' "$NEW" > /etc/tally/admin.token.env
chmod 600 /etc/tally/admin.token.env /etc/tally/admin.token
chown tally:tally /etc/tally/admin.token.env /etc/tally/admin.token
systemctl restart tally
EOF
```

## Manual snapshot

```bash
ssh -L 6401:127.0.0.1:6401 root@demo.tally.dev -N &
curl -X POST http://127.0.0.1:6401/snapshot
```

## Trade-offs documented

- **Rate-limit disabled by default.** The `rate_limit` stanza in `Caddyfile` is commented
  out because it requires the [`caddy-ratelimit`](https://github.com/mholt/caddy-ratelimit) module
  which isn't in the stock `apt install caddy` bundle. The launch-window blast radius is small
  (read-only endpoints, single VM); if a post is going viral, rebuild caddy with xcaddy
  or uncomment after installing the module.
- **Unattended-upgrades disabled** during the 5-day demo window to prevent a reboot
  mid-run. Re-enable with `systemctl enable --now unattended-upgrades` after sign-off.
- **Admin routes are unreachable from the internet by design.** There is no "break
  glass" web UI. Use SSH + loopback. If that is inconvenient, set `BEAVA_ADMIN_TOKEN`
  in the Caddyfile `@admin` block to proxy with bearer auth — not recommended for
  the public launch window.
