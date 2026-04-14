#!/usr/bin/env bash
# provision.sh — one-shot Hetzner CX22 (Debian 12) bootstrap for the Tally demo.
#
# Usage (on the fresh VM, as root):
#   sudo bash provision.sh demo.tally.dev
#
# Prereqs (things this script does NOT do — you must do them first):
#   - DNS A record for $DOMAIN must already point at the VM's public IPv4 (Caddy's
#     ACME challenge will fail otherwise).
#   - The Tally release binary must be present as ./tally in the same directory as
#     this script (scp it alongside tally.service and Caddyfile before running).
#   - Optionally: export TALLY_ADMIN_TOKEN before running; if unset, a 32-byte hex
#     token is generated with `openssl rand -hex 32` and printed at the end.
#
# What it does:
#   1. creates the `tally` system user
#   2. installs Caddy v2 from the official Cloudsmith repo
#   3. installs the Tally binary to /usr/local/bin
#   4. writes admin token to /etc/tally/admin.token (mode 600, tally:tally)
#   5. installs the systemd unit and starts the service
#   6. configures Caddy with TLS for $DOMAIN
#   7. configures UFW: allow 22/80/443, deny 6400/6401
#   8. disables unattended-upgrades for the demo window
#   9. caps journald to 500 MB
#  10. waits up to 120s for https://$DOMAIN/health to return 200
set -euo pipefail

DOMAIN="${1:?usage: provision.sh <domain>}"
ADMIN_TOKEN="${TALLY_ADMIN_TOKEN:-$(openssl rand -hex 32)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Provisioning Tally demo for ${DOMAIN}"

# Sanity: required artefacts alongside this script
for f in tally tally.service Caddyfile; do
	if [[ ! -f "${SCRIPT_DIR}/${f}" ]]; then
		echo "FAIL: missing ${SCRIPT_DIR}/${f}. scp all four files (tally, tally.service, Caddyfile, provision.sh) to the VM before running." >&2
		exit 2
	fi
done

# 1. System user (idempotent)
if ! id -u tally >/dev/null 2>&1; then
	echo "==> Creating tally system user"
	useradd --system --home /var/lib/tally --shell /usr/sbin/nologin tally
fi

# 2. Caddy
echo "==> Installing Caddy"
apt-get update -y
apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl gnupg openssl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list >/dev/null
apt-get update -y
apt-get install -y caddy

# 3. Tally binary
echo "==> Installing /usr/local/bin/tally"
install -m 0755 "${SCRIPT_DIR}/tally" /usr/local/bin/tally

# 4. Admin token
echo "==> Writing admin token"
install -d -m 0750 -o tally -g tally /etc/tally
umask 077
# systemd EnvironmentFile format
printf 'TALLY_ADMIN_TOKEN=%s\n' "${ADMIN_TOKEN}" > /etc/tally/admin.token.env
chown tally:tally /etc/tally/admin.token.env
chmod 0600 /etc/tally/admin.token.env
# Plain token for operator-side access (cat over ssh)
printf '%s\n' "${ADMIN_TOKEN}" > /etc/tally/admin.token
chown tally:tally /etc/tally/admin.token
chmod 0600 /etc/tally/admin.token
umask 022

# Ensure the snapshot dir exists before systemd starts the unit. StateDirectory=tally
# creates/owns it, but creating it here too is idempotent and makes the order of
# operations explicit.
install -d -m 0750 -o tally -g tally /var/lib/tally

# 5. systemd unit
echo "==> Installing systemd unit"
install -m 0644 "${SCRIPT_DIR}/tally.service" /etc/systemd/system/tally.service
systemctl daemon-reload
systemctl enable --now tally.service

# 6. Caddy config (substitute the real domain)
echo "==> Configuring Caddy for ${DOMAIN}"
sed "s/demo\\.tally\\.dev/${DOMAIN}/g" "${SCRIPT_DIR}/Caddyfile" > /etc/caddy/Caddyfile
systemctl reload caddy

# 7. Firewall
echo "==> Configuring UFW"
apt-get install -y ufw
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp    comment 'ssh'
ufw allow 80/tcp    comment 'http (caddy acme + redirect)'
ufw allow 443/tcp   comment 'https (caddy)'
ufw deny  6400/tcp  comment 'tally tcp proto — loopback only, double-enforced by --tcp-bind'
ufw deny  6401/tcp  comment 'tally http mgmt — reached only via caddy reverse_proxy on 127.0.0.1'
ufw --force enable

# 8. Disable unattended-upgrades during the demo window (operator re-enables after signoff)
echo "==> Disabling unattended-upgrades for the demo window"
systemctl disable --now unattended-upgrades 2>/dev/null || true

# 9. journald cap
echo "==> Capping journald at 500M"
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/tally.conf <<'EOF'
[Journal]
SystemMaxUse=500M
EOF
systemctl restart systemd-journald

# 10. Wait for health
echo "==> Waiting for https://${DOMAIN}/health"
for i in $(seq 1 24); do
	if curl -fsS --max-time 5 "https://${DOMAIN}/health" >/dev/null 2>&1; then
		echo
		echo "OK: https://${DOMAIN}/health"
		echo "OK: tally.service = $(systemctl is-active tally.service)"
		echo "OK: caddy = $(systemctl is-active caddy)"
		echo
		echo "Admin token (store securely, rotate after signoff):"
		echo "  $(cat /etc/tally/admin.token)"
		echo
		echo "SSH tunnel for admin access:"
		echo "  ssh -L 6401:127.0.0.1:6401 root@${DOMAIN}"
		echo "  # then curl http://127.0.0.1:6401/debug/memory — loopback bypasses auth"
		exit 0
	fi
	printf '.'
	sleep 5
done

echo
echo "FAIL: https://${DOMAIN}/health did not respond 200 within 120s" >&2
echo "--- last 50 lines of tally journal ---" >&2
journalctl -u tally -n 50 --no-pager >&2 || true
echo "--- last 50 lines of caddy journal ---" >&2
journalctl -u caddy -n 50 --no-pager >&2 || true
exit 1
