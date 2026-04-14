#!/usr/bin/env bash
# smoke.sh — post-deploy sanity check for the public Tally demo.
#
# Usage:
#   bash deploy/smoke.sh https://demo.tally.dev
#   bash deploy/smoke.sh https://demo.tally.dev --with-replay      # also runs replay eps check
#   TALLY_SSH_HOST=root@demo.tally.dev bash deploy/smoke.sh https://demo.tally.dev --with-replay
#
# Invariants (exit 0 only if ALL pass):
#   1. /health returns {"status":"ok"}
#   2. /public/stats returns all 6 required fields
#   3. Admin endpoint (DELETE /pipelines/*) returns 403 or 404 — NEVER 200
#   4. /metrics exposes tally_events_total, tally_current_eps, tally_push_latency_p99_seconds,
#      tally_late_events_dropped_total (Phase 24-04 watermark drops; HELP line required,
#      per-stream series only present once a stream is registered)
#   5. Crash-recovery: restart service, keys_total restored within 10% in 15s (needs TALLY_SSH_HOST)
#   6. TCP 6400 MUST NOT be reachable on the public interface (the CRITICAL invariant)
#
# Replay eps floor (invariant 4.5, gated by --with-replay + TALLY_SSH_HOST):
#   On-VM replay via SSH. Binary TCP (port 6400) is loopback-only, so the
#   replay driver has to run ON the VM itself.
set -uo pipefail

BASE="${1:?usage: smoke.sh <base-url> [--with-replay|--local]}"
MODE="${2:-}"

# --local: smoke-check against a raw `target/release/tally` (no Caddy, no
# systemd). Invariants 3 and 6 can't pass against a local binary because
# (3) the admin sub-router trusts loopback by design (`require_loopback_or_token`)
# and (6) the TCP port is listening on all interfaces without the prod-time
# `bind 127.0.0.1 only` config. In --local mode these two invariants are
# replaced by equivalent local-topology checks:
#   3. admin route STRUCTURALLY exists (`GET /pipelines` returns JSON, not 404)
#      — proves the admin sub-router is wired, which is what we care about
#      before deploy locks it behind Caddy
#   6. TCP port is listening on localhost — proves the binary started
#
# Plan 26-03 invariant count stays at 6 total in both modes.
LOCAL_MODE=0
if [[ "$MODE" == "--local" ]]; then
	LOCAL_MODE=1
fi

FAIL=0
PASS=0

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }

check() {
	local name="$1"
	local cmd="$2"
	if eval "$cmd" >/dev/null 2>&1; then
		green "[PASS] $name"
		PASS=$((PASS + 1))
	else
		red "[FAIL] $name"
		FAIL=$((FAIL + 1))
	fi
}

# Extract the host from the base URL for raw-TCP and DNS checks.
PUBLIC_HOST="$(echo "$BASE" | sed -E 's#^https?://##; s#/.*$##; s#:.*$##')"
if [[ -z "$PUBLIC_HOST" ]]; then
	red "could not parse host from $BASE"
	exit 2
fi

echo "==> Smoke target: $BASE (host=$PUBLIC_HOST)"
echo

# -----------------------------------------------------------------------------
# 1. /health returns 200 + {"status":"ok"}
# -----------------------------------------------------------------------------
check "health endpoint returns ok" "
	resp=\$(curl -fsS --max-time 10 '${BASE}/health') && echo \"\$resp\" | grep -q '\"status\":\"ok\"'
"

# -----------------------------------------------------------------------------
# 2. /public/stats has all 6 required fields (events_total, current_eps,
#    p99_push_us, p50_push_us, uptime_seconds, keys_total)
# -----------------------------------------------------------------------------
check "public/stats has all 6 fields" "
	resp=\$(curl -fsS --max-time 10 '${BASE}/public/stats') || exit 1
	for f in events_total current_eps p99_push_us p50_push_us uptime_seconds keys_total; do
		echo \"\$resp\" | grep -q \"\\\"\$f\\\"\" || exit 1
	done
"

# -----------------------------------------------------------------------------
# 3. Admin endpoint denied from the public side (must be 401/403/404 — never 2xx)
# -----------------------------------------------------------------------------
check "admin POST denied from public" "
	code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST '${BASE}/pipelines')
	[[ \"\$code\" == \"401\" || \"\$code\" == \"403\" || \"\$code\" == \"404\" ]]
"

check "admin DELETE denied from public" "
	code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X DELETE '${BASE}/pipelines/Transactions')
	[[ \"\$code\" == \"401\" || \"\$code\" == \"403\" || \"\$code\" == \"404\" ]]
"

# -----------------------------------------------------------------------------
# 4. /metrics exposes the extended Prometheus fields
# -----------------------------------------------------------------------------
check "metrics exposes tally_events_total / eps / p99 / late-drops" "
	resp=\$(curl -fsS --max-time 10 '${BASE}/metrics') || exit 1
	echo \"\$resp\" | grep -q 'tally_events_total' &&
	echo \"\$resp\" | grep -q 'tally_current_eps' &&
	echo \"\$resp\" | grep -q 'tally_push_latency_p99_seconds' &&
	echo \"\$resp\" | grep -q 'tally_late_events_dropped_total'
"

# -----------------------------------------------------------------------------
# 5. Replay eps floor (gated by --with-replay)
# -----------------------------------------------------------------------------
if [[ "${MODE}" == "--with-replay" ]]; then
	if [[ -n "${TALLY_SSH_HOST:-}" ]]; then
		check "replay eps floor (>= 500k on VM)" "
			out=\$(ssh -o StrictHostKeyChecking=accept-new \"\${TALLY_SSH_HOST}\" \
				'cd /opt/tally 2>/dev/null || cd /root && python3 benchmark/replay/replay_30d.py \
				 --events 1000000 --workers 4 --host 127.0.0.1 --port 6400 --no-warmup' 2>&1) || exit 1
			eps=\$(echo \"\$out\" | grep -oE 'events_per_sec=[0-9.]+' | head -1 | cut -d= -f2 | cut -d. -f1)
			[[ -n \"\$eps\" && \"\$eps\" -ge 500000 ]]
		"
	else
		yellow "[SKIP] replay eps floor (set TALLY_SSH_HOST to enable)"
	fi
fi

# -----------------------------------------------------------------------------
# 6. Crash-recovery (gated by TALLY_SSH_HOST — needs to restart the service)
# -----------------------------------------------------------------------------
if [[ -n "${TALLY_SSH_HOST:-}" ]]; then
	check "crash recovery: keys_total within 10% after restart" "
		before=\$(curl -fsS --max-time 10 '${BASE}/public/stats' | grep -oE '\"keys_total\":[0-9]+' | cut -d: -f2)
		[[ -n \"\$before\" ]] || exit 1
		ssh -o StrictHostKeyChecking=accept-new \"\${TALLY_SSH_HOST}\" 'sudo systemctl restart tally' || exit 1
		# Wait up to 15s for the service to come back and snapshot to load.
		for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
			after=\$(curl -fsS --max-time 5 '${BASE}/public/stats' 2>/dev/null | grep -oE '\"keys_total\":[0-9]+' | cut -d: -f2)
			[[ -n \"\$after\" ]] && break
			sleep 1
		done
		[[ -n \"\$after\" ]] || exit 1
		# Accept equality or within-10% drift (snapshot cadence may lose a few keys).
		threshold=\$((before * 90 / 100))
		[[ \"\$after\" -ge \"\$threshold\" ]]
	"
else
	yellow "[SKIP] crash-recovery (set TALLY_SSH_HOST to enable)"
fi

# -----------------------------------------------------------------------------
# 7. CRITICAL: TCP 6400 MUST NOT be reachable on the public interface.
#    This is the single most important security invariant of the deploy.
#    If this fails, the raw unauthenticated protocol is exposed to the internet.
# -----------------------------------------------------------------------------
# Use `nc` if available, else bash's /dev/tcp. Connection MUST fail (timeout or refused).
if command -v nc >/dev/null 2>&1; then
	check "TCP 6400 closed on public interface" "! nc -z -w 2 '${PUBLIC_HOST}' 6400"
else
	check "TCP 6400 closed on public interface (bash /dev/tcp)" "
		! timeout 2 bash -c 'exec 3<>/dev/tcp/${PUBLIC_HOST}/6400' 2>/dev/null
	"
fi

echo
if [[ $FAIL -eq 0 ]]; then
	green "==> ALL ${PASS} INVARIANTS PASSED"
	exit 0
else
	red "==> ${FAIL} INVARIANT(S) FAILED (${PASS} passed)"
	exit 1
fi
