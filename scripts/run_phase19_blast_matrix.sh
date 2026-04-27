#!/usr/bin/env bash
# Phase 19 throughput-run script per CLAUDE.md §Performance Discipline.
#
# Drives the Rust + Python harnesses across the mandatory matrix subset and
# appends rows to .planning/throughput-baselines.md under the new
# "## 1M-event blast" section.
#
# Mandatory subset (12 cells: 10 Rust + 2 Python):
#   Rust  — canonical regression-gate cell + shape sweep (4) + size sweep (3)
#         + mode comparison (1) + wire-format sweep (1) + transport sweep (1)
#   Python — canonical burst (msgpack/tcp) + HTTP path (json/http)
#
# Per Plan 19-05 §Task 5.1; per CONTEXT.md `<specifics>` "Threshold goals (M4)":
# only the canonical cell (small + zipfian + continuous + msgpack + tcp + rust)
# blocks phase verification.
#
# Per Warning 9 deferral (Plan 19-03): Python harness is BURST-ONLY; continuous-
# mode Python is a Phase 19.1 (asyncio) follow-up.

set -uo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
LEDGER="$REPO_ROOT/.planning/throughput-baselines.md"
BENCH_BIN="$REPO_ROOT/target/release/beava-bench-v18"
SERVER_BIN="$REPO_ROOT/target/release/beava"
PYTHON_BLAST="$REPO_ROOT/python/benches/blast.py"
N_DEFAULT="${N:-1000000}"
PARALLEL_DEFAULT=16
PD_DEFAULT=1024
DATE="$(date -u +%Y-%m-%d)"
COMMIT="$(git rev-parse --short HEAD)"

# Build the bench binary + server in release. Plan 19-05 keeps this in the
# script body (rather than a separate task) so re-running the script always
# produces consistent rows even after a code edit.
echo "=== building beava-bench-v18 + beava (release) ==="
cargo build -p beava-bench --release --bin beava-bench-v18
cargo build -p beava-server --release --bin beava

# Helper: run one Rust cell, parse stderr, append a markdown row to LEDGER.
# `beava-bench-v18` boots its own in-process ServerV18 — no external server
# needed for Rust cells.
run_rust_cell() {
    local pipeline="$1" shape="$2" mode="$3" wire="$4" transport="$5" notes="$6"
    local cont_flag="true"
    if [[ "$mode" == "burst" ]]; then cont_flag="false"; fi

    echo ">>> RUST cell: $pipeline + $shape + $mode + $wire + $transport (notes=\"$notes\")"

    local out
    out=$("$BENCH_BIN" \
        --pipeline "$pipeline" \
        --transport "$transport" \
        --wire-format "$wire" \
        --blast-shape "$shape" \
        --total-events "$N_DEFAULT" \
        --duration-secs 60 \
        --parallel "$PARALLEL_DEFAULT" \
        --pipeline-depth "$PD_DEFAULT" \
        --continuous-pipeline "$cont_flag" \
        --no-ledger \
        --isolation-mode \
        2>&1) || {
            echo "FAIL: $pipeline + $shape + $mode + $wire + $transport"
            return 1
        }

    # Parse the invariant tuple + isolation columns from stderr (Plan 19-02 prints them).
    local pushed wall_ms send_ms ack_ms p50 p95 p99 rss eps
    pushed=$(grep -oE "pushed=[0-9]+"        <<<"$out" | head -1 | cut -d= -f2)
    wall_ms=$(grep -oE "wall_clock_ms=[0-9]+" <<<"$out" | head -1 | cut -d= -f2)
    send_ms=$(grep -oE "send_drain_ms=[0-9]+" <<<"$out" | head -1 | cut -d= -f2)
    ack_ms=$(grep  -oE "ack_lag_ms=[0-9]+"    <<<"$out" | head -1 | cut -d= -f2)
    rss=$(grep    -oE "peak_rss_mb:[ ]*[0-9]+" <<<"$out" | head -1 | grep -oE "[0-9]+")
    # P50/P95/P99 from the existing format_report human block (continuous mode only)
    if [[ "$mode" == "continuous" ]]; then
        # Format string: "push p50/p95/p99: NNN / NNN / NNN µs"
        local lat_line
        lat_line=$(grep -E "push p50/p95/p99:" <<<"$out" | head -1)
        if [[ -n "$lat_line" ]]; then
            p50=$(grep -oE "[0-9]+" <<<"$lat_line" | sed -n '1p')
            p95=$(grep -oE "[0-9]+" <<<"$lat_line" | sed -n '2p')
            p99=$(grep -oE "[0-9]+" <<<"$lat_line" | sed -n '3p')
        else
            p50="n/a"; p95="n/a"; p99="n/a"
        fi
    else
        p50="n/a"; p95="n/a"; p99="n/a"
    fi

    if [[ -z "${pushed:-}" || "$pushed" -eq 0 ]]; then
        echo "FAIL: pushed=0 for $pipeline+$shape+$mode+$wire+$transport"
        echo "--- bench output dump (first 4KB) ---"
        head -c 4096 <<<"$out"
        echo "--- end bench output dump ---"
        return 2
    fi

    # EPS = N / (wall_clock_ms / 1000)
    eps=$(awk -v n="$pushed" -v w="$wall_ms" 'BEGIN{ if (w>0) printf("%d", n*1000/w); else print "0" }')

    local row="| 19 | $DATE | $pipeline | $transport/$wire | $shape | $mode | rust | $PARALLEL_DEFAULT | $PD_DEFAULT | $N_DEFAULT | ${wall_ms:-n/a} | ${send_ms:-n/a} | ${ack_ms:-n/a} | $eps | $p50 | $p95 | $p99 | ${rss:-n/a} | $COMMIT | $notes |"
    echo "$row" | tee -a "$LEDGER"
}

# Helper: run one Python cell. Spawns target/release/beava (NOT the bench binary)
# to provide an external server target for the Python harness.
run_python_cell() {
    local pipeline="$1" shape="$2" mode="$3" wire="$4" transport="$5" notes="$6"

    echo ">>> PYTHON cell: $pipeline + $shape + $mode + $wire + $transport (notes=\"$notes\")"

    # Per Warning 9 fix in Plan 19-03: Python harness is BURST-ONLY (continuous
    # mode deferred to Phase 19.1 asyncio follow-up). Emit an explicit "n/a"
    # placeholder row for any caller that asks for continuous mode.
    if [[ "$mode" == "continuous" ]]; then
        echo "| 19 | $DATE | $pipeline | $transport/$wire | $shape | $mode | python | n/a | n/a | $N_DEFAULT | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | $COMMIT | python(burst-only) — D-05 continuous-mode deferred to Phase 19.1 (asyncio) |" | tee -a "$LEDGER"
        return 0
    fi

    # BLOCKER 4 fix (revision 1):
    # ===========================
    # The previous draft tried `cargo run -p beava-server -- --http-port 0 --tcp-port 0`
    # but `crates/beava-server/src/cli.rs` lines 14-18 declares ONLY `--config: PathBuf`.
    # clap rejects --http-port/--tcp-port as unrecognized arguments.
    # Furthermore the previous log-parse regex `"http listener bound on 127.0.0.1:[0-9]+"`
    # does NOT match the actual structured-tracing log output from
    # `crates/beava-server/src/server.rs:101-106` (kind=server.http_bound, addr=...) or
    # `crates/beava-server/src/server.rs:124-129` (kind=server.tcp_bound, addr=...).
    #
    # Correct approach (matches the working python/tests/bench/conftest.py pattern):
    # write a temp YAML config + use BEAVA_LISTEN_ADDR / BEAVA_TCP_PORT env vars
    # (port 0 for ephemeral) on both HTTP and TCP. Logs are JSON on stdout.
    #
    # Per crates/beava-core/src/config.rs:
    #   - Top-level `listen_addr` is the HTTP listen address (string "host:port")
    #   - Nested `tcp.host` + `tcp.port` (port 0 for ephemeral) controls TCP wire
    #   - `tcp.enabled = true` is the default; ensure it stays true
    #   - BEAVA_WAL_DIR / BEAVA_SNAPSHOT_DIR isolate per-cell WAL state
    local cfg_file wal_dir snap_dir
    cfg_file=$(mktemp /tmp/beava-blast-XXXXXX.yaml)
    wal_dir=$(mktemp -d /tmp/beava-blast-wal-XXXXXX)
    snap_dir=$(mktemp -d /tmp/beava-blast-snap-XXXXXX)
    # Remove the empty pre-created dirs — beava creates them itself and refuses
    # to start over an existing WAL directory ("File exists").
    rmdir "$wal_dir" "$snap_dir"

    cat > "$cfg_file" <<YAML
listen_addr: "127.0.0.1:0"
log_level: info
tcp:
  enabled: true
  host: "127.0.0.1"
  port: 0
YAML

    local srv_log
    srv_log=$(mktemp /tmp/beava-blast-log-XXXXXX)

    # Spawn the server with isolated WAL/snapshot dirs (BEAVA_WAL_DIR /
    # BEAVA_SNAPSHOT_DIR) so consecutive cells can't collide on disk state.
    # Explicit env-var overrides match the python/tests/bench/conftest.py pattern.
    BEAVA_WAL_DIR="$wal_dir" \
        BEAVA_SNAPSHOT_DIR="$snap_dir" \
        BEAVA_LISTEN_ADDR="127.0.0.1:0" \
        BEAVA_TCP_PORT="0" \
        BEAVA_DEV_ENDPOINTS="1" \
        "$SERVER_BIN" --config "$cfg_file" > "$srv_log" 2>&1 &
    local srv_pid=$!

    # Wait for the server to print its bound addresses (timeout 10s).
    # IMPORTANT: beava-server installs `tracing_subscriber::fmt::layer().json()` in
    # `crates/beava-server/src/logging.rs:49-55` — log lines are JSON.
    # Verified actual output (2026-04-26) matches python/tests/bench/conftest.py:
    #   {"timestamp":"...","level":"INFO","fields":{"message":"HTTP server bound","kind":"server.http_bound","addr":"127.0.0.1:NNNN"},"target":"beava.server"}
    #   {"timestamp":"...","level":"INFO","fields":{"message":"TCP wire listener bound","kind":"server.tcp_bound","addr":"127.0.0.1:NNNN"},"target":"beava.server"}
    # We grep on the JSON `"kind":"<discriminator>"` substring so HTTP and TCP
    # rows cannot cross-match (both have `"addr":"127.0.0.1:NNNN"` in the same line).
    # `flatten_event(true)` in logging.rs may emit either flattened JSON or
    # nested `"fields"`; both forms include the same kind/addr substrings, so the
    # grep is robust to either.
    local http_addr tcp_addr
    for _ in {1..100}; do
        # HTTP-bound line — JSON form: "kind":"server.http_bound" + "addr":"127.0.0.1:NNNN"
        http_addr=$(grep -E '"kind":"server\.http_bound"' "$srv_log" \
                    | grep -oE '"addr":"127\.0\.0\.1:[0-9]+"' \
                    | grep -oE '127\.0\.0\.1:[0-9]+' | head -1) || true
        # TCP-bound line — JSON form: "kind":"server.tcp_bound" (or "tcp.listener_bound" fallback)
        tcp_addr=$(grep -E '"kind":"(server\.tcp_bound|tcp\.listener_bound)"' "$srv_log" \
                   | grep -oE '"addr":"127\.0\.0\.1:[0-9]+"' \
                   | grep -oE '127\.0\.0\.1:[0-9]+' | head -1) || true
        if [[ -n "${http_addr:-}" && -n "${tcp_addr:-}" ]]; then break; fi
        sleep 0.1
    done
    if [[ -z "${http_addr:-}" || -z "${tcp_addr:-}" ]]; then
        echo "FAIL: could not parse server URLs from $srv_log"
        echo "--- srv_log dump (first 2KB) ---"; head -c 2048 "$srv_log"; echo; echo "--- end srv_log ---"
        echo "(tracing format may have changed — see crates/beava-server/src/logging.rs)"
        kill "$srv_pid" 2>/dev/null || true
        wait "$srv_pid" 2>/dev/null || true
        rm -f "$cfg_file" "$srv_log"
        rm -rf "$wal_dir" "$snap_dir"
        return 3
    fi
    # Regression-resistant assertion: both addresses must be non-empty AFTER the loop.
    # If a future tracing-format change breaks the regex, this turns into a loud failure
    # at the per-cell boundary instead of a silent "0 Python rows in ledger" regression.
    [[ -n "$http_addr" && -n "$tcp_addr" ]] || { echo "FAIL: empty addr after parse"; return 3; }

    echo "    server: http=$http_addr tcp=$tcp_addr (pid=$srv_pid)"

    # Number of Python workers — cpu_count - 1, floor 1.
    local n_cpu workers
    n_cpu=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
    workers=$(( n_cpu > 1 ? n_cpu - 1 : 1 ))

    local out rc
    out=$(python "$PYTHON_BLAST" \
        --total-events "$N_DEFAULT" \
        --blast-shape "$shape" \
        --transport "$transport" \
        --wire-format "$wire" \
        --pipeline "$pipeline" \
        --parallel "$workers" \
        --pipeline-depth "$PD_DEFAULT" \
        --no-ledger \
        --isolation-mode \
        --server-url "http://$http_addr,tcp://$tcp_addr" \
        2>&1)
    rc=$?
    kill "$srv_pid" 2>/dev/null || true
    wait "$srv_pid" 2>/dev/null || true
    rm -f "$cfg_file" "$srv_log"
    rm -rf "$wal_dir" "$snap_dir"

    if (( rc != 0 )); then
        echo "FAIL python rc=$rc"
        echo "--- python output (first 2KB) ---"; head -c 2048 <<<"$out"; echo; echo "--- end ---"
        return $rc
    fi

    local pushed wall_ms send_ms ack_ms eps
    pushed=$(grep -oE "pushed=[0-9]+"        <<<"$out" | head -1 | cut -d= -f2)
    wall_ms=$(grep -oE "wall_clock_ms=[0-9]+" <<<"$out" | head -1 | cut -d= -f2)
    send_ms=$(grep -oE "send_drain_ms=[0-9]+" <<<"$out" | head -1 | cut -d= -f2)
    ack_ms=$(grep  -oE "ack_lag_ms=[0-9]+"    <<<"$out" | head -1 | cut -d= -f2)
    if [[ -z "${pushed:-}" || "$pushed" -eq 0 ]]; then
        echo "FAIL: python pushed=0 for $pipeline+$shape+$mode+$wire+$transport"
        echo "--- python output dump (first 2KB) ---"; head -c 2048 <<<"$out"; echo; echo "--- end ---"
        return 4
    fi
    eps=$(awk -v n="$pushed" -v w="$wall_ms" 'BEGIN{ if (w>0) printf("%d", n*1000/w); else print "0" }')

    local row_notes="${notes:-python harness}"
    if [[ -z "$notes" ]]; then
        row_notes="python(burst-only) — D-05 continuous deferred to Phase 19.1 (asyncio)"
    else
        row_notes="$notes; python(burst-only) — D-05 continuous deferred to Phase 19.1"
    fi

    local row="| 19 | $DATE | $pipeline | $transport/$wire | $shape | $mode | python | $workers | $PD_DEFAULT | $N_DEFAULT | ${wall_ms:-n/a} | ${send_ms:-n/a} | ${ack_ms:-n/a} | $eps | n/a | n/a | n/a | n/a | $COMMIT | $row_notes |"
    echo "$row" | tee -a "$LEDGER"
}

# ─── MANDATORY MATRIX SUBSET (12 cells) ───────────────────────────────────────

echo "=== Phase 19 mandatory matrix subset (12 cells) ==="

# Rust cells (10 total)
# Canonical regression-gate cell — small + zipfian + continuous + msgpack + tcp + rust
run_rust_cell small zipfian continuous msgpack tcp "regression-gate cell"
# Shape sweep at canonical size (small + canonical mode + canonical wire/transport)
run_rust_cell small fixed   continuous msgpack tcp ""
run_rust_cell small uniform continuous msgpack tcp ""
run_rust_cell small mixed   continuous msgpack tcp ""
# Size sweep at canonical shape (zipfian + continuous + msgpack + tcp)
run_rust_cell medium       zipfian continuous msgpack tcp ""
run_rust_cell large        zipfian continuous msgpack tcp ""
run_rust_cell large_phase9 zipfian continuous msgpack tcp ""
# Mode comparison (continuous → burst, otherwise canonical)
run_rust_cell small zipfian burst      msgpack tcp ""
# Wire-format sweep (msgpack → json, otherwise canonical)
run_rust_cell small zipfian continuous json    tcp ""
# Transport sweep (tcp → http, json + http inherently — http transport ignores wire-format)
run_rust_cell small zipfian continuous json    http ""

# Python parity (2 mandatory cells — burst-only per Warning 9 deferral)
run_python_cell small zipfian burst msgpack tcp ""
run_python_cell small zipfian burst json    http ""

# WARNING 6 fix: emit a final count check so a missing/extra cell is loud.
echo "=== Phase 19 matrix run complete ==="
NEW_ROWS=$(grep -cE '^\| 19 \|' "$LEDGER")
echo "ledger now has $NEW_ROWS Phase 19 rows total (expected at least 12 from this run)"
