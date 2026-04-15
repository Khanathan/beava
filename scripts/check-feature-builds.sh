#!/usr/bin/env bash
# Phase 28-01: local smoke test — every feature flavor builds; default tests green.
# Phase 41-01: also compile the `demo` flavor (gates `/public/recent-events`).
set -euo pipefail
echo "== building --no-default-features --features client (lib) =="
cargo build --no-default-features --features client --lib
echo "== building default (server) =="
cargo build
echo "== building --features demo (server + demo) =="
cargo build --features demo
echo "== running default tests =="
cargo test --quiet
echo "OK: every feature flavor builds; default tests green."
