#!/usr/bin/env bash
# Phase 28-01: local smoke test — both feature flavors build; default tests green.
set -euo pipefail
echo "== building --no-default-features --features client (lib) =="
cargo build --no-default-features --features client --lib
echo "== building default (server) =="
cargo build
echo "== running default tests =="
cargo test --quiet
echo "OK: both feature flavors build; default tests green."
