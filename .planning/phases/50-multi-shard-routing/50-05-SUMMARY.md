---
phase: 50-multi-shard-routing
plan: "05"
subsystem: shard-sockets
tags: [tpc, so-reuseport, socket2, per-shard-listener, macos-fallback]
dependency_graph:
  requires: [50-03]
  provides: [bind_reuseport_tcp, per_shard_socket_linux, single_listener_macos]
  affects: [src/server/tcp.rs, src/server/http.rs]
tech_stack:
  added: [socket2 = "0.5"]
  patterns: [SO_REUSEPORT + SO_REUSEADDR + nonblocking via socket2, cfg(target_os = "linux") gating]
key_files:
  created: []
  modified:
    - src/server/tcp.rs
    - src/server/http.rs
decisions:
  - "SO_REUSEPORT only on Linux (cfg guard); macOS uses single-listener dispatch (kernel ignores SO_REUSEPORT_LB in many configs)"
  - "build_shard_router delegates to build_router — identical middleware stack including require_loopback_or_token"
  - "two_reuseport_sockets_bind_same_port test gated behind cfg(target_os = 'linux')"
metrics:
  duration_minutes: 25
  completed: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  files_modified: 2
---

# Phase 50 Plan 05: SO_REUSEPORT Per-Shard Sockets (D-09, TPC-PERF-04) Summary

One-liner: Linux: each shard binds own TCP accept socket on shared port via SO_REUSEPORT (socket2); macOS: single-listener dispatch fallback; identical auth middleware on all shard routers.

## What Was Built

`src/server/tcp.rs`:
- `bind_reuseport_tcp(addr)` (cfg linux): socket2 socket with SO_REUSEPORT + SO_REUSEADDR + nonblocking + convert to std::net::TcpListener → tokio::net::TcpListener
- `two_reuseport_sockets_bind_same_port` test (cfg linux): verifies two sockets bind same port
- `run_tcp_server`: on Linux calls `bind_reuseport_tcp` per shard; on macOS uses single TcpListener

`src/server/http.rs`:
- `build_shard_router(state, _shard_index)`: delegates to `build_router(state)` — full middleware stack

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED
