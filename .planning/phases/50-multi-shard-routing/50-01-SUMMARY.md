---
phase: 50-multi-shard-routing
plan: "01"
subsystem: metrics
tags: [tpc, prometheus, metrics-crate, parallel-emit]
dependency_graph:
  requires: []
  provides: [prometheus_recorder, parallel_metrics_emit]
  affects: [src/metrics/mod.rs, src/metrics/prometheus.rs, src/server/http.rs, src/main.rs, Cargo.toml, src/lib.rs]
tech_stack:
  added: [metrics = "0.24", metrics-exporter-prometheus = "0.16"]
  patterns: [OnceLock global PrometheusHandle, install_recorder() API (not build() Future)]
key_files:
  created:
    - src/metrics/mod.rs
    - src/metrics/prometheus.rs
  modified:
    - Cargo.toml
    - src/lib.rs
    - src/main.rs
    - src/server/http.rs
decisions:
  - "Used install_recorder() not build() — v0.16.2 build() returns (recorder, Pin<Box<Future>>) not (recorder, handle); install_recorder() sets global recorder and returns PrometheusHandle directly"
  - "prometheus.rs test uses build_recorder().handle() to avoid setting global recorder in unit tests"
metrics:
  duration_minutes: 30
  completed: "2026-04-18T00:00:00Z"
  tasks_completed: 2
  files_modified: 6
---

# Phase 50 Plan 01: Prometheus Recorder + Parallel /metrics Emit Summary

One-liner: Global OnceLock PrometheusRecorder installed at startup; /metrics endpoint appends metrics-exporter-prometheus scrape after hand-rolled block (D-06 parallel period).

## What Was Built

- `src/metrics/mod.rs`: `install_prometheus_recorder()` (idempotent OnceLock guard), `handle()` returns `Option<&'static PrometheusHandle>`
- `src/metrics/prometheus.rs`: `PrometheusHandle { inner }` wrapping `metrics_exporter_prometheus::PrometheusHandle`; `scrape()` calls `inner.render()`
- `src/main.rs`: `beava::metrics::install_prometheus_recorder()` called at top of `async_main()`
- `src/server/http.rs` `/metrics` endpoint: after hand-rolled block, appends `handle().scrape()` if non-empty
- Cargo.toml: `metrics = "0.24"`, `metrics-exporter-prometheus = "0.16"`, `core_affinity = "0.8.3"`, `crossbeam-channel = "0.5.15"`, `crossbeam-utils = "0.8"`, `socket2 = "0.5"`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] PrometheusBuilder::build() API mismatch**
- `build()` in v0.16.2 returns `(PrometheusRecorder, Pin<Box<Future>>)`, not `(recorder, handle)` as plan expected
- Fixed by using `install_recorder()` which calls `set_global_recorder` and returns `PrometheusHandle` directly

## Self-Check: PASSED
