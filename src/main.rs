#![cfg(feature = "server")]

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use beava::engine::pipeline::PipelineEngine;
use beava::server::auth::resolve_tcp_bind;
use beava::server::http::run_http_server;
use beava::server::protocol::{
    convert_register_request, convert_view_register_request, RegisterRequest,
};
use beava::server::replica_client::{ReplicaBootConfig, ReplicaClient};
use beava::server::tcp::{
    make_concurrent_state_full, run_backfill, run_tcp_server, BackfillStatus, BackfillTracker,
    SharedState,
};
use beava::state::event_log::EventLog;
use beava::state::eviction::evict_expired_keys;
use beava::state::snapshot::{
    load_legacy_v5, load_snapshot_file, save_base_snapshot, save_delta_snapshot, BaseSnapshotState,
    DeltaSnapshotState, SerializablePipeline, SnapshotFile, SnapshotHeader, SnapshotState,
    SnapshotType,
};
use beava::state::store::StateStore;

/// Local enum used by the periodic snapshot timer to pass a fully-prepared
/// snapshot payload (base or delta) into the blocking serialization task.
enum SnapshotData {
    Base(BaseSnapshotState),
    Delta(DeltaSnapshotState),
}

/// Phase 25-02: Poll every non-event-driven signal source on the snapshot
/// cycle (default 30s). Emitters dedupe by stable id, so repeat calls are
/// free. Called from the periodic snapshot task after each write attempt.
fn poll_signal_sources(state: &SharedState) {
    use beava::server::signals;

    let now = SystemTime::now();

    // 1. Late-event drop rate (data_quality). Pull the per-stream counter
    //    from the pipeline engine's shared `late_drops` map and let the
    //    emitter compute a per-second rate against the previous sample.
    //    Threshold: 1 drop/sec default (placeholder SLO per CONTEXT).
    let drops: Vec<(String, u64)> = {
        let engine = state.engine.read();
        
        engine.late_drops.snapshot()
    };
    signals::emit_late_drop_signals(&state.signals, &drops, now, 1.0);

    // 2. Memory pressure (operational). `BEAVA_MEMORY_LIMIT_MB` env var
    //    drives the threshold; if unset the emitter is a no-op.
    let limit_bytes = std::env::var("BEAVA_MEMORY_LIMIT_MB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(|mb| mb * 1_048_576);
    signals::emit_memory_pressure_signal(&state.signals, limit_bytes);

    // Phase 43 T4: flip the reject-writes flag when RSS exceeds the limit;
    // clear it again under 95% (5% hysteresis prevents flapping). On
    // macOS dev boxes sample_rss_bytes returns None and the flag stays
    // false — memory gating is a Linux-production feature.
    signals::update_memory_ceiling_flag(&state.at_memory_ceiling, limit_bytes);

    // 3. PUSH p99 SLO breach (performance). Sample from the latency
    //    tracker; threshold 1ms (10× the CLAUDE.md 100µs design target).
    let p99_us = state
        .latency
        .lock()
        .push_percentile_us(99.0, std::time::Instant::now());
    signals::emit_perf_p99_signal(&state.signals, p99_us, 1000.0);

    // 4. Plan 25-03: fan config recommendations into the registry as
    //    Category::Config / Severity::Info. `recommend_config` is already
    //    deterministic and idempotent; emitting its output through the same
    //    registry path gives the UI one feed for everything.
    let recs = {
        let engine = state.engine.read();
        beava::engine::recommend::recommend_config(&engine, &state.eviction_tracker)
    };
    signals::emit_config_recommendations(&state.signals, &recs);
}

/// Phase 37-01: fork-synthesized replica config. Populated by `main()` when
/// `beava fork ...` is invoked; consumed by `async_main` in place of the
/// normal `--replica-*` parsing path.
static FORK_CONFIG: std::sync::OnceLock<ReplicaBootConfig> = std::sync::OnceLock::new();

fn main() {
    // Phase 37-01: handle `beava fork` subcommand. Parse fork flags, set
    // BEAVA_TCP_PORT / BEAVA_HTTP_PORT for the local-port override, print
    // the banner, and stash the synthesized replica config for async_main.
    if is_fork_subcommand() {
        let args: Vec<String> = std::env::args().collect();
        let cfg = match parse_fork_args_from(&args) {
            Ok(cfg) => cfg,
            Err(e) if e == "__HELP__" => {
                print_fork_help();
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("{}", e);
                eprintln!("Run `beava fork --help` for usage.");
                std::process::exit(2);
            }
        };
        // Re-read the local-port from the args to drive env overrides
        // (it lives on the fork config only implicitly — we stashed it by
        // setting block_until_catchup=true and by writing env vars now).
        let local_port_raw = args
            .iter()
            .skip(2)
            .enumerate()
            .find_map(|(i, a)| {
                if a == "--local-port" {
                    args.get(i + 3).cloned()
                } else { a.strip_prefix("--local-port=").map(|rest| rest.to_string()) }
            })
            .unwrap_or_else(|| "7400".into());
        // `--local-port` is the scientist-facing HTTP port (tl.Client,
        // /debug/ready, /debug/key/*). TCP is +1 for the raw protocol
        // (needed to reject local PUSHes and for future SUBSCRIBE clients).
        let local_http: u16 = local_port_raw.parse().unwrap_or(7400);
        let local_tcp = local_http.saturating_add(1);
        std::env::set_var("BEAVA_HTTP_PORT", &local_port_raw);
        std::env::set_var("BEAVA_TCP_PORT", local_tcp.to_string());
        eprintln!(
            "beava fork — remote={} scope={:?} since_ms={} -> http://localhost:{} (tcp :{})",
            cfg.remote, cfg.streams, cfg.since_millis, local_http, local_tcp
        );
        FORK_CONFIG
            .set(cfg)
            .expect("FORK_CONFIG must be set exactly once");
    }

    let worker_threads: usize = std::env::var("BEAVA_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(worker_threads);
    builder.enable_all();
    let runtime = builder.build().expect("failed to build tokio runtime");
    eprintln!("Worker threads: {}", worker_threads);
    runtime.block_on(async_main());
}

/// Phase 20: minimal CLI arg lookup. Scans `std::env::args()` for
/// `--<name> <value>` or `--<name>=<value>`; returns the first match. Boolean
/// flags use `arg_flag(name)`. We deliberately avoid pulling in `clap` for one
/// or two flags.
fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    let long = format!("--{}", name);
    let long_eq = format!("--{}=", name);
    while let Some(a) = args.next() {
        if a == long {
            return args.next();
        }
        if let Some(rest) = a.strip_prefix(&long_eq) {
            return Some(rest.to_string());
        }
    }
    None
}

fn arg_flag(name: &str) -> bool {
    let long = format!("--{}", name);
    std::env::args().skip(1).any(|a| a == long)
}

/// Phase 37-01: detect `beava fork` subcommand — looks at `args[1]`.
fn is_fork_subcommand() -> bool {
    std::env::args().nth(1).as_deref() == Some("fork")
}

/// Phase 37-01: `beava fork` is a scientist-facing wrapper around the
/// Phase 36 replica-mode boot. It parses its own flag set (`--remote`,
/// `--streams`, etc.), translates them into a `ReplicaBootConfig`, and
/// sets `BEAVA_TCP_PORT` / `BEAVA_HTTP_PORT` env vars so downstream code
/// binds the listener on the scientist-requested local port.
///
/// `args` is the full argv slice (args[0] = binary, args[1] = "fork",
/// remainder = fork flags). Pulled out so we can unit-test flag parsing
/// without spawning a subprocess.
fn parse_fork_args_from(args: &[String]) -> Result<ReplicaBootConfig, String> {
    // Mini arg reader that scans `args[2..]` (skip binary + "fork").
    fn get(args: &[String], name: &str) -> Option<String> {
        let long = format!("--{}", name);
        let long_eq = format!("--{}=", name);
        let mut it = args.iter().skip(2);
        while let Some(a) = it.next() {
            if a == &long {
                return it.next().cloned();
            }
            if let Some(rest) = a.strip_prefix(&long_eq) {
                return Some(rest.to_string());
            }
        }
        None
    }
    fn has_help(args: &[String]) -> bool {
        args.iter()
            .skip(2)
            .any(|a| a == "--help" || a == "-h")
    }

    if has_help(args) {
        return Err("__HELP__".into());
    }

    let remote = get(args, "remote")
        .ok_or_else(|| "beava fork: --remote HOST:PORT required".to_string())?;
    let streams_raw = get(args, "streams")
        .ok_or_else(|| "beava fork: --streams s1,s2,... required".to_string())?;
    let streams: Vec<String> = streams_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if streams.is_empty() {
        return Err("beava fork: --streams must list at least one stream".into());
    }
    // `--since` defaults to full history.
    let since_raw = get(args, "since").unwrap_or_else(|| "1970-01-01T00:00:00Z".into());
    let since_millis = parse_replica_since(&since_raw)
        .map_err(|e| format!("beava fork: bad --since: {}", e))?;
    let keys = get(args, "keys").map(|s| {
        s.split(',')
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect::<Vec<_>>()
    });
    let key_prefix = get(args, "key-prefix");
    if keys.is_some() && key_prefix.is_some() {
        return Err("beava fork: --keys and --key-prefix are mutually exclusive".into());
    }
    let token = get(args, "token")
        .or_else(|| std::env::var("BEAVA_REPLICA_TOKEN").ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "beava fork: --token (or BEAVA_REPLICA_TOKEN env var) required".to_string()
        })?;
    // `--local-port` defaults to 7400.
    let local_port_raw = get(args, "local-port").unwrap_or_else(|| "7400".into());
    let local_port: u16 = local_port_raw
        .parse()
        .map_err(|_| format!("beava fork: bad --local-port '{}'", local_port_raw))?;
    if local_port == 0 {
        return Err("beava fork: --local-port must be > 0".into());
    }
    let pipeline_file = get(args, "pipeline-file").map(std::path::PathBuf::from);
    // Phase 44-01: scientist-facing `--extract-at T1,T2,...` → translate to
    // the replica-mode `extract_at_millis` list. Each entry accepts the
    // same ISO-8601 / u64-ms shapes as `--since`.
    let extract_at_millis: Vec<u64> = match get(args, "extract-at") {
        Some(raw) => {
            let mut out = Vec::new();
            for tok in raw.split(',') {
                let t = tok.trim();
                if t.is_empty() {
                    continue;
                }
                let ms = parse_replica_since(t).map_err(|e| {
                    format!("beava fork: bad --extract-at entry '{}': {}", t, e)
                })?;
                out.push(ms);
            }
            out.sort_unstable();
            out
        }
        None => Vec::new(),
    };

    Ok(ReplicaBootConfig {
        remote,
        since_millis,
        streams,
        keys,
        key_prefix,
        token,
        block_until_catchup: true,
        pipeline_file,
        extract_at_millis,
    })
}

fn print_fork_help() {
    eprintln!(
        "usage: beava fork --remote HOST:PORT --streams s1,s2 [OPTIONS]\n\
         \n\
         Scoped local replica for scientists. Wraps `beava --replica-*` with\n\
         scientist-friendly defaults: blocks until catchup, picks a loopback\n\
         local port, defaults `--since` to full history.\n\
         \n\
         Required flags:\n\
           --remote HOST:PORT      Upstream beava cluster.\n\
           --streams s1,s2,...     Streams to replicate.\n\
           --token TOKEN           Admin token (or BEAVA_REPLICA_TOKEN env).\n\
         \n\
         Optional flags:\n\
           --since TS              ISO-8601 UTC or u64 ms; default 1970-01-01T00:00:00Z.\n\
           --keys k1,k2            Exact keys to replicate (mutex with --key-prefix).\n\
           --key-prefix P          Prefix filter (mutex with --keys).\n\
           --local-port PORT       TCP + HTTP bind port; default 7400.\n\
           --pipeline-file PATH    REGISTER JSON file (single object or array).\n\
                                   Same shape as HTTP POST /pipelines.\n\
           --extract-at T1,T2,...  Historical extraction timestamps (ISO-8601\n\
                                   UTC or u64 ms). Replay captures per-scope-\n\
                                   key feature state as it crosses each Tᵢ;\n\
                                   query via GET /extracts after catchup.\n\
         \n\
         Python-authored pipelines: launch `beava fork` without --pipeline-file,\n\
         then from Python run `tl.register(pipeline, remote=\"localhost:PORT\")`\n\
         after the fork prints its ready banner.\n"
    );
}

/// Phase 36-01: parse `--replica-since` as either ISO-8601 UTC or raw
/// u64 milliseconds. Ambiguous / unparseable inputs return an Err that
/// `async_main` surfaces with a clear error message.
///
/// Supported formats:
///   * `1712345678000` — plain u64 milliseconds since UNIX epoch.
///   * `2026-04-14T12:34:56Z` — ISO-8601 UTC; subsecond `.fff` optional.
///   * `2026-04-14T12:34:56.789Z` — with millisecond fraction.
fn parse_replica_since(input: &str) -> Result<u64, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty --replica-since".into());
    }
    // Numeric: u64 ms since epoch. Bare integer — no suffix.
    if s.chars().all(|c| c.is_ascii_digit()) {
        return s
            .parse::<u64>()
            .map_err(|e| format!("invalid u64 ms: {}", e));
    }
    // ISO-8601 UTC. We hand-roll a parser rather than pull in chrono —
    // the expected shape is YYYY-MM-DDTHH:MM:SS[.fff]Z.
    let s = s.strip_suffix('Z').ok_or_else(|| {
        format!(
            "--replica-since: '{}' is neither u64 ms nor ISO-8601 UTC (missing trailing Z)",
            s
        )
    })?;
    let (date_part, time_part) = s
        .split_once('T')
        .ok_or_else(|| format!("--replica-since: missing 'T' separator in '{}'", s))?;
    let date_parts: Vec<&str> = date_part.split('-').collect();
    if date_parts.len() != 3 {
        return Err(format!("--replica-since: bad date in '{}'", date_part));
    }
    let year: i32 = date_parts[0]
        .parse()
        .map_err(|_| format!("--replica-since: bad year '{}'", date_parts[0]))?;
    let month: u32 = date_parts[1]
        .parse()
        .map_err(|_| format!("--replica-since: bad month '{}'", date_parts[1]))?;
    let day: u32 = date_parts[2]
        .parse()
        .map_err(|_| format!("--replica-since: bad day '{}'", date_parts[2]))?;
    let (hms, fraction_ms): (&str, u64) = match time_part.split_once('.') {
        Some((hms, frac)) => {
            // Accept 1..=9 digit fraction; truncate/pad to ms.
            if !frac.chars().all(|c| c.is_ascii_digit()) || frac.is_empty() {
                return Err(format!("--replica-since: bad fraction '{}'", frac));
            }
            let padded: String = frac.chars().take(3).collect();
            let padded = format!("{:0<3}", padded);
            (hms, padded.parse::<u64>().unwrap_or(0))
        }
        None => (time_part, 0),
    };
    let hms_parts: Vec<&str> = hms.split(':').collect();
    if hms_parts.len() != 3 {
        return Err(format!("--replica-since: bad time '{}'", hms));
    }
    let hour: u32 = hms_parts[0]
        .parse()
        .map_err(|_| format!("--replica-since: bad hour '{}'", hms_parts[0]))?;
    let minute: u32 = hms_parts[1]
        .parse()
        .map_err(|_| format!("--replica-since: bad minute '{}'", hms_parts[1]))?;
    let second: u32 = hms_parts[2]
        .parse()
        .map_err(|_| format!("--replica-since: bad second '{}'", hms_parts[2]))?;
    // Convert to unix timestamp. Days-from-civil algorithm (Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let m = month as i32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u32;
    let days_since_epoch: i64 = (era * 146_097 + doe as i32 - 719_468) as i64;
    let secs: i64 = days_since_epoch * 86_400
        + hour as i64 * 3600
        + minute as i64 * 60
        + second as i64;
    if secs < 0 {
        return Err(format!("--replica-since: pre-epoch timestamp '{}'", input));
    }
    Ok((secs as u64) * 1_000 + fraction_ms)
}

/// Phase 36-01: parse all `--replica-*` CLI flags (and env-var fallbacks)
/// into a `ReplicaBootConfig`. Returns:
///   * `Ok(None)` if `--replica-from` is absent — server stays in normal
///     (non-replica) mode.
///   * `Ok(Some(config))` on valid replica-mode flags.
///   * `Err(msg)` on flag validation errors (missing required flag,
///     unparseable --replica-since, both --replica-keys and
///     --replica-key-prefix set, etc.).
fn parse_replica_boot_config() -> Result<Option<ReplicaBootConfig>, String> {
    let remote = match arg_value("replica-from") {
        Some(v) => v,
        None => return Ok(None),
    };
    let since_raw = arg_value("replica-since")
        .ok_or_else(|| "--replica-from set but --replica-since missing".to_string())?;
    let since_millis = parse_replica_since(&since_raw)?;
    let streams_raw = arg_value("replica-streams")
        .ok_or_else(|| "--replica-from set but --replica-streams missing".to_string())?;
    let streams: Vec<String> = streams_raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if streams.is_empty() {
        return Err("--replica-streams must list at least one stream".into());
    }
    let keys = arg_value("replica-keys").map(|s| {
        s.split(',')
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty())
            .collect::<Vec<_>>()
    });
    let key_prefix = arg_value("replica-key-prefix");
    if keys.is_some() && key_prefix.is_some() {
        return Err("--replica-keys and --replica-key-prefix are mutually exclusive".into());
    }
    let token = arg_value("replica-token")
        .or_else(|| std::env::var("BEAVA_REPLICA_TOKEN").ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "--replica-token (or BEAVA_REPLICA_TOKEN env var) required in replica mode".to_string()
        })?;
    // Default true per 36-CONTEXT.md §Listener gating.
    let block_until_catchup = match arg_value("replica-block-until-catchup") {
        Some(v) => !matches!(v.as_str(), "false" | "0" | "no"),
        None => true,
    };
    let pipeline_file = arg_value("replica-pipeline-file").map(std::path::PathBuf::from);
    // Phase 44-01: `--replica-extract-at T1,T2,...` — historical extraction
    // timestamps. Each entry parsed via `parse_replica_since` (ISO-8601 UTC
    // or raw u64 ms). Sorted ascending; empty when flag absent.
    let extract_at_millis: Vec<u64> = match arg_value("replica-extract-at") {
        Some(raw) => {
            let mut out = Vec::new();
            for tok in raw.split(',') {
                let t = tok.trim();
                if t.is_empty() {
                    continue;
                }
                let ms = parse_replica_since(t).map_err(|e| {
                    format!("--replica-extract-at: bad entry '{}': {}", t, e)
                })?;
                out.push(ms);
            }
            out.sort_unstable();
            out
        }
        None => Vec::new(),
    };
    Ok(Some(ReplicaBootConfig {
        remote,
        since_millis,
        streams,
        keys,
        key_prefix,
        token,
        block_until_catchup,
        pipeline_file,
        extract_at_millis,
    }))
}

/// Phase 36-01: register pipelines from a JSON file before catchup begins.
/// Accepts either a single REGISTER object or a JSON array of them.
fn seed_pipelines_from_file(
    state: &SharedState,
    path: &std::path::Path,
) -> Result<usize, String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    let doc: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("parse {}: {}", path.display(), e))?;
    let items: Vec<serde_json::Value> = match doc {
        serde_json::Value::Array(a) => a,
        other @ serde_json::Value::Object(_) => vec![other],
        _ => return Err("pipeline file must be object or array of objects".into()),
    };
    let mut n = 0usize;
    let mut engine = state.engine.write();
    for item in items {
        // Plan 39-01 (T1 follow-up): mirror the v0-vs-legacy dispatch from the
        // OP_REGISTER TCP handler (src/server/tcp.rs::Command::Register).
        // The Python SDK emits v0-shape REGISTER docs (`kind:"stream"|"table"`,
        // `aggregation:{...}`) which serde_json::from_value::<RegisterRequest>
        // can't parse — legacy path expects `features:[...]`. Route through
        // V0RegisterPayload::parse → v0_*_to_stream_def when `kind` is present.
        let is_v0 = item.get("kind").is_some();
        let name = if is_v0 {
            let v0_bytes = serde_json::to_vec(&item)
                .map_err(|e| format!("v0 REGISTER: re-serialize failed: {}", e))?;
            let parsed = beava::engine::register::V0RegisterPayload::parse(&v0_bytes)
                .map_err(|e| format!("parse v0 REGISTER: {}", e))?;
            let stream_def = match &parsed {
                beava::engine::register::V0RegisterPayload::Source(desc) => {
                    beava::engine::register::v0_source_to_stream_def(desc)
                        .map_err(|e| format!("v0 source → stream_def: {}", e))?
                }
                beava::engine::register::V0RegisterPayload::Aggregation(desc) => {
                    beava::engine::register::v0_aggregation_to_stream_def(desc)
                        .map_err(|e| format!("v0 aggregation → stream_def: {}", e))?
                }
                other => {
                    return Err(format!(
                        "v0 REGISTER: descriptor kind '{}' not supported in pipeline-file seed \
                         (joins/stateless-chains/union come later)",
                        other.descriptor_kind()
                    ));
                }
            };
            let def_name = stream_def.name.clone();
            engine
                .register(stream_def)
                .map_err(|e| format!("register v0 stream {}: {}", def_name, e))?;
            let history_ttl = engine.get_stream(&def_name).and_then(|s| s.history_ttl);
            if let Some(ref log) = state.event_log {
                let _ = log.register_stream(&def_name, history_ttl);
            }
            def_name
        } else {
            let req: RegisterRequest = serde_json::from_value(item.clone())
                .map_err(|e| format!("bad register JSON: {}", e))?;
            let name = req.name.clone();
            let is_view = req.definition_type.as_deref() == Some("view");
            if is_view {
                let view_def = convert_view_register_request(req)
                    .map_err(|e| format!("convert view: {}", e))?;
                engine
                    .register_view(view_def)
                    .map_err(|e| format!("register view {}: {}", name, e))?;
            } else {
                let stream_def = convert_register_request(req)
                    .map_err(|e| format!("convert stream: {}", e))?;
                engine
                    .register(stream_def)
                    .map_err(|e| format!("register stream {}: {}", name, e))?;
                // Also register with the event log so replicated PUSHes persist.
                let history_ttl = engine
                    .get_stream(&name)
                    .and_then(|s| s.history_ttl);
                if let Some(ref log) = state.event_log {
                    let _ = log.register_stream(&name, history_ttl);
                }
            }
            name
        };
        engine.store_raw_register_json(&name, item);
        n += 1;
    }
    Ok(n)
}

async fn async_main() {
    let tcp_port = std::env::var("BEAVA_TCP_PORT").unwrap_or_else(|_| "6400".into());
    let http_port = std::env::var("BEAVA_HTTP_PORT").unwrap_or_else(|_| "6401".into());
    let snapshot_path = PathBuf::from(
        std::env::var("BEAVA_SNAPSHOT_PATH").unwrap_or_else(|_| "beava.snapshot".into()),
    );
    let ttl_multiplier: u32 = std::env::var("BEAVA_TTL_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let event_log_enabled = std::env::var("BEAVA_EVENT_LOG")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);
    let snapshot_enabled = std::env::var("BEAVA_SNAPSHOT")
        .map(|v| v != "false" && v != "0")
        .unwrap_or(true);

    // Phase 20 (TRAC-05): TCP listener defaults to loopback so the raw TCP
    // protocol (PUSH/SET/MSET/REGISTER) is never reachable on the public
    // internet unless the operator opts in via `--tcp-bind 0.0.0.0`.
    let tcp_bind_env = std::env::var("BEAVA_TCP_BIND").ok();
    let tcp_bind_cli = arg_value("tcp-bind");
    let tcp_addr = resolve_tcp_bind(tcp_bind_env.as_deref(), tcp_bind_cli.as_deref(), &tcp_port);
    // HTTP continues to bind 0.0.0.0 — it is the public surface (deploy/Caddyfile
    // further restricts at the edge; admin routes are middleware-gated).
    let http_addr = format!("0.0.0.0:{}", http_port);

    // Phase 20: admin bearer token (TRAC-05). Presence is optional — without
    // one, admin routes only work from loopback. Public demo hosts set this so
    // ops can call admin routes through the Caddy reverse-proxy.
    let admin_token = std::env::var("BEAVA_ADMIN_TOKEN").ok().filter(|s| !s.is_empty());
    // Phase 20: public-mode toggle (TRAC-06). When set, `GET /` serves
    // `demo.html` from the embed root. Otherwise it serves the debug UI.
    let public_mode = arg_flag("public-mode")
        || std::env::var("BEAVA_PUBLIC_MODE")
            .map(|v| v != "false" && v != "0")
            .unwrap_or(false);

    // Initialize event log directory (skip if disabled)
    let event_log = if event_log_enabled {
        let event_log_dir =
            PathBuf::from(std::env::var("BEAVA_DATA_DIR").unwrap_or_else(|_| ".".into()))
                .join("events");
        EventLog::new(event_log_dir).map(Some).unwrap_or_else(|e| {
            eprintln!("Failed to initialize event log: {}", e);
            None
        })
    } else {
        eprintln!("Event log: disabled");
        None
    };

    // Phase 14: ConcurrentAppState with per-field locking.
    // Phase 20: also carries admin_token + public_mode.
    let state: SharedState = make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        event_log,
        snapshot_path.clone(),
        Arc::new(BackfillTracker::default()),
        snapshot_enabled,
        event_log_enabled,
        admin_token,
        public_mode,
    );

    // Phase 9: how often to write a full base snapshot. Every Nth cycle is a
    // base, all other cycles are deltas. Default 10 (= one base per ~5 minutes
    // at the default 30s interval).
    let full_snapshot_interval: u64 = std::env::var("BEAVA_FULL_SNAPSHOT_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // Load snapshot on startup -- incremental recovery (OPS-04).
    // Skip if snapshots are disabled.
    let recovery = if snapshot_enabled {
        let snap_dir_startup = snapshot_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        load_incremental_snapshots(&snap_dir_startup, &snapshot_path)
    } else {
        eprintln!("Snapshots: disabled");
        None
    };
    if let Some((snapshot_state, next_seq, loaded_base_seq)) = recovery {
        *state.snapshot_seq.lock() = next_seq;
        *state.last_base_seq.lock() = loaded_base_seq;
        *state.previous_base_seq.lock() = 0;

        // Restore entity state
        state.store.restore_from_snapshot(snapshot_state.entities);
        // Clear any dirty/deleted tracking
        state.store.clear_dirty();
        let _ = state.store.take_deleted();

        // Re-register pipelines from stored JSON
        {
            let mut engine = state.engine.write();
            for pipeline in snapshot_state.pipelines {
                let parsed: Result<serde_json::Value, _> =
                    serde_json::from_str(&pipeline.raw_register_json);
                if let Ok(json_val) = parsed {
                    let req: Result<RegisterRequest, _> = serde_json::from_value(json_val.clone());
                    if let Ok(req) = req {
                        let def_name = req.name.clone();
                        let is_view = req.definition_type.as_deref() == Some("view");
                        let registered: Result<(), beava::error::BeavaError> = if is_view {
                            convert_view_register_request(req)
                                .and_then(|view_def| engine.register_view(view_def))
                        } else {
                            convert_register_request(req)
                                .and_then(|stream_def| engine.register(stream_def).map(|_diff| ()))
                        };
                        if registered.is_ok() {
                            engine.store_raw_register_json(&def_name, json_val);
                            // Register stream with event log for persistence
                            if !is_view {
                                let history_ttl =
                                    engine.get_stream(&def_name).and_then(|s| s.history_ttl);
                                if let Some(ref log) = state.event_log {
                                    let _ = log.register_stream(&def_name, history_ttl);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Restore backfill_complete markers from snapshot
        {
            let mut bc = state.backfill_complete.lock();
            for (stream, feature) in &snapshot_state.backfill_complete {
                bc.insert((stream.clone(), feature.clone()));
            }
        }

        // Phase 9 WR-05: one-shot GC pass.
        {
            let engine = state.engine.read();
            let valid_features = engine.valid_features_map();
            state.store.gc_invalid_operators(&valid_features);
        }

        // Detect incomplete backfills
        let mut incomplete_backfills: Vec<(String, Vec<String>)> = Vec::new();
        {
            let engine = state.engine.read();
            let bc = state.backfill_complete.lock();
            for stream in engine.list_streams() {
                let missing: Vec<String> = stream
                    .features
                    .iter()
                    .filter(|(_, def)| beava::engine::pipeline::get_backfill_flag(def))
                    .filter(|(name, _)| !bc.contains(&(stream.name.clone(), name.clone())))
                    .map(|(name, _)| name.clone())
                    .collect();
                if !missing.is_empty() {
                    incomplete_backfills.push((stream.name.clone(), missing));
                }
            }
        }

        eprintln!("Loaded snapshot (next_seq={})", next_seq);

        // Spawn backfill tasks for incomplete backfills
        for (stream_name, features) in incomplete_backfills {
            let entries = state
                .event_log
                .as_ref()
                .map(|log| log.read_entries(&stream_name).unwrap_or_default())
                .unwrap_or_default();
            if !entries.is_empty() {
                let status = Arc::new(BackfillStatus {
                    stream: stream_name.clone(),
                    features: features.clone(),
                    total_events: entries.len(),
                    processed_events: Arc::new(AtomicUsize::new(0)),
                    started_at: SystemTime::now(),
                    completed_at: std::sync::Mutex::new(None),
                });
                state
                    .backfill_tracker
                    .tasks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(Arc::clone(&status));
                eprintln!(
                    "Resuming incomplete backfill for {} features: {:?}",
                    stream_name, features
                );
                tokio::spawn(run_backfill(
                    state.clone(),
                    stream_name,
                    features,
                    entries,
                    status,
                ));
            }
        }
    }

    // Phase 36-01: if the process was launched with `--replica-from`,
    // parse the replica-mode flags, seed pipelines from the optional
    // `--replica-pipeline-file`, and spawn the replica-client loop.
    // Listener startup waits on `catchup_done_rx` when
    // `--replica-block-until-catchup=true` (the default).
    let replica_boot = if let Some(cfg) = FORK_CONFIG.get() {
        // Phase 37-01: `beava fork` path — config was synthesized in main().
        Some(cfg.clone())
    } else {
        match parse_replica_boot_config() {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Replica-mode CLI error: {}", e);
                std::process::exit(1);
            }
        }
    };
    let catchup_rx: Option<tokio::sync::oneshot::Receiver<()>> = if let Some(ref cfg) =
        replica_boot
    {
        eprintln!(
            "Replica mode: from={} since_ms={} streams={:?} keys={:?} key_prefix={:?} block_until_catchup={}",
            cfg.remote,
            cfg.since_millis,
            cfg.streams,
            cfg.keys,
            cfg.key_prefix,
            cfg.block_until_catchup
        );
        state
            .replica_mode
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // Seed pipelines from file before catchup starts so the ingested
        // events flow through the scientist's registered aggregates.
        if let Some(ref pf) = cfg.pipeline_file {
            match seed_pipelines_from_file(&state, pf) {
                Ok(n) => eprintln!("Replica: seeded {} pipelines from {}", n, pf.display()),
                Err(e) => {
                    eprintln!("Replica: pipeline-file error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        let (catchup_tx, catchup_rx) = tokio::sync::oneshot::channel();
        let block = cfg.block_until_catchup;
        let client = ReplicaClient::new(cfg.clone(), state.clone(), catchup_tx);
        tokio::spawn(async move {
            if let Err(e) = client.run().await {
                eprintln!("Replica client FATAL: {}", e);
                std::process::exit(1);
            }
        });
        if block {
            Some(catchup_rx)
        } else {
            None
        }
    } else {
        None
    };

    // Gate listeners on catchup-done if requested.
    if let Some(rx) = catchup_rx {
        let _ = rx.await;
    }

    let tcp_state = state.clone();
    let tcp_handle = tokio::spawn(async move {
        if let Err(e) = run_tcp_server(&tcp_addr, tcp_state).await {
            eprintln!("TCP server error: {}", e);
        }
    });

    let http_state = state.clone();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = run_http_server(&http_addr, http_state).await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    // Periodic incremental snapshot timer (PERS-01, PERS-04, OPS-03).
    // Skip if snapshots are disabled.
    if snapshot_enabled {
        let snap_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await; // First tick completes immediately -- skip it
            loop {
                interval.tick().await;

                // Phase 15: cycle guard — skip if a previous snapshot write is
                // still in progress (from either the timer or a manual trigger).
                if snap_state
                    .snapshot_in_progress
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_err()
                {
                    snap_state.metrics.lock().snapshots_skipped += 1;
                    eprintln!("Snapshot cycle skipped: previous write still in progress");
                    continue;
                }
                // RAII guard clears the flag even on panic.
                struct SnapGuard(SharedState);
                impl Drop for SnapGuard {
                    fn drop(&mut self) {
                        self.0
                            .snapshot_in_progress
                            .store(false, std::sync::atomic::Ordering::Release);
                    }
                }
                let _guard = SnapGuard(snap_state.clone());

                // Decide base vs delta, clone the required state, and advance
                // the cycle counter — using individual locks.
                let prepared: Option<(SnapshotData, u64, bool, PathBuf, u64)> = {
                    let engine = snap_state.engine.read();
                    let store = &snap_state.store;
                    let cycle = *snap_state.snapshot_cycle.lock();
                    let seq = *snap_state.snapshot_seq.lock();
                    let is_full = cycle.is_multiple_of(full_snapshot_interval);
                    let valid_features = engine.valid_features_map();
                    let snap_dir = snap_state
                        .snapshot_path
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("."))
                        .to_path_buf();

                    let last_base_seq_for_delta = *snap_state.last_base_seq.lock();
                    if is_full {
                        // Full base snapshot -- clone everything.
                        let entities = store.clone_for_snapshot_with_gc(&valid_features);
                        let mut pipelines: Vec<SerializablePipeline> = engine
                            .list_streams()
                            .filter_map(|stream| {
                                engine.get_raw_register_json(&stream.name).map(|json| {
                                    SerializablePipeline {
                                        name: stream.name.clone(),
                                        key_field: stream.key_field.clone().unwrap_or_default(),
                                        raw_register_json: serde_json::to_string(json)
                                            .unwrap_or_default(),
                                    }
                                })
                            })
                            .collect();
                        for view in engine.list_views() {
                            if let Some(json) = engine.get_raw_register_json(&view.name) {
                                pipelines.push(SerializablePipeline {
                                    name: view.name.clone(),
                                    key_field: view.key_field.clone(),
                                    raw_register_json: serde_json::to_string(json)
                                        .unwrap_or_default(),
                                });
                            }
                        }
                        let backfill_complete: Vec<(String, String)> = snap_state
                            .backfill_complete
                            .lock()
                            .iter()
                            .cloned()
                            .collect();
                        // Clear tracking
                        store.clear_dirty();
                        let _ = store.take_deleted();

                        let base = BaseSnapshotState {
                            header: SnapshotHeader {
                                snapshot_type: SnapshotType::Base,
                                sequence: seq,
                            },
                            entities,
                            pipelines,
                            backfill_complete,
                        };
                        *snap_state.snapshot_cycle.lock() = cycle + 1;
                        *snap_state.snapshot_seq.lock() = seq + 1;
                        let prev_base = *snap_state.last_base_seq.lock();
                        *snap_state.previous_base_seq.lock() = prev_base;
                        *snap_state.last_base_seq.lock() = seq;
                        Some((SnapshotData::Base(base), seq, true, snap_dir, prev_base))
                    } else {
                        // Delta -- clone only dirty entities.
                        let changed = store.clone_dirty_for_snapshot_with_gc(&valid_features);
                        let deleted = store.take_deleted();
                        store.clear_dirty();

                        if changed.is_empty() && deleted.is_empty() {
                            *snap_state.snapshot_cycle.lock() = cycle + 1;
                            None
                        } else {
                            let delta = DeltaSnapshotState {
                                header: SnapshotHeader {
                                    snapshot_type: SnapshotType::Delta {
                                        base_seq: last_base_seq_for_delta,
                                    },
                                    sequence: seq,
                                },
                                changed_entities: changed,
                                deleted_keys: deleted,
                            };
                            *snap_state.snapshot_cycle.lock() = cycle + 1;
                            *snap_state.snapshot_seq.lock() = seq + 1;
                            Some((SnapshotData::Delta(delta), seq, false, snap_dir, 0))
                        }
                    }
                };

                let (snapshot_data, seq, is_full, snap_dir, prev_base_seq_for_cleanup) =
                    match prepared {
                        Some(p) => p,
                        None => continue, // No changes this cycle
                    };

                // Serialize on blocking thread pool
                let snap_start = std::time::Instant::now();
                let result = tokio::task::spawn_blocking(move || {
                    let (bytes, filename) = match snapshot_data {
                        SnapshotData::Base(base) => {
                            let bytes = save_base_snapshot(&base).map_err(std::io::Error::other)?;
                            let filename = format!("beava.snapshot.base.{:010}", seq);
                            Ok::<(Vec<u8>, String), std::io::Error>((bytes, filename))
                        }
                        SnapshotData::Delta(delta) => {
                            let bytes =
                                save_delta_snapshot(&delta).map_err(std::io::Error::other)?;
                            let filename = format!("beava.snapshot.delta.{:010}", seq);
                            Ok((bytes, filename))
                        }
                    }?;
                    let file_path = snap_dir.join(&filename);
                    let tmp_path = snap_dir.join(format!("{}.tmp", filename));
                    {
                        use std::fs::OpenOptions;
                        use std::io::Write;
                        let mut f = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&tmp_path)?;
                        f.write_all(&bytes)?;
                        f.sync_all()?;
                    }
                    std::fs::rename(&tmp_path, &file_path)?;
                    if let Ok(dir) = std::fs::File::open(&snap_dir) {
                        let _ = dir.sync_all();
                    }
                    if is_full {
                        let cutoff = if prev_base_seq_for_cleanup == 0 {
                            seq
                        } else {
                            prev_base_seq_for_cleanup
                        };
                        cleanup_old_snapshots(&snap_dir, cutoff);
                    }
                    Ok::<usize, std::io::Error>(bytes.len())
                })
                .await;
                match result {
                    Ok(Ok(size)) => {
                        let snap_elapsed = snap_start.elapsed();
                        snap_state.metrics.lock().snapshot_duration_ms =
                            snap_elapsed.as_millis() as u64;
                        eprintln!(
                            "Snapshot saved ({} bytes, {}ms, {})",
                            size,
                            snap_elapsed.as_millis(),
                            if is_full { "base" } else { "delta" },
                        );
                    }
                    Ok(Err(e)) => {
                        eprintln!("Snapshot write failed: {}", e);
                        // Phase 25-02: emit operational signal so the failure
                        // surfaces on /debug/warnings. record() does no disk
                        // I/O, so we cannot recurse on repeat failures.
                        beava::server::signals::emit_snapshot_failure(
                            &snap_state.signals,
                            &format!("{}", e),
                        );
                    }
                    Err(e) => {
                        eprintln!("Snapshot task panicked: {}", e);
                        beava::server::signals::emit_snapshot_failure(
                            &snap_state.signals,
                            &format!("snapshot task panicked: {}", e),
                        );
                    }
                }

                // Phase 25-02: poll the remaining signal sources on each
                // snapshot cycle. These emitters are idempotent (dedupe by
                // stable id) so firing every 30s is free.
                poll_signal_sources(&snap_state);
            }
        });
    } // if snapshot_enabled

    // Periodic eviction timer (PERS-05)
    let evict_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await; // Skip first immediate tick
        loop {
            interval.tick().await;
            let now = std::time::SystemTime::now();
            let engine = evict_state.engine.read();
            let evicted = evict_expired_keys(&evict_state.store, &engine, now, ttl_multiplier);
            // Phase 25-02: evict expired Table rows (per-Table TTL) and record
            // each eviction in the EvictionTracker so eviction→reinit signals
            // surface on /metrics and /debug/config-recommendations.
            let table_evicted = beava::state::eviction::evict_expired_table_rows(
                &evict_state.store,
                &engine,
                &evict_state.eviction_tracker,
                now,
            );
            // Rotate per-Table bloom generations so the 7d rolling window
            // actually rolls.
            evict_state.eviction_tracker.rotate_generation(now);
            if evicted > 0 || table_evicted > 0 {
                eprintln!(
                    "Evicted {} expired stream entries, {} expired Table rows",
                    evicted, table_evicted
                );
            }
        }
    });

    // Periodic event log fsync timer (ELOG-04: 1-second interval, Redis everysec pattern)
    // Skip if event log is disabled.
    if event_log_enabled {
        let fsync_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // Skip first immediate tick
            loop {
                interval.tick().await;
                // Phase 43 T2: measure fsync_all wall time so operators can
                // alert on stalled durability via beava_fsync_stall_seconds_total.
                let fsync_start = std::time::Instant::now();
                let result = match &fsync_state.event_log {
                    Some(log) => log.fsync_all(),
                    None => Ok(()),
                };
                let elapsed_nanos = fsync_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                fsync_state
                    .fsync_stall_nanos_total
                    .fetch_add(elapsed_nanos, std::sync::atomic::Ordering::Relaxed);
                fsync_state
                    .fsync_calls_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                fsync_state
                    .fsync_last_nanos
                    .store(elapsed_nanos, std::sync::atomic::Ordering::Relaxed);
                if let Err(e) = result {
                    eprintln!("Event log fsync failed: {}", e);
                }
            }
        });
    } // if event_log_enabled

    // Periodic event log compaction timer (ELOG-05: 60-second interval)
    // Skip if event log is disabled.
    if event_log_enabled {
        let compact_state = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // Skip first immediate tick
            loop {
                interval.tick().await;
                let now = SystemTime::now();
                // Get list of streams to compact
                let streams_to_compact: Vec<String> = match &compact_state.event_log {
                    Some(log) => log.registered_streams(),
                    None => vec![],
                };
                // Compact each stream (interior mutability inside `log`).
                for stream_name in &streams_to_compact {
                    if let Some(ref log) = compact_state.event_log {
                        match log.compact_stream(stream_name, now) {
                            Ok(removed) if removed > 0 => {
                                // Phase 25-02: bump per-stream compaction counter.
                                let mut m = compact_state.metrics.lock();
                                *m.history_compacted_total
                                    .entry(stream_name.clone())
                                    .or_insert(0) += 1;
                                drop(m);
                                eprintln!(
                                    "Compacted {}: removed {} expired entries",
                                    stream_name, removed
                                );
                            }
                            Err(e) => {
                                eprintln!("Compaction failed for {}: {}", stream_name, e);
                            }
                            _ => {}
                        }
                    }
                    // Yield between streams for cooperative scheduling
                    tokio::task::yield_now().await;
                }
            }
        });
    } // if event_log_enabled

    // Log ephemeral mode if both persistence mechanisms are disabled
    if !snapshot_enabled && !event_log_enabled {
        eprintln!("Running in ephemeral mode (no persistence)");
    }

    // Phase 25-02: startup advisory log. If we loaded a snapshot that
    // carries eviction/reinit history, recommendations may fire immediately
    // at boot. Emit one terse line per knob (or a single summary line if
    // there are more than 3) so operators see the signal without grepping
    // Prometheus.
    {
        let engine = state.engine.read();
        let recs =
            beava::engine::recommend::recommend_config(&engine, &state.eviction_tracker);
        drop(engine);
        if !recs.is_empty() {
            if recs.len() > 3 {
                eprintln!(
                    "advisory: {} config recommendations available; run \
                     'beava suggest-config' or query /debug/config-recommendations",
                    recs.len()
                );
            } else {
                for r in &recs {
                    eprintln!(
                        "advisory: {} '{}' → '{}' ({})",
                        r.knob, r.current, r.suggested, r.reason
                    );
                }
            }
        }
    }

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}

// ================ Phase 9: Incremental Snapshot Helpers ================

/// Remove snapshot files whose sequence is strictly less than the current
/// base's sequence.
fn cleanup_old_snapshots(dir: &Path, current_base_seq: u64) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let seq_opt = name_str
            .strip_prefix("beava.snapshot.base.")
            .or_else(|| name_str.strip_prefix("beava.snapshot.delta."));
        if let Some(seq_str) = seq_opt {
            if let Ok(seq) = seq_str.parse::<u64>() {
                if seq < current_base_seq {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

/// Scan the snapshot directory and load the latest base + subsequent deltas.
pub(crate) fn load_incremental_snapshots(
    snap_dir: &Path,
    legacy_path: &Path,
) -> Option<(SnapshotState, u64, u64)> {
    let mut bases: Vec<(u64, PathBuf)> = Vec::new();
    let mut deltas: Vec<(u64, PathBuf)> = Vec::new();

    if let Ok(entries) = std::fs::read_dir(snap_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy().into_owned();
            if let Some(seq_str) = name_str.strip_prefix("beava.snapshot.base.") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    bases.push((seq, entry.path()));
                }
            } else if let Some(seq_str) = name_str.strip_prefix("beava.snapshot.delta.") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    deltas.push((seq, entry.path()));
                }
            }
        }
    }

    bases.sort_by_key(|(seq, _)| *seq);

    let loaded = bases.iter().rev().find_map(|(seq, path)| {
        let bytes = std::fs::read(path).ok()?;
        match load_snapshot_file(&bytes)? {
            SnapshotFile::Base(b) => Some((*seq, b)),
            _ => None,
        }
    });

    if let Some((base_seq, base)) = loaded {
        let store = StateStore::new();
        store.restore_from_snapshot(base.entities.clone());

        let mut applicable: Vec<(u64, PathBuf)> = deltas
            .into_iter()
            .filter(|(seq, _)| *seq > base_seq)
            .collect();
        applicable.sort_by_key(|(seq, _)| *seq);

        let mut max_seq = base_seq;
        for (seq, delta_path) in &applicable {
            let bytes = match std::fs::read(delta_path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            match load_snapshot_file(&bytes) {
                Some(SnapshotFile::Delta(delta)) => {
                    store.apply_delta(delta.changed_entities, delta.deleted_keys);
                    if *seq > max_seq {
                        max_seq = *seq;
                    }
                }
                _ => continue,
            }
        }

        let state = SnapshotState {
            entities: store.clone_for_snapshot(),
            pipelines: base.pipelines,
            backfill_complete: base.backfill_complete,
        };
        return Some((state, max_seq + 1, base_seq));
    }

    if legacy_path.exists() {
        let bytes = std::fs::read(legacy_path).ok()?;
        let legacy = load_legacy_v5(&bytes)?;
        eprintln!("Loaded legacy v5 snapshot from {}", legacy_path.display());
        return Some((legacy, 1, 0));
    }

    None
}

// ================ Phase 36-01: replica-mode CLI parser tests ===============

#[cfg(test)]
mod replica_cli_tests {
    use super::parse_replica_since;

    #[test]
    fn parses_raw_u64_millis() {
        assert_eq!(parse_replica_since("0").unwrap(), 0);
        assert_eq!(parse_replica_since("1712345678000").unwrap(), 1712345678000);
    }

    #[test]
    fn parses_iso_8601_utc_whole_seconds() {
        // 1970-01-01T00:00:00Z = 0ms.
        assert_eq!(parse_replica_since("1970-01-01T00:00:00Z").unwrap(), 0);
        // 2021-01-01T00:00:00Z = 1609459200000ms.
        let ms = parse_replica_since("2021-01-01T00:00:00Z").unwrap();
        assert_eq!(ms, 1_609_459_200_000);
    }

    #[test]
    fn parses_iso_8601_with_millis() {
        let ms = parse_replica_since("2021-01-01T00:00:00.123Z").unwrap();
        assert_eq!(ms, 1_609_459_200_123);
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_replica_since("").is_err());
        assert!(parse_replica_since("   ").is_err());
    }

    #[test]
    fn rejects_missing_z_suffix() {
        assert!(parse_replica_since("2021-01-01T00:00:00").is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_replica_since("not-a-timestamp").is_err());
        assert!(parse_replica_since("2021/01/01").is_err());
    }
}

// ================ Phase 37-01: `beava fork` flag parser tests =============

#[cfg(test)]
mod fork_cli_tests {
    use super::parse_fork_args_from;

    fn argv(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_minimal_happy_path() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "localhost:6400",
            "--streams",
            "Transactions",
            "--token",
            "T",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert_eq!(cfg.remote, "localhost:6400");
        assert_eq!(cfg.streams, vec!["Transactions".to_string()]);
        assert_eq!(cfg.token, "T");
        // Default since = 1970 epoch.
        assert_eq!(cfg.since_millis, 0);
        // Default block_until_catchup = true.
        assert!(cfg.block_until_catchup);
        assert!(cfg.keys.is_none());
        assert!(cfg.key_prefix.is_none());
        assert!(cfg.pipeline_file.is_none());
    }

    #[test]
    fn parses_all_flags() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "10.0.0.1:7000",
            "--streams",
            "Transactions,Clicks",
            "--since",
            "2021-01-01T00:00:00Z",
            "--keys",
            "u1,u2",
            "--token",
            "tok",
            "--local-port",
            "8080",
            "--pipeline-file",
            "/tmp/p.json",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert_eq!(cfg.streams.len(), 2);
        assert_eq!(cfg.since_millis, 1_609_459_200_000);
        assert_eq!(cfg.keys.as_ref().unwrap().len(), 2);
        assert_eq!(
            cfg.pipeline_file.as_ref().unwrap().to_str().unwrap(),
            "/tmp/p.json"
        );
    }

    #[test]
    fn accepts_eq_form() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote=h:1",
            "--streams=s",
            "--token=t",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert_eq!(cfg.remote, "h:1");
    }

    #[test]
    fn rejects_missing_remote() {
        let args = argv(&["beava", "fork", "--streams", "s", "--token", "t"]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert!(err.contains("--remote"), "{}", err);
    }

    #[test]
    fn rejects_missing_streams() {
        let args = argv(&["beava", "fork", "--remote", "h:1", "--token", "t"]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert!(err.contains("--streams"), "{}", err);
    }

    #[test]
    fn rejects_missing_token_without_env() {
        // Ensure env is not set for this test.
        let saved = std::env::var("BEAVA_REPLICA_TOKEN").ok();
        std::env::remove_var("BEAVA_REPLICA_TOKEN");
        let args = argv(&["beava", "fork", "--remote", "h:1", "--streams", "s"]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert!(err.contains("--token"), "{}", err);
        if let Some(v) = saved {
            std::env::set_var("BEAVA_REPLICA_TOKEN", v);
        }
    }

    #[test]
    fn rejects_ambiguous_since() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--since",
            "not-a-timestamp",
        ]);
        assert!(parse_fork_args_from(&args).is_err());
    }

    #[test]
    fn rejects_keys_and_key_prefix_mutex() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--keys",
            "a",
            "--key-prefix",
            "p",
        ]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert!(err.contains("mutually exclusive"), "{}", err);
    }

    #[test]
    fn rejects_invalid_local_port() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--local-port",
            "not-a-number",
        ]);
        assert!(parse_fork_args_from(&args).is_err());
    }

    #[test]
    fn rejects_zero_local_port() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--local-port",
            "0",
        ]);
        assert!(parse_fork_args_from(&args).is_err());
    }

    #[test]
    fn help_flag_returns_help_sentinel() {
        let args = argv(&["beava", "fork", "--help"]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert_eq!(err, "__HELP__");
    }

    // Phase 44-01: --extract-at parsing.
    #[test]
    fn parses_extract_at_iso8601_list_and_sorts() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--extract-at",
            "2026-04-01T10:00:00Z,2026-03-01T10:00:00Z,2026-03-15T10:00:00Z",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert_eq!(cfg.extract_at_millis.len(), 3);
        // Must be sorted ascending regardless of input order.
        assert!(cfg.extract_at_millis[0] < cfg.extract_at_millis[1]);
        assert!(cfg.extract_at_millis[1] < cfg.extract_at_millis[2]);
        // March 1 2026 < March 15 2026 < April 1 2026 (exact ms values
        // computed by the shared parse_replica_since helper; here we just
        // assert the parsed values round-trip in sorted order).
        assert_eq!(
            cfg.extract_at_millis[0],
            super::parse_replica_since("2026-03-01T10:00:00Z").unwrap()
        );
        assert_eq!(
            cfg.extract_at_millis[2],
            super::parse_replica_since("2026-04-01T10:00:00Z").unwrap()
        );
    }

    #[test]
    fn parses_extract_at_u64_millis_mix() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--extract-at",
            "5000,1000,3000",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert_eq!(cfg.extract_at_millis, vec![1000, 3000, 5000]);
    }

    #[test]
    fn extract_at_default_empty_when_absent() {
        let args = argv(&[
            "beava", "fork", "--remote", "h:1", "--streams", "s", "--token", "t",
        ]);
        let cfg = parse_fork_args_from(&args).unwrap();
        assert!(cfg.extract_at_millis.is_empty());
    }

    #[test]
    fn rejects_bad_extract_at_entry() {
        let args = argv(&[
            "beava",
            "fork",
            "--remote",
            "h:1",
            "--streams",
            "s",
            "--token",
            "t",
            "--extract-at",
            "1000,not-a-ts,3000",
        ]);
        let err = parse_fork_args_from(&args).unwrap_err();
        assert!(err.contains("--extract-at"), "{}", err);
    }
}

// ============ Plan 39-01: seed_pipelines_from_file dispatch tests ==========

#[cfg(test)]
mod seed_pipelines_tests {
    use super::seed_pipelines_from_file;
    use std::io::Write;
    use std::sync::Arc;
    use beava::engine::pipeline::PipelineEngine;
    use beava::server::tcp::{make_concurrent_state, BackfillTracker};
    use beava::state::store::StateStore;

    fn fresh_state() -> beava::server::tcp::SharedState {
        let tmp = tempfile::tempdir().unwrap();
        let snapshot_path = tmp.path().join("snap");
        // Leak tempdir so the path is valid for the life of the test; tests are
        // short-lived and tempdir cleanup is best-effort.
        std::mem::forget(tmp);
        make_concurrent_state(
            PipelineEngine::new(),
            StateStore::new(),
            None,
            snapshot_path,
            Arc::new(BackfillTracker::default()),
            false,
            false,
        )
    }

    fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn loads_v0_stream_shape() {
        // v0 SDK shape: carries `kind: "stream"`.
        let doc = serde_json::json!({
            "kind": "stream",
            "name": "Transactions",
            "fields": {
                "user_id": {"type": "string"},
                "amount": {"type": "f64"}
            },
            "key_field": "user_id"
        });
        let file = write_tmp(&doc.to_string());
        let state = fresh_state();
        let n = seed_pipelines_from_file(&state, file.path()).expect("v0 stream should load");
        assert_eq!(n, 1);
        let engine = state.engine.read();
        assert!(engine.get_stream("Transactions").is_some());
    }

    #[test]
    fn loads_legacy_features_shape() {
        // Legacy v2.0 shape — features array, no `kind` field.
        let doc = serde_json::json!({
            "name": "LegacyStream",
            "event_schema": {
                "user_id": "string",
                "amount": "f64"
            },
            "key_field": "user_id",
            "features": []
        });
        let file = write_tmp(&doc.to_string());
        let state = fresh_state();
        let n = seed_pipelines_from_file(&state, file.path())
            .expect("legacy features shape should load");
        assert_eq!(n, 1);
        let engine = state.engine.read();
        assert!(engine.get_stream("LegacyStream").is_some());
    }

    #[test]
    fn loads_mixed_array_both_shapes() {
        let doc = serde_json::json!([
            {
                "kind": "stream",
                "name": "S1",
                "fields": {"k": {"type": "string"}},
                "key_field": "k"
            },
            {
                "name": "S2",
                "event_schema": {"k": "string"},
                "key_field": "k",
                "features": []
            }
        ]);
        let file = write_tmp(&doc.to_string());
        let state = fresh_state();
        let n = seed_pipelines_from_file(&state, file.path())
            .expect("mixed array should load both");
        assert_eq!(n, 2);
        let engine = state.engine.read();
        assert!(engine.get_stream("S1").is_some());
        assert!(engine.get_stream("S2").is_some());
    }
}
