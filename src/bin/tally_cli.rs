//! tally_cli — client CLI for scoped local replicas. Subcommands: clone, sync.
//!
//! Phase 28-02: stubs only. Argument parsing is complete for the full Phase 29
//! flag surface (`--remote`, `--streams`, `--keys | --key-prefix`, `--mode`,
//! `--token`). Both subcommand handlers print a "not implemented yet" message
//! and exit 0 (success-for-a-stub). `--mode streaming` is the one rejection
//! path — it exits 2 and points at Phase 31. No network code.
//!
//! Hand-rolled arg parsing (no `clap` dependency). Style matches
//! `src/bin/tally_suggest_config.rs`.

use std::env;
use std::process;

use tally::client::clone::{run_clone, CloneArgs, CloneError};
use tally::client::wire::Scope;
use tally::client::{FrozenClient, SessionMode};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Subcommand {
    Clone,
    Sync,
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
struct ParsedArgs {
    remote: Option<String>,
    streams: Vec<String>,
    keys: Option<Vec<String>>,
    key_prefix: Option<String>,
    mode: String,
    token: Option<String>,
    dump_json: bool,
}

fn print_usage() {
    eprintln!(
        "usage: tally_cli <SUBCOMMAND> [FLAGS]\n\
         \n\
         subcommands:\n\
           clone    Clone a scoped local replica from a tally server.\n\
           sync     Resume / keep a local replica in sync with the server.\n\
         \n\
         flags:\n\
           --remote <host:port>        Server address (required).\n\
           --streams <name>[,name...]  Streams to clone (required for clone).\n\
           --keys <key>[,key...]       Key allow-list (mutually exclusive with --key-prefix).\n\
           --key-prefix <prefix>       Key prefix scope (mutually exclusive with --keys).\n\
           --mode historical|streaming Default: historical. streaming is Phase 31.\n\
           --token <token>             Admin token (overrides TALLY_TOKEN env var).\n\
           -h, --help                  Show this message.\n\
         \n\
         environment:\n\
           TALLY_TOKEN                 Admin token, used if --token not passed.\n\
         \n\
         Phase 28 status: clone/sync are stubs; Phase 29 wires the real session."
    );
}

/// Pure, unit-testable argv parser. `argv` must NOT include the binary name
/// (caller should pass `env::args().skip(1).collect()`).
fn parse_args(argv: &[String]) -> Result<(Subcommand, ParsedArgs), String> {
    if argv.is_empty() {
        return Err("missing subcommand (expected `clone` or `sync`)".to_string());
    }
    let sub = match argv[0].as_str() {
        "clone" => Subcommand::Clone,
        "sync" => Subcommand::Sync,
        "-h" | "--help" => return Err("__help__".to_string()),
        other => return Err(format!("unknown subcommand: `{}` (expected `clone` or `sync`)", other)),
    };

    let mut parsed = ParsedArgs {
        mode: "historical".to_string(),
        ..Default::default()
    };

    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--remote" => {
                if i + 1 >= argv.len() {
                    return Err("--remote requires a value (host:port)".to_string());
                }
                parsed.remote = Some(argv[i + 1].clone());
                i += 2;
            }
            "--streams" => {
                if i + 1 >= argv.len() {
                    return Err("--streams requires a value".to_string());
                }
                parsed.streams = argv[i + 1]
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                i += 2;
            }
            "--keys" => {
                if i + 1 >= argv.len() {
                    return Err("--keys requires a value".to_string());
                }
                let vs: Vec<String> = argv[i + 1]
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                parsed.keys = Some(vs);
                i += 2;
            }
            "--key-prefix" => {
                if i + 1 >= argv.len() {
                    return Err("--key-prefix requires a value".to_string());
                }
                parsed.key_prefix = Some(argv[i + 1].clone());
                i += 2;
            }
            "--mode" => {
                if i + 1 >= argv.len() {
                    return Err("--mode requires a value (historical|streaming)".to_string());
                }
                let m = argv[i + 1].clone();
                if m != "historical" && m != "streaming" {
                    return Err(format!(
                        "--mode must be `historical` or `streaming`, got `{}`",
                        m
                    ));
                }
                parsed.mode = m;
                i += 2;
            }
            "--token" => {
                if i + 1 >= argv.len() {
                    return Err("--token requires a value".to_string());
                }
                parsed.token = Some(argv[i + 1].clone());
                i += 2;
            }
            "--dump-json" => {
                parsed.dump_json = true;
                i += 1;
            }
            "-h" | "--help" => {
                return Err("__help__".to_string());
            }
            other => {
                return Err(format!("unknown flag: `{}`", other));
            }
        }
    }

    // Semantic validation — shared across subcommands.
    if parsed.keys.is_some() && parsed.key_prefix.is_some() {
        return Err("--keys and --key-prefix are mutually exclusive".to_string());
    }
    if parsed.remote.is_none() {
        return Err("--remote <host:port> is required".to_string());
    }
    if matches!(sub, Subcommand::Clone) && parsed.streams.is_empty() {
        return Err("--streams is required for `clone`".to_string());
    }

    Ok((sub, parsed))
}

/// Resolve the admin token: flag wins; else env lookup; else None.
/// `env_lookup` is injected for testability.
fn resolve_token(
    flag: Option<String>,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if let Some(f) = flag {
        return Some(f);
    }
    env_lookup("TALLY_TOKEN")
}

fn format_scope(args: &ParsedArgs) -> String {
    let keys_s = match &args.keys {
        Some(v) => format!("[{}]", v.join(",")),
        None => "<none>".to_string(),
    };
    let kp_s = args.key_prefix.as_deref().unwrap_or("<none>");
    format!(
        "remote={} streams=[{}] keys={} key_prefix={} mode={} token_set={}",
        args.remote.as_deref().unwrap_or("<none>"),
        args.streams.join(","),
        keys_s,
        kp_s,
        args.mode,
        args.token.is_some(),
    )
}

fn handle_clone(args: ParsedArgs) -> i32 {
    if args.mode == "streaming" {
        eprintln!(
            "error: --mode streaming is not supported yet (Phase 31 will enable streaming mode)"
        );
        return 2;
    }
    let remote = match args.remote.clone() {
        Some(r) => r,
        None => {
            eprintln!("error: --remote is required");
            return 2;
        }
    };
    let scope = Scope {
        streams: args.streams.clone(),
        keys: args.keys.clone(),
        key_prefix: args.key_prefix.clone(),
        pull: "all".to_string(),
    };
    let clone_args = CloneArgs {
        remote,
        scope,
        token: args.token.clone(),
        mode: SessionMode::Historical,
        max_attempts: 5,
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: failed to build tokio runtime: {}", e);
            return 1;
        }
    };
    let result = rt.block_on(run_clone(&clone_args));
    match result {
        Ok(frozen) => {
            if args.dump_json {
                emit_json_dump(&frozen);
            } else {
                let rfc = rfc3339(frozen.snapshot_taken_at);
                let n = frozen.iter_entities().len();
                println!("tally clone: ok — snapshot_taken_at={} entities={}", rfc, n);
            }
            0
        }
        Err(CloneError::StreamingNotSupported) => {
            eprintln!("error: streaming mode not supported (Phase 31 will enable it)");
            2
        }
        Err(e) => {
            eprintln!("tally clone failed: {}", e);
            1
        }
    }
}

/// Serialize `FrozenClient` state to stdout as JSON. Schema:
/// ```json
/// {
///   "snapshot_taken_at": "2026-04-15T10:23:45.123Z",
///   "scope": { "streams": [...], "keys": [...]|null, "key_prefix": null, "pull": "all" },
///   "entities": [ {"stream": "...", "key": "...", "last_event_at_epoch_ms": 12345|null}, ... ]
/// }
/// ```
/// The `value` field is intentionally minimal — we emit per-(stream, key) rows
/// keyed by `last_event_at` rather than a full operator dump, because
/// `SerializableEntityState` contains non-JSON-friendly postcard-serializable
/// variants. Sufficient for Phase 28-04's in-scope/out-of-scope filter check;
/// Phase 30's Python binding will evolve a richer dump.
fn emit_json_dump(fc: &FrozenClient) {
    use serde_json::json;
    let scope = fc.scope();
    let entities: Vec<serde_json::Value> = fc
        .iter_entities()
        .into_iter()
        .map(|(stream, key, state)| {
            // Find the matching per-stream state for last_event_at.
            let last_ms: Option<u128> = state
                .streams
                .iter()
                .find(|(sn, _)| sn == &stream)
                .and_then(|(_, s)| s.last_event_at)
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis());
            json!({
                "stream": stream,
                "key": key,
                "last_event_at_epoch_ms": last_ms,
            })
        })
        .collect();
    let out = json!({
        "snapshot_taken_at": rfc3339(fc.snapshot_taken_at),
        "scope": {
            "streams": scope.streams,
            "keys": scope.keys,
            "key_prefix": scope.key_prefix,
            "pull": scope.pull,
        },
        "entities": entities,
    });
    println!("{}", serde_json::to_string(&out).unwrap_or_else(|_| "{}".into()));
}

/// Minimal RFC3339-ish formatting of a `SystemTime` without pulling in a new
/// time crate. Output shape: `"<secs>.<nanos>s since unix epoch"` — good
/// enough for the dev/test `--dump-json` hook; Phase 30 can upgrade.
fn rfc3339(t: std::time::SystemTime) -> String {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => format!("{}.{:09}s", d.as_secs(), d.subsec_nanos()),
        Err(_) => "pre-epoch".to_string(),
    }
}

fn handle_sync(args: ParsedArgs) -> i32 {
    if args.mode == "streaming" {
        eprintln!(
            "error: --mode streaming is not supported yet (Phase 31 will enable streaming mode)"
        );
        return 2;
    }
    println!("tally sync: not implemented yet — Phase 29 will wire the historical catch-up loop.");
    println!("(parsed) {}", format_scope(&args));
    0
}

fn main() {
    let argv: Vec<String> = env::args().skip(1).collect();
    match parse_args(&argv) {
        Ok((sub, mut parsed)) => {
            // Fill token from env if not passed via flag.
            parsed.token = resolve_token(parsed.token, |k| env::var(k).ok());
            let code = match sub {
                Subcommand::Clone => handle_clone(parsed),
                Subcommand::Sync => handle_sync(parsed),
            };
            process::exit(code);
        }
        Err(e) if e == "__help__" => {
            print_usage();
            process::exit(0);
        }
        Err(e) => {
            eprintln!("error: {}", e);
            print_usage();
            process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(x: &str) -> String {
        x.to_string()
    }

    // Test 1: clone happy path (parser only — handle_clone now makes a real
    // network call, so its exit behavior is covered by the integration test
    // `tests/integration/test_tally_clone.py`).
    #[test]
    fn clone_happy_path_parses() {
        let argv = vec![s("clone"), s("--remote"), s("foo:6400"), s("--streams"), s("Transactions,Logins")];
        let (sub, parsed) = parse_args(&argv).expect("should parse");
        assert_eq!(sub, Subcommand::Clone);
        assert_eq!(parsed.remote.as_deref(), Some("foo:6400"));
        assert_eq!(parsed.streams, vec![s("Transactions"), s("Logins")]);
        assert_eq!(parsed.mode, "historical");
        assert!(!parsed.dump_json);
    }

    // Test 2: sync happy path.
    #[test]
    fn sync_happy_path_parses() {
        let argv = vec![s("sync"), s("--remote"), s("foo:6400")];
        let (sub, parsed) = parse_args(&argv).expect("should parse");
        assert_eq!(sub, Subcommand::Sync);
        assert_eq!(parsed.remote.as_deref(), Some("foo:6400"));
        assert!(parsed.streams.is_empty()); // optional for sync
        assert_eq!(handle_sync(parsed), 0);
    }

    // Test 3: --mode streaming is parser-accepted but handler-rejected with Phase 31 message.
    #[test]
    fn mode_streaming_rejected_by_handler() {
        let argv = vec![
            s("clone"), s("--remote"), s("foo:6400"),
            s("--streams"), s("Txn"),
            s("--mode"), s("streaming"),
        ];
        let (_sub, parsed) = parse_args(&argv).expect("parser accepts streaming");
        assert_eq!(parsed.mode, "streaming");
        assert_eq!(handle_clone(parsed.clone()), 2);
        assert_eq!(handle_sync(parsed), 2);
    }

    // Test 4: missing --remote is a parse error.
    #[test]
    fn missing_remote_errors() {
        let argv = vec![s("clone"), s("--streams"), s("Txn")];
        let err = parse_args(&argv).unwrap_err();
        assert!(err.contains("--remote"), "got: {}", err);
    }

    // Test 5: --keys + --key-prefix mutual exclusion.
    #[test]
    fn keys_and_key_prefix_mutually_exclusive() {
        let argv = vec![
            s("clone"), s("--remote"), s("foo:6400"),
            s("--streams"), s("A"),
            s("--keys"), s("k1"),
            s("--key-prefix"), s("pre"),
        ];
        let err = parse_args(&argv).unwrap_err();
        assert!(err.contains("mutually exclusive"), "got: {}", err);
    }

    // Test 6: token precedence — flag wins over env, env used when flag missing, None otherwise.
    #[test]
    fn token_precedence_injected_env() {
        // Flag wins.
        let got = resolve_token(Some(s("cli")), |_| Some(s("envtoken")));
        assert_eq!(got.as_deref(), Some("cli"));
        // Env only.
        let got = resolve_token(None, |k| {
            if k == "TALLY_TOKEN" { Some(s("envtoken")) } else { None }
        });
        assert_eq!(got.as_deref(), Some("envtoken"));
        // Neither.
        let got = resolve_token(None, |_| None);
        assert!(got.is_none());
    }

    // Test 7: --help returns the __help__ sentinel.
    #[test]
    fn help_flag_returns_help_sentinel() {
        let argv = vec![s("--help")];
        let err = parse_args(&argv).unwrap_err();
        assert_eq!(err, "__help__");
        let argv = vec![s("-h")];
        let err = parse_args(&argv).unwrap_err();
        assert_eq!(err, "__help__");
        // help after a valid subcommand also works.
        let argv = vec![s("clone"), s("--help")];
        let err = parse_args(&argv).unwrap_err();
        assert_eq!(err, "__help__");
    }

    // Test 8 (new for 28-04): --dump-json parses to args.dump_json = true and
    // is rejected when passed an extra value (it's a boolean flag).
    #[test]
    fn dump_json_flag_parses() {
        let argv = vec![
            s("clone"), s("--remote"), s("foo:6400"),
            s("--streams"), s("Txn"),
            s("--dump-json"),
        ];
        let (sub, parsed) = parse_args(&argv).expect("should parse");
        assert_eq!(sub, Subcommand::Clone);
        assert!(parsed.dump_json);
        // And default is false when not passed.
        let argv2 = vec![s("clone"), s("--remote"), s("foo:6400"), s("--streams"), s("Txn")];
        let (_sub, parsed2) = parse_args(&argv2).unwrap();
        assert!(!parsed2.dump_json);
    }

    // Test 9: unknown subcommand errors.
    #[test]
    fn unknown_subcommand_errors() {
        let argv = vec![s("foo")];
        let err = parse_args(&argv).unwrap_err();
        assert!(err.contains("unknown subcommand"), "got: {}", err);
    }
}
