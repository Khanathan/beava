//! Integration tests for the Phase 10 Debug UI (DBUI-01 through DBUI-05).
//!
//! These tests start a real Beava HTTP server on a random localhost port
//! and exercise every endpoint the embedded debug UI talks to, plus the
//! static asset serving for the vendored JS. Pattern matches
//! `tests/test_server.rs` -- raw TCP, no reqwest dependency.
//!
//! Test case names MUST match `.planning/phases/10-debug-ui/10-VALIDATION.md`
//! exactly; the Phase 10 verifier greps the file for these names.

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{
    FeatureDef, PipelineEngine, StreamDefinition, ViewDefinition, ViewFeatureDef,
};
use beava::server::tcp::{make_concurrent_state_default, BackfillTracker, SharedState};

// ---------------------------------------------------------------------------
// Helper A: start a Beava HTTP server on a random localhost port.
// ---------------------------------------------------------------------------

/// Build a fresh `SharedState` with an empty engine/store.
fn make_test_state() -> SharedState {
    make_concurrent_state_default(
        PipelineEngine::new(),
        None,
        std::path::PathBuf::from("test-debug-ui.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        true,
    )
}

/// Start a Beava HTTP server on a random loopback port. Returns
/// `(http_port, state)` so the caller can mutate the state directly (register
/// pipelines, push events) and also send real HTTP requests through the
/// running axum router.
async fn start_debug_ui_server() -> (u16, SharedState) {
    let state = make_test_state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_state = state.clone();
    tokio::spawn(async move {
        beava::server::http::run_http_server_with_listener(listener, server_state)
            .await
            .unwrap();
    });
    // Small delay for the accept loop to come online.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (port, state)
}

// ---------------------------------------------------------------------------
// Helper B: raw HTTP/1.1 GET over tokio::net::TcpStream (no reqwest).
// ---------------------------------------------------------------------------

/// Parsed HTTP/1.1 response: (status code, header map, raw body bytes).
/// Header names are lowercased for case-insensitive lookup.
type HttpResponse = (u16, std::collections::HashMap<String, String>, Vec<u8>);

/// Send a raw `GET {path} HTTP/1.1` request to 127.0.0.1:{port} with
/// `Connection: close` so the server closes the socket and we can read to
/// EOF. This mirrors the raw-TCP pattern already used in `tests/test_server.rs`
/// and deliberately avoids depending on reqwest or hyper.
async fn http_get(port: u16, path: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        path
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    // Read until EOF (server closes after response per Connection: close).
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();

    // Split headers from body at the first \r\n\r\n.
    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("missing HTTP header/body separator");
    let head = std::str::from_utf8(&buf[..sep])
        .expect("HTTP headers must be ASCII")
        .to_string();
    let body = buf[sep + 4..].to_vec();

    // Status line: "HTTP/1.1 200 OK"
    let mut lines = head.lines();
    let status_line = lines.next().expect("empty HTTP response");
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("malformed status line")
        .parse()
        .expect("non-numeric status code");

    // Header map, lowercased for case-insensitive lookup.
    let mut headers = std::collections::HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    (status_code, headers, body)
}

fn body_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

fn body_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("valid JSON body")
}

// ---------------------------------------------------------------------------
// Helper C: register a minimal four-node pipeline so topology/throughput/
// memory tests have something to walk.
//
// Shape:
//   - Transactions (stream, key=user_id, count(tx_count_1h, 1h))
//   - Logins       (stream, key=user_id, count(login_count_1h, 1h))
//   - Aggregates   (stream, key=user_id, sum(daily_sum/amount, 24h),
//                    depends_on=["Transactions"])  <-- cascade edge
//   - UserRisk     (view,   key=user_id, lookup(Transactions.tx_count_1h
//                    on merchant_id))              <-- lookup edge
// ---------------------------------------------------------------------------

fn register_test_pipeline(state: &SharedState) {
    let mut engine = state.engine.write();

    let transactions = StreamDefinition {
        name: "Transactions".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "tx_count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    engine
        .register(transactions)
        .expect("register Transactions");

    let logins = StreamDefinition {
        name: "Logins".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "login_count_1h".into(),
            FeatureDef::Count {
                window: Duration::from_secs(3600),
                bucket: Duration::from_secs(60),
                where_expr: None,
                backfill: false,
            },
        )],
        depends_on: None,
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    engine.register(logins).expect("register Logins");

    let aggregates = StreamDefinition {
        name: "Aggregates".into(),
        key_field: Some("user_id".into()),
        group_by_keys: None,
        features: vec![(
            "daily_sum".into(),
            FeatureDef::Sum {
                field: "amount".into(),
                window: Duration::from_secs(86_400),
                bucket: Duration::from_secs(3600),
                optional: false,
                where_expr: None,
                backfill: false,
            },
        )],
        // Cascade edge: Transactions -> Aggregates
        depends_on: Some(vec!["Transactions".into()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
        watermark_lateness: None,
        shard_key: None,
    };
    engine.register(aggregates).expect("register Aggregates");

    // View with a lookup feature pointing back at Transactions.
    // Emits a lookup edge in /debug/topology with kind="lookup".
    let user_risk = ViewDefinition {
        name: "UserRisk".into(),
        key_field: "user_id".into(),
        features: vec![(
            "merchant_tx_count".into(),
            ViewFeatureDef::Lookup {
                target_stream: "Transactions".into(),
                target_feature: "tx_count_1h".into(),
                on_field: "merchant_id".into(),
            },
        )],
    };
    engine.register_view(user_risk).expect("register UserRisk");
}

// ---------------------------------------------------------------------------
// Helper D: push a single event to a stream via the engine and bump the
// throughput tracker the same way handle_sync_command's Push arm does.
// ---------------------------------------------------------------------------

fn push_event(state: &SharedState, stream: &str, event: &serde_json::Value) {
    use std::time::Instant;
    let now_ts = SystemTime::now();
    let now_inst = Instant::now();
    {
        let engine = state.engine.read();
        let _store = &state.store;
        let _ = engine.push(stream, event, &state.store, now_ts);
    }
    // Bump the throughput tracker so the /debug/throughput endpoint observes
    // the push. We pass a single-element slice of the stream name.
    let name = stream.to_string();
    state
        .throughput
        .lock()
        .bump_unique([name.as_str()], now_inst);
}

// ---------------------------------------------------------------------------
// Helper E: SHA256 + VENDOR.md parser for the drift tests.
// ---------------------------------------------------------------------------

/// Parse `src/server/ui/vendor/VENDOR.md` and return the 64-char hex SHA256
/// associated with `filename`. Tolerates extra columns by scanning each pipe
/// cell in reverse until we hit one that looks like a 64-hex-char string.
fn expected_hash_for(filename: &str) -> String {
    let vendor_md = std::fs::read_to_string("src/server/ui/vendor/VENDOR.md")
        .expect("VENDOR.md must exist; see Plan 10-01");
    for line in vendor_md.lines() {
        if line.contains(filename) {
            for cell in line.split('|').rev() {
                let t = cell.trim();
                if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                    return t.to_lowercase();
                }
            }
        }
    }
    panic!("No SHA256 hash found for {} in VENDOR.md", filename);
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

// ===========================================================================
// DBUI-01: /debug/topology
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn topology_endpoint_emits_nodes_and_edges() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    let (status, headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200, "expected 200 from /debug/topology");
    assert!(
        headers
            .get("content-type")
            .map(|ct| ct.contains("application/json"))
            .unwrap_or(false),
        "expected JSON content-type, got {:?}",
        headers.get("content-type")
    );

    let json = body_json(&body);
    assert!(json.get("nodes").is_some(), "missing `nodes` field");
    assert!(json.get("edges").is_some(), "missing `edges` field");
    assert!(
        json.get("topo_order").is_some(),
        "missing `topo_order` field"
    );

    let nodes = json["nodes"].as_array().expect("nodes must be array");
    assert!(
        nodes.len() >= 4,
        "expected >=4 nodes (3 streams + 1 view), got {}",
        nodes.len()
    );

    let topo_order = json["topo_order"].as_array().expect("topo_order array");
    assert!(
        topo_order.len() >= 3,
        "expected topo_order >= 3 streams, got {}",
        topo_order.len()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topology_includes_cascade_edges() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    let (status, _headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let edges = json["edges"].as_array().expect("edges array");

    // At least one cascade edge Transactions -> Aggregates must be present.
    let found_cascade = edges.iter().any(|e| {
        e.get("from").and_then(|v| v.as_str()) == Some("Transactions")
            && e.get("to").and_then(|v| v.as_str()) == Some("Aggregates")
            && e.get("kind").and_then(|v| v.as_str()) == Some("cascade")
    });
    assert!(
        found_cascade,
        "expected cascade edge Transactions->Aggregates, got edges={:?}",
        edges
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topology_includes_view_nodes() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    let (status, _headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let nodes = json["nodes"].as_array().expect("nodes array");

    let has_view_node = nodes
        .iter()
        .any(|n| n.get("kind").and_then(|v| v.as_str()) == Some("view"));
    assert!(has_view_node, "expected at least one node with kind='view'");

    let edges = json["edges"].as_array().expect("edges array");
    let has_lookup_edge = edges
        .iter()
        .any(|e| e.get("kind").and_then(|v| v.as_str()) == Some("lookup"));
    assert!(
        has_lookup_edge,
        "expected at least one edge with kind='lookup', got edges={:?}",
        edges
    );
}

// ===========================================================================
// Phase 10.1 DBUI-06: /debug/topology nodes gain an additive `operators` array
// sourced from PipelineEngine::raw_register_jsons pass-through (RESEARCH
// Pattern 8). These tests lock the three key contract points:
//   1. Presence + basic shape (name, op, window) for a stream feature
//   2. Byte-for-byte where-clause preservation (no AST round-trip)
//   3. View lookup shape (op="lookup", target, on)
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn topology_nodes_include_operators_field() {
    // Phase 10.1 DBUI-06: /debug/topology nodes gain an `operators` array
    // sourced from PipelineEngine::raw_register_jsons pass-through. This test
    // locks the happy-path shape for a stream with a single Count feature.
    let (port, state) = start_debug_ui_server().await;

    // Register a Transactions stream directly via the engine AND call
    // store_raw_register_json to simulate the full register path that
    // main.rs / tcp.rs / http.rs follow. Without the raw JSON store we would
    // hit the Pitfall 7 empty-array fallback instead.
    {
        let mut engine = state.engine.write();
        let tx_def = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        };
        engine.register(tx_def).expect("register Transactions");
        engine.store_raw_register_json(
            "Transactions",
            serde_json::json!({
                "name": "Transactions",
                "key_field": "user_id",
                "features": [
                    {"name": "tx_count_1h", "type": "count", "window": "1h"}
                ]
            }),
        );
    }

    let (status, _headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200);
    let json = body_json(&body);

    let nodes = json["nodes"].as_array().expect("nodes array");
    let tx_node = nodes
        .iter()
        .find(|n| n["name"] == "Transactions")
        .expect("Transactions node present");

    // The additive field must be an array (never null, never missing).
    let operators = tx_node["operators"]
        .as_array()
        .expect("operators is an array");
    assert_eq!(
        operators.len(),
        1,
        "expected exactly 1 operator for tx_count_1h, got {:?}",
        operators
    );

    let op0 = &operators[0];
    assert_eq!(op0["name"], "tx_count_1h");
    assert_eq!(
        op0["op"], "count",
        "type -> op rename per CONTEXT backend contract"
    );
    assert_eq!(op0["window"], "1h");

    // features field MUST remain -- Phase 10 backward compat.
    let features = tx_node["features"]
        .as_array()
        .expect("features still present");
    assert_eq!(features.len(), 1);
    assert_eq!(features[0], "tx_count_1h");
}

#[tokio::test(flavor = "current_thread")]
async fn topology_operators_pass_through_where_clause() {
    // Phase 10.1 DBUI-06 + RESEARCH Pattern 8: operator entries must emit
    // the user's original `where` string verbatim -- no AST round-trip, no
    // normalization. The drill-in panel shows the exact text the user wrote.
    //
    // The backend reads directly from raw_register_jsons so we register the
    // stream with where_expr: None and rely on the raw JSON store to carry
    // the where-clause text. The backend does NOT cross-check the parsed
    // AST against the raw JSON -- it just passes the raw JSON through.
    let (port, state) = start_debug_ui_server().await;

    {
        let mut engine = state.engine.write();
        let tx_def = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "failed_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        };
        engine.register(tx_def).expect("register Transactions");
        engine.store_raw_register_json(
            "Transactions",
            serde_json::json!({
                "name": "Transactions",
                "key_field": "user_id",
                "features": [
                    {
                        "name": "failed_1h",
                        "type": "count",
                        "window": "1h",
                        "where": "status == 'failed'"
                    }
                ]
            }),
        );
    }

    let (status, _headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200);
    let json = body_json(&body);

    let tx_node = json["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "Transactions")
        .expect("Transactions node");
    let operators = tx_node["operators"].as_array().expect("operators array");
    assert_eq!(operators.len(), 1);
    let op0 = &operators[0];
    assert_eq!(op0["name"], "failed_1h");
    assert_eq!(op0["op"], "count");
    assert_eq!(op0["window"], "1h");
    // Exact byte-for-byte pass-through -- NOT a re-parse of the AST.
    assert_eq!(
        op0["where"], "status == 'failed'",
        "where-clause must be preserved verbatim; got {:?}",
        op0["where"]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn topology_view_operators_include_lookup_shape() {
    // Phase 10.1 DBUI-06: view nodes emit operators with lookup-specific
    // fields -- op == "lookup", target, on -- so the drill-in panel's view
    // variant can render them.
    let (port, state) = start_debug_ui_server().await;

    // First register the target stream (MerchantActivity) so the view's
    // lookup target exists. Then register the view. Both need
    // raw_register_jsons entries for the operators projection to emit.
    {
        let mut engine = state.engine.write();

        let merchant_def = StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            group_by_keys: None,
            features: vec![(
                "chargeback_count_24h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(86_400),
                    bucket: Duration::from_secs(3600),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        };
        engine
            .register(merchant_def)
            .expect("register MerchantActivity");
        engine.store_raw_register_json(
            "MerchantActivity",
            serde_json::json!({
                "name": "MerchantActivity",
                "key_field": "merchant_id",
                "features": [
                    {"name": "chargeback_count_24h", "type": "count", "window": "24h"}
                ]
            }),
        );

        let view_def = ViewDefinition {
            name: "FraudSignals".into(),
            key_field: "user_id".into(),
            features: vec![(
                "merchant_chargebacks".into(),
                ViewFeatureDef::Lookup {
                    target_stream: "MerchantActivity".into(),
                    target_feature: "chargeback_count_24h".into(),
                    on_field: "merchant_id".into(),
                },
            )],
        };
        engine
            .register_view(view_def)
            .expect("register FraudSignals");
        engine.store_raw_register_json(
            "FraudSignals",
            serde_json::json!({
                "name": "FraudSignals",
                "key_field": "user_id",
                "features": [
                    {
                        "name": "merchant_chargebacks",
                        "type": "lookup",
                        "target": "MerchantActivity.chargeback_count_24h",
                        "on": "merchant_id"
                    }
                ]
            }),
        );
    }

    let (status, _headers, body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200);
    let json = body_json(&body);

    let fraud_node = json["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "FraudSignals")
        .expect("FraudSignals view node");
    assert_eq!(fraud_node["kind"], "view");

    let operators = fraud_node["operators"]
        .as_array()
        .expect("view operators array");
    assert_eq!(operators.len(), 1);
    let op0 = &operators[0];
    assert_eq!(op0["name"], "merchant_chargebacks");
    assert_eq!(op0["op"], "lookup");
    assert_eq!(op0["target"], "MerchantActivity.chargeback_count_24h");
    assert_eq!(op0["on"], "merchant_id");
}

// ===========================================================================
// DBUI-02: /debug/throughput
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn throughput_endpoint_emits_per_stream_ewma() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // One push so the throughput tracker has a Transactions entry.
    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "u1", "amount": 10.0}),
    );

    let (status, _headers, body) = http_get(port, "/debug/throughput").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let streams = json["streams"].as_array().expect("streams array");

    let tx_entry = streams
        .iter()
        .find(|s| s.get("name").and_then(|v| v.as_str()) == Some("Transactions"))
        .expect("expected a Transactions entry in throughput snapshot");
    assert!(tx_entry.get("ewma_5s").is_some(), "missing ewma_5s field");
    assert!(tx_entry.get("ewma_1m").is_some(), "missing ewma_1m field");
    assert!(tx_entry.get("ewma_5m").is_some(), "missing ewma_5m field");
    assert!(tx_entry["ewma_5s"].is_number(), "ewma_5s must be a number");
    assert!(tx_entry["ewma_1m"].is_number(), "ewma_1m must be a number");
    assert!(tx_entry["ewma_5m"].is_number(), "ewma_5m must be a number");
}

#[tokio::test(flavor = "current_thread")]
async fn throughput_reflects_recent_pushes() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // Push a few events, spaced slightly apart so bump_unique sees non-zero
    // inter-arrival times and folds them into the EWMA. The first bump
    // initializes the EWMA to zero (there is no dt to measure yet), so we
    // need at least two bumps with dt > 0 to see a non-zero rate.
    for i in 0..5 {
        push_event(
            &state,
            "Transactions",
            &serde_json::json!({"user_id": format!("u{}", i), "amount": 10.0}),
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let (status, _headers, body) = http_get(port, "/debug/throughput").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let streams = json["streams"].as_array().expect("streams array");
    let tx_entry = streams
        .iter()
        .find(|s| s.get("name").and_then(|v| v.as_str()) == Some("Transactions"))
        .expect("expected Transactions entry after pushes");
    let ewma_5s = tx_entry["ewma_5s"].as_f64().expect("ewma_5s is a number");
    assert!(
        ewma_5s > 0.0,
        "expected Transactions ewma_5s > 0 after burst pushes, got {}",
        ewma_5s
    );
}

#[tokio::test(flavor = "current_thread")]
async fn throughput_decays_when_idle() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // Drive the 5s EWMA up with a tight burst (non-zero dt between bumps).
    for i in 0..3 {
        push_event(
            &state,
            "Transactions",
            &serde_json::json!({"user_id": format!("u{}", i), "amount": 1.0}),
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Snapshot initial ewma_5s via the public endpoint.
    let (_status, _headers, body) = http_get(port, "/debug/throughput").await;
    let initial = body_json(&body);
    let initial_ewma = initial["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s.get("name").and_then(|v| v.as_str()) == Some("Transactions"))
        .map(|s| s["ewma_5s"].as_f64().unwrap_or(0.0))
        .unwrap_or(0.0);
    assert!(
        initial_ewma > 0.0,
        "expected initial ewma_5s > 0 for Transactions after burst, got {}",
        initial_ewma
    );

    // Idle the tracker: no pushes, wait 500 ms of wall time. The 5s EWMA has
    // a time constant of 5.0 s, so after 0.5 s we expect a ~10% drop
    // (ewma * exp(-0.5/5)) -- strictly less than the initial value.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (_status, _headers, body2) = http_get(port, "/debug/throughput").await;
    let later = body_json(&body2);
    let later_ewma = later["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s.get("name").and_then(|v| v.as_str()) == Some("Transactions"))
        .map(|s| s["ewma_5s"].as_f64().unwrap_or(0.0))
        .unwrap_or(0.0);

    assert!(
        later_ewma < initial_ewma,
        "expected ewma_5s to decay when idle: initial={} later={}",
        initial_ewma,
        later_ewma
    );
}

// ===========================================================================
// DBUI-03: /debug/key/{key} (entity inspection, reuses existing endpoint)
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn entity_lookup_reuses_existing_endpoint() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // Push one event for u_demo so the entity exists.
    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "u_demo", "amount": 42.0}),
    );

    let (status, headers, body) = http_get(port, "/debug/key/u_demo").await;
    assert_eq!(status, 200, "expected 200 from /debug/key/u_demo");
    assert!(
        headers
            .get("content-type")
            .map(|ct| ct.contains("application/json"))
            .unwrap_or(false),
        "expected JSON content-type"
    );

    let json = body_json(&body);
    assert_eq!(
        json.get("key").and_then(|v| v.as_str()),
        Some("u_demo"),
        "expected key field to echo back u_demo"
    );
    let computed = json
        .get("computed_features")
        .and_then(|v| v.as_object())
        .expect("computed_features object");
    assert!(
        !computed.is_empty(),
        "expected at least one computed feature for u_demo, got {:?}",
        computed
    );
}

// ===========================================================================
// CR-01 regression guard (Phase 10.1 review)
//
// The client-side entity lookup filter in src/server/ui/app.js was originally
// written as if /debug/key/{key} returned feature names in dotted
// `StreamName.feature_name` form, but the authoritative server emits FLAT
// names (see `StateStore::get_all_features`, src/state/store.rs). That
// mismatch broke the filter: the dotted-name split unconditionally hit the
// `continue` branch and every entity lookup reported "No features for {key}".
//
// The CR-01 fix in app.js reworks the filter to build an allow-list from the
// selected topology node's `features` array and match against the flat keys.
// This test pins down every part of that contract so the regression cannot
// silently slip back in:
//
//   1. /debug/key/{key} must emit flat feature names for a two-stream
//      pipeline (no "Transactions." or "Logins." prefix on the keys).
//   2. /debug/topology must carry a per-stream `features` array matching
//      those flat names exactly (so the frontend's allow-list will hit).
//   3. The simulated frontend filter (built here from the topology
//      allow-list + the flat computed_features) must return a non-empty
//      subset for the selected stream — i.e. the feature the frontend was
//      failing to render pre-CR-01 is now present.
//   4. The served app.js must NOT contain the old dotted-name parsing
//      pattern, and MUST contain the new allow-list markers. This is a
//      source-level regression guard paralleling
//      `app_js_has_no_innerhtml_or_eval_sinks`.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn entity_lookup_filter_uses_flat_feature_names() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // Push events for the SAME key on two different streams so
    // computed_features carries one feature per stream. This is the
    // cross-stream scenario the original dotted-name filter was (wrongly)
    // trying to support.
    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "u_cr01", "amount": 9.0}),
    );
    push_event(&state, "Logins", &serde_json::json!({"user_id": "u_cr01"}));

    // --- (1) /debug/key/{key} must emit FLAT feature names.
    let (status, _headers, body) = http_get(port, "/debug/key/u_cr01").await;
    assert_eq!(status, 200, "expected 200 from /debug/key/u_cr01");
    let key_json = body_json(&body);
    let computed = key_json
        .get("computed_features")
        .and_then(|v| v.as_object())
        .expect("computed_features object");

    assert!(
        computed.contains_key("tx_count_1h"),
        "expected flat feature name tx_count_1h in computed_features, got keys {:?}",
        computed.keys().collect::<Vec<_>>()
    );
    assert!(
        computed.contains_key("login_count_1h"),
        "expected flat feature name login_count_1h in computed_features, got keys {:?}",
        computed.keys().collect::<Vec<_>>()
    );
    for flat_key in computed.keys() {
        assert!(
            !flat_key.contains('.'),
            "CR-01: computed_features key {:?} must be flat (no stream prefix) \
             — frontend filter relies on flat names",
            flat_key
        );
    }

    // --- (2) /debug/topology must carry a per-stream `features` array that
    //         contains the same flat names, which is the allow-list source
    //         the frontend uses for its filter.
    let (status, _headers, topo_body) = http_get(port, "/debug/topology").await;
    assert_eq!(status, 200, "expected 200 from /debug/topology");
    let topo = body_json(&topo_body);
    let nodes = topo
        .get("nodes")
        .and_then(|v| v.as_array())
        .expect("topology.nodes array");

    let find_node = |name: &str| -> &serde_json::Value {
        nodes
            .iter()
            .find(|n| n.get("name").and_then(|v| v.as_str()) == Some(name))
            .unwrap_or_else(|| panic!("topology node {:?} missing", name))
    };
    let tx_node = find_node("Transactions");
    let tx_features: Vec<&str> = tx_node
        .get("features")
        .and_then(|v| v.as_array())
        .expect("Transactions.features array")
        .iter()
        .map(|v| v.as_str().expect("feature name is a string"))
        .collect();
    assert!(
        tx_features.contains(&"tx_count_1h"),
        "CR-01: Transactions.features must contain tx_count_1h so the \
         frontend allow-list filter can match the flat computed_features \
         key. Got {:?}",
        tx_features
    );
    let logins_node = find_node("Logins");
    let logins_features: Vec<&str> = logins_node
        .get("features")
        .and_then(|v| v.as_array())
        .expect("Logins.features array")
        .iter()
        .map(|v| v.as_str().expect("feature name is a string"))
        .collect();
    assert!(
        logins_features.contains(&"login_count_1h"),
        "CR-01: Logins.features must contain login_count_1h. Got {:?}",
        logins_features
    );

    // --- (3) Simulate the frontend filter: allow-list from the selected
    //         stream's topology `features` array ∩ flat computed_features.
    //         Pre-CR-01 this intersection was empty for every stream.
    let allowed: std::collections::HashSet<&str> = tx_features.iter().copied().collect();
    let filtered: Vec<&String> = computed
        .keys()
        .filter(|k| allowed.contains(k.as_str()))
        .collect();
    assert!(
        filtered.iter().any(|k| k.as_str() == "tx_count_1h"),
        "CR-01: simulated frontend filter for stream=Transactions must \
         return tx_count_1h. filtered={:?}, allowed={:?}, computed={:?}",
        filtered,
        allowed,
        computed.keys().collect::<Vec<_>>()
    );
    // Cross-stream isolation: Transactions' allow-list must NOT leak the
    // Logins feature. This was never broken, but the intent was ambiguous
    // pre-CR-01 — pin it down.
    assert!(
        !filtered.iter().any(|k| k.as_str() == "login_count_1h"),
        "CR-01: Transactions allow-list must not include Logins' flat \
         feature name. filtered={:?}",
        filtered
    );
}

#[tokio::test(flavor = "current_thread")]
async fn app_js_entity_lookup_uses_allow_list_not_dotted_names() {
    // Source-level regression guard complementing the functional test
    // above. If a future refactor rewrites the filter as
    //   `const dot = fullName.indexOf('.'); if (dot <= 0) continue;`
    // (the original broken pattern), this test fails at compile-check
    // speed — no server state, no event push required.
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/static/app.js").await;
    assert_eq!(status, 200, "expected 200 for /static/app.js");
    let text = String::from_utf8(body).expect("app.js is utf-8");

    // Forbidden: the broken dotted-name parsing pattern. Any recurrence
    // indicates a regression — the filter has gone back to assuming the
    // server emits `Stream.feature` dotted names, which it does NOT.
    for forbidden in &[
        "fullName.indexOf('.')",
        "fullName.substring(0, dot)",
        "fullName.substring(dot + 1)",
    ] {
        assert!(
            !text.contains(forbidden),
            "CR-01 regression: forbidden dotted-name filter token {:?} \
             reappeared in app.js — the entity lookup filter must use the \
             topology `features` allow-list instead",
            forbidden
        );
    }

    // Required: the new allow-list markers must be present. If someone
    // deletes the filter block entirely (or the function), this catches
    // it at test time.
    for required in &[
        "data.computed_features",
        "(node && node.features)",
        "allowed.has(name)",
    ] {
        assert!(
            text.contains(required),
            "CR-01 regression: expected allow-list marker {:?} missing from \
             app.js — the entity lookup filter has been rewritten in an \
             unexpected way",
            required
        );
    }
}

// ===========================================================================
// DBUI-04: /debug/memory (per-stream breakdown + backward compat)
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn memory_endpoint_emits_per_stream_breakdown() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    // Push for two user_ids across Transactions and Logins to populate
    // per-stream key counts in the state store.
    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "uA", "amount": 1.0}),
    );
    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "uB", "amount": 2.0}),
    );
    push_event(&state, "Logins", &serde_json::json!({"user_id": "uA"}));

    let (status, _headers, body) = http_get(port, "/debug/memory").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let per_stream = json
        .get("per_stream")
        .and_then(|v| v.as_array())
        .expect("per_stream array missing");

    let find_named = |name: &str| {
        per_stream
            .iter()
            .find(|e| e.get("name").and_then(|v| v.as_str()) == Some(name))
    };

    let tx = find_named("Transactions").expect("Transactions entry missing");
    assert_eq!(
        tx.get("kind").and_then(|v| v.as_str()),
        Some("stream"),
        "Transactions must be kind=stream"
    );
    assert!(tx.get("name").is_some());
    assert!(tx.get("key_count").is_some());
    assert!(tx.get("estimated_bytes").is_some());

    let logins = find_named("Logins").expect("Logins entry missing");
    assert_eq!(
        logins.get("kind").and_then(|v| v.as_str()),
        Some("stream"),
        "Logins must be kind=stream"
    );

    let user_risk = find_named("UserRisk").expect("UserRisk entry missing");
    assert_eq!(
        user_risk.get("kind").and_then(|v| v.as_str()),
        Some("view"),
        "UserRisk must be kind=view"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_endpoint_backward_compatible() {
    let (port, state) = start_debug_ui_server().await;
    register_test_pipeline(&state);

    push_event(
        &state,
        "Transactions",
        &serde_json::json!({"user_id": "uA", "amount": 1.0}),
    );

    let (status, _headers, body) = http_get(port, "/debug/memory").await;
    assert_eq!(status, 200);
    let json = body_json(&body);

    // Three original top-level fields must still be present (Phase 6 contract).
    assert!(
        json.get("entity_count").is_some(),
        "missing pre-existing entity_count field"
    );
    assert!(
        json.get("stream_count").is_some(),
        "missing pre-existing stream_count field"
    );
    assert!(
        json.get("estimated_bytes").is_some(),
        "missing pre-existing estimated_bytes field"
    );
    // And the new per_stream extension.
    assert!(
        json.get("per_stream").is_some(),
        "missing new per_stream field"
    );
}

// ===========================================================================
// DBUI-05: embedded UI + static assets
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn static_index_is_embedded() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, headers, body) = http_get(port, "/").await;
    assert_eq!(status, 200);
    let ct = headers
        .get("content-type")
        .expect("content-type header missing");
    assert!(
        ct.starts_with("text/html"),
        "expected text/html, got {}",
        ct
    );
    let body = body_string(&body);
    assert!(
        body.contains("beava \u{2014} debug"),
        "expected `beava \u{2014} debug` title in index.html body"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn static_css_is_embedded() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, headers, body) = http_get(port, "/static/app.css").await;
    assert_eq!(status, 200);
    let ct = headers
        .get("content-type")
        .expect("content-type header missing");
    assert!(ct.starts_with("text/css"), "expected text/css, got {}", ct);
    let body = body_string(&body);
    assert!(!body.is_empty(), "app.css body must not be empty");
    assert!(
        body.contains("--accent-primary") || body.contains("--accent-blue"),
        "expected a primary accent CSS custom property in app.css"
    );
}

// ===========================================================================
// Phase 10.1 DBUI-06: split-view shell regression tests
// ===========================================================================
//
// Phase 10.1 deletes the Phase 10 four-tab layout and replaces it with a
// split-view shell: a DAG canvas on the left and a drill-in panel on the
// right. These two tests lock the contract of the shell REWRITE:
//
//   * `split_view_shell_has_no_tab_bar` — forbidden-substrings grep; every
//     Phase 10 tab-bar identifier and htmx attribute must be gone.
//   * `split_view_shell_has_dag_canvas_and_drill_in_panel` — required-
//     substrings grep; new structural markers plus preserved header/footer
//     hooks must all be present, and the vendor script order is asserted.
//
// Both tests are source-level (they grep the served HTML bytes); they
// complement the existing `app_js_has_no_innerhtml_or_eval_sinks` sink-level
// regression and the Phase 10 `static_index_is_embedded` title regression.

#[tokio::test(flavor = "current_thread")]
async fn split_view_shell_has_no_tab_bar() {
    // Phase 10.1 DBUI-06: the four-tab flat layout from Phase 10 is deleted
    // wholesale. The index.html served at `/` must contain zero references
    // to the old tab-bar DOM. Regression guard — see RESEARCH Pitfall 8.
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/").await;
    assert_eq!(status, 200, "GET / must succeed");
    let body = body_string(&body);

    for forbidden in &[
        "class=\"tab-bar\"",
        "id=\"tab-topology\"",
        "id=\"tab-streams\"",
        "id=\"tab-entity\"",
        "id=\"tab-memory\"",
        "id=\"panel-topology\"",
        "id=\"panel-streams\"",
        "id=\"panel-entity\"",
        "id=\"panel-memory\"",
        "role=\"tablist\"",
        "role=\"tab\"",
        "role=\"tabpanel\"",
        "class=\"tab-panel\"",
        "class=\"topology-card\"",
        "class=\"streams-card\"",
        "class=\"entity-search-card\"",
        "class=\"memory-card\"",
    ] {
        assert!(
            !body.contains(forbidden),
            "Phase 10.1 regression: legacy tab-bar markup {:?} must be removed from index.html",
            forbidden
        );
    }

    // Additionally: no htmx hx-* attributes in the static shell — Plan 03
    // uses vanilla fetch + setInterval for all polling (RESEARCH Pitfall 2).
    // Phase 10.1's index.html must not carry any of them.
    for forbidden_htmx in &[
        "hx-get=\"/debug/",
        "hx-trigger=\"load",
        "hx-trigger=\"every",
        "hx-swap=\"",
        "hx-on::",
    ] {
        assert!(
            !body.contains(forbidden_htmx),
            "Phase 10.1: no htmx attributes in static shell; found {:?}",
            forbidden_htmx
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn split_view_shell_has_dag_canvas_and_drill_in_panel() {
    // Phase 10.1 DBUI-06: the new split-view shell has a left-side DAG
    // canvas and a right-side drill-in panel. This test locks the specific
    // DOM hooks Plan 03's app.js will bind to.
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/").await;
    assert_eq!(status, 200, "GET / must succeed");
    let body = body_string(&body);

    // Shell containers
    assert!(
        body.contains("class=\"split-view\"") || body.contains("class=\"app-main split-view\""),
        "missing main.split-view container"
    );
    assert!(
        body.contains("class=\"dag-canvas\""),
        "missing .dag-canvas section"
    );
    assert!(
        body.contains("class=\"drill-in-panel\"")
            || body.contains("drill-in-panel\" data-empty")
            || body.contains("data-empty=\"true\""),
        "missing .drill-in-panel aside"
    );

    // SVG + ARIA
    assert!(
        body.contains("id=\"topology-svg\""),
        "missing #topology-svg element"
    );
    assert!(body.contains("role=\"img\""), "missing role=\"img\" on SVG");
    assert!(
        body.contains("aria-label=\"Pipeline topology graph\""),
        "missing SVG aria-label"
    );

    // Drill-in panel placeholder copy
    assert!(
        body.contains("Select a stream to see details"),
        "missing drill-in placeholder copy"
    );

    // Header hooks preserved from Phase 10 (Plan 03 app.js binds to these)
    assert!(body.contains("id=\"pause-btn\""), "missing pause button id");
    assert!(
        body.contains("id=\"poll-status\""),
        "missing poll-status aria-live span"
    );
    assert!(
        body.contains("id=\"poll-dot\"") || body.contains("class=\"poll-dot\""),
        "missing poll-dot element"
    );
    assert!(
        body.contains("id=\"poll-label\"") || body.contains("class=\"poll-label\""),
        "missing poll-label element"
    );

    // Footer hooks preserved from Phase 10
    assert!(
        body.contains("id=\"footer-version\""),
        "missing footer-version span"
    );
    assert!(
        body.contains("id=\"footer-host\""),
        "missing footer-host span"
    );
    assert!(
        body.contains("id=\"footer-update\""),
        "missing footer-update span"
    );

    // Title locked by Phase 10 regression test static_index_is_embedded,
    // re-asserted here for defense-in-depth
    assert!(
        body.contains("beava \u{2014} debug"),
        "title must remain 'beava — debug'"
    );

    // Vendor script order: d3 before dagre-d3 before htmx before app.js
    let d3_pos = body.find("/static/vendor/d3.min.js").expect("d3 script");
    let dagre_pos = body
        .find("/static/vendor/dagre-d3.min.js")
        .expect("dagre-d3 script");
    let htmx_pos = body
        .find("/static/vendor/htmx.min.js")
        .expect("htmx script");
    let app_pos = body.find("/static/app.js").expect("app.js script");
    assert!(
        d3_pos < dagre_pos,
        "d3 must load before dagre-d3 (dagre-d3 depends on d3)"
    );
    assert!(dagre_pos < app_pos, "dagre-d3 must load before app.js");
    assert!(htmx_pos < app_pos, "htmx must load before app.js");
}

#[tokio::test(flavor = "current_thread")]
async fn static_htmx_is_vendored_and_hashed() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, headers, body) = http_get(port, "/static/vendor/htmx.min.js").await;
    assert_eq!(status, 200, "expected 200 for vendored htmx.min.js");
    let ct = headers.get("content-type").cloned().unwrap_or_default();
    assert!(
        ct.contains("javascript"),
        "expected javascript content-type, got {}",
        ct
    );
    assert!(!body.is_empty(), "htmx body empty");

    let actual = sha256_hex(&body);
    let expected = expected_hash_for("htmx.min.js");
    assert_eq!(
        actual, expected,
        "htmx.min.js SHA256 drift: on-disk bytes differ from VENDOR.md manifest"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn static_dagre_is_vendored_and_hashed() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, headers, body) = http_get(port, "/static/vendor/dagre-d3.min.js").await;
    assert_eq!(status, 200, "expected 200 for vendored dagre-d3.min.js");
    let ct = headers.get("content-type").cloned().unwrap_or_default();
    assert!(
        ct.contains("javascript"),
        "expected javascript content-type, got {}",
        ct
    );

    let actual = sha256_hex(&body);
    let expected = expected_hash_for("dagre-d3.min.js");
    assert_eq!(
        actual, expected,
        "dagre-d3.min.js SHA256 drift: on-disk bytes differ from VENDOR.md manifest"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn static_d3_is_vendored_and_hashed() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, headers, body) = http_get(port, "/static/vendor/d3.min.js").await;
    assert_eq!(status, 200, "expected 200 for vendored d3.min.js");
    let ct = headers.get("content-type").cloned().unwrap_or_default();
    assert!(
        ct.contains("javascript"),
        "expected javascript content-type, got {}",
        ct
    );

    let actual = sha256_hex(&body);
    let expected = expected_hash_for("d3.min.js");
    assert_eq!(
        actual, expected,
        "d3.min.js SHA256 drift: on-disk bytes differ from VENDOR.md manifest"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn static_unknown_returns_404() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, _body) = http_get(port, "/static/does-not-exist.css").await;
    assert_eq!(status, 404, "expected 404 for unknown static asset");
}

// ===========================================================================
// Phase 10 review WR-03: XSS sink regression test for app.js
// ===========================================================================
//
// Every entity-key DOM write in app.js goes through `.textContent` (or the
// default dagre-d3 `labelType: 'text'` which emits an escaped <text> SVG
// element). The XSS threat model (T-10-02) requires that no user-supplied
// string reach an HTML-parsing sink. This test greps the served app.js bytes
// for the forbidden sink substrings so that a future refactor which flips a
// `.textContent` assignment over to `.innerHTML` fails `cargo test` instead
// of silently regressing.
//
// This is a source-level check, not a rendered-DOM check — it complements,
// does not replace, the planned Phase 10.2 headless-browser smoke test.
// Vendor bundles are intentionally NOT scanned: htmx, d3, and dagre-d3 all
// legitimately contain these substrings inside their library internals.

#[tokio::test(flavor = "current_thread")]
async fn app_js_has_no_innerhtml_or_eval_sinks() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/static/app.js").await;
    assert_eq!(status, 200, "expected 200 for /static/app.js");
    let text = String::from_utf8(body).expect("app.js is utf-8");

    // These are the sinks forbidden by Plan 10-04's XSS contract. Any match
    // indicates a regression — a user-supplied string is being written into
    // the DOM via an HTML-parsing sink instead of `.textContent`.
    for forbidden in &[
        ".innerHTML",
        ".outerHTML",
        "insertAdjacentHTML",
        "document.write",
        "eval(",
        "new Function(",
        "labelType: 'html'",
        "labelType:\"html\"",
    ] {
        assert!(
            !text.contains(forbidden),
            "forbidden XSS sink {:?} found in app.js — Phase 10 review WR-03",
            forbidden
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 10.2 (DBUI-07): Latency endpoint tests
// ---------------------------------------------------------------------------

/// GET /debug/latency returns valid JSON with per_command (4 entries),
/// per_stream (empty initially), and slow_queries (empty initially).
#[tokio::test]
async fn debug_latency_returns_valid_json_with_all_sections() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/debug/latency").await;
    assert_eq!(status, 200);

    let json = body_json(&body);

    // per_command: 4 entries (PUSH/GET/SET/MSET)
    let per_command = json["per_command"].as_array().expect("per_command array");
    assert_eq!(per_command.len(), 4, "should have 4 command entries");

    // Each command entry has the required fields
    for entry in per_command {
        assert!(entry["command"].is_string());
        assert!(entry["count"].is_number());
        assert!(entry["p50_us"].is_number());
        assert!(entry["p95_us"].is_number());
        assert!(entry["p99_us"].is_number());
        assert!(entry["histogram"]["bin_edges_us"].is_array());
        assert!(entry["histogram"]["counts"].is_array());
    }

    // Command names are correct
    let commands: Vec<&str> = per_command
        .iter()
        .map(|e| e["command"].as_str().unwrap())
        .collect();
    assert_eq!(commands, vec!["PUSH", "GET", "SET", "MSET"]);

    // per_stream is an array (empty with no pushes)
    assert!(json["per_stream"].is_array());
    assert_eq!(json["per_stream"].as_array().unwrap().len(), 0);

    // slow_queries is an array (empty)
    assert!(json["slow_queries"].is_array());
    assert_eq!(json["slow_queries"].as_array().unwrap().len(), 0);
}

/// Histogram bins have correct count (30 bins, 31 edges).
#[tokio::test]
async fn debug_latency_histogram_has_correct_bin_count() {
    let (port, _state) = start_debug_ui_server().await;
    let (status, _headers, body) = http_get(port, "/debug/latency").await;
    assert_eq!(status, 200);

    let json = body_json(&body);
    let push = &json["per_command"][0];
    let edges = push["histogram"]["bin_edges_us"].as_array().unwrap();
    let counts = push["histogram"]["counts"].as_array().unwrap();
    assert_eq!(edges.len(), 31, "should have 31 bin edges for 30 bins");
    assert_eq!(counts.len(), 30, "should have 30 bin counts");
}
