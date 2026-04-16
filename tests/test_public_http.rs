//! Phase 20 TRAC-04 / TRAC-07: public HTTP surface tests.
//!
//! All three `/public/*` endpoints plus the three new Prometheus metrics.
//! Uses the same real-HTTP-over-loopback pattern as `test_debug_ui.rs`:
//! bind a listener on 127.0.0.1:0 → spawn the server → issue raw HTTP/1.1
//! requests over a `TcpStream`. This ensures `ConnectInfo<SocketAddr>` is
//! populated correctly and exercises the full middleware stack end-to-end.
//!
//! Names must match `.planning/phases/20-traction-demo/20-VALIDATION.md`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use beava::engine::pipeline::{FeatureDef, PipelineEngine, StreamDefinition};
use beava::server::tcp::{make_concurrent_state_full, BackfillTracker, SharedState};
#[cfg(feature = "demo")]
use beava::server::tcp::RecentEvent;
use beava::state::store::StateStore;

// ---------------------------------------------------------------------------
// Test harness (mirrors tests/test_debug_ui.rs:start_debug_ui_server).
// ---------------------------------------------------------------------------

fn make_test_state() -> SharedState {
    make_concurrent_state_full(
        PipelineEngine::new(),
        StateStore::new(),
        None,
        std::path::PathBuf::from("/tmp/beava-test-public-http.snapshot"),
        Arc::new(BackfillTracker::default()),
        true,
        false,
        Some("secret".into()),
        false,
    )
}

async fn start_server() -> (u16, SharedState) {
    let state = make_test_state();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_state = state.clone();
    tokio::spawn(async move {
        beava::server::http::run_http_server_with_listener(listener, server_state)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (port, state)
}

type HttpResponse = (u16, std::collections::HashMap<String, String>, Vec<u8>);

async fn http_get(port: u16, path: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        path
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("missing HTTP header/body separator");
    let head = std::str::from_utf8(&buf[..sep]).unwrap().to_string();
    let body = buf[sep + 4..].to_vec();
    let mut lines = head.lines();
    let status_line = lines.next().unwrap();
    let status_code: u16 = status_line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut headers = std::collections::HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    (status_code, headers, body)
}

fn body_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("valid JSON body")
}

fn body_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

// ---------------------------------------------------------------------------
// Helpers to seed state.
// ---------------------------------------------------------------------------

fn register_txns(state: &SharedState) {
    let mut engine = state.engine.write();
    engine
        .register(StreamDefinition {
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
        })
        .expect("register Transactions");
}

fn push_event(state: &SharedState, payload: &serde_json::Value) {
    let now = SystemTime::now();
    let engine = state.engine.read();
    let _ = engine.push("Transactions", payload, &state.store, now);
}

fn push_many(state: &SharedState, key: &str, n: usize) {
    for _ in 0..n {
        let payload = serde_json::json!({"user_id": key, "amount": 1.0});
        push_event(state, &payload);
    }
}

#[cfg(feature = "demo")]
fn seed_recent_events(state: &SharedState, n: usize) {
    let mut ring = state.recent_events.lock();
    for i in 0..n {
        ring.push(RecentEvent {
            ts_ms: 1_000_000 + i as u64,
            stream: "Transactions".into(),
            key: format!("u{}", i),
            payload_preview: "{\"amount\":1}".into(),
        });
    }
}

// ===========================================================================
// TRAC-04: /public/features/{key}
// ===========================================================================

#[tokio::test]
async fn public_features_returns_feature_map() {
    let (port, state) = start_server().await;
    register_txns(&state);
    push_many(&state, "u1", 50);

    let (status, _headers, body) = http_get(port, "/public/features/u1").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    assert_eq!(json["key"], "u1");
    let feats = json["features"].as_object().expect("features object");
    assert!(feats.contains_key("tx_count_1h"), "features: {:?}", feats);
}

#[tokio::test]
async fn public_features_no_operator_state() {
    // Security-critical: the PUBLIC endpoint must not leak operator internals.
    let (port, state) = start_server().await;
    register_txns(&state);
    push_many(&state, "u1", 10);

    let (_status, _headers, body) = http_get(port, "/public/features/u1").await;
    let text = body_string(&body);
    for leaky in ["buckets", "hll", "operator_state", "estimated_bytes", "live_operators"] {
        assert!(
            !text.contains(leaky),
            "leaked internal field `{}` in public response: {}",
            leaky,
            text
        );
    }
}

#[tokio::test]
async fn public_features_unknown_key() {
    let (port, state) = start_server().await;
    register_txns(&state);
    let (status, _headers, _body) = http_get(port, "/public/features/does_not_exist").await;
    assert_eq!(status, 404);
}

// ===========================================================================
// TRAC-04: /public/recent-events
// ===========================================================================

#[cfg(feature = "demo")]
#[tokio::test]
async fn public_recent_events_default_limit() {
    let (port, state) = start_server().await;
    seed_recent_events(&state, 40);

    let (status, _headers, body) = http_get(port, "/public/recent-events").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let events = json["events"].as_array().expect("events array");
    assert!(events.len() <= 20, "default limit should cap at 20, got {}", events.len());
    assert_eq!(events.len(), 20, "should return exactly 20 with 40 seeded");
}

#[cfg(feature = "demo")]
#[tokio::test]
async fn public_recent_events_limit_clamp() {
    let (port, state) = start_server().await;
    seed_recent_events(&state, 100);

    let (status, _headers, body) = http_get(port, "/public/recent-events?limit=9999").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    let events = json["events"].as_array().unwrap();
    assert!(
        events.len() <= 100,
        "limit must clamp to ring capacity (100), got {}",
        events.len()
    );
}

// ===========================================================================
// TRAC-04: /public/stats
// ===========================================================================

#[tokio::test]
async fn public_stats_shape() {
    let (port, _state) = start_server().await;
    let (status, _headers, body) = http_get(port, "/public/stats").await;
    assert_eq!(status, 200);
    let json = body_json(&body);
    for field in [
        "events_total",
        "current_eps",
        "p99_push_us",
        "p50_push_us",
        "uptime_seconds",
        "keys_total",
    ] {
        assert!(json.get(field).is_some(), "missing field `{}`", field);
        assert!(
            json[field].is_number(),
            "field `{}` must be numeric",
            field
        );
    }
    assert!(json["uptime_seconds"].as_u64().unwrap_or(u64::MAX) < 3600);
}

#[tokio::test]
async fn public_stats_cors_header() {
    let (port, _state) = start_server().await;
    let (_status, headers, _body) = http_get(port, "/public/stats").await;
    assert_eq!(
        headers.get("access-control-allow-origin").map(String::as_str),
        Some("*"),
        "CORS header missing on /public/stats; headers: {:?}",
        headers
    );
}

// ===========================================================================
// TRAC-07: extended /metrics
// ===========================================================================

#[tokio::test]
async fn metrics_contains_new_fields() {
    let (port, _state) = start_server().await;
    let (status, _headers, body) = http_get(port, "/metrics").await;
    assert_eq!(status, 200);
    let text = body_string(&body);
    for field in [
        "beava_events_total",
        "beava_push_latency_p99_seconds",
        "beava_current_eps",
    ] {
        assert!(
            text.contains(field),
            "missing metric `{}` in /metrics; body:\n{}",
            field,
            text
        );
    }
}
