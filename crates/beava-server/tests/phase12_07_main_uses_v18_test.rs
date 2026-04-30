//! Plan 12-07 Wave 6 — production binary `target/release/beava` must boot
//! ServerV18 (mio data plane) and serve /get without `BEAVA_DEV_ENDPOINTS=1`.
//!
//! Three tests:
//! 1. `test_release_binary_exists_and_runs_with_minimal_config` — sanity gate;
//!    spawn the binary, poll /health, SIGTERM. PASSES today (binary boots).
//! 2. `test_release_binary_responds_to_get_without_dev_endpoints_env` — RED
//!    today; legacy `Server::bind` gates `feature_query_router` behind
//!    `BEAVA_DEV_ENDPOINTS=1`. Becomes GREEN when main.rs flips to ServerV18.
//! 3. `test_release_binary_responds_to_post_get_without_dev_endpoints_env` —
//!    same scenario for batch /get.

#![cfg(feature = "testing")]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Serializer for tests that boot the release binary subprocess + bind ports.
/// Pattern: `{ let _g = RELEASE_BINARY_SERIALIZER.lock(); }` — drop guard
/// before any await. Mirrors phase18_04_6_integration_test.rs:23 etc.
static RELEASE_BINARY_SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Path to `target/release/beava`. Walks up two levels from this crate's
/// manifest (workspace root + `target/release/beava`).
fn release_binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/release/beava")
}

/// Build the release binary if missing.
fn ensure_release_binary_built() {
    let bin = release_binary_path();
    if !bin.is_file() {
        let status = Command::new("cargo")
            .args(["build", "--release", "-p", "beava-server", "--bin", "beava"])
            .status()
            .expect("cargo build");
        assert!(status.success(), "release build failed");
    }
}

/// Allocate a free TCP port by binding ephemeral, recording, and dropping.
fn alloc_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

struct SpawnedServer {
    proc: Child,
    http_port: u16,
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
    }
}

/// Write a YAML config matching read_bench.py's shape and spawn the
/// release binary against it. Polls /health on the data-plane HTTP port for
/// up to 10s before returning.
async fn spawn_with_minimal_config() -> SpawnedServer {
    ensure_release_binary_built();

    let http_port = alloc_free_port();
    let tcp_port = alloc_free_port();
    let admin_port = alloc_free_port();

    let dir = tempfile::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");
    let snap_dir = dir.path().join("snap");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&snap_dir).unwrap();

    // admin_addr set explicitly to avoid the default 127.0.0.1:8090 colliding
    // when multiple test instances spawn concurrently.
    let cfg_text = format!(
        r#"listen_addr: "127.0.0.1:{http_port}"
log_level: warn
admin_addr: "127.0.0.1:{admin_port}"
tcp:
  enabled: true
  host: "127.0.0.1"
  port: {tcp_port}
durability:
  wal_dir: "{wal_dir}"
  snapshot_dir: "{snap_dir}"
"#,
        http_port = http_port,
        tcp_port = tcp_port,
        admin_port = admin_port,
        wal_dir = wal_dir.display(),
        snap_dir = snap_dir.display(),
    );
    let cfg_path = dir.path().join("beava.yaml");
    std::fs::write(&cfg_path, cfg_text).unwrap();
    // Keep the tempdir alive for the lifetime of the spawned process.
    std::mem::forget(dir);

    let bin = release_binary_path();
    let mut proc = Command::new(&bin)
        .arg("--config")
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn beava");

    // Poll /health until 200 (10s budget).
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{}/health", http_port);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().as_u16() == 200 {
                return SpawnedServer { proc, http_port };
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Kill+wait the spawned process to avoid leaving a zombie if /health never came up.
    let _ = proc.kill();
    let _ = proc.wait();
    panic!("release binary /health never returned 200 within 10s");
}

async fn register_pipeline(http_port: u16) {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "nodes": [
            {
                "kind": "event",
                "name": "Txn",
                "schema": {
                    "fields": {
                        "event_time": "i64",
                        "user_id": "str",
                        "amount": "f64"
                    },
                    "optional_fields": []
                },
            },
            {
                "kind": "derivation",
                "name": "TxnAgg",
                "output_kind": "table",
                "upstreams": ["Txn"],
                "ops": [{"op": "group_by", "keys": ["user_id"], "agg": {
                    "cnt": {"op": "count", "params": {}}
                }}],
                "schema": {
                    "fields": {"user_id": "str", "cnt": "i64"},
                    "optional_fields": []
                },
                "table_primary_key": ["user_id"]
            }
        ]
    });
    let resp = client
        .post(format!("http://127.0.0.1:{}/register", http_port))
        .json(&payload)
        .send()
        .await
        .expect("register");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "register failed: {} {}", status, body);
}

async fn push_event_for_alice(http_port: u16) {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "event_time": 1000,
        "user_id": "alice",
        "amount": 42.0
    });
    let resp = client
        .post(format!("http://127.0.0.1:{}/push/Txn", http_port))
        .json(&body)
        .send()
        .await
        .expect("push");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "push failed: {} {}", status, body);
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Sanity gate — release binary boots and /health returns 200. Already passes
/// today, but kept for diagnostic clarity if main.rs migration regresses
/// startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_release_binary_exists_and_runs_with_minimal_config() {
    {
        let _g = RELEASE_BINARY_SERIALIZER
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop guard before awaits — serialises test start only
    let _server = spawn_with_minimal_config().await;
    // Spawn helper already polled /health; reaching here means PASS.
}

/// RED today (legacy `Server::bind` gates `feature_query_router` behind
/// `BEAVA_DEV_ENDPOINTS=1`). GREEN once Task 6.b flips main.rs to ServerV18,
/// because the mio HTTP listener routes `/get/:feature/:key` unconditionally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_release_binary_responds_to_get_without_dev_endpoints_env() {
    {
        let _g = RELEASE_BINARY_SERIALIZER
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop guard before awaits — serialises test start only
    let server = spawn_with_minimal_config().await;
    register_pipeline(server.http_port).await;
    push_event_for_alice(server.http_port).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "http://127.0.0.1:{}/get/cnt/alice",
            server.http_port
        ))
        .send()
        .await
        .expect("get cnt/alice");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status.as_u16(),
        200,
        "expected 200 from GET /get/cnt/alice without BEAVA_DEV_ENDPOINTS; got {} body={}",
        status,
        body_text
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body parses as JSON");
    assert_eq!(body["value"], 1, "expected value=1, got {body:#}");
}

/// RED today for the same reason as the GET test above. POST /get is the
/// batch endpoint that feeds `read_bench.py`'s primary measurement workload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_release_binary_responds_to_post_get_without_dev_endpoints_env() {
    {
        let _g = RELEASE_BINARY_SERIALIZER
            .lock()
            .unwrap_or_else(|e| e.into_inner());
    } // drop guard before awaits — serialises test start only
    let server = spawn_with_minimal_config().await;
    register_pipeline(server.http_port).await;
    push_event_for_alice(server.http_port).await;

    let client = reqwest::Client::new();
    let body = serde_json::json!({"keys": ["alice"], "features": ["cnt"]});
    let resp = client
        .post(format!("http://127.0.0.1:{}/get", server.http_port))
        .json(&body)
        .send()
        .await
        .expect("post /get");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status.as_u16(),
        200,
        "expected 200 from POST /get without BEAVA_DEV_ENDPOINTS; got {} body={}",
        status,
        body_text
    );
    let body: serde_json::Value = serde_json::from_str(&body_text).expect("body parses as JSON");
    assert_eq!(
        body["result"]["alice"]["cnt"], 1,
        "expected result.alice.cnt=1, got {body:#}"
    );
}
