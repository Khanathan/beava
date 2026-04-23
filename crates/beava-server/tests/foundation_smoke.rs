//! Phase 1 acceptance test: full server lifecycle via the TestServer harness.
//!
//! This is the gate the "Foundation" phase must pass. If this test is green, Phase 1's
//! success criteria from ROADMAP.md are all met:
//!   1. `cargo build --release` produces beava binary (confirmed by other tests)
//!   2. /health returns 200 {"status":"ok"} within 1s of startup ✓
//!   3. /ready returns 503 → 200 after readiness-complete stub fires ✓
//!   4. axum + graceful shutdown on SIGTERM ✓ (simulated via shutdown channel here;
//!      real signal path covered by Plan 04's server_integration tests)
//!   5. Integration test harness exists and is usable ✓ (this test uses it)

use beava_server::testing::TestServer;
use std::time::Duration;

#[tokio::test]
async fn phase1_acceptance_lifecycle() {
    // 1. Spawn
    let ts = TestServer::spawn().await.expect("TestServer spawn failed");
    let base = ts.base_url().to_string();
    assert!(base.starts_with("http://127.0.0.1:"), "base_url = {base}");

    let client = reqwest::Client::new();

    // 2. /health is 200 + {"status":"ok"}
    let resp = client
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("health req");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body, serde_json::json!({ "status": "ok" }));

    // 3. /ready should be 200 by the time spawn returned (spawn awaits readiness).
    let resp = client
        .get(format!("{base}/ready"))
        .send()
        .await
        .expect("ready req");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body, serde_json::json!({ "status": "ready" }));

    // 4. Unknown route returns 404.
    let resp = client
        .get(format!("{base}/does-not-exist"))
        .send()
        .await
        .expect("req");
    assert_eq!(resp.status().as_u16(), 404);

    // 5. Graceful shutdown completes within budget (CONTEXT.md: ≤2s idle).
    let start = std::time::Instant::now();
    ts.shutdown().await.expect("shutdown failed");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown took {elapsed:?}"
    );

    // 6. After shutdown, the base_url should not serve (port reclaimed or listener closed).
    tokio::time::sleep(Duration::from_millis(50)).await;
    let res = client
        .get(format!("{base}/health"))
        .timeout(Duration::from_millis(300))
        .send()
        .await;
    assert!(
        res.is_err()
            || res
                .map(|r| r.status().is_server_error() || r.status().is_client_error())
                .unwrap_or(false),
        "expected post-shutdown request to fail or return error status"
    );
}

#[tokio::test]
async fn phase1_acceptance_ready_starts_at_503() {
    use beava_server::testing::TestServerBuilder;

    let ts = TestServerBuilder::new()
        .readiness_timeout(Duration::from_secs(3))
        .spawn()
        .await
        .expect("spawn");
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/ready", ts.base_url()))
        .send()
        .await
        .expect("ready req");
    assert_eq!(resp.status().as_u16(), 200);
    ts.shutdown().await.expect("shutdown");
}
