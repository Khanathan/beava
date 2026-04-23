//! In-process integration-test harness for the beava server.
//!
//! Used by Phase 1's `foundation_smoke.rs` and every subsequent phase's HTTP tests.
//! Spawns a real `Server` on an OS-allocated port, waits for readiness via `/ready`,
//! hands back a `TestServer` whose `.base_url()` can be curled, and shuts down
//! gracefully on `.shutdown().await`.
//!
//! Usage:
//! ```no_run
//! # async fn ex() {
//! use beava_server::testing::TestServer;
//! let ts = TestServer::spawn().await.expect("spawn");
//! let url = format!("{}/health", ts.base_url());
//! // issue requests with reqwest / hyper / etc.
//! ts.shutdown().await.expect("shutdown");
//! # }
//! ```
//!
//! Availability: feature-gated behind `testing`. Consumers in Cargo.toml:
//! ```toml
//! [dev-dependencies]
//! beava-server = { path = "...", features = ["testing"] }
//! ```

#![cfg(any(feature = "testing", test))]

use crate::server::{Server, ServerError};
use crate::Config;
use serde::Serialize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub enum TestServerError {
    #[error(transparent)]
    Server(#[from] ServerError),
    #[error("readiness timed out after {0:?}")]
    ReadinessTimeout(Duration),
    #[error("server task join failed: {0}")]
    Join(String),
}

/// Builder for a TestServer with overrideable config knobs.
pub struct TestServerBuilder {
    cfg: Config,
    readiness_timeout: Duration,
    readiness_poll_interval: Duration,
    dev_endpoints: bool,
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        Self {
            cfg: Config {
                listen_addr: "127.0.0.1:0".to_string(), // OS-allocated
                log_level: "info".to_string(),
            },
            readiness_timeout: Duration::from_secs(5),
            readiness_poll_interval: Duration::from_millis(20),
            dev_endpoints: false,
        }
    }
}

impl TestServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.cfg.listen_addr = addr.into();
        self
    }

    pub fn log_level(mut self, lvl: impl Into<String>) -> Self {
        self.cfg.log_level = lvl.into();
        self
    }

    pub fn readiness_timeout(mut self, t: Duration) -> Self {
        self.readiness_timeout = t;
        self
    }

    /// Enable the GET /registry dev endpoint on the spawned server.
    /// Passes `dev_endpoints=true` directly to `Server::bind` — no env-var
    /// mutation needed, so no lock is held across the await.
    pub fn dev_endpoints(mut self, enabled: bool) -> Self {
        self.dev_endpoints = enabled;
        self
    }

    /// Spawn the server, wait for `/ready` to report 200, return the handle.
    pub async fn spawn(self) -> Result<TestServer, TestServerError> {
        let server = Server::bind(&self.cfg, self.dev_endpoints).await?;
        let base_url = format!("http://{}", server.local_addr());

        let (tx, rx) = oneshot::channel::<()>();
        let shutdown = async move {
            let _ = rx.await;
        };

        let serve_task: JoinHandle<Result<(), ServerError>> =
            tokio::spawn(async move { server.serve(shutdown).await });

        let harness = TestServer {
            base_url,
            shutdown_tx: Some(tx),
            serve_task: Some(serve_task),
        };

        harness
            .wait_ready(self.readiness_timeout, self.readiness_poll_interval)
            .await?;

        Ok(harness)
    }
}

pub struct TestServer {
    base_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    serve_task: Option<JoinHandle<Result<(), ServerError>>>,
}

impl TestServer {
    pub async fn spawn() -> Result<Self, TestServerError> {
        TestServerBuilder::default().spawn().await
    }

    pub fn builder() -> TestServerBuilder {
        TestServerBuilder::default()
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// POST arbitrary JSON body to `path`. Returns the raw reqwest Response so
    /// callers can assert on status and parse the body themselves.
    pub async fn post_json<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url, path);
        reqwest::Client::new()
            .post(&url)
            .header("Content-Type", "application/json")
            .json(body)
            .timeout(Duration::from_secs(5))
            .send()
            .await
    }

    /// GET `path`; parse response body as JSON. Panics if non-2xx OR non-JSON.
    /// For error-path tests use `get_raw` instead.
    pub async fn get_json(&self, path: &str) -> serde_json::Value {
        let url = format!("{}{}", self.base_url, path);
        let resp = reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("GET request");
        assert!(
            resp.status().is_success(),
            "GET {} returned {}",
            path,
            resp.status()
        );
        resp.json().await.expect("JSON body")
    }

    /// Raw GET that does NOT assert on status. Use for 404 / 503 / error-path tests.
    pub async fn get_raw(&self, path: &str) -> reqwest::Response {
        let url = format!("{}{}", self.base_url, path);
        reqwest::Client::new()
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("GET request")
    }

    async fn wait_ready(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<(), TestServerError> {
        let url = format!("{}/ready", self.base_url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .expect("build reqwest client");

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(TestServerError::ReadinessTimeout(timeout));
            }
            match client.get(&url).send().await {
                Ok(r) if r.status().as_u16() == 200 => return Ok(()),
                _ => tokio::time::sleep(poll_interval).await,
            }
        }
    }

    /// Trigger graceful shutdown and await the serve task. Idempotent.
    pub async fn shutdown(mut self) -> Result<(), TestServerError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.serve_task.take() {
            let serve_result = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .map_err(|_| {
                    TestServerError::Join("serve task did not exit within 2s".to_string())
                })?;
            match serve_result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(TestServerError::Server(e)),
                Err(join_err) => Err(TestServerError::Join(join_err.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Fire-and-forget shutdown on drop. The serve task will observe the channel
        // closed (if not yet signalled) — axum's graceful shutdown future awakens when
        // the channel's sender is dropped and the receiver errors.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // JoinHandle drop detaches the task; it will still exit cleanly because
        // shutdown was signalled above.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_serves_health() {
        let ts = TestServer::spawn().await.expect("spawn");
        let url = format!("{}/health", ts.base_url());
        let resp = reqwest::get(&url).await.expect("health req");
        assert_eq!(resp.status().as_u16(), 200);
        let json: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(json, serde_json::json!({ "status": "ok" }));
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn two_test_servers_use_different_ports() {
        let a = TestServer::spawn().await.expect("spawn a");
        let b = TestServer::spawn().await.expect("spawn b");
        assert_ne!(a.base_url(), b.base_url(), "expected distinct ports");
        a.shutdown().await.ok();
        b.shutdown().await.ok();
    }

    #[tokio::test]
    async fn shutdown_exits_within_budget() {
        let ts = TestServer::spawn().await.expect("spawn");
        let start = std::time::Instant::now();
        ts.shutdown().await.expect("shutdown");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "shutdown took {elapsed:?}, expected <500ms"
        );
    }

    #[tokio::test]
    async fn readiness_wait_respects_timeout() {
        let ts = TestServer::builder()
            .readiness_timeout(Duration::from_secs(1))
            .spawn()
            .await
            .expect("spawn");
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn drop_without_explicit_shutdown_does_not_hang() {
        let base_url = {
            let ts = TestServer::spawn().await.expect("spawn");
            ts.base_url().to_string()
            // ts drops here without explicit shutdown
        };
        // Give the background task a beat to see the dropped tx and exit.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = base_url; // keep clippy happy
    }

    #[tokio::test]
    async fn post_json_returns_response() {
        let ts = TestServer::spawn().await.expect("spawn");
        let body = serde_json::json!({"nodes": []});
        let resp = ts.post_json("/register", &body).await.expect("post_json");
        assert_eq!(resp.status().as_u16(), 200);
        let val: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(val["status"], "ok");
        assert_eq!(val["registry_version"], 0);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn post_json_404_on_unknown_path() {
        let ts = TestServer::spawn().await.expect("spawn");
        let body = serde_json::json!({});
        let resp = ts.post_json("/nope", &body).await.expect("post_json");
        assert_eq!(resp.status().as_u16(), 404);
        ts.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn get_raw_returns_non_success_without_panicking() {
        let ts = TestServer::spawn().await.expect("spawn");
        let resp = ts.get_raw("/nope").await;
        assert_eq!(
            resp.status().as_u16(),
            404,
            "get_raw should not panic on 404"
        );
        ts.shutdown().await.expect("shutdown");
    }
}
