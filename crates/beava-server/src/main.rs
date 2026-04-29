//! Beava v2 server entry point.
//!
//! Wiring order (Plan 12-07 — production binary now boots `ServerV18`,
//! the mio data-plane runtime; legacy `Server` is retained only for
//! `phase6_crash_probe` + `TestServer` per memory `project_phase18_no_dual_runtime`):
//!
//! 1. Parse CLI
//! 2. Load config (YAML + `BEAVA_*` env overrides + validation)
//! 3. Init JSON logging (so steps 4+ log structured)
//! 4. Build single-thread tokio runtime (admin endpoints + serve_with_dirs
//!    orchestration; the apply thread is a separate std::thread spawned
//!    inside ServerV18::serve_with_dirs and never touches tokio).
//! 5. Bind ServerV18 (data-plane HTTP + TCP listeners + admin axum sidecar)
//! 6. serve_with_dirs with SIGTERM/SIGINT graceful shutdown
//!
//! Note: `BEAVA_DEV_ENDPOINTS=1` is a Phase 1-era flag for the legacy
//! `Server` axum path. Production binary post-Plan-12-07 uses `ServerV18`
//! (mio); admin endpoints are always mounted via `BoundAdminServer` on
//! `cfg.admin_addr` (default `127.0.0.1:8090`). The legacy gate is
//! preserved in `Server::bind` for the `phase6_crash_probe` binary and
//! `TestServer` (`crates/beava-server/src/testing.rs:76`).

use anyhow::{Context, Result};
use beava_server::{banner, cli::Cli, logging, shutdown::shutdown_signal, ServerV18};
use clap::Parser;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let cfg = beava_server::config::load_config(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;

    logging::init(&cfg.log_level).context("init logging")?;

    tracing::info!(
        target: "beava.server",
        version = beava_server::VERSION,
        config_path = %cli.config.display(),
        "beava starting"
    );
    // Print banner to stdout as well — useful when logs are JSON but operators want
    // a quick "yep it started" line.
    println!("{}", banner());

    // Single-thread tokio runtime: admin endpoints + serve_with_dirs orchestration
    // run on this thread. The hand-rolled apply loop runs in a dedicated std::thread
    // spawned inside ServerV18::serve_with_dirs (no tokio dependency on the data
    // plane).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async move {
        let http_addr: std::net::SocketAddr = cfg
            .listen_addr
            .parse()
            .with_context(|| format!("parse listen_addr {:?}", cfg.listen_addr))?;
        let tcp_addr: std::net::SocketAddr = format!("{}:{}", cfg.tcp.host, cfg.tcp.port)
            .parse()
            .with_context(|| format!("parse tcp addr {}:{}", cfg.tcp.host, cfg.tcp.port))?;
        let admin_addr: std::net::SocketAddr = cfg
            .admin_addr
            .parse()
            .with_context(|| format!("parse admin_addr {:?}", cfg.admin_addr))?;

        tracing::info!(
            target: "beava.server",
            kind = "server.boot.v18",
            http_addr = %http_addr,
            tcp_addr = %tcp_addr,
            admin_addr = %admin_addr,
            "booting ServerV18 (Plan 12-07)"
        );

        let server = ServerV18::bind(http_addr, tcp_addr, admin_addr)
            .await
            .context("bind ServerV18 listeners")?;
        server
            .serve_with_dirs(
                shutdown_signal(),
                cfg.durability.wal_dir.clone(),
                cfg.durability.snapshot_dir.clone(),
            )
            .await
            .context("serve ServerV18")?;
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}
