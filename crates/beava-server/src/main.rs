//! Beava v2 server entry point.
//!
//! Plan 12.6-07: legacy axum `Server` deleted. ServerV18 (mio) is the SOLE
//! data-plane runtime per `project_phase18_no_dual_runtime`. Admin endpoints
//! mount via `BoundAdminServer` on `cfg.admin_addr` (default 127.0.0.1:8090).
//!
//! Wiring order:
//! 1. Parse CLI
//! 2. Load config (YAML + `BEAVA_*` env overrides + validation)
//! 3. Init JSON logging (so steps 4+ log structured)
//! 4. Build single-thread tokio runtime (admin endpoints + serve_with_dirs
//!    orchestration; the apply thread is a separate std::thread spawned
//!    inside ServerV18::serve_with_dirs and never touches tokio).
//! 5. Bind ServerV18 (data-plane HTTP + TCP listeners + admin axum sidecar)
//! 6. serve_with_dirs with SIGTERM/SIGINT graceful shutdown

use anyhow::{Context, Result};
use beava_persistence::{Persistence, SyncMode};
use beava_server::server::ServerV18Config;
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
        let tcp_addr: std::net::SocketAddr =
            format!("{}:{}", cfg.tcp.host, cfg.tcp.port)
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

        // Phase 13.5.3: production binary now constructs ServerV18Config
        // via `from_env()` (which reads BEAVA_* env vars at boot) and calls
        // `bind_with_config(...)`, replacing the legacy `bind() +
        // serve_with_dirs(...)` path. The old path read BEAVA_TEST_MODE /
        // BEAVA_MEMORY_GOV_ENFORCE / BEAVA_IO_THREADS / BEAVA_WAL_*
        // process-globally on the hot path; the new path consolidates
        // env-reading into a single boot-time site (preserving the operator
        // env interface) and plumbs the resolved values through struct
        // fields. See .planning/quick/260505-bn7-workspace-test-determinism-phase-13-5-3/.
        let mut sv18_cfg = ServerV18Config::from_env();
        sv18_cfg.persistence = Persistence::Disk {
            wal_dir: cfg.durability.wal_dir.clone(),
            snapshot_dir: cfg.durability.snapshot_dir.clone(),
            sync_mode: SyncMode::Periodic,
        };
        sv18_cfg.tcp_max_frame_bytes = cfg.tcp.max_frame_bytes;
        let server = ServerV18::bind_with_config(http_addr, Some(tcp_addr), admin_addr, sv18_cfg)
            .await
            .context("bind ServerV18 listeners")?;
        server
            .serve(shutdown_signal())
            .await
            .context("serve ServerV18")?;
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}
