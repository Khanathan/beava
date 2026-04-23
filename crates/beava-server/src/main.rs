//! Beava v2 server entry point.
//!
//! Wiring order:
//! 1. Parse CLI
//! 2. Load config
//! 3. Init JSON logging (so steps 4+ log structured)
//! 4. Build single-thread tokio runtime (per locked architecture decision)
//! 5. Bind the HTTP server
//! 6. Serve with SIGTERM/SIGINT graceful shutdown

use anyhow::{Context, Result};
use beava_server::{banner, cli::Cli, logging, server::Server, shutdown::shutdown_signal};
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

    // Single-thread runtime: one OS thread drives the HTTP accept + handlers.
    // In Phase 3 the apply loop takes over a dedicated thread; the HTTP runtime
    // will likely move to its own thread pool then. For Phase 1, a current_thread
    // runtime is the simplest thing that matches the locked single-thread mental model.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async move {
        let server = Server::bind(&cfg).await.context("bind HTTP listener")?;
        server
            .serve(shutdown_signal())
            .await
            .context("serve HTTP")?;
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}
