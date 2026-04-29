//! Phase 6 Plan 04 crash probe binary.
//!
//! Spawned by `crates/beava-server/tests/phase6_crash.rs` as a subprocess.
//! Reads BEAVA_WAL_DIR + BEAVA_WAL_FSYNC_INTERVAL_MS from env, starts a
//! minimal beava server on an ephemeral port with a single `Test` event
//! registered, prints `PORT=<n>` to stdout, then blocks until SIGKILL.
//!
//! Flow:
//!   1. Parse env vars (BEAVA_WAL_DIR required, BEAVA_WAL_FSYNC_INTERVAL_MS
//!      optional, defaults to 2).
//!   2. Build a `Config` pointing at the tempdir.
//!   3. Spawn the server via `Server::bind`.
//!   4. Register a tiny `Test` event in the shared registry: fields
//!      {event_time: i64, user_id: str, amount: f64}. No dedupe_key.
//!   5. Print `PORT=<n>` + flush stdout.
//!   6. Serve until SIGTERM / SIGKILL.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use beava_core::registry_diff::PayloadNode;
use beava_core::schema::{EventSchema, FieldType};
use beava_server::config::Config;
use beava_server::Server;

fn main() {
    let wal_dir = std::env::var("BEAVA_WAL_DIR").expect("BEAVA_WAL_DIR required");
    let fsync_ms: u64 = std::env::var("BEAVA_WAL_FSYNC_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt");

    rt.block_on(async move {
        let cfg = Config {
            listen_addr: "127.0.0.1:0".to_string(),
            log_level: "warn".to_string(),
            tcp: beava_core::config::TcpConfig {
                enabled: false,
                ..Default::default()
            },
            durability: beava_core::config::DurabilityConfig {
                wal_dir: PathBuf::from(&wal_dir),
                wal_fsync_interval_ms: fsync_ms,
                ..Default::default()
            },
            admin_addr: "127.0.0.1:0".to_string(),
        };

        let server = Server::bind(&cfg, false).await.expect("bind");
        let addr = server.local_addr();

        // Register a minimal Test event so /push/Test is accepted.
        let registry: Arc<beava_core::registry::Registry> = server.registry();
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("user_id".to_string(), FieldType::Str);
        fields.insert("amount".to_string(), FieldType::F64);
        let event = beava_core::registry::EventDescriptor {
            name: "Test".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""),
            apply_field_names: vec![],
        };
        registry.apply_registration(vec![PayloadNode::Event(event)], vec![], vec![], vec![]);

        // Announce the bound port so the parent test can issue the push.
        println!("PORT={}", addr.port());
        let _ = std::io::stdout().flush();

        // Serve until SIGKILL (the test process kills us).
        let shutdown = async {
            // This future never resolves — the probe exits only via SIGKILL.
            std::future::pending::<()>().await;
        };
        let _ = server.serve(shutdown).await;
    });
}
