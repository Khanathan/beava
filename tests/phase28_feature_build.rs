//! Phase 28-01: verify that engine + state modules are reachable
//! under the default (server) feature. The mirror check under
//! `--no-default-features --features client` is enforced by the
//! cargo build verification step (scripts/check-feature-builds.sh),
//! not by this test binary (which itself only compiles under default
//! features).

#[test]
fn engine_and_state_are_shared_across_features() {
    // These types must exist regardless of feature flag.
    // If a future change accidentally puts them behind
    // #[cfg(feature = "server")], this test stops compiling.
    use tally::engine::pipeline::PipelineEngine;
    use tally::state::store::StateStore;

    let _ = std::any::type_name::<PipelineEngine>();
    let _ = std::any::type_name::<StateStore>();
}

#[test]
#[cfg(feature = "server")]
fn server_module_present_under_server_feature() {
    let _ = std::any::type_name::<tally::server::tcp::SharedState>();
}
