//! Command-line interface for the `beava` binary.

use clap::Parser;
use std::path::PathBuf;

/// Beava v2 — single-binary real-time feature server.
#[derive(Debug, Parser)]
#[command(
    name = "beava",
    version,
    about = "Beava v2 — real-time feature server for fraud, ad-tech, and behavioral analytics",
    long_about = None,
)]
pub struct Cli {
    /// Path to YAML config file. Defaults to ./beava.yaml if omitted.
    #[arg(short, long, default_value = "./beava.yaml")]
    pub config: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn help_contains_config_flag() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        assert!(
            help.contains("--config"),
            "help must document --config: {help}"
        );
    }

    #[test]
    fn default_config_path_is_local_beava_yaml() {
        let cli = Cli::try_parse_from(["beava"]).expect("parses with no args");
        assert_eq!(cli.config, PathBuf::from("./beava.yaml"));
    }

    #[test]
    fn explicit_config_path_is_captured() {
        let cli = Cli::try_parse_from(["beava", "--config", "/tmp/custom.yaml"])
            .expect("parses with explicit config");
        assert_eq!(cli.config, PathBuf::from("/tmp/custom.yaml"));
    }
}
