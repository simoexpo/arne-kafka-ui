use arne::cluster::registry::ClusterRegistry;
use arne::config::Config;
use arne::state::AppState;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,rdkafka=warn".into()),
        )
        .init();

    let path = config_path(std::env::args().nth(1), std::env::var("ARNE_CONFIG").ok());
    let config = Config::load(&path)?; // fail fast, precise message

    // Nothing samples in the background any more (owner design 2026-08-19):
    // `GET /throughput` samples the topic it was asked about, so an idle
    // cluster is never touched. See cluster::sampler.
    let registry = Arc::new(ClusterRegistry::from_config(config.clusters.clone())?);
    let state = AppState {
        registry,
        limits: Arc::new(config.limits.clone()),
    };

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.server.port)).await?;
    tracing::info!("arne listening on port {}", config.server.port);
    axum::serve(listener, arne::api::app(state)).await?;
    Ok(())
}

/// First CLI argument, then `ARNE_CONFIG`, then the conventional filename.
fn config_path(arg: Option<String>, env: Option<String>) -> PathBuf {
    arg.or(env).unwrap_or_else(|| "config.yaml".into()).into()
}

#[cfg(test)]
mod tests {
    use super::config_path;

    #[test]
    fn the_cli_argument_wins_then_the_env_var_then_the_default() {
        assert_eq!(
            config_path(Some("a.yaml".into()), Some("b.yaml".into())),
            std::path::PathBuf::from("a.yaml")
        );
        assert_eq!(
            config_path(None, Some("b.yaml".into())),
            std::path::PathBuf::from("b.yaml")
        );
        assert_eq!(
            config_path(None, None),
            std::path::PathBuf::from("config.yaml")
        );
    }
}
