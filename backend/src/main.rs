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

    let path: PathBuf = std::env::args().nth(1)
        .or_else(|| std::env::var("BETRACHTUNG_CONFIG").ok())
        .unwrap_or_else(|| "config.yaml".into())
        .into();
    let config = Config::load(&path)?; // fail fast, precise message

    // Nothing samples in the background any more (owner design 2026-08-19):
    // `GET /throughput` samples the topic it was asked about, so an idle
    // cluster is never touched. See cluster::sampler.
    let registry = Arc::new(ClusterRegistry::from_config(config.clusters.clone())?);
    let state = AppState { registry, limits: Arc::new(config.limits.clone()) };

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.server.port)).await?;
    tracing::info!("arne listening on port {}", config.server.port);
    axum::serve(listener, arne::api::app(state)).await?;
    Ok(())
}
