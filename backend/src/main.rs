use betrachtung::cluster::registry::ClusterRegistry;
use betrachtung::cluster::sampler::spawn_sampler;
use betrachtung::config::Config;
use betrachtung::state::AppState;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
    let config = Config::load(&path).map_err(|e| anyhow::anyhow!("{e}"))?; // fail fast, precise message

    let registry = Arc::new(ClusterRegistry::from_config(config.clusters.clone())?);
    for handle in registry.all() {
        let name = handle.name.clone();
        spawn_sampler(handle, Duration::from_secs(config.limits.sampler_interval_secs));
        tracing::info!(cluster = %name, "sampler started");
    }
    let state = AppState { registry, limits: Arc::new(config.limits.clone()) };

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.server.port)).await?;
    tracing::info!("betrachtung listening on port {}", config.server.port);
    axum::serve(listener, betrachtung::api::app(state)).await?;
    Ok(())
}
