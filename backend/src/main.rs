use betrachtung::{api, cluster::registry::ClusterRegistry, config::Config, state::AppState};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(std::path::Path::new("config.yaml"))?;
    let state = AppState {
        registry: Arc::new(ClusterRegistry::from_config(config.clusters.clone())?),
        limits: Arc::new(config.limits.clone()),
    };
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.server.port)).await?;
    axum::serve(listener, api::app(state)).await?;
    Ok(())
}
