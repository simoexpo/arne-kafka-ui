use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::extract::{Path, State};
use futures_util::future::join_all;
use serde_json::{Value, json};

pub async fn list(State(state): State<AppState>) -> Json<Value> {
    let handles = state.registry.all();
    let healths = join_all(handles.iter().map(|h| h.health())).await;
    let clusters: Vec<Value> = handles
        .iter()
        .zip(healths)
        .map(|(h, health)| {
            json!({
                "name": h.name,
                "status": health.status,
                "broker_count": health.broker_count,
                "error": health.error,
            })
        })
        .collect();
    // The build's identity is verbatim `git describe --tags --always` output,
    // baked in when the image was built (compile-time env, set from a Docker
    // build arg). It rides this response because the sidebar already polls it
    // — no extra request. Absent (null) when the build was given none: a bare
    // `cargo run` states nothing rather than inventing a version. option_env!
    // is compile-time, so the positive path is verified against the built
    // image, not in unit tests.
    Json(json!({ "clusters": clusters, "version": option_env!("ARNE_BUILD_VERSION") }))
}

/// What Arne has asked this cluster's brokers, as librdkafka counted it.
/// Diagnostic: empty until `broker_call_stats_ms` is configured for the
/// cluster, and it never itself talks to a broker.
pub async fn broker_calls(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<crate::cluster::call_stats::CallReport>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    handle.serve_client_events();
    Ok(Json(handle.call_stats.report()))
}

pub async fn overview(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<std::sync::Arc<admin::Overview>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::overview(handle).await?))
}
