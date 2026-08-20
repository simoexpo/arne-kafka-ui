use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;
use futures_util::future::join_all;
use serde_json::{json, Value};

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
    Json(json!({ "clusters": clusters }))
}

pub async fn overview(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<std::sync::Arc<admin::Overview>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::overview(handle).await?))
}
