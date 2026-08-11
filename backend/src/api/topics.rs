use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;

pub async fn list(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<admin::TopicList>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::list_topics(handle).await?))
}
