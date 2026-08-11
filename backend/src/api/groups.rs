use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::Json;

pub async fn list(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<admin::GroupList>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::list_groups(handle).await?))
}

pub async fn detail(
    State(state): State<AppState>,
    Path((cluster, group)): Path<(String, String)>,
) -> Result<Json<admin::GroupDetail>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::group_detail(handle, group).await?))
}
