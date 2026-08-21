use crate::cluster::admin;
use crate::error::ApiError;
use crate::state::AppState;
use axum::Json;
use axum::extract::{Path, Query, State};

pub async fn list(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
) -> Result<Json<std::sync::Arc<admin::GroupList>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::list_groups(handle).await?))
}

/// Lag for the groups named in `?groups=a,b,c` — the rows a paginated
/// consumers list is actually showing.
pub async fn lag(
    State(state): State<AppState>,
    Path(cluster): Path<String>,
    Query(params): Query<LagQuery>,
) -> Result<Json<admin::GroupLagBatch>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    let groups: Vec<String> = params
        .groups
        .split(',')
        .map(str::trim)
        .filter(|g| !g.is_empty())
        .map(str::to_string)
        .collect();
    Ok(Json(admin::groups_lag(handle, groups).await?))
}

#[derive(serde::Deserialize)]
pub struct LagQuery {
    #[serde(default)]
    pub groups: String,
}

pub async fn detail(
    State(state): State<AppState>,
    Path((cluster, group)): Path<(String, String)>,
) -> Result<Json<std::sync::Arc<admin::GroupDetail>>, ApiError> {
    let handle = state.registry.get(&cluster)?;
    Ok(Json(admin::group_detail(handle, group).await?))
}
