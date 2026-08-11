use crate::cluster::registry::ClusterRegistry;
use crate::config::Limits;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<ClusterRegistry>,
    pub limits: Arc<Limits>,
}
