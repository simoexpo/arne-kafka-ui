use super::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub message_estimate: i64,
    pub size_bytes: Option<u64>,
    pub internal: bool,
}

#[derive(Debug, Serialize)]
pub struct TopicList {
    pub topics: Vec<TopicSummary>,
    pub as_of: i64,
}

pub async fn list_topics(handle: Arc<ClusterHandle>) -> Result<TopicList, ApiError> {
    tokio::task::spawn_blocking(move || {
        let md = handle.consumer()
            .fetch_metadata(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
        let mut topics = Vec::new();
        for t in md.topics() {
            let mut estimate = 0i64;
            for p in t.partitions() {
                let (lo, hi) = handle.consumer()
                    .fetch_watermarks(t.name(), p.id(), ADMIN_TIMEOUT)
                    .map_err(|e| error::from_kafka(&handle.name, "fetch watermarks", &e))?;
                estimate += hi - lo;
            }
            topics.push(TopicSummary {
                name: t.name().to_string(),
                partitions: t.partitions().len() as i32,
                replication_factor: t.partitions().first().map(|p| p.replicas().len()).unwrap_or(0) as i32,
                message_estimate: estimate,
                size_bytes: None, // librdkafka has no DescribeLogDirs; stable API shape, filled when possible
                internal: t.name().starts_with("__"),
            });
        }
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(TopicList { topics, as_of: now_ms() })
    })
    .await
    .map_err(|e| ApiError::internal(format!("task join: {e}")))?
}
