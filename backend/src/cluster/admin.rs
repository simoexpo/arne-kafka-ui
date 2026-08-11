use super::{ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
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

#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Serialize)]
pub struct ConfigEntryOut {
    pub name: String,
    pub value: Option<String>,
    pub is_default: bool,
}

#[derive(Debug, Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub configs: Vec<ConfigEntryOut>,
    pub as_of: i64,
}

pub async fn topic_detail(handle: Arc<ClusterHandle>, topic: String) -> Result<TopicDetail, ApiError> {
    // configs via AdminClient (async), partitions/offsets via blocking consumer
    let cfg_handle = handle.clone();
    let cfg_topic = topic.clone();
    let configs_fut = async move {
        let res = cfg_handle.admin()
            .describe_configs(&[ResourceSpecifier::Topic(&cfg_topic)], &AdminOptions::new())
            .await
            .map_err(|e| error::from_kafka(&cfg_handle.name, "describe configs", &e))?;
        let mut out = Vec::new();
        for r in res {
            let resource = r.map_err(|e| ApiError::kafka(&cfg_handle.name, format!("describe configs: {e}")))?;
            for entry in resource.entries {
                out.push(ConfigEntryOut { name: entry.name, value: entry.value, is_default: entry.is_default });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok::<_, ApiError>(out)
    };

    let part_handle = handle.clone();
    let part_topic = topic.clone();
    let partitions_fut = tokio::task::spawn_blocking(move || {
        let md = part_handle.consumer()
            .fetch_metadata(Some(&part_topic), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&part_handle.name, "fetch metadata", &e))?;
        let t = md.topics().iter()
            .find(|t| t.name() == part_topic && !t.partitions().is_empty())
            .ok_or_else(|| ApiError::topic_not_found(&part_handle.name, &part_topic))?;
        let mut partitions = Vec::new();
        for p in t.partitions() {
            let (lo, hi) = part_handle.consumer()
                .fetch_watermarks(t.name(), p.id(), ADMIN_TIMEOUT)
                .map_err(|e| error::from_kafka(&part_handle.name, "fetch watermarks", &e))?;
            partitions.push(PartitionInfo {
                id: p.id(), leader: p.leader(),
                replicas: p.replicas().to_vec(), isr: p.isr().to_vec(),
                start_offset: lo, end_offset: hi,
            });
        }
        Ok::<_, ApiError>(partitions)
    });

    let (configs, partitions) = tokio::join!(configs_fut, partitions_fut);
    // partition errors (incl. topic_not_found) take precedence over config errors
    let partitions = partitions.map_err(|e| ApiError::internal(format!("task join: {e}")))??;
    let configs = configs?;
    Ok(TopicDetail { name: topic, partitions, configs, as_of: now_ms() })
}
