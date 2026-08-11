use super::{group_consumer, ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
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

#[derive(Debug, Serialize, Clone)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
}

#[derive(Debug, Serialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub member_count: usize,
    pub total_lag: i64,
}

#[derive(Debug, Serialize)]
pub struct GroupList { pub groups: Vec<GroupSummary>, pub as_of: i64 }

#[derive(Debug, Serialize)]
pub struct MemberInfo { pub member_id: String, pub client_id: String, pub client_host: String }

#[derive(Debug, Serialize)]
pub struct GroupDetail {
    pub group_id: String,
    pub state: String,
    pub members: Vec<MemberInfo>,
    pub partitions: Vec<PartitionLag>,
    pub as_of: i64,
}

/// Committed offsets fetched WITHOUT joining the group (OffsetFetch only):
/// a throwaway consumer configured with the group.id never subscribes,
/// so it cannot trigger a rebalance of the real group.
pub fn group_lag_blocking(
    handle: &ClusterHandle,
    group: &str,
    topic_filter: Option<&str>,
) -> Result<Vec<PartitionLag>, ApiError> {
    let md = handle.consumer()
        .fetch_metadata(topic_filter, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
    let mut tpl = TopicPartitionList::new();
    for t in md.topics() {
        if let Some(f) = topic_filter { if t.name() != f { continue; } }
        for p in t.partitions() {
            tpl.add_partition(t.name(), p.id());
        }
    }
    let gc = group_consumer(&handle.config, group)
        .map_err(|e| error::from_kafka(&handle.name, "create group consumer", &e))?;
    // NotCoordinator/CoordinatorNotAvailable/CoordinatorLoadInProgress are
    // transient by protocol contract: the coordinator is moving or still
    // loading. Retry briefly instead of surfacing a 502 for a healthy cluster.
    let committed = {
        use rdkafka::error::RDKafkaErrorCode::*;
        let mut attempt = 0u32;
        loop {
            match gc.committed_offsets(tpl.clone(), ADMIN_TIMEOUT) {
                Ok(c) => break c,
                Err(e) if attempt < 4 && matches!(
                    e.rdkafka_error_code(),
                    Some(NotCoordinator | CoordinatorNotAvailable | CoordinatorLoadInProgress)
                ) => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(100 * u64::from(attempt)));
                }
                Err(e) => return Err(error::from_kafka(&handle.name, "fetch committed offsets", &e)),
            }
        }
    };
    let mut out = Vec::new();
    for e in committed.elements() {
        if let Offset::Offset(c) = e.offset() {
            let (_, hi) = handle.consumer()
                .fetch_watermarks(e.topic(), e.partition(), ADMIN_TIMEOUT)
                .map_err(|err| error::from_kafka(&handle.name, "fetch watermarks", &err))?;
            out.push(PartitionLag {
                topic: e.topic().to_string(), partition: e.partition(),
                committed_offset: c, end_offset: hi, lag: (hi - c).max(0),
            });
        }
    }
    out.sort_by(|a, b| (a.topic.clone(), a.partition).cmp(&(b.topic.clone(), b.partition)));
    Ok(out)
}

pub async fn list_groups(handle: Arc<ClusterHandle>) -> Result<GroupList, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "list groups", &e))?;
        let mut groups = Vec::new();
        for g in gl.groups() {
            let lag = group_lag_blocking(&handle, g.name(), None)?;
            groups.push(GroupSummary {
                group_id: g.name().to_string(),
                state: g.state().to_string(),
                protocol_type: g.protocol_type().to_string(),
                member_count: g.members().len(),
                total_lag: lag.iter().map(|p| p.lag).sum(),
            });
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(GroupList { groups, as_of: now_ms() })
    })
    .await
    .map_err(|e| ApiError::internal(format!("task join: {e}")))?
}

pub async fn group_detail(handle: Arc<ClusterHandle>, group: String) -> Result<GroupDetail, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(Some(&group), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "describe group", &e))?;
        let info = gl.groups().iter().find(|g| g.name() == group);
        // a group entry with state "Dead" counts as absent
        let info = info.filter(|i| i.state() != "Dead");
        let partitions = group_lag_blocking(&handle, &group, None)?;
        // A group with no broker-side entry AND no committed offsets does not exist.
        let info = match (info, partitions.is_empty()) {
            (Some(i), _) => Some(i),
            (None, false) => None, // empty/expired group that still has offsets
            (None, true) => return Err(ApiError::group_not_found(&handle.name, &group)),
        };
        Ok(GroupDetail {
            group_id: group.clone(),
            state: info.map(|i| i.state().to_string()).unwrap_or_else(|| "Empty".into()),
            members: info.map(|i| i.members().iter().map(|m| MemberInfo {
                member_id: m.id().to_string(),
                client_id: m.client_id().to_string(),
                client_host: m.client_host().to_string(),
            }).collect()).unwrap_or_default(),
            partitions,
            as_of: now_ms(),
        })
    })
    .await
    .map_err(|e| ApiError::internal(format!("task join: {e}")))?
}

#[derive(Debug, Serialize)]
pub struct TopicGroupLag {
    pub group_id: String,
    pub state: String,
    pub total_lag: i64,
    pub partitions: Vec<PartitionLag>,
}

#[derive(Debug, Serialize)]
pub struct TopicConsumers {
    pub topic: String,
    pub groups: Vec<TopicGroupLag>,
    pub as_of: i64,
}

pub async fn topic_consumers(handle: Arc<ClusterHandle>, topic: String) -> Result<TopicConsumers, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "list groups", &e))?;
        let mut groups = Vec::new();
        for g in gl.groups() {
            let partitions = group_lag_blocking(&handle, g.name(), Some(&topic))?;
            if partitions.is_empty() { continue; }
            groups.push(TopicGroupLag {
                group_id: g.name().to_string(),
                state: g.state().to_string(),
                total_lag: partitions.iter().map(|p| p.lag).sum(),
                partitions,
            });
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(TopicConsumers { topic, groups, as_of: now_ms() })
    })
    .await
    .map_err(|e| ApiError::internal(format!("task join: {e}")))?
}
