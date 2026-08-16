use super::{group_consumer, ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "test-hooks")]
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    /// `null` whenever `estimate_error` is set — a failed watermark fetch
    /// never produces a partial or misleading count (see
    /// `assemble_topic_estimate`: one bad partition aborts the whole
    /// topic's estimate, not just its own contribution). Also `null` for an
    /// internal (`__`-prefixed) topic, whose estimate is never computed.
    pub message_estimate: Option<i64>,
    /// `TopicSummary` derives a plain `Serialize` with no
    /// `skip_serializing_if`, so this field is ALWAYS present on the wire —
    /// as JSON `null` whenever there's nothing to report. Only actually
    /// SET (non-null) when `message_estimate` is `null` because a
    /// partition's watermark fetch failed — the Kafka-side reason a client
    /// should render next to the blank count, per "never show stale data
    /// silently: every metric carries its sample timestamp [or, on
    /// failure, why it's missing]". `null` for a healthy estimate and for
    /// an internal topic (no estimate attempted at all).
    pub estimate_error: Option<String>,
    pub size_bytes: Option<u64>,
    pub internal: bool,
}

/// The text that ends up in `TopicSummary.estimate_error` and is rendered
/// VERBATIM inside a tooltip by the frontend's
/// `estimateErrorTitle` (already prefixed there with "Kafka couldn't
/// provide a count — "). Deliberately NOT `error::from_kafka(...).message`
/// — that helper's `"{what}: {err}"` shape leaked the internal operation
/// name ("fetch watermarks") straight into the tooltip. This gives a plain,
/// standalone reason: the raw broker-side text for an ordinary failure, or
/// an honest one-line timeout message — no internal vocabulary either way.
fn estimate_error_message(err: &KafkaError) -> String {
    match err.rdkafka_error_code() {
        Some(RDKafkaErrorCode::OperationTimedOut) | Some(RDKafkaErrorCode::RequestTimedOut) => {
            "counting messages timed out".to_string()
        }
        // The raw `KafkaError`/`RDKafkaErrorCode` text ("Meta data fetch
        // error: BrokerTransportFailure (Local: Broker transport
        // failure)") is Kafka's own diagnostic — same quality bar as any
        // other panel's raw error detail — it just must never be prefixed
        // with the operation name we happened to call it with.
        _ => err.to_string(),
    }
}

/// Sums per-partition watermark deltas for one topic, tolerating a failure:
/// a broken partition yields `(None, Some(first_error))` instead of aborting
/// the caller's whole request. Since a partition that fails once is not
/// going to succeed a moment later within the same request, `fetch` is not
/// called for any partition after the first failure — one bad partition
/// costs one failed call, not the full remaining set.
fn assemble_topic_estimate(
    partition_ids: impl IntoIterator<Item = i32>,
    mut fetch: impl FnMut(i32) -> Result<(i64, i64), String>,
) -> (Option<i64>, Option<String>) {
    let mut estimate = 0i64;
    for id in partition_ids {
        match fetch(id) {
            Ok((lo, hi)) => estimate += hi - lo,
            Err(e) => return (None, Some(e)),
        }
    }
    (Some(estimate), None)
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
            let internal = t.name().starts_with("__");
            // Internal topics (e.g. `__transaction_state`, which alone can
            // carry 50 partitions on a cluster with transactional producers)
            // are hidden by default and their estimates aren't shown
            // meaningfully — skip the watermark fetches entirely rather than
            // paying their tax on every inventory load.
            let (message_estimate, estimate_error) = if internal {
                (None, None)
            } else {
                assemble_topic_estimate(t.partitions().iter().map(|p| p.id()), |id| {
                    handle.consumer()
                        .fetch_watermarks(t.name(), id, ADMIN_TIMEOUT)
                        .map_err(|e| estimate_error_message(&e))
                })
            };
            topics.push(TopicSummary {
                name: t.name().to_string(),
                partitions: t.partitions().len() as i32,
                replication_factor: t.partitions().first().map(|p| p.replicas().len()).unwrap_or(0) as i32,
                message_estimate,
                estimate_error,
                size_bytes: None, // librdkafka has no DescribeLogDirs; stable API shape, filled when possible
                internal,
            });
        }
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(TopicList { topics, as_of: now_ms() })
    })
    .await
    .map_err(ApiError::task_join)?
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
        let opts = AdminOptions::new()
            .operation_timeout(Some(ADMIN_TIMEOUT))
            .request_timeout(Some(ADMIN_TIMEOUT));
        let res = cfg_handle.admin()
            .describe_configs(&[ResourceSpecifier::Topic(&cfg_topic)], &opts)
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
    let partitions = partitions.map_err(ApiError::task_join)??;
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

/// Per-`(cluster, topic_filter)` count of real `fetch_metadata` calls made
/// while building a group-lag topic-partition list. Genuinely test-only now
/// (M7): compiled only under the `test-hooks` feature — see
/// `fetch::FETCH_CALLS`'s doc comment for how `cargo test` always carries
/// it — so a release build never allocates this map or pays its mutex lock
/// on any real request. Scoped by key so it stays meaningful when other
/// tests' group-lag lookups run concurrently against the shared test
/// broker: proves metadata is fetched once per `/groups` or
/// `/topics/{t}/consumers` request and shared across every group in that
/// request's loop, not refetched per group.
#[cfg(feature = "test-hooks")]
static GROUP_METADATA_FETCHES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

#[cfg(feature = "test-hooks")]
fn record_group_metadata_fetch(topic_filter: Option<&str>) {
    let calls = GROUP_METADATA_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry(topic_filter.unwrap_or("*").to_string()).or_insert(0) += 1;
}
/// No-op outside `test-hooks` builds — see `GROUP_METADATA_FETCHES`'s doc comment.
#[cfg(not(feature = "test-hooks"))]
fn record_group_metadata_fetch(_topic_filter: Option<&str>) {}

#[cfg(feature = "test-hooks")]
pub fn group_metadata_fetch_count(topic_filter: Option<&str>) -> u64 {
    GROUP_METADATA_FETCHES.get()
        .and_then(|m| m.lock().unwrap().get(topic_filter.unwrap_or("*")).copied())
        .unwrap_or(0)
}

/// Per-topic count of real (cache-miss) `fetch_watermarks` calls made while
/// resolving group lag. Genuinely test-only now (M7): compiled only under
/// the `test-hooks` feature — see `fetch::FETCH_CALLS`'s doc comment.
/// Proves a partition read by several groups in the same request is
/// watermarked once, not once per group.
#[cfg(feature = "test-hooks")]
static GROUP_WATERMARK_FETCHES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

#[cfg(feature = "test-hooks")]
fn record_group_watermark_fetch(topic: &str) {
    let calls = GROUP_WATERMARK_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry(topic.to_string()).or_insert(0) += 1;
}
/// No-op outside `test-hooks` builds — see `GROUP_WATERMARK_FETCHES`'s doc comment.
#[cfg(not(feature = "test-hooks"))]
fn record_group_watermark_fetch(_topic: &str) {}

#[cfg(feature = "test-hooks")]
pub fn group_watermark_fetch_count(topic: &str) -> u64 {
    GROUP_WATERMARK_FETCHES.get().and_then(|m| m.lock().unwrap().get(topic).copied()).unwrap_or(0)
}

/// Every partition of `topic_filter` (or the whole cluster, when `None`) —
/// one metadata round trip, meant to be built ONCE per `/groups` or
/// `/topics/{t}/consumers` request and shared across every group in the
/// caller's loop, rather than refetched per group.
fn group_lag_topic_partitions(handle: &ClusterHandle, topic_filter: Option<&str>) -> Result<TopicPartitionList, ApiError> {
    record_group_metadata_fetch(topic_filter);
    let md = handle.consumer()
        .fetch_metadata(topic_filter, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
    let mut tpl = TopicPartitionList::new();
    for t in md.topics() {
        if topic_filter.is_some_and(|f| t.name() != f) { continue; }
        for p in t.partitions() {
            tpl.add_partition(t.name(), p.id());
        }
    }
    Ok(tpl)
}

/// Per-request cache of a partition's high watermark, keyed by
/// `(topic, partition)`: shared across every group in one `/groups` or
/// `/topics/{t}/consumers` request's loop, so a partition committed by
/// several groups pays for one `fetch_watermarks` call, not one per group.
type WatermarkCache = HashMap<(String, i32), i64>;

fn cached_high_watermark(handle: &ClusterHandle, cache: &mut WatermarkCache, topic: &str, partition: i32) -> Result<i64, ApiError> {
    if let Some(&hi) = cache.get(&(topic.to_string(), partition)) {
        return Ok(hi);
    }
    record_group_watermark_fetch(topic);
    let (_, hi) = handle.consumer()
        .fetch_watermarks(topic, partition, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "fetch watermarks", &e))?;
    cache.insert((topic.to_string(), partition), hi);
    Ok(hi)
}

/// Committed offsets fetched WITHOUT joining the group (OffsetFetch only):
/// a throwaway consumer configured with the group.id never subscribes, so
/// it cannot trigger a rebalance of the real group. `tpl` and
/// `watermark_cache` are the caller's request-scoped, cross-group state
/// (`group_lag_topic_partitions`, `cached_high_watermark`) — this function
/// itself only ever creates the one thing that must be per-group: the
/// throwaway consumer, since `committed_offsets` is bound to the calling
/// client's own `group.id`.
pub fn group_lag_blocking(
    handle: &ClusterHandle,
    group: &str,
    tpl: &TopicPartitionList,
    watermark_cache: &mut WatermarkCache,
) -> Result<Vec<PartitionLag>, ApiError> {
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
            let hi = cached_high_watermark(handle, watermark_cache, e.topic(), e.partition())?;
            out.push(PartitionLag {
                topic: e.topic().to_string(), partition: e.partition(),
                committed_offset: c, end_offset: hi, lag: (hi - c).max(0),
            });
        }
    }
    out.sort_by_key(|a| (a.topic.clone(), a.partition));
    Ok(out)
}

pub async fn list_groups(handle: Arc<ClusterHandle>) -> Result<GroupList, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "list groups", &e))?;
        let tpl = group_lag_topic_partitions(&handle, None)?;
        let mut watermark_cache = WatermarkCache::new();
        let mut groups = Vec::new();
        for g in gl.groups() {
            let lag = group_lag_blocking(&handle, g.name(), &tpl, &mut watermark_cache)?;
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
    .map_err(ApiError::task_join)?
}

pub async fn group_detail(handle: Arc<ClusterHandle>, group: String) -> Result<GroupDetail, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(Some(&group), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "describe group", &e))?;
        let info = gl.groups().iter().find(|g| g.name() == group);
        // a group entry with state "Dead" counts as absent
        let info = info.filter(|i| i.state() != "Dead");
        let tpl = group_lag_topic_partitions(&handle, None)?;
        let mut watermark_cache = WatermarkCache::new();
        let partitions = group_lag_blocking(&handle, &group, &tpl, &mut watermark_cache)?;
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
    .map_err(ApiError::task_join)?
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
        let tpl = group_lag_topic_partitions(&handle, Some(&topic))?;
        let mut watermark_cache = WatermarkCache::new();
        let mut groups = Vec::new();
        for g in gl.groups() {
            let partitions = group_lag_blocking(&handle, g.name(), &tpl, &mut watermark_cache)?;
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
    .map_err(ApiError::task_join)?
}

#[derive(Debug, Serialize)]
pub struct BrokerInfo { pub id: i32, pub host: String, pub port: i32 }

#[derive(Debug, Serialize)]
pub struct Overview {
    pub brokers: Vec<BrokerInfo>,
    pub controller_id: Option<i32>, // librdkafka metadata does not expose the controller; null in v1
    pub topic_count: usize,
    pub partition_count: usize,
    pub under_replicated_partitions: usize,
    pub as_of: i64,
}

pub async fn overview(handle: Arc<ClusterHandle>) -> Result<Overview, ApiError> {
    tokio::task::spawn_blocking(move || {
        let md = handle.consumer()
            .fetch_metadata(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
        let brokers = md.brokers().iter()
            .map(|b| BrokerInfo { id: b.id(), host: b.host().to_string(), port: b.port() })
            .collect();
        let mut partition_count = 0;
        let mut urp = 0;
        for t in md.topics() {
            partition_count += t.partitions().len();
            urp += t.partitions().iter().filter(|p| p.isr().len() < p.replicas().len()).count();
        }
        Ok(Overview {
            brokers,
            controller_id: None,
            topic_count: md.topics().len(),
            partition_count,
            under_replicated_partitions: urp,
            as_of: now_ms(),
        })
    })
    .await
    .map_err(ApiError::task_join)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn all_partitions_succeed_sums_estimate() {
        let calls = RefCell::new(0);
        let (estimate, error) = assemble_topic_estimate([0, 1, 2], |id| {
            *calls.borrow_mut() += 1;
            Ok((0, (i64::from(id) + 1) * 10))
        });
        assert_eq!(estimate, Some(10 + 20 + 30));
        assert_eq!(error, None);
        assert_eq!(*calls.borrow(), 3);
    }

    #[test]
    fn partition_failure_yields_null_estimate_and_first_error() {
        let calls = RefCell::new(0);
        let (estimate, error) = assemble_topic_estimate([0, 1, 2], |id| {
            *calls.borrow_mut() += 1;
            if id == 1 { Err("boom".to_string()) } else { Ok((0, 5)) }
        });
        assert_eq!(estimate, None);
        assert_eq!(error.as_deref(), Some("boom"));
        // one broken partition must not abort the whole topic count, but it
        // also must not keep paying for partitions after the first failure
        assert_eq!(*calls.borrow(), 2);
    }

    #[test]
    fn no_partitions_yields_zero_estimate() {
        let (estimate, error) = assemble_topic_estimate(std::iter::empty(), |_: i32| -> Result<(i64, i64), String> {
            unreachable!("fetch must not be called for a topic with no partitions")
        });
        assert_eq!(estimate, Some(0));
        assert_eq!(error, None);
    }

    /// `estimate_error` (rendered verbatim in a tooltip by
    /// `estimateErrorTitle`) must never carry the internal "fetch
    /// watermarks:" phrasing `from_kafka`'s generic `{what}: {err}` shape
    /// produces — only the underlying (already Kafka-attributed by the
    /// caller's own "Kafka couldn't provide a count —" prefix) reason.
    #[test]
    fn estimate_error_message_never_carries_the_internal_operation_prefix() {
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure);
        let msg = estimate_error_message(&err);
        assert!(
            !msg.to_lowercase().contains("fetch watermarks"),
            "must not leak the internal operation name we chose to interpolate: {msg:?}"
        );
    }

    #[test]
    fn estimate_error_message_is_honest_about_a_timeout() {
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        assert_eq!(estimate_error_message(&err), "counting messages timed out");
    }
}
