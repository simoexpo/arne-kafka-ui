use super::{group_consumer, ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Serialize)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    /// The WORST partition's in-sync replica count — equal to
    /// `replication_factor` means every partition is fully replicated
    /// (owner ruling 2026-08-18). No message counts here: estimates cost
    /// one watermark round trip per partition; real sizes arrive when
    /// librdkafka ships DescribeLogDirs (confluentinc/librdkafka#5333).
    pub isr: i32,
    pub internal: bool,
}

/// Kafka's own internals are `__`-prefixed; `_schemas` (the schema
/// registry's storage topic) is formally a regular topic but effectively
/// internal (owner ruling 2026-08-17) — it hides with the rest. Other
/// single-underscore names are user topics.
fn is_internal_topic(name: &str) -> bool {
    name.starts_with("__") || name == "_schemas"
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
            topics.push(TopicSummary {
                name: t.name().to_string(),
                partitions: t.partitions().len() as i32,
                replication_factor: t.partitions().first().map(|p| p.replicas().len()).unwrap_or(0) as i32,
                isr: t.partitions().iter().map(|p| p.isr().len()).min().unwrap_or(0) as i32,
                internal: is_internal_topic(t.name()),
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
/// while building a group-lag topic-partition list. Read only by tests:
/// proves metadata is fetched once per `/groups` or
/// `/topics/{t}/consumers` request and shared across every group in that
/// request's loop, not refetched per group. Scoped by key so it stays
/// meaningful when other tests' group-lag lookups run concurrently against
/// the shared test broker. One counter bump per broker round trip is noise
/// next to the round trip itself, so this compiles unconditionally.
static GROUP_METADATA_FETCHES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn record_group_metadata_fetch(topic_filter: Option<&str>) {
    let calls = GROUP_METADATA_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry(topic_filter.unwrap_or("*").to_string()).or_insert(0) += 1;
}

pub fn group_metadata_fetch_count(topic_filter: Option<&str>) -> u64 {
    GROUP_METADATA_FETCHES.get()
        .and_then(|m| m.lock().unwrap().get(topic_filter.unwrap_or("*")).copied())
        .unwrap_or(0)
}

/// Per-topic count of real (cache-miss) `fetch_watermarks` calls made while
/// resolving group lag. Read only by tests: proves a partition read by
/// several groups in the same request is watermarked once, not once per
/// group. Compiles unconditionally — see `GROUP_METADATA_FETCHES`.
static GROUP_WATERMARK_FETCHES: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn record_group_watermark_fetch(topic: &str) {
    let calls = GROUP_WATERMARK_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry(topic.to_string()).or_insert(0) += 1;
}

/// Per-`(scope, group)` count of group-offset INSPECTIONS — one bump per
/// `group_lag_blocking` call, whether its `committed_offsets` succeeds,
/// errors, or takes several coordinator retries. Scope is the endpoint's
/// calling endpoint ("*" for the whole-cluster group list/detail). Read only
/// by tests: proves the consumers-tab cache and the moved-away filter
/// suppress per-group fetches. Scoped so a concurrent test exercising
/// `/groups` against the shared broker (which fetches EVERY group) cannot
/// bump a topic-scoped assertion.
static GROUP_OFFSET_FETCHES: OnceLock<Mutex<HashMap<(String, String), u64>>> = OnceLock::new();

/// Bounds the counter's keyspace, which is `topics viewed × groups` — a
/// long-lived process on a large cluster would otherwise accumulate entries
/// forever for numbers only tests read. Far above any test's cardinality,
/// so assertions stay exact.
const MAX_TRACKED_GROUP_FETCHES: usize = 4096;

fn record_group_offset_fetch(scope: &str, group: &str) {
    let calls = GROUP_OFFSET_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut calls = calls.lock().unwrap();
    let key = (scope.to_string(), group.to_string());
    let room = calls.len() < MAX_TRACKED_GROUP_FETCHES;
    match calls.get_mut(&key) {
        Some(n) => *n += 1,
        None if room => {
            calls.insert(key, 1);
        }
        None => {}
    }
}

pub fn group_offset_fetch_count(scope: &str, group: &str) -> u64 {
    GROUP_OFFSET_FETCHES.get()
        .and_then(|m| m.lock().unwrap().get(&(scope.to_string(), group.to_string())).copied())
        .unwrap_or(0)
}

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
    scope: &str,
    group: &str,
    tpl: &TopicPartitionList,
    watermark_cache: &mut WatermarkCache,
) -> Result<Vec<PartitionLag>, ApiError> {
    record_group_offset_fetch(scope, group);
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

/// Coordination-only groups (schema registry's "sr", Connect's "connect")
/// never commit offsets, so the consumers page has nothing true to say
/// about them — hidden entirely (owner ruling 2026-08-17). An empty
/// protocol type is a consumer group with no active members: visible,
/// that's the lagging-dead-consumer case the page exists for.
fn is_consumer_group_protocol(protocol_type: &str) -> bool {
    protocol_type == "consumer" || protocol_type.is_empty()
}

/// Topic names only — one metadata round trip, none of `list_topics`'s
/// per-partition watermark fetches. For callers that need just the
/// inventory of names (subject-usage inference).
pub async fn topic_names(handle: Arc<ClusterHandle>) -> Result<Vec<String>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let md = handle.consumer()
            .fetch_metadata(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "fetch metadata", &e))?;
        Ok(md.topics().iter().map(|t| t.name().to_string()).collect())
    })
    .await
    .map_err(ApiError::task_join)?
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
            if !is_consumer_group_protocol(g.protocol_type()) {
                continue;
            }
            let lag = group_lag_blocking(&handle, "*", g.name(), &tpl, &mut watermark_cache)?;
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
        let partitions = group_lag_blocking(&handle, "*", &group, &tpl, &mut watermark_cache)?;
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

/// Owner design 2026-08-19. One group-list call classifies every group via
/// its members' assignment blobs; only a topic's current consumers and the
/// unfilterable groups (empty or undecodable — fail open) are OffsetFetched,
/// and even those through the handle's age-tiered cache
/// (`group_lag_cache`), so repeated polls refresh only what's due. A live
/// group fully assigned to other topics is skipped outright: its residual
/// offsets on this topic stay visible on the group's own detail page until
/// Kafka expires them, but it is no longer a consumer of this topic.
pub async fn topic_consumers(handle: Arc<ClusterHandle>, topic: String) -> Result<TopicConsumers, ApiError> {
    tokio::task::spawn_blocking(move || {
        use super::group_lag_cache::{classify, needs_refresh, CachedEntry, Classification};
        let now = now_ms();
        let gl = handle.consumer()
            .fetch_group_list(None, ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "list groups", &e))?;
        // Built on first use only: a poll that finds every group's lag fresh
        // must cost nothing beyond the group list above.
        let mut tpl: Option<TopicPartitionList> = None;
        let mut watermark_cache = WatermarkCache::new();
        let mut groups = Vec::new();
        let mut keep = std::collections::HashSet::new();
        let mut oldest_served: Option<i64> = None;
        for g in gl.groups() {
            if !is_consumer_group_protocol(g.protocol_type()) {
                continue;
            }
            let member_topics: Vec<Option<Vec<String>>> = g.members().iter()
                .map(|m| m.assignment().and_then(super::assignment::assigned_topics))
                .collect();
            let class = classify(&member_topics, &topic);
            if class == Classification::MovedAway {
                continue;
            }
            keep.insert(g.name().to_string());
            // A cached entry that vanished between the probe and the read
            // (a concurrent poll's eviction) lands in the refresh branch
            // rather than dropping the group from the response.
            let cached = if needs_refresh(&class, handle.group_lag_cache.freshness(&topic, g.name()), now) {
                None
            } else {
                handle.group_lag_cache.get(&topic, g.name())
            };
            let entry = match cached {
                Some(e) => e,
                None => {
                    let tpl = match tpl {
                        Some(ref t) => t,
                        None => tpl.insert(group_lag_topic_partitions(&handle, Some(&topic))?),
                    };
                    let partitions = group_lag_blocking(&handle, &topic, g.name(), tpl, &mut watermark_cache)?;
                    // stamped on completion, not at request start: a long
                    // sweep must not record its last entries as already stale
                    let entry = CachedEntry { partitions, sampled_at: now_ms() };
                    handle.group_lag_cache.insert(&topic, g.name(), entry.clone());
                    entry
                }
            };
            if entry.partitions.is_empty() {
                continue;
            }
            oldest_served = Some(oldest_served.map_or(entry.sampled_at, |o: i64| o.min(entry.sampled_at)));
            groups.push(TopicGroupLag {
                group_id: g.name().to_string(),
                state: g.state().to_string(),
                total_lag: entry.partitions.iter().map(|p| p.lag).sum(),
                partitions: entry.partitions,
            });
        }
        handle.group_lag_cache.evict(&topic, &|g| keep.contains(g), now);
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(TopicConsumers { topic, groups, as_of: oldest_served.unwrap_or(now) })
    })
    .await
    .map_err(ApiError::task_join)?
}

#[derive(Debug, Serialize)]
pub struct BrokerInfo { pub id: i32, pub host: String, pub port: i32 }

#[derive(Debug, Serialize)]
pub struct TopicPartitions { pub name: String, pub partitions: usize }

#[derive(Debug, Serialize)]
pub struct Overview {
    pub brokers: Vec<BrokerInfo>,
    pub controller_id: Option<i32>, // librdkafka metadata does not expose the controller; null in v1
    pub topic_count: usize,
    pub partition_count: usize,
    pub under_replicated_partitions: usize,
    pub top_topics: Vec<TopicPartitions>,
    pub as_of: i64,
}

/// Ranked by partition count — the only size-ish signal the overview's one
/// metadata call already carries. Message estimates cost O(partitions)
/// watermark calls and real sizes need DescribeLogDirs, which librdkafka
/// lacks (confluentinc/librdkafka#5333).
fn top_topics_by_partitions<'a>(topics: impl IntoIterator<Item = (&'a str, usize)>) -> Vec<TopicPartitions> {
    let mut out: Vec<TopicPartitions> = topics
        .into_iter()
        .filter(|(name, _)| !is_internal_topic(name))
        .map(|(name, partitions)| TopicPartitions { name: name.to_string(), partitions })
        .collect();
    out.sort_by(|a, b| b.partitions.cmp(&a.partitions).then_with(|| a.name.cmp(&b.name)));
    out.truncate(10);
    out
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
            top_topics: top_topics_by_partitions(md.topics().iter().map(|t| (t.name(), t.partitions().len()))),
            as_of: now_ms(),
        })
    })
    .await
    .map_err(ApiError::task_join)?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owner ruling 2026-08-17: groups that use Kafka's membership protocol
    /// only for coordination (schema registry's "sr", Connect's "connect")
    /// never commit offsets — they hide from the consumers page entirely.
    /// An EMPTY protocol type stays visible: that's a consumer group with
    /// no active members, exactly the lagging-dead-consumer case the page
    /// exists to surface.
    #[test]
    fn coordination_only_groups_are_not_consumer_groups() {
        assert!(is_consumer_group_protocol("consumer"));
        assert!(is_consumer_group_protocol(""));
        assert!(!is_consumer_group_protocol("sr"));
        assert!(!is_consumer_group_protocol("connect"));
    }

    /// Owner ruling 2026-08-18: the overview's top-topics table ranks by
    /// partition count (free from the one metadata call) instead of message
    /// estimates (O(partitions) watermark calls) — revisit when librdkafka
    /// ships DescribeLogDirs (confluentinc/librdkafka#5333) and real sizes
    /// become available.
    #[test]
    fn top_topics_ranked_by_partitions_internal_excluded_capped_at_ten() {
        let mut input: Vec<(String, usize)> = (0..12).map(|i| (format!("t-{i:02}"), i + 1)).collect();
        input.push(("__consumer_offsets".into(), 50));
        input.push(("_schemas".into(), 50));
        input.push(("t-tie".into(), 12));
        let top = top_topics_by_partitions(input.iter().map(|(n, p)| (n.as_str(), *p)));
        assert_eq!(top.len(), 10);
        assert_eq!(top[0].name, "t-11");
        assert_eq!(top[0].partitions, 12);
        assert_eq!(top[1].name, "t-tie"); // tie broken by name, after t-11
        assert!(top.iter().all(|t| !t.name.starts_with("__") && t.name != "_schemas"));
    }

    /// Owner ruling 2026-08-17: `_schemas` (the schema registry's storage
    /// topic) is formally not internal but effectively is — it hides with
    /// the `__`-prefixed ones. Other single-underscore names stay visible.
    #[test]
    fn schema_registry_storage_topic_counts_as_internal() {
        assert!(is_internal_topic("__consumer_offsets"));
        assert!(is_internal_topic("_schemas"));
        assert!(!is_internal_topic("_my_topic"));
        assert!(!is_internal_topic("demo-orders"));
    }

}
