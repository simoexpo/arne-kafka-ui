use super::group_lag_cache::{commits_ttl, lag_rows, partitions_of, CommittedOffset, LagSnapshot};
use super::keyed_cache::Stamped;
use super::{ffi, single_flight, snapshot, ClusterHandle, ADMIN_TIMEOUT};
use crate::error::{self, ApiError};
use crate::util::now_ms;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Serialize, Clone)]
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

#[derive(Debug, Serialize, Clone)]
pub struct TopicList {
    pub topics: Vec<TopicSummary>,
    pub as_of: i64,
}

/// One metadata call, shared by every tab for `TOPICS_TTL_MS` (just under the
/// page's 30s poll, so a lone tab still refreshes every poll).
pub const TOPICS_TTL_MS: i64 = 25_000;

pub async fn list_topics(handle: Arc<ClusterHandle>) -> Result<Arc<TopicList>, ApiError> {
    tokio::task::spawn_blocking(move || {
        snapshot::cached_or_refresh(
            &handle.topics_snapshot,
            &handle.snapshot_flight,
            "topics",
            TOPICS_TTL_MS,
            || list_topics_blocking(&handle).map(Arc::new),
        )
    })
    .await
    .map_err(ApiError::task_join)?
}

fn list_topics_blocking(handle: &ClusterHandle) -> Result<TopicList, ApiError> {
    {
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
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub start_offset: i64,
    pub end_offset: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct ConfigEntryOut {
    pub name: String,
    pub value: Option<String>,
    pub is_default: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub configs: Vec<ConfigEntryOut>,
    pub as_of: i64,
}

/// Cached per topic, shared by every tab looking at that topic (owner ruling
/// 2026-08-20): the backend answers from memory and reaches the broker only
/// when it has nothing fresh, so broker load follows our refresh policy
/// rather than how many people have Arne open.
pub async fn topic_detail(handle: Arc<ClusterHandle>, topic: String) -> Result<Arc<TopicDetail>, ApiError> {
    if let Some(fresh) = handle.topic_detail_cache.fresh(&topic, super::DETAIL_TTL_MS, now_ms()) {
        return Ok(fresh);
    }
    // The guard is HELD across the fetch below: acquiring it and letting it
    // drop with the acquiring closure would release the key before any work
    // began.
    let flight = {
        let flights = handle.detail_flight.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || flights.begin_or_wait_owned(("topic", topic), single_flight::MAX_WAIT))
            .await
            .map_err(ApiError::task_join)?
    };
    // Another request just fetched this one (or is still fetching): read what
    // it produced rather than asking the broker again.
    // Another request just fetched this one (or is still fetching): read what
    // it produced rather than asking the broker again.
    if let (None, Some(value)) = (&flight, handle.topic_detail_cache.get(&topic).map(|e| e.value)) {
        return Ok(value);
    }
    let fresh = Arc::new(topic_detail_uncached(handle.clone(), topic.clone()).await?);
    handle.topic_detail_cache.insert(topic, fresh.clone(), now_ms());
    drop(flight);
    Ok(fresh)
}

async fn topic_detail_uncached(handle: Arc<ClusterHandle>, topic: String) -> Result<TopicDetail, ApiError> {
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
        // Two batched ListOffsets calls for the whole topic, not a sequential
        // watermark round trip per partition: a 1000-partition topic used to
        // cost 1000 round trips on every 10s poll of this tab.
        let wanted: Vec<(String, i32)> = t.partitions().iter().map(|p| (part_topic.clone(), p.id())).collect();
        let starts = ffi::offsets_by_partition(&part_handle, &wanted, ffi::OffsetSpec::Earliest, ADMIN_TIMEOUT)?;
        // High watermarks come from the shared cache at this page's own
        // cadence; low watermarks move only with retention, so they are not
        // worth caching separately.
        let mut heads = Watermarks::labelled(&part_handle, super::DETAIL_TTL_MS, "detail");
        let heads_at = heads.ensure(&wanted)?;
        let ends: std::collections::HashMap<i32, i64> = wanted
            .iter()
            .filter_map(|(t, p)| heads.get(t, *p).map(|hi| (*p, hi)))
            .collect();
        let partitions = t.partitions().iter().map(|p| PartitionInfo {
            id: p.id(), leader: p.leader(),
            replicas: p.replicas().to_vec(), isr: p.isr().to_vec(),
            start_offset: starts.get(&p.id()).copied().unwrap_or(-1),
            end_offset: ends.get(&p.id()).copied().unwrap_or(-1),
        }).collect();
        Ok::<_, ApiError>((partitions, heads_at))
    });

    let (configs, partitions) = tokio::join!(configs_fut, partitions_fut);
    // partition errors (incl. topic_not_found) take precedence over config errors
    let (partitions, heads_at) = partitions.map_err(ApiError::task_join)??;
    let configs = configs?;
    // No fresher than the heads it shows.
    Ok(TopicDetail { name: topic, partitions, configs, as_of: heads_at.min(now_ms()) })
}

#[derive(Debug, Serialize, Clone)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct GroupSummary {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub member_count: usize,
}

/// One group's cluster-wide lag, asked for by name. `total_lag` is `null`
/// when the group has committed nothing anywhere (so it has no position to
/// be behind) or when its lookup failed — `error` then says why.
#[derive(Debug, Serialize, Clone)]
pub struct GroupLagEntry {
    pub group_id: String,
    pub total_lag: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct GroupLagBatch {
    pub groups: Vec<GroupLagEntry>,
    pub as_of: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct GroupList { pub groups: Vec<GroupSummary>, pub as_of: i64 }

#[derive(Debug, Serialize, Clone)]
pub struct MemberInfo { pub member_id: String, pub client_id: String, pub client_host: String }

#[derive(Debug, Serialize, Clone)]
pub struct GroupDetail {
    pub group_id: String,
    pub state: String,
    pub members: Vec<MemberInfo>,
    pub partitions: Vec<PartitionLag>,
    pub as_of: i64,
}

/// Per-`(caller, topic)` count of real (cache-miss) watermark batches. Keyed
/// by CALLER because four different paths now fill the shared watermark cache
/// (lag, the partitions tab, the messages window, throughput sampling) — an
/// unlabelled counter would let a test that opens the messages tab silently
/// break a lag assertion on the same topic. Read only by tests. Compiles
/// unconditionally — see `GROUP_METADATA_FETCHES`.
static GROUP_WATERMARK_FETCHES: OnceLock<Mutex<HashMap<(String, String), u64>>> = OnceLock::new();

fn record_group_watermark_fetch(label: &str, topic: &str) {
    let calls = GROUP_WATERMARK_FETCHES.get_or_init(|| Mutex::new(HashMap::new()));
    *calls.lock().unwrap().entry((label.to_string(), topic.to_string())).or_insert(0) += 1;
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

pub fn group_watermark_fetch_count(caller: &str, topic: &str) -> u64 {
    GROUP_WATERMARK_FETCHES.get()
        .and_then(|m| m.lock().unwrap().get(&(caller.to_string(), topic.to_string())).copied())
        .unwrap_or(0)
}

/// A view over the cluster's shared watermark cache, scoped to one request.
///
/// High watermarks are the fastest-moving thing we read, and four call sites
/// need them — the partitions tab, the messages window, throughput sampling
/// and lag — so they live in ONE cache on the handle (owner ruling
/// 2026-08-20) rather than being refetched per request. Each caller brings
/// its own freshness policy: `ttl_ms` is how old a value may be for THIS
/// purpose. Whatever isn't fresh enough is fetched in a single batched
/// ListOffsets, and every fetch populates the shared cache for everyone else.
///
/// `ensure` reports how old the values it served are, because lag computed
/// against a cached watermark is only as current as that watermark: callers
/// stamp the PAIR with the older of their two sides, never the flattering one.
pub struct Watermarks<'a> {
    handle: &'a ClusterHandle,
    ttl_ms: i64,
    label: &'static str,
    /// Only what THIS request validated: a shared-cache entry that passed the
    /// caller's TTL, or a value just fetched. Reading the shared cache
    /// directly would hand back a value whose refresh FAILED — per-partition
    /// errors are reported per entry, not per batch — and callers rely on a
    /// failed partition being ABSENT so they can skip it instead of scanning,
    /// summing or displaying a stale head.
    validated: HashMap<(String, i32), i64>,
}

impl<'a> Watermarks<'a> {
    pub fn new(handle: &'a ClusterHandle, ttl_ms: i64) -> Self {
        Self::labelled(handle, ttl_ms, "lag")
    }

    pub fn labelled(handle: &'a ClusterHandle, ttl_ms: i64, label: &'static str) -> Self {
        Self { handle, ttl_ms, label, validated: HashMap::new() }
    }

    /// A caller that must see the current head (a bounded scan deciding
    /// whether a partition is exhausted, a rate sample measuring a delta)
    /// cannot accept ANY age: a stale watermark would change its behaviour,
    /// not just its display.
    pub fn always_fresh(handle: &'a ClusterHandle, label: &'static str) -> Self {
        Self::labelled(handle, 0, label)
    }

    /// Returns when the OLDEST value backing `wanted` was sampled — the
    /// caller's honest lower bound for anything it computes from them.
    pub fn ensure(&mut self, wanted: &[(String, i32)]) -> Result<i64, ApiError> {
        let now = now_ms();
        let mut oldest = None;
        let mut missing = Vec::new();
        for key in wanted {
            match self.handle.watermarks.get(key) {
                Some(e) if self.ttl_ms > 0 && now - e.sampled_at < self.ttl_ms => {
                    self.validated.insert(key.clone(), e.value);
                    oldest = Some(oldest.map_or(e.sampled_at, |o: i64| o.min(e.sampled_at)));
                }
                _ => missing.push(key.clone()),
            }
        }
        if missing.is_empty() {
            return Ok(oldest.unwrap_or(now));
        }
        for topic in missing.iter().map(|(t, _)| t.as_str()).collect::<std::collections::BTreeSet<_>>() {
            record_group_watermark_fetch(self.label, topic);
        }
        let fetched_at = now_ms();
        // A partition whose own entry carried an error is deliberately NOT
        // recorded: absent means "unknown", which every caller handles.
        let fetched: Vec<((String, i32), i64)> = ffi::list_offsets_blocking(
            self.handle,
            &missing,
            ffi::OffsetSpec::Latest,
            ADMIN_TIMEOUT,
        )?
        .into_iter()
        .filter(|p| p.error.is_none())
        .map(|p| ((p.topic, p.partition), p.offset))
        .collect();
        for (key, offset) in &fetched {
            self.validated.insert(key.clone(), *offset);
        }
        // One lock, one sweep — not one per partition.
        self.handle.watermarks.insert_many(fetched, fetched_at);
        Ok(oldest.map_or(fetched_at, |o: i64| o.min(fetched_at)))
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<i64> {
        self.validated.get(&(topic.to_string(), partition)).copied()
    }

}

/// Committed offsets fetched WITHOUT joining the group: ListConsumerGroupOffsets
/// is an admin read on the shared client, so it can never trigger a rebalance
/// of the real group and costs no connection of its own (it replaced a
/// throwaway consumer built per group — owner-approved 2026-08-19).
/// `partitions` and `watermark_cache` are the caller's request-scoped,
/// cross-group state, so the metadata lookup and each partition's watermark
/// are paid once per request, not once per group.
/// Every partition this group has committed on, in ONE request (librdkafka's
/// NULL-partitions mode). Never narrowed to a topic: one answer serves the
/// consumers list, every topic's tab and the group's own page, each of which
/// filters it for itself.
/// A group's lag rows, read-through: served from the shared cache when younger
/// than `ttl_ms`, otherwise sampled and stored for every other view. A
/// concurrent asker waits for the sample already running instead of starting
/// its own — but if that sample outlasts the wait AND nothing is cached, the
/// waiter samples too: a page that renders nothing is worse than a duplicated
/// read on an already-struggling cluster.
///
/// The stored value is a PAIR — commits and the heads they were measured
/// against, sampled together — so a cached row is old but never wrong. Public
/// as a test hook: reuse must be asserted with an explicit window, not by
/// racing two HTTP requests against a production TTL.
#[doc(hidden)]
pub fn group_lag_cached(
    handle: &ClusterHandle,
    group: &str,
    ttl_ms: Option<i64>,
    now: i64,
) -> Result<Stamped<Arc<LagSnapshot>>, ApiError> {
    let cached = handle.group_lag.get(&group.to_string());
    // `None` means this group must not be read at all — only served if we
    // happen to know it already.
    let Some(ttl_ms) = ttl_ms else {
        return cached.ok_or_else(|| ApiError::kafka(&handle.name, format!("{group} is not read from this view")));
    };
    if let Some(entry) = cached.filter(|e| now - e.sampled_at < ttl_ms) {
        return Ok(entry);
    }
    match handle.commits_flight.begin_or_wait(group.to_string(), single_flight::MAX_WAIT) {
        // Someone else is reading this group right now: take their answer
        // rather than asking the broker the same question again. If their read
        // is still running, do it ourselves — liveness beats perfect
        // deduplication on a path that only runs when something is slow.
        None => match handle.group_lag.get(&group.to_string()) {
            Some(entry) => Ok(entry),
            None => sample_group_lag(handle, group),
        },
        Some(_flight) => sample_group_lag(handle, group),
    }
}

/// Every partition this group has committed on, in ONE request (librdkafka's
/// NULL-partitions mode, verified by spike 2026-08-20). Never narrowed to a
/// topic: one answer serves the consumers list, every topic's tab and the
/// group's own page.
fn fetch_group_commits(handle: &ClusterHandle, group: &str) -> Result<Vec<CommittedOffset>, ApiError> {
    record_group_offset_fetch(CLUSTER_WIDE, group);
    // NotCoordinator/CoordinatorNotAvailable/CoordinatorLoadInProgress are
    // transient by protocol contract: the coordinator is moving or still
    // loading. Retry briefly instead of surfacing a 502 for a healthy cluster.
    let mut attempt = 0u32;
    let committed = loop {
        match ffi::committed_offsets_blocking(handle, group, None, ADMIN_TIMEOUT) {
            Ok(c) => break c,
            Err(e) if attempt < 4 && e.retriable && is_coordinator_hiccup(&e.message) => {
                attempt += 1;
                std::thread::sleep(std::time::Duration::from_millis(100 * u64::from(attempt)));
            }
            Err(e) => return Err(e),
        }
    };
    Ok(committed
        .into_iter()
        .map(|((topic, partition), offset)| CommittedOffset { topic, partition, offset })
        .collect())
}

/// The coordinator moving or still loading its state reads as a failure but
/// isn't one — the three conditions worth a brief retry.
fn is_coordinator_hiccup(message: &str) -> bool {
    ["NOT_COORDINATOR", "COORDINATOR_NOT_AVAILABLE", "COORDINATOR_LOAD_IN_PROGRESS",
     "Not coordinator", "Coordinator not available", "Coordinator load in progress"]
        .iter()
        .any(|needle| message.contains(needle))
}

/// Reads a group's commits and then, immediately, the heads for exactly those
/// partitions — and stores the pair.
///
/// The heads are read FRESH rather than taken from the shared cache, even
/// though that costs a call: a cached head is older than the commits we just
/// read, and `committed > end` cannot happen in Kafka, so showing it would be
/// visibly incoherent. Reading heads just after the commits errs the only
/// harmless way — by at most the microseconds between the two calls.
fn sample_group_lag(handle: &ClusterHandle, group: &str) -> Result<Stamped<Arc<LagSnapshot>>, ApiError> {
    let commits = fetch_group_commits(handle, group)?;
    let wanted = partitions_of(&commits, None);
    let mut heads = Watermarks::always_fresh(handle, "lag");
    heads.ensure(&wanted)?;
    let snapshot = lag_rows(&commits, |t, p| heads.get(t, p));
    // Stamped on COMPLETION: the reads above can spend a second in coordinator
    // retries, and an entry born older than its own TTL would never serve a
    // hit — every poll would re-read every group.
    let sampled_at = now_ms();
    let snapshot = Arc::new(snapshot);
    handle.group_lag.insert(group.to_string(), snapshot.clone(), sampled_at);
    Ok(Stamped { value: snapshot, sampled_at })
}

/// A group whose protocol type is anything other than a consumer group's uses
/// Kafka's membership machinery for coordination only (Schema Registry's "sr",
/// Connect's "connect") and never commits offsets worth showing. An EMPTY
/// protocol type stays visible: that's a consumer group with no active
/// members, exactly the lagging-dead-consumer case the page exists to surface
/// (owner ruling 2026-08-17).
fn is_consumer_group_protocol(protocol_type: &str) -> bool {
    protocol_type == "consumer" || protocol_type.is_empty()
}

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

/// Just under the consumers page's 10s poll.
pub const GROUPS_TTL_MS: i64 = 8_000;

/// One group as the coordinator describes it, including each live member's
/// assigned topics. The assignments are why this is cached rather than
/// re-fetched per view: a topic's tab needs them to tell a current consumer
/// from one that moved away, and they arrive in the same call the consumers
/// list already makes (owner ruling 2026-08-20).
#[derive(Clone)]
pub struct GroupMembership {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub member_count: usize,
    /// One entry per live member: `Some(topics)` when its assignment decoded,
    /// `None` when it did not — which callers must treat as "could be anything".
    pub member_topics: Vec<Option<Vec<String>>>,
}

/// Every consumer group on the cluster, described once and shared by every
/// view that needs to know who exists.
#[derive(Clone)]
pub struct GroupRoster {
    pub groups: Vec<GroupMembership>,
    pub as_of: i64,
}

/// The roster, read-through: one group-list call per window, shared by the
/// consumers list and every topic's tab.
///
/// Public as a test hook — sharing must be asserted with an explicit window,
/// not by racing requests against a production TTL.
#[doc(hidden)]
pub fn group_roster_cached(handle: &ClusterHandle, ttl_ms: i64) -> Result<Arc<GroupRoster>, ApiError> {
    snapshot::cached_or_refresh(
        &handle.groups_snapshot,
        &handle.snapshot_flight,
        "groups",
        ttl_ms,
        || fetch_group_roster(handle).map(Arc::new),
    )
}

fn fetch_group_roster(handle: &ClusterHandle) -> Result<GroupRoster, ApiError> {
    let gl = handle.consumer()
        .fetch_group_list(None, ADMIN_TIMEOUT)
        .map_err(|e| error::from_kafka(&handle.name, "list groups", &e))?;
    let mut groups: Vec<GroupMembership> = gl.groups().iter()
        .filter(|g| is_consumer_group_protocol(g.protocol_type()))
        .map(|g| GroupMembership {
            group_id: g.name().to_string(),
            state: g.state().to_string(),
            protocol_type: g.protocol_type().to_string(),
            member_count: g.members().len(),
            member_topics: g.members().iter()
                .map(|m| m.assignment().and_then(super::assignment::assigned_topics))
                .collect(),
        })
        .collect();
    groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    Ok(GroupRoster { groups, as_of: now_ms() })
}

pub async fn list_groups(handle: Arc<ClusterHandle>) -> Result<Arc<GroupList>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let roster = group_roster_cached(&handle, GROUPS_TTL_MS)?;
        Ok(Arc::new(GroupList {
            groups: roster.groups.iter().map(|g| GroupSummary {
                group_id: g.group_id.clone(),
                state: g.state.clone(),
                protocol_type: g.protocol_type.clone(),
                member_count: g.member_count,
            }).collect(),
            // The roster's own sample time, not this request's.
            as_of: roster.as_of,
        }))
    })
    .await
    .map_err(ApiError::task_join)?
}

/// Cluster-wide lag for the named groups only — what a paginated consumers
/// list asks for, one page at a time. Reads the shared per-group entry, so a
/// group already sampled for its own page or a topic's tab costs nothing here.
pub async fn groups_lag(handle: Arc<ClusterHandle>, groups: Vec<String>) -> Result<GroupLagBatch, ApiError> {
    tokio::task::spawn_blocking(move || {
        use super::group_lag_cache::LIVE_TTL_MS;
        let now = now_ms();
        let mut out = Vec::with_capacity(groups.len());
        // The batch is only as fresh as the oldest entry it serves — a cached
        // row must not be presented under a "just now" stamp.
        let mut oldest_served: Option<i64> = None;
        for group in groups {
            // Every row here is displayed, so this view always wants the fast tier.
            match group_lag_cached(&handle, &group, Some(LIVE_TTL_MS), now) {
                Ok(entry) => {
                    oldest_served = Some(oldest_served.map_or(entry.sampled_at, |o: i64| o.min(entry.sampled_at)));
                    let rows = &entry.value.rows;
                    // Undetermined, never a confident number, when the group
                    // has committed nothing anywhere OR when a partition's head
                    // could not be read — summing the rest would under-report.
                    let incomplete = entry.value.incomplete_for(None);
                    out.push(GroupLagEntry {
                        group_id: group,
                        total_lag: if rows.is_empty() || incomplete {
                            None
                        } else {
                            Some(rows.iter().map(|r| r.lag).sum())
                        },
                        error: incomplete.then(|| {
                            format!("{} partition(s) could not be read", entry.value.unknown.len())
                        }),
                    });
                }
                Err(e) => out.push(GroupLagEntry {
                    group_id: group,
                    total_lag: None,
                    error: Some(lag_error_reason(&e)),
                }),
            }
        }
        Ok(GroupLagBatch { groups: out, as_of: oldest_served.unwrap_or(now) })
    })
    .await
    .map_err(ApiError::task_join)?
}

/// How old a watermark may be when it is being paired with a freshly fetched
/// committed offset to make a lag number. Kept short deliberately: lag is a
/// SUBTRACTION between two samples, so a stale watermark does not make the
/// answer old, it makes it wrong — a number that was never true at any
/// instant. The pair's reported age is the older of the two sides.
pub const LAG_WATERMARK_TTL_MS: i64 = 2_000;

/// Label for the whole-cluster scope in the test call counter. A group's
/// commits are read once for every view, so every read carries this label.
const CLUSTER_WIDE: &str = "*";

pub async fn group_detail(handle: Arc<ClusterHandle>, group: String) -> Result<Arc<GroupDetail>, ApiError> {
    if let Some(fresh) = handle.group_detail_cache.fresh(&group, super::DETAIL_TTL_MS, now_ms()) {
        return Ok(fresh);
    }
    // The guard is HELD across the fetch below: acquiring it and letting it
    // drop with the acquiring closure would release the key before any work
    // began.
    let flight = {
        let flights = handle.detail_flight.clone();
        let group = group.clone();
        tokio::task::spawn_blocking(move || flights.begin_or_wait_owned(("group", group), single_flight::MAX_WAIT))
            .await
            .map_err(ApiError::task_join)?
    };
    // Another request just fetched this one (or is still fetching): read what
    // it produced rather than asking the broker again.
    if let (None, Some(value)) = (&flight, handle.group_detail_cache.get(&group).map(|e| e.value)) {
        return Ok(value);
    }
    let fresh = Arc::new(group_detail_uncached(handle.clone(), group.clone()).await?);
    handle.group_detail_cache.insert(group, fresh.clone(), now_ms());
    drop(flight);
    Ok(fresh)
}

async fn group_detail_uncached(handle: Arc<ClusterHandle>, group: String) -> Result<GroupDetail, ApiError> {
    tokio::task::spawn_blocking(move || {
        let gl = handle.consumer()
            .fetch_group_list(Some(&group), ADMIN_TIMEOUT)
            .map_err(|e| error::from_kafka(&handle.name, "describe group", &e))?;
        let info = gl.groups().iter().find(|g| g.name() == group);
        // a group entry with state "Dead" counts as absent
        let info = info.filter(|i| i.state() != "Dead");
        // The same shared entry the consumers list uses, so opening a group's
        // page after seeing it in the list costs nothing. Errors keep their own
        // ApiError, so a broker timeout still reports as a timeout.
        let entry = group_lag_cached(&handle, &group, Some(super::group_lag_cache::LIVE_TTL_MS), now_ms())?;
        // The snapshot is shared, so this view copies just the rows it renders.
        let (partitions, pair_at) = (entry.value.rows.clone(), entry.sampled_at);
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
            // as fresh as the older of the two samples behind the lag
            as_of: pair_at,
        })
    })
    .await
    .map_err(ApiError::task_join)?
}

#[derive(Debug, Serialize)]
pub struct TopicGroupLag {
    pub group_id: String,
    pub state: String,
    /// `null` when this group's position on the topic isn't determinable: it
    /// holds an assignment but hasn't committed yet (where it reads is
    /// decided by `auto.offset.reset` or an explicit seek, not by us), or its
    /// offset lookup failed — `error` then carries the reason. Never 0 as a
    /// stand-in for "we don't know" (owner ruling 2026-08-19).
    pub total_lag: Option<i64>,
    pub partitions: Vec<PartitionLag>,
    /// Always present on the wire, `null` when there is nothing to report.
    pub error: Option<String>,
}

/// A group whose offset lookup failed while we did not know whether it
/// consumes this topic at all. Listing it would invent a consumer; dropping
/// it would hide a gap — so it is disclosed on its own.
#[derive(Debug, Serialize)]
pub struct UncheckedGroup {
    pub group_id: String,
    pub error: String,
}

enum Inspection {
    Listed(TopicGroupLag),
    Unchecked(UncheckedGroup),
    Skipped,
}

/// The reason text shown next to one group's blank lag. Drops the internal
/// operation name `from_kafka` interpolates — the UI's own sentence already
/// says what failed.
fn lag_error_reason(err: &ApiError) -> String {
    ["fetch committed offsets: ", "fetch watermarks: ", "fetch metadata: "]
        .iter()
        .find_map(|p| err.message.strip_prefix(p))
        .unwrap_or(&err.message)
        .to_string()
}

fn inspection_of(
    group_id: &str,
    state: &str,
    class: &super::group_lag_cache::Classification,
    lag: Result<Vec<PartitionLag>, String>,
) -> Inspection {
    use super::group_lag_cache::Classification;
    let row = |total_lag, partitions, error| {
        Inspection::Listed(TopicGroupLag {
            group_id: group_id.to_string(),
            state: state.to_string(),
            total_lag,
            partitions,
            error,
        })
    };
    match (class, lag) {
        (Classification::MovedAway, _) => Inspection::Skipped,
        (Classification::AssignedToTopic, Ok(partitions)) if partitions.is_empty() => {
            row(None, partitions, None)
        }
        (_, Ok(partitions)) if partitions.is_empty() => Inspection::Skipped,
        (_, Ok(partitions)) => {
            let total = partitions.iter().map(|p| p.lag).sum();
            row(Some(total), partitions, None)
        }
        (Classification::AssignedToTopic, Err(e)) => row(None, Vec::new(), Some(e)),
        (Classification::MustInspect, Err(e)) => Inspection::Unchecked(UncheckedGroup {
            group_id: group_id.to_string(),
            error: e,
        }),
    }
}

#[derive(Debug, Serialize)]
pub struct TopicConsumers {
    pub topic: String,
    pub groups: Vec<TopicGroupLag>,
    pub unchecked: Vec<UncheckedGroup>,
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
        use super::group_lag_cache::{classify, Classification};
        let now = now_ms();
        // The SAME roster the consumers list reads: who exists, and what each
        // live member is assigned to. One group-list call per window now serves
        // every tab and every topic (owner ruling 2026-08-20) — the cost being
        // that classification is as old as the roster.
        let roster = group_roster_cached(&handle, GROUPS_TTL_MS)?;
        let mut groups = Vec::new();
        let mut unchecked = Vec::new();
        let mut oldest_served: Option<i64> = None;
        for g in &roster.groups {
            let class = classify(&g.member_topics, &topic);
            if class == Classification::MovedAway {
                continue;
            }
            // The tier is a property of THIS read: a group that shows nothing
            // on this topic is worth re-reading once a minute, even though the
            // same shared entry may be serving another view at 8s.
            // Probed without cloning the group's rows: this only asks whether
            // the topic appears at all.
            let shows_rows_here = handle
                .group_lag
                .with(&g.group_id, |snapshot| snapshot.covers(&topic))
                .unwrap_or(false);
            let ttl = commits_ttl(&class, shows_rows_here);
            let outcome = group_lag_cached(&handle, &g.group_id, ttl, now);
            let pair_at = outcome.as_ref().ok().map(|e| e.sampled_at);
            // Trouble on another topic is none of this tab's business; only a
            // gap in THIS topic's partitions makes its total unstateable.
            let missing_here = outcome
                .as_ref()
                .ok()
                .map(|e| e.value.unknown.iter().filter(|(t, _)| *t == topic).count())
                .unwrap_or(0);
            let narrowed = outcome
                .map(|e| e.value.narrowed(Some(&topic)))
                .map_err(|e| lag_error_reason(&e));
            match inspection_of(&g.group_id, &g.state, &class, narrowed) {
                Inspection::Listed(mut row) => {
                    if missing_here > 0 {
                        // The rows we do have stay visible; the total does not
                        // pretend to be complete.
                        row.total_lag = None;
                        row.error = Some(format!("{missing_here} partition(s) could not be read"));
                    }
                    // Every listed row rests on a real sample, including one
                    // that says "assigned but nothing committed yet".
                    if let Some(at) = pair_at {
                        oldest_served = Some(oldest_served.map_or(at, |o: i64| o.min(at)));
                    }
                    groups.push(row);
                }
                Inspection::Unchecked(u) => unchecked.push(u),
                Inspection::Skipped => {}
            }
        }
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        unchecked.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        // With nothing rendered, the honest stamp is when we learned who
        // exists — not the moment of the request.
        Ok(TopicConsumers { topic, groups, unchecked, as_of: oldest_served.unwrap_or(roster.as_of) })
    })
    .await
    .map_err(ApiError::task_join)?
}

#[derive(Debug, Serialize, Clone)]
pub struct BrokerInfo { pub id: i32, pub host: String, pub port: i32 }

#[derive(Debug, Serialize, Clone)]
pub struct TopicPartitions { pub name: String, pub partitions: usize }

#[derive(Debug, Serialize, Clone)]
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

/// Just under the overview's 10s poll.
pub const OVERVIEW_TTL_MS: i64 = 8_000;

pub async fn overview(handle: Arc<ClusterHandle>) -> Result<Arc<Overview>, ApiError> {
    tokio::task::spawn_blocking(move || {
        snapshot::cached_or_refresh(
            &handle.overview_snapshot,
            &handle.snapshot_flight,
            "overview",
            OVERVIEW_TTL_MS,
            || overview_blocking(&handle).map(Arc::new),
        )
    })
    .await
    .map_err(ApiError::task_join)?
}

fn overview_blocking(handle: &ClusterHandle) -> Result<Overview, ApiError> {
    {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::group_lag_cache::Classification;

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

    /// Owner rulings 2026-08-19: a group holding an assignment for the topic
    /// is listed even with no committed offsets — it IS consuming, we just
    /// cannot know its position (auto.offset.reset, an explicit seek) until it
    /// commits, so its lag reads as undetermined rather than zero. And one
    /// group's failed offset lookup must never blank the whole tab: a group we
    /// know consumes the topic keeps its row with the reason attached, while a
    /// group whose relationship to the topic is unknown is disclosed
    /// separately instead of being listed or silently dropped.
    #[test]
    fn assigned_group_without_commits_is_listed_with_undetermined_lag() {
        let ins = inspection_of("g", "Stable", &Classification::AssignedToTopic, Ok(vec![]));
        let Inspection::Listed(row) = ins else { panic!("must be listed") };
        assert_eq!(row.total_lag, None);
        assert!(row.partitions.is_empty());
        assert_eq!(row.error, None);
    }

    #[test]
    fn assigned_group_with_commits_reports_summed_lag() {
        let ins = inspection_of("g", "Stable", &Classification::AssignedToTopic, Ok(vec![lag(3), lag(4)]));
        let Inspection::Listed(row) = ins else { panic!("must be listed") };
        assert_eq!(row.total_lag, Some(7));
        assert_eq!(row.partitions.len(), 2);
    }

    #[test]
    fn inspected_group_without_offsets_here_is_not_a_consumer_of_this_topic() {
        assert!(matches!(
            inspection_of("g", "Empty", &Classification::MustInspect, Ok(vec![])),
            Inspection::Skipped
        ));
    }

    #[test]
    fn inspected_group_with_offsets_here_is_listed() {
        let ins = inspection_of("g", "Empty", &Classification::MustInspect, Ok(vec![lag(5)]));
        let Inspection::Listed(row) = ins else { panic!("must be listed") };
        assert_eq!(row.total_lag, Some(5));
    }

    #[test]
    fn a_known_consumers_failed_lookup_keeps_its_row_and_says_why() {
        let ins = inspection_of("g", "Stable", &Classification::AssignedToTopic, Err("Broker: Not coordinator".into()));
        let Inspection::Listed(row) = ins else { panic!("a known consumer stays listed") };
        assert_eq!(row.total_lag, None);
        assert_eq!(row.error.as_deref(), Some("Broker: Not coordinator"));
    }

    #[test]
    fn an_unknown_groups_failed_lookup_is_disclosed_not_listed() {
        let ins = inspection_of("g", "Empty", &Classification::MustInspect, Err("Broker: Not coordinator".into()));
        let Inspection::Unchecked(u) = ins else { panic!("must be disclosed as unchecked") };
        assert_eq!(u.group_id, "g");
        assert_eq!(u.error, "Broker: Not coordinator");
    }

    /// The UI sentence already says what failed; the internal operation name
    /// `from_kafka` interpolates must not show up inside the reason.
    #[test]
    fn lag_error_reason_drops_the_internal_operation_name() {
        let err = ApiError::kafka("c", "fetch committed offsets: Broker: Not coordinator");
        assert_eq!(lag_error_reason(&err), "Broker: Not coordinator");
        let plain = ApiError::kafka("c", "everything is on fire");
        assert_eq!(lag_error_reason(&plain), "everything is on fire");
    }

    fn lag(l: i64) -> PartitionLag {
        PartitionLag { topic: "t".into(), partition: 0, committed_offset: 1, end_offset: 1 + l, lag: l }
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
